"""Document rollback cost follows the write-set instead of collection size."""

import asyncio

from copy import deepcopy
from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine


_INJECTED_INDEX_FAILURE = "injected index update failure"
_INJECTED_PUBLICATION_FAILURE = "injected publication failure"
_SECOND_INDEX_UPDATE = 2
_GROUP_MEMBER_COUNT = 33


@pytest.mark.parametrize("mutation", ["insert", "update", "delete"])
def test_document_mutations_do_not_capture_complete_collection_state(mutation):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": value, "group": value % 5} for value in range(500)]
            )
            await collection.create_index("group")

            with patch.object(
                engine,
                "_snapshot_collection_state_locked",
                side_effect=AssertionError("complete collection snapshot"),
            ):
                if mutation == "insert":
                    await collection.insert_one({"_id": 501, "group": 1})
                elif mutation == "update":
                    await collection.update_one(
                        {"_id": 250},
                        {"$set": {"group": 3}},
                    )
                else:
                    await collection.delete_one({"_id": 250})

    asyncio.run(exercise())


def test_document_delta_restores_storage_and_indexes_after_partial_bulk_failure():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": value, "group": value % 3} for value in range(100)]
            )
            await collection.create_index("group")
            expected = await collection.find({}).to_list()
            expected_index_data = deepcopy(engine._index_data)
            update_indexes = engine._update_indexes_locked
            calls = 0

            def fail_after_second_index_update(*args, **kwargs):
                nonlocal calls
                update_indexes(*args, **kwargs)
                calls += 1
                if calls == _SECOND_INDEX_UPDATE:
                    raise RuntimeError(_INJECTED_INDEX_FAILURE)

            with (
                patch.object(
                    engine,
                    "_snapshot_collection_state_locked",
                    side_effect=AssertionError("complete collection snapshot"),
                ),
                patch.object(
                    engine,
                    "_update_indexes_locked",
                    side_effect=fail_after_second_index_update,
                ),
                pytest.raises(RuntimeError, match=_INJECTED_INDEX_FAILURE),
            ):
                await collection.insert_many(
                    [
                        {"_id": 100, "group": 1},
                        {"_id": 101, "group": 2},
                    ]
                )

            assert await collection.find({}).to_list() == expected
            assert engine._index_data == expected_index_data
            assert await collection.count_documents({"group": 1}) == _GROUP_MEMBER_COUNT
            assert await collection.count_documents({"group": 2}) == _GROUP_MEMBER_COUNT

    asyncio.run(exercise())


def test_document_delta_does_not_retry_failed_publication_during_rollback():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 1, "value": "before"})
            await collection.create_index("value")

            with (
                patch.object(
                    engine,
                    "_snapshot_collection_state_locked",
                    side_effect=AssertionError("complete collection snapshot"),
                ),
                patch.object(
                    engine,
                    "_record_committed_change",
                    side_effect=RuntimeError(_INJECTED_PUBLICATION_FAILURE),
                ) as publication,
                pytest.raises(RuntimeError, match=_INJECTED_PUBLICATION_FAILURE),
            ):
                await collection.update_one(
                    {"_id": 1},
                    {"$set": {"value": "after"}},
                )

            assert publication.call_count == 1
            assert await collection.find_one({"_id": 1}) == {
                "_id": 1,
                "value": "before",
            }
            assert await collection.count_documents({"value": "before"}) == 1
            assert await collection.count_documents({"value": "after"}) == 0

    asyncio.run(exercise())


def test_failed_insert_restores_implicit_collection_registration_and_order():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            with (
                patch.object(
                    engine,
                    "_record_committed_change",
                    side_effect=RuntimeError(_INJECTED_PUBLICATION_FAILURE),
                ),
                pytest.raises(RuntimeError, match=_INJECTED_PUBLICATION_FAILURE),
            ):
                await collection.insert_one({"_id": "failed"})

            assert "records" not in await client.test.list_collection_names()
            await collection.insert_many([{"_id": 0}, {"_id": 2}])
            with (
                patch.object(
                    engine,
                    "_record_committed_change",
                    side_effect=RuntimeError(_INJECTED_PUBLICATION_FAILURE),
                ),
                pytest.raises(RuntimeError, match=_INJECTED_PUBLICATION_FAILURE),
            ):
                await collection.insert_one({"_id": 1})
            await collection.insert_one({"_id": 1})
            assert [doc["_id"] for doc in await collection.find({}).to_list()] == [
                0,
                2,
                1,
            ]

    asyncio.run(exercise())


def test_document_delta_rolls_back_inside_transaction_view():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 1, "value": "before"})
            await collection.create_index("value")
            session = client.start_session()
            try:
                session.start_transaction()
                with (
                    patch.object(
                        engine,
                        "_record_committed_change",
                        side_effect=RuntimeError(_INJECTED_PUBLICATION_FAILURE),
                    ),
                    pytest.raises(
                        RuntimeError,
                        match=_INJECTED_PUBLICATION_FAILURE,
                    ),
                ):
                    await collection.update_one(
                        {"_id": 1},
                        {"$set": {"value": "after"}},
                        session=session,
                    )
                assert await collection.find_one(
                    {"value": "before"},
                    session=session,
                ) == {"_id": 1, "value": "before"}
                assert (
                    await collection.count_documents(
                        {"value": "after"},
                        session=session,
                    )
                    == 0
                )
                state = engine._mvcc_states[session.session_id]
                assert state.touched_namespaces == set()
                assert not state.has_writes
                session.abort_transaction()
            finally:
                session.close()

            assert await collection.find_one({"_id": 1}) == {
                "_id": 1,
                "value": "before",
            }

    asyncio.run(exercise())
