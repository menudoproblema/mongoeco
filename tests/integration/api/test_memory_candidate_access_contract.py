"""An eligible index selects rows before collection-sized capture or traversal."""

import asyncio

from datetime import UTC, datetime, timedelta

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines._memory_collection import MemoryIndexMap
from mongoeco.engines.memory import MemoryEngine


@pytest.mark.parametrize("operation", ["find", "count", "update", "delete"])
@pytest.mark.parametrize("matches", [0, 1, 3])
def test_selective_index_never_enumerates_the_collection(operation, matches):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            documents = [
                {"_id": index, "key": "yes" if index < matches else "no"}
                for index in reversed(range(1000))
            ]
            await collection.insert_many(documents)
            await collection.create_index("key")
            storage = engine._storage["test"]["records"]

            class NoScan(type(storage)):
                def items(self):
                    pytest.fail("indexed access enumerated collection items")

                def __iter__(self):
                    pytest.fail("indexed access enumerated collection keys")

                def values(self):
                    pytest.fail("indexed access enumerated collection values")

            engine._storage["test"]["records"] = NoScan(storage)
            if operation == "find":
                assert await collection.find({"key": "yes"}).to_list() == [
                    document for document in documents if document["key"] == "yes"
                ]
            elif operation == "count":
                assert await collection.count_documents({"key": "yes"}) == matches
            else:
                expected = [doc for doc in documents if doc["key"] == "yes"]
                if operation == "update":
                    result = await collection.update_one(
                        {"key": "yes"}, {"$set": {"changed": True}}
                    )
                    assert result.modified_count == min(matches, 1)
                    if expected:
                        expected[0] = {**expected[0], "changed": True}
                else:
                    result = await collection.delete_one({"key": "yes"})
                    assert result.deleted_count == min(matches, 1)
                    expected = expected[1:]
                assert await collection.find({"key": "yes"}).to_list() == expected

    asyncio.run(exercise())


@pytest.mark.parametrize("transaction", [False, True])
def test_candidate_order_survives_mutation_and_reinsertion(transaction):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": key, "key": "yes"} for key in ["z", "a", "m", "b"]]
            )
            await collection.create_index("key")
            session = client.start_session() if transaction else None
            if session is not None:
                session.start_transaction()
            try:
                await collection.update_one(
                    {"_id": "a"}, {"$set": {"extra": 1}}, session=session
                )
                await collection.delete_one({"_id": "z"}, session=session)
                await collection.insert_one({"_id": "z", "key": "yes"}, session=session)
                result = await collection.find(
                    {"key": "yes"}, session=session
                ).to_list()
                assert [row["_id"] for row in result] == ["a", "m", "b", "z"]
                if session is not None:
                    session.commit_transaction()
                result = (
                    await collection.find({"key": "yes"}).skip(1).limit(2).to_list()
                )
                assert [row["_id"] for row in result] == ["m", "b"]
            finally:
                if session is not None:
                    if session.in_transaction:
                        session.abort_transaction()
                    session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("transaction", [False, True])
def test_failed_index_mutation_restores_candidate_order(monkeypatch, transaction):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            initial = [{"_id": key, "key": "yes"} for key in ["z", "a", "m"]]
            await collection.insert_many(initial)
            await collection.create_index("key")
            session = client.start_session() if transaction else None
            if session is not None:
                session.start_transaction()
            try:
                update_indexes = engine._update_indexes_locked

                def fail_after_index_change(*args, **kwargs):
                    update_indexes(*args, **kwargs)
                    message = "injected index failure"
                    raise RuntimeError(message)

                with monkeypatch.context() as patch:
                    patch.setattr(
                        engine, "_update_indexes_locked", fail_after_index_change
                    )
                    with pytest.raises(RuntimeError, match="injected index failure"):
                        await collection.delete_one({"key": "yes"}, session=session)
                assert (
                    await collection.find({"key": "yes"}, session=session).to_list()
                    == initial
                )
                await collection.delete_one({"_id": "z"}, session=session)
                await collection.insert_one(initial[0], session=session)
                expected = initial[1:] + initial[:1]
                assert (
                    await collection.find({"key": "yes"}, session=session).to_list()
                    == expected
                )
                if session is not None:
                    session.abort_transaction()
                    expected = initial
                assert await collection.find({"key": "yes"}).to_list() == expected
            finally:
                if session is not None:
                    if session.in_transaction:
                        session.abort_transaction()
                    session.close()

    asyncio.run(exercise())


def test_partial_and_multikey_access_keep_residuals_and_order():
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            collection = client.test.records
            documents = [
                {"_id": "z", "key": ["yes", "yes"], "active": False},
                {"_id": "a", "key": ["no", "yes"], "active": True},
                {"_id": "m", "key": "yes", "active": True},
                {"_id": "b", "key": "no", "active": True},
            ]
            await collection.insert_many(documents)
            await collection.create_index(
                "key", partial_filter_expression={"active": True}
            )
            assert await collection.find({"key": "yes"}).to_list() == documents[:3]
            selector = {"key": "yes", "active": True}
            assert await collection.find(selector).to_list() == documents[1:3]
            assert await collection.count_documents(selector) == len(documents[1:3])
            await collection.update_one(selector, {"$set": {"changed": True}})
            documents[1]["changed"] = True
            assert await collection.find(selector).to_list() == documents[1:3]
            await collection.delete_one(selector)
            assert await collection.find({"key": "yes"}).to_list() == [
                documents[0],
                documents[2],
            ]

    asyncio.run(exercise())


def test_candidate_order_follows_ttl_rename_and_recreation():
    async def exercise():
        now = [datetime(2026, 1, 1, tzinfo=UTC)]
        async with AsyncMongoClient(
            MemoryEngine(), now_factory=lambda: now[0]
        ) as client:
            collection = client.test.records
            await collection.create_index("key")
            await collection.create_index("expires", expire_after_seconds=0)
            await collection.insert_many(
                [
                    {
                        "_id": "z",
                        "key": "yes",
                        "expires": now[0] + timedelta(seconds=1),
                    },
                    {"_id": "a", "key": "yes"},
                    {"_id": "m", "key": "yes"},
                ]
            )
            now[0] += timedelta(seconds=2)
            assert [
                row["_id"] for row in await collection.find({"key": "yes"}).to_list()
            ] == ["a", "m"]
            await collection.insert_one({"_id": "z", "key": "yes"})
            await collection.rename("renamed")
            collection = client.test.renamed
            assert [
                row["_id"] for row in await collection.find({"key": "yes"}).to_list()
            ] == ["a", "m", "z"]
            await collection.drop()
            await collection.insert_many(
                [{"_id": key, "key": "yes"} for key in ["z", "m", "a"]]
            )
            await collection.create_index("key")
            assert [
                row["_id"] for row in await collection.find({"key": "yes"}).to_list()
            ] == ["z", "m", "a"]

    asyncio.run(exercise())


def test_exact_indexed_first_selects_only_the_required_natural_prefix(
    monkeypatch,
):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": index, "key": "yes"} for index in range(1000)]
            )
            await collection.create_index("key")
            observed_limits = []
            ordered_keys = MemoryIndexMap.ordered_storage_keys

            def observe(self, key, *, limit=None):
                observed_limits.append(limit)
                return ordered_keys(self, key, limit=limit)

            monkeypatch.setattr(MemoryIndexMap, "ordered_storage_keys", observe)
            result = await collection.find({"key": "yes"}).skip(2).first()

            assert result == {"_id": 2, "key": "yes"}
            assert observed_limits == [3]

    asyncio.run(exercise())
