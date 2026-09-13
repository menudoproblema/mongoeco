"""Memory snapshots own versions, not an eagerly copied public result set."""

import asyncio

from datetime import UTC, datetime, timedelta
from typing import ClassVar
from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.core.codec import DocumentCodec
from mongoeco.engines.memory import MemoryEngine


@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("batch_size", [2, None])
def test_first_batch_copies_only_requested_rows_and_projected_fields(
    ordered, batch_size
):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": value, "discarded": [value] * 100} for value in range(300)]
            )
            roots = []
            copy = engine._copy_document_containers

            def observe(value):
                if isinstance(value, dict) and "_id" in value:
                    roots.append(set(value))
                return copy(value)

            with patch.object(engine, "_copy_document_containers", side_effect=observe):
                cursor = collection.find(
                    {},
                    {"_id": 1},
                    batch_size=batch_size,
                    sort=[("_id", 1)] if ordered else None,
                )
                iterator = aiter(cursor)
                assert await anext(iterator) == {"_id": 0}
                expected_count = batch_size or 1
                assert len(roots) == expected_count
                assert all(fields == {"_id"} for fields in roots)
                await cursor.close()

    asyncio.run(exercise())


def test_custom_storage_decoder_reused_buffer_does_not_mutate_cached_versions():
    class BufferCodec:
        buffer: ClassVar[dict] = {}
        encode = staticmethod(DocumentCodec.encode)

        @classmethod
        def decode(cls, payload):
            cls.buffer.clear()
            cls.buffer.update(DocumentCodec.decode(payload))
            return cls.buffer

    async def exercise():
        async with AsyncMongoClient(MemoryEngine(codec=BufferCodec)) as client:
            collection = client.test.records
            expected = [{"_id": i, "nested": {"value": i}} for i in range(5)]
            await collection.insert_many(expected)
            # Warming another decoded row must not change a previously cached row.
            assert await collection.find_one({"_id": 0}) == expected[0]
            assert await collection.find_one({"_id": 1}) == expected[1]
            assert await collection.find_one({"_id": 0}) == expected[0]
            cursor = collection.find({}, batch_size=1)
            iterator = aiter(cursor)
            assert await anext(iterator) == expected[0]
            BufferCodec.buffer.clear()
            await collection.update_one({"_id": 2}, {"$set": {"nested.value": 99}})
            assert [doc async for doc in iterator] == expected[1:]

    asyncio.run(exercise())


def test_custom_storage_decoder_is_invoked_only_for_requested_batch():
    class CountingCodec:
        decode_calls = 0
        encode = staticmethod(DocumentCodec.encode)

        @classmethod
        def decode(cls, payload):
            cls.decode_calls += 1
            return DocumentCodec.decode(payload)

    async def exercise():
        requested_batch = 2
        async with AsyncMongoClient(MemoryEngine(codec=CountingCodec)) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": value, "discarded": [value] * 100} for value in range(300)]
            )
            CountingCodec.decode_calls = 0
            cursor = collection.find({}, {"_id": 1}, batch_size=requested_batch)
            assert await anext(aiter(cursor)) == {"_id": 0}
            assert CountingCodec.decode_calls == requested_batch
            await cursor.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("finish", ["close", "exhaust"])
def test_snapshot_releases_lazy_source_frame_on_terminal_state(finish):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            collection = client.test.records
            await collection.insert_many([{"_id": i} for i in range(5)])
            cursor = collection.find({})
            iterator = aiter(cursor)
            assert await anext(iterator) == {"_id": 0}

            snapshot = iterator._source
            source = snapshot._source
            assert source.ag_frame is not None
            # The suspended scan retains only its result pipeline, not mutable
            # collection/index roots that are unrelated to the remaining rows.
            assert {
                "coll",
                "indexes",
                "index_data",
                "candidate_items",
            }.isdisjoint(source.ag_frame.f_locals)

            if finish == "close":
                await cursor.close()
            else:
                assert [doc async for doc in iterator] == [
                    {"_id": i} for i in range(1, 5)
                ]
            assert source.ag_frame is None
            assert snapshot._close_task is None

    asyncio.run(exercise())


def test_snapshot_diagnostics_account_for_retained_versions_until_close():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            document_count = 3
            await collection.insert_many(
                [{"_id": i, "value": i} for i in range(document_count)]
            )
            cursor = collection.find({}, batch_size=1)
            iterator = aiter(cursor)
            assert await anext(iterator) == {"_id": 0, "value": 0}

            retained = engine._runtime_diagnostics_info()["mvcc"]
            assert retained["activeReadSnapshots"] == 1
            assert retained["retainedReferences"] == document_count
            assert retained["retainedDocumentVersions"] == document_count
            assert retained["supersededDocumentVersions"] == 0
            assert retained["retainedBytes"] > 0

            await collection.update_one({"_id": 1}, {"$set": {"value": 10}})
            retained = engine._runtime_diagnostics_info()["mvcc"]
            assert retained["supersededDocumentVersions"] == 1

            await cursor.close()
            released = engine._runtime_diagnostics_info()["mvcc"]
            assert released["activeReadSnapshots"] == 0
            assert released["retainedReferences"] == 0
            assert released["retainedDocumentVersions"] == 0
            assert released["supersededDocumentVersions"] == 0
            assert released["retainedBytes"] == 0

    asyncio.run(exercise())


def test_cancelled_snapshot_releases_lazy_source_frame():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 1})
            lock = engine._get_lock("test", "records")
            async with lock:
                cursor = collection.find({})
                iterator = aiter(cursor)
                snapshot = iterator._source
                source = snapshot._source
                task = asyncio.create_task(anext(iterator))
                await asyncio.sleep(0)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert source.ag_frame is None
            assert snapshot.closed

    asyncio.run(exercise())


def test_snapshot_retains_pre_expiration_view_and_bound_clock():
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        now = [initial]
        async with AsyncMongoClient(
            MemoryEngine(), now_factory=lambda: now[0]
        ) as client:
            collection = client.test.records
            await collection.create_index("expires", expire_after_seconds=0)
            await collection.insert_many(
                [{"_id": i, "expires": initial + timedelta(days=1)} for i in range(5)]
            )
            cursor = collection.find({}, {"_id": 1}, batch_size=1)
            iterator = aiter(cursor)
            assert await anext(iterator) == {"_id": 0}
            now[0] += timedelta(days=2)
            assert await collection.find({}).to_list() == []
            assert [doc async for doc in iterator] == [{"_id": i} for i in range(1, 5)]

    asyncio.run(exercise())


@pytest.mark.parametrize("finish", ["commit", "abort"])
def test_snapshot_retains_transactional_view_after_session_finishes(finish):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            collection = client.test.records
            await collection.insert_many([{"_id": i, "value": i} for i in range(4)])
            session = client.start_session()
            try:
                session.start_transaction()
                await collection.update_one(
                    {"_id": 1}, {"$set": {"value": 10}}, session=session
                )
                cursor = collection.find({}, session=session, batch_size=1)
                iterator = aiter(cursor)
                assert await anext(iterator) == {"_id": 0, "value": 0}
                if finish == "commit":
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                await collection.update_one({"_id": 2}, {"$set": {"value": 20}})
                assert [doc async for doc in iterator] == [
                    {"_id": 1, "value": 10},
                    {"_id": 2, "value": 2},
                    {"_id": 3, "value": 3},
                ]
            finally:
                session.close()

    asyncio.run(exercise())


def test_classic_text_snapshot_keeps_original_index_weights():
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            collection = client.test.records
            await collection.create_index(
                [("body", "text")], name="words", weights={"body": 1}
            )
            await collection.insert_many(
                [{"_id": i, "body": "alpha"} for i in range(5)]
            )
            selector = {"$text": {"$search": "alpha"}}
            projection = {"_id": 1, "score": {"$meta": "textScore"}}
            expected = await collection.find(selector, projection).to_list()
            cursor = collection.find(selector, projection, batch_size=1)
            iterator = aiter(cursor)
            assert await anext(iterator) == expected[0]
            await collection.drop_index("words")
            await collection.create_index(
                [("body", "text")], name="words", weights={"body": 10}
            )
            assert [doc async for doc in iterator] == expected[1:]

    asyncio.run(exercise())


@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("change", ["crud", "drop", "rename"])
def test_paused_snapshot_preserves_rows_across_writes_and_catalog_changes(
    ordered, change
):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            expected = [{"_id": i, "nested": {"value": i}} for i in range(5)]
            await collection.insert_many(expected)
            cursor = collection.find(
                {}, batch_size=1, sort=[("_id", 1)] if ordered else None
            )
            iterator = aiter(cursor)
            first = await anext(iterator)
            first["nested"]["value"] = "user mutation"

            async def write():
                if change == "crud":
                    await collection.update_one(
                        {"_id": 1}, {"$set": {"nested.value": 99}}
                    )
                    await collection.delete_one({"_id": 2})
                    await collection.insert_one({"_id": 5, "nested": {"value": 5}})
                elif change == "drop":
                    await collection.drop()
                    await collection.insert_one({"_id": "new-generation"})
                else:
                    await collection.rename("renamed")

            # A paused consumer must not own the collection lock.
            await asyncio.wait_for(write(), timeout=2)
            assert [doc async for doc in iterator] == expected[1:]
            await cursor.close()

    asyncio.run(exercise())
