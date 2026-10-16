"""Public batch failures preserve partial results, publication and engine health."""

import asyncio

from dataclasses import replace

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import BulkWriteError, DuplicateKeyError

from tests.support import ENGINE_FACTORIES


THREE_DOCUMENTS = 3


class SingleWriteMemoryEngine(MemoryEngine):
    capabilities = replace(MemoryEngine.capabilities, batch_inserts=False)


BATCH_FACTORIES = [*ENGINE_FACTORIES.values(), SingleWriteMemoryEngine]


@pytest.mark.parametrize("factory", BATCH_FACTORIES)
@pytest.mark.parametrize("ordered", [True, False])
@pytest.mark.parametrize("position", [0, 1, 2])
def test_invalid_id_retains_global_error_index_and_publishes_each_success_once(
    factory, ordered, position
):
    async def exercise():
        documents = [{"_id": "a"}, {"_id": "b"}, {"_id": "c"}]
        documents[position]["_id"] = ["invalid"]
        expected = [
            document["_id"]
            for index, document in enumerate(documents)
            if index != position and (not ordered or index < position)
        ]
        async with AsyncMongoClient(factory()) as client:
            collection = client.identity.items
            stream = collection.watch(max_await_time_ms=5)
            try:
                with pytest.raises(BulkWriteError) as failure:
                    await collection.insert_many(documents, ordered=ordered)
                assert failure.value.code == 65  # noqa: PLR2004 - native bulk error code
                assert failure.value.details["nInserted"] == len(expected)
                assert [
                    [entry["index"], entry["code"]]
                    for entry in failure.value.details["writeErrors"]
                ] == [[position, 53]]
                assert [
                    (await stream.try_next())["documentKey"]["_id"] for _ in expected
                ] == expected
                assert await stream.try_next() is None
                assert [
                    document["_id"]
                    for document in await collection.find({}).sort("_id", 1).to_list()
                ] == expected
                await collection.insert_one({"_id": "recovery"})
                assert (await stream.try_next())["documentKey"]["_id"] == "recovery"
                assert await stream.try_next() is None
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("factory", BATCH_FACTORIES)
def test_unordered_schema_errors_publish_each_success_once_with_global_indices(factory):
    async def exercise():
        async with AsyncMongoClient(factory()) as client:
            collection = await client.batch.create_collection(
                "items", validator={"$jsonSchema": {"required": ["name"]}}
            )
            stream = collection.watch(max_await_time_ms=5)
            try:
                with pytest.raises(BulkWriteError) as failure:
                    await collection.insert_many(
                        [
                            {"_id": "a", "name": "Ada"},
                            {"_id": "bad1"},
                            {"_id": "b", "name": "Bea"},
                            {"_id": "bad2"},
                            {"_id": "a", "name": "duplicate"},
                            {"_id": "c", "name": "Cid"},
                        ],
                        ordered=False,
                    )
                assert failure.value.details["nInserted"] == THREE_DOCUMENTS
                assert [
                    [e["index"], e["code"]]
                    for e in failure.value.details["writeErrors"]
                ] == [[1, 121], [3, 121], [4, 11000]]
                assert [
                    (await stream.try_next())["documentKey"]["_id"] for _ in range(3)
                ] == ["a", "b", "c"]
                assert await stream.try_next() is None
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("factory", BATCH_FACTORIES)
def test_many_unordered_duplicate_errors_complete_without_recursive_continuation(
    factory,
):
    size = 1050

    async def exercise():
        async with AsyncMongoClient(factory()) as client:
            collection = client.batch.items
            await collection.insert_one({"_id": "dup"})
            with pytest.raises(BulkWriteError) as failure:
                await collection.insert_many(
                    [{"_id": "dup"} for _ in range(size)], ordered=False
                )
            assert failure.value.details["nInserted"] == 0
            assert [e["index"] for e in failure.value.details["writeErrors"]] == list(
                range(size)
            )
            assert await collection.count_documents({}) == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("factory", BATCH_FACTORIES)
def test_failed_unique_update_retains_index_and_allows_subsequent_writes(factory):
    async def exercise():
        async with AsyncMongoClient(factory()) as client:
            collection = client.recovery.items
            await collection.create_index([("email", 1)], unique=True)
            await collection.insert_many(
                [{"_id": "a", "email": "a"}, {"_id": "b", "email": "b"}]
            )
            with pytest.raises(DuplicateKeyError):
                await collection.update_one({"_id": "b"}, {"$set": {"email": "a"}})
            result = await collection.update_one({"_id": "b"}, {"$set": {"email": "c"}})
            assert result.modified_count == 1
            with pytest.raises(DuplicateKeyError):
                await collection.insert_one({"_id": "d", "email": "a"})
            await collection.insert_one({"_id": "d", "email": "b"})
            assert await collection.count_documents({}) == THREE_DOCUMENTS

    asyncio.run(exercise())
