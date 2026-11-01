"""Rejected bulk frames preserve earlier groups, indexes, persistence and events."""

import asyncio

from decimal import Decimal

import pytest

from bson.errors import InvalidDocument
from pymongo import InsertOne, ReplaceOne, UpdateOne

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import DuplicateKeyError


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("ordered", [True, False])
@pytest.mark.parametrize("prefix", [True, False])
def test_rejected_family_publishes_only_prior_groups_and_barrier(
    backend, ordered, prefix, tmp_path
):
    path = str(tmp_path / "bulk.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    expected = [{"_id": 0, "v": 4}, {"_id": 1, "v": 2}]
    if prefix:
        expected.append({"_id": 2, "v": 3})

    async def exercise():
        async with AsyncMongoClient(engine()) as client:
            c = client.test.rows
            await c.insert_many([{"_id": 0, "v": 1}, {"_id": 1, "v": 2}])
            await c.create_index("v", unique=True)
            stream = c.watch(max_await_time_ms=1)
            try:
                models = [
                    UpdateOne({"_id": 0}, {"$set": {"v": 99}}),
                    UpdateOne({"_id": 1}, {"$set": {"v": Decimal(1)}}),
                ]
                if prefix:
                    models.insert(0, InsertOne({"_id": 2, "v": 3}))
                with pytest.raises(InvalidDocument):
                    await c.bulk_write(models, ordered=ordered)
                await c.update_one({"_id": 0}, {"$set": {"v": 4}})
                events = [await stream.try_next() for _ in range(2 if prefix else 1)]
                assert [e["operationType"] for e in events] == (
                    ["insert", "update"] if prefix else ["update"]
                )
                assert [e["documentKey"] for e in events] == (
                    [{"_id": 2}, {"_id": 0}] if prefix else [{"_id": 0}]
                )
                assert await stream.try_next() is None
                assert await c.find().sort("_id").to_list() == expected
                with pytest.raises(DuplicateKeyError):
                    await c.insert_one({"_id": 9, "v": 4})
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert list(client.test.rows.find().sort("_id")) == expected
            with pytest.raises(DuplicateKeyError):
                client.test.rows.insert_one({"_id": 9, "v": 4})


@pytest.mark.parametrize("model", [InsertOne, UpdateOne, ReplaceOne])
def test_model_adapters_do_not_copy_unsupported_objects_before_bson_admission(model):
    class Unsupported:
        def __deepcopy__(self, memo):
            message = "copy preceded BSON admission"
            raise AssertionError(message)

    with MongoClient(MemoryEngine()) as client:
        if model is InsertOne:
            request = model({"_id": 2, "v": Unsupported()})
        elif model is ReplaceOne:
            request = model({}, {"v": Unsupported()})
        else:
            request = model({}, {"$set": {"v": Unsupported()}})
        with pytest.raises(InvalidDocument):
            client.test.rows.bulk_write([request])
