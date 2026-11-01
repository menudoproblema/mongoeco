"""Prepared projections preserve operation context and evaluation phase."""

import asyncio

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.core.aggregation import scalar_expressions
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("shape", ["stream", "facet"])
def test_project_evaluates_each_field_once_per_row(factory, shape):
    with MongoClient(factory()) as client:
        collection = client.test.rows
        collection.insert_many([{"_id": 1}, {"_id": 2}])
        project = {"$project": {"_id": 0, "value": {"$rand": {}}}}
        pipeline = [project] if shape == "stream" else [{"$facet": {"rows": [project]}}]
        with patch.object(
            scalar_expressions.random, "random", side_effect=[0.25, 0.75]
        ) as draw:
            rows = list(collection.aggregate(pipeline))
        assert draw.call_count == len(rows[0]["rows"] if shape == "facet" else rows)
        if shape == "facet":
            rows = rows[0]["rows"]
        assert rows == [{"value": 0.25}, {"value": 0.75}]


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("route", ["update", "replace", "delete"])
@pytest.mark.parametrize("constant", [False, True])
def test_find_modify_projection_error_preserves_native_commit_phase(
    factory, route, constant
):
    async def run():
        async with AsyncMongoClient(factory()) as client:
            collection = client.test.rows
            await collection.insert_one({"_id": 1, "v": 7, "zero": 0})
            expected_error_code = 2
            updated_value = 8
            stream = collection.watch(max_await_time_ms=1)
            try:
                projection = {"value": {"$divide": [1, 0 if constant else "$zero"]}}

                async def invoke():
                    if route == "update":
                        return await collection.find_one_and_update(
                            {"_id": 1}, {"$set": {"v": 8}}, projection=projection
                        )
                    if route == "replace":
                        return await collection.find_one_and_replace(
                            {"_id": 1},
                            {"_id": 1, "v": 8, "zero": 0},
                            projection=projection,
                        )
                    return await collection.find_one_and_delete(
                        {"_id": 1}, projection=projection
                    )

                with pytest.raises(OperationFailure) as caught:
                    await invoke()
                assert caught.value.code == expected_error_code
                await collection.insert_one({"_id": 9})
                event = await stream.try_next()
                if constant:
                    assert event["documentKey"] == {"_id": 9}
                    assert await collection.find_one({"_id": 1}) == {
                        "_id": 1,
                        "v": 7,
                        "zero": 0,
                    }
                else:
                    assert event["documentKey"] == {"_id": 1}
                    assert (await stream.try_next())["documentKey"] == {"_id": 9}
                    row = await collection.find_one({"_id": 1})
                    assert (
                        row is None if route == "delete" else row["v"] == updated_value
                    )
                assert await stream.try_next() is None
            finally:
                await stream.close()

    asyncio.run(run())


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
def test_read_metadata_survives_projection_without_leaking_to_public_bson(factory):
    with MongoClient(factory()) as client:
        collection = client.test.rows
        collection.insert_one({"_id": 1, "name": "Ada", "nested": {"v": [1]}})
        collection.create_index([("name", "text")])
        query = {"$text": {"$search": "Ada"}}
        pipeline = [
            {"$match": query},
            {"$project": {"name": 1}},
            {"$project": {"available": {"$gt": [{"$meta": "textScore"}, 0]}}},
        ]
        rows = list(collection.aggregate(pipeline))
        assert rows == [{"_id": 1, "available": True}]
        public = collection.find_one(query)
        assert type(public) is dict
        assert not hasattr(public, "runtime_metadata")
        public["nested"]["v"].append("changed")
        assert collection.find_one({"_id": 1}) == {
            "_id": 1,
            "name": "Ada",
            "nested": {"v": [1]},
        }
        assert type(rows[0]) is dict
        assert not hasattr(rows[0], "runtime_metadata")
