"""BSON field order is observable across projection and merge writeback."""

import asyncio
import copy

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.core.identity import canonical_document_id
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("route", ["find", "aggregate", "merge", "merge_matched"])
def test_projection_merge_order_matches_real_golden(backend, surface, dialect, route):
    fixture = loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / f"mongodb_version_deltas_{dialect.replace('.', '_')}.json"
        ).read_text()
    )
    name = f"projection_field_order_{route}"
    manifest = next(item for item in fixture["case_manifests"] if item["name"] == name)
    projection = {"_id": 1, "a": 1, "m.a": 1, "m.z": 1, "z": 1}
    pipeline = [{"$project": projection}]
    if route.startswith("merge"):
        pipeline.append({"$merge": {"into": "archive"}})
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            collection = client.test.records
            collection.insert_many(copy.deepcopy(manifest["seed_documents"]))
            if route == "merge_matched":
                client.test.archive.insert_one({"_id": "seed", "prior": 0})
            if route == "find":
                actual = list(collection.find({}, projection))
            else:
                actual = list(collection.aggregate(pipeline))
                if route.startswith("merge"):
                    actual = list(client.test.archive.find({}))
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                collection = client.test.records
                await collection.insert_many(copy.deepcopy(manifest["seed_documents"]))
                if route == "merge_matched":
                    await client.test.archive.insert_one({"_id": "seed", "prior": 0})
                if route == "find":
                    return await collection.find({}, projection).to_list()
                result = await collection.aggregate(pipeline).to_list()
                if route.startswith("merge"):
                    result = await client.test.archive.find({}).to_list()
                return result

        actual = asyncio.run(exercise())
    assert canonical_document_id(actual) == canonical_document_id(
        fixture["cases"][name]["result"]
    )
