"""Exercise existing real captures for every supported dialect and facade."""

import asyncio
import copy

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.differential.version_delta_cases import VERSION_DELTA_CASES


CASES = tuple(
    case for case in VERSION_DELTA_CASES if case.name.startswith("densify_bounds_")
)
FIXTURES = {
    dialect: loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / f"mongodb_version_deltas_{dialect.replace('.', '_')}.json"
        ).read_text()
    )["cases"]
    for dialect in ("7.0", "8.0", "9.0")
}


def pipeline_for(case, route):
    pipelines = []

    class CapturePipeline:
        def aggregate(self, pipeline):
            pipelines.append(copy.deepcopy(pipeline))
            return []

    case.action(CapturePipeline())
    assert len(pipelines) == 1
    pipeline = pipelines[0]
    # Densify does not promise ordering. Sort explicitly before comparison.
    pipeline.append({"$sort": {"n": 1}})
    if route == "facet":
        return [{"$facet": {"values": pipeline}}]
    if route in {"out", "merge"}:
        # Supply deterministic unique identifiers for the writeback subset.
        target = {"into": "output"} if route == "merge" else "output"
        pipeline.extend([{"$set": {"_id": "$n"}}, {f"${route}": target}])
    return pipeline


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("route", ["direct", "facet", "merge"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_densify_matches_real_capture(backend, surface, dialect, route, case):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    pipeline = pipeline_for(case, route)
    expected = FIXTURES[dialect][case.name]
    assert expected["ok"]
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            collection = client.test.records
            for document in copy.deepcopy(case.seed_documents):
                collection.insert_one(document)
            result = list(collection.aggregate(pipeline))
            if route in {"out", "merge"}:
                result = list(client.test.output.find({}, {"_id": 0}).sort("n", 1))
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                collection = client.test.records
                for document in copy.deepcopy(case.seed_documents):
                    await collection.insert_one(document)
                result = await collection.aggregate(pipeline).to_list()
                if route in {"out", "merge"}:
                    result = (
                        await client.test.output.find({}, {"_id": 0})
                        .sort("n", 1)
                        .to_list()
                    )
                return result

        result = asyncio.run(exercise())
    if route == "facet":
        assert len(result) == 1
        result = result[0]["values"]
    assert result == sorted(expected["result"], key=lambda document: document["n"])


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_out_remains_an_explicit_rejection_without_writes(backend, surface, dialect):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    pipeline = pipeline_for(CASES[0], "out")
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            with pytest.raises(
                OperationFailure, match=r"Unsupported aggregation stage: \$out"
            ):
                list(client.test.records.aggregate(pipeline))
            assert "output" not in client.test.list_collection_names()
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                with pytest.raises(
                    OperationFailure, match=r"Unsupported aggregation stage: \$out"
                ):
                    await client.test.records.aggregate(pipeline).to_list()
                assert "output" not in await client.test.list_collection_names()

        asyncio.run(exercise())
