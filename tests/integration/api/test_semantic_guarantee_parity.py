"""4.9 semantic regressions: independent native oracles and all local facades."""

import asyncio
import copy
import json

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.differential.semantic_guarantee_cases import SEMANTIC_GUARANTEE_CASES
from tests.integration.api.test_mongodb9_conversions import comparable_value
from tests.integration.api.test_review_improvement_parity import failure_outcome


FIXTURES = {
    dialect: loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / f"mongodb_semantic_guarantees_{dialect.replace('.', '_')}.json"
        ).read_text()
    )
    for dialect in ("7.0", "8.0", "9.0")
}


def pipeline_for(case):
    pipelines = []

    class CapturePipeline:
        def aggregate(self, pipeline):
            pipelines.append(copy.deepcopy(pipeline))
            return []

    case.action(CapturePipeline())
    assert len(pipelines) == 1
    return pipelines[0]


def assert_multiset_outcome(actual, expected):
    assert actual["ok"] == expected["ok"]
    if expected["ok"]:
        # Equal sort keys do not establish a native order. Sorting representations
        # compares bags, retaining every occurrence and every document field.
        def bag(documents):
            return sorted(
                json.dumps(doc, sort_keys=True, default=str)
                for doc in comparable_value(documents)
            )

        assert bag(actual["result"]) == bag(expected["result"])
    else:
        for key in ("error_type", "code", "code_name", "error_labels"):
            assert actual[key] == expected[key]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("route", ["facade", "command"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("pymongo_profile", ["4.9", "4.18"])
@pytest.mark.parametrize("case", SEMANTIC_GUARANTEE_CASES, ids=lambda case: case.name)
def test_semantic_guarantee_matches_native(  # noqa: PLR0913, PLR0917 - explicit parity axes
    backend, surface, route, dialect, pymongo_profile, case
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    pipeline = pipeline_for(case)
    if surface == "sync":
        with MongoClient(
            engine, mongodb_dialect=dialect, pymongo_profile=pymongo_profile
        ) as client:
            coll = client.test.records
            for doc in copy.deepcopy(case.seed_documents):
                coll.insert_one(doc)
            try:
                result = (
                    coll.aggregate(pipeline).to_list()
                    if route == "facade"
                    else client.test.command(
                        {"aggregate": "records", "pipeline": pipeline, "cursor": {}}
                    )["cursor"]["firstBatch"]
                )
                actual = {"ok": True, "result": result}
            except OperationFailure as error:
                actual = failure_outcome(error)
    else:

        async def exercise():
            async with AsyncMongoClient(
                engine, mongodb_dialect=dialect, pymongo_profile=pymongo_profile
            ) as client:
                coll = client.test.records
                for doc in copy.deepcopy(case.seed_documents):
                    await coll.insert_one(doc)
                try:
                    result = (
                        await coll.aggregate(pipeline).to_list()
                        if route == "facade"
                        else (
                            await client.test.command(
                                {
                                    "aggregate": "records",
                                    "pipeline": pipeline,
                                    "cursor": {},
                                }
                            )
                        )["cursor"]["firstBatch"]
                    )
                    return {"ok": True, "result": result}
                except OperationFailure as error:
                    return failure_outcome(error)

        actual = asyncio.run(exercise())
    assert_multiset_outcome(actual, FIXTURES[dialect]["cases"][case.name])
