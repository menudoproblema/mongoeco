"""Array semantics are checked against captured MongoDB 9 outcomes."""

import asyncio
import copy

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.core.aggregation.runtime import evaluate_expression
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.error_catalog import UNDEFINED_VARIABLE_ERROR
from mongoeco.errors import OperationFailure

from tests.differential.version_delta_cases import VERSION_DELTA_CASES
from tests.integration.api.test_mongodb9_aggregation_validation import DIALECT9


ARRAY_CASES = tuple(
    case
    for case in VERSION_DELTA_CASES
    if case.name.startswith(("map_", "filter_", "reduce_", "idx_"))
)
GOLDEN = loads(
    (
        Path(__file__).parents[2] / "fixtures" / "mongodb_version_deltas_9_0.json"
    ).read_text()
)["cases"]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("case", ARRAY_CASES, ids=lambda case: case.name)
def test_array_results_and_errors_match_real_golden(backend, surface, case):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    expected = GOLDEN[case.name]
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
            collection = client.test.records
            for document in copy.deepcopy(case.seed_documents):
                collection.insert_one(document)
            actual = case.action(collection)
    else:
        # The corpus uses PyMongo's synchronous collection helpers. The
        # same owned pipelines and results exercise async through its
        # public collection boundary, without invoking a sync wrapper.
        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=DIALECT9) as client:
                collection = client.test.records
                for document in copy.deepcopy(case.seed_documents):
                    await collection.insert_one(document)
                pipelines = []

                class CapturePipeline:
                    def aggregate(self, pipeline):
                        pipelines.append(copy.deepcopy(pipeline))
                        return []

                case.action(CapturePipeline())
                assert len(pipelines) == 1
                try:
                    result = await collection.aggregate(pipelines[0]).to_list()
                    return {"ok": True, "result": result}
                except OperationFailure as error:
                    return {
                        "ok": False,
                        "error_type": type(error).__name__,
                        "code": error.code,
                        "code_name": (error.details or {}).get(
                            "codeName", None
                        ),
                        "error_labels": list(error.error_labels),
                    }

        actual = asyncio.run(exercise())
    assert actual["ok"] == expected["ok"], case.name
    if expected["ok"]:
        assert actual["result"] == expected["result"], case.name
    else:
        for key in ("error_type", "code", "code_name", "error_labels"):
            assert actual[key] == expected[key], (case.name, key)


def test_bindings_do_not_escape_or_mutate_after_direct_evaluation():
    variables = {"outer": "kept"}
    expression = {"$map": {"input": [10, 20], "in": "$$IDX"}}
    assert evaluate_expression({}, expression, variables, dialect=DIALECT9) == [0, 1]
    assert variables == {"outer": "kept"}
    with pytest.raises(OperationFailure) as error:
        evaluate_expression({}, "$$IDX", variables, dialect=DIALECT9)
    assert error.value.code == UNDEFINED_VARIABLE_ERROR.code
