"""Critical-review regressions with independent, versioned native oracles."""

import asyncio
import copy

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90
from mongoeco.core.aggregation.runtime import evaluate_expression
from mongoeco.core.codec import DocumentCodec
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.differential.review_improvement_cases import REVIEW_IMPROVEMENT_CASES
from tests.integration.api.test_mongodb9_conversions import comparable_value


FIXTURES = {
    dialect: loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / f"mongodb_review_improvements_{dialect.replace('.', '_')}.json"
        ).read_text()
    )["cases"]
    for dialect in ("7.0", "8.0", "9.0")
}
CASES = tuple(
    (dialect, case)
    for case in REVIEW_IMPROVEMENT_CASES
    for dialect in ("7.0", "8.0", "9.0")
    if (case.name.startswith("densify_") and case.name != "densify_partition_local")
    or (
        dialect == "9.0"
        and case.name.startswith(("window_", "variable_"))
        and case.name != "variable_const_empty"
    )
)


def assert_native_outcome(actual, expected):
    assert actual["ok"] == expected["ok"]
    if expected["ok"]:
        assert comparable_value(
            DocumentCodec.to_internal(actual["result"])
        ) == comparable_value(DocumentCodec.to_internal(expected["result"]))
    else:
        for key in ("error_type", "code", "code_name", "error_labels"):
            assert actual[key] == expected[key]


def failure_outcome(error):
    return {
        "ok": False,
        "error_type": type(error).__name__,
        "code": error.code,
        "code_name": (error.details or {}).get("codeName"),
        "error_labels": list(error.error_labels),
    }


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize(
    ["dialect", "case"],
    CASES,
    ids=[f"{dialect}-{case.name}" for dialect, case in CASES],
)
def test_review_case_matches_native(backend, surface, dialect, case):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            collection = client.test.records
            for document in copy.deepcopy(case.seed_documents):
                collection.insert_one(document)
            actual = case.action(collection)
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                collection = client.test.records
                for document in copy.deepcopy(case.seed_documents):
                    await collection.insert_one(document)
                pipelines = []

                class CapturePipeline:
                    def aggregate(self, pipeline):
                        pipelines.append(copy.deepcopy(pipeline))
                        return []

                if case.name == "variable_empty_find_expr":
                    try:
                        return {
                            "ok": True,
                            "result": await collection.find(
                                {"$expr": {"$eq": ["$$", 1]}}
                            ).to_list(),
                        }
                    except OperationFailure as error:
                        return failure_outcome(error)
                case.action(CapturePipeline())
                assert len(pipelines) == 1
                try:
                    return {
                        "ok": True,
                        "result": await collection.aggregate(pipelines[0]).to_list(),
                    }
                except OperationFailure as error:
                    return failure_outcome(error)

        actual = asyncio.run(exercise())
    assert_native_outcome(actual, FIXTURES[dialect][case.name])


@pytest.mark.parametrize(
    "dialect", [MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90]
)
@pytest.mark.parametrize(
    ["expression", "name"],
    [
        ("$$", "empty"),
        ("$$.value", "empty_dotted"),
        ("$$.", "empty_dot"),
        ("$$unknown", "unknown"),
    ],
)
def test_direct_variable_evaluation_preserves_native_error_classification(
    dialect, expression, name
):
    with pytest.raises(OperationFailure) as caught:
        evaluate_expression({}, expression, dialect=dialect)
    assert_native_outcome(
        failure_outcome(caught.value),
        FIXTURES[dialect.server_version][f"variable_{name}"],
    )


@pytest.mark.parametrize(
    "dialect", [MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90]
)
@pytest.mark.parametrize("operator", ["$literal"])
def test_direct_literal_empty_variable_is_preserved(dialect, operator):
    assert evaluate_expression({}, {operator: "$$"}, dialect=dialect) == "$$"
