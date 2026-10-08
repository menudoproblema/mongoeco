"""Compare shared conversion semantics with the independent real oracle."""

import asyncio
import copy

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.core.bson_scalars import unwrap_bson_numeric
from mongoeco.core.codec import DocumentCodec
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import Decimal128

from tests.differential.version_delta_cases import VERSION_DELTA_CASES
from tests.integration.api.test_mongodb9_aggregation_validation import DIALECT9


DEFERRED = {
    "convert_json_array",
    "convert_json_object",
    "convert_json_extended",
    "convert_binary_subtype",
}


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_find_expr_retains_conversion_spec_guard_without_pipeline_preparation(
    backend, surface
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    filter_spec = {
        "$expr": {
            "$eq": [
                {
                    "$convert": {
                        "input": "10",
                        "to": "int",
                        "format": "hex",
                        "onError": 10,
                    }
                },
                10,
            ]
        }
    }
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
            client.test.records.insert_one({"_id": 1})
            with pytest.raises(OperationFailure, match="supported subset"):
                list(client.test.records.find(filter_spec))
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=DIALECT9) as client:
                await client.test.records.insert_one({"_id": 1})
                with pytest.raises(OperationFailure, match="supported subset"):
                    await client.test.records.find(filter_spec).to_list()

        asyncio.run(exercise())


CASES = tuple(
    case
    for case in VERSION_DELTA_CASES
    if case.name.startswith(
        (
            "convert_",
            "to_string",
            "dates_",
            "densify_dates",
            "window_dates",
            "densify_bounds",
            "trim_chars",
            "cluster_time",
        )
    )
    and case.name not in DEFERRED
)
GOLDEN = loads(
    (
        Path(__file__).parents[2] / "fixtures" / "mongodb_version_deltas_9_0.json"
    ).read_text()
)["cases"]


def comparable_value(value):
    value = unwrap_bson_numeric(value)
    if isinstance(value, Decimal128):
        return value.to_decimal()
    if isinstance(value, list):
        return [comparable_value(item) for item in value]
    if isinstance(value, dict):
        return {key: comparable_value(item) for key, item in value.items()}
    return value


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_conversion_matches_real_golden(backend, surface, case):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
            collection = client.test.records
            for document in copy.deepcopy(case.seed_documents):
                collection.insert_one(document)
            actual = case.action(collection)
    else:

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
                    return {
                        "ok": True,
                        "result": await collection.aggregate(pipelines[0]).to_list(),
                    }
                except OperationFailure as error:
                    return {
                        "ok": False,
                        "error_type": type(error).__name__,
                        "code": error.code,
                        "code_name": (error.details or {}).get("codeName"),
                        "error_labels": list(error.error_labels),
                    }

        actual = asyncio.run(exercise())
    expected = GOLDEN[case.name]
    assert actual["ok"] == expected["ok"]
    if expected["ok"]:
        assert comparable_value(
            DocumentCodec.to_internal(actual["result"])
        ) == comparable_value(DocumentCodec.to_internal(expected["result"]))
    else:
        for key in ("error_type", "code", "code_name", "error_labels"):
            assert actual[key] == expected[key]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize(
    "extension",
    [
        {"to": "array"},
        {"to": "object"},
        {"to": "binData"},
        {"to": {"type": "binData", "subtype": 4}},
        {"format": "hex"},
        {"byteOrder": "big"},
        {"unknown": True},
    ],
)
@pytest.mark.parametrize("nested", [False, True])
def test_deferred_extensions_fail_before_empty_pipeline_or_write(
    backend, surface, extension, nested
):
    spec = {"input": "10", "to": "int", "onError": "hidden-error", **extension}
    pipeline = [{"$project": {"v": {"$convert": spec}}}]
    if nested:
        pipeline = [{"$facet": {"nested": pipeline}}]
    pipeline.append({"$out": "output"})
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
            with pytest.raises(OperationFailure, match="supported subset"):
                client.test.records.aggregate(pipeline)
            assert "output" not in client.test.list_collection_names()
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=DIALECT9) as client:
                with pytest.raises(OperationFailure, match="supported subset"):
                    client.test.records.aggregate(pipeline)
                assert "output" not in await client.test.list_collection_names()

        asyncio.run(exercise())


@pytest.mark.parametrize("dialect", ["7.0", "8.0"])
def test_older_conversion_contract_is_not_changed(dialect):
    with MongoClient(MemoryEngine(), mongodb_dialect=dialect) as client:
        collection = client.test.records
        collection.insert_one({"_id": 1})
        assert list(
            collection.aggregate(
                [
                    {
                        "$project": {
                            "v": {"$convert": {"input": "10", "to": "int", "base": 2}}
                        }
                    }
                ]
            )
        ) == [{"_id": 1, "v": 10}]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_conversion_validation_and_error_fallback_remain_separate(backend):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
        collection = client.test.records
        for invalid in ([], {"input": 1}, {"to": "int"}):
            with pytest.raises(OperationFailure, match="requires input and to"):
                collection.aggregate([{"$project": {"v": {"$convert": invalid}}}])
        collection.insert_one({"_id": 1, "target": "object"})
        with pytest.raises(OperationFailure, match="supported subset"):
            list(
                collection.aggregate(
                    [
                        {
                            "$project": {
                                "v": {
                                    "$convert": {
                                        "input": "{}",
                                        "to": "$target",
                                        "onError": "hidden",
                                    }
                                }
                            }
                        }
                    ]
                )
            )
        assert list(
            collection.aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "v": {
                                "$convert": {
                                    "input": "not-an-integer",
                                    "to": {"type": "int"},
                                    "onError": "fallback",
                                }
                            },
                        }
                    }
                ]
            )
        ) == [{"v": "fallback"}]
        assert list(
            collection.aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "v": {
                                "$convert": {
                                    "input": None,
                                    "to": "int",
                                    "base": 3,
                                    "onNull": "absent",
                                }
                            },
                        }
                    }
                ]
            )
        ) == [{"v": "absent"}]
