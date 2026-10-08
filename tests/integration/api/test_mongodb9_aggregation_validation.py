"""MongoDB 9 rejects invalid shapes before rows, compilation or writes."""

import asyncio
import datetime

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat import MONGODB_DIALECT_90
from mongoeco.core.aggregation.compiled_aggregation import CompiledGroup
from mongoeco.core.aggregation.grouping_stages import _apply_group, _IncrementalGroup
from mongoeco.core.aggregation.preparation import prepare_pipeline
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure


DIALECT9 = MONGODB_DIALECT_90
GROUP = {"$group": {"_id": None, "": {"$sum": 1}}}
EMPTY_GROUP_FIELD_CODE = 12116300
UNDEFINED_VARIABLE_CODE = 17276


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("profile", ["4.9", "4.18"])
def test_standalone_cluster_time_guard_survives_direct_find_expression(
    backend, surface, profile
):
    expected = loads(
        (
            Path(__file__).parents[2]
            / "fixtures/mongodb_version_deltas_9_0.json"
        ).read_text()
    )["cases"]["cluster_time_standalone"]
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    query = {"$expr": {"$eq": ["$$CLUSTER_TIME", 1]}}
    if surface == "sync":
        with MongoClient(
            engine, mongodb_dialect=DIALECT9, pymongo_profile=profile
        ) as client:
            collection = client.test.records
            collection.insert_one({"_id": "seed"})
            with pytest.raises(OperationFailure) as error:
                list(collection.find(query))
    else:

        async def exercise():
            async with AsyncMongoClient(
                engine, mongodb_dialect=DIALECT9, pymongo_profile=profile
            ) as client:
                collection = client.test.records
                await collection.insert_one({"_id": "seed"})
                with pytest.raises(OperationFailure) as error:
                    await collection.find(query).to_list()
                return error

        error = asyncio.run(exercise())
    assert error.value.code == expected["code"]
    assert (error.value.details or {}).get("codeName") == expected["code_name"]
    assert list(error.value.error_labels) == expected["error_labels"]


def invalid_pipeline_cases():
    yield [GROUP], 12116300
    yield [{"$facet": {"nested": [GROUP]}}], 12116300
    yield (
        [{"$lookup": {"from": "other", "as": "nested", "pipeline": [GROUP]}}],
        12116300,
    )
    yield [{"$unionWith": {"coll": "other", "pipeline": [GROUP]}}], 12116300
    for partition, code in (("a", 9554500), ("a.b", 8993000), ("a.b.c", 8993000)):
        yield (
            [
                {
                    "$densify": {
                        "field": "a.b",
                        "partitionByFields": [partition],
                        "range": {"step": 1, "bounds": [0, 3]},
                    }
                }
            ],
            code,
        )


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("has_input", [False, True])
def test_invalid_pipeline_is_rejected_before_cursor_creation(
    backend, surface, has_input
):
    def check(collection):
        for pipeline, code in invalid_pipeline_cases():
            with pytest.raises(OperationFailure) as error:
                collection.aggregate(pipeline)
            assert error.value.code == code
            assert error.value.code_name == f"Location{code}"
            assert error.value.error_labels == ()

    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
            collection = client.test.records
            if has_input:
                collection.insert_one({"_id": "seed", "a": {"b": 1}})
            check(collection)
            assert collection.count_documents({}) == int(has_input)
            assert client.test.other.count_documents({}) == 0
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=DIALECT9) as client:
                collection = client.test.records
                if has_input:
                    await collection.insert_one({"_id": "seed", "a": {"b": 1}})
                check(collection)
                assert await collection.count_documents({}) == int(has_input)
                assert await client.test.other.count_documents({}) == 0

        asyncio.run(exercise())


def test_group_validation_is_shared_with_direct_compiled_and_runtime_paths():
    for action in (
        lambda: CompiledGroup(GROUP["$group"], dialect=DIALECT9),
        lambda: _apply_group([], GROUP["$group"], dialect=DIALECT9),
        lambda: _IncrementalGroup(GROUP["$group"], dialect=DIALECT9),
        lambda: prepare_pipeline([GROUP], dialect=DIALECT9),
    ):
        with pytest.raises(OperationFailure) as error:
            action()
        assert error.value.code == EMPTY_GROUP_FIELD_CODE


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_lexical_variables_survive_nested_planning_without_becoming_globals(backend):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
        collection = client.test.records
        collection.insert_one({"_id": 1, "n": 2, "literal": "$$unbound"})
        pipeline = [
            {
                "$match": {
                    "$and": [
                        {"$expr": {"$eq": ["$n", "$$expected"]}},
                        {"literal": "$$unbound"},
                    ]
                }
            },
            {
                "$set": {
                    "n": {
                        "$let": {
                            "vars": {"next": {"$add": ["$$expected", 1]}},
                            "in": "$$next",
                        }
                    }
                }
            },
            {
                "$bucket": {
                    "groupBy": "$n",
                    "boundaries": [0, 5],
                    "output": {"sum": {"$sum": "$$expected"}},
                }
            },
        ]
        assert list(collection.aggregate(pipeline, let={"expected": 2})) == [
            {"_id": 0, "sum": 2}
        ]
        with pytest.raises(OperationFailure) as error:
            collection.aggregate([{"$set": {"v": "$$next"}}])
        assert error.value.code == UNDEFINED_VARIABLE_CODE


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_date_window_current_unbounded_and_invalid_empty_shapes(backend):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
        collection = client.test.records
        for window in (
            {"unit": "day"},
            {"unit": "day", "range": [0, 1], "documents": [0, 1]},
            {"unit": "day", "range": [0, 0.5]},
            {"unit": "day", "range": [False, 1]},
        ):
            with pytest.raises(OperationFailure):
                collection.aggregate(
                    [
                        {
                            "$setWindowFields": {
                                "sortBy": {"at": 1},
                                "output": {"n": {"$sum": 1, "window": window}},
                            }
                        }
                    ]
                )
        collection.insert_many(
            [
                {
                    "_id": index,
                    "at": datetime.datetime(2026, 1, index + 1, tzinfo=datetime.UTC),
                }
                for index in range(3)
            ]
        )
        results = list(
            collection.aggregate(
                [
                    {
                        "$setWindowFields": {
                            "sortBy": {"at": 1},
                            "output": {
                                "before": {
                                    "$sum": 1,
                                    "window": {
                                        "unit": "day",
                                        "range": ["unbounded", "current"],
                                    },
                                },
                                "after": {
                                    "$sum": 1,
                                    "window": {
                                        "unit": "day",
                                        "range": ["current", "unbounded"],
                                    },
                                },
                            },
                        }
                    }
                ]
            )
        )
        assert [(row["before"], row["after"]) for row in results] == [
            (1, 3),
            (2, 2),
            (3, 1),
        ]
        collection.insert_one({"_id": "invalid", "at": "2026-01-04"})
        with pytest.raises(OperationFailure, match="date sort values"):
            list(
                collection.aggregate(
                    [
                        {
                            "$setWindowFields": {
                                "sortBy": {"at": 1},
                                "output": {
                                    "n": {
                                        "$sum": 1,
                                        "window": {"unit": "day", "range": [0, 1]},
                                    }
                                },
                            }
                        }
                    ]
                )
            )


@pytest.mark.parametrize("dialect", ["7.0", "8.0"])
def test_previous_dialects_keep_empty_group_field_contract(dialect):
    with MongoClient(MemoryEngine(), mongodb_dialect=dialect) as client:
        collection = client.test.records
        assert list(collection.aggregate([GROUP])) == []
        collection.insert_one({"_id": "seed"})
        assert list(collection.aggregate([GROUP])) == [{"_id": None, "": 1}]
