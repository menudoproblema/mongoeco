"""Reuse requires equivalent namespaces, bindings, scopes and registry state."""

import asyncio

from dataclasses import replace
from types import MappingProxyType
from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api.operations import compile_aggregate_operation
from mongoeco.compat import MONGODB_DIALECT_90
from mongoeco.core.aggregation import preparation
from mongoeco.core.aggregation.extensions import (
    register_aggregation_stage,
    unregister_aggregation_stage,
)
from mongoeco.core.aggregation.preparation import prepare_pipeline
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure


PIPELINE = [
    {"$match": {"$expr": {"$eq": ["$n", "$$wanted"]}}},
    {"$project": {"n": 1}},
]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("route", ["collection", "command"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_equivalent_cursor_context_prepares_each_stage_once(
    backend, surface, route, dialect
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            client.test.records.insert_one({"_id": 1, "n": 2})
            with patch.object(
                preparation,
                "_prepare_builtin_stage",
                wraps=preparation._prepare_builtin_stage,
            ) as validate:
                if route == "collection":
                    cursor = client.test.records.aggregate(PIPELINE, let={"wanted": 2})
                    result = list(cursor)
                    prepared = cursor._async_aggregation_cursor._pipeline
                    assert prepared.context.collection == "records"
                    assert prepared.context.variables == frozenset({"wanted"})
                else:
                    result = client.test.command(
                        {
                            "aggregate": "records",
                            "pipeline": PIPELINE,
                            "let": {"wanted": 2},
                            "cursor": {},
                        }
                    )["cursor"]["firstBatch"]
                assert validate.call_count == len(PIPELINE)
            assert result == [{"_id": 1, "n": 2}]
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                await client.test.records.insert_one({"_id": 1, "n": 2})
                with patch.object(
                    preparation,
                    "_prepare_builtin_stage",
                    wraps=preparation._prepare_builtin_stage,
                ) as validate:
                    if route == "collection":
                        cursor = client.test.records.aggregate(
                            PIPELINE, let={"wanted": 2}
                        )
                        result = await cursor.to_list()
                        assert cursor._pipeline.context.collection == "records"
                        assert cursor._pipeline.context.variables == frozenset(
                            {"wanted"}
                        )
                    else:
                        result = (
                            await client.test.command(
                                {
                                    "aggregate": "records",
                                    "pipeline": PIPELINE,
                                    "let": {"wanted": 2},
                                    "cursor": {},
                                }
                            )
                        )["cursor"]["firstBatch"]
                    assert validate.call_count == len(PIPELINE)
                assert result == [{"_id": 1, "n": 2}]

        asyncio.run(exercise())


def dialect_with_flags(**changes):
    return replace(
        MONGODB_DIALECT_90,
        catalog_behavior_flags=MappingProxyType(
            {**MONGODB_DIALECT_90.catalog_behavior_flags, **changes}
        ),
    )


@pytest.mark.parametrize(
    "stage",
    [
        {"$project": {"v": {"$convert": {"input": 1, "to": "int", "format": "hex"}}}},
        {"$project": {"v": {"$trim": {"input": "x", "chars": "x" * 100001}}}},
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "n": {"$sum": 1, "window": {"unit": "day", "range": [0, 0.5]}}
                },
            }
        },
        {"$project": {"v": "$$"}},
    ],
)
def test_general_validation_is_independent_of_array_index_capability(stage):
    dialect = dialect_with_flags(supports_array_index_variables=False)
    with pytest.raises(OperationFailure):
        prepare_pipeline([stage], dialect=dialect)


def test_documents_namespace_rule_is_independent_of_early_validation():
    dialect = dialect_with_flags(validates_aggregation_syntax_early=False)
    pipeline = [
        {
            "$lookup": {
                "from": "foreign",
                "pipeline": [{"$documents": []}],
                "as": "values",
            }
        }
    ]
    with pytest.raises(OperationFailure, match="omit its collection namespace"):
        prepare_pipeline(pipeline, dialect=dialect, collection="records")
    prepared = prepare_pipeline(
        pipeline,
        dialect=dialect_with_flags(documents_join_omits_collection_namespace=False),
        collection="records",
    )
    assert prepared[0]["$lookup"]["pipeline"].context.collection == "foreign"


def test_reuse_invalidates_when_bindings_or_registry_change():
    prepared = prepare_pipeline(
        PIPELINE, dialect=MONGODB_DIALECT_90, collection="records", variables={"wanted"}
    )
    assert prepare_pipeline(prepared, dialect=MONGODB_DIALECT_90) is prepared
    assert (
        prepare_pipeline(
            prepared, dialect=MONGODB_DIALECT_90, variables={"wanted", "other"}
        )
        is not prepared
    )
    with pytest.raises(OperationFailure, match="undefined variable"):
        prepare_pipeline(prepared, dialect=MONGODB_DIALECT_90, variables={"other"})
    register_aggregation_stage(
        "$reviewProbe", lambda documents, spec, context: documents
    )
    try:
        changed = prepare_pipeline(prepared, dialect=MONGODB_DIALECT_90)
        assert changed is not prepared
        assert changed.context.registry_version != prepared.context.registry_version
        assert changed.addresses == prepared.addresses
        assert changed.context.collection == prepared.context.collection
        assert changed.context.variables == prepared.context.variables
    finally:
        unregister_aggregation_stage("$reviewProbe")


def test_recompilation_retains_prepared_addresses_and_namespace():
    prepared = prepare_pipeline(
        PIPELINE,
        dialect=MONGODB_DIALECT_90,
        collection="records",
        variables={"wanted"},
        path=(3, "$lookup.pipeline"),
        scope="$lookup",
    )
    operation = compile_aggregate_operation(
        prepared, collection="records", dialect=MONGODB_DIALECT_90, let={"wanted": 2}
    )
    assert operation.pipeline is prepared
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_90, bindings={"wanted": 3, "other": 4}
    )
    rebound = operation.bind(context)
    assert rebound.pipeline is not prepared
    assert rebound.pipeline.addresses == prepared.addresses
    assert rebound.pipeline.context.collection == "records"
    assert rebound.pipeline.context.scopes == prepared.context.scopes
    assert rebound.pipeline.context.path == prepared.context.path
    assert rebound.pipeline.context.variables == frozenset(context.expressions)
