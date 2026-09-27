"""Command-level evidence for aggregate option effects and limits."""

from __future__ import annotations

import asyncio

from typing import Any
from unittest.mock import patch

import pytest

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import ExecutionTimeout, OperationFailure
from mongoeco.wire import AsyncMongoEcoProxyServer


ENGINE_TYPES = [MemoryEngine, SQLiteEngine]
SURFACES = ["api", "wire"]
ITEMS = [
    {"_id": 1, "kind": "view"},
    {"_id": 2, "kind": "click"},
]
OTHER = [
    {"_id": "a", "kind": "view"},
    {"_id": "b", "kind": "click"},
]


def execute_aggregate_sequence(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    requests: list[dict[str, object]],
    *,
    engine_options: dict[str, object] | None = None,
) -> list[dict[str, Any]]:
    engine = engine_type(**(engine_options or {}))
    if surface == "api":
        with MongoClient(engine) as client:
            database = client.audit
            database.items.insert_many(ITEMS)
            database.other.insert_many(OTHER)
            database.items.create_index([("kind", 1)], name="kind_idx")
            return [database.command(request) for request in requests]

    async def run_wire() -> list[dict[str, Any]]:
        async with AsyncMongoEcoProxyServer(engine=engine) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, Any]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": ITEMS})
            await execute({"insert": "other", "documents": OTHER})
            await execute(
                {
                    "createIndexes": "items",
                    "indexes": [{"key": {"kind": 1}, "name": "kind_idx"}],
                }
            )
            return [await execute(request) for request in requests]

    return asyncio.run(run_wire())


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_allow_disk_use_requires_real_spill_policy_for_blocking_pipeline(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    pipeline = [{"$group": {"_id": "$kind", "n": {"$sum": 1}}}]
    request = {"aggregate": "items", "pipeline": pipeline}
    budget = {"aggregation_materialization_limit": 1}
    with pytest.raises(OperationFailure, match="allowDiskUse or spill-to-disk"):
        execute_aggregate_sequence(
            engine_type,
            surface,
            [{**request, "allowDiskUse": False}],
            engine_options={**budget, "aggregation_spill_threshold": 1},
        )
    with pytest.raises(OperationFailure, match="allowDiskUse or spill-to-disk"):
        execute_aggregate_sequence(
            engine_type,
            surface,
            [{**request, "allowDiskUse": True}],
            engine_options=budget,
        )
    result = execute_aggregate_sequence(
        engine_type,
        surface,
        [{**request, "allowDiskUse": True}],
        engine_options={**budget, "aggregation_spill_threshold": 1},
    )[0]
    assert sorted(result["cursor"]["firstBatch"], key=lambda row: row["_id"]) == [
        {"_id": "click", "n": 1},
        {"_id": "view", "n": 1},
    ]
    with pytest.raises((TypeError, ValueError, OperationFailure)):
        execute_aggregate_sequence(
            engine_type, surface, [{**request, "allowDiskUse": 1}]
        )


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_aggregate_hint_is_validated_and_visible_in_explain(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    request = {
        "aggregate": "items",
        "pipeline": [{"$match": {"kind": "view"}}],
        "hint": "kind_idx",
    }
    result, explanation = execute_aggregate_sequence(
        engine_type, surface, [request, {"explain": request}]
    )
    assert result["cursor"]["firstBatch"] == [ITEMS[0]]
    assert explanation["hint"] == "kind_idx"
    with pytest.raises(OperationFailure):
        execute_aggregate_sequence(
            engine_type, surface, [{**request, "hint": "missing_idx"}]
        )


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_aggregate_let_binds_top_level_and_lookup_subpipeline(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    select = {
        "aggregate": "items",
        "pipeline": [{"$match": {"$expr": {"$eq": ["$kind", "$$target"]}}}],
    }
    view, click = execute_aggregate_sequence(
        engine_type,
        surface,
        [{**select, "let": {"target": "view"}}, {**select, "let": {"target": "click"}}],
    )
    assert view["cursor"]["firstBatch"] == [ITEMS[0]]
    assert click["cursor"]["firstBatch"] == [ITEMS[1]]
    lookup = {
        "aggregate": "items",
        "pipeline": [
            {
                "$lookup": {
                    "from": "other",
                    "let": {"selected": "$$target"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$kind", "$$selected"]}}}
                    ],
                    "as": "matches",
                }
            }
        ],
    }
    result = execute_aggregate_sequence(
        engine_type, surface, [{**lookup, "let": {"target": "view"}}]
    )[0]
    assert [row["matches"] for row in result["cursor"]["firstBatch"]] == [
        [OTHER[0]],
        [OTHER[0]],
    ]
    with pytest.raises((TypeError, ValueError, OperationFailure)):
        execute_aggregate_sequence(engine_type, surface, [{**select, "let": 1}])


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_aggregate_max_time_ms_reaches_execution_and_explain(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    request = {"aggregate": "items", "pipeline": []}

    def fail_if_deadline(deadline: float | None) -> None:
        if deadline is not None:
            message = "controlled deadline"
            raise ExecutionTimeout(message)

    with patch(
        "mongoeco.api._async.aggregation_cursor.enforce_deadline",
        side_effect=fail_if_deadline,
    ):
        baseline = execute_aggregate_sequence(engine_type, surface, [request])[0]
        assert (
            sorted(baseline["cursor"]["firstBatch"], key=lambda row: row["_id"])
            == ITEMS
        )
        with pytest.raises(ExecutionTimeout, match="controlled deadline"):
            execute_aggregate_sequence(
                engine_type, surface, [{**request, "maxTimeMS": 17}]
            )
        with pytest.raises(ExecutionTimeout, match="controlled deadline"):
            execute_aggregate_sequence(
                engine_type,
                surface,
                [{"explain": {**request, "maxTimeMS": 17}}],
            )
    with pytest.raises((TypeError, ValueError, OperationFailure)):
        execute_aggregate_sequence(
            engine_type, surface, [{**request, "maxTimeMS": "invalid"}]
        )
