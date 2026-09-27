"""Independent command-level oracles for local admin option effects."""

from __future__ import annotations

import asyncio

from typing import Any

import pytest

from mongoeco import MongoClient
from mongoeco.api import AsyncMongoClient
from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.wire import AsyncMongoEcoProxyServer


ENGINE_TYPES = [MemoryEngine, SQLiteEngine]
SURFACES = ["api", "wire"]
DOCUMENTS = [
    {"_id": 1, "kind": "view", "payload": "x" * 1000},
    {"_id": 2, "kind": "view", "payload": "y" * 1000},
]


def execute_sequence(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    requests: list[dict[str, object]],
) -> list[dict[str, Any]]:
    if surface == "api":
        with MongoClient(engine_type()) as client:
            database = client.audit
            database.items.insert_many(DOCUMENTS)
            database.other.insert_one({"_id": 3, "kind": "click"})
            return [database.command(request) for request in requests]

    async def run_wire() -> list[dict[str, Any]]:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, Any]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": DOCUMENTS})
            await execute(
                {"insert": "other", "documents": [{"_id": 3, "kind": "click"}]}
            )
            return [await execute(request) for request in requests]

    return asyncio.run(run_wire())


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize(
    ["command", "size_fields"],
    [
        ("collStats", ("size", "storageSize", "totalIndexSize")),
        ("dbStats", ("dataSize", "storageSize", "indexSize")),
    ],
)
def test_scale_changes_only_size_metrics_and_rejects_invalid_values(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    command: str,
    size_fields: tuple[str, ...],
) -> None:
    scale_factor = 2
    name = "items" if command == "collStats" else 1
    baseline, scaled = execute_sequence(
        engine_type,
        surface,
        [{command: name}, {command: name, "scale": scale_factor}],
    )
    assert baseline["scaleFactor"] == 1
    assert scaled["scaleFactor"] == scale_factor
    assert baseline[size_fields[0]] > scale_factor
    for field in size_fields:
        assert scaled[field] == baseline[field] // scale_factor
    for invalid in (0, -1, True, "2"):
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            execute_sequence(engine_type, surface, [{command: name, "scale": invalid}])


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_show_privileges_controls_auth_status_shape(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    default, included, omitted = execute_sequence(
        engine_type,
        surface,
        [
            {"connectionStatus": 1},
            {"connectionStatus": 1, "showPrivileges": True},
            {"connectionStatus": 1, "showPrivileges": False},
        ],
    )
    key = "authenticatedUserPrivileges"
    assert key not in default["authInfo"]
    assert included["authInfo"][key] == []
    assert key not in omitted["authInfo"]
    with pytest.raises((TypeError, ValueError, OperationFailure)):
        execute_sequence(
            engine_type, surface, [{"connectionStatus": 1, "showPrivileges": 1}]
        )


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_db_hash_collections_selects_exact_names_and_stable_order(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    all_hash, selected, reversed_order = execute_sequence(
        engine_type,
        surface,
        [
            {"dbHash": 1},
            {"dbHash": 1, "collections": ["items"]},
            {"dbHash": 1, "collections": ["other", "items"]},
        ],
    )
    assert set(all_hash["collections"]) == {"items", "other"}
    assert selected["collections"] == {"items": all_hash["collections"]["items"]}
    assert selected["md5"] != all_hash["md5"]
    assert reversed_order["collections"] == all_hash["collections"]
    assert reversed_order["md5"] == all_hash["md5"]
    for invalid in (["missing"], ["items", ""], "items"):
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            execute_sequence(
                engine_type, surface, [{"dbHash": 1, "collections": invalid}]
            )


EXPLAINED_COMMANDS = [
    {"find": "items"},
    {"aggregate": "items", "pipeline": []},
    {"count": "items"},
    {"distinct": "items", "key": "kind"},
    {"update": "items", "updates": [{"q": {"_id": 1}, "u": {"$set": {"kind": "new"}}}]},
    {"delete": "items", "deletes": [{"q": {"_id": 1}, "limit": 1}]},
    {
        "findAndModify": "items",
        "query": {"_id": 1},
        "update": {"$set": {"kind": "new"}},
    },
]


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("explained", EXPLAINED_COMMANDS)
def test_explain_verbosity_is_surfaced_for_every_routed_command(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    explained: dict[str, object],
) -> None:
    default, selected = execute_sequence(
        engine_type,
        surface,
        [
            {"explain": explained},
            {"explain": explained, "verbosity": "executionStats"},
        ],
    )
    assert "verbosity" not in default
    assert selected["verbosity"] == "executionStats"
    assert selected["explained_command"] == next(iter(explained))
    with pytest.raises((TypeError, ValueError, OperationFailure)):
        execute_sequence(engine_type, surface, [{"explain": explained, "verbosity": 1}])


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_profile_slowms_controls_level_one_recording(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    high_threshold = 1_000_000
    high, _count_high, status_high, low, _count_low, status_low = execute_sequence(
        engine_type,
        surface,
        [
            {"profile": 1, "slowms": high_threshold},
            {"count": "items"},
            {"profile": -1},
            {"profile": 1, "slowms": 0},
            {"count": "items"},
            {"profile": -1},
        ],
    )
    assert high["slowms"] == status_high["slowms"] == high_threshold
    assert status_high["entryCount"] == 0
    assert low["slowms"] == status_low["slowms"] == 0
    assert status_low["entryCount"] > 0
    for invalid in ("invalid", -1, True):
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            execute_sequence(engine_type, surface, [{"profile": 1, "slowms": invalid}])


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_kill_op_targets_live_registered_operation_and_rejects_invalid_id(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    async def exercise(engine: MemoryEngine | SQLiteEngine, execute) -> None:
        ready = asyncio.Event()
        waiting = asyncio.Event()
        operation_ids: list[str] = []

        async def worker() -> None:
            with track_active_operation(
                engine,
                command_name="find",
                operation_type="read",
                namespace="audit.items",
            ) as operation_id:
                assert operation_id is not None
                operation_ids.append(operation_id)
                ready.set()
                await waiting.wait()

        task = asyncio.create_task(worker())
        await ready.wait()
        operation_id = operation_ids[0]
        current = await execute({"currentOp": 1})
        assert operation_id in {row["opid"] for row in current["inprog"]}
        assert (await execute({"killOp": 1, "op": "missing"}))["numKilled"] == 0
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            await execute({"killOp": 1, "op": ""})
        assert (await execute({"killOp": 1, "op": operation_id}))["numKilled"] == 1
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        current = await execute({"currentOp": 1})
        assert operation_id not in {row["opid"] for row in current["inprog"]}
        assert (await execute({"killOp": 1, "op": operation_id}))["numKilled"] == 0

    async def run_api() -> None:
        engine = engine_type()
        async with AsyncMongoClient(engine) as client:
            await exercise(engine, client.audit.command)

    async def run_wire() -> None:
        engine = engine_type()
        async with AsyncMongoEcoProxyServer(engine=engine) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, Any]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await exercise(engine, execute)

    asyncio.run(run_api() if surface == "api" else run_wire())
