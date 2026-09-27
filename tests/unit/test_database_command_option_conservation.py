"""Audit advertised command options without inferring physical guarantees."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import time

from pathlib import Path
from unittest.mock import patch

import pytest

from mongoeco import MongoClient
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api._async.cursor import AsyncCursor
from mongoeco.api._async.database_commands import list_commands_document
from mongoeco.compat._catalog_models import OptionSupportStatus
from mongoeco.compat._catalog_operation_options import (
    DATABASE_COMMAND_OPTION_SUPPORT_CATALOG,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import ExecutionTimeout, OperationFailure
from mongoeco.wire import AsyncMongoEcoProxyServer


ROOT = Path(__file__).resolve().parents[2]
MATRIX = ROOT / "docs/cxp-database-command-option-conservation.csv"
EXPECTED_COMMAND_COUNT = 22
EXPECTED_OPTION_COUNT = 71
ACCEPTED_NOOP = {
    ("listCollections", "authorizedCollections"),
    ("validate", "scandata"),
    ("validate", "full"),
    ("validate", "background"),
}
EFFECTIVE_VERIFIED = {
    ("aggregate", "batchSize"),
    ("find", "batchSize"),
    ("listCollections", "filter"),
    ("listCollections", "nameOnly"),
    ("listDatabases", "filter"),
    ("listDatabases", "nameOnly"),
    ("explain", "comment"),
    ("explain", "maxTimeMS"),
    ("find", "filter"),
    ("find", "sort"),
    ("find", "projection"),
    ("find", "skip"),
    ("find", "limit"),
    ("find", "let"),
    ("find", "hint"),
    ("find", "maxTimeMS"),
    ("count", "query"),
    ("count", "skip"),
    ("count", "limit"),
    ("count", "hint"),
    ("count", "maxTimeMS"),
    ("distinct", "query"),
    ("distinct", "hint"),
    ("distinct", "maxTimeMS"),
}
COMMENT_COMMANDS = {
    "aggregate": {"aggregate": "items", "pipeline": []},
    "count": {"count": "items"},
    "createIndexes": {
        "createIndexes": "items",
        "indexes": [{"key": {"value": 1}, "name": "new_idx"}],
    },
    "currentOp": {"currentOp": 1},
    "dbHash": {"dbHash": 1},
    "delete": {"delete": "items", "deletes": [{"q": {"_id": 1}, "limit": 1}]},
    "distinct": {"distinct": "items", "key": "value"},
    "dropIndexes": {"dropIndexes": "items", "index": "value_idx"},
    "explain": {
        "explain": {"find": "items", "filter": {}},
        "verbosity": "queryPlanner",
    },
    "find": {"find": "items"},
    "findAndModify": {
        "findAndModify": "items",
        "query": {"_id": 1},
        "update": {"$set": {"value": 2}},
    },
    "insert": {"insert": "items", "documents": [{"_id": 2}]},
    "killOp": {"killOp": 1, "op": "999"},
    "listCollections": {"listCollections": 1},
    "listDatabases": {"listDatabases": 1},
    "listIndexes": {"listIndexes": "items"},
    "update": {
        "update": "items",
        "updates": [{"q": {"_id": 1}, "u": {"$set": {"value": 2}}, "multi": False}],
    },
    "validate": {"validate": "items"},
}
PROFILE_SCOPE_VERIFIED = {
    (command, "comment") for command in COMMENT_COMMANDS if command != "explain"
}


def test_inventory_tracks_exact_owner_and_public_command_option_sets() -> None:
    with MATRIX.open(newline="") as source:
        rows = list(csv.DictReader(source))
    elements = {row["element"] for row in rows}
    owner = {
        (command, option)
        for command, options in DATABASE_COMMAND_OPTION_SUPPORT_CATALOG.items()
        for option in options
    }
    commands = list_commands_document()["commands"]
    advertised = {
        (command, option)
        for command, metadata in commands.items()
        for option in metadata.get("supportedOptions", [])
    }
    matrix = {
        (parts[2], parts[4]) for row in rows if (parts := row["element"].split("/"))
    }
    assert len(DATABASE_COMMAND_OPTION_SUPPORT_CATALOG) == EXPECTED_COMMAND_COUNT
    assert len(rows) == len(elements) == len(owner) == EXPECTED_OPTION_COUNT
    assert matrix == owner == advertised
    assert "wire firstBatch" in commands["find"]["note"]
    assert "wire firstBatch" in commands["aggregate"]["note"]
    assert "cursor.batchSize" in commands["aggregate"]["note"]
    source = ROOT / "src/mongoeco/compat/_catalog_operation_options.py"
    assert {row["source_sha256"] for row in rows} == {
        hashlib.sha256(source.read_bytes()).hexdigest()
    }
    assert {row["source_revision"] for row in rows} == {
        "867139d9c99f885f6fbaa2a93a7a38b5f6df53ac"
    }
    assert {
        (command, option)
        for command, options in DATABASE_COMMAND_OPTION_SUPPORT_CATALOG.items()
        for option, support in options.items()
        if support.status is OptionSupportStatus.ACCEPTED_NOOP
    } == ACCEPTED_NOOP
    assert {
        (row["element"].split("/")[2], row["element"].split("/")[4])
        for row in rows
        if row["disposition"] == "accepted_noop"
    } == ACCEPTED_NOOP
    verified = {
        (row["element"].split("/")[2], row["element"].split("/")[4])
        for row in rows
        if row["disposition"] == "owner_claim_effective"
        and row["status"] == "bounded_behavior_verified"
        and row["positive_evidence"]
        and row["negative_evidence"]
    }
    assert verified == EFFECTIVE_VERIFIED
    profile_verified = {
        (row["element"].split("/")[2], row["element"].split("/")[4])
        for row in rows
        if row["status"] == "profile_scope_verified_other_effects_pending"
        and row["positive_evidence"]
        and row["negative_evidence"]
    }
    assert profile_verified == PROFILE_SCOPE_VERIFIED
    assert all(
        row["status"] == "option_level_oracle_pending"
        and not row["positive_evidence"]
        and not row["negative_evidence"]
        for row in rows
        if row["disposition"] == "owner_claim_effective"
        and (row["element"].split("/")[2], row["element"].split("/")[4])
        not in EFFECTIVE_VERIFIED | PROFILE_SCOPE_VERIFIED
    )


@pytest.mark.parametrize(
    ["command", "option", "value"],
    [
        ("listCollections", "authorizedCollections", True),
        ("listCollections", "authorizedCollections", False),
        ("validate", "scandata", True),
        ("validate", "full", True),
        ("validate", "background", True),
    ],
)
def test_accepted_noop_options_preserve_result(
    command: str, option: str, value: object
) -> None:
    client = MongoClient(MemoryEngine())
    try:
        database = client["audit"]
        database.create_collection("items")
        request = {command: "items" if command == "validate" else 1}
        baseline = database.command(request)
        flagged = database.command({**request, option: value})
        if command == "validate":
            assert {
                key: value for key, value in flagged.items() if key != "warnings"
            } == {key: value for key, value in baseline.items() if key != "warnings"}
            assert any(option in warning for warning in flagged["warnings"])
        else:
            assert flagged == baseline
    finally:
        client.close()


@pytest.mark.parametrize(
    ["command", "option"],
    sorted(ACCEPTED_NOOP),
)
def test_accepted_noop_options_reject_wrong_type(command: str, option: str) -> None:
    client = MongoClient(MemoryEngine())
    try:
        database = client["audit"]
        database.create_collection("items")
        request = {command: "items" if command == "validate" else 1}
        with pytest.raises(TypeError):
            database.command({**request, option: "invalid"})
    finally:
        client.close()


@pytest.mark.parametrize("command", ["listCollections", "listDatabases"])
def test_list_filter_narrows_named_results(command: str) -> None:
    with MongoClient(MemoryEngine()) as client:
        client.alpha.create_collection("events")
        client.alpha.create_collection("logs")
        client.beta.create_collection("items")
        baseline = client.alpha.command({command: 1})
        selected = client.alpha.command(
            {
                command: 1,
                "filter": {"name": "alpha" if command == "listDatabases" else "events"},
            }
        )
        rows = (
            baseline["databases"]
            if command == "listDatabases"
            else baseline["cursor"]["firstBatch"]
        )
        filtered = (
            selected["databases"]
            if command == "listDatabases"
            else selected["cursor"]["firstBatch"]
        )
        assert {row["name"] for row in rows} == (
            {"alpha", "beta"} if command == "listDatabases" else {"events", "logs"}
        )
        assert len(filtered) == 1
        assert filtered[0]["name"] == (
            "alpha" if command == "listDatabases" else "events"
        )


@pytest.mark.parametrize("command", ["listCollections", "listDatabases"])
def test_list_name_only_limits_result_fields(command: str) -> None:
    with MongoClient(MemoryEngine()) as client:
        client.alpha.create_collection("events")
        client.beta.create_collection("items")
        baseline = client.alpha.command({command: 1})
        selected = client.alpha.command({command: 1, "nameOnly": True})
        rows = (
            baseline["databases"]
            if command == "listDatabases"
            else baseline["cursor"]["firstBatch"]
        )
        narrowed = (
            selected["databases"]
            if command == "listDatabases"
            else selected["cursor"]["firstBatch"]
        )
        assert len(rows) == len(narrowed)
        assert all(set(row) > {"name"} for row in rows)
        assert all(
            set(row) == ({"name"} if command == "listDatabases" else {"name", "type"})
            for row in narrowed
        )


@pytest.mark.parametrize("command", ["listCollections", "listDatabases"])
@pytest.mark.parametrize(["option", "invalid"], [("filter", []), ("nameOnly", 1)])
def test_list_effective_options_reject_wrong_type(
    command: str, option: str, invalid: object
) -> None:
    with MongoClient(MemoryEngine()) as client:
        client.alpha.create_collection("events")
        with pytest.raises(TypeError):
            client.alpha.command({command: 1, option: invalid})


@pytest.mark.parametrize("command", ["find", "aggregate"])
def test_batch_size_limits_wire_first_batch(command: str) -> None:
    async def run() -> None:
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        async def execute(document: dict[str, object]) -> dict[str, object]:
            return await proxy._executor.execute_command(
                {**document, "$db": "audit"}, connection=connection
            )

        await execute({"create": "items"})
        await execute(
            {"insert": "items", "documents": [{"_id": 1}, {"_id": 2}, {"_id": 3}]}
        )
        command_document: dict[str, object] = {command: "items", "batchSize": 1}
        if command == "aggregate":
            command_document["pipeline"] = []
        first = await execute(command_document)
        cursor = first["cursor"]
        assert [row["_id"] for row in cursor["firstBatch"]] == [1]
        assert cursor["id"] > 0
        second = await execute(
            {"getMore": cursor["id"], "collection": "items", "batchSize": 1}
        )
        assert [row["_id"] for row in second["cursor"]["nextBatch"]] == [2]
        third = await execute(
            {"getMore": cursor["id"], "collection": "items", "batchSize": 1}
        )
        assert [row["_id"] for row in third["cursor"]["nextBatch"]] == [3]
        assert third["cursor"]["id"] == 0

    asyncio.run(run())


@pytest.mark.parametrize(
    ["command", "error_type"],
    [("find", OperationFailure), ("aggregate", TypeError)],
)
def test_batch_size_rejects_invalid_wire_type(
    command: str, error_type: type[Exception]
) -> None:
    async def run() -> None:
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))
        command_document: dict[str, object] = {
            command: "items",
            "batchSize": "invalid",
            "$db": "audit",
        }
        if command == "aggregate":
            command_document["pipeline"] = []
        with pytest.raises(error_type):
            await proxy._executor.execute_command(
                command_document, connection=connection
            )

    asyncio.run(run())


def test_find_batch_size_prefetches_locally_without_truncating_api_result() -> None:
    original = AsyncCursor._fetch_batch
    requested_sizes: list[int] = []

    async def capture(cursor: AsyncCursor, offset: int, batch_size: int):
        requested_sizes.append(batch_size)
        return await original(cursor, offset, batch_size)

    with MongoClient(MemoryEngine()) as client:
        client.audit.items.insert_many([{"_id": 1}, {"_id": 2}, {"_id": 3}])
        with patch.object(AsyncCursor, "_fetch_batch", capture):
            result = client.audit.command({"find": "items", "batchSize": 1})
        assert [row["_id"] for row in result["cursor"]["firstBatch"]] == [1, 2, 3]
        assert requested_sizes
        assert set(requested_sizes) == {1}
        with pytest.raises(TypeError):
            client.audit.command({"find": "items", "batchSize": "invalid"})


def test_aggregate_top_level_batch_size_is_wire_only() -> None:
    original = AsyncAggregationCursor._stream_windowed_pipeline
    requested_sizes: list[int | None] = []

    async def capture(cursor: AsyncAggregationCursor, *args, **kwargs):
        requested_sizes.append(cursor._batch_size)
        async for item in original(cursor, *args, **kwargs):
            yield item

    with MongoClient(MemoryEngine()) as client:
        client.audit.items.insert_many([{"_id": 1}, {"_id": 2}, {"_id": 3}])
        baseline = client.audit.command({"aggregate": "items", "pipeline": []})
        top_level = client.audit.command(
            {"aggregate": "items", "pipeline": [], "batchSize": 1}
        )
        assert top_level == baseline
        assert (
            client.audit.command(
                {"aggregate": "items", "pipeline": [], "batchSize": "ignored"}
            )
            == baseline
        )
        with patch.object(AsyncAggregationCursor, "_stream_windowed_pipeline", capture):
            nested = client.audit.command(
                {"aggregate": "items", "pipeline": [], "cursor": {"batchSize": 1}}
            )
        assert nested == baseline
        assert requested_sizes == [1]
        with pytest.raises(TypeError):
            client.audit.command(
                {
                    "aggregate": "items",
                    "pipeline": [],
                    "cursor": {"batchSize": "invalid"},
                }
            )


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize(
    ["command", "command_document"], sorted(COMMENT_COMMANDS.items())
)
def test_comment_is_recorded_only_under_enabled_profiling(
    engine_type: type[MemoryEngine | SQLiteEngine],
    command: str,
    command_document: dict[str, object],
) -> None:
    marker = f"audit-{command}"
    for profiling_enabled in (False, True):
        with MongoClient(engine_type()) as client:
            database = client.audit
            database.items.insert_one({"_id": 1, "value": 1})
            database.items.create_index([("value", 1)], name="value_idx")
            if profiling_enabled:
                database.command({"profile": 2, "slowms": 0})
            result = database.command({**command_document, "comment": marker})
            if command == "explain":
                assert result["comment"] == marker
            events = list(database["system.profile"].find({}))
            matches = [
                event for event in events if event["command"].get("comment") == marker
            ]
            assert len(matches) == int(profiling_enabled)
            if matches:
                assert command in matches[0]["command"]


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize(
    "explained_command",
    [
        {"find": "items", "filter": {}},
        {"aggregate": "items", "pipeline": []},
        {"count": "items"},
        {"distinct": "items", "key": "value"},
        {
            "update": "items",
            "updates": [{"q": {"_id": 1}, "u": {"$set": {"value": 2}}}],
        },
        {"delete": "items", "deletes": [{"q": {"_id": 1}, "limit": 1}]},
        {
            "findAndModify": "items",
            "query": {"_id": 1},
            "update": {"$set": {"value": 2}},
        },
    ],
)
def test_explain_propagates_outer_options_with_explicit_inner_precedence(
    engine_type: type[MemoryEngine | SQLiteEngine],
    explained_command: dict[str, object],
) -> None:
    outer_max_time_ms = 17
    inner_max_time_ms = 23
    with MongoClient(engine_type()) as client:
        database = client.audit
        database.items.insert_one({"_id": 1, "value": 1})
        outer = database.command(
            {
                "explain": explained_command,
                "comment": "outer",
                "maxTimeMS": outer_max_time_ms,
            }
        )
        assert outer["comment"] == "outer"
        assert outer["max_time_ms"] == outer_max_time_ms

        inner = database.command(
            {
                "explain": {
                    **explained_command,
                    "comment": "inner",
                    "maxTimeMS": inner_max_time_ms,
                },
                "comment": "outer",
                "maxTimeMS": outer_max_time_ms,
            }
        )
        assert inner["comment"] == "inner"
        assert inner["max_time_ms"] == inner_max_time_ms


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("invalid", ["invalid", -1, True])
def test_explain_rejects_invalid_outer_max_time_even_with_valid_inner_value(
    engine_type: type[MemoryEngine | SQLiteEngine], invalid: object
) -> None:
    with MongoClient(engine_type()) as client, pytest.raises((TypeError, ValueError)):
        client.audit.command(
            {
                "explain": {"find": "items", "maxTimeMS": 23},
                "maxTimeMS": invalid,
            }
        )


def test_explain_outer_options_are_preserved_on_wire() -> None:
    async def run() -> None:
        outer_max_time_ms = 17
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))
        result = await proxy._executor.execute_command(
            {
                "explain": {"find": "items", "filter": {}},
                "comment": "wire outer",
                "maxTimeMS": outer_max_time_ms,
                "$db": "audit",
            },
            connection=connection,
        )
        assert result["comment"] == "wire outer"
        assert result["max_time_ms"] == outer_max_time_ms
        with pytest.raises((TypeError, ValueError)):
            await proxy._executor.execute_command(
                {
                    "explain": {"find": "items", "maxTimeMS": 23},
                    "maxTimeMS": -1,
                    "$db": "audit",
                },
                connection=connection,
            )

    asyncio.run(run())


FIND_OPTION_CASES = [
    (
        "filter",
        {"filter": {"kind": "click"}},
        [{"_id": 3, "kind": "click", "rank": 2}],
        {"filter": []},
    ),
    (
        "sort",
        {"sort": {"rank": 1}},
        [
            {"_id": 2, "kind": "view", "rank": 1},
            {"_id": 3, "kind": "click", "rank": 2},
            {"_id": 1, "kind": "view", "rank": 3},
        ],
        {"sort": 1},
    ),
    (
        "projection",
        {"projection": {"rank": 1, "_id": 0}},
        [{"rank": 3}, {"rank": 1}, {"rank": 2}],
        {"projection": 1},
    ),
    (
        "skip",
        {"skip": 1},
        [
            {"_id": 2, "kind": "view", "rank": 1},
            {"_id": 3, "kind": "click", "rank": 2},
        ],
        {"skip": -1},
    ),
    (
        "limit",
        {"limit": 2},
        [
            {"_id": 1, "kind": "view", "rank": 3},
            {"_id": 2, "kind": "view", "rank": 1},
        ],
        {"limit": -1},
    ),
    (
        "let",
        {
            "filter": {"$expr": {"$eq": ["$kind", "$$target"]}},
            "let": {"target": "view"},
        },
        [
            {"_id": 1, "kind": "view", "rank": 3},
            {"_id": 2, "kind": "view", "rank": 1},
        ],
        {"let": 1},
    ),
]
FIND_DOCUMENTS = [
    {"_id": 1, "kind": "view", "rank": 3},
    {"_id": 2, "kind": "view", "rank": 1},
    {"_id": 3, "kind": "click", "rank": 2},
]


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["api", "wire"])
@pytest.mark.parametrize("case", FIND_OPTION_CASES)
def test_find_option_changes_result_and_rejects_invalid_input(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    case: tuple[str, dict[str, object], list[dict[str, object]], dict[str, object]],
) -> None:
    option, positive, expected, invalid = case
    assert option in positive

    if surface == "api":
        with MongoClient(engine_type()) as client:
            database = client.audit
            database.items.insert_many(FIND_DOCUMENTS)
            response = database.command({"find": "items", **positive})
            assert response["cursor"]["firstBatch"] == expected
            if option == "let":
                changed = database.command(
                    {"find": "items", **positive, "let": {"target": "click"}}
                )
                assert changed["cursor"]["firstBatch"] == [FIND_DOCUMENTS[2]]
            with pytest.raises((TypeError, ValueError, OperationFailure)):
                database.command({"find": "items", **invalid})
        return

    async def run_wire() -> None:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, object]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": FIND_DOCUMENTS})
            response = await execute({"find": "items", **positive})
            assert response["cursor"]["firstBatch"] == expected
            if option == "let":
                changed = await execute(
                    {"find": "items", **positive, "let": {"target": "click"}}
                )
                assert changed["cursor"]["firstBatch"] == [FIND_DOCUMENTS[2]]
            with pytest.raises((TypeError, ValueError, OperationFailure)):
                await execute({"find": "items", **invalid})

    asyncio.run(run_wire())


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
def test_find_hint_requires_usable_index_and_surfaces_plan(
    engine_type: type[MemoryEngine | SQLiteEngine],
) -> None:
    with MongoClient(engine_type()) as client:
        database = client.audit
        database.items.insert_many(FIND_DOCUMENTS)
        database.items.create_index([("kind", 1)], name="kind_idx")
        response = database.command({"find": "items", "hint": "kind_idx"})
        assert sorted(response["cursor"]["firstBatch"], key=lambda row: row["_id"]) == (
            FIND_DOCUMENTS
        )
        explained = database.command({"explain": {"find": "items", "hint": "kind_idx"}})
        assert explained["hinted_index"] == "kind_idx"
        with pytest.raises(OperationFailure):
            database.command({"find": "items", "hint": "missing_idx"})

    async def run_wire() -> None:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, object]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": FIND_DOCUMENTS})
            await execute(
                {
                    "createIndexes": "items",
                    "indexes": [{"key": {"kind": 1}, "name": "kind_idx"}],
                }
            )
            response = await execute({"find": "items", "hint": "kind_idx"})
            assert (
                sorted(response["cursor"]["firstBatch"], key=lambda row: row["_id"])
                == FIND_DOCUMENTS
            )
            explained = await execute(
                {"explain": {"find": "items", "hint": "kind_idx"}}
            )
            assert explained["hinted_index"] == "kind_idx"
            with pytest.raises(OperationFailure):
                await execute({"find": "items", "hint": "missing_idx"})

    asyncio.run(run_wire())


@pytest.mark.parametrize(
    ["engine_type", "module"],
    [(MemoryEngine, "memory"), (SQLiteEngine, "sqlite")],
)
def test_find_max_time_ms_reaches_engine_deadline(
    engine_type: type[MemoryEngine | SQLiteEngine], module: str
) -> None:
    def fail_if_deadline(deadline: float | None) -> None:
        if deadline is not None:
            message = "controlled deadline"
            raise ExecutionTimeout(message)

    with MongoClient(engine_type()) as client:
        database = client.audit
        database.items.insert_one({"_id": 1})
        with patch(
            f"mongoeco.engines.{module}.enforce_deadline", side_effect=fail_if_deadline
        ):
            assert database.command({"find": "items"})["cursor"]["firstBatch"] == [
                {"_id": 1}
            ]
            with pytest.raises(ExecutionTimeout, match="controlled deadline"):
                database.command({"find": "items", "maxTimeMS": 17})
        with pytest.raises(TypeError):
            database.command({"find": "items", "maxTimeMS": "invalid"})

    async def run_wire() -> None:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, object]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": [{"_id": 1}]})
            with patch(
                f"mongoeco.engines.{module}.enforce_deadline",
                side_effect=fail_if_deadline,
            ):
                assert (await execute({"find": "items"}))["cursor"]["firstBatch"] == [
                    {"_id": 1}
                ]
                with pytest.raises(ExecutionTimeout, match="controlled deadline"):
                    await execute({"find": "items", "maxTimeMS": 17})
            with pytest.raises((TypeError, ValueError, OperationFailure)):
                await execute({"find": "items", "maxTimeMS": "invalid"})

    asyncio.run(run_wire())


COUNT_DISTINCT_CASES = [
    ("count", "query", {"query": {"kind": "view"}}, 2, {"query": []}),
    ("count", "skip", {"skip": 1}, 2, {"skip": -1}),
    ("count", "limit", {"limit": 2}, 2, {"limit": -1}),
    (
        "distinct",
        "query",
        {"query": {"rank": {"$gte": 3}}},
        ["view"],
        {"query": []},
    ),
]


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["api", "wire"])
@pytest.mark.parametrize("case", COUNT_DISTINCT_CASES)
def test_count_distinct_selection_options_change_result_and_reject_invalid(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    case: tuple[str, str, dict[str, object], object, dict[str, object]],
) -> None:
    command, option, positive, expected, invalid = case
    assert option in positive
    request = {command: "items"}
    if command == "distinct":
        request["key"] = "kind"
    result_field = "n" if command == "count" else "values"

    def assert_result(result: dict[str, object]) -> None:
        actual = result[result_field]
        assert (sorted(actual) if isinstance(actual, list) else actual) == expected

    if surface == "api":
        with MongoClient(engine_type()) as client:
            database = client.audit
            database.items.insert_many(FIND_DOCUMENTS)
            baseline = database.command(request)
            assert baseline[result_field] != expected
            assert_result(database.command({**request, **positive}))
            with pytest.raises((TypeError, ValueError, OperationFailure)):
                database.command({**request, **invalid})
        return

    async def run_wire() -> None:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, object]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": FIND_DOCUMENTS})
            baseline = await execute(request)
            assert baseline[result_field] != expected
            assert_result(await execute({**request, **positive}))
            with pytest.raises((TypeError, ValueError, OperationFailure)):
                await execute({**request, **invalid})

    asyncio.run(run_wire())


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["api", "wire"])
def test_count_python_fallback_preserves_skip_and_limit(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    query = {"kind": {"$regex": "view"}}
    cases = [
        ({}, 2),
        ({"skip": 1}, 1),
        ({"limit": 1}, 1),
        ({"skip": 1, "limit": 1}, 1),
        ({"skip": 2}, 0),
        ({"limit": 0}, 0),
    ]

    if surface == "api":
        with MongoClient(engine_type()) as client:
            database = client.audit
            database.items.insert_many(FIND_DOCUMENTS)
            for options, expected in cases:
                assert (
                    database.command({"count": "items", "query": query, **options})["n"]
                    == expected
                )
        return

    async def run_wire() -> None:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, object]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": FIND_DOCUMENTS})
            for options, expected in cases:
                assert (await execute({"count": "items", "query": query, **options}))[
                    "n"
                ] == expected

    asyncio.run(run_wire())


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("command", ["count", "distinct"])
def test_count_distinct_hint_requires_index_and_surfaces_plan(
    engine_type: type[MemoryEngine | SQLiteEngine], command: str
) -> None:
    expected_view_count = 2
    request = {command: "items", "hint": "kind_idx"}
    if command == "distinct":
        request["key"] = "kind"

    def assert_hint(result: dict[str, object]) -> None:
        assert result["hinted_index"] == "kind_idx"

    with MongoClient(engine_type()) as client:
        database = client.audit
        database.items.insert_many(FIND_DOCUMENTS)
        database.items.create_index([("kind", 1)], name="kind_idx")
        database.command(request)
        assert_hint(database.command({"explain": request}))
        with pytest.raises(OperationFailure):
            database.command({**request, "hint": "missing_idx"})
        if command == "count":
            fallback = {**request, "query": {"kind": {"$regex": "view"}}}
            assert database.command(fallback)["n"] == expected_view_count
            with pytest.raises(OperationFailure):
                database.command({**fallback, "hint": "missing_idx"})

    async def run_wire() -> None:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, object]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": FIND_DOCUMENTS})
            await execute(
                {
                    "createIndexes": "items",
                    "indexes": [{"key": {"kind": 1}, "name": "kind_idx"}],
                }
            )
            await execute(request)
            assert_hint(await execute({"explain": request}))
            with pytest.raises(OperationFailure):
                await execute({**request, "hint": "missing_idx"})
            if command == "count":
                fallback = {**request, "query": {"kind": {"$regex": "view"}}}
                assert (await execute(fallback))["n"] == expected_view_count
                with pytest.raises(OperationFailure):
                    await execute({**fallback, "hint": "missing_idx"})

    asyncio.run(run_wire())


@pytest.mark.parametrize(
    ["engine_type", "module"],
    [(MemoryEngine, "memory"), (SQLiteEngine, "sqlite")],
)
@pytest.mark.parametrize("command", ["count", "distinct"])
def test_count_distinct_max_time_ms_reaches_engine_deadline(
    engine_type: type[MemoryEngine | SQLiteEngine], module: str, command: str
) -> None:
    request = {command: "items"}
    if command == "distinct":
        request["key"] = "kind"

    def fail_if_deadline(deadline: float | None) -> None:
        if deadline is not None:
            message = "controlled deadline"
            raise ExecutionTimeout(message)

    with MongoClient(engine_type()) as client:
        database = client.audit
        database.items.insert_many(FIND_DOCUMENTS)
        with patch(
            f"mongoeco.engines.{module}.enforce_deadline", side_effect=fail_if_deadline
        ):
            database.command(request)
            with pytest.raises(ExecutionTimeout, match="controlled deadline"):
                database.command({**request, "maxTimeMS": 17})
        with pytest.raises(TypeError):
            database.command({**request, "maxTimeMS": "invalid"})

    async def run_wire() -> None:
        async with AsyncMongoEcoProxyServer(engine=engine_type()) as proxy:
            connection = proxy._connections.create(("127.0.0.1", 27017))

            async def execute(document: dict[str, object]) -> dict[str, object]:
                return await proxy._executor.execute_command(
                    {**document, "$db": "audit"}, connection=connection
                )

            await execute({"insert": "items", "documents": FIND_DOCUMENTS})
            with patch(
                f"mongoeco.engines.{module}.enforce_deadline",
                side_effect=fail_if_deadline,
            ):
                await execute(request)
                with pytest.raises(ExecutionTimeout, match="controlled deadline"):
                    await execute({**request, "maxTimeMS": 17})
            with pytest.raises((TypeError, ValueError, OperationFailure)):
                await execute({**request, "maxTimeMS": "invalid"})

    asyncio.run(run_wire())


def test_sqlite_count_interrupts_expired_sql_statement() -> None:
    document_count = 300
    engine = SQLiteEngine()
    with MongoClient(engine) as client:
        database = client.audit
        database.items.insert_many(
            {"_id": index, "kind": "view"} for index in range(document_count)
        )
        calls = 0

        def slow_extract(document: str, path: str) -> object:
            nonlocal calls
            calls += 1
            time.sleep(0.00005)
            return json.loads(document).get(path.removeprefix("$."))

        connection = engine._connection
        assert connection is not None
        connection.create_function("json_extract", 2, slow_extract)
        request = {"count": "items", "query": {"kind": "view"}}
        assert database.command(request)["n"] == document_count
        baseline_calls = calls
        calls = 0
        with pytest.raises(ExecutionTimeout):
            database.command({**request, "maxTimeMS": 20})
        assert 0 < calls < baseline_calls
        assert database.command({"count": "items"})["n"] == document_count
