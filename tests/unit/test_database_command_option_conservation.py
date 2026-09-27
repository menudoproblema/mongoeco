"""Audit advertised command options without inferring physical guarantees."""

from __future__ import annotations

import asyncio
import csv
import hashlib

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
from mongoeco.errors import OperationFailure
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
    assert all(
        row["status"] == "option_level_oracle_pending"
        and not row["positive_evidence"]
        and not row["negative_evidence"]
        for row in rows
        if row["disposition"] == "owner_claim_effective"
        and (row["element"].split("/")[2], row["element"].split("/")[4])
        not in EFFECTIVE_VERIFIED
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
