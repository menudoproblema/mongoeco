"""Audit advertised command options without inferring physical guarantees."""

from __future__ import annotations

import csv
import hashlib

from pathlib import Path

import pytest

from mongoeco import MongoClient
from mongoeco.api._async.database_commands import list_commands_document
from mongoeco.compat._catalog_models import OptionSupportStatus
from mongoeco.compat._catalog_operation_options import (
    DATABASE_COMMAND_OPTION_SUPPORT_CATALOG,
)
from mongoeco.engines.memory import MemoryEngine


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
    advertised = {
        (command, option)
        for command, metadata in list_commands_document()["commands"].items()
        for option in metadata.get("supportedOptions", [])
    }
    matrix = {
        (parts[2], parts[4]) for row in rows if (parts := row["element"].split("/"))
    }
    assert len(DATABASE_COMMAND_OPTION_SUPPORT_CATALOG) == EXPECTED_COMMAND_COUNT
    assert len(rows) == len(elements) == len(owner) == EXPECTED_OPTION_COUNT
    assert matrix == owner == advertised
    source = ROOT / "src/mongoeco/compat/_catalog_operation_options.py"
    assert {row["source_sha256"] for row in rows} == {
        hashlib.sha256(source.read_bytes()).hexdigest()
    }
    assert {row["source_revision"] for row in rows} == {
        "013b5c7fa5131912ce44996c231137f1c37d25e0"
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
