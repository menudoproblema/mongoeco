"""Keep the row-level wire audit in sync with its owner sources."""

from __future__ import annotations

import ast
import csv
import hashlib

from pathlib import Path

from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.surface import WireSurface


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MATRIX = PROJECT_ROOT / "docs" / "cxp-wire-command-conservation.csv"
CURSOR_MATRIX = PROJECT_ROOT / "docs" / "cxp-wire-cursor-field-conservation.csv"
HELLO_MATRIX = PROJECT_ROOT / "docs" / "cxp-wire-hello-field-conservation.csv"
EXPECTED_COMMAND_COUNT = 46
EXPECTED_CURSOR_FIELDS = frozenset(
    {
        "/firstBatch/response/cursor/id",
        "/firstBatch/response/cursor/ns",
        "/firstBatch/response/cursor/firstBatch",
        "/firstBatch/response/ok",
        "/firstBatch/request/lsid",
        "/firstBatch/context/authenticated_user",
        "/getMore/request/getMore",
        "/getMore/request/collection",
        "/getMore/request/batchSize",
        "/getMore/request/effective_database",
        "/getMore/request/lsid",
        "/getMore/context/authenticated_user",
        "/getMore/response/cursor/id",
        "/getMore/response/cursor/ns",
        "/getMore/response/cursor/nextBatch",
        "/getMore/response/ok",
        "/killCursors/request/killCursors",
        "/killCursors/request/cursors",
        "/killCursors/request/effective_database",
        "/killCursors/request/lsid",
        "/killCursors/context/authenticated_user",
        "/killCursors/response/cursorsKilled",
        "/killCursors/response/cursorsUnknown",
        "/killCursors/response/cursorsAlive",
        "/killCursors/response/cursorsNotFound",
        "/killCursors/response/ok",
    }
)
EXPECTED_HELLO_FIELDS = frozenset(
    {
        "/request/hello", "/request/isMaster", "/request/ismaster",
        "/request/$db", "/request/client", "/request/compression",
        "/response/helloOk", "/response/isWritablePrimary",
        "/response/ismaster", "/response/maxBsonObjectSize",
        "/response/maxMessageSizeBytes", "/response/maxWriteBatchSize",
        "/response/logicalSessionTimeoutMinutes", "/response/connectionId",
        "/response/minWireVersion", "/response/maxWireVersion",
        "/response/readOnly", "/response/localTime", "/response/ok",
        "/response/version", "/response/versionArray", "/response/gitVersion",
        "/response/compression", "/response/setName", "/response/hosts",
        "/response/serviceId", "/response/loadBalanced",
    }
)
SHA256_HEX_LENGTH = 64
GIT_SHA_LENGTH = 40


def _assert_test_reference_exists(reference: str) -> None:
    path_text, class_name, method_name = reference.split("::")
    source_path = PROJECT_ROOT / path_text
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    test_class = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    assert any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
        for node in test_class.body
    ), reference


def _assert_test_references_exist(references: str) -> None:
    for reference in references.split("; "):
        _assert_test_reference_exists(reference)


def test_wire_conservation_matrix_covers_every_advertised_command() -> None:
    with MATRIX.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))

    names = [row["element"].removeprefix("/supported_commands/") for row in rows]
    assert len(rows) == EXPECTED_COMMAND_COUNT
    assert len(names) == len(set(names))
    assert set(names) == set(WireSurface().supported_commands)
    assert all(row["product"] == "Mongoeco" for row in rows)
    assert all(row["owner"] == "Mongoeco" for row in rows)

    surface = PROJECT_ROOT / "src/mongoeco/wire/surface.py"
    routing = PROJECT_ROOT / "src/mongoeco/wire/capabilities.py"
    surface_hash = hashlib.sha256(surface.read_bytes()).hexdigest()
    routing_hash = hashlib.sha256(routing.read_bytes()).hexdigest()
    for row, name in zip(rows, names, strict=True):
        capability = resolve_wire_command_capability(name)
        assert row["surface_sha256"] == surface_hash
        assert row["routing_sha256"] == routing_hash
        routing_claim = f"routing kind={capability.kind}; family={capability.family}"
        assert routing_claim in row["meaning"]
        assert len(row["surface_sha256"]) == SHA256_HEX_LENGTH
        assert len(row["routing_sha256"]) == SHA256_HEX_LENGTH
        assert len(row["source_revision"]) == GIT_SHA_LENGTH
        assert row["disposition"] == (
            "owner_operational; no_exchange_consumer_found_in_inspected_repos"
        )
        assert row["status"] == (
            "bounded_behavior_verified; exchange_classified_owner_operational"
        )
        assert row["positive_evidence"]
        assert row["negative_evidence"]
        _assert_test_references_exist(row["positive_evidence"])
        _assert_test_references_exist(row["negative_evidence"])


def test_wire_cursor_field_matrix_covers_request_and_result_fields() -> None:
    with CURSOR_MATRIX.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    elements = [row["element"] for row in rows]
    assert len(elements) == len(EXPECTED_CURSOR_FIELDS)
    assert len(elements) == len(set(elements))
    assert set(elements) == EXPECTED_CURSOR_FIELDS

    sources = {
        "validation_sha256": PROJECT_ROOT / "src/mongoeco/wire/_executor_validation.py",
        "context_sha256": PROJECT_ROOT / "src/mongoeco/wire/_executor_support.py",
        "cursors_sha256": PROJECT_ROOT / "src/mongoeco/wire/cursors.py",
        "session_identity_sha256": (
            PROJECT_ROOT / "src/mongoeco/wire/_session_identity.py"
        ),
        "connections_sha256": PROJECT_ROOT / "src/mongoeco/wire/connections.py",
        "handlers_sha256": PROJECT_ROOT / "src/mongoeco/wire/_executor_handlers.py",
        "passthrough_sha256": (
            PROJECT_ROOT / "src/mongoeco/wire/_executor_passthrough.py"
        ),
    }
    for row in rows:
        assert row["product"] == "Mongoeco"
        assert row["owner"] == "Mongoeco"
        assert row["disposition"] == "owner_operational"
        assert row["status"] == "bounded_behavior_verified"
        assert len(row["source_revision"]) == GIT_SHA_LENGTH
        for field, source in sources.items():
            assert row[field] == hashlib.sha256(source.read_bytes()).hexdigest()
        assert row["positive_evidence"] != row["negative_evidence"]
        _assert_test_references_exist(row["positive_evidence"])
        _assert_test_references_exist(row["negative_evidence"])


def test_wire_hello_field_matrix_covers_bounded_request_and_result_fields() -> None:
    with HELLO_MATRIX.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    elements = [row["element"] for row in rows]
    assert len(elements) == len(EXPECTED_HELLO_FIELDS)
    assert len(elements) == len(set(elements))
    assert set(elements) == EXPECTED_HELLO_FIELDS

    sources = {
        "handshake_sha256": PROJECT_ROOT / "src/mongoeco/wire/handshake.py",
        "surface_sha256": PROJECT_ROOT / "src/mongoeco/wire/surface.py",
        "connections_sha256": PROJECT_ROOT / "src/mongoeco/wire/connections.py",
        "context_sha256": PROJECT_ROOT / "src/mongoeco/wire/_executor_support.py",
        "validation_sha256": PROJECT_ROOT / "src/mongoeco/wire/_executor_validation.py",
    }
    expected_source = "; ".join(
        str(path.relative_to(PROJECT_ROOT)) for path in sources.values()
    )
    for row in rows:
        assert row["product"] == "Mongoeco"
        assert row["owner"] == "Mongoeco"
        assert row["source"] == expected_source
        assert row["disposition"] == "owner_operational"
        assert row["status"] == "bounded_behavior_verified"
        assert row["source_revision"] == "1b8369ff8d8466fab6bcfa51393a9bd43ee2ec61"
        assert row["meaning"]
        assert row["conditions_scope"]
        assert row["consumer_decision"]
        assert row["destination"]
        for field, source in sources.items():
            assert row[field] == hashlib.sha256(source.read_bytes()).hexdigest()
        assert row["positive_evidence"] != row["negative_evidence"]
        _assert_test_references_exist(row["positive_evidence"])
        _assert_test_references_exist(row["negative_evidence"])
