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
EXPECTED_COMMAND_COUNT = 46
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


def test_wire_conservation_matrix_covers_every_advertised_command() -> None:
    with MATRIX.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))

    names = [row["element"].removeprefix("/supported_commands/") for row in rows]
    assert len(rows) == EXPECTED_COMMAND_COUNT
    assert len(names) == len(set(names))
    assert set(names) == set(WireSurface().supported_commands)
    assert all(row["product"] == "Mongoeco" for row in rows)
    assert all(row["owner"] == "Mongoeco" for row in rows)
    assert all(row["status"].startswith("bounded_behavior_verified") for row in rows)

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
            "owner_operational; compatibility_classification_pending"
        )
        if row["status"].startswith("bounded_behavior_verified"):
            assert row["positive_evidence"]
            assert row["negative_evidence"]
            _assert_test_reference_exists(row["positive_evidence"])
            _assert_test_reference_exists(row["negative_evidence"])
        else:
            assert row["status"] == (
                "command_behavior_oracles_pending; exchange_classification_pending"
            )
            assert not row["positive_evidence"]
            assert not row["negative_evidence"]
