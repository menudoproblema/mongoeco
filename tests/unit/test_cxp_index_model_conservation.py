"""Keep the IndexModel inventory tied to its source and bounded evidence."""

from __future__ import annotations

import ast
import csv
import hashlib

from dataclasses import fields
from pathlib import Path

from mongoeco.types import IndexModel


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MATRIX = PROJECT_ROOT / "docs" / "cxp-index-model-conservation.csv"
NOOP_FIELDS = {"background", "wildcard_projection"}
EXPECTED_FIELD_COUNT = 16
GIT_SHA_LENGTH = 40


def _reference_exists(reference: str) -> bool:
    path_text, *names = reference.split("::")
    source_path = PROJECT_ROOT / path_text
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    if len(names) == 1:
        return any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == names[0]
            for node in tree.body
        )
    class_name, method_name = names
    return any(
        isinstance(node, ast.ClassDef)
        and node.name == class_name
        and any(
            isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            and child.name == method_name
            for child in node.body
        )
        for node in tree.body
    )


def test_index_model_conservation_matrix_covers_every_field() -> None:
    with MATRIX.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))

    names = [row["element"].removeprefix("/IndexModel/") for row in rows]
    assert len(names) == len(set(names))
    assert set(names) == {field.name for field in fields(IndexModel)}
    assert len(rows) == EXPECTED_FIELD_COUNT

    for row, name in zip(rows, names, strict=True):
        for source_key, hash_key in (
            ("source", "source_sha256"),
            ("creation_source", "creation_sha256"),
        ):
            source = PROJECT_ROOT / row[source_key]
            assert row[hash_key] == hashlib.sha256(source.read_bytes()).hexdigest()
        assert row["product"] == row["owner"] == "Mongoeco"
        assert len(row["source_revision"]) == GIT_SHA_LENGTH
        assert "owner operational" in row["disposition"]
        if name == "wildcard_projection":
            assert row["status"] == (
                "bounded accepted-noop verified for 7.0/8.0; rejected for 9.0"
            )
        elif name in NOOP_FIELDS:
            assert row["status"] == "bounded accepted-noop verified"
        else:
            assert row["status"] == "forwarded; engine effect outside this inventory"
        for evidence_key in ("positive_evidence", "negative_evidence"):
            assert all(
                _reference_exists(reference)
                for reference in row[evidence_key].split("; ")
            )
