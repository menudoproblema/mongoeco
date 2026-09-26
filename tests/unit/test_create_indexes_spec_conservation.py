"""Independent field-level oracles for the createIndexes command contract."""

from __future__ import annotations

import csv
import hashlib

from pathlib import Path

import pytest

from mongoeco import MongoClient
from mongoeco.api._async.database_commands import list_commands_document
from mongoeco.api.admin_parsing import normalize_index_models_from_command
from mongoeco.engines.memory import MemoryEngine


ROOT = Path(__file__).resolve().parents[2]
MATRIX = ROOT / "docs/cxp-create-indexes-spec-conservation.csv"
EXPECTED_FIELD_COUNT = 22

# Field samples are authored from the owner index contract, independently of
# the parser's allowlist. Each invalid sample exercises the field's condition.
SAMPLES = (
    ("key", None, {"x": 1}, {"x": 0}, "keys"),
    ("name", {"x": 1}, "idx", "", "name"),
    ("unique", {"x": 1}, True, 1, "unique"),
    ("sparse", {"x": 1}, True, 1, "sparse"),
    ("background", {"x": 1}, True, 1, "background"),
    ("hidden", {"x": 1}, True, 1, "hidden"),
    ("collation", {"x": 1}, {"locale": "en"}, "en", "collation"),
    (
        "partialFilterExpression",
        {"x": 1},
        {"x": {"$gt": 0}},
        "invalid",
        "partial_filter_expression",
    ),
    ("expireAfterSeconds", {"x": 1}, 60, -1, "expire_after_seconds"),
    ("weights", {"text": "text"}, {"text": 2}, {"text": 0}, "weights"),
    (
        "wildcardProjection",
        {"$**": 1},
        {"private": 0},
        {"private": 2},
        "wildcard_projection",
    ),
    ("defaultLanguage", {"text": "text"}, "english", "", "default_language"),
    ("languageOverride", {"text": "text"}, "lang", "", "language_override"),
    ("min", {"x": 1}, -10, True, "min_value"),
    ("max", {"x": 1}, 10, True, "max_value"),
    ("bucketSize", {"x": 1}, 2, 0, "bucket_size"),
    (
        "wildcard_projection",
        {"$**": 1},
        {"private": 0},
        {"private": 2},
        "wildcard_projection",
    ),
    ("default_language", {"text": "text"}, "english", "", "default_language"),
    ("language_override", {"text": "text"}, "lang", "", "language_override"),
    ("min_value", {"x": 1}, -10, True, "min_value"),
    ("max_value", {"x": 1}, 10, True, "max_value"),
    ("bucket_size", {"x": 1}, 2, 0, "bucket_size"),
)
ALIASES = {
    "wildcard_projection": "wildcardProjection",
    "default_language": "defaultLanguage",
    "language_override": "languageOverride",
    "min_value": "min",
    "max_value": "max",
    "bucket_size": "bucketSize",
}
ACCEPTED_NOOP = {"background", "wildcardProjection", "wildcard_projection"}


def _spec(
    field: str, keys: dict[str, object] | None, value: object
) -> dict[str, object]:
    if field == "key":
        return {"key": value}
    assert keys is not None
    return {"key": keys, field: value}


def test_inventory_matches_exact_advertised_field_set_and_parser_source() -> None:
    with MATRIX.open(newline="") as source:
        rows = list(csv.DictReader(source))
    names = {row["element"].rsplit("/", 1)[-1] for row in rows}
    authored = {field for field, *_ in SAMPLES}
    advertised = set(
        list_commands_document()["commands"]["createIndexes"]["indexSpecFields"]
    )
    assert len(rows) == len(names) == len(authored) == EXPECTED_FIELD_COUNT
    assert names == authored == advertised
    parser_path = ROOT / "src/mongoeco/api/admin_parsing.py"
    parser_sha = hashlib.sha256(parser_path.read_bytes()).hexdigest()
    assert {row["source"] for row in rows} == {"src/mongoeco/api/admin_parsing.py"}
    assert {row["source_sha256"] for row in rows} == {parser_sha}
    assert {row["source_revision"] for row in rows} == {
        "5b6ab7dbdebf6107ef57da0060081a1f5f8a017b"
    }
    assert {
        row["alias_of"]: row["element"].rsplit("/", 1)[-1]
        for row in rows
        if row["alias_of"]
    } == {canonical: alias for alias, canonical in ALIASES.items()}
    assert {
        row["element"].rsplit("/", 1)[-1]
        for row in rows
        if row["disposition"] == "accepted_noop"
    } == ACCEPTED_NOOP
    help_document = list_commands_document()["commands"]["createIndexes"]
    assert help_document["indexSpecAliases"] == ALIASES
    assert set(help_document["acceptedNoopIndexSpecFields"]) == ACCEPTED_NOOP


@pytest.mark.parametrize(["field", "keys", "valid", "invalid", "attribute"], SAMPLES)
def test_each_field_accepts_valid_sample(
    field: str,
    keys: dict[str, object] | None,
    valid: object,
    invalid: object,
    attribute: str,
) -> None:
    model = normalize_index_models_from_command([_spec(field, keys, valid)])[0]
    if field == "key":
        assert model.document["key"] == valid
    else:
        assert getattr(model, attribute) == valid


@pytest.mark.parametrize(["field", "keys", "valid", "invalid", "attribute"], SAMPLES)
def test_each_field_rejects_invalid_sample(
    field: str,
    keys: dict[str, object] | None,
    valid: object,
    invalid: object,
    attribute: str,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        normalize_index_models_from_command([_spec(field, keys, invalid)])


def test_accepted_noop_fields_are_not_installed_index_guarantees() -> None:
    client = MongoClient(MemoryEngine())
    collection = client["db"]["items"]
    response = client["db"].command(
        {
            "createIndexes": "items",
            "indexes": [
                {
                    "key": {"$**": 1},
                    "name": "wild_idx",
                    "background": True,
                    "wildcardProjection": {"private": 0},
                }
            ],
        }
    )
    assert response["ok"] == 1.0
    installed = collection.index_information()["wild_idx"]
    assert "background" not in installed
    assert "wildcardProjection" not in installed
    client.close()
