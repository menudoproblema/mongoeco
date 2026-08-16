from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from jsonschema import Draft202012Validator, ValidationError

from mongoeco.compat import (
    DEPRECATION_CATALOG_SCHEMA_VERSION,
    DeprecationStatus,
    deprecation_catalog,
    deprecation_catalog_schema,
    deprecation_entries,
    deprecations as deprecations_module,
)
from mongoeco.compat.deprecations import DeprecationEntry


EXPECTED_IDENTIFIERS = {
    "engine.spi-v1",
    "engine.legacy-adapter",
    "engine.put-document",
    "engine.put-documents-bulk",
    "engine.legacy-on-commit",
    "engine.capture-document",
    "engine.capture-documents",
    "engine.supports-injected-clock",
    "search.legacy-engine-methods",
    "search.automatic-highlights-field",
    "search.count-preview",
    "search.facet-preview",
    "search.highlight-preview",
    "search.matched-count",
    "search.unbound-execution-trace",
    "operations.duplicated-context-fields",
}


def _entry(**overrides: object) -> DeprecationEntry:
    values = {
        "identifier": "test.entry",
        "category": "test",
        "subject": "test subject",
        "deprecated_since": None,
        "planned_removal": None,
        "replacement": "replacement",
        "impact": "impact",
        "migration": "migration",
        "status": DeprecationStatus.DECISION_PENDING,
        "references": ("docs/deprecations.md",),
    }
    values.update(overrides)
    return DeprecationEntry(**values)


def test_deprecation_catalog_is_versioned_complete_and_schema_valid() -> None:
    catalog = deprecation_catalog()
    Draft202012Validator(deprecation_catalog_schema()).validate(catalog)
    assert catalog["schemaVersion"] == DEPRECATION_CATALOG_SCHEMA_VERSION
    assert {entry["id"] for entry in catalog["entries"]} == EXPECTED_IDENTIFIERS
    assert len(catalog["entries"]) == len(EXPECTED_IDENTIFIERS)


def test_deprecation_entries_are_typed_and_have_actionable_replacements() -> None:
    entries = deprecation_entries()
    assert {entry.identifier for entry in entries} == EXPECTED_IDENTIFIERS
    for entry in entries:
        assert entry.replacement
        assert entry.migration
        assert entry.references
        if entry.status is DeprecationStatus.DEPRECATED:
            assert entry.deprecated_since
            assert entry.planned_removal == "5.0.0"
    pending = next(
        entry
        for entry in entries
        if entry.identifier == "operations.duplicated-context-fields"
    )
    assert pending.status is DeprecationStatus.DECISION_PENDING
    assert pending.deprecated_since is None
    assert pending.planned_removal is None


def test_deprecation_references_point_to_repository_documents() -> None:
    for entry in deprecation_entries():
        for reference in entry.references:
            path, _separator, _fragment = reference.partition("#")
            assert Path(path).is_file(), (entry.identifier, reference)


def test_deprecation_catalog_returns_owned_documents() -> None:
    first = deprecation_catalog()
    original = deepcopy(first)
    first["entries"][0]["references"].append("caller-mutated")
    assert deprecation_catalog() == original


def test_static_v1_fixture_remains_valid() -> None:
    fixture = Path("tests/fixtures/deprecations_v1.json")
    document = __import__("json").loads(fixture.read_text(encoding="utf-8"))
    Draft202012Validator(deprecation_catalog_schema()).validate(document)


def test_schema_rejects_deprecated_entry_without_migration_version() -> None:
    document = deprecation_catalog()
    document["entries"][0]["plannedRemoval"] = None
    with pytest.raises(ValidationError, match="None is not of type 'string'"):
        Draft202012Validator(deprecation_catalog_schema()).validate(document)


@pytest.mark.parametrize(
    "field",
    ["identifier", "category", "subject", "replacement", "impact", "migration"],
)
def test_entry_rejects_empty_required_text(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        _entry(**{field: ""})


def test_entry_rejects_invalid_status_versions_and_references() -> None:
    with pytest.raises(TypeError, match="DeprecationStatus"):
        _entry(status="deprecated")
    with pytest.raises(ValueError, match="deprecated_since"):
        _entry(deprecated_since="")
    with pytest.raises(ValueError, match="requires deprecated_since"):
        _entry(status=DeprecationStatus.DEPRECATED)
    with pytest.raises(ValueError, match="requires planned_removal"):
        _entry(status=DeprecationStatus.REMOVED)
    with pytest.raises(ValueError, match="references"):
        _entry(references=())


def test_catalog_loader_rejects_non_object_resource(tmp_path: Path) -> None:
    resource = tmp_path / "invalid.json"
    resource.write_text("[]", encoding="utf-8")
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(deprecations_module, "files", lambda _package: tmp_path)
        with pytest.raises(RuntimeError, match="must contain a JSON object"):
            deprecations_module._load_resource(resource.name)


def test_catalog_rejects_invalid_entries_version_and_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match="must be documents"):
        deprecations_module._entry_from_document("invalid")
    with pytest.raises(RuntimeError, match="invalid deprecation catalog entry"):
        deprecations_module._entry_from_document({"references": "invalid"})

    monkeypatch.setattr(
        deprecations_module,
        "_load_resource",
        lambda _path: {"schemaVersion": "future", "entries": []},
    )
    with pytest.raises(RuntimeError, match="unsupported"):
        deprecation_entries()

    monkeypatch.setattr(
        deprecations_module,
        "_load_resource",
        lambda _path: {
            "schemaVersion": DEPRECATION_CATALOG_SCHEMA_VERSION,
            "entries": "invalid",
        },
    )
    with pytest.raises(RuntimeError, match="must be a list"):
        deprecation_entries()

    document = _entry().to_document()
    monkeypatch.setattr(
        deprecations_module,
        "_load_resource",
        lambda _path: {
            "schemaVersion": DEPRECATION_CATALOG_SCHEMA_VERSION,
            "entries": [document, document],
        },
    )
    with pytest.raises(RuntimeError, match="must be unique"):
        deprecation_entries()
