from __future__ import annotations

import json

from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from importlib.resources import files
from typing import Any


DEPRECATION_CATALOG_SCHEMA_VERSION = "mongoeco-deprecations/v1"
DEPRECATION_CATALOG_RESOURCE = "resources/deprecations-v1.json"
DEPRECATION_CATALOG_SCHEMA_RESOURCE = "schemas/deprecations-v1.schema.json"


class DeprecationStatus(StrEnum):
    DEPRECATED = "deprecated"
    PLANNED = "planned"
    DECISION_PENDING = "decision-pending"
    REMOVED = "removed"


@dataclass(frozen=True, slots=True)
class DeprecationEntry:
    identifier: str
    category: str
    subject: str
    deprecated_since: str | None
    planned_removal: str | None
    replacement: str
    impact: str
    migration: str
    status: DeprecationStatus
    references: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in (
            "identifier",
            "category",
            "subject",
            "replacement",
            "impact",
            "migration",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                message = f"deprecation {name} must be a non-empty string"
                raise ValueError(message)
        if not isinstance(self.status, DeprecationStatus):
            message = "deprecation status must be DeprecationStatus"
            raise TypeError(message)
        for name in ("deprecated_since", "planned_removal"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value):
                message = f"deprecation {name} must be a non-empty string or None"
                raise ValueError(message)
        if (
            self.status is DeprecationStatus.DEPRECATED
            and self.deprecated_since is None
        ):
            message = "deprecated entry requires deprecated_since"
            raise ValueError(message)
        if self.status is DeprecationStatus.REMOVED and self.planned_removal is None:
            message = "removed entry requires planned_removal"
            raise ValueError(message)
        if not self.references or not all(
            isinstance(reference, str) and reference for reference in self.references
        ):
            message = "deprecation references must contain non-empty strings"
            raise ValueError(message)

    def to_document(self) -> dict[str, object]:
        return {
            "id": self.identifier,
            "category": self.category,
            "subject": self.subject,
            "deprecatedSince": self.deprecated_since,
            "plannedRemoval": self.planned_removal,
            "replacement": self.replacement,
            "impact": self.impact,
            "migration": self.migration,
            "status": self.status.value,
            "references": list(self.references),
        }


def _load_resource(relative_path: str) -> dict[str, Any]:
    resource = files("mongoeco.compat").joinpath(relative_path)
    loaded = json.loads(resource.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        message = f"{relative_path} must contain a JSON object"
        raise RuntimeError(message)
    return loaded


def _entry_from_document(document: object) -> DeprecationEntry:
    if not isinstance(document, dict):
        message = "deprecation catalog entries must be documents"
        raise RuntimeError(message)
    try:
        references = document["references"]
        if not isinstance(references, list):
            raise TypeError
        return DeprecationEntry(
            identifier=document["id"],
            category=document["category"],
            subject=document["subject"],
            deprecated_since=document["deprecatedSince"],
            planned_removal=document["plannedRemoval"],
            replacement=document["replacement"],
            impact=document["impact"],
            migration=document["migration"],
            status=DeprecationStatus(document["status"]),
            references=tuple(references),
        )
    except (KeyError, TypeError, ValueError) as error:
        message = "invalid deprecation catalog entry"
        raise RuntimeError(message) from error


def deprecation_entries() -> tuple[DeprecationEntry, ...]:
    catalog = _load_resource(DEPRECATION_CATALOG_RESOURCE)
    if catalog.get("schemaVersion") != DEPRECATION_CATALOG_SCHEMA_VERSION:
        message = "unsupported deprecation catalog schema version"
        raise RuntimeError(message)
    documents = catalog.get("entries")
    if not isinstance(documents, list):
        message = "deprecation catalog entries must be a list"
        raise RuntimeError(message)
    entries = tuple(_entry_from_document(document) for document in documents)
    identifiers = [entry.identifier for entry in entries]
    if len(identifiers) != len(set(identifiers)):
        message = "deprecation catalog identifiers must be unique"
        raise RuntimeError(message)
    return entries


def deprecation_catalog() -> dict[str, object]:
    entries = deprecation_entries()
    return {
        "schemaVersion": DEPRECATION_CATALOG_SCHEMA_VERSION,
        "entries": [entry.to_document() for entry in entries],
    }


def deprecation_catalog_schema() -> dict[str, object]:
    return deepcopy(_load_resource(DEPRECATION_CATALOG_SCHEMA_RESOURCE))
