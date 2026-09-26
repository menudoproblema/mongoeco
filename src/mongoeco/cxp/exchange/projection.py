"""Explain the installed Mongoeco declaration using exchange results."""

from __future__ import annotations

import json

from copy import deepcopy
from functools import lru_cache
from importlib.resources import files

from cxp.exchange import (
    Document,
    catalog_reference,
    evaluate_requirements_detailed,
)

from mongoeco.cxp.exchange import (
    PROFILE_NAMES,
    load_mongodb_catalog,
    load_mongodb_declared_snapshot,
    load_mongodb_profile,
    mongodb_catalog_store,
)


@lru_cache(maxsize=1)
def _operational_metadata() -> dict[str, dict[str, object]]:
    path = files("mongoeco.cxp.exchange").joinpath("data/operational-metadata.json")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        message = "Invalid Mongoeco operational metadata"
        raise ValueError(message)
    return value


@lru_cache(maxsize=1)
def _profile_verdicts() -> dict[str, str]:
    snapshot = load_mongodb_declared_snapshot()
    context = Document(
        {
            "document_type": "cxp.context",
            "spec_version": 2,
            "payload": {
                "subject_id": snapshot.payload["subject_id"],
                "configuration_revision": snapshot.payload["configuration_revision"],
                "accepted_sources": ["declared"],
            },
        },
        expected_type="cxp.context",
    )
    store = mongodb_catalog_store()
    return {
        name: evaluate_requirements_detailed(
            snapshot,
            load_mongodb_profile(name),
            context,
            catalogs=store,
        ).verdict
        for name in PROFILE_NAMES
    }


def build_mongodb_exchange_explain_projection(
    *,
    capability: str,
    additional_capabilities: tuple[str, ...] = (),
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    """Report provider profile results without operation-level inference."""
    catalog = load_mongodb_catalog()
    known = {item["name"] for item in catalog.payload["capabilities"]}
    selected = (capability, *additional_capabilities)
    unknown = sorted(set(selected) - known)
    if unknown:
        message = f"Unknown MongoDB capability: {unknown!r}"
        raise ValueError(message)
    operational = _operational_metadata()
    subject = (
        "vector_search"
        if "vector_search" in additional_capabilities
        else "search"
        if "search" in additional_capabilities
        else capability
    )
    projection: dict[str, object] = {
        "catalog": catalog_reference(catalog),
        "interface": "database/mongodb",
        "provider": "mongoeco",
        "capability": capability,
        "profileVerdicts": dict(_profile_verdicts()),
    }
    if additional_capabilities:
        projection["additionalCapabilities"] = list(additional_capabilities)
    operation_metadata = operational.get(subject, {}).get("operationMetadata")
    if isinstance(operation_metadata, dict):
        operation_name = (
            "find"
            if capability == "read"
            else "aggregate"
            if capability == "aggregation"
            else None
        )
        if operation_name is not None:
            selected_metadata = operation_metadata.get(operation_name)
            if isinstance(selected_metadata, dict):
                projection["operationName"] = operation_name
                projection["operationMetadata"] = deepcopy(selected_metadata)
    if metadata:
        projection["metadata"] = deepcopy(metadata)
    return projection


__all__ = ("build_mongodb_exchange_explain_projection",)
