"""Independent checks for Mongoeco's portable compatibility declarations."""

from copy import deepcopy

import pytest

from cxp.exchange import (
    Document,
    InvalidDocumentError,
    catalog_reference,
    evaluate_requirements_detailed,
)

from mongoeco.cxp.exchange import (
    PROFILE_NAMES,
    TIER_NAMES,
    load_mongodb_catalog,
    load_mongodb_declared_snapshot,
    load_mongodb_profile,
    load_mongodb_tier,
    mongodb_catalog_store,
)


def _context() -> Document:
    return Document(
        {
            "document_type": "cxp.context",
            "spec_version": 2,
            "payload": {
                "subject_id": "mongoeco-public-catalog",
                "configuration_revision": "mongoeco-public-catalog-1.0.0",
                "accepted_sources": ["declared"],
            },
        },
        expected_type="cxp.context",
    )


def _snapshot_with(
    *, missing_capability: str | None = None, missing_metadata: str | None = None
) -> Document:
    content = deepcopy(load_mongodb_declared_snapshot().as_dict())
    if missing_capability:
        content["payload"]["capabilities"] = [
            item
            for item in content["payload"]["capabilities"]
            if item["name"] != missing_capability
        ]
    if missing_metadata:
        for item in content["payload"]["capabilities"]:
            if item["name"] == "aggregation":
                item["properties"]["metadata_keys"].remove(missing_metadata)
    return Document(content, expected_type="cxp.snapshot")


@pytest.mark.parametrize("name", PROFILE_NAMES)
def test_declared_surface_satisfies_every_profile(name: str) -> None:
    result = evaluate_requirements_detailed(
        load_mongodb_declared_snapshot(),
        load_mongodb_profile(name),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "compatible"


@pytest.mark.parametrize("name", TIER_NAMES)
def test_declared_surface_satisfies_every_tier(name: str) -> None:
    result = evaluate_requirements_detailed(
        load_mongodb_declared_snapshot(),
        load_mongodb_tier(name),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "compatible"


def test_missing_capability_is_insufficient_information() -> None:
    result = evaluate_requirements_detailed(
        _snapshot_with(missing_capability="aggregation"),
        load_mongodb_profile("mongodb-core"),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"


def test_declared_missing_metadata_key_fails_profile() -> None:
    result = evaluate_requirements_detailed(
        _snapshot_with(missing_metadata="supportedStages"),
        load_mongodb_profile("mongodb-core"),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "incompatible"


def test_unreported_metadata_keys_are_insufficient_information() -> None:
    content = load_mongodb_declared_snapshot().as_dict()
    for item in content["payload"]["capabilities"]:
        if item["name"] == "aggregation":
            del item["properties"]["metadata_keys"]
    snapshot = Document(content, expected_type="cxp.snapshot")
    result = evaluate_requirements_detailed(
        snapshot,
        load_mongodb_profile("mongodb-core"),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"


def test_catalog_identity_and_hash_are_exact() -> None:
    catalog = load_mongodb_catalog()
    reference = catalog_reference(catalog)
    assert catalog.sha256 == (
        "2426ee7a7d9b4c06ab6d22b3eb9de2dcc16f2cd7b385efb618c7eb373c84dc5a"
    )
    assert reference == load_mongodb_declared_snapshot().payload["catalog"]
    assert all(
        reference == load_mongodb_profile(name).payload["catalog"]
        for name in PROFILE_NAMES
    )
    assert all(
        reference == load_mongodb_tier(name).payload["catalog"] for name in TIER_NAMES
    )
    changed = load_mongodb_declared_snapshot().as_dict()
    changed["payload"]["catalog"]["sha256"] = "0" * 64
    with pytest.raises(InvalidDocumentError):
        evaluate_requirements_detailed(
            Document(changed, expected_type="cxp.snapshot"),
            load_mongodb_profile("mongodb-core"),
            _context(),
            catalogs=mongodb_catalog_store(),
        )


def test_declared_source_does_not_satisfy_observed_only_policy() -> None:
    content = _context().as_dict()
    content["payload"]["accepted_sources"] = ["observed"]
    result = evaluate_requirements_detailed(
        load_mongodb_declared_snapshot(),
        load_mongodb_profile("mongodb-core"),
        Document(content, expected_type="cxp.context"),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"
    assert {finding.code for finding in result.findings} == {"source_not_accepted"}
