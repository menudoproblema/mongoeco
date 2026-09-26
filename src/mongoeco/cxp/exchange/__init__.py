"""Mongoeco-owned, content-pinned CXP exchange declarations."""

from __future__ import annotations

from importlib.resources import files

from cxp.exchange import CatalogStore, Document, load_document

from mongoeco.cxp.exchange.snapshot import (
    MongoCapabilityClaim,
    MongoOperationClaim,
    MongoSnapshotIdentity,
    build_mongodb_snapshot,
)


PROFILE_NAMES = (
    "mongodb-core",
    "mongodb-text-search",
    "mongodb-search",
    "mongodb-platform",
    "mongodb-aggregate-rich",
)
TIER_NAMES = ("core", "search", "platform")


def _read(name: str, document_type: str) -> Document:
    resource = files(__name__).joinpath(f"data/{name}.json")
    return load_document(resource.read_bytes(), expected_type=document_type)


def load_mongodb_catalog() -> Document:
    return _read("catalog", "cxp.catalog")


def load_mongodb_declared_snapshot() -> Document:
    """The published library declaration; never a deployment observation."""
    return _read("declared-snapshot", "cxp.snapshot")


def load_mongodb_profile(name: str) -> Document:
    if name not in PROFILE_NAMES:
        message = f"Unknown MongoDB profile: {name!r}"
        raise ValueError(message)
    return _read(f"profile-{name}", "cxp.requirements")


def load_mongodb_tier(name: str) -> Document:
    if name not in TIER_NAMES:
        message = f"Unknown MongoDB tier: {name!r}"
        raise ValueError(message)
    return _read(f"tier-{name}", "cxp.requirements")


def mongodb_catalog_store() -> CatalogStore:
    return CatalogStore((load_mongodb_catalog(),))


__all__ = (
    "PROFILE_NAMES",
    "TIER_NAMES",
    "MongoCapabilityClaim",
    "MongoOperationClaim",
    "MongoSnapshotIdentity",
    "build_mongodb_snapshot",
    "load_mongodb_catalog",
    "load_mongodb_declared_snapshot",
    "load_mongodb_profile",
    "load_mongodb_tier",
    "mongodb_catalog_store",
)
