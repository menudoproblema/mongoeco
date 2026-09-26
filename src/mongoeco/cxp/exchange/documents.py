"""Load Mongoeco-owned exchange documents from packaged JSON data."""

from __future__ import annotations

from importlib.resources import files

from cxp.exchange import CatalogStore, Document, load_document


PROFILE_NAMES = (
    "mongodb-core",
    "mongodb-text-search",
    "mongodb-search",
    "mongodb-platform",
    "mongodb-aggregate-rich",
    "mongodb-mock-safe",
)
TIER_NAMES = ("core", "search", "platform")


def _read(name: str, document_type: str) -> Document:
    resource = files("mongoeco.cxp.exchange").joinpath(f"data/{name}.json")
    return load_document(resource.read_bytes(), expected_type=document_type)


def load_mongodb_catalog() -> Document:
    """Load the exact producer-owned catalog."""
    return _read("catalog", "cxp.catalog")


def load_mongodb_declared_snapshot() -> Document:
    """Load the library declaration, never a deployment observation."""
    return _read("declared-snapshot", "cxp.snapshot")


def load_mongodb_profile(name: str) -> Document:
    """Load one exact producer-owned profile by its public name."""
    if name not in PROFILE_NAMES:
        message = f"Unknown MongoDB profile: {name!r}"
        raise ValueError(message)
    return _read(f"profile-{name}", "cxp.requirements")


def load_mongodb_tier(name: str) -> Document:
    """Load one exact producer-owned tier by its public name."""
    if name not in TIER_NAMES:
        message = f"Unknown MongoDB tier: {name!r}"
        raise ValueError(message)
    return _read(f"tier-{name}", "cxp.requirements")


def mongodb_catalog_store() -> CatalogStore:
    """Resolve only the packaged producer catalog."""
    return CatalogStore((load_mongodb_catalog(),))
