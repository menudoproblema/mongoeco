from __future__ import annotations

from types import MappingProxyType

from mongoeco.compat._catalog_constants import PYMONGO_CAP_UPDATE_ONE_SORT
from mongoeco.compat._catalog_models import PyMongoProfileCatalogEntry

PYMONGO_PROFILE_CATALOG = MappingProxyType(
    {
        "4.9": PyMongoProfileCatalogEntry(
            key="4.9",
            driver_series="4.x",
            label="PyMongo 4.9",
            aliases=("4", "4.9"),
            behavior_flags=MappingProxyType({"supports_update_one_sort": False}),
        ),
        "4.11": PyMongoProfileCatalogEntry(
            key="4.11",
            driver_series="4.x",
            label="PyMongo 4.11",
            aliases=("4.11",),
            behavior_flags=MappingProxyType({"supports_update_one_sort": True}),
            capabilities=frozenset({PYMONGO_CAP_UPDATE_ONE_SORT}),
        ),
        "4.13": PyMongoProfileCatalogEntry(
            key="4.13",
            driver_series="4.x",
            label="PyMongo 4.13",
            aliases=("4.13",),
            behavior_flags=MappingProxyType({"supports_update_one_sort": True}),
            capabilities=frozenset({PYMONGO_CAP_UPDATE_ONE_SORT}),
        ),
    }
)

PYMONGO_PROFILE_ALIASES = MappingProxyType(
    {alias: entry.key for entry in PYMONGO_PROFILE_CATALOG.values() for alias in entry.aliases}
)

SUPPORTED_PYMONGO_MAJORS = frozenset(int(key.split(".", 1)[0]) for key in PYMONGO_PROFILE_CATALOG)
