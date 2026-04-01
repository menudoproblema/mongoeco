from __future__ import annotations

from copy import deepcopy
from typing import Any

from mongoeco.engines.profiling import EngineProfiler
from mongoeco.errors import CollectionInvalid


def merge_profile_database_names(
    names: list[str],
    profiler: EngineProfiler,
) -> list[str]:
    merged = sorted(set(names))
    for db_name in list(profiler._settings.keys()):
        if db_name not in merged:
            merged.append(db_name)
    return sorted(merged)


def merge_profile_collection_names(
    names: list[str],
    *,
    db_name: str,
    profiler: EngineProfiler,
    profile_collection_name: str,
) -> list[str]:
    merged = sorted(set(names))
    if profiler.namespace_visible(db_name):
        merged = sorted(set(merged) | {profile_collection_name})
    return merged


def profile_namespace_options(
    *,
    db_name: str,
    coll_name: str,
    profiler: EngineProfiler,
    profile_collection_name: str,
) -> dict[str, object] | None:
    if coll_name == profile_collection_name and profiler.namespace_visible(db_name):
        return {}
    return None


def namespace_database_names(
    *,
    storage: dict[str, object],
    indexes: dict[str, object],
    search_indexes: dict[str, object],
    collections: dict[str, object],
    options: dict[str, object],
) -> list[str]:
    return sorted(
        set(storage.keys())
        | set(indexes.keys())
        | set(search_indexes.keys())
        | set(collections.keys())
        | set(options.keys())
    )


def namespace_collection_names(
    db_name: str,
    *,
    storage: dict[str, dict[str, object]],
    indexes: dict[str, dict[str, object]],
    search_indexes: dict[str, dict[str, object]],
    collections: dict[str, set[str]],
) -> list[str]:
    return sorted(
        set(storage.get(db_name, {}).keys())
        | set(indexes.get(db_name, {}).keys())
        | set(search_indexes.get(db_name, {}).keys())
        | set(collections.get(db_name, set()))
    )


def namespace_exists(
    db_name: str,
    coll_name: str,
    *,
    storage: dict[str, dict[str, object]],
    indexes: dict[str, dict[str, object]],
    search_indexes: dict[str, dict[str, object]],
    collections: dict[str, set[str]],
) -> bool:
    return (
        coll_name in collections.get(db_name, set())
        or coll_name in storage.get(db_name, {})
        or coll_name in indexes.get(db_name, {})
        or coll_name in search_indexes.get(db_name, {})
    )


def namespace_collection_options(
    db_name: str,
    coll_name: str,
    *,
    storage: dict[str, dict[str, Any]],
    indexes: dict[str, dict[str, Any]],
    search_indexes: dict[str, dict[str, Any]],
    collections: dict[str, set[str]],
    options: dict[str, dict[str, dict[str, object]]],
    profiler: EngineProfiler,
    profile_collection_name: str,
) -> dict[str, object]:
    profile_options = profile_namespace_options(
        db_name=db_name,
        coll_name=coll_name,
        profiler=profiler,
        profile_collection_name=profile_collection_name,
    )
    if profile_options is not None:
        return profile_options
    if not namespace_exists(
        db_name,
        coll_name,
        storage=storage,
        indexes=indexes,
        search_indexes=search_indexes,
        collections=collections,
    ):
        raise CollectionInvalid(f"collection '{coll_name}' does not exist")
    return deepcopy(options.get(db_name, {}).get(coll_name, {}))
