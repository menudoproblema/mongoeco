from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import TypeVar

from mongoeco.core.search import build_search_index_document
from mongoeco.engines.profiling import EngineProfiler
from mongoeco.types import SearchIndexDocument

_T = TypeVar("_T")


def merge_profile_database_names(
    names: Iterable[str],
    profiler: EngineProfiler,
) -> list[str]:
    merged = sorted(set(names))
    for db_name in list(profiler._settings.keys()):
        if db_name not in merged:
            merged.append(db_name)
    return sorted(merged)


def merge_profile_collection_names(
    names: Iterable[str],
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


def build_search_index_documents(
    entries: Sequence[_T],
    *,
    get_name: Callable[[_T], str],
    get_definition: Callable[[_T], object],
    get_ready_at_epoch: Callable[[_T], float | None],
    is_ready: Callable[[float | None], bool],
    name: str | None = None,
) -> list[SearchIndexDocument]:
    documents = [
        build_search_index_document(
            get_definition(entry),
            ready=is_ready(get_ready_at_epoch(entry)),
            ready_at_epoch=get_ready_at_epoch(entry),
        )
        for entry in sorted(entries, key=get_name)
    ]
    if name is None:
        return documents
    return [document for document in documents if document["name"] == name]
