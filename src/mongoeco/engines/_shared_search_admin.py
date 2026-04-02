from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TypeVar

from mongoeco.core.search import (
    is_text_search_query,
    SearchVectorQuery,
    SearchQuery,
    validate_search_index_definition,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import SearchIndexDefinition, SearchIndexDocument

_T = TypeVar("_T")


def build_search_index_documents(
    entries: Sequence[_T],
    *,
    get_name: Callable[[_T], str],
    get_definition: Callable[[_T], object],
    get_ready_at_epoch: Callable[[_T], float | None],
    is_ready: Callable[[float | None], bool],
    name: str | None = None,
) -> list[SearchIndexDocument]:
    from mongoeco.core.search import build_search_index_document

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


def normalize_search_index_definition(definition: SearchIndexDefinition) -> SearchIndexDefinition:
    return SearchIndexDefinition(
        validate_search_index_definition(
            definition.definition,
            index_type=definition.index_type,
        ),
        name=definition.name,
        index_type=definition.index_type,
    )


def search_index_not_found(name: str) -> OperationFailure:
    return OperationFailure(f"search index not found with name [{name}]")


def ensure_search_index_query_supported(
    definition: SearchIndexDefinition,
    query: SearchQuery,
    *,
    ready: bool,
    enforce_ready: bool = True,
) -> None:
    if enforce_ready and not ready:
        raise OperationFailure(f"search index [{query.index_name}] is not ready yet")
    if is_text_search_query(query) and definition.index_type != "search":
        raise OperationFailure(f"search index [{query.index_name}] does not support $search")
    if isinstance(query, SearchVectorQuery) and definition.index_type != "vectorSearch":
        raise OperationFailure(f"search index [{query.index_name}] does not support $vectorSearch")
