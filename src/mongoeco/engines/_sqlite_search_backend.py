from __future__ import annotations

from dataclasses import dataclass

from mongoeco.core.search import (
    SearchAutocompleteQuery,
    SearchCompoundQuery,
    SearchExistsQuery,
    SearchPhraseQuery,
    SearchQuery,
    SearchTextQuery,
    SearchVectorQuery,
    SearchWildcardQuery,
    sqlite_fts5_query,
)


@dataclass(frozen=True, slots=True)
class SQLiteSearchBackendDecision:
    backend: str
    backend_available: bool
    backend_materialized: bool
    fts5_available: bool | None
    ann_available: bool
    fts5_match: str | None
    physical_name: str | None


def supports_sqlite_fts5_search_query(query: SearchQuery) -> bool:
    return isinstance(query, (SearchTextQuery, SearchPhraseQuery, SearchAutocompleteQuery))


def decide_sqlite_search_backend(
    query: SearchQuery,
    *,
    physical_name: str | None,
    fts5_available: bool | None,
    backend_materialized: bool,
    ann_available: bool = True,
) -> SQLiteSearchBackendDecision:
    if isinstance(query, SearchVectorQuery):
        return SQLiteSearchBackendDecision(
            backend="usearch" if ann_available else "python",
            backend_available=ann_available,
            backend_materialized=backend_materialized if ann_available else False,
            fts5_available=fts5_available,
            ann_available=ann_available,
            fts5_match=None,
            physical_name=physical_name,
        )
    if (
        supports_sqlite_fts5_search_query(query)
        and physical_name
        and bool(fts5_available)
        and backend_materialized
    ):
        return SQLiteSearchBackendDecision(
            backend="fts5",
            backend_available=True,
            backend_materialized=True,
            fts5_available=fts5_available,
            ann_available=ann_available,
            fts5_match=sqlite_fts5_query(query),
            physical_name=physical_name,
        )
    return SQLiteSearchBackendDecision(
        backend="python",
        backend_available=isinstance(query, (SearchWildcardQuery, SearchExistsQuery, SearchCompoundQuery)) or bool(fts5_available),
        backend_materialized=backend_materialized,
        fts5_available=fts5_available,
        ann_available=ann_available,
        fts5_match=None,
        physical_name=physical_name,
    )
