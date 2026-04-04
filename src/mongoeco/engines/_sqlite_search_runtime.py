from __future__ import annotations

from copy import deepcopy
import heapq
import math
import sqlite3
import time
from typing import Any, Protocol

from mongoeco.api.operations import FindOperation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.search_filter_prefilter import evaluate_candidate_filter, flatten_candidate_filter_clauses
from mongoeco.core.search import (
    MaterializedSearchDocument,
    build_search_index_document,
    compile_search_stage,
    is_text_search_query,
    iter_searchable_text_entries,
    materialize_search_document,
    matches_search_query,
    search_compound_ranking,
    search_near_distance,
    score_vector_document,
    search_query_explain_details,
    search_query_operator_name,
    sqlite_fts5_query,
    tokenize_classic_text,
    SearchAutocompleteQuery,
    SearchCompoundQuery,
    SearchEqualsQuery,
    SearchExistsQuery,
    SearchInQuery,
    SearchNearQuery,
    SearchPhraseQuery,
    SearchQuery,
    SearchRangeQuery,
    SearchTextQuery,
    SearchVectorQuery,
    SearchWildcardQuery,
    vector_field_paths,
)
from mongoeco.engines._sqlite_catalog import load_search_index_rows as _sqlite_load_search_index_rows
from mongoeco.engines._sqlite_compound_prefilter import (
    clause_search_paths as _compound_clause_search_paths,
    compound_clause_candidate_state as _compound_clause_candidate_state,
    describe_compound_prefilter as _describe_compound_prefilter,
    document_for_search_path as _compound_document_for_search_path,
    downstream_filter_implies_clause as _compound_downstream_filter_implies_clause,
    sqlite_compound_candidate_plan as _sqlite_compound_candidate_plan,
    textual_search_field_types as _compound_textual_search_field_types,
)
from mongoeco.engines._sqlite_compound_ranking import (
    exact_candidateable_should_scores as _compound_exact_candidateable_should_scores,
    load_materialized_search_documents_by_storage_key as _compound_load_materialized_search_documents_by_storage_key,
    load_search_entries_by_storage_key as _compound_load_search_entries_by_storage_key,
    materialized_search_document_from_entries as _compound_materialized_search_document_from_entries,
    materialized_search_entry_cache_bucket as _compound_materialized_search_entry_cache_bucket,
    prefilter_candidate_storage_keys_by_matched_should as _compound_prefilter_candidate_storage_keys_by_matched_should,
    prune_candidate_storage_keys_for_topk as _compound_prune_candidate_storage_keys_for_topk,
    prune_candidate_storage_keys_with_candidateable_ranking as _compound_prune_candidate_storage_keys_with_candidateable_ranking,
    rank_compound_candidate_storage_keys_from_entries as _compound_rank_candidate_storage_keys_from_entries,
    sort_search_documents_for_query as _compound_sort_search_documents_for_query,
)
from mongoeco.engines._sqlite_search_admin import (
    create_search_index as _sqlite_create_search_index,
    drop_search_index as _sqlite_drop_search_index,
    list_search_index_documents as _sqlite_list_search_index_documents,
    update_search_index as _sqlite_update_search_index,
)
from mongoeco.engines._sqlite_search_backend import decide_sqlite_search_backend
from mongoeco.engines._sqlite_vector_backend import (
    SQLiteVectorBackendState,
    build_sqlite_vector_backend,
    search_sqlite_vector_backend,
    vector_backend_stats_document,
    vector_filter_candidate_storage_keys,
)
from mongoeco.engines._sqlite_read_ops import search_documents as _sqlite_search_documents
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import Document, QueryPlanExplanation, SearchIndexDefinition, SearchIndexDocument

class _SQLiteSearchRuntimeEngine(Protocol):
    _simulate_search_index_latency: float
    _ensured_search_backends: set[str]
    _vector_search_backends: dict[tuple[str, str], SQLiteVectorBackendState]
    _materialized_search_entry_cache: dict[
        tuple[str, str, str, int],
        dict[str, tuple[tuple[tuple[str, str], ...], MaterializedSearchDocument]],
    ]

    def _load_search_index_rows(
        self,
        db_name: str,
        coll_name: str,
        *,
        name: str | None = None,
    ) -> list[tuple[SearchIndexDefinition, str | None, float | None]]: ...
    def _require_connection(self, context: ClientSession | None = None) -> sqlite3.Connection: ...
    def _quote_identifier(self, identifier: str) -> str: ...
    def _load_documents(self, db_name: str, coll_name: str) -> list[tuple[str, Document]]: ...
    def _load_documents_by_storage_keys(
        self,
        db_name: str,
        coll_name: str,
        storage_keys: list[str],
    ) -> dict[str, Document]: ...
    def _sqlite_table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool: ...
    def _supports_fts5(self, conn: sqlite3.Connection) -> bool: ...
    def _physical_search_index_name(self, db_name: str, coll_name: str, index_name: str) -> str: ...
    def _search_backend_version(self, db_name: str, coll_name: str) -> int: ...
    def _mark_search_backend_changed(self, db_name: str, coll_name: str) -> None: ...
    def _ensure_collection_row(self, conn: sqlite3.Connection, db_name: str, coll_name: str) -> None: ...
    def _begin_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None: ...
    def _commit_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None: ...
    def _rollback_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None: ...
    def _bind_connection(self, conn: sqlite3.Connection) -> Any: ...


def _vector_filter_residual_description(
    filter_spec: dict[str, object] | None,
    vector_filter_description: dict[str, object] | None,
) -> dict[str, object] | None:
    if filter_spec is None:
        return None
    if vector_filter_description is None:
        return {
            "required": True,
            "reason": "filter-not-candidateable",
            "candidateable": False,
            "exact": False,
            "spec": deepcopy(filter_spec),
        }
    residual_required = not bool(vector_filter_description.get("exact"))
    return {
        "required": residual_required,
        "reason": (
            None
            if not residual_required
            else "unsupported-clauses"
            if int(vector_filter_description.get("unsupportedClauseCount", 0)) > 0
            else "non-exact-prefilter"
        ),
        "candidateable": bool(vector_filter_description.get("candidateable")),
        "exact": bool(vector_filter_description.get("exact")),
        "supportedClauseCount": int(vector_filter_description.get("supportedClauseCount", 0)),
        "unsupportedClauseCount": int(vector_filter_description.get("unsupportedClauseCount", 0)),
        "spec": deepcopy(filter_spec),
    }


def load_search_index_rows(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    *,
    name: str | None = None,
) -> list[tuple[SearchIndexDefinition, str | None, float | None]]:
    return _sqlite_load_search_index_rows(
        engine._require_connection(),
        db_name,
        coll_name,
        name=name,
    )


def load_search_indexes(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
) -> list[SearchIndexDefinition]:
    return [
        definition
        for definition, _physical_name, _ready_at_epoch in engine._load_search_index_rows(
            db_name,
            coll_name,
        )
    ]


def pending_search_index_ready_at(engine: _SQLiteSearchRuntimeEngine) -> float | None:
    if engine._simulate_search_index_latency <= 0:
        return None
    return time.time() + engine._simulate_search_index_latency


def search_index_is_ready_sync(ready_at_epoch: float | None) -> bool:
    return ready_at_epoch is None or time.time() >= ready_at_epoch


def drop_search_backend_sync(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    physical_name: str | None,
) -> None:
    if not physical_name:
        return
    conn.execute(f"DROP TABLE IF EXISTS {engine._quote_identifier(physical_name)}")
    engine._ensured_search_backends.discard(physical_name)
    stale_keys = [
        key
        for key, state in engine._vector_search_backends.items()
        if state.physical_name == physical_name
    ]
    for key in stale_keys:
        engine._vector_search_backends.pop(key, None)


def ensure_search_backend_sync(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    physical_name: str | None,
) -> str | None:
    resolved_physical_name = physical_name or engine._physical_search_index_name(
        db_name,
        coll_name,
        definition.name,
    )
    if definition.index_type == "vectorSearch":
        return resolved_physical_name
    if definition.index_type != "search":
        return None
    if not engine._supports_fts5(conn):
        return resolved_physical_name
    if (
        resolved_physical_name in engine._ensured_search_backends
        and engine._sqlite_table_exists(conn, resolved_physical_name)
    ):
        return resolved_physical_name
    had_transaction = conn.in_transaction
    conn.execute(
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {engine._quote_identifier(resolved_physical_name)}
        USING fts5(storage_key UNINDEXED, field_path UNINDEXED, content, tokenize='unicode61')
        """
    )
    conn.execute(f"DELETE FROM {engine._quote_identifier(resolved_physical_name)}")
    rows: list[tuple[str, str, str]] = []
    for storage_key, document in engine._load_documents(db_name, coll_name):
        rows.extend(
            (storage_key, field_path, content)
            for field_path, content in iter_searchable_text_entries(document, definition)
        )
    if rows:
        conn.executemany(
            f"""
            INSERT INTO {engine._quote_identifier(resolved_physical_name)} (
                storage_key, field_path, content
            ) VALUES (?, ?, ?)
            """,
            rows,
        )
    engine._ensured_search_backends.add(resolved_physical_name)
    if not had_transaction and conn.in_transaction:
        conn.commit()
    return resolved_physical_name


def _load_candidate_documents(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    storage_keys: list[str],
) -> list[tuple[str, Document]]:
    if not storage_keys:
        return []
    documents = engine._load_documents_by_storage_keys(db_name, coll_name, storage_keys)
    return [
        (storage_key, documents[storage_key])
        for storage_key in storage_keys
        if storage_key in documents
    ]


def _intersect_storage_key_lists(lists: list[list[str]]) -> list[str]:
    if not lists:
        return []
    allowed = set(lists[0])
    for values in lists[1:]:
        allowed &= set(values)
    return [storage_key for storage_key in lists[0] if storage_key in allowed]


def _union_storage_key_lists(lists: list[list[str]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for values in lists:
        for storage_key in values:
            if storage_key in seen:
                continue
            seen.add(storage_key)
            ordered.append(storage_key)
    return ordered


def _storage_keys_with_minimum_frequency(
    lists: list[list[str]],
    minimum: int,
) -> list[str]:
    if minimum <= 1:
        return _union_storage_key_lists(lists)
    counts: dict[str, int] = {}
    order: dict[str, int] = {}
    for values in lists:
        for storage_key in values:
            counts[storage_key] = counts.get(storage_key, 0) + 1
            order.setdefault(storage_key, len(order))
    return [
        storage_key
        for storage_key, count in sorted(counts.items(), key=lambda item: order[item[0]])
        if count >= minimum
    ]


def _sqlite_leaf_candidate_storage_keys(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    physical_name: str,
    query: SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchExistsQuery,
) -> tuple[list[str], str, bool]:
    sql: str
    params: list[object]
    if isinstance(query, (SearchTextQuery, SearchPhraseQuery, SearchAutocompleteQuery)):
        sql = (
            f"SELECT DISTINCT storage_key FROM {engine._quote_identifier(physical_name)} "
            "WHERE content MATCH ?"
        )
        params = [sqlite_fts5_query(query)]
        backend = "fts5"
        exact = not isinstance(query, SearchPhraseQuery)
    elif isinstance(query, SearchWildcardQuery):
        sql = (
            f"SELECT DISTINCT storage_key FROM {engine._quote_identifier(physical_name)} "
            "WHERE lower(content) GLOB ?"
        )
        params = [query.normalized_pattern]
        backend = "fts5-glob"
        exact = True
    else:
        sql = f"SELECT DISTINCT storage_key FROM {engine._quote_identifier(physical_name)}"
        params = []
        backend = "fts5-path"
        exact = True
    if query.paths is not None:
        placeholders = ", ".join("?" for _ in query.paths)
        clause = f"field_path IN ({placeholders})"
        sql = f"{sql} WHERE {clause}" if " WHERE " not in sql else f"{sql} AND {clause}"
        params.extend(query.paths)
    return [row[0] for row in conn.execute(sql, tuple(params)).fetchall()], backend, exact


def _sqlite_candidate_storage_keys_for_query(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    physical_name: str | None,
    query: SearchQuery,
    definition: SearchIndexDefinition | None = None,
) -> tuple[list[str] | None, str | None, bool]:
    if physical_name is None or not engine._sqlite_table_exists(conn, physical_name):
        return None, None, False
    if isinstance(query, (SearchTextQuery, SearchPhraseQuery, SearchAutocompleteQuery, SearchWildcardQuery, SearchExistsQuery)):
        return _sqlite_leaf_candidate_storage_keys(
            engine,
            conn,
            physical_name=physical_name,
            query=query,
        )
    if isinstance(query, SearchCompoundQuery):
        if definition is None:
            return None, None, False
        candidates, backend, exact, _should_candidates, _non_candidateable = _sqlite_compound_candidate_state(
            engine,
            conn,
            physical_name=physical_name,
            definition=definition,
            query=query,
        )
        return candidates, backend, exact
    if isinstance(query, (SearchInQuery, SearchEqualsQuery, SearchRangeQuery, SearchNearQuery)):
        return None, None, False
    return None, None, False


def _sqlite_compound_candidate_state(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    physical_name: str | None,
    definition: SearchIndexDefinition,
    query: SearchCompoundQuery,
    downstream_filter_storage_keys: list[str] | None = None,
    downstream_filter_spec: dict[str, object] | None = None,
    downstream_filter_exact: bool = False,
) -> tuple[list[str] | None, str | None, bool, list[list[str]], int]:
    plan = _sqlite_compound_candidate_plan(
        definition,
        query,
        candidate_resolver=lambda clause: _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=physical_name,
            query=clause,
        ),
        must_not_resolver=lambda clause: _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=physical_name,
            query=clause,
            definition=definition,
        ),
        downstream_filter_storage_keys=downstream_filter_storage_keys,
        downstream_filter_spec=downstream_filter_spec,
        downstream_filter_exact=downstream_filter_exact,
    )
    return plan.to_legacy_tuple()


def _textual_search_field_types(definition: SearchIndexDefinition) -> dict[str, str]:
    return _compound_textual_search_field_types(definition)


def _flatten_downstream_filter_clauses(filter_spec: dict[str, object]) -> list[tuple[str, object]] | None:
    clauses = flatten_candidate_filter_clauses(filter_spec)
    return list(clauses) if clauses is not None else None


def _document_for_search_path(path: str, value: object) -> Document:
    return _compound_document_for_search_path(path, value)


def _clause_search_paths(clause: SearchQuery) -> tuple[str, ...] | None:
    return _compound_clause_search_paths(clause)


def _downstream_filter_implies_clause(
    clause: SearchQuery,
    *,
    definition: SearchIndexDefinition,
    filter_spec: dict[str, object] | None,
) -> tuple[str | None, list[object] | None]:
    return _compound_downstream_filter_implies_clause(
        clause,
        definition=definition,
        filter_spec=filter_spec,
    )


def _sqlite_candidate_state_for_compound_clause(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    physical_name: str | None,
    definition: SearchIndexDefinition,
    clause: SearchQuery,
    downstream_filter_storage_keys: list[str] | None,
    downstream_filter_spec: dict[str, object] | None,
    downstream_filter_exact: bool,
) -> tuple[list[str] | None, str | None, bool, dict[str, object] | None]:
    storage_keys, backend, clause_exact, refinement = _compound_clause_candidate_state(
        definition=definition,
        clause=clause,
        candidate_resolver=lambda query: _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=physical_name,
            query=query,
        ),
        downstream_filter_storage_keys=downstream_filter_storage_keys,
        downstream_filter_spec=downstream_filter_spec,
        downstream_filter_exact=downstream_filter_exact,
    )
    return storage_keys, backend, clause_exact, refinement.to_dict() if refinement is not None else None


def _sqlite_candidate_storage_keys_for_downstream_filter(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    physical_name: str | None,
    definition: SearchIndexDefinition,
    filter_spec: dict[str, object] | None,
) -> tuple[list[str] | None, dict[str, object] | None]:
    if physical_name is None or filter_spec is None or not engine._sqlite_table_exists(conn, physical_name):
        return None, None
    field_types = _textual_search_field_types(definition)
    candidate_result = evaluate_candidate_filter(
        filter_spec,
        all_candidates=(),
        clause_resolver=lambda path, clause: _candidate_storage_keys_for_downstream_clause(
            engine,
            conn,
            physical_name,
            path=path,
            clause=clause,
            field_types=field_types,
        ),
    )
    if candidate_result is None:
        return None, {
            "spec": deepcopy(filter_spec),
            "candidateable": False,
            "exact": False,
            "backend": None,
            "supportedPaths": [],
            "supportedClauseCount": 0,
            "unsupportedClauseCount": 1,
            "supportedOperators": [],
            "booleanShape": None,
        }
    if candidate_result.matches is None:
        return None, {
            "spec": deepcopy(filter_spec),
            "candidateable": False,
            "exact": False,
            "backend": None,
            "supportedPaths": list(candidate_result.plan.supported_paths),
            "supportedClauseCount": candidate_result.plan.supported_clause_count,
            "unsupportedClauseCount": candidate_result.plan.unsupported_clause_count,
            "supportedOperators": list(candidate_result.plan.supported_operators),
            "booleanShape": candidate_result.plan.shape,
        }
    return list(candidate_result.matches), candidate_result.to_metadata(backend="fts5-downstream-filter")


def _candidate_storage_keys_for_downstream_filter_node(
    engine: _SQLiteSearchRuntimeEngine | None,
    conn: sqlite3.Connection | None,
    physical_name: str | None,
    *,
    filter_spec: dict[str, object],
    field_types: dict[str, str],
    return_clauses_only: bool = False,
) -> dict[str, object] | None:
    clauses = flatten_candidate_filter_clauses(filter_spec)
    if clauses is None:
        return None
    if return_clauses_only:
        return {
            "clauses": list(clauses),
            "keys": None,
            "supported_paths": [],
            "supported_operators": [],
            "supported_clause_count": 0,
            "unsupported_clause_count": 0,
            "exact": True,
            "shape": None,
        }
    all_candidates: tuple[str, ...] = ()
    result = evaluate_candidate_filter(
        filter_spec,
        all_candidates=all_candidates,
        clause_resolver=lambda path, clause: _candidate_storage_keys_for_downstream_clause(
            engine,
            conn,
            physical_name,
            path=path,
            clause=clause,
            field_types=field_types,
        ),
    )
    if result is None:
        return None
    plan = result.plan
    return {
        "clauses": list(clauses),
        "keys": list(result.matches) if result.matches is not None else None,
        "supported_paths": list(plan.supported_paths),
        "supported_operators": list(plan.supported_operators),
        "supported_clause_count": plan.supported_clause_count,
        "unsupported_clause_count": plan.unsupported_clause_count,
        "exact": plan.exact,
        "shape": plan.shape,
    }


def _candidate_storage_keys_for_downstream_clause(
    engine: _SQLiteSearchRuntimeEngine | None,
    conn: sqlite3.Connection | None,
    physical_name: str | None,
    *,
    path: str,
    clause: object,
    field_types: dict[str, str],
) -> tuple[list[str] | None, str]:
    mapping_type = field_types.get(path)
    if mapping_type not in {"string", "autocomplete", "token"}:
        return None, "eq"
    if engine is None or conn is None or physical_name is None:
        return None, "eq"
    sql: str
    params: list[object]
    if isinstance(clause, str):
        sql = (
            f"SELECT DISTINCT storage_key FROM {engine._quote_identifier(physical_name)} "
            "WHERE field_path = ? AND lower(content) = ?"
        )
        params = [path, clause.lower()]
        operator_name = "eq"
    elif isinstance(clause, dict) and set(clause) == {"$in"} and isinstance(clause["$in"], list):
        values = [item.lower() for item in clause["$in"] if isinstance(item, str)]
        if not values or len(values) != len(clause["$in"]):
            return None, "$in"
        placeholders = ", ".join("?" for _ in values)
        sql = (
            f"SELECT DISTINCT storage_key FROM {engine._quote_identifier(physical_name)} "
            f"WHERE field_path = ? AND lower(content) IN ({placeholders})"
        )
        params = [path, *values]
        operator_name = "$in"
    elif isinstance(clause, dict) and clause == {"$exists": True}:
        sql = (
            f"SELECT DISTINCT storage_key FROM {engine._quote_identifier(physical_name)} "
            "WHERE field_path = ?"
        )
        params = [path]
        operator_name = "$exists"
    else:
        return None, "eq"
    return [row[0] for row in conn.execute(sql, tuple(params)).fetchall()], operator_name


def _describe_compound_prefilter_sync(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    physical_name: str | None,
    definition: SearchIndexDefinition,
    query: SearchCompoundQuery,
    downstream_filter: dict[str, object] | None = None,
    downstream_filter_storage_keys: list[str] | None = None,
) -> dict[str, object]:
    return _describe_compound_prefilter(
        definition,
        query,
        candidate_resolver=lambda clause: _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=physical_name,
            query=clause,
            definition=definition,
        ),
        physical_name=physical_name,
        downstream_filter=downstream_filter,
        downstream_filter_storage_keys=downstream_filter_storage_keys,
    )


def _compound_clause_class(description: dict[str, object]) -> str:
    if not bool(description.get("candidateable")):
        return "post-match-only"
    if bool(description.get("exact")):
        return "candidateable-exact"
    return "candidateable-ranking"


def _prune_candidate_storage_keys_for_topk(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str | None,
    query: SearchQuery,
    candidate_storage_keys: list[str] | None,
    candidate_exact: bool,
    result_limit_hint: int | None,
    should_candidates: list[list[str]] | None = None,
) -> tuple[list[str] | None, dict[str, object] | None]:
    return _compound_prune_candidate_storage_keys_for_topk(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        query=query,
        candidate_storage_keys=candidate_storage_keys,
        candidate_exact=candidate_exact,
        result_limit_hint=result_limit_hint,
        should_candidates=should_candidates,
        candidate_resolver=lambda clause: _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=physical_name,
            query=clause,
            definition=None,
        ),
    )


def _prune_candidate_storage_keys_with_candidateable_ranking(
    candidate_storage_keys: list[str],
    should_candidates: list[list[str]],
    *,
    result_limit_hint: int,
) -> tuple[list[str], int | None] | None:
    return _compound_prune_candidate_storage_keys_with_candidateable_ranking(
        candidate_storage_keys,
        should_candidates,
        result_limit_hint=result_limit_hint,
    )


def _prefilter_candidate_storage_keys_by_matched_should(
    candidate_storage_keys: list[str],
    should_candidates: list[list[str]],
    *,
    result_limit_hint: int,
) -> tuple[list[str] | None, int | None]:
    return _compound_prefilter_candidate_storage_keys_by_matched_should(
        candidate_storage_keys,
        should_candidates,
        result_limit_hint=result_limit_hint,
    )


def _exact_candidateable_should_scores(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str | None,
    query: SearchCompoundQuery,
    candidate_storage_keys: list[str],
) -> dict[str, dict[str, float]] | None:
    if physical_name is None or not candidate_storage_keys or any(
        isinstance(clause, (SearchNearQuery, SearchCompoundQuery))
        for clause in query.should
    ):
        return None
    if not engine._sqlite_table_exists(conn, physical_name):
        return None
    prepared_by_storage_key = _load_materialized_search_documents_by_storage_key(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        storage_keys=candidate_storage_keys,
    )
    if not prepared_by_storage_key:
        return None
    return _compound_exact_candidateable_should_scores(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        query=query,
        candidate_storage_keys=candidate_storage_keys,
    )


def _load_search_entries_by_storage_key(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
    storage_keys: list[str],
) -> dict[str, tuple[tuple[str, str], ...]]:
    return _compound_load_search_entries_by_storage_key(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        storage_keys=storage_keys,
    )


def _load_materialized_search_documents_by_storage_key(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
    storage_keys: list[str],
) -> dict[str, MaterializedSearchDocument]:
    return _compound_load_materialized_search_documents_by_storage_key(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        storage_keys=storage_keys,
    )


def _materialized_search_entry_cache_bucket(
    engine: _SQLiteSearchRuntimeEngine,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
) -> dict[str, tuple[tuple[tuple[str, str], ...], MaterializedSearchDocument]]:
    return _compound_materialized_search_entry_cache_bucket(
        engine,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
    )


def _materialized_search_document_from_entries(
    entries: list[tuple[str, str]] | tuple[tuple[str, str], ...],
) -> MaterializedSearchDocument:
    return _compound_materialized_search_document_from_entries(entries)


def _sort_search_documents_for_query(
    documents: list[tuple[Document, SearchIndexDefinition, object | None]],
    *,
    query: SearchQuery,
) -> list[Document]:
    return _compound_sort_search_documents_for_query(documents, query=query)


def _compound_entry_ranking_supported(
    query: SearchCompoundQuery,
    *,
    physical_name: str | None,
) -> bool:
    from mongoeco.engines._sqlite_compound_prefilter import compound_entry_ranking_supported

    return compound_entry_ranking_supported(query, physical_name=physical_name)


def _rank_compound_candidate_storage_keys_from_entries(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
    query: SearchCompoundQuery,
    candidate_storage_keys: list[str],
    result_limit_hint: int | None,
) -> list[str] | None:
    return _compound_rank_candidate_storage_keys_from_entries(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        query=query,
        candidate_storage_keys=candidate_storage_keys,
        result_limit_hint=result_limit_hint,
    )


def _next_vector_candidate_request(
    current_request: int,
    *,
    matched_count: int,
    limit: int,
    max_requested: int,
) -> int:
    if matched_count <= 0:
        return min(max_requested, max(current_request + 1, current_request * 4))
    estimated = math.ceil((current_request * limit) / matched_count)
    safety_margin = max(1, limit - matched_count)
    return min(max_requested, max(current_request + 1, estimated + safety_margin))


def ensure_vector_search_backend_sync(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    physical_name: str,
    path: str,
) -> SQLiteVectorBackendState:
    cache_key = (physical_name, path)
    collection_version = engine._search_backend_version(db_name, coll_name)
    cached = engine._vector_search_backends.get(cache_key)
    if cached is not None and cached.collection_version == collection_version:
        return cached
    state = build_sqlite_vector_backend(
        db_name=db_name,
        coll_name=coll_name,
        definition=definition,
        physical_name=physical_name,
        path=path,
        collection_version=collection_version,
        documents=list(engine._load_documents(db_name, coll_name)),
    )
    engine._vector_search_backends[cache_key] = state
    return state


def delete_search_entries_for_storage_key(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    storage_key: str,
    search_indexes: list[tuple[SearchIndexDefinition, str | None, float | None]] | None = None,
) -> None:
    rows = search_indexes if search_indexes is not None else engine._load_search_index_rows(
        db_name,
        coll_name,
    )
    for definition, physical_name, _ready_at_epoch in rows:
        if definition.index_type != "search" or not physical_name:
            continue
        if not engine._sqlite_table_exists(conn, physical_name):
            continue
        conn.execute(
            f"DELETE FROM {engine._quote_identifier(physical_name)} WHERE storage_key = ?",
            (storage_key,),
        )
    engine._mark_search_backend_changed(db_name, coll_name)


def replace_search_entries_for_document(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    storage_key: str,
    document: Document,
    search_indexes: list[tuple[SearchIndexDefinition, str | None, float | None]] | None = None,
) -> None:
    rows = search_indexes if search_indexes is not None else engine._load_search_index_rows(
        db_name,
        coll_name,
    )
    delete_search_entries_for_storage_key(
        engine,
        conn,
        db_name,
        coll_name,
        storage_key,
        search_indexes=rows,
    )
    for definition, physical_name, _ready_at_epoch in rows:
        if definition.index_type != "search":
            continue
        resolved_physical_name = physical_name
        if resolved_physical_name is not None and not engine._sqlite_table_exists(conn, resolved_physical_name):
            resolved_physical_name = ensure_search_backend_sync(
                engine,
                conn,
                db_name,
                coll_name,
                definition,
                resolved_physical_name,
            )
        if not resolved_physical_name or not engine._sqlite_table_exists(conn, resolved_physical_name):
            continue
        entries = iter_searchable_text_entries(document, definition)
        if not entries:
            continue
        conn.executemany(
            f"""
            INSERT INTO {engine._quote_identifier(resolved_physical_name)} (
                storage_key, field_path, content
            ) VALUES (?, ?, ?)
            """,
            [(storage_key, field_path, content) for field_path, content in entries],
        )


def create_search_index_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    max_time_ms: int | None,
    context: ClientSession | None,
) -> str:
    deadline = operation_deadline(max_time_ms)
    conn = engine._require_connection(context)
    result = _sqlite_create_search_index(
        conn,
        db_name=db_name,
        coll_name=coll_name,
        definition=definition,
        deadline=deadline,
        begin_write=lambda current: engine._begin_write(current, context),
        ensure_collection_row=engine._ensure_collection_row,
        commit_write=lambda current: engine._commit_write(current, context),
        rollback_write=lambda current: engine._rollback_write(current, context),
        ensure_search_backend=lambda current, current_db, current_coll, current_definition, current_physical: ensure_search_backend_sync(
            engine,
            current,
            current_db,
            current_coll,
            current_definition,
            current_physical,
        ),
        physical_search_index_name=engine._physical_search_index_name,
        pending_ready_at=lambda: pending_search_index_ready_at(engine),
    )
    engine._mark_search_backend_changed(db_name, coll_name)
    return result


def list_search_indexes_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    name: str | None,
    context: ClientSession | None,
) -> list[SearchIndexDocument]:
    conn = engine._require_connection(context)
    with engine._bind_connection(conn):
        rows = engine._load_search_index_rows(db_name, coll_name)
    return _sqlite_list_search_index_documents(
        rows,
        is_ready=search_index_is_ready_sync,
        name=name,
    )


def update_search_index_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    name: str,
    definition: Document,
    max_time_ms: int | None,
    context: ClientSession | None,
) -> None:
    deadline = operation_deadline(max_time_ms)
    conn = engine._require_connection(context)
    _sqlite_update_search_index(
        conn,
        db_name=db_name,
        coll_name=coll_name,
        name=name,
        definition=definition,
        deadline=deadline,
        begin_write=lambda current: engine._begin_write(current, context),
        commit_write=lambda current: engine._commit_write(current, context),
        rollback_write=lambda current: engine._rollback_write(current, context),
        drop_search_backend=lambda current, current_physical: drop_search_backend_sync(engine, current, current_physical),
        ensure_search_backend=lambda current, current_db, current_coll, current_definition, current_physical: ensure_search_backend_sync(
            engine,
            current,
            current_db,
            current_coll,
            current_definition,
            current_physical,
        ),
        physical_search_index_name=engine._physical_search_index_name,
        pending_ready_at=lambda: pending_search_index_ready_at(engine),
    )
    engine._mark_search_backend_changed(db_name, coll_name)


def drop_search_index_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    name: str,
    max_time_ms: int | None,
    context: ClientSession | None,
) -> None:
    deadline = operation_deadline(max_time_ms)
    conn = engine._require_connection(context)
    _sqlite_drop_search_index(
        conn,
        db_name=db_name,
        coll_name=coll_name,
        name=name,
        deadline=deadline,
        begin_write=lambda current: engine._begin_write(current, context),
        commit_write=lambda current: engine._commit_write(current, context),
        rollback_write=lambda current: engine._rollback_write(current, context),
        drop_search_backend=lambda current, current_physical: drop_search_backend_sync(engine, current, current_physical),
    )
    engine._mark_search_backend_changed(db_name, coll_name)


def exact_vector_hits_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    query: SearchVectorQuery,
    *,
    candidate_storage_keys: list[str] | None = None,
    skip_filter_match: bool = False,
) -> list[tuple[float, Document]]:
    vector_hits: list[tuple[float, Document]] = []
    if candidate_storage_keys is not None:
        documents = [
            (storage_key, document)
            for storage_key, document in engine._load_documents_by_storage_keys(
                db_name,
                coll_name,
                candidate_storage_keys,
            ).items()
        ]
    else:
        documents = engine._load_documents(db_name, coll_name)
    for _, document in documents:
        if not skip_filter_match and query.filter_spec is not None and not QueryEngine.match(
            document,
            query.filter_spec,
            dialect=MONGODB_DIALECT_70,
        ):
            continue
        score = score_vector_document(
            document,
            definition=definition,
            query=query,
        )
        if score is None:
            continue
        if query.min_score is not None and score < query.min_score:
            continue
        vector_hits.append((score, document))
    vector_hits.sort(key=lambda item: item[0], reverse=True)
    return vector_hits


def _sqlite_vector_candidate_documents(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    query: SearchVectorQuery,
    backend_state: SQLiteVectorBackendState,
    prefilter_storage_keys: list[str] | None,
    prefilter_exact: bool,
) -> tuple[list[Document], int, int, int, int, str | None]:
    if prefilter_storage_keys is not None and prefilter_exact and not prefilter_storage_keys:
        return [], 0, 0, 0, 0, None
    requested = max(query.limit, query.num_candidates)
    max_requested = min(
        backend_state.valid_vectors,
        max(requested, query.limit * 4),
    )
    matched_documents: list[Document] = []
    documents_filtered = 0
    documents_filtered_by_min_score = 0
    candidates_evaluated = 0
    exact_fallback_reason: str | None = None
    seen_storage_keys: set[str] = set()
    allowed_storage_keys = set(prefilter_storage_keys) if prefilter_storage_keys is not None else None
    current_request = requested

    while current_request > 0:
        ann_hits = search_sqlite_vector_backend(
            backend_state,
            query_vector=query.query_vector,
            count=current_request,
        )
        new_storage_keys = [
            storage_key
            for storage_key, _distance in ann_hits
            if storage_key not in seen_storage_keys
            and (allowed_storage_keys is None or storage_key in allowed_storage_keys)
        ]
        if not new_storage_keys:
            break
        seen_storage_keys.update(new_storage_keys)
        for storage_key, document in _load_candidate_documents(
            engine,
            db_name,
            coll_name,
            new_storage_keys,
        ):
            candidates_evaluated += 1
            if query.filter_spec is not None and not prefilter_exact and not QueryEngine.match(
                document,
                query.filter_spec,
                dialect=MONGODB_DIALECT_70,
            ):
                documents_filtered += 1
                continue
            score = score_vector_document(
                document,
                definition=definition,
                query=query,
            )
            if score is None:
                continue
            if query.min_score is not None and score < query.min_score:
                documents_filtered_by_min_score += 1
                continue
            matched_documents.append(document)
            if len(matched_documents) >= query.limit:
                return (
                    matched_documents[: query.limit],
                    current_request,
                    candidates_evaluated,
                    documents_filtered,
                    documents_filtered_by_min_score,
                    None,
                )
        if query.filter_spec is None or current_request >= max_requested:
            break
        current_request = _next_vector_candidate_request(
            current_request,
            matched_count=len(matched_documents),
            limit=query.limit,
            max_requested=max_requested,
        )

    if query.filter_spec is not None and len(matched_documents) < query.limit:
        exact_fallback_reason = "candidate-prefilter-underflow" if prefilter_exact else "post-filter-underflow"
    return (
        matched_documents[: query.limit],
        current_request,
        candidates_evaluated,
        documents_filtered,
        documents_filtered_by_min_score,
        exact_fallback_reason,
    )


def execute_sqlite_search_query(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    query: SearchQuery,
    physical_name: str | None,
    deadline: float | None,
    result_limit_hint: int | None,
    downstream_filter_spec: dict[str, object] | None,
) -> list[Document]:
    resolved_physical_name = ensure_search_backend_sync(
        engine,
        conn,
        db_name,
        coll_name,
        definition,
        physical_name,
    )
    if isinstance(query, SearchVectorQuery):
        decision = decide_sqlite_search_backend(
            query,
            physical_name=resolved_physical_name,
            fts5_available=engine._supports_fts5(conn),
            backend_materialized=resolved_physical_name is not None,
            ann_available=True,
        )
        if decision.backend == "usearch" and resolved_physical_name is not None:
            backend_state = ensure_vector_search_backend_sync(
                engine,
                conn,
                db_name,
                coll_name,
                definition,
                resolved_physical_name,
                query.path,
            )
            vector_filter_storage_keys, vector_filter_description = vector_filter_candidate_storage_keys(
                backend_state,
                filter_spec=query.filter_spec,
            )
            (
                filtered_documents,
                _requested_candidates,
                _candidates_evaluated,
                _documents_filtered,
                _documents_filtered_by_min_score,
                exact_fallback_reason,
            ) = _sqlite_vector_candidate_documents(
                engine,
                db_name,
                coll_name,
                definition,
                query,
                backend_state,
                prefilter_storage_keys=vector_filter_storage_keys,
                prefilter_exact=bool(vector_filter_description and vector_filter_description.get("exact")),
            )
            if exact_fallback_reason is None:
                enforce_deadline(deadline)
                if downstream_filter_spec is not None:
                    filtered_documents = [
                        document
                        for document in filtered_documents
                        if QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70)
                    ]
                effective_limit = min(query.limit, result_limit_hint) if result_limit_hint is not None else query.limit
                return filtered_documents[:effective_limit]
        exact_hits = exact_vector_hits_sync(
            engine,
            db_name,
            coll_name,
            definition,
            query,
            candidate_storage_keys=(
                vector_filter_storage_keys
                if vector_filter_storage_keys is not None
                and vector_filter_description is not None
                and bool(vector_filter_description.get("exact"))
                else None
            ),
            skip_filter_match=bool(
                vector_filter_storage_keys is not None
                and vector_filter_description is not None
                and bool(vector_filter_description.get("exact"))
            ),
        )
        enforce_deadline(deadline)
        effective_limit = min(query.limit, result_limit_hint) if result_limit_hint is not None else query.limit
        return [document for _score, document in exact_hits[:effective_limit]]

    decision = decide_sqlite_search_backend(
        query,
        physical_name=resolved_physical_name,
        fts5_available=engine._supports_fts5(conn),
        backend_materialized=bool(
            resolved_physical_name
            and engine._sqlite_table_exists(conn, resolved_physical_name)
        ),
    )
    downstream_filter_storage_keys, downstream_filter_description = _sqlite_candidate_storage_keys_for_downstream_filter(
        engine,
        conn,
        physical_name=resolved_physical_name,
        definition=definition,
        filter_spec=downstream_filter_spec,
    )
    compound_should_candidates: list[list[str]] | None = None
    if isinstance(query, SearchCompoundQuery):
        (
            candidate_storage_keys,
            _candidate_backend,
            candidate_exact,
            compound_should_candidates,
            _non_candidateable_should,
        ) = _sqlite_compound_candidate_state(
            engine,
            conn,
            physical_name=resolved_physical_name,
            definition=definition,
            query=query,
            downstream_filter_storage_keys=downstream_filter_storage_keys,
            downstream_filter_spec=downstream_filter_spec,
            downstream_filter_exact=bool(downstream_filter_description and downstream_filter_description.get("exact")),
        )
    else:
        candidate_storage_keys, _candidate_backend, candidate_exact = _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=resolved_physical_name,
            query=query,
            definition=definition,
        )
        if downstream_filter_storage_keys is not None:
            if candidate_storage_keys is None:
                candidate_storage_keys = downstream_filter_storage_keys
                candidate_exact = False
            else:
                downstream_set = set(downstream_filter_storage_keys)
                candidate_storage_keys = [
                    storage_key for storage_key in candidate_storage_keys if storage_key in downstream_set
                ]
                candidate_exact = candidate_exact and bool(
                    downstream_filter_description and downstream_filter_description.get("exact")
                )
            candidate_storage_keys, _topk_prefilter = _prune_candidate_storage_keys_for_topk(
                engine,
                conn,
                db_name=db_name,
                coll_name=coll_name,
                physical_name=resolved_physical_name,
                query=query,
                candidate_storage_keys=candidate_storage_keys,
        candidate_exact=candidate_exact,
        result_limit_hint=result_limit_hint,
        should_candidates=compound_should_candidates,
    )
    if candidate_storage_keys is not None:
        if not candidate_storage_keys:
            return []
        if candidate_exact:
            enforce_deadline(deadline)
            if isinstance(query, SearchCompoundQuery) and query.should:
                if resolved_physical_name is not None and _compound_entry_ranking_supported(
                    query,
                    physical_name=resolved_physical_name,
                ):
                    ranked_storage_keys = _rank_compound_candidate_storage_keys_from_entries(
                        engine,
                        conn,
                        db_name=db_name,
                        coll_name=coll_name,
                        physical_name=resolved_physical_name,
                        query=query,
                        candidate_storage_keys=candidate_storage_keys,
                        result_limit_hint=result_limit_hint,
                    )
                    if ranked_storage_keys is not None:
                        ranked_documents_by_key = engine._load_documents_by_storage_keys(
                            db_name,
                            coll_name,
                            ranked_storage_keys,
                        )
                        return [
                            ranked_documents_by_key[storage_key]
                            for storage_key in ranked_storage_keys
                            if storage_key in ranked_documents_by_key
                            and (
                                downstream_filter_spec is None
                                or QueryEngine.match(
                                    ranked_documents_by_key[storage_key],
                                    downstream_filter_spec,
                                    dialect=MONGODB_DIALECT_70,
                                )
                            )
                        ]
                candidate_documents = _load_candidate_documents(
                    engine,
                    db_name,
                    coll_name,
                    candidate_storage_keys,
                )
                exact_documents = [
                    (
                        document,
                        definition,
                        materialize_search_document(document, definition),
                    )
                    for _storage_key, document in candidate_documents
                    if downstream_filter_spec is None
                    or QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70)
                ]
                sorted_documents = _sort_search_documents_for_query(exact_documents, query=query)
                return sorted_documents[:result_limit_hint] if result_limit_hint is not None else sorted_documents
            candidate_documents = _load_candidate_documents(
                engine,
                db_name,
                coll_name,
                candidate_storage_keys,
            )
            documents = [
                document
                for _storage_key, document in candidate_documents
                if downstream_filter_spec is None
                or QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70)
            ]
            return documents[:result_limit_hint] if result_limit_hint is not None else documents
        candidate_documents = _load_candidate_documents(
            engine,
            db_name,
            coll_name,
            candidate_storage_keys,
        )
        filtered_documents = [
            (document, definition, materialized_search_document)
            for _storage_key, document in candidate_documents
            if downstream_filter_spec is None
            or QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70)
            if (
                materialized_search_document := materialize_search_document(document, definition)
            ) is not None
            and matches_search_query(
                document,
                definition=definition,
                query=query,
                materialized=materialized_search_document,
            )
        ]
        enforce_deadline(deadline)
        sorted_documents = _sort_search_documents_for_query(filtered_documents, query=query)
        return sorted_documents[:result_limit_hint] if result_limit_hint is not None else sorted_documents

    documents = [
        (document, definition, materialized_search_document)
        for _, document in engine._load_documents(db_name, coll_name)
        if downstream_filter_spec is None
        or QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70)
        if (
            materialized_search_document := materialize_search_document(document, definition)
        ) is not None
        and matches_search_query(
            document,
            definition=definition,
            query=query,
            materialized=materialized_search_document,
        )
    ]
    enforce_deadline(deadline)
    sorted_documents = _sort_search_documents_for_query(documents, query=query)
    return sorted_documents[:result_limit_hint] if result_limit_hint is not None else sorted_documents


def search_documents_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    operator: str,
    spec: object,
    max_time_ms: int | None,
    context: ClientSession | None,
    result_limit_hint: int | None,
    downstream_filter_spec: dict[str, object] | None,
) -> list[Document]:
    deadline = operation_deadline(max_time_ms)
    conn = engine._require_connection(context)
    with engine._bind_connection(conn):
        return _sqlite_search_documents(
            db_name=db_name,
            coll_name=coll_name,
            operator=operator,
            spec=spec,
            deadline=deadline,
            load_search_index_rows=lambda current_db_name, current_coll_name, name: engine._load_search_index_rows(
                current_db_name,
                current_coll_name,
                name=name,
            ),
            search_index_is_ready=search_index_is_ready_sync,
            load_documents=lambda current_db_name, current_coll_name: list(
                engine._load_documents(current_db_name, current_coll_name)
            ),
            search_sql=lambda current_db_name, current_coll_name, definition, query, physical_name, current_limit_hint: execute_sqlite_search_query(
                engine,
                conn,
                current_db_name,
                current_coll_name,
                definition,
                query,
                physical_name,
                deadline,
                current_limit_hint,
                downstream_filter_spec,
            ),
            result_limit_hint=result_limit_hint,
        )


def explain_search_documents_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    operator: str,
    spec: object,
    max_time_ms: int | None,
    context: ClientSession | None,
    result_limit_hint: int | None,
    downstream_filter_spec: dict[str, object] | None,
) -> QueryPlanExplanation:
    query = compile_search_stage(operator, spec)
    ready_at_epoch: float | None = None
    ready = True
    fts5_available: bool | None = None
    backend_materialized = False
    resolved_physical_name: str | None = None
    vector_state: SQLiteVectorBackendState | None = None
    vector_filter_storage_keys: list[str] | None = None
    vector_filter_description: dict[str, object] | None = None
    exact_fallback_reason: str | None = None
    documents_filtered = 0
    documents_filtered_by_min_score: int | None = None
    candidates_evaluated: int | None = None
    candidates_requested: int | None = None
    candidate_storage_keys: list[str] | None = None
    candidate_count_before_topk: int | None = None
    candidate_exact: bool | None = None
    topk_prefilter: dict[str, object] | None = None
    compound_prefilter: dict[str, object] | None = None
    conn = engine._require_connection(context)
    with engine._bind_connection(conn):
        rows = engine._load_search_index_rows(db_name, coll_name, name=query.index_name)
        if not rows:
            raise OperationFailure(f"search index not found with name [{query.index_name}]")
        definition, physical_name, ready_at_epoch = rows[0]
        ready = search_index_is_ready_sync(ready_at_epoch)
        decision = decide_sqlite_search_backend(
            query,
            physical_name=physical_name,
            fts5_available=None,
            backend_materialized=False,
        )
        if is_text_search_query(query):
            resolved_physical_name = ensure_search_backend_sync(
                engine,
                conn,
                db_name,
                coll_name,
                definition,
                physical_name,
            )
            fts5_available = engine._supports_fts5(conn)
            backend_materialized = bool(
                resolved_physical_name
                and engine._sqlite_table_exists(conn, resolved_physical_name)
            )
            decision = decide_sqlite_search_backend(
                query,
                physical_name=resolved_physical_name or physical_name,
                fts5_available=fts5_available,
                backend_materialized=backend_materialized,
            )
            downstream_filter_storage_keys, downstream_filter_description = _sqlite_candidate_storage_keys_for_downstream_filter(
                engine,
                conn,
                physical_name=resolved_physical_name,
                definition=definition,
                filter_spec=downstream_filter_spec,
            )
            compound_should_candidates: list[list[str]] | None = None
            if isinstance(query, SearchCompoundQuery):
                (
                    candidate_storage_keys,
                    candidate_backend,
                    candidate_exact,
                    compound_should_candidates,
                    _non_candidateable_should,
                ) = _sqlite_compound_candidate_state(
                    engine,
                    conn,
                    physical_name=resolved_physical_name,
                    definition=definition,
                    query=query,
                    downstream_filter_storage_keys=downstream_filter_storage_keys,
                    downstream_filter_spec=downstream_filter_spec,
                    downstream_filter_exact=bool(downstream_filter_description and downstream_filter_description.get("exact")),
                )
            else:
                candidate_storage_keys, candidate_backend, candidate_exact = _sqlite_candidate_storage_keys_for_query(
                    engine,
                    conn,
                    physical_name=resolved_physical_name,
                    query=query,
                    definition=definition,
                )
                if downstream_filter_storage_keys is not None:
                    if candidate_storage_keys is None:
                        candidate_storage_keys = downstream_filter_storage_keys
                        candidate_exact = False
                    else:
                        downstream_set = set(downstream_filter_storage_keys)
                        candidate_storage_keys = [
                            storage_key for storage_key in candidate_storage_keys if storage_key in downstream_set
                        ]
                        candidate_exact = candidate_exact and bool(
                            downstream_filter_description and downstream_filter_description.get("exact")
                        )
            candidate_count_before_topk = (
                len(candidate_storage_keys) if candidate_storage_keys is not None else None
            )
            candidate_storage_keys, topk_prefilter = _prune_candidate_storage_keys_for_topk(
                engine,
                conn,
                db_name=db_name,
                coll_name=coll_name,
                physical_name=resolved_physical_name,
                query=query,
                candidate_storage_keys=candidate_storage_keys,
                candidate_exact=bool(candidate_exact),
                result_limit_hint=result_limit_hint,
                should_candidates=compound_should_candidates,
            )
            if isinstance(query, SearchCompoundQuery):
                compound_prefilter = _describe_compound_prefilter_sync(
                    engine,
                    conn,
                    physical_name=resolved_physical_name,
                    definition=definition,
                    query=query,
                    downstream_filter=downstream_filter_description,
                    downstream_filter_storage_keys=downstream_filter_storage_keys,
                )
            if candidate_backend is not None:
                decision = type(decision)(
                    backend=candidate_backend,
                    backend_available=True,
                    backend_materialized=backend_materialized,
                    fts5_available=fts5_available,
                    ann_available=decision.ann_available,
                    fts5_match=decision.fts5_match,
                    physical_name=decision.physical_name,
                )
        elif isinstance(query, SearchVectorQuery):
            resolved_physical_name = ensure_search_backend_sync(
                engine,
                conn,
                db_name,
                coll_name,
                definition,
                physical_name,
            )
            if resolved_physical_name is not None:
                vector_state = ensure_vector_search_backend_sync(
                    engine,
                    conn,
                    db_name,
                    coll_name,
                    definition,
                    resolved_physical_name,
                    query.path,
                )
                vector_filter_storage_keys, vector_filter_description = vector_filter_candidate_storage_keys(
                    vector_state,
                    filter_spec=query.filter_spec,
                )
            decision = decide_sqlite_search_backend(
                query,
                physical_name=resolved_physical_name,
                fts5_available=fts5_available,
                backend_materialized=vector_state is not None,
                ann_available=True,
            )
        if isinstance(query, SearchVectorQuery) and vector_state is not None:
            (
                _matched_documents,
                candidates_requested,
                evaluated_count,
                documents_filtered,
                documents_filtered_by_min_score,
                exact_fallback_reason,
            ) = _sqlite_vector_candidate_documents(
                engine,
                db_name,
                coll_name,
                definition,
                query,
                vector_state,
                prefilter_storage_keys=vector_filter_storage_keys if vector_state is not None else None,
                prefilter_exact=bool(vector_filter_description and vector_filter_description.get("exact")),
            )
            candidates_evaluated = evaluated_count

    return QueryPlanExplanation(
        engine="sqlite",
        strategy="search",
        plan=(
            "usearch-vector-search"
            if isinstance(query, SearchVectorQuery) and decision.backend == "usearch"
            else "python-vector-search"
            if isinstance(query, SearchVectorQuery)
            else f"{decision.backend}-search"
        ),
        sort=None,
        skip=0,
        limit=None,
        hint=None,
        hinted_index=query.index_name,
        comment=None,
        max_time_ms=max_time_ms,
        details={
            "operator": operator,
            "index": query.index_name,
            "backend": decision.backend,
            "status": "READY" if ready else "PENDING",
            "backendAvailable": decision.backend_available,
            "backendMaterialized": decision.backend_materialized,
            "physicalName": decision.physical_name,
            "readyAtEpoch": ready_at_epoch,
            "fts5Available": decision.fts5_available,
            "annAvailable": decision.ann_available,
            "definition": build_search_index_document(
                definition,
                ready=ready,
                ready_at_epoch=ready_at_epoch,
            ),
            **search_query_explain_details(query),
            "similarity": (
                vector_state.similarity
                if isinstance(query, SearchVectorQuery) and vector_state is not None
                else query.similarity
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "fts5_match": decision.fts5_match,
            "vector_paths": list(vector_field_paths(definition)) if definition.index_type == "vectorSearch" else None,
            "mode": (
                "ann"
                if isinstance(query, SearchVectorQuery) and decision.backend == "usearch"
                else "exact"
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "filterMode": (
                "candidate-prefilter"
                if isinstance(query, SearchVectorQuery)
                and query.filter_spec is not None
                and bool(vector_filter_description and vector_filter_description.get("exact"))
                else "candidate-prefilter+post-candidate"
                if isinstance(query, SearchVectorQuery)
                and query.filter_spec is not None
                and vector_filter_description is not None
                else "post-candidate"
                if isinstance(query, SearchVectorQuery) and query.filter_spec is not None
                else None
            ),
            "candidateExpansionStrategy": (
                "adaptive-retention"
                if isinstance(query, SearchVectorQuery) and query.filter_spec is not None
                else None
            ),
            "vectorFilterPrefilter": (
                vector_filter_description
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "vectorFilterResidual": (
                _vector_filter_residual_description(query.filter_spec, vector_filter_description)
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "candidateCount": len(candidate_storage_keys) if candidate_storage_keys is not None else None,
            "candidateCountBeforeTopK": candidate_count_before_topk,
            "candidatePrefilterExact": candidate_exact,
            "compoundPrefilter": compound_prefilter,
            "topKLimitHint": result_limit_hint,
            "topKPrefilter": topk_prefilter,
            "rankingSource": (
                "fts-materialized-entries"
                if isinstance(query, SearchCompoundQuery)
                and candidate_exact
                and _compound_entry_ranking_supported(query, physical_name=resolved_physical_name)
                else None
            ),
            "downstreamFilterPrefilter": deepcopy(downstream_filter_spec) if downstream_filter_spec is not None else None,
            "exactFallbackReason": exact_fallback_reason,
            "candidatesEvaluated": (
                candidates_evaluated
                if isinstance(query, SearchVectorQuery) and vector_state is not None
                else None
            ),
            "candidatesRequested": (
                candidates_requested
                if isinstance(query, SearchVectorQuery) and vector_state is not None
                else None
            ),
            "documentsFiltered": (
                documents_filtered
                if isinstance(query, SearchVectorQuery) and query.filter_spec is not None
                else None
            ),
            "documentsFilteredByMinScore": (
                documents_filtered_by_min_score
                if isinstance(query, SearchVectorQuery) and query.min_score is not None
                else None
            ),
            "documentsScanned": (
                vector_state.documents_scanned
                if isinstance(query, SearchVectorQuery) and vector_state is not None
                else None
            ),
            "validVectors": (
                vector_state.valid_vectors
                if isinstance(query, SearchVectorQuery) and vector_state is not None
                else None
            ),
            "invalidVectors": (
                vector_state.invalid_vectors
                if isinstance(query, SearchVectorQuery) and vector_state is not None
                else None
            ),
            "vectorBackend": (
                vector_backend_stats_document(vector_state)
                if isinstance(query, SearchVectorQuery) and vector_state is not None
                else None
            ),
        },
    )
