from __future__ import annotations

from collections import Counter
import sqlite3
import time
from typing import Any, Protocol

from mongoeco.api.operations import FindOperation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.search import (
    build_search_index_document,
    compile_search_stage,
    is_text_search_query,
    iter_searchable_text_entries,
    materialize_search_document,
    matches_search_query,
    search_near_distance,
    score_vector_document,
    search_query_explain_details,
    search_query_operator_name,
    sqlite_fts5_query,
    SearchAutocompleteQuery,
    SearchCompoundQuery,
    SearchExistsQuery,
    SearchNearQuery,
    SearchPhraseQuery,
    SearchQuery,
    SearchTextQuery,
    SearchVectorQuery,
    SearchWildcardQuery,
    vector_field_paths,
)
from mongoeco.engines._sqlite_catalog import load_search_index_rows as _sqlite_load_search_index_rows
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
)
from mongoeco.engines._sqlite_read_ops import search_documents as _sqlite_search_documents
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import Document, QueryPlanExplanation, SearchIndexDefinition, SearchIndexDocument


class _SQLiteSearchRuntimeEngine(Protocol):
    _simulate_search_index_latency: float
    _ensured_search_backends: set[str]
    _vector_search_backends: dict[tuple[str, str], SQLiteVectorBackendState]

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
    counts: Counter[str] = Counter()
    order: dict[str, int] = {}
    for values in lists:
        for storage_key in values:
            counts[storage_key] += 1
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
        candidate_intersections: list[list[str]] = []
        should_candidates: list[list[str]] = []
        saw_non_candidateable_should = False
        non_candidateable_should = 0
        exact = True

        for clause in (*query.must, *query.filter):
            storage_keys, _backend, clause_exact = _sqlite_candidate_storage_keys_for_query(
                engine,
                conn,
                physical_name=physical_name,
                query=clause,
            )
            if storage_keys is not None:
                candidate_intersections.append(storage_keys)
                exact = exact and clause_exact
            else:
                exact = False

        for clause in query.should:
            storage_keys, _backend, clause_exact = _sqlite_candidate_storage_keys_for_query(
                engine,
                conn,
                physical_name=physical_name,
                query=clause,
            )
            if storage_keys is None:
                saw_non_candidateable_should = True
                non_candidateable_should += 1
                exact = False
                continue
            should_candidates.append(storage_keys)
            exact = exact and clause_exact

        candidates = _intersect_storage_key_lists(candidate_intersections) if candidate_intersections else None

        if should_candidates:
            required_candidateable_should = max(
                0,
                query.minimum_should_match - non_candidateable_should,
            )
            if required_candidateable_should > 0:
                merged_should = _storage_keys_with_minimum_frequency(
                    should_candidates,
                    required_candidateable_should,
                )
                if candidates is None:
                    candidates = merged_should
                else:
                    merged_should_set = set(merged_should)
                    candidates = [
                        storage_key
                        for storage_key in candidates
                        if storage_key in merged_should_set
                    ]
        elif candidates is None:
            return None, None, False

        if candidates is None:
            return None, None, False
        if query.must_not:
            excluded: set[str] = set()
            for clause in query.must_not:
                storage_keys, _backend, clause_exact = _sqlite_candidate_storage_keys_for_query(
                    engine,
                    conn,
                    physical_name=physical_name,
                    query=clause,
                )
                if storage_keys is None:
                    exact = False
                    continue
                if clause_exact:
                    excluded.update(storage_keys)
                else:
                    exact = False
            if excluded:
                candidates = [storage_key for storage_key in candidates if storage_key not in excluded]
        return candidates, "fts5-prefilter", exact
    if isinstance(query, SearchNearQuery):
        return None, None, False
    return None, None, False


def _describe_compound_prefilter_sync(
    engine: _SQLiteSearchRuntimeEngine,
    conn: sqlite3.Connection,
    *,
    physical_name: str | None,
    query: SearchCompoundQuery,
) -> dict[str, object]:
    def _describe_clause(clause: SearchQuery) -> dict[str, object]:
        storage_keys, backend, exact = _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=physical_name,
            query=clause,
        )
        return {
            "operator": search_query_operator_name(clause),
            "candidateable": storage_keys is not None,
            "exact": exact if storage_keys is not None else False,
            "backend": backend,
        }

    non_candidateable_should = sum(
        1
        for clause in query.should
        if _sqlite_candidate_storage_keys_for_query(
            engine,
            conn,
            physical_name=physical_name,
            query=clause,
        )[0]
        is None
    )
    return {
        "must": [_describe_clause(clause) for clause in query.must],
        "filter": [_describe_clause(clause) for clause in query.filter],
        "should": [_describe_clause(clause) for clause in query.should],
        "mustNot": [_describe_clause(clause) for clause in query.must_not],
        "requiredCandidateableShould": max(
            0,
            query.minimum_should_match - non_candidateable_should,
        ),
    }


def _sort_search_documents_for_query(
    documents: list[Document],
    *,
    query: SearchQuery,
) -> list[Document]:
    if isinstance(query, SearchNearQuery):
        ranked: list[tuple[float, Document]] = []
        for document in documents:
            distance = search_near_distance(document, query=query)
            ranked.append((distance if distance is not None else float("inf"), document))
        ranked.sort(key=lambda item: item[0])
        return [document for _distance, document in ranked]
    return documents


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
) -> list[tuple[float, Document]]:
    vector_hits: list[tuple[float, Document]] = []
    for _, document in engine._load_documents(db_name, coll_name):
        if query.filter_spec is not None and not QueryEngine.match(
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
        vector_hits.append((score, document))
    vector_hits.sort(key=lambda item: item[0], reverse=True)
    return vector_hits


def _sqlite_vector_candidate_documents(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    query: SearchVectorQuery,
    backend_state: SQLiteVectorBackendState,
) -> tuple[list[Document], int, int, int, str | None]:
    requested = max(query.limit, query.num_candidates)
    max_requested = min(
        backend_state.valid_vectors,
        max(requested, query.limit * 4),
    )
    matched_documents: list[Document] = []
    documents_filtered = 0
    candidates_evaluated = 0
    exact_fallback_reason: str | None = None
    seen_storage_keys: set[str] = set()
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
            if query.filter_spec is not None and not QueryEngine.match(
                document,
                query.filter_spec,
                dialect=MONGODB_DIALECT_70,
            ):
                documents_filtered += 1
                continue
            matched_documents.append(document)
            if len(matched_documents) >= query.limit:
                return (
                    matched_documents[: query.limit],
                    current_request,
                    candidates_evaluated,
                    documents_filtered,
                    None,
                )
        if query.filter_spec is None or current_request >= max_requested:
            break
        current_request = min(max_requested, current_request * 2)

    if query.filter_spec is not None and len(matched_documents) < query.limit:
        exact_fallback_reason = "post-filter-underflow"
    return (
        matched_documents[: query.limit],
        current_request,
        candidates_evaluated,
        documents_filtered,
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
            (
                filtered_documents,
                _requested_candidates,
                _candidates_evaluated,
                _documents_filtered,
                exact_fallback_reason,
            ) = _sqlite_vector_candidate_documents(
                engine,
                db_name,
                coll_name,
                query,
                backend_state,
            )
            if exact_fallback_reason is None:
                enforce_deadline(deadline)
                return filtered_documents[: query.limit]
        exact_hits = exact_vector_hits_sync(engine, db_name, coll_name, definition, query)
        enforce_deadline(deadline)
        return [document for _score, document in exact_hits[: query.limit]]

    decision = decide_sqlite_search_backend(
        query,
        physical_name=resolved_physical_name,
        fts5_available=engine._supports_fts5(conn),
        backend_materialized=bool(
            resolved_physical_name
            and engine._sqlite_table_exists(conn, resolved_physical_name)
        ),
    )
    candidate_storage_keys, _candidate_backend, candidate_exact = _sqlite_candidate_storage_keys_for_query(
        engine,
        conn,
        physical_name=resolved_physical_name,
        query=query,
    )
    if candidate_storage_keys is not None:
        if not candidate_storage_keys:
            return []
        candidate_documents = _load_candidate_documents(
            engine,
            db_name,
            coll_name,
            candidate_storage_keys,
        )
        if candidate_exact:
            enforce_deadline(deadline)
            return [document for _storage_key, document in candidate_documents]
        filtered_documents = [
            document
            for _storage_key, document in candidate_documents
            if matches_search_query(
                document,
                definition=definition,
                query=query,
                materialized=materialize_search_document(document, definition),
            )
        ]
        enforce_deadline(deadline)
        return _sort_search_documents_for_query(filtered_documents, query=query)

    documents = [
        document
        for _, document in engine._load_documents(db_name, coll_name)
        if matches_search_query(
            document,
            definition=definition,
            query=query,
            materialized=materialize_search_document(document, definition),
        )
    ]
    enforce_deadline(deadline)
    return _sort_search_documents_for_query(documents, query=query)


def search_documents_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    operator: str,
    spec: object,
    max_time_ms: int | None,
    context: ClientSession | None,
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
            search_sql=lambda current_db_name, current_coll_name, definition, query, physical_name: execute_sqlite_search_query(
                engine,
                conn,
                current_db_name,
                current_coll_name,
                definition,
                query,
                physical_name,
                deadline,
            ),
        )


def explain_search_documents_sync(
    engine: _SQLiteSearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    operator: str,
    spec: object,
    max_time_ms: int | None,
    context: ClientSession | None,
) -> QueryPlanExplanation:
    query = compile_search_stage(operator, spec)
    ready_at_epoch: float | None = None
    ready = True
    fts5_available: bool | None = None
    backend_materialized = False
    resolved_physical_name: str | None = None
    vector_state: SQLiteVectorBackendState | None = None
    exact_fallback_reason: str | None = None
    documents_filtered = 0
    candidates_evaluated: int | None = None
    candidates_requested: int | None = None
    candidate_storage_keys: list[str] | None = None
    candidate_exact: bool | None = None
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
            candidate_storage_keys, candidate_backend, candidate_exact = _sqlite_candidate_storage_keys_for_query(
                engine,
                conn,
                physical_name=resolved_physical_name,
                query=query,
            )
            if isinstance(query, SearchCompoundQuery):
                compound_prefilter = _describe_compound_prefilter_sync(
                    engine,
                    conn,
                    physical_name=resolved_physical_name,
                    query=query,
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
                exact_fallback_reason,
            ) = _sqlite_vector_candidate_documents(
                engine,
                db_name,
                coll_name,
                query,
                vector_state,
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
            "fts5_match": decision.fts5_match,
            "vector_paths": list(vector_field_paths(definition)) if definition.index_type == "vectorSearch" else None,
            "mode": (
                "ann"
                if isinstance(query, SearchVectorQuery) and decision.backend == "usearch"
                else "exact"
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "filterMode": "post-candidate" if isinstance(query, SearchVectorQuery) and query.filter_spec is not None else None,
            "candidateCount": len(candidate_storage_keys) if candidate_storage_keys is not None else None,
            "candidatePrefilterExact": candidate_exact,
            "compoundPrefilter": compound_prefilter,
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
