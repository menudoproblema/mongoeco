from __future__ import annotations

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
    matches_search_query,
    score_vector_document,
    search_query_explain_details,
    SearchQuery,
    SearchVectorQuery,
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
            documents_by_storage_key = {
                storage_key: document
                for storage_key, document in engine._load_documents(db_name, coll_name)
            }
            ann_hits = search_sqlite_vector_backend(
                backend_state,
                query_vector=query.query_vector,
                count=query.num_candidates,
            )
            filtered_documents: list[Document] = []
            for storage_key, _distance in ann_hits:
                document = documents_by_storage_key.get(storage_key)
                if document is None:
                    continue
                if query.filter_spec is not None and not QueryEngine.match(
                    document,
                    query.filter_spec,
                    dialect=MONGODB_DIALECT_70,
                ):
                    continue
                filtered_documents.append(document)
                if len(filtered_documents) >= query.limit:
                    break
            if query.filter_spec is None or len(filtered_documents) >= query.limit:
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
    if decision.backend == "fts5":
        sql = (
            f"SELECT DISTINCT storage_key FROM {engine._quote_identifier(decision.physical_name)} "
            "WHERE content MATCH ?"
        )
        params: list[object] = [decision.fts5_match]
        if is_text_search_query(query) and query.paths is not None:
            placeholders = ", ".join("?" for _ in query.paths)
            sql += f" AND field_path IN ({placeholders})"
            params.extend(query.paths)
        storage_keys = [row[0] for row in conn.execute(sql, tuple(params)).fetchall()]
        if not storage_keys:
            return []
        storage_key_set = set(storage_keys)
        documents = {
            storage_key: document
            for storage_key, document in engine._load_documents(db_name, coll_name)
            if storage_key in storage_key_set
        }
        enforce_deadline(deadline)
        return [documents[storage_key] for storage_key in storage_keys if storage_key in documents]

    documents = [
        document
        for _, document in engine._load_documents(db_name, coll_name)
        if matches_search_query(
            document,
            definition=definition,
            query=query,
        )
    ]
    enforce_deadline(deadline)
    return documents


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
        if isinstance(query, SearchVectorQuery) and vector_state is not None and query.filter_spec is not None:
            documents_by_storage_key = {
                storage_key: document
                for storage_key, document in engine._load_documents(db_name, coll_name)
            }
            ann_hits = search_sqlite_vector_backend(
                vector_state,
                query_vector=query.query_vector,
                count=query.num_candidates,
            )
            matched = 0
            for storage_key, _distance in ann_hits:
                document = documents_by_storage_key.get(storage_key)
                if document is None:
                    continue
                if QueryEngine.match(document, query.filter_spec, dialect=MONGODB_DIALECT_70):
                    matched += 1
                else:
                    documents_filtered += 1
                if matched >= query.limit:
                    break
            if matched < query.limit:
                exact_fallback_reason = "post-filter-underflow"

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
            "exactFallbackReason": exact_fallback_reason,
            "candidatesEvaluated": (
                min(query.num_candidates, vector_state.valid_vectors)
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
