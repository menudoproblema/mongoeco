import asyncio
import atexit
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
import datetime
from dataclasses import replace
from functools import partial
import hashlib
import inspect
import json
import math
import os
import queue
import sqlite3
import threading
import time
import uuid
from copy import deepcopy
from decimal import Decimal
from typing import Any, AsyncIterable, override

from mongoeco.api.operations import FindOperation, UpdateOperation, compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect, MongoDialect70, MongoDialect80
from mongoeco.core.bson_ordering import SQLITE_SORT_BUCKET_WEIGHTS, bson_engine_key, bson_numeric_index_key
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.aggregation.cost import AggregationCostPolicy
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.identity import canonical_document_id
from mongoeco.core.json_compat import json_dumps_compact, json_loads
from mongoeco.core.operators import CompiledUpdatePlan, UpdateEngine
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.paths import get_document_value
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    NotCondition,
    OrCondition,
    QueryNode,
    ensure_query_plan,
)
from mongoeco.core.search import (
    SearchPhraseQuery,
    SearchTextQuery,
    SearchVectorQuery,
    build_search_index_document,
    compile_search_stage,
    iter_searchable_text_entries,
    matches_search_phrase_query,
    matches_search_text_query,
    score_vector_document,
    sqlite_fts5_query,
    validate_search_index_definition,
    vector_field_paths,
)
from mongoeco.core.sorting import sort_documents
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.profiling import EngineProfiler
from mongoeco.engines.semantic_core import (
    EngineFindSemantics,
    EngineReadExecutionPlan,
    build_query_plan_explanation,
    compile_find_semantics,
    compile_find_semantics_from_operation,
    compile_update_semantics,
    enforce_collection_document_validation,
    filter_documents,
    finalize_documents,
    iter_filtered_documents,
    stream_finalize_documents,
)
from mongoeco.engines.sqlite_planner import (
    SQLiteReadExecutionPlan,
    compile_sqlite_read_execution_plan,
)
from mongoeco.engines.virtual_indexes import (
    describe_virtual_index_usage,
    document_in_virtual_index,
    normalize_partial_filter_expression,
    query_can_use_index,
)
from mongoeco.engines.sqlite_query import (
    _normalize_comparable_value,
    _translate_equals_scalar_only,
    _translate_same_type_comparison,
    _translate_scalar_equals,
    index_expressions_sql,
    json_path_for_field,
    path_array_prefixes,
    type_expression_sql,
    value_expression_sql,
    translate_query_plan,
    translate_compiled_update_plan,
    SQLiteQueryTranslator,
)
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.session import EngineTransactionContext
from mongoeco.types import (
    ArrayFilters,
    CollationDocument,
    DeleteResult,
    Document,
    DocumentId,
    ExecutionLineageStep,
    EngineIndexRecord,
    Filter,
    IndexDocument,
    IndexInformation,
    IndexKeySpec,
    ObjectId,
    ProfilingCommandResult,
    Projection,
    QueryPlanExplanation,
    SearchIndexDefinition,
    SearchIndexDocument,
    SortSpec,
    Update,
    UpdateResult,
    default_id_index_definition,
    default_id_index_document,
    default_id_index_information,
    default_index_name,
    index_fields,
    is_ordered_index_spec,
    normalize_index_keys,
    special_index_directions,
)

_SQLITE_SHARED_EXECUTOR_LOCK = threading.Lock()
_SQLITE_SHARED_EXECUTORS: dict[int, ThreadPoolExecutor] = {}
_ASYNC_SCAN_QUEUE_BATCH_SIZE = 64


def _shutdown_sqlite_shared_executors() -> None:
    with _SQLITE_SHARED_EXECUTOR_LOCK:
        executors = list(_SQLITE_SHARED_EXECUTORS.values())
        _SQLITE_SHARED_EXECUTORS.clear()
    for executor in executors:
        executor.shutdown(wait=True, cancel_futures=True)


atexit.register(_shutdown_sqlite_shared_executors)


class SQLiteEngine(AsyncStorageEngine):
    """Motor SQLite async-first usando la stdlib como backend persistente."""

    _PROFILE_COLLECTION_NAME = "system.profile"

    def __init__(
        self,
        path: str = ":memory:",
        codec: type[DocumentCodec] = DocumentCodec,
        executor_workers: int | None = None,
        aggregation_materialization_limit: int | None = 50_000,
        simulate_search_index_latency: float = 0.0,
    ):
        self._path = path
        self._codec = codec
        self._connection: sqlite3.Connection | None = None
        self._connection_count = 0
        self._transaction_owner_session_id: str | None = None
        self._lock = threading.RLock()
        self._scan_condition = threading.Condition()
        self._active_scan_count = 0
        self._thread_local = threading.local()
        self._executor: ThreadPoolExecutor | None = None
        self._owns_executor = False
        if executor_workers is not None and executor_workers < 1:
            raise ValueError("executor_workers must be positive")
        self._executor_workers = executor_workers or (
            min(32, max(4, (os.cpu_count() or 1) + 4))
            if path != ":memory:"
            else 1
        )
        self._use_shared_executor = executor_workers is None
        self._profiler = EngineProfiler("sqlite")
        self._mvcc_version = 0
        self._index_cache: dict[tuple[str, str], tuple[int, list[EngineIndexRecord]]] = {}
        self._index_metadata_versions: dict[tuple[str, str], int] = {}
        self._collection_id_cache: dict[tuple[str, str], int] = {}
        self._collection_features_cache: dict[tuple[str, str, str], bool] = {}
        self._ensured_multikey_physical_indexes: set[str] = set()
        self._fts5_available: bool | None = None
        self._ensured_search_backends: set[str] = set()
        self.aggregation_cost_policy = (
            None
            if aggregation_materialization_limit is None
            else AggregationCostPolicy(
                max_materialized_documents=aggregation_materialization_limit,
                require_spill_for_blocking_stages=True,
            )
        )
        self._simulate_search_index_latency = max(0.0, float(simulate_search_index_latency))
        self._sql_translator = SQLiteQueryTranslator()

    def _ensure_executor(self) -> ThreadPoolExecutor:
        executor = self._executor
        if executor is None:
            if self._use_shared_executor:
                with _SQLITE_SHARED_EXECUTOR_LOCK:
                    executor = _SQLITE_SHARED_EXECUTORS.get(self._executor_workers)
                    if executor is None:
                        executor = ThreadPoolExecutor(
                            max_workers=self._executor_workers,
                            thread_name_prefix="mongoeco-sqlite",
                        )
                        _SQLITE_SHARED_EXECUTORS[self._executor_workers] = executor
                self._owns_executor = False
            else:
                executor = ThreadPoolExecutor(
                    max_workers=self._executor_workers,
                    thread_name_prefix="mongoeco-sqlite",
                )
                self._owns_executor = True
            self._executor = executor
        return executor

    def _shutdown_executor(self) -> None:
        executor = self._executor
        self._executor = None
        owns_executor = self._owns_executor
        self._owns_executor = False
        if executor is not None and owns_executor:
            executor.shutdown(wait=True, cancel_futures=True)

    async def _run_blocking(self, func, /, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._ensure_executor(), partial(func, *args, **kwargs))

    def _engine_key(self) -> str:
        return f"sqlite:{id(self)}"

    def _invalidate_index_cache(self, db_name: str | None = None, coll_name: str | None = None) -> None:
        if db_name is None or coll_name is None:
            self._index_cache.clear()
            return
        self._index_cache.pop((db_name, coll_name), None)

    def _invalidate_collection_id_cache(self, db_name: str | None = None, coll_name: str | None = None) -> None:
        if db_name is None or coll_name is None:
            self._collection_id_cache.clear()
            return
        self._collection_id_cache.pop((db_name, coll_name), None)

    def _invalidate_collection_features_cache(
        self,
        db_name: str | None = None,
        coll_name: str | None = None,
    ) -> None:
        if db_name is None or coll_name is None:
            self._collection_features_cache.clear()
            return
        prefix = (db_name, coll_name)
        stale_keys = [
            key
            for key in self._collection_features_cache
            if key[:2] == prefix
        ]
        for key in stale_keys:
            self._collection_features_cache.pop(key, None)

    def _index_cache_version(self, db_name: str, coll_name: str) -> int:
        return self._index_metadata_versions.get((db_name, coll_name), 0)

    def _mark_index_metadata_changed(self, db_name: str, coll_name: str) -> None:
        cache_key = (db_name, coll_name)
        self._index_metadata_versions[cache_key] = self._index_metadata_versions.get(cache_key, 0) + 1
        self._invalidate_index_cache(db_name, coll_name)

    def _ensure_multikey_physical_indexes_sync(
        self,
        conn: sqlite3.Connection,
        indexes: list[EngineIndexRecord],
    ) -> None:
        had_transaction = conn.in_transaction
        created_any = False
        for index in indexes:
            if not index.get("multikey"):
                continue
            physical_name = str(index["multikey_physical_name"])
            if physical_name in self._ensured_multikey_physical_indexes:
                continue
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {self._quote_identifier(physical_name)} "
                "ON multikey_entries (collection_id, index_name, type_score, element_key, storage_key)"
            )
            self._ensured_multikey_physical_indexes.add(physical_name)
            created_any = True
        if created_any and not had_transaction and conn.in_transaction:
            conn.commit()

    def _ensure_scalar_physical_indexes_sync(
        self,
        conn: sqlite3.Connection,
        indexes: list[EngineIndexRecord],
    ) -> None:
        had_transaction = conn.in_transaction
        created_any = False
        for index in indexes:
            if not self._supports_scalar_index(index):
                continue
            physical_name = index.get("scalar_physical_name")
            if not physical_name:
                continue
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {self._quote_identifier(str(physical_name))} "
                "ON scalar_index_entries (collection_id, index_name, type_score, element_key, storage_key)"
            )
            created_any = True
        if created_any and not had_transaction and conn.in_transaction:
            conn.commit()

    @staticmethod
    def _multikey_type_score(element_type: str) -> int:
        if element_type == "undefined":
            return 0
        return SQLITE_SORT_BUCKET_WEIGHTS.get(element_type, SQLITE_SORT_BUCKET_WEIGHTS["fallback"])

    def _record_operation_metadata(
        self,
        context: ClientSession | None,
        *,
        operation: str,
        comment: object | None,
        max_time_ms: int | None,
        hint: str | IndexKeySpec | None,
    ) -> None:
        if context is None:
            return
        context.update_engine_state(
            self._engine_key(),
            last_operation={
                "operation": operation,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "hint": hint,
                "recorded_at": time.monotonic(),
            },
        )

    def _record_profile_event(
        self,
        db_name: str,
        *,
        op: str,
        command: dict[str, object],
        duration_micros: int,
        execution_lineage: tuple[ExecutionLineageStep, ...] = (),
        fallback_reason: str | None = None,
        ok: float = 1.0,
        errmsg: str | None = None,
    ) -> None:
        self._profiler.record(
            db_name,
            op=op,
            namespace=f"{db_name}.{self._PROFILE_COLLECTION_NAME}",
            command=command,
            duration_micros=duration_micros,
            execution_lineage=execution_lineage,
            fallback_reason=fallback_reason,
            ok=ok,
            errmsg=errmsg,
        )

    def _is_profile_namespace(self, coll_name: str) -> bool:
        return coll_name == self._PROFILE_COLLECTION_NAME

    @override
    def create_session_state(self, session: ClientSession) -> None:
        engine_key = self._engine_key()
        session.bind_engine_context(
            EngineTransactionContext(
                engine_key=engine_key,
                connected=self._connection is not None,
                supports_transactions=True,
                transaction_active=False,
                metadata={"path": self._path, "snapshot_version": self._mvcc_version},
            )
        )
        session.register_transaction_hooks(
            engine_key,
            start=self._start_session_transaction,
            commit=self._commit_session_transaction,
            abort=self._abort_session_transaction,
        )

    def _sync_session_state(
        self,
        session: ClientSession,
        *,
        transaction_active: bool | None = None,
    ) -> None:
        state = session.get_engine_context(self._engine_key())
        if state is None:
            return
        state.connected = self._connection is not None
        if transaction_active is not None:
            state.transaction_active = transaction_active
        state.metadata["snapshot_version"] = self._mvcc_version

    def _start_session_transaction(self, session: ClientSession) -> None:
        with self._lock:
            if self._connection is None:
                raise InvalidOperation("SQLiteEngine must be connected before starting a transaction")
            if self._transaction_owner_session_id is not None:
                raise InvalidOperation("SQLiteEngine already has an active transaction bound to another session")
            conn = self._connection
            conn.execute("BEGIN")
            self._transaction_owner_session_id = session.session_id
            self._mvcc_version += 1
            self._sync_session_state(session, transaction_active=True)

    def _commit_session_transaction(self, session: ClientSession) -> None:
        with self._lock:
            if self._connection is None:
                raise InvalidOperation("SQLiteEngine is not connected")
            if self._transaction_owner_session_id != session.session_id:
                raise InvalidOperation("This session does not own the active SQLite transaction")
            try:
                self._connection.commit()
            finally:
                self._transaction_owner_session_id = None
                self._mvcc_version += 1
                self._sync_session_state(session, transaction_active=False)

    def _abort_session_transaction(self, session: ClientSession) -> None:
        with self._lock:
            if self._connection is None:
                return
            if self._transaction_owner_session_id != session.session_id:
                return
            try:
                self._connection.rollback()
            finally:
                self._transaction_owner_session_id = None
                self._sync_session_state(session, transaction_active=False)

    @contextmanager
    def _bind_connection(self, conn: sqlite3.Connection):
        previous = getattr(self._thread_local, "connection", None)
        self._thread_local.connection = conn
        try:
            yield
        finally:
            self._thread_local.connection = previous

    def _session_owns_transaction(self, context: ClientSession | None) -> bool:
        return (
            context is not None
            and context.in_transaction
            and self._transaction_owner_session_id == context.session_id
        )

    def _require_connection(self, context: ClientSession | None = None) -> sqlite3.Connection:
        thread_bound = getattr(self._thread_local, "connection", None)
        if thread_bound is not None:
            return thread_bound
        if self._connection is None:
            raise RuntimeError("SQLiteEngine is not connected")
        if self._transaction_owner_session_id is not None and not self._session_owns_transaction(context):
            raise InvalidOperation("SQLiteEngine has an active transaction bound to another session")
        return self._connection

    def _begin_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self._session_owns_transaction(context):
            conn.execute("BEGIN")

    def _commit_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self._session_owns_transaction(context):
            conn.commit()

    def _rollback_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self._session_owns_transaction(context):
            conn.rollback()

    def _dialect_requires_python_fallback(self, dialect: MongoDialect) -> bool:
        return type(dialect) not in (MongoDialect70, MongoDialect80)

    @staticmethod
    def _coerce_ttl_datetime(value: object) -> datetime.datetime | None:
        if not isinstance(value, datetime.datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)

    def _document_expired_by_ttl(
        self,
        document: Document,
        index: EngineIndexRecord,
        *,
        now: datetime.datetime,
    ) -> bool:
        if index.expire_after_seconds is None or len(index.fields) != 1:
            return False
        values = QueryEngine.extract_values(document, index.fields[0])
        ttl_candidates = [
            candidate
            for candidate in (
                self._coerce_ttl_datetime(value)
                for value in values
            )
            if candidate is not None
        ]
        if not ttl_candidates:
            return False
        expires_at = min(ttl_candidates) + datetime.timedelta(seconds=index.expire_after_seconds)
        return expires_at <= now

    def _purge_expired_documents_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None,
    ) -> int:
        try:
            ttl_indexes = [
                index
                for index in self._load_indexes(db_name, coll_name)
                if index.expire_after_seconds is not None
            ]
        except (RuntimeError, TypeError, ValueError, AttributeError):
            return 0
        if not ttl_indexes:
            return 0
        now = datetime.datetime.now(datetime.timezone.utc)
        expired: list[tuple[str, Document]] = []
        for storage_key, document in self._load_documents(db_name, coll_name):
            if any(
                self._document_expired_by_ttl(document, index, now=now)
                for index in ttl_indexes
            ):
                expired.append((storage_key, document))
        if not expired:
            return 0
        self._begin_write(conn, context)
        try:
            for storage_key, _document in expired:
                conn.execute(
                    """
                    DELETE FROM documents
                    WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                    """,
                    (db_name, coll_name, storage_key),
                )
                self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                self._delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                self._delete_search_entries_for_storage_key(conn, db_name, coll_name, storage_key)
            self._commit_write(conn, context)
        except Exception:
            self._rollback_write(conn, context)
            raise
        self._invalidate_collection_features_cache(db_name, coll_name)
        return len(expired)

    def _storage_key(self, value: Any) -> str:
        return repr(canonical_document_id(value))

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _physical_index_name(self, db_name: str, coll_name: str, index_name: str) -> str:
        digest = hashlib.sha1(f"{db_name}:{coll_name}:{index_name}".encode("utf-8")).hexdigest()[:16]
        return f"idx_{digest}"

    def _physical_multikey_index_name(self, db_name: str, coll_name: str, index_name: str) -> str:
        digest = hashlib.sha1(f"{db_name}:{coll_name}:{index_name}:multikey".encode("utf-8")).hexdigest()[:16]
        return f"mkidx_{digest}"

    def _physical_scalar_index_name(self, db_name: str, coll_name: str, index_name: str) -> str:
        digest = hashlib.sha1(f"{db_name}:{coll_name}:{index_name}:scalar".encode("utf-8")).hexdigest()[:16]
        return f"scidx_{digest}"

    def _physical_search_index_name(self, db_name: str, coll_name: str, index_name: str) -> str:
        digest = hashlib.sha1(f"{db_name}:{coll_name}:{index_name}:search".encode("utf-8")).hexdigest()[:16]
        return f"fts_{digest}"

    def _sqlite_table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type IN ('table', 'view') AND name = ?
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    def _supports_fts5(self, conn: sqlite3.Connection) -> bool:
        if self._fts5_available is not None:
            return self._fts5_available
        try:
            compile_options = {
                row[0]
                for row in conn.execute("PRAGMA compile_options").fetchall()
            }
        except sqlite3.DatabaseError:
            compile_options = set()
        self._fts5_available = "ENABLE_FTS5" in compile_options
        return self._fts5_available

    @staticmethod
    def _is_builtin_id_index(keys: IndexKeySpec) -> bool:
        return keys == [("_id", 1)]

    def _builtin_id_hint_index(self) -> EngineIndexRecord:
        return EngineIndexRecord(
            name="_id_",
            physical_name=None,
            fields=["_id"],
            key=[("_id", 1)],
            unique=True,
            multikey=False,
            multikey_physical_name=None,
        )

    def _resolve_hint_index(
        self,
        db_name: str,
        coll_name: str,
        hint: str | IndexKeySpec | None,
        *,
        indexes: list[EngineIndexRecord] | None = None,
        plan: QueryNode | None = None,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> EngineIndexRecord | None:
        if hint is None:
            return None

        if isinstance(hint, str):
            if hint == "_id_":
                return self._builtin_id_hint_index()
        else:
            normalized_hint = normalize_index_keys(hint)
            if self._is_builtin_id_index(normalized_hint):
                return self._builtin_id_hint_index()

        if indexes is None:
            indexes = self._load_indexes(db_name, coll_name)

        for index in indexes:
            if isinstance(hint, str):
                if index["name"] == hint:
                    if plan is not None and not query_can_use_index(index, plan, dialect=dialect):
                        raise OperationFailure("hint does not correspond to a usable index for this query")
                    return deepcopy(index)
            else:
                if index["key"] == normalized_hint:
                    if plan is not None and not query_can_use_index(index, plan, dialect=dialect):
                        raise OperationFailure("hint does not correspond to a usable index for this query")
                    return deepcopy(index)

        raise OperationFailure("hint does not correspond to an existing index")

    def _serialize_document(self, document: Document) -> str:
        return json_dumps_compact(self._codec.encode(document), sort_keys=False)

    def _deserialize_document(
        self,
        payload: str,
        *,
        preserve_bson_wrappers: bool = True,
    ) -> Document:
        parsed = json_loads(payload)
        if DocumentCodec._MARKER not in payload:
            return parsed
        return self._decode_codec_payload(parsed, preserve_bson_wrappers=preserve_bson_wrappers)

    def _decode_codec_payload(
        self,
        payload: object,
        *,
        preserve_bson_wrappers: bool,
    ) -> Document:
        decode = self._codec.decode
        try:
            parameters = inspect.signature(decode).parameters
        except (TypeError, ValueError):
            parameters = {}
        if "preserve_bson_wrappers" not in parameters:
            return decode(payload)
        return decode(payload, preserve_bson_wrappers=preserve_bson_wrappers)

    def _load_documents(self, db_name: str, coll_name: str):
        conn = self._require_connection()
        cursor = conn.execute(
            """
            SELECT storage_key, document
            FROM documents
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        )
        try:
            for storage_key, document in cursor:
                yield storage_key, self._deserialize_document(document)
        finally:
            cursor.close()

    def _build_select_sql(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
        *,
        select_clause: str,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: str | IndexKeySpec | None = None,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> tuple[str, list[object]]:
        scalar_sort_sql = self._build_scalar_sort_select_sql(
            db_name,
            coll_name,
            plan,
            select_clause=select_clause,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
        )
        if scalar_sort_sql is not None:
            return scalar_sort_sql
        hinted_index = self._resolve_hint_index(db_name, coll_name, hint, plan=plan, dialect=dialect)
        where_fragment = self._translate_query_plan_with_multikey(db_name, coll_name, plan)
        from_clause = "documents"
        if hinted_index is not None and hinted_index.get("physical_name") is not None:
            from_clause = (
                "documents INDEXED BY "
                f"{self._quote_identifier(str(hinted_index['physical_name']))}"
            )
        statement = self._sql_translator.build_select_statement(
            select_clause=select_clause,
            from_clause=from_clause,
            namespace_sql="db_name = ? AND coll_name = ?",
            namespace_params=(db_name, coll_name),
            where_fragment=where_fragment,
            sort=sort,
            skip=skip,
            limit=limit,
        )
        return statement.sql, list(statement.params)

    def _field_has_uniform_scalar_sort_type_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> str | None:
        feature_key = (db_name, coll_name, f"uniform_scalar_sort_type:{field}")
        if feature_key in self._collection_features_cache:
            cached = self._collection_features_cache[feature_key]
            return cached if isinstance(cached, str) else None

        conn = self._require_connection()
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        row = conn.execute(
            f"""
            SELECT
                COUNT(*),
                SUM(CASE WHEN json_type(document, {path_literal}) IN ('integer', 'real') THEN 1 ELSE 0 END),
                SUM(CASE WHEN json_type(document, {path_literal}) = 'text' THEN 1 ELSE 0 END),
                SUM(CASE WHEN json_type(document, {path_literal}) IN ('true', 'false') THEN 1 ELSE 0 END)
            FROM documents
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        ).fetchone()
        if row is None:
            self._collection_features_cache[feature_key] = False
            return None
        total_count, numeric_count, string_count, bool_count = (int(value or 0) for value in row)
        if total_count == 0:
            self._collection_features_cache[feature_key] = False
            return None
        if numeric_count == total_count:
            self._collection_features_cache[feature_key] = "number"
            return "number"
        if string_count == total_count:
            self._collection_features_cache[feature_key] = "string"
            return "string"
        if bool_count == total_count:
            self._collection_features_cache[feature_key] = "bool"
            return "bool"
        self._collection_features_cache[feature_key] = False
        return None

    def _find_scalar_sort_index(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> EngineIndexRecord | None:
        for index in self._load_indexes(db_name, coll_name):
            if index["fields"] != [field]:
                continue
            if index.get("sparse") or index.get("partial_filter_expression"):
                continue
            if not index.get("scalar_physical_name"):
                continue
            return index
        return None

    def _resolve_select_clause_for_scalar_sort(self, select_clause: str) -> str:
        if select_clause == "document":
            return "documents.document"
        if select_clause == "storage_key, document":
            return "documents.storage_key, documents.document"
        return select_clause

    def _build_select_statement_with_custom_order(
        self,
        *,
        select_clause: str,
        from_clause: str,
        namespace_sql: str,
        namespace_params: tuple[object, ...],
        where_fragment: tuple[str, list[object]],
        order_sql: str,
        skip: int = 0,
        limit: int | None = None,
    ) -> tuple[str, list[object]]:
        where_sql, where_params = where_fragment
        sql = (
            f"SELECT {select_clause} "
            f"FROM {from_clause} "
            f"WHERE {namespace_sql} AND ({where_sql}) "
            f"{order_sql}"
        )
        params: list[object] = [*namespace_params, *where_params]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        elif skip:
            sql += " LIMIT -1"
        if skip:
            sql += " OFFSET ?"
            params.append(skip)
        return sql, params

    def _build_scalar_sort_select_sql(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
        *,
        select_clause: str,
        sort: SortSpec | None,
        skip: int,
        limit: int | None,
        hint: str | IndexKeySpec | None,
    ) -> tuple[str, list[object]] | None:
        if hint is not None or not sort or len(sort) != 1:
            return None

        field, direction = sort[0]
        if direction not in (1, -1):
            return None
        if self._field_traverses_array_in_collection(db_name, coll_name, field):
            return None
        if self._field_contains_tagged_bytes_in_collection(db_name, coll_name, field):
            return None
        if self._field_contains_tagged_undefined_in_collection(db_name, coll_name, field):
            return None
        if self._field_has_uniform_scalar_sort_type_in_collection(db_name, coll_name, field) is None:
            return None

        order = "ASC" if direction == 1 else "DESC"
        where_fragment = self._translate_query_plan_with_multikey(db_name, coll_name, plan)

        index = self._find_scalar_sort_index(db_name, coll_name, field)
        if index is not None and index.get("scalar_physical_name"):
            conn = self._require_connection()
            collection_id = self._lookup_collection_id(conn, db_name, coll_name)
            if collection_id is not None:
                return self._build_select_statement_with_custom_order(
                    select_clause=self._resolve_select_clause_for_scalar_sort(select_clause),
                    from_clause=(
                        f"scalar_index_entries INDEXED BY {self._quote_identifier(str(index['scalar_physical_name']))} "
                        "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
                        "AND documents.storage_key = scalar_index_entries.storage_key"
                    ),
                    namespace_sql="scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ?",
                    namespace_params=(db_name, coll_name, collection_id, index["name"]),
                    where_fragment=where_fragment,
                    order_sql=(
                        f" ORDER BY scalar_index_entries.element_key {order}, "
                        "documents.storage_key ASC"
                    ),
                    skip=skip,
                    limit=limit,
                )

        return self._build_select_statement_with_custom_order(
            select_clause=select_clause,
            from_clause="documents",
            namespace_sql="db_name = ? AND coll_name = ?",
            namespace_params=(db_name, coll_name),
            where_fragment=where_fragment,
            order_sql=f" ORDER BY {value_expression_sql(field)} {order}",
            skip=skip,
            limit=limit,
        )

    @staticmethod
    def _normalize_multikey_number(value: int | float) -> str:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise NotImplementedError("NaN and infinity are not supported in SQLite multikey indexes")
        return bson_numeric_index_key(value)

    @staticmethod
    def _multikey_value_signature(value: object) -> tuple[str, str] | None:
        if value is None:
            return ("null", "")
        if isinstance(value, bool):
            return ("bool", "1" if value else "0")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return ("number", SQLiteEngine._normalize_multikey_number(value))
        if isinstance(value, str):
            return ("string", value)

        encoded = DocumentCodec.encode(value)
        if not DocumentCodec._is_tagged_value(encoded):
            return None
        payload = encoded[DocumentCodec._MARKER]
        value_type = payload[DocumentCodec._TYPE]
        raw_value = payload[DocumentCodec._VALUE]
        if value_type in {"decimal128_public", "decimal128"}:
            return ("number", bson_numeric_index_key(Decimal(str(raw_value))))
        if value_type in {"datetime", "uuid", "objectid", "bytes"}:
            return (value_type, str(raw_value))
        if value_type == "undefined":
            return ("undefined", "1")
        return None

    @classmethod
    def _multikey_signatures_for_query_value(
        cls,
        value: object,
        *,
        null_matches_undefined: bool = False,
    ) -> tuple[tuple[str, str], ...]:
        signature = cls._multikey_value_signature(value)
        if signature is None:
            raise NotImplementedError("Unsupported multikey query value")
        if value is None and null_matches_undefined:
            return (("null", ""), ("undefined", "1"))
        return (signature,)

    @classmethod
    def _extract_multikey_entries(cls, document: Document, field: str) -> set[tuple[str, str]]:
        found, value = get_document_value(document, field)
        if not found or not isinstance(value, list):
            return set()
        entries: set[tuple[str, str]] = set()
        for item in value:
            signature = cls._multikey_value_signature(item)
            if signature is not None:
                entries.add(signature)
        return entries

    @staticmethod
    def _supports_multikey_index(fields: list[str], unique: bool) -> bool:
        return not unique and len(fields) == 1 and not path_array_prefixes(fields[0])

    @staticmethod
    def _supports_scalar_index(index: EngineIndexRecord) -> bool:
        return len(index["fields"]) == 1 and is_ordered_index_spec(index["key"])

    def _find_multikey_index(self, db_name: str, coll_name: str, field: str) -> EngineIndexRecord | None:
        for index in self._load_indexes(db_name, coll_name):
            if not index.get("multikey"):
                continue
            if index["fields"] == [field]:
                return index
        return None

    def _find_scalar_index(self, db_name: str, coll_name: str, field: str) -> EngineIndexRecord | None:
        for index in self._load_indexes(db_name, coll_name):
            if not self._supports_scalar_index(index):
                continue
            if index["fields"] == [field]:
                return index
        return None

    @staticmethod
    def _scalar_range_signature(value: object) -> tuple[str, int, str] | None:
        signature = SQLiteEngine._multikey_value_signature(value)
        if signature is None:
            return None
        element_type, element_key = signature
        if element_type not in {"number", "string", "bool"}:
            return None
        return element_type, SQLiteEngine._multikey_type_score(element_type), element_key

    def _find_scalar_fast_path_index(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> EngineIndexRecord | None:
        for index in self._load_indexes(db_name, coll_name):
            if index["key"] != [(field, 1)]:
                continue
            if index.get("sparse") or index.get("partial_filter_expression"):
                continue
            if not index.get("scalar_physical_name"):
                continue
            return index
        return None

    def _can_use_scalar_range_fast_path(
        self,
        db_name: str,
        coll_name: str,
        field: str,
        value: object,
    ) -> tuple[EngineIndexRecord, int, str] | None:
        if "." in field:
            return None
        if self._field_is_top_level_array_in_collection(db_name, coll_name, field):
            return None
        signature = self._scalar_range_signature(value)
        if signature is None:
            return None
        element_type, type_score, element_key = signature
        if self._field_has_comparison_type_mismatch_in_collection(
            db_name,
            coll_name,
            field,
            element_type,
        ):
            return None
        index = self._find_scalar_fast_path_index(db_name, coll_name, field)
        if index is None:
            return None
        return index, type_score, element_key

    def _lookup_collection_id(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        *,
        create: bool = False,
    ) -> int | None:
        cache_key = (db_name, coll_name)
        cached = self._collection_id_cache.get(cache_key)
        if cached is not None:
            return cached
        row = conn.execute(
            """
            SELECT collection_id, rowid
            FROM collections
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        ).fetchone()
        if row is None:
            if not create:
                return None
            self._ensure_collection_row(conn, db_name, coll_name)
            row = conn.execute(
                """
                SELECT collection_id, rowid
                FROM collections
                WHERE db_name = ? AND coll_name = ?
                """,
                (db_name, coll_name),
            ).fetchone()
        if row is None:
            return None
        if not isinstance(row, (tuple, list)) or len(row) < 2:
            return None
        collection_id, rowid = row
        if collection_id is None or int(collection_id) == 0:
            collection_id = int(rowid)
            conn.execute(
                """
                UPDATE collections
                SET collection_id = ?
                WHERE db_name = ? AND coll_name = ?
                """,
                (collection_id, db_name, coll_name),
            )
        resolved = int(collection_id)
        self._collection_id_cache[cache_key] = resolved
        return resolved

    def _translate_multikey_exists_clause(
        self,
        collection_id: int,
        db_name: str,
        coll_name: str,
        index: EngineIndexRecord,
        signatures: tuple[tuple[str, str], ...],
    ) -> tuple[str, list[object]]:
        physical_name = self._quote_identifier(str(index["multikey_physical_name"]))
        clauses: list[str] = []
        params: list[object] = []
        for element_type, element_key in signatures:
            clauses.append(
                "EXISTS ("
                f"SELECT 1 FROM multikey_entries INDEXED BY {physical_name} "
                "WHERE collection_id = ? AND index_name = ? "
                "AND storage_key = documents.storage_key "
                "AND element_type = ? AND element_key = ?)"
            )
            params.extend([collection_id, index["name"], element_type, element_key])
        return "(" + " OR ".join(clauses) + ")", params

    def _translate_multikey_ordered_exists_clause(
        self,
        collection_id: int,
        db_name: str,
        coll_name: str,
        index: EngineIndexRecord,
        *,
        element_type: str,
        element_key: str,
        operator: str,
    ) -> tuple[str, list[object]]:
        physical_name = self._quote_identifier(str(index["multikey_physical_name"]))
        type_score = self._multikey_type_score(element_type)
        if operator in (">", ">="):
            comparison_sql = (
                "(type_score > ? OR (type_score = ? AND element_key "
                f"{operator} ?))"
            )
            params_tail = [type_score, type_score, element_key]
        else:
            comparison_sql = (
                "(type_score < ? OR (type_score = ? AND element_key "
                f"{operator} ?))"
            )
            params_tail = [type_score, type_score, element_key]
        return (
            "EXISTS ("
            f"SELECT 1 FROM multikey_entries INDEXED BY {physical_name} "
            "WHERE collection_id = ? AND index_name = ? "
            "AND storage_key = documents.storage_key "
            "AND "
            f"{comparison_sql})",
            [collection_id, index["name"], *params_tail],
        )

    def _translate_equals_with_multikey(
        self,
        collection_id: int,
        db_name: str,
        coll_name: str,
        plan: EqualsCondition,
        index: EngineIndexRecord,
    ) -> tuple[str, list[object]]:
        scalar_sql, scalar_params = _translate_scalar_equals(
            plan.field,
            plan.value,
            null_matches_undefined=plan.null_matches_undefined,
        )
        array_signatures = self._multikey_signatures_for_query_value(
            plan.value,
            null_matches_undefined=plan.null_matches_undefined,
        )
        array_sql, array_params = self._translate_multikey_exists_clause(
            collection_id,
            db_name,
            coll_name,
            index,
            array_signatures,
        )
        return f"(({scalar_sql}) OR {array_sql})", [*scalar_params, *array_params]

    def _field_has_comparison_type_mismatch_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
        expected_type: str,
    ) -> bool:
        feature_key = (db_name, coll_name, f"comparison_mismatch:{field}:{expected_type}")
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        if expected_type == "number":
            mismatch_predicate = "json_type(document, {path}) IS NOT NULL AND json_type(document, {path}) NOT IN ('integer', 'real')"
        elif expected_type == "string":
            mismatch_predicate = "json_type(document, {path}) IS NOT NULL AND json_type(document, {path}) != 'text'"
        elif expected_type == "bool":
            mismatch_predicate = "json_type(document, {path}) IS NOT NULL AND json_type(document, {path}) NOT IN ('true', 'false')"
        else:
            # Tagged comparable values still need full BSON type ordering in SQL.
            self._collection_features_cache[feature_key] = True
            return True
        conn = self._require_connection()
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        row = conn.execute(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND ({mismatch_predicate.format(path=path_literal)})
            LIMIT 1
            """,
            (db_name, coll_name),
        ).fetchone()
        result = row is not None
        self._collection_features_cache[feature_key] = result
        return result

    def _translate_in_with_multikey(
        self,
        collection_id: int,
        db_name: str,
        coll_name: str,
        plan: InCondition,
        index: EngineIndexRecord,
    ) -> tuple[str, list[object]]:
        clauses: list[str] = []
        params: list[object] = []
        for value in plan.values:
            eq_sql, eq_params = self._translate_equals_with_multikey(
                collection_id,
                db_name,
                coll_name,
                EqualsCondition(
                    plan.field,
                    value,
                    null_matches_undefined=plan.null_matches_undefined,
                ),
                index,
            )
            clauses.append(eq_sql)
            params.extend(eq_params)
        return " OR ".join(clauses), params

    def _translate_range_with_multikey(
        self,
        collection_id: int,
        db_name: str,
        coll_name: str,
        plan: GreaterThanCondition | GreaterThanOrEqualCondition | LessThanCondition | LessThanOrEqualCondition,
        index: EngineIndexRecord,
        *,
        operator: str,
    ) -> tuple[str, list[object]]:
        signature = self._multikey_value_signature(plan.value)
        if signature is None or signature[0] != "number":
            return translate_query_plan(plan)
        scalar_sql, scalar_params = translate_query_plan(plan)
        array_sql, array_params = self._translate_multikey_ordered_exists_clause(
            collection_id,
            db_name,
            coll_name,
            index,
            element_type=signature[0],
            element_key=signature[1],
            operator=operator,
        )
        return f"(({scalar_sql}) OR {array_sql})", [*scalar_params, *array_params]

    def _translate_query_plan_with_multikey(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> tuple[str, list[object]]:
        if isinstance(plan, MatchAll):
            return "1 = 1", []
        if isinstance(plan, EqualsCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                if not self._field_is_top_level_array_in_collection(db_name, coll_name, plan.field):
                    return _translate_equals_scalar_only(
                        plan.field,
                        plan.value,
                        null_matches_undefined=plan.null_matches_undefined,
                    )
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(self._require_connection(), db_name, coll_name)
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_equals_with_multikey(collection_id, db_name, coll_name, plan, index)
        if isinstance(plan, InCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(self._require_connection(), db_name, coll_name)
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_in_with_multikey(collection_id, db_name, coll_name, plan, index)
        if isinstance(plan, GreaterThanCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison(">", plan.field, plan.value)
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(self._require_connection(), db_name, coll_name)
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(collection_id, db_name, coll_name, plan, index, operator=">")
        if isinstance(plan, GreaterThanOrEqualCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison(">=", plan.field, plan.value)
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(self._require_connection(), db_name, coll_name)
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(collection_id, db_name, coll_name, plan, index, operator=">=")
        if isinstance(plan, LessThanCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison("<", plan.field, plan.value)
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(self._require_connection(), db_name, coll_name)
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(collection_id, db_name, coll_name, plan, index, operator="<")
        if isinstance(plan, LessThanOrEqualCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison("<=", plan.field, plan.value)
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(self._require_connection(), db_name, coll_name)
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(collection_id, db_name, coll_name, plan, index, operator="<=")
        if isinstance(plan, AndCondition):
            fragments = [self._translate_query_plan_with_multikey(db_name, coll_name, clause) for clause in plan.clauses]
            sql = " AND ".join(f"({fragment_sql})" for fragment_sql, _ in fragments)
            params: list[object] = []
            for _, fragment_params in fragments:
                params.extend(fragment_params)
            return sql, params
        if isinstance(plan, OrCondition):
            fragments = [self._translate_query_plan_with_multikey(db_name, coll_name, clause) for clause in plan.clauses]
            sql = " OR ".join(f"({fragment_sql})" for fragment_sql, _ in fragments)
            params: list[object] = []
            for _, fragment_params in fragments:
                params.extend(fragment_params)
            return sql, params
        if isinstance(plan, NotCondition):
            clause_sql, clause_params = self._translate_query_plan_with_multikey(db_name, coll_name, plan.clause)
            return f"NOT COALESCE(({clause_sql}), 0)", clause_params
        return translate_query_plan(plan)

    def _sort_requires_python(self, db_name: str, coll_name: str, plan: QueryNode, sort: SortSpec | None) -> bool:
        if not sort:
            return False

        conn = self._require_connection()
        if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
            return True
        where_sql, params = translate_query_plan(plan)
        for field, _direction in sort:
            if self._field_traverses_array_in_collection(db_name, coll_name, field):
                return True
            if self._field_contains_tagged_bytes_in_collection(db_name, coll_name, field):
                return True
            if self._field_contains_tagged_undefined_in_collection(db_name, coll_name, field):
                return True
            path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
            row = conn.execute(
                f"""
                SELECT 1
                FROM documents
                WHERE db_name = ? AND coll_name = ? AND ({where_sql})
                  AND (
                    json_type(document, {path_literal}) = 'array'
                    OR (
                        json_type(document, {path_literal}) = 'object'
                        AND {type_expression_sql(field)} = ''
                    )
                  )
                LIMIT 1
                """,
                (db_name, coll_name, *params),
            ).fetchone()
            if row is not None:
                return True
        return False

    @staticmethod
    def _plan_fields(plan: QueryNode) -> set[str]:
        if isinstance(plan, MatchAll):
            return set()
        if hasattr(plan, "field"):
            field = getattr(plan, "field")
            if isinstance(field, str):
                return {field}
        if hasattr(plan, "clause"):
            return SQLiteEngine._plan_fields(getattr(plan, "clause"))
        if hasattr(plan, "clauses"):
            fields: set[str] = set()
            for clause in getattr(plan, "clauses"):
                fields.update(SQLiteEngine._plan_fields(clause))
            return fields
        return set()

    def _field_traverses_array_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        feature_key = (db_name, coll_name, f"traverses_array:{field}")
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        prefixes = path_array_prefixes(field)
        if not prefixes:
            return False
        conn = self._require_connection()
        result = False
        for prefix in prefixes:
            path_literal = "'" + json_path_for_field(prefix).replace("'", "''") + "'"
            row = conn.execute(
                f"""
                SELECT 1
                FROM documents
                WHERE db_name = ? AND coll_name = ?
                  AND json_type(document, {path_literal}) = 'array'
                LIMIT 1
                """,
                (db_name, coll_name),
            ).fetchone()
            if row is not None:
                result = True
                break
        self._collection_features_cache[feature_key] = result
        return result

    def _plan_has_array_traversing_paths(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_traverses_array_in_collection(db_name, coll_name, field)
            for field in self._plan_fields(plan)
        )

    def _field_traverses_dbref_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        if "." not in field:
            return False
        feature_key = (db_name, coll_name, f"traverses_dbref:{field}")
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        conn = self._require_connection()
        parts = field.split(".")
        result = False
        for prefix_length in range(1, len(parts)):
            prefix = ".".join(parts[:prefix_length])
            tagged_type_path = json_path_for_field(prefix) + f'."{DocumentCodec._MARKER}".{DocumentCodec._TYPE}'
            row = conn.execute(
                """
                SELECT 1
                FROM documents
                WHERE db_name = ? AND coll_name = ?
                  AND json_extract(document, ?) = 'dbref'
                LIMIT 1
                """,
                (db_name, coll_name, tagged_type_path),
            ).fetchone()
            if row is not None:
                result = True
                break
        self._collection_features_cache[feature_key] = result
        return result

    def _plan_requires_python_for_dbref_paths(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_traverses_dbref_in_collection(db_name, coll_name, field)
            for field in self._plan_fields(plan)
        )

    @staticmethod
    def _comparison_fields(plan: QueryNode) -> set[str]:
        if isinstance(plan, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)):
            return {plan.field}
        if isinstance(plan, NotCondition):
            return SQLiteEngine._comparison_fields(plan.clause)
        if isinstance(plan, (AndCondition, OrCondition)):
            fields: set[str] = set()
            for clause in plan.clauses:
                fields.update(SQLiteEngine._comparison_fields(clause))
            return fields
        return set()

    def _field_contains_tagged_value_type_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
        value_type: str,
    ) -> bool:
        feature_key = (db_name, coll_name, f"tagged_type:{field}:{value_type}")
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        conn = self._require_connection()
        tagged_type_path = json_path_for_field(field) + f'."{DocumentCodec._MARKER}".{DocumentCodec._TYPE}'
        row = conn.execute(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND json_extract(document, ?) = ?
            LIMIT 1
            """,
            (db_name, coll_name, tagged_type_path, value_type),
        ).fetchone()
        result = row is not None
        self._collection_features_cache[feature_key] = result
        return result

    def _field_contains_tagged_bytes_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        return self._field_contains_tagged_value_type_in_collection(db_name, coll_name, field, 'bytes')

    def _field_contains_tagged_undefined_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        return self._field_contains_tagged_value_type_in_collection(db_name, coll_name, field, 'undefined')

    def _plan_requires_python_for_bytes(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_contains_tagged_bytes_in_collection(db_name, coll_name, field)
            for field in self._comparison_fields(plan)
        )

    def _plan_requires_python_for_undefined(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_contains_tagged_undefined_in_collection(db_name, coll_name, field)
            for field in self._comparison_fields(plan)
        )

    def _field_is_top_level_array_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        feature_key = (db_name, coll_name, f"top_level_array:{field}")
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        conn = self._require_connection()
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        row = conn.execute(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND json_type(document, {path_literal}) = 'array'
            LIMIT 1
            """,
            (db_name, coll_name),
        ).fetchone()
        result = row is not None
        self._collection_features_cache[feature_key] = result
        return result

    def _plan_requires_python_for_array_comparisons(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_is_top_level_array_in_collection(db_name, coll_name, field)
            for field in self._comparison_fields(plan)
        )

    @staticmethod
    def _document_traverses_array_on_field(document: Document, field: str) -> bool:
        for prefix in path_array_prefixes(field):
            found, value = get_document_value(document, prefix)
            if found and isinstance(value, list):
                return True
        return False

    @staticmethod
    def _typed_engine_key(value: Any) -> Any:
        return bson_engine_key(value)

    def _validate_document_against_unique_indexes(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        *,
        exclude_storage_key: str | None = None,
    ) -> None:
        conn = self._require_connection()
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        for index in self._load_indexes(db_name, coll_name):
            if not index["unique"]:
                continue
            if not document_in_virtual_index(document, index):
                continue
            fields = index["fields"]
            if any(self._document_traverses_array_on_field(document, field) for field in fields):
                raise OperationFailure("SQLite unique indexes do not support paths that traverse arrays")
            scalar_conflict = self._unique_index_conflict_via_scalar_entries(
                conn,
                collection_id,
                index,
                document,
                exclude_storage_key=exclude_storage_key,
            )
            if scalar_conflict is True:
                raise DuplicateKeyError(
                    f"Duplicate key for unique index '{index['name']}': {fields}={tuple(self._index_value(document, field) for field in fields)!r}"
                )
            if scalar_conflict is False:
                continue
            key = tuple(self._index_value(document, field) for field in fields)
            for storage_key, existing_document in self._load_documents(db_name, coll_name):
                if exclude_storage_key is not None and storage_key == exclude_storage_key:
                    continue
                if not document_in_virtual_index(existing_document, index):
                    continue
                existing_key = tuple(self._index_value(existing_document, field) for field in fields)
                if existing_key == key:
                    raise DuplicateKeyError(
                        f"Duplicate key for unique index '{index['name']}': {fields}={key!r}"
                    )

    def _unique_index_conflict_via_scalar_entries(
        self,
        conn: sqlite3.Connection,
        collection_id: int | None,
        index: EngineIndexRecord,
        document: Document,
        *,
        exclude_storage_key: str | None,
    ) -> bool | None:
        if collection_id is None or not self._supports_scalar_index(index):
            return None
        physical_name = index.get("scalar_physical_name")
        if not physical_name:
            return None
        field = index["fields"][0]
        signature = self._scalar_value_signature_for_document(document, field)
        if signature is None:
            return None
        element_type, element_key = signature
        params: list[object] = [
            collection_id,
            str(index["name"]),
            self._multikey_type_score(element_type),
            element_key,
        ]
        sql = (
            f"SELECT 1 FROM scalar_index_entries INDEXED BY {self._quote_identifier(str(physical_name))} "
            "WHERE collection_id = ? AND index_name = ? AND type_score = ? AND element_key = ?"
        )
        if exclude_storage_key is not None:
            sql += " AND storage_key != ?"
            params.append(exclude_storage_key)
        sql += " LIMIT 1"
        return conn.execute(sql, tuple(params)).fetchone() is not None

    def _index_value(self, document: Document, field: str) -> Any:
        values = QueryEngine.extract_values(document, field)
        if not values:
            return self._typed_engine_key(None)
        return self._typed_engine_key(values[0])

    def _select_first_document_for_plan(self, db_name: str, coll_name: str, plan: QueryNode, *, hint: str | IndexKeySpec | None = None) -> tuple[str, Document] | None:
        if isinstance(plan, EqualsCondition) and "." not in plan.field:
            if plan.field == "_id":
                conn = self._require_connection()
                storage_key = self._storage_key(plan.value)
                row = conn.execute(
                    "SELECT storage_key, document FROM documents "
                    "WHERE db_name = ? AND coll_name = ? AND storage_key = ?",
                    (db_name, coll_name, storage_key),
                ).fetchone()
                if row is None:
                    return None
                storage_key, document = row
                return storage_key, self._deserialize_document(document)
            selected = self._select_first_document_for_scalar_index(
                db_name,
                coll_name,
                field=plan.field,
                value=plan.value,
                null_matches_undefined=plan.null_matches_undefined,
            )
            if selected is not None:
                return selected
        if isinstance(plan, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)) and "." not in plan.field:
            operator = (
                ">" if isinstance(plan, GreaterThanCondition)
                else ">=" if isinstance(plan, GreaterThanOrEqualCondition)
                else "<" if isinstance(plan, LessThanCondition)
                else "<="
            )
            selected = self._select_first_document_for_scalar_range(
                db_name,
                coll_name,
                field=plan.field,
                value=plan.value,
                operator=operator,
            )
            if selected is not None:
                return selected
        conn = self._require_connection()
        execution_plan = self._compile_read_execution_plan(
            db_name,
            coll_name,
            compile_find_semantics({}, plan=plan, limit=1),
            select_clause="storage_key, document",
            hint=hint,
        )
        sql, params = execution_plan.require_sql()
        row = conn.execute(sql, tuple(params)).fetchone()
        if row is None:
            return None
        storage_key, document = row
        return storage_key, self._deserialize_document(document)

    def _select_first_document_for_scalar_index(
        self,
        db_name: str,
        coll_name: str,
        *,
        field: str,
        value: Any,
        null_matches_undefined: bool = False,
    ) -> tuple[str, Document] | None:
        if self._field_is_top_level_array_in_collection(db_name, coll_name, field):
            return None
        index = self._find_scalar_index(db_name, coll_name, field)
        if index is None or not index.get("scalar_physical_name"):
            return None
        conn = self._require_connection()
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        if collection_id is None:
            return None
        try:
            signatures = self._multikey_signatures_for_query_value(
                value,
                null_matches_undefined=null_matches_undefined,
            )
        except NotImplementedError:
            return None
        filters = " OR ".join("(scalar_index_entries.type_score = ? AND scalar_index_entries.element_key = ?)" for _ in signatures)
        params: list[object] = [db_name, coll_name, collection_id, index["name"]]
        for element_type, element_key in signatures:
            params.extend([self._multikey_type_score(element_type), element_key])
        row = conn.execute(
            "SELECT documents.storage_key, documents.document "
            f"FROM scalar_index_entries INDEXED BY {self._quote_identifier(str(index['scalar_physical_name']))} "
            "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
            "AND documents.storage_key = scalar_index_entries.storage_key "
            "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
            f"AND ({filters}) LIMIT 1",
            tuple(params),
        ).fetchone()
        if row is None:
            return None
        storage_key, document = row
        return storage_key, self._deserialize_document(document)

    def _build_scalar_indexed_top_level_range_sql(
        self,
        db_name: str,
        coll_name: str,
        *,
        field: str,
        value: Any,
        operator: str,
        index_name: str,
        physical_name: str,
        limit: int | None = None,
    ) -> tuple[str, tuple[object, ...]] | None:
        scalar_path = self._can_use_scalar_range_fast_path(
            db_name,
            coll_name,
            field,
            value,
        )
        if scalar_path is None:
            return None
        _index, type_score, element_key = scalar_path
        conn = self._require_connection()
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        if collection_id is None:
            return None
        sql = (
            "SELECT documents.document "
            f"FROM scalar_index_entries INDEXED BY {self._quote_identifier(physical_name)} "
            "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
            "AND documents.storage_key = scalar_index_entries.storage_key "
            "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
            "AND scalar_index_entries.type_score = ? "
            f"AND scalar_index_entries.element_key {operator} ?"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return sql, (db_name, coll_name, collection_id, index_name, type_score, element_key)

    def _select_first_document_for_scalar_range(
        self,
        db_name: str,
        coll_name: str,
        *,
        field: str,
        value: Any,
        operator: str,
    ) -> tuple[str, Document] | None:
        scalar_path = self._can_use_scalar_range_fast_path(
            db_name,
            coll_name,
            field,
            value,
        )
        if scalar_path is None:
            return None
        index, type_score, element_key = scalar_path
        if not index.get("scalar_physical_name"):
            return None
        conn = self._require_connection()
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        if collection_id is None:
            return None
        row = conn.execute(
            "SELECT documents.storage_key, documents.document "
            f"FROM scalar_index_entries INDEXED BY {self._quote_identifier(str(index['scalar_physical_name']))} "
            "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
            "AND documents.storage_key = scalar_index_entries.storage_key "
            "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
            "AND scalar_index_entries.type_score = ? "
            f"AND scalar_index_entries.element_key {operator} ? "
            "LIMIT 1",
            (db_name, coll_name, collection_id, index["name"], type_score, element_key),
        ).fetchone()
        if row is None:
            return None
        storage_key, document = row
        return storage_key, self._deserialize_document(document)

    def _compile_read_execution_plan(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        select_clause: str = "document",
        hint: str | IndexKeySpec | None = None,
    ) -> SQLiteReadExecutionPlan:
        return compile_sqlite_read_execution_plan(
            db_name=db_name,
            coll_name=coll_name,
            semantics=semantics,
            select_clause=select_clause,
            hint=hint,
            dialect_requires_python_fallback=self._dialect_requires_python_fallback,
            plan_has_array_traversing_paths=self._plan_has_array_traversing_paths,
            plan_requires_python_for_dbref_paths=self._plan_requires_python_for_dbref_paths,
            plan_requires_python_for_array_comparisons=self._plan_requires_python_for_array_comparisons,
            plan_requires_python_for_undefined=self._plan_requires_python_for_undefined,
            plan_requires_python_for_bytes=self._plan_requires_python_for_bytes,
            sort_requires_python=self._sort_requires_python,
            build_select_sql=self._build_select_sql,
        )

    async def plan_find_execution(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> EngineReadExecutionPlan:
        semantics = compile_find_semantics_from_operation(operation, dialect=dialect)
        return await self.plan_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
        )

    def _plan_find_semantics_sync(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
    ) -> EngineReadExecutionPlan:
        query_plan = semantics.query_plan
        if semantics.collation is None and semantics.sort is None and semantics.skip == 0:
            if (
                isinstance(query_plan, EqualsCondition)
                and "." not in query_plan.field
                and (semantics.limit is None or semantics.limit >= 1)
            ):
                field = query_plan.field
                if field == "_id":
                    storage_key = self._storage_key(query_plan.value)
                    sql = (
                        "SELECT document FROM documents "
                        "WHERE db_name = ? AND coll_name = ? AND storage_key = ?"
                    )
                    if semantics.limit is not None:
                        sql += f" LIMIT {int(semantics.limit)}"
                    params = (db_name, coll_name, storage_key)
                    return SQLiteReadExecutionPlan(
                        semantics=semantics,
                        strategy="sql",
                        execution_lineage=(),
                        physical_plan=(),
                        use_sql=True,
                        sql=sql,
                        params=params,
                    )

                index = self._find_scalar_fast_path_index(db_name, coll_name, field)
                if index is not None and not self._field_is_top_level_array_in_collection(db_name, coll_name, field):
                    indexed_sql = self._build_scalar_indexed_top_level_equals_sql(
                        db_name,
                        coll_name,
                        field=field,
                        value=query_plan.value,
                        index_name=str(index["name"]),
                        physical_name=str(index["scalar_physical_name"]),
                        limit=semantics.limit,
                        null_matches_undefined=query_plan.null_matches_undefined,
                    )
                    if indexed_sql is not None:
                        sql, params = indexed_sql
                        return SQLiteReadExecutionPlan(
                            semantics=semantics,
                            strategy="sql",
                            execution_lineage=(),
                            physical_plan=(),
                            use_sql=True,
                            sql=sql,
                            params=params,
                        )

            if isinstance(query_plan, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)):
                operator = (
                    ">" if isinstance(query_plan, GreaterThanCondition)
                    else ">=" if isinstance(query_plan, GreaterThanOrEqualCondition)
                    else "<" if isinstance(query_plan, LessThanCondition)
                    else "<="
                )
                index = self._find_scalar_fast_path_index(db_name, coll_name, query_plan.field)
                if index is not None:
                    indexed_sql = self._build_scalar_indexed_top_level_range_sql(
                        db_name,
                        coll_name,
                        field=query_plan.field,
                        value=query_plan.value,
                        operator=operator,
                        index_name=str(index["name"]),
                        physical_name=str(index["scalar_physical_name"]),
                        limit=semantics.limit,
                    )
                    if indexed_sql is not None:
                        sql, params = indexed_sql
                        return SQLiteReadExecutionPlan(
                            semantics=semantics,
                            strategy="sql",
                            execution_lineage=(),
                            physical_plan=(),
                            use_sql=True,
                            sql=sql,
                            params=params,
                        )

        return self._compile_read_execution_plan(
            db_name,
            coll_name,
            semantics,
            hint=semantics.hint,
        )

    async def plan_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> EngineReadExecutionPlan:
        del context
        return await self._run_blocking(
            self._plan_find_semantics_sync,
            db_name,
            coll_name,
            semantics,
        )

    def _serialize_value_for_sql(self, value: Any) -> Any:
        """Serialize a value for direct JSON comparisons in SQL."""
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        return self._codec.encode({"v": value})["v"]

    def _build_scalar_indexed_top_level_equals_sql(
        self,
        db_name: str,
        coll_name: str,
        *,
        field: str,
        value: Any,
        index_name: str,
        physical_name: str,
        limit: int | None = None,
        null_matches_undefined: bool = False,
    ) -> tuple[str, tuple[object, ...]] | None:
        conn = self._require_connection()
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        if collection_id is None:
            return None
        try:
            signatures = self._multikey_signatures_for_query_value(
                value,
                null_matches_undefined=null_matches_undefined,
            )
        except NotImplementedError:
            return None
        if not signatures:
            return None
        filters = " OR ".join("(scalar_index_entries.type_score = ? AND scalar_index_entries.element_key = ?)" for _ in signatures)
        params: list[object] = [db_name, coll_name, collection_id, index_name]
        for element_type, element_key in signatures:
            params.extend([self._multikey_type_score(element_type), element_key])
        sql = (
            "SELECT documents.document "
            f"FROM scalar_index_entries INDEXED BY {self._quote_identifier(physical_name)} "
            "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
            "AND documents.storage_key = scalar_index_entries.storage_key "
            "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
            f"AND ({filters})"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return sql, tuple(params)

    def _explain_query_plan_sync(
        self,
        db_name: str,
        coll_name: str,
        semantics_or_filter_spec: EngineFindSemantics | Filter | None = None,
        *,
        plan: QueryNode | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: str | IndexKeySpec | None = None,
        max_time_ms: int | None = None,
        dialect: MongoDialect | None = None,
    ) -> list[str]:
        semantics = (
            semantics_or_filter_spec
            if isinstance(semantics_or_filter_spec, EngineFindSemantics)
            else compile_find_semantics(
                semantics_or_filter_spec,
                plan=plan,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                max_time_ms=max_time_ms,
                dialect=dialect,
            )
        )
        deadline = semantics.deadline
        with self._lock:
            conn = self._require_connection()
            enforce_deadline(deadline)
            execution_plan = self._plan_find_semantics_sync(
                db_name,
                coll_name,
                semantics,
            )
            sql, params = execution_plan.require_sql()
            rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", tuple(params)).fetchall()
        enforce_deadline(deadline)
        return [str(row[3]) for row in rows]

    def _load_indexes(self, db_name: str, coll_name: str) -> list[EngineIndexRecord]:
        cache_key = (db_name, coll_name)
        cached = self._index_cache.get(cache_key)
        cache_version = self._index_cache_version(db_name, coll_name)
        if cached is not None and cached[0] == cache_version:
            return deepcopy(cached[1])
        conn = self._require_connection()
        cursor = conn.execute(
            """
            SELECT name, physical_name, fields, keys, unique_flag, sparse_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name
            FROM indexes
            WHERE db_name = ? AND coll_name = ?
            ORDER BY name
            """,
            (db_name, coll_name),
        )
        indexes: list[EngineIndexRecord] = []
        for row in cursor.fetchall():
            if len(row) == 9:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    partial_filter_json,
                    multikey_flag,
                    multikey_physical_name,
                ) = row
                expire_after_seconds = None
                scalar_physical_name = None
            elif len(row) == 10:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    partial_filter_json,
                    multikey_flag,
                    multikey_physical_name,
                ) = row
                expire_after_seconds = None
                scalar_physical_name = None
            else:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    partial_filter_json,
                    expire_after_seconds,
                    multikey_flag,
                    multikey_physical_name,
                    scalar_physical_name,
                ) = row
            try:
                parsed_fields = json_loads(fields)
            except json.JSONDecodeError as exc:
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}"
                ) from exc
            if not isinstance(parsed_fields, list) or not all(isinstance(field, str) for field in parsed_fields):
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}"
                )
            if keys is None:
                parsed_keys = [(field, 1) for field in parsed_fields]
            else:
                try:
                    parsed_keys = normalize_index_keys(json_loads(keys))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}"
                    ) from exc
            partial_filter_expression: Filter | None = None
            if partial_filter_json is not None:
                try:
                    partial_filter_expression = normalize_partial_filter_expression(json_loads(partial_filter_json))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}"
                    ) from exc
            indexes.append(
                EngineIndexRecord(
                    name=name,
                    physical_name=physical_name or self._physical_index_name(db_name, coll_name, name),
                    fields=parsed_fields,
                    key=parsed_keys,
                    unique=bool(unique_flag),
                    sparse=bool(sparse_flag),
                    partial_filter_expression=partial_filter_expression,
                    expire_after_seconds=(
                        int(expire_after_seconds)
                        if expire_after_seconds is not None
                        else None
                    ),
                    multikey=bool(multikey_flag),
                    multikey_physical_name=multikey_physical_name
                    or self._physical_multikey_index_name(db_name, coll_name, name),
                    scalar_physical_name=scalar_physical_name
                    or (
                        self._physical_scalar_index_name(db_name, coll_name, name)
                        if len(parsed_fields) == 1
                        else None
                    ),
                )
            )
        self._ensure_multikey_physical_indexes_sync(conn, indexes)
        self._index_cache[cache_key] = (cache_version, deepcopy(indexes))
        return indexes

    def _build_multikey_rows_for_document(
        self,
        storage_key: str,
        document: Document,
        indexes: list[EngineIndexRecord],
    ) -> list[tuple[str, str, int, str]]:
        rows: list[tuple[str, str, int, str]] = []
        for index in indexes:
            if not index.get("multikey"):
                continue
            field = index["fields"][0]
            for element_type, element_key in self._extract_multikey_entries(document, field):
                rows.append(
                    (
                        str(index["name"]),
                        element_type,
                        self._multikey_type_score(element_type),
                        element_key,
                    )
                )
        return rows

    def _scalar_value_signature_for_document(
        self,
        document: Document,
        field: str,
    ) -> tuple[str, str] | None:
        found, value = get_document_value(document, field)
        if not found or isinstance(value, dict | list):
            return None
        return self._multikey_value_signature(value)

    def _build_scalar_rows_for_document(
        self,
        storage_key: str,
        document: Document,
        indexes: list[EngineIndexRecord],
    ) -> list[tuple[str, str, int, str]]:
        rows: list[tuple[str, str, int, str]] = []
        for index in indexes:
            if not self._supports_scalar_index(index):
                continue
            if not document_in_virtual_index(document, index):
                continue
            signature = self._scalar_value_signature_for_document(document, index["fields"][0])
            if signature is None:
                continue
            rows.append(
                (
                    str(index["name"]),
                    signature[0],
                    self._multikey_type_score(signature[0]),
                    signature[1],
                )
            )
        return rows

    def _load_search_index_rows(
        self,
        db_name: str,
        coll_name: str,
        *,
        name: str | None = None,
    ) -> list[tuple[SearchIndexDefinition, str | None, float | None]]:
        conn = self._require_connection()
        sql = """
            SELECT name, index_type, definition_json, physical_name, ready_at_epoch
            FROM search_indexes
            WHERE db_name = ? AND coll_name = ?
        """
        params: list[object] = [db_name, coll_name]
        if name is not None:
            sql += " AND name = ?"
            params.append(name)
        sql += " ORDER BY name"
        cursor = conn.execute(sql, tuple(params))
        if cursor is None:
            return []
        try:
            rows = cursor.fetchall()
            if not isinstance(rows, list):
                return []
            return [
                (
                    SearchIndexDefinition(
                        json_loads(definition_json),
                        name=row_name,
                        index_type=index_type,
                    ),
                    physical_name,
                    ready_at_epoch,
                )
                for row_name, index_type, definition_json, physical_name, ready_at_epoch in rows
            ]
        finally:
            close = getattr(cursor, "close", None)
            if callable(close):
                close()

    def _load_search_indexes(self, db_name: str, coll_name: str) -> list[SearchIndexDefinition]:
        return [definition for definition, _physical_name, _ready_at_epoch in self._load_search_index_rows(db_name, coll_name)]

    def _pending_search_index_ready_at(self) -> float | None:
        if self._simulate_search_index_latency <= 0:
            return None
        return time.time() + self._simulate_search_index_latency

    def _search_index_is_ready_sync(self, ready_at_epoch: float | None) -> bool:
        return ready_at_epoch is None or time.time() >= ready_at_epoch

    def _drop_search_backend_sync(self, conn: sqlite3.Connection, physical_name: str | None) -> None:
        if not physical_name:
            return
        conn.execute(f"DROP TABLE IF EXISTS {self._quote_identifier(physical_name)}")
        self._ensured_search_backends.discard(physical_name)

    def _ensure_search_backend_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        physical_name: str | None,
    ) -> str | None:
        if definition.index_type != "search":
            return None
        resolved_physical_name = physical_name or self._physical_search_index_name(
            db_name,
            coll_name,
            definition.name,
        )
        if not self._supports_fts5(conn):
            return resolved_physical_name
        if resolved_physical_name in self._ensured_search_backends and self._sqlite_table_exists(
            conn,
            resolved_physical_name,
        ):
            return resolved_physical_name
        had_transaction = conn.in_transaction
        conn.execute(
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {self._quote_identifier(resolved_physical_name)}
            USING fts5(storage_key UNINDEXED, field_path UNINDEXED, content, tokenize='unicode61')
            """
        )
        conn.execute(f"DELETE FROM {self._quote_identifier(resolved_physical_name)}")
        rows: list[tuple[str, str, str]] = []
        for storage_key, document in self._load_documents(db_name, coll_name):
            rows.extend(
                (storage_key, field_path, content)
                for field_path, content in iter_searchable_text_entries(document, definition)
            )
        if rows:
            conn.executemany(
                f"""
                INSERT INTO {self._quote_identifier(resolved_physical_name)} (
                    storage_key, field_path, content
                ) VALUES (?, ?, ?)
                """,
                rows,
            )
        self._ensured_search_backends.add(resolved_physical_name)
        if not had_transaction and conn.in_transaction:
            conn.commit()
        return resolved_physical_name

    def _delete_search_entries_for_storage_key(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        search_indexes: list[tuple[SearchIndexDefinition, str | None, float | None]] | None = None,
    ) -> None:
        rows = search_indexes if search_indexes is not None else self._load_search_index_rows(db_name, coll_name)
        for definition, physical_name, _ready_at_epoch in rows:
            if definition.index_type != "search" or not physical_name:
                continue
            if not self._sqlite_table_exists(conn, physical_name):
                continue
            conn.execute(
                f"DELETE FROM {self._quote_identifier(physical_name)} WHERE storage_key = ?",
                (storage_key,),
            )

    def _replace_search_entries_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        search_indexes: list[tuple[SearchIndexDefinition, str | None, float | None]] | None = None,
    ) -> None:
        rows = search_indexes if search_indexes is not None else self._load_search_index_rows(db_name, coll_name)
        self._delete_search_entries_for_storage_key(
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
            if resolved_physical_name is not None and not self._sqlite_table_exists(conn, resolved_physical_name):
                resolved_physical_name = self._ensure_search_backend_sync(
                    conn,
                    db_name,
                    coll_name,
                    definition,
                    resolved_physical_name,
                )
            if not resolved_physical_name or not self._sqlite_table_exists(conn, resolved_physical_name):
                continue
            entries = iter_searchable_text_entries(document, definition)
            if not entries:
                continue
            conn.executemany(
                f"""
                INSERT INTO {self._quote_identifier(resolved_physical_name)} (
                    storage_key, field_path, content
                ) VALUES (?, ?, ?)
                """,
                [(storage_key, field_path, content) for field_path, content in entries],
            )

    def _delete_multikey_entries_for_storage_key(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
    ) -> None:
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        if collection_id is None:
            return
        conn.execute(
            """
            DELETE FROM multikey_entries
            WHERE collection_id = ? AND storage_key = ?
            """,
            (collection_id, storage_key),
        )

    def _delete_scalar_entries_for_storage_key(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
    ) -> None:
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        if collection_id is None:
            return
        conn.execute(
            """
            DELETE FROM scalar_index_entries
            WHERE collection_id = ? AND storage_key = ?
            """,
            (collection_id, storage_key),
        )

    def _rebuild_multikey_entries_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        indexes: list[EngineIndexRecord],
    ) -> None:
        self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
        collection_id = self._lookup_collection_id(conn, db_name, coll_name, create=True)
        if collection_id is None:
            return
        rows = [
            (collection_id, index_name, storage_key, element_type, type_score, element_key)
            for index_name, element_type, type_score, element_key in self._build_multikey_rows_for_document(
                storage_key,
                document,
                indexes,
            )
        ]
        if rows:
            self._ensure_multikey_physical_indexes_sync(conn, indexes)
        if rows:
            conn.executemany(
                """
                INSERT OR IGNORE INTO multikey_entries (
                    collection_id, index_name, storage_key, element_type, type_score, element_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _rebuild_scalar_entries_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        indexes: list[EngineIndexRecord],
    ) -> None:
        self._delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
        collection_id = self._lookup_collection_id(conn, db_name, coll_name, create=True)
        if collection_id is None:
            return
        rows = [
            (collection_id, index_name, storage_key, element_type, type_score, element_key)
            for index_name, element_type, type_score, element_key in self._build_scalar_rows_for_document(
                storage_key,
                document,
                indexes,
            )
        ]
        if rows:
            self._ensure_scalar_physical_indexes_sync(conn, indexes)
            conn.executemany(
                """
                INSERT OR IGNORE INTO scalar_index_entries (
                    collection_id, index_name, storage_key, element_type, type_score, element_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _replace_multikey_entries_for_index_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        index: EngineIndexRecord,
    ) -> None:
        collection_id = self._lookup_collection_id(conn, db_name, coll_name, create=True)
        if collection_id is None:
            return
        conn.execute(
            """
            DELETE FROM multikey_entries
            WHERE collection_id = ? AND storage_key = ? AND index_name = ?
            """,
            (collection_id, storage_key, index["name"]),
        )
        if not document_in_virtual_index(document, index):
            return
        if not index.get("multikey"):
            return
        rows = [
            (collection_id, index_name, storage_key, element_type, type_score, element_key)
            for index_name, element_type, type_score, element_key in self._build_multikey_rows_for_document(
                storage_key,
                document,
                [index],
            )
        ]
        if rows:
            self._ensure_multikey_physical_indexes_sync(conn, [index])
            conn.executemany(
                """
                INSERT OR IGNORE INTO multikey_entries (
                    collection_id, index_name, storage_key, element_type, type_score, element_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _replace_scalar_entries_for_index_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        index: EngineIndexRecord,
    ) -> None:
        collection_id = self._lookup_collection_id(conn, db_name, coll_name, create=True)
        if collection_id is None:
            return
        conn.execute(
            """
            DELETE FROM scalar_index_entries
            WHERE collection_id = ? AND storage_key = ? AND index_name = ?
            """,
            (collection_id, storage_key, index["name"]),
        )
        if not self._supports_scalar_index(index):
            return
        if not document_in_virtual_index(document, index):
            return
        signature = self._scalar_value_signature_for_document(document, index["fields"][0])
        if signature is None:
            return
        self._ensure_scalar_physical_indexes_sync(conn, [index])
        conn.execute(
            """
            INSERT OR IGNORE INTO scalar_index_entries (
                collection_id, index_name, storage_key, element_type, type_score, element_key
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                collection_id,
                index["name"],
                storage_key,
                signature[0],
                self._multikey_type_score(signature[0]),
                signature[1],
            ),
        )

    def _backfill_scalar_indexes_sync(self, conn: sqlite3.Connection) -> None:
        indexes = conn.execute(
            """
            SELECT db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag,
                   partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name
            FROM indexes
            ORDER BY db_name, coll_name, name
            """
        ).fetchall()
        if not indexes:
            return
        grouped: dict[tuple[str, str], list[EngineIndexRecord]] = {}
        for (
            db_name,
            coll_name,
            name,
            physical_name,
            fields,
            keys,
            unique_flag,
            sparse_flag,
            partial_filter_json,
            expire_after_seconds,
            multikey_flag,
            multikey_physical_name,
            scalar_physical_name,
        ) in indexes:
            parsed_fields = json_loads(fields)
            parsed_keys = normalize_index_keys(json_loads(keys)) if keys is not None else [(field, 1) for field in parsed_fields]
            partial_filter_expression = (
                normalize_partial_filter_expression(json_loads(partial_filter_json))
                if partial_filter_json is not None
                else None
            )
            resolved_scalar_name = scalar_physical_name
            if len(parsed_fields) == 1 and not resolved_scalar_name:
                resolved_scalar_name = self._physical_scalar_index_name(db_name, coll_name, name)
                conn.execute(
                    """
                    UPDATE indexes
                    SET scalar_physical_name = ?
                    WHERE db_name = ? AND coll_name = ? AND name = ?
                    """,
                    (resolved_scalar_name, db_name, coll_name, name),
                )
            grouped.setdefault((db_name, coll_name), []).append(
                EngineIndexRecord(
                    name=name,
                    physical_name=physical_name or self._physical_index_name(db_name, coll_name, name),
                    fields=parsed_fields,
                    key=parsed_keys,
                    unique=bool(unique_flag),
                    sparse=bool(sparse_flag),
                    partial_filter_expression=partial_filter_expression,
                    expire_after_seconds=(
                        int(expire_after_seconds)
                        if expire_after_seconds is not None
                        else None
                    ),
                    multikey=bool(multikey_flag),
                    multikey_physical_name=multikey_physical_name or self._physical_multikey_index_name(db_name, coll_name, name),
                    scalar_physical_name=resolved_scalar_name,
                )
            )
        for collection_indexes in grouped.values():
            self._ensure_scalar_physical_indexes_sync(conn, collection_indexes)
        for (db_name, coll_name), collection_indexes in grouped.items():
            collection_id = self._lookup_collection_id(conn, db_name, coll_name)
            if collection_id is None:
                continue
            conn.execute(
                """
                DELETE FROM scalar_index_entries
                WHERE collection_id = ?
                """,
                (collection_id,),
            )
            for storage_key, document in self._load_documents(db_name, coll_name):
                rows = [
                    (collection_id, index_name, storage_key, element_type, type_score, element_key)
                    for index_name, element_type, type_score, element_key in self._build_scalar_rows_for_document(
                        storage_key,
                        document,
                        collection_indexes,
                    )
                ]
                if rows:
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO scalar_index_entries (
                            collection_id, index_name, storage_key, element_type, type_score, element_key
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )

    def _connect_sync(self) -> None:
        with self._lock:
            if self._connection_count == 0:
                connection = self._create_sqlite_connection()
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS collections (
                        collection_id INTEGER,
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        options_json TEXT NOT NULL DEFAULT '{}',
                        PRIMARY KEY (db_name, coll_name)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        storage_key TEXT NOT NULL,
                        document TEXT NOT NULL,
                        PRIMARY KEY (db_name, coll_name, storage_key)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS indexes (
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        name TEXT NOT NULL,
                        physical_name TEXT,
                        fields TEXT NOT NULL,
                        keys TEXT,
                        unique_flag INTEGER NOT NULL,
                        sparse_flag INTEGER NOT NULL DEFAULT 0,
                        partial_filter_json TEXT,
                        expire_after_seconds INTEGER,
                        multikey_flag INTEGER NOT NULL DEFAULT 0,
                        multikey_physical_name TEXT,
                        PRIMARY KEY (db_name, coll_name, name)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS search_indexes (
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        name TEXT NOT NULL,
                        index_type TEXT NOT NULL,
                        definition_json TEXT NOT NULL,
                        ready_at_epoch REAL,
                        PRIMARY KEY (db_name, coll_name, name)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS multikey_entries (
                        collection_id INTEGER NOT NULL DEFAULT 0,
                        index_name TEXT NOT NULL,
                        storage_key TEXT NOT NULL,
                        element_type TEXT NOT NULL,
                        type_score INTEGER NOT NULL DEFAULT 100,
                        element_key TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scalar_index_entries (
                        collection_id INTEGER NOT NULL DEFAULT 0,
                        index_name TEXT NOT NULL,
                        storage_key TEXT NOT NULL,
                        element_type TEXT NOT NULL,
                        type_score INTEGER NOT NULL DEFAULT 100,
                        element_key TEXT NOT NULL
                    )
                    """
                )
                collection_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(collections)").fetchall()
                }
                if "options_json" not in collection_columns:
                    connection.execute(
                        "ALTER TABLE collections ADD COLUMN options_json TEXT NOT NULL DEFAULT '{}'"
                    )
                if "collection_id" not in collection_columns:
                    connection.execute(
                        "ALTER TABLE collections ADD COLUMN collection_id INTEGER"
                    )
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(indexes)").fetchall()
                }
                if "physical_name" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN physical_name TEXT")
                if "keys" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN keys TEXT")
                if "sparse_flag" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN sparse_flag INTEGER NOT NULL DEFAULT 0")
                if "partial_filter_json" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN partial_filter_json TEXT")
                if "expire_after_seconds" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN expire_after_seconds INTEGER")
                if "multikey_flag" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN multikey_flag INTEGER NOT NULL DEFAULT 0")
                if "multikey_physical_name" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN multikey_physical_name TEXT")
                if "scalar_physical_name" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN scalar_physical_name TEXT")
                search_index_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(search_indexes)").fetchall()
                }
                if "physical_name" not in search_index_columns:
                    connection.execute("ALTER TABLE search_indexes ADD COLUMN physical_name TEXT")
                if "ready_at_epoch" not in search_index_columns:
                    connection.execute("ALTER TABLE search_indexes ADD COLUMN ready_at_epoch REAL")
                multikey_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(multikey_entries)").fetchall()
                }
                if (
                    "collection_id" not in multikey_columns
                    or "type_score" not in multikey_columns
                    or "db_name" in multikey_columns
                    or "coll_name" in multikey_columns
                ):
                    connection.execute(
                        """
                        CREATE TABLE multikey_entries_new (
                            collection_id INTEGER NOT NULL,
                            index_name TEXT NOT NULL,
                            storage_key TEXT NOT NULL,
                            element_type TEXT NOT NULL,
                            type_score INTEGER NOT NULL DEFAULT 100,
                            element_key TEXT NOT NULL
                        )
                        """
                    )
                    select_type_score = (
                        "multikey_entries.type_score"
                        if "type_score" in multikey_columns
                        else "100"
                    )
                    join_db_name = "multikey_entries.db_name" if "db_name" in multikey_columns else "collections.db_name"
                    join_coll_name = "multikey_entries.coll_name" if "coll_name" in multikey_columns else "collections.coll_name"
                    collection_id_sql = (
                        "CASE "
                        "WHEN multikey_entries.collection_id IS NOT NULL AND multikey_entries.collection_id != 0 "
                        "THEN multikey_entries.collection_id "
                        "ELSE collections.collection_id END"
                        if "collection_id" in multikey_columns
                        else "collections.collection_id"
                    )
                    connection.execute(
                        f"""
                        INSERT INTO multikey_entries_new (
                            collection_id, index_name, storage_key, element_type, type_score, element_key
                        )
                        SELECT
                            {collection_id_sql},
                            multikey_entries.index_name,
                            multikey_entries.storage_key,
                            multikey_entries.element_type,
                            {select_type_score},
                            multikey_entries.element_key
                        FROM multikey_entries
                        LEFT JOIN collections
                          ON collections.db_name = {join_db_name}
                         AND collections.coll_name = {join_coll_name}
                        """
                    )
                    connection.execute("DROP TABLE multikey_entries")
                    connection.execute("ALTER TABLE multikey_entries_new RENAME TO multikey_entries")
                scalar_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(scalar_index_entries)").fetchall()
                }
                if (
                    not scalar_columns
                    or "collection_id" not in scalar_columns
                    or "type_score" not in scalar_columns
                    or "db_name" in scalar_columns
                    or "coll_name" in scalar_columns
                ):
                    connection.execute("DROP TABLE IF EXISTS scalar_index_entries_new")
                    connection.execute(
                        """
                        CREATE TABLE scalar_index_entries_new (
                            collection_id INTEGER NOT NULL,
                            index_name TEXT NOT NULL,
                            storage_key TEXT NOT NULL,
                            element_type TEXT NOT NULL,
                            type_score INTEGER NOT NULL DEFAULT 100,
                            element_key TEXT NOT NULL
                        )
                        """
                    )
                    if scalar_columns:
                        select_type_score = (
                            "scalar_index_entries.type_score"
                            if "type_score" in scalar_columns
                            else "100"
                        )
                        join_db_name = "scalar_index_entries.db_name" if "db_name" in scalar_columns else "collections.db_name"
                        join_coll_name = "scalar_index_entries.coll_name" if "coll_name" in scalar_columns else "collections.coll_name"
                        collection_id_sql = (
                            "CASE "
                            "WHEN scalar_index_entries.collection_id IS NOT NULL AND scalar_index_entries.collection_id != 0 "
                            "THEN scalar_index_entries.collection_id "
                            "ELSE collections.collection_id END"
                            if "collection_id" in scalar_columns
                            else "collections.collection_id"
                        )
                        connection.execute(
                            f"""
                            INSERT INTO scalar_index_entries_new (
                                collection_id, index_name, storage_key, element_type, type_score, element_key
                            )
                            SELECT
                                {collection_id_sql},
                                scalar_index_entries.index_name,
                                scalar_index_entries.storage_key,
                                scalar_index_entries.element_type,
                                {select_type_score},
                                scalar_index_entries.element_key
                            FROM scalar_index_entries
                            LEFT JOIN collections
                              ON collections.db_name = {join_db_name}
                             AND collections.coll_name = {join_coll_name}
                            """
                        )
                    connection.execute("DROP TABLE IF EXISTS scalar_index_entries")
                    connection.execute("ALTER TABLE scalar_index_entries_new RENAME TO scalar_index_entries")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO collections (db_name, coll_name, options_json)
                    SELECT db_name, coll_name, '{}' FROM documents
                    UNION
                    SELECT db_name, coll_name, '{}' FROM indexes
                    UNION
                    SELECT db_name, coll_name, '{}' FROM search_indexes
                    """
                )
                connection.execute(
                    """
                    UPDATE collections
                    SET collection_id = rowid
                    WHERE collection_id IS NULL OR collection_id = 0
                    """
                )
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_collections_collection_id
                    ON collections (collection_id)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scalar_index_entries_storage
                    ON scalar_index_entries (collection_id, storage_key)
                    """
                )
                search_index_rows = connection.execute(
                    """
                    SELECT db_name, coll_name, name
                    FROM search_indexes
                    WHERE index_type = 'search'
                      AND (physical_name IS NULL OR physical_name = '')
                    """
                ).fetchall()
                for db_name, coll_name, name in search_index_rows:
                    connection.execute(
                        """
                        UPDATE search_indexes
                        SET physical_name = ?
                        WHERE db_name = ? AND coll_name = ? AND name = ?
                        """,
                        (
                            self._physical_search_index_name(db_name, coll_name, name),
                            db_name,
                            coll_name,
                            name,
                        ),
                    )
                self._connection = connection
                with self._bind_connection(connection):
                    self._backfill_scalar_indexes_sync(connection)
                connection.commit()
                self._ensured_multikey_physical_indexes.clear()
                self._ensured_search_backends.clear()
                self._fts5_available = None
                self._invalidate_index_cache()
                self._invalidate_collection_id_cache()
                self._invalidate_collection_features_cache()
            self._connection_count += 1

    def _create_sqlite_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, check_same_thread=False)
        if self._path != ":memory:":
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _can_use_dedicated_reader(self, context: ClientSession | None) -> bool:
        return self._path != ":memory:" and not self._session_owns_transaction(context)

    def _ensure_collection_row(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        *,
        options: dict[str, object] | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO collections (db_name, coll_name, options_json)
            VALUES (?, ?, ?)
            """,
            (db_name, coll_name, json_dumps_compact(options or {}, sort_keys=True)),
        )
        self._lookup_collection_id(conn, db_name, coll_name, create=True)

    def _collection_exists_sync(self, conn: sqlite3.Connection, db_name: str, coll_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM collections
            WHERE db_name = ? AND coll_name = ?
            UNION
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
            UNION
            SELECT 1
            FROM indexes
            WHERE db_name = ? AND coll_name = ?
            UNION
            SELECT 1
            FROM search_indexes
            WHERE db_name = ? AND coll_name = ?
            LIMIT 1
            """,
            (db_name, coll_name, db_name, coll_name, db_name, coll_name, db_name, coll_name),
        ).fetchone()
        return row is not None

    def _collection_options_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        if self._is_profile_namespace(coll_name) and self._profiler.namespace_visible(db_name):
            return {}
        with self._lock:
            conn = self._require_connection(context)
            row = conn.execute(
                """
                SELECT options_json
                FROM collections
                WHERE db_name = ? AND coll_name = ?
                """,
                (db_name, coll_name),
            ).fetchone()
            if row is not None:
                return json_loads(row[0] or "{}")
            if self._collection_exists_sync(conn, db_name, coll_name):
                return {}
            raise CollectionInvalid(f"collection '{coll_name}' does not exist")

    def _collection_options_or_empty_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
    ) -> dict[str, object]:
        row = conn.execute(
            """
            SELECT options_json
            FROM collections
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        ).fetchone()
        if row is None:
            return {}
        return json_loads(row[0] or "{}")

    def _load_existing_document_for_storage_key(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
    ) -> Document | None:
        row = conn.execute(
            """
            SELECT document
            FROM documents
            WHERE db_name = ? AND coll_name = ? AND storage_key = ?
            """,
            (db_name, coll_name, storage_key),
        ).fetchone()
        if row is None:
            return None
        return self._deserialize_document(row[0])

    def _disconnect_sync(self) -> None:
        connection: sqlite3.Connection | None = None
        with self._lock:
            if self._connection_count == 0:
                return
            self._connection_count -= 1
            if self._connection_count != 0:
                return
            with self._scan_condition:
                while self._active_scan_count > 0:
                    self._scan_condition.wait()
            connection = self._connection
            self._connection = None
            self._transaction_owner_session_id = None
            self._invalidate_index_cache()
            self._invalidate_collection_id_cache()
            self._invalidate_collection_features_cache()
            self._ensured_search_backends.clear()
            self._fts5_available = None
        if connection is not None:
            connection.close()

    def _profile_documents(self, db_name: str) -> list[Document]:
        return self._profiler.list_entries(db_name)

    def _put_document_sync(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        overwrite: bool,
        context: ClientSession | None,
        *,
        bypass_document_validation: bool = False,
    ) -> bool:
        storage_key = self._storage_key(document.get("_id"))
        serialized_document = self._serialize_document(document)

        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._purge_expired_documents_sync(conn, db_name, coll_name, context=context)
                self._begin_write(conn, context)
                try:
                    collection_options = self._collection_options_or_empty_sync(conn, db_name, coll_name)
                    original_document = None
                    if not bypass_document_validation or not overwrite:
                        original_document = self._load_existing_document_for_storage_key(conn, db_name, coll_name, storage_key)

                    if not overwrite and original_document is not None:
                        self._rollback_write(conn, context)
                        return False

                    if not bypass_document_validation:
                        enforce_collection_document_validation(
                            document,
                            options=collection_options,
                            original_document=original_document,
                            dialect=MONGODB_DIALECT_70,
                        )

                    self._ensure_collection_row(conn, db_name, coll_name, options=collection_options)
                    self._validate_document_against_unique_indexes(
                        db_name,
                        coll_name,
                        document,
                        exclude_storage_key=storage_key if overwrite else None,
                    )
                    
                    try:
                        if overwrite:
                            conn.execute(
                                """
                                INSERT INTO documents (db_name, coll_name, storage_key, document)
                                VALUES (?, ?, ?, ?)
                                ON CONFLICT(db_name, coll_name, storage_key)
                                DO UPDATE SET document = excluded.document
                                """,
                                (db_name, coll_name, storage_key, serialized_document),
                            )
                        else:
                            conn.execute(
                                """
                                INSERT INTO documents (db_name, coll_name, storage_key, document)
                                VALUES (?, ?, ?, ?)
                                """,
                                (db_name, coll_name, storage_key, serialized_document),
                            )
                    except sqlite3.IntegrityError as exc:
                        raise DuplicateKeyError(f"Duplicate key error: {exc}") from exc

                    indexes = self._load_indexes(db_name, coll_name)
                    if any(idx.get("multikey") for idx in indexes):
                        self._rebuild_multikey_entries_for_document(conn, db_name, coll_name, storage_key, document, indexes)
                    if any(self._supports_scalar_index(idx) for idx in indexes):
                        self._rebuild_scalar_entries_for_document(conn, db_name, coll_name, storage_key, document, indexes)
                    
                    search_indexes = self._load_search_index_rows(db_name, coll_name)
                    if search_indexes:
                        self._replace_search_entries_for_document(conn, db_name, coll_name, storage_key, document, search_indexes=search_indexes)
                    
                    self._commit_write(conn, context)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                    return True
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    def _snapshot_bulk_insert_validation_options_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> dict[str, object]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                return self._collection_options_or_empty_sync(conn, db_name, coll_name)

    def _put_documents_bulk_sync(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        prepared_documents: list[tuple[str, str, list[tuple[str, str, int, str]]]],
        snapshot_indexes: list[EngineIndexRecord],
        context: ClientSession | None,
        *,
        bypass_document_validation: bool = False,
        snapshot_options: dict[str, object] | None = None,
    ) -> list[bool]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._purge_expired_documents_sync(conn, db_name, coll_name, context=context)
                collection_options = self._collection_options_or_empty_sync(
                    conn,
                    db_name,
                    coll_name,
                )
                if (
                    not bypass_document_validation
                    and snapshot_options is not None
                    and collection_options != snapshot_options
                ):
                    for document in documents:
                        enforce_collection_document_validation(
                            document,
                            options=collection_options,
                            original_document=None,
                            dialect=MONGODB_DIALECT_70,
                        )
                indexes = self._load_indexes(db_name, coll_name)
                search_indexes = self._load_search_index_rows(db_name, coll_name)
                results: list[bool] = []
                try:
                    self._begin_write(conn, context)
                    self._ensure_collection_row(conn, db_name, coll_name)
                    collection_id = self._lookup_collection_id(conn, db_name, coll_name, create=True)
                    for index, (document, (storage_key, serialized_document, prepared_multikey_rows)) in enumerate(
                        zip(documents, prepared_documents, strict=False)
                    ):
                        try:
                            self._validate_document_against_unique_indexes(
                                db_name,
                                coll_name,
                                document,
                                exclude_storage_key=None,
                            )
                            cursor = conn.execute(
                                """
                                INSERT INTO documents (db_name, coll_name, storage_key, document)
                                VALUES (?, ?, ?, ?)
                                ON CONFLICT(db_name, coll_name, storage_key) DO NOTHING
                                """,
                                (db_name, coll_name, storage_key, serialized_document),
                            )
                            if cursor.rowcount == 0:
                                results.append(False)
                                break
                            self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                            self._delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                            effective_rows = prepared_multikey_rows
                            if indexes != snapshot_indexes:
                                effective_rows = self._build_multikey_rows_for_document(storage_key, document, indexes)
                            if collection_id is not None and effective_rows:
                                self._ensure_multikey_physical_indexes_sync(conn, indexes)
                                conn.executemany(
                                    """
                                    INSERT OR IGNORE INTO multikey_entries (
                                        collection_id, index_name, storage_key, element_type, type_score, element_key
                                    ) VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    [
                                        (
                                            collection_id,
                                            index_name,
                                            storage_key,
                                            element_type,
                                            type_score,
                                            element_key,
                                        )
                                        for index_name, element_type, type_score, element_key in effective_rows
                                    ],
                                )
                            scalar_rows = self._build_scalar_rows_for_document(storage_key, document, indexes)
                            if collection_id is not None and scalar_rows:
                                self._ensure_scalar_physical_indexes_sync(conn, indexes)
                                conn.executemany(
                                    """
                                    INSERT OR IGNORE INTO scalar_index_entries (
                                        collection_id, index_name, storage_key, element_type, type_score, element_key
                                    ) VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    [
                                        (
                                            collection_id,
                                            index_name,
                                            storage_key,
                                            element_type,
                                            type_score,
                                            element_key,
                                        )
                                        for index_name, element_type, type_score, element_key in scalar_rows
                                    ],
                                )
                            self._replace_search_entries_for_document(
                                conn,
                                db_name,
                                coll_name,
                                storage_key,
                                document,
                                search_indexes=search_indexes,
                            )
                            results.append(True)
                        except (DuplicateKeyError, sqlite3.IntegrityError):
                            results.append(False)
                            break
                    self._commit_write(conn, context)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                    return results
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    def _prepare_bulk_document_sync(self, document: Document) -> tuple[str, str]:
        return self._storage_key(document.get("_id")), self._serialize_document(document)

    def _snapshot_bulk_insert_preparation_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> tuple[dict[str, object], list[EngineIndexRecord]]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                return (
                    self._collection_options_or_empty_sync(conn, db_name, coll_name),
                    self._load_indexes(db_name, coll_name),
                )

    def _prepare_bulk_document_with_indexes_sync(
        self,
        document: Document,
        indexes: list[EngineIndexRecord],
    ) -> tuple[str, str, list[tuple[str, str, int, str]]]:
        storage_key, serialized_document = self._prepare_bulk_document_sync(document)
        return (
            storage_key,
            serialized_document,
            self._build_multikey_rows_for_document(storage_key, document, indexes),
        )

    def _get_document_sync(self, db_name: str, coll_name: str, doc_id: DocumentId, projection: Projection | None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        effective_dialect = dialect or MONGODB_DIALECT_70
        if self._is_profile_namespace(coll_name):
            document = self._profiler.get_entry(db_name, doc_id)
            if document is None:
                return None
            return apply_projection(document, projection, dialect=effective_dialect)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._purge_expired_documents_sync(conn, db_name, coll_name, context=context)
                row = conn.execute(
                    """
                    SELECT document
                    FROM documents
                    WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                    """,
                    (db_name, coll_name, self._storage_key(doc_id)),
                ).fetchone()
        if row is None:
            return None
        return apply_projection(
            DocumentCodec.to_public(self._deserialize_document(row[0])),
            projection,
            dialect=effective_dialect,
        )

    def _delete_document_sync(self, db_name: str, coll_name: str, doc_id: DocumentId, context: ClientSession | None) -> bool:
        if self._is_profile_namespace(coll_name):
            return False
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                storage_key = self._storage_key(doc_id)
                try:
                    self._begin_write(conn, context)
                    cursor = conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (db_name, coll_name, storage_key),
                    )
                    self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._delete_search_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._commit_write(conn, context)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                    return cursor.rowcount > 0
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    def _iter_scan_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        semantics_or_filter_spec: EngineFindSemantics | Filter | None,
        plan: QueryNode | None = None,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        context: ClientSession | None = None,
        stop_event: threading.Event | None = None,
        *,
        hint: str | IndexKeySpec | None = None,
        max_time_ms: int | None = None,
        dialect: MongoDialect | None = None,
    ):
        semantics = (
            semantics_or_filter_spec
            if isinstance(semantics_or_filter_spec, EngineFindSemantics)
            else compile_find_semantics(
                semantics_or_filter_spec,
                plan=plan,
                projection=projection,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                max_time_ms=max_time_ms,
                dialect=dialect,
            )
        )
        python_semantics = replace(semantics, compiled_query=None)
        deadline = semantics.deadline
        if self._is_profile_namespace(coll_name):
            documents_iter = iter(self._profile_documents(db_name))
            if python_semantics.sort is None:
                for document in stream_finalize_documents(
                    iter_filtered_documents(documents_iter, python_semantics),
                    python_semantics,
                ):
                    enforce_deadline(deadline)
                    if stop_event is not None and stop_event.is_set():
                        break
                    yield document
                return
            documents = filter_documents(documents_iter, python_semantics)
            documents = sort_documents(
                documents,
                python_semantics.sort,
                dialect=python_semantics.dialect,
                collation=python_semantics.collation,
            )
            documents = finalize_documents(documents, python_semantics, apply_sort_phase=False)
            for document in documents:
                enforce_deadline(deadline)
                if stop_event is not None and stop_event.is_set():
                    break
                yield document
            return
        with self._lock:
            try:
                purge_connection = self._require_connection(context)
            except RuntimeError:
                purge_connection = None
            if purge_connection is not None:
                with self._bind_connection(purge_connection):
                    self._purge_expired_documents_sync(
                        purge_connection,
                        db_name,
                        coll_name,
                        context=context,
                    )
        if (
            semantics.sort is None
            and semantics.skip == 0
            and semantics.limit == 1
            and semantics.collation is None
            and isinstance(semantics.query_plan, EqualsCondition)
            and "." not in semantics.query_plan.field
        ):
            try:
                selected = self._select_first_document_for_plan(
                    db_name,
                    coll_name,
                    semantics.query_plan,
                    hint=hint,
                )
            except NotImplementedError:
                selected = None
            if selected is None:
                pass
            else:
                _storage_key, document = selected
                yield apply_projection(
                    document,
                    semantics.projection,
                    selector_filter=semantics.filter_spec,
                    dialect=semantics.dialect,
                )
                return
        shared_connection: sqlite3.Connection | None = None
        dedicated_reader: sqlite3.Connection | None = None
        if self._session_owns_transaction(context):
            with self._lock:
                shared_connection = self._require_connection(context)
        elif self._can_use_dedicated_reader(context):
            dedicated_reader = self._create_sqlite_connection()

        active_connection = dedicated_reader or shared_connection
        try:
            with self._bind_connection(active_connection) if active_connection is not None else nullcontext():
                try:
                    if shared_connection is None:
                        enforce_deadline(deadline)
                        execution_plan = self._compile_read_execution_plan(
                            db_name,
                            coll_name,
                            semantics,
                            hint=hint,
                        )
                    else:
                        with self._lock:
                            enforce_deadline(deadline)
                            execution_plan = self._compile_read_execution_plan(
                                db_name,
                                coll_name,
                                semantics,
                                hint=hint,
                            )
                    sql, sql_params = execution_plan.require_sql()
                    if active_connection is None:
                        with self._lock:
                            active_connection = self._require_connection(context)
                    cursor = active_connection.execute(sql, tuple(sql_params))
                    try:
                        if execution_plan.apply_python_sort:
                            documents = finalize_documents(
                                (self._deserialize_document(payload) for (payload,) in cursor),
                                semantics,
                            )
                            for document in documents:
                                enforce_deadline(deadline)
                                if stop_event is not None and stop_event.is_set():
                                    break
                                yield document
                            return
                        for (payload,) in cursor:
                            enforce_deadline(deadline)
                            if stop_event is not None and stop_event.is_set():
                                break
                            yield apply_projection(
                                self._deserialize_document(payload),
                                semantics.projection,
                                selector_filter=semantics.filter_spec,
                                dialect=semantics.dialect,
                            )
                    finally:
                        try:
                            cursor.close()
                        except sqlite3.ProgrammingError:
                            # El consumidor puede cerrar la conexión antes de drenar el generador.
                            pass
                    return
                except (NotImplementedError, TypeError):
                    documents_iter = (document for _, document in self._load_documents(db_name, coll_name))
                    try:
                        if python_semantics.sort is None:
                            for document in stream_finalize_documents(
                                iter_filtered_documents(documents_iter, python_semantics),
                                python_semantics,
                            ):
                                if stop_event is not None and stop_event.is_set():
                                    break
                                yield document
                            return

                        documents = finalize_documents(
                            filter_documents(documents_iter, python_semantics),
                            python_semantics,
                        )
                    finally:
                        close = getattr(documents_iter, "close", None)
                        if callable(close):
                            close()
        finally:
            if dedicated_reader is not None:
                dedicated_reader.close()

        for document in documents:
            enforce_deadline(deadline)
            if stop_event is not None and stop_event.is_set():
                break
            yield document

    def _update_matching_document_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter,
        update_spec: Update,
        upsert: bool,
        upsert_seed: Document | None,
        selector_filter: Filter | None,
        array_filters: ArrayFilters | None,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
        bypass_document_validation: bool = False,
    ) -> UpdateResult[DocumentId]:
        effective_dialect = dialect or MONGODB_DIALECT_70
        operation = compile_update_operation(
            filter_spec,
            array_filters=array_filters,
            dialect=effective_dialect,
            plan=plan,
            update_spec=update_spec,
        )
        return self._update_with_operation_sync(
            db_name,
            coll_name,
            operation,
            upsert,
            upsert_seed,
            selector_filter,
            context,
            effective_dialect,
            bypass_document_validation,
        )

    def _delete_matching_document_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
        collation: CollationDocument | None = None,
    ) -> DeleteResult:
        effective_dialect = dialect or MONGODB_DIALECT_70
        plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        semantics = compile_find_semantics(
            filter_spec,
            plan=plan,
            collation=collation,
            dialect=effective_dialect,
        )
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._purge_expired_documents_sync(conn, db_name, coll_name, context=context)
                try:
                    if semantics.collation is not None:
                        raise NotImplementedError("Collation requires Python delete fallback")
                    selected = self._select_first_document_for_plan(
                        db_name,
                        coll_name,
                        semantics.query_plan,
                    )
                    if selected is None:
                        return DeleteResult(deleted_count=0)
                    storage_key, _document = selected
                    self._begin_write(conn, context)
                    conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (db_name, coll_name, storage_key),
                    )
                    self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._delete_search_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._commit_write(conn, context)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                    return DeleteResult(deleted_count=1)
                except (NotImplementedError, TypeError):
                    self._rollback_write(conn, context)
                    pass

                for storage_key, document in self._load_documents(db_name, coll_name):
                    if not QueryEngine.match_plan(
                        document,
                        semantics.query_plan,
                        dialect=effective_dialect,
                        collation=semantics.collation,
                    ):
                        continue
                    self._begin_write(conn, context)
                    conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (db_name, coll_name, storage_key),
                    )
                    self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._delete_search_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._commit_write(conn, context)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                    return DeleteResult(deleted_count=1)
                return DeleteResult(deleted_count=0)

    def _count_matching_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        semantics_or_filter_spec: EngineFindSemantics | Filter,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
    ) -> int:
        semantics = (
            semantics_or_filter_spec
            if isinstance(semantics_or_filter_spec, EngineFindSemantics)
            else compile_find_semantics(
                semantics_or_filter_spec,
                plan=plan,
                dialect=dialect,
            )
        )
        effective_dialect = semantics.dialect
        with self._lock:
            conn: sqlite3.Connection | None = None
            if self._session_owns_transaction(context):
                conn = self._require_connection(context)
            with self._bind_connection(conn) if conn is not None else nullcontext():
                if conn is not None:
                    self._purge_expired_documents_sync(conn, db_name, coll_name, context=context)
                try:
                    execution_plan = self._compile_read_execution_plan(
                        db_name,
                        coll_name,
                        semantics,
                    )
                    sql, params = execution_plan.require_sql()
                    if conn is None:
                        conn = self._require_connection(context)
                    row = conn.execute(
                        f"SELECT COUNT(*) FROM ({sql})",
                        tuple(params),
                    ).fetchone()
                    return int(row[0])
                except (NotImplementedError, TypeError):
                    return sum(
                        1
                        for _, document in self._load_documents(db_name, coll_name)
                        if QueryEngine.match_plan(
                            document,
                            semantics.query_plan,
                            dialect=effective_dialect,
                            collation=semantics.collation,
                        )
                    )

    def _create_index_sync(
        self,
        db_name: str,
        coll_name: str,
        keys: IndexKeySpec,
        unique: bool,
        name: str | None,
        sparse: bool,
        partial_filter_expression: Filter | None,
        expire_after_seconds: int | None,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> str:
        normalized_keys = normalize_index_keys(keys)
        partial_filter_expression = normalize_partial_filter_expression(partial_filter_expression)
        fields = index_fields(normalized_keys)
        index_name = name or default_index_name(normalized_keys)
        special_directions = special_index_directions(normalized_keys)
        deadline = operation_deadline(max_time_ms)
        if expire_after_seconds is not None:
            if (
                not isinstance(expire_after_seconds, int)
                or isinstance(expire_after_seconds, bool)
                or expire_after_seconds < 0
            ):
                raise TypeError("expire_after_seconds must be a non-negative int or None")
            if len(fields) != 1:
                raise OperationFailure("TTL indexes require a single-field key pattern")
            if fields[0] == "_id":
                raise OperationFailure("TTL indexes cannot be created on _id")
        if special_directions:
            if len(normalized_keys) != 1:
                raise OperationFailure("special index types currently require a single-field key pattern")
            if unique:
                raise OperationFailure(f"{special_directions[0]} indexes do not support unique")
        if self._is_builtin_id_index(normalized_keys):
            if (
                name not in (None, "_id_")
                or sparse
                or partial_filter_expression is not None
                or expire_after_seconds is not None
                or not unique
            ):
                raise OperationFailure("Conflicting index definition for '_id_'")
            return "_id_"
        if index_name == "_id_":
            raise OperationFailure("Conflicting index definition for '_id_'")
        ordered_index = is_ordered_index_spec(normalized_keys)
        physical_name = (
            self._physical_index_name(db_name, coll_name, index_name)
            if ordered_index
            else None
        )
        multikey = ordered_index and self._supports_multikey_index(fields, unique)
        multikey_physical_name = (
            self._physical_multikey_index_name(db_name, coll_name, index_name)
            if multikey
            else None
        )
        scalar_physical_name = (
            self._physical_scalar_index_name(db_name, coll_name, index_name)
            if ordered_index and len(fields) == 1
            else None
        )
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                enforce_deadline(deadline)
                indexes = self._load_indexes(db_name, coll_name)
                for index in indexes:
                    enforce_deadline(deadline)
                    if index["name"] == index_name:
                        if (
                            index["key"] != normalized_keys
                            or index["unique"] != unique
                            or index.get("sparse") != sparse
                            or index.get("partial_filter_expression") != partial_filter_expression
                            or index.get("expire_after_seconds") != expire_after_seconds
                        ):
                            raise OperationFailure(f"Conflicting index definition for '{index_name}'")
                        return index_name
                    if index["key"] == normalized_keys:
                        if (
                            index["unique"] != unique
                            or index.get("sparse") != sparse
                            or index.get("partial_filter_expression") != partial_filter_expression
                            or index.get("expire_after_seconds") != expire_after_seconds
                        ):
                            raise OperationFailure(
                                f"Conflicting index definition for key pattern '{normalized_keys!r}'"
                            )
                        continue

                if unique:
                    for field in fields:
                        if self._field_traverses_array_in_collection(db_name, coll_name, field):
                            raise OperationFailure("SQLite unique indexes do not support paths that traverse arrays")
        expressions = None
        if ordered_index:
            expressions = ", ".join(
                [
                    "db_name",
                    "coll_name",
                    *[
                        f"{expression}{' DESC' if direction == -1 else ''}"
                        for field, direction in normalized_keys
                        for expression in index_expressions_sql(field)
                    ],
                ]
            )
        unique_sql = "UNIQUE " if unique and not (sparse or partial_filter_expression is not None) else ""
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                enforce_deadline(deadline)
                try:
                    self._begin_write(conn, context)
                    if physical_name is not None and expressions is not None:
                        conn.execute(
                            f"CREATE {unique_sql}INDEX {self._quote_identifier(physical_name)} "
                            f"ON documents ({expressions})"
                        )
                    if multikey:
                        enforce_deadline(deadline)
                        conn.execute(
                            f"CREATE INDEX {self._quote_identifier(multikey_physical_name)} "
                            "ON multikey_entries (collection_id, index_name, type_score, element_key, storage_key)"
                        )
                    if scalar_physical_name is not None:
                        enforce_deadline(deadline)
                        conn.execute(
                            f"CREATE INDEX {self._quote_identifier(scalar_physical_name)} "
                            "ON scalar_index_entries (collection_id, index_name, type_score, element_key, storage_key)"
                        )
                    conn.execute(
                        """
                        INSERT INTO indexes (
                            db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            db_name,
                            coll_name,
                            index_name,
                            physical_name,
                            json_dumps_compact(fields),
                            json_dumps_compact(normalized_keys),
                            1 if unique else 0,
                            1 if sparse else 0,
                            json_dumps_compact(partial_filter_expression) if partial_filter_expression is not None else None,
                            expire_after_seconds,
                            1 if multikey else 0,
                            multikey_physical_name if multikey else None,
                            scalar_physical_name,
                        ),
                    )
                    if multikey:
                        enforce_deadline(deadline)
                        index_metadata = EngineIndexRecord(
                            name=index_name,
                            physical_name=physical_name,
                            fields=fields,
                            key=normalized_keys,
                            unique=unique,
                            sparse=sparse,
                            partial_filter_expression=deepcopy(partial_filter_expression),
                            expire_after_seconds=expire_after_seconds,
                            multikey=True,
                            multikey_physical_name=multikey_physical_name,
                            scalar_physical_name=scalar_physical_name,
                        )
                        for storage_key, document in self._load_documents(db_name, coll_name):
                            enforce_deadline(deadline)
                            self._replace_multikey_entries_for_index_for_document(
                                conn,
                                db_name,
                                coll_name,
                                storage_key,
                                document,
                                index_metadata,
                            )
                    if scalar_physical_name is not None:
                        enforce_deadline(deadline)
                        index_metadata = EngineIndexRecord(
                            name=index_name,
                            physical_name=physical_name,
                            fields=fields,
                            key=normalized_keys,
                            unique=unique,
                            sparse=sparse,
                            partial_filter_expression=deepcopy(partial_filter_expression),
                            expire_after_seconds=expire_after_seconds,
                            multikey=multikey,
                            multikey_physical_name=multikey_physical_name,
                            scalar_physical_name=scalar_physical_name,
                        )
                        for storage_key, document in self._load_documents(db_name, coll_name):
                            enforce_deadline(deadline)
                            self._replace_scalar_entries_for_index_for_document(
                                conn,
                                db_name,
                                coll_name,
                                storage_key,
                                document,
                                index_metadata,
                            )
                    enforce_deadline(deadline)
                    self._commit_write(conn, context)
                    self._purge_expired_documents_sync(
                        conn,
                        db_name,
                        coll_name,
                        context=context,
                    )
                    self._mark_index_metadata_changed(db_name, coll_name)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                    return index_name
                except sqlite3.IntegrityError as exc:
                    self._rollback_write(conn, context)
                    self._mark_index_metadata_changed(db_name, coll_name)
                    raise DuplicateKeyError(str(exc)) from exc

    def _list_indexes_sync(self, db_name: str, coll_name: str, context: ClientSession | None) -> list[IndexDocument]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = deepcopy(self._load_indexes(db_name, coll_name))
        result = [default_id_index_definition().to_list_document()]
        result.extend(
            index.to_definition().to_list_document()
            for index in indexes
        )
        return result

    def _index_information_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> IndexInformation:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = deepcopy(self._load_indexes(db_name, coll_name))
        return {
            **default_id_index_information(),
            **{
                str(index["name"]): index.to_definition().to_information_entry()
                for index in indexes
            },
        }

    def _drop_index_sync(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        context: ClientSession | None,
    ) -> None:
        normalized_keys: IndexKeySpec | None = None
        target_name: str
        if isinstance(index_or_name, str):
            if index_or_name == "_id_":
                raise OperationFailure("cannot drop _id index")
            target_name = index_or_name
        else:
            normalized_keys = normalize_index_keys(index_or_name)
            if self._is_builtin_id_index(normalized_keys):
                raise OperationFailure("cannot drop _id index")
            target_name = default_index_name(normalized_keys)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = self._load_indexes(db_name, coll_name)
                if normalized_keys is not None:
                    matches = [index for index in indexes if index["key"] == normalized_keys]
                    if not matches:
                        raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")
                    if len(matches) > 1:
                        raise OperationFailure(
                            f"multiple indexes found with key pattern {normalized_keys!r}; drop by name instead"
                        )
                    target_name = str(matches[0]["name"])
                target: EngineIndexRecord | None = None
                for index in indexes:
                    if index["name"] == target_name:
                        target = index
                        break
                if target is None:
                    if isinstance(index_or_name, str):
                        raise OperationFailure(f"index not found with name [{index_or_name}]")
                    raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")
                try:
                    self._begin_write(conn, context)
                    conn.execute(
                        f"DROP INDEX IF EXISTS {self._quote_identifier(str(target['physical_name']))}"
                    )
                    if target.get("scalar_physical_name"):
                        conn.execute(
                            f"DROP INDEX IF EXISTS {self._quote_identifier(str(target['scalar_physical_name']))}"
                        )
                        conn.execute(
                            """
                            DELETE FROM scalar_index_entries
                            WHERE collection_id = ? AND index_name = ?
                            """,
                            (self._lookup_collection_id(conn, db_name, coll_name), target["name"]),
                        )
                    if target.get("multikey"):
                        conn.execute(
                            f"DROP INDEX IF EXISTS {self._quote_identifier(str(target['multikey_physical_name']))}"
                        )
                        conn.execute(
                            """
                            DELETE FROM multikey_entries
                            WHERE collection_id = ? AND index_name = ?
                            """,
                            (self._lookup_collection_id(conn, db_name, coll_name), target["name"]),
                        )
                    conn.execute(
                        """
                        DELETE FROM indexes
                        WHERE db_name = ? AND coll_name = ? AND name = ?
                        """,
                        (db_name, coll_name, target["name"]),
                    )
                    self._commit_write(conn, context)
                    self._mark_index_metadata_changed(db_name, coll_name)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    def _drop_database_sync(
        self,
        db_name: str,
        context: ClientSession | None = None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                index_rows = conn.execute(
                    """
                    SELECT physical_name, multikey_physical_name, scalar_physical_name
                    FROM indexes
                    WHERE db_name = ?
                    """,
                    (db_name,),
                ).fetchall()
                search_rows = conn.execute(
                    """
                    SELECT physical_name
                    FROM search_indexes
                    WHERE db_name = ?
                    """,
                    (db_name,),
                ).fetchall()
                try:
                    self._begin_write(conn, context)
                    for physical_name, multikey_physical_name, scalar_physical_name in index_rows:
                        if physical_name:
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(str(physical_name))}"
                            )
                        if scalar_physical_name:
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(str(scalar_physical_name))}"
                            )
                        if multikey_physical_name:
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(str(multikey_physical_name))}"
                            )
                    for (physical_name,) in search_rows:
                        self._drop_search_backend_sync(conn, physical_name)
                    conn.execute(
                        """
                        DELETE FROM scalar_index_entries
                        WHERE collection_id IN (
                            SELECT collection_id FROM collections WHERE db_name = ?
                        )
                        """,
                        (db_name,),
                    )
                    conn.execute(
                        """
                        DELETE FROM multikey_entries
                        WHERE collection_id IN (
                            SELECT collection_id FROM collections WHERE db_name = ?
                        )
                        """,
                        (db_name,),
                    )
                    conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ?
                        """,
                        (db_name,),
                    )
                    conn.execute(
                        """
                        DELETE FROM indexes
                        WHERE db_name = ?
                        """,
                        (db_name,),
                    )
                    conn.execute(
                        """
                        DELETE FROM search_indexes
                        WHERE db_name = ?
                        """,
                        (db_name,),
                    )
                    conn.execute(
                        """
                        DELETE FROM collections
                        WHERE db_name = ?
                        """,
                        (db_name,),
                    )
                    self._commit_write(conn, context)
                except Exception:
                    self._rollback_write(conn, context)
                    raise
        stale_index_keys = [
            key
            for key in self._index_metadata_versions
            if key[0] == db_name
        ]
        for key in stale_index_keys:
            self._index_metadata_versions.pop(key, None)
        self._invalidate_index_cache()
        self._invalidate_collection_id_cache()
        self._invalidate_collection_features_cache()
        self._profiler.clear(db_name)

    def _drop_indexes_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = self._load_indexes(db_name, coll_name)
                search_indexes = self._load_search_index_rows(db_name, coll_name)
                try:
                    self._begin_write(conn, context)
                    for index in indexes:
                        conn.execute(
                            f"DROP INDEX IF EXISTS {self._quote_identifier(str(index['physical_name']))}"
                        )
                        if index.get("scalar_physical_name"):
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(str(index['scalar_physical_name']))}"
                            )
                        if index.get("multikey"):
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(str(index['multikey_physical_name']))}"
                            )
                    conn.execute(
                        """
                        DELETE FROM scalar_index_entries
                        WHERE collection_id = ?
                        """,
                        (self._lookup_collection_id(conn, db_name, coll_name),),
                    )
                    conn.execute(
                        """
                        DELETE FROM multikey_entries
                        WHERE collection_id = ?
                        """,
                        (self._lookup_collection_id(conn, db_name, coll_name),),
                    )
                    conn.execute(
                        """
                        DELETE FROM indexes
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (db_name, coll_name),
                    )
                    self._commit_write(conn, context)
                    self._mark_index_metadata_changed(db_name, coll_name)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    def _create_search_index_sync(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> str:
        deadline = operation_deadline(max_time_ms)
        normalized_definition = SearchIndexDefinition(
            validate_search_index_definition(
                definition.definition,
                index_type=definition.index_type,
            ),
            name=definition.name,
            index_type=definition.index_type,
        )
        physical_name = (
            self._physical_search_index_name(db_name, coll_name, normalized_definition.name)
            if normalized_definition.index_type == "search"
            else None
        )
        with self._lock:
            conn = self._require_connection(context)
            try:
                self._begin_write(conn, context)
                self._ensure_collection_row(conn, db_name, coll_name)
                row = conn.execute(
                    """
                    SELECT index_type, definition_json
                    FROM search_indexes
                    WHERE db_name = ? AND coll_name = ? AND name = ?
                    """,
                    (db_name, coll_name, normalized_definition.name),
                ).fetchone()
                if row is not None:
                    existing = SearchIndexDefinition(
                        json_loads(row[1]),
                        name=normalized_definition.name,
                        index_type=row[0],
                    )
                    if existing != normalized_definition:
                        raise OperationFailure(
                            f"Conflicting search index definition for '{normalized_definition.name}'"
                        )
                    self._commit_write(conn, context)
                    return normalized_definition.name
                enforce_deadline(deadline)
                conn.execute(
                    """
                    INSERT INTO search_indexes (
                        db_name, coll_name, name, index_type, definition_json, physical_name, ready_at_epoch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        db_name,
                        coll_name,
                        normalized_definition.name,
                        normalized_definition.index_type,
                        json_dumps_compact(normalized_definition.definition, sort_keys=True),
                        physical_name,
                        self._pending_search_index_ready_at(),
                    ),
                )
                self._ensure_search_backend_sync(
                    conn,
                    db_name,
                    coll_name,
                    normalized_definition,
                    physical_name,
                )
                self._commit_write(conn, context)
                return normalized_definition.name
            except Exception:
                self._rollback_write(conn, context)
                raise

    def _list_search_indexes_sync(
        self,
        db_name: str,
        coll_name: str,
        name: str | None,
        context: ClientSession | None,
    ) -> list[SearchIndexDocument]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                rows = self._load_search_index_rows(db_name, coll_name)
        documents = [
            build_search_index_document(
                definition,
                ready=self._search_index_is_ready_sync(ready_at_epoch),
                ready_at_epoch=ready_at_epoch,
            )
            for definition, _physical_name, ready_at_epoch in rows
        ]
        if name is None:
            return documents
        return [document for document in documents if document["name"] == name]

    def _update_search_index_sync(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        definition: Document,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> None:
        deadline = operation_deadline(max_time_ms)
        with self._lock:
            conn = self._require_connection(context)
            try:
                self._begin_write(conn, context)
                row = conn.execute(
                    """
                    SELECT index_type, physical_name
                    FROM search_indexes
                    WHERE db_name = ? AND coll_name = ? AND name = ?
                    """,
                    (db_name, coll_name, name),
                ).fetchone()
                if row is None:
                    raise OperationFailure(f"search index not found with name [{name}]")
                normalized_definition = validate_search_index_definition(
                    definition,
                    index_type=row[0],
                )
                enforce_deadline(deadline)
                self._drop_search_backend_sync(conn, row[1])
                physical_name = (
                    self._physical_search_index_name(db_name, coll_name, name)
                    if row[0] == "search"
                    else None
                )
                conn.execute(
                    """
                    UPDATE search_indexes
                    SET definition_json = ?, physical_name = ?, ready_at_epoch = ?
                    WHERE db_name = ? AND coll_name = ? AND name = ?
                    """,
                    (
                        json_dumps_compact(normalized_definition, sort_keys=True),
                        physical_name,
                        self._pending_search_index_ready_at(),
                        db_name,
                        coll_name,
                        name,
                    ),
                )
                self._ensure_search_backend_sync(
                    conn,
                    db_name,
                    coll_name,
                    SearchIndexDefinition(
                        normalized_definition,
                        name=name,
                        index_type=row[0],
                    ),
                    physical_name,
                )
                self._commit_write(conn, context)
            except Exception:
                self._rollback_write(conn, context)
                raise

    def _drop_search_index_sync(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> None:
        deadline = operation_deadline(max_time_ms)
        with self._lock:
            conn = self._require_connection(context)
            try:
                self._begin_write(conn, context)
                row = conn.execute(
                    """
                    SELECT physical_name
                    FROM search_indexes
                    WHERE db_name = ? AND coll_name = ? AND name = ?
                    """,
                    (db_name, coll_name, name),
                ).fetchone()
                if row is None:
                    raise OperationFailure(f"search index not found with name [{name}]")
                enforce_deadline(deadline)
                cursor = conn.execute(
                    """
                    DELETE FROM search_indexes
                    WHERE db_name = ? AND coll_name = ? AND name = ?
                    """,
                    (db_name, coll_name, name),
                )
                self._drop_search_backend_sync(conn, row[0])
                self._commit_write(conn, context)
            except Exception:
                self._rollback_write(conn, context)
                raise

    def _search_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        operator: str,
        spec: object,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> list[Document]:
        deadline = operation_deadline(max_time_ms)
        query = compile_search_stage(operator, spec)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                rows = self._load_search_index_rows(db_name, coll_name, name=query.index_name)
                if not rows:
                    raise OperationFailure(f"search index not found with name [{query.index_name}]")
                definition, physical_name, ready_at_epoch = rows[0]
                if not self._search_index_is_ready_sync(ready_at_epoch):
                    raise OperationFailure(f"search index [{query.index_name}] is not ready yet")
                if isinstance(query, (SearchTextQuery, SearchPhraseQuery)) and definition.index_type != "search":
                    raise OperationFailure(f"search index [{query.index_name}] does not support $search")
                if isinstance(query, SearchVectorQuery) and definition.index_type != "vectorSearch":
                    raise OperationFailure(f"search index [{query.index_name}] does not support $vectorSearch")
                if isinstance(query, SearchVectorQuery):
                    documents = [
                        document
                        for _, document in self._load_documents(db_name, coll_name)
                    ]
                    enforce_deadline(deadline)
                    vector_hits: list[tuple[float, Document]] = []
                    for document in documents:
                        score = score_vector_document(
                            document,
                            definition=definition,
                            query=query,
                        )
                        if score is None:
                            continue
                        vector_hits.append((score, document))
                    vector_hits.sort(key=lambda item: item[0], reverse=True)
                    return [document for _score, document in vector_hits[: query.limit]]
                resolved_physical_name = self._ensure_search_backend_sync(
                    conn,
                    db_name,
                    coll_name,
                    definition,
                    physical_name,
                )
                if resolved_physical_name and self._supports_fts5(conn) and self._sqlite_table_exists(
                    conn,
                    resolved_physical_name,
                ):
                    sql = (
                        f"SELECT DISTINCT storage_key FROM {self._quote_identifier(resolved_physical_name)} "
                        "WHERE content MATCH ?"
                    )
                    params: list[object] = [sqlite_fts5_query(query)]
                    if query.paths is not None:
                        placeholders = ", ".join("?" for _ in query.paths)
                        sql += f" AND field_path IN ({placeholders})"
                        params.extend(query.paths)
                    storage_keys = [row[0] for row in conn.execute(sql, tuple(params)).fetchall()]
                    if not storage_keys:
                        return []
                    storage_key_set = set(storage_keys)
                    documents = {
                        storage_key: document
                        for storage_key, document in self._load_documents(db_name, coll_name)
                        if storage_key in storage_key_set
                    }
                    enforce_deadline(deadline)
                    return [documents[storage_key] for storage_key in storage_keys if storage_key in documents]

                documents = [
                    document
                    for _, document in self._load_documents(db_name, coll_name)
                    if (
                        matches_search_text_query(
                            document,
                            definition=definition,
                            query=query,
                        )
                        if isinstance(query, SearchTextQuery)
                        else matches_search_phrase_query(
                        document,
                        definition=definition,
                        query=query,
                        )
                    )
                ]
                enforce_deadline(deadline)
                return documents

    def _explain_search_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        operator: str,
        spec: object,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> QueryPlanExplanation:
        query = compile_search_stage(operator, spec)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                rows = self._load_search_index_rows(db_name, coll_name, name=query.index_name)
                if not rows:
                    raise OperationFailure(f"search index not found with name [{query.index_name}]")
                definition, physical_name, ready_at_epoch = rows[0]
                backend = "python"
                fts5_match: str | None = None
                if isinstance(query, (SearchTextQuery, SearchPhraseQuery)):
                    resolved_physical_name = self._ensure_search_backend_sync(
                        conn,
                        db_name,
                        coll_name,
                        definition,
                        physical_name,
                    )
                    if (
                        resolved_physical_name
                        and self._supports_fts5(conn)
                        and self._sqlite_table_exists(conn, resolved_physical_name)
                    ):
                        backend = "fts5"
                        fts5_match = sqlite_fts5_query(query)
                elif isinstance(query, SearchVectorQuery):
                    backend = "python"
        return QueryPlanExplanation(
            engine="sqlite",
            strategy="search",
            plan="python-vector-search" if isinstance(query, SearchVectorQuery) else f"{backend}-search",
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
                "backend": backend,
                "status": "READY" if self._search_index_is_ready_sync(ready_at_epoch) else "PENDING",
                "definition": build_search_index_document(
                    definition,
                    ready=self._search_index_is_ready_sync(ready_at_epoch),
                    ready_at_epoch=ready_at_epoch,
                ),
                "queryOperator": "phrase" if isinstance(query, SearchPhraseQuery) else "text" if isinstance(query, SearchTextQuery) else None,
                "query": query.raw_query if isinstance(query, (SearchTextQuery, SearchPhraseQuery)) else None,
                "paths": list(query.paths) if isinstance(query, (SearchTextQuery, SearchPhraseQuery)) and query.paths is not None else None,
                "fts5_match": fts5_match,
                "path": query.path if isinstance(query, SearchVectorQuery) else None,
                "queryVector": list(query.query_vector) if isinstance(query, SearchVectorQuery) else None,
                "limit": query.limit if isinstance(query, SearchVectorQuery) else None,
                "numCandidates": query.num_candidates if isinstance(query, SearchVectorQuery) else None,
                "vector_paths": list(vector_field_paths(definition)) if definition.index_type == "vectorSearch" else None,
            },
        )

    def _list_databases_sync(self, context: ClientSession | None = None) -> list[str]:
        with self._lock:
            conn = self._require_connection(context)
            cursor = conn.execute(
                """
                SELECT db_name
                FROM collections
                UNION
                SELECT db_name
                FROM documents
                UNION
                SELECT db_name
                FROM indexes
                UNION
                SELECT db_name
                FROM search_indexes
                ORDER BY db_name
                """
            )
            names = [row[0] for row in cursor.fetchall()]
        for db_name in list(self._profiler._settings.keys()):
            if db_name not in names:
                names.append(db_name)
        return sorted(names)

    def _list_collections_sync(self, db_name: str, context: ClientSession | None = None) -> list[str]:
        with self._lock:
            conn = self._require_connection(context)
            cursor = conn.execute(
                """
                SELECT coll_name
                FROM collections
                WHERE db_name = ?
                UNION
                SELECT coll_name
                FROM documents
                WHERE db_name = ?
                UNION
                SELECT coll_name
                FROM indexes
                WHERE db_name = ?
                UNION
                SELECT coll_name
                FROM search_indexes
                WHERE db_name = ?
                ORDER BY coll_name
                """,
                (db_name, db_name, db_name, db_name),
            )
            names = [row[0] for row in cursor.fetchall()]
        if self._profiler.namespace_visible(db_name):
            names = sorted(set(names) | {self._PROFILE_COLLECTION_NAME})
        return names

    def _create_collection_sync(
        self,
        db_name: str,
        coll_name: str,
        options: dict[str, object] | None = None,
        context: ClientSession | None = None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            try:
                self._begin_write(conn, context)
                if self._collection_exists_sync(conn, db_name, coll_name):
                    raise CollectionInvalid(f"collection '{coll_name}' already exists")
                self._ensure_collection_row(conn, db_name, coll_name, options=options)
                self._commit_write(conn, context)
            except Exception:
                self._rollback_write(conn, context)
                raise

    def _rename_collection_sync(
        self,
        db_name: str,
        coll_name: str,
        new_name: str,
        context: ClientSession | None = None,
    ) -> None:
        if coll_name == new_name:
            raise CollectionInvalid("collection names must differ")
        with self._lock:
            conn = self._require_connection(context)
            try:
                self._begin_write(conn, context)
                if not self._collection_exists_sync(conn, db_name, coll_name):
                    raise CollectionInvalid(f"collection '{coll_name}' does not exist")
                if self._collection_exists_sync(conn, db_name, new_name):
                    raise CollectionInvalid(f"collection '{new_name}' already exists")
                for table_name in ("collections", "documents", "indexes", "search_indexes"):
                    conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET coll_name = ?
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (new_name, db_name, coll_name),
                    )
                self._commit_write(conn, context)
                self._mark_index_metadata_changed(db_name, coll_name)
                self._mark_index_metadata_changed(db_name, new_name)
                self._invalidate_collection_id_cache(db_name, coll_name)
                self._invalidate_collection_id_cache(db_name, new_name)
                self._invalidate_collection_features_cache(db_name, coll_name)
                self._invalidate_collection_features_cache(db_name, new_name)
            except Exception:
                self._rollback_write(conn, context)
                raise

    def _drop_collection_sync(self, db_name: str, coll_name: str, context: ClientSession | None = None) -> None:
        if self._is_profile_namespace(coll_name):
            self._profiler.clear(db_name)
            return
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = self._load_indexes(db_name, coll_name)
                search_indexes = self._load_search_index_rows(db_name, coll_name)
                try:
                    self._begin_write(conn, context)
                    for index in indexes:
                        conn.execute(f"DROP INDEX IF EXISTS {self._quote_identifier(index['physical_name'])}")
                        if index.get("scalar_physical_name"):
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(index['scalar_physical_name'])}"
                            )
                        if index.get("multikey"):
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(index['multikey_physical_name'])}"
                            )
                    for _definition, physical_name, _ready_at_epoch in search_indexes:
                        self._drop_search_backend_sync(conn, physical_name)
                    conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (db_name, coll_name),
                    )
                    conn.execute(
                        """
                        DELETE FROM scalar_index_entries
                        WHERE collection_id = ?
                        """,
                        (self._lookup_collection_id(conn, db_name, coll_name),),
                    )
                    conn.execute(
                        """
                        DELETE FROM multikey_entries
                        WHERE collection_id = ?
                        """,
                        (self._lookup_collection_id(conn, db_name, coll_name),),
                    )
                    conn.execute(
                        """
                        DELETE FROM indexes
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (db_name, coll_name),
                    )
                    conn.execute(
                        """
                        DELETE FROM search_indexes
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (db_name, coll_name),
                    )
                    conn.execute(
                        """
                        DELETE FROM collections
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (db_name, coll_name),
                    )
                    self._commit_write(conn, context)
                    self._mark_index_metadata_changed(db_name, coll_name)
                    self._invalidate_collection_id_cache(db_name, coll_name)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    @override
    async def connect(self) -> None:
        await self._run_blocking(self._connect_sync)

    @override
    async def disconnect(self) -> None:
        await self._run_blocking(self._disconnect_sync)
        if self._connection_count == 0:
            self._shutdown_executor()

    @override
    async def set_profiling_level(
        self,
        db_name: str,
        level: int,
        *,
        slow_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> ProfilingCommandResult:
        del context
        return self._profiler.set_level(db_name, level, slow_ms=slow_ms)

    @override
    async def put_document(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        overwrite: bool = True,
        *,
        context: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ) -> bool:
        return await self._run_blocking(
            self._put_document_sync,
            db_name,
            coll_name,
            document,
            overwrite,
            context,
            bypass_document_validation=bypass_document_validation,
        )

    async def put_documents_bulk(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        *,
        context: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ) -> list[bool]:
        snapshot_options: dict[str, object] | None = None
        snapshot_indexes: list[EngineIndexRecord] = []
        loop = asyncio.get_running_loop()
        if not bypass_document_validation or documents:
            snapshot_options, snapshot_indexes = await self._run_blocking(
                self._snapshot_bulk_insert_preparation_sync,
                db_name,
                coll_name,
                context,
            )
        if not bypass_document_validation:
            validations = [
                loop.run_in_executor(
                    self._ensure_executor(),
                    partial(
                        enforce_collection_document_validation,
                        document,
                        options=snapshot_options,
                        original_document=None,
                        dialect=MONGODB_DIALECT_70,
                    ),
                )
                for document in documents
            ]
            await asyncio.gather(*validations)
        prepared_documents = await asyncio.gather(
            *[
                loop.run_in_executor(
                    self._ensure_executor(),
                    partial(self._prepare_bulk_document_with_indexes_sync, document, snapshot_indexes),
                )
                for document in documents
            ]
        )
        return await self._run_blocking(
            self._put_documents_bulk_sync,
            db_name,
            coll_name,
            documents,
            list(prepared_documents),
            snapshot_indexes,
            context,
            bypass_document_validation=bypass_document_validation,
            snapshot_options=snapshot_options,
        )

    @override
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        return await self._run_blocking(self._get_document_sync, db_name, coll_name, doc_id, projection, dialect, context)

    @override
    async def delete_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, context: ClientSession | None = None) -> bool:
        return await self._run_blocking(self._delete_document_sync, db_name, coll_name, doc_id, context)

    @override
    def scan_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> AsyncIterable[Document]:

        async def _scan() -> AsyncIterable[Document]:
            self._record_operation_metadata(
                context,
                operation="scan_collection",
                comment=semantics.comment,
                max_time_ms=semantics.max_time_ms,
                hint=semantics.hint,
            )

            if self._path == ":memory:":
                for document in self._iter_scan_documents_sync(
                    db_name,
                    coll_name,
                    semantics,
                    context=context,
                    hint=semantics.hint,
                ):
                    yield document
                return

            items: queue.Queue[object] = queue.Queue()
            sentinel = object()
            stop_event = threading.Event()

            def _produce() -> None:
                with self._scan_condition:
                    self._active_scan_count += 1
                batch: list[Document] = []
                try:
                    for document in self._iter_scan_documents_sync(
                        db_name,
                        coll_name,
                        semantics,
                        context=context,
                        hint=semantics.hint,
                        stop_event=stop_event,
                    ):
                        if stop_event.is_set():
                            break
                        batch.append(document)
                        if len(batch) >= _ASYNC_SCAN_QUEUE_BATCH_SIZE:
                            items.put(batch)
                            batch = []
                except Exception as exc:
                    if batch:
                        items.put(batch)
                    items.put(exc)
                finally:
                    if batch:
                        items.put(batch)
                    with self._scan_condition:
                        self._active_scan_count -= 1
                        self._scan_condition.notify_all()
                    items.put(sentinel)

            producer = asyncio.create_task(self._run_blocking(_produce))
            try:
                while True:
                    item = await self._run_blocking(items.get)
                    if item is sentinel:
                        break
                    if isinstance(item, Exception):
                        raise item
                    for document in item:
                        yield document
            finally:
                stop_event.set()
                await producer

        return _scan()

    @override
    async def update_with_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        upsert: bool = False,
        upsert_seed: Document | None = None,
        *,
        selector_filter: Filter | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ) -> UpdateResult[DocumentId]:
        if operation.compiled_update_plan is None or operation.compiled_upsert_plan is None:
            raise OperationFailure("update operation does not include a compiled update plan")
        return await self._run_blocking(
            self._update_with_operation_sync,
            db_name,
            coll_name,
            operation,
            upsert,
            upsert_seed,
            selector_filter,
            context,
            dialect,
            bypass_document_validation,
        )

    def _update_with_operation_sync(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        upsert: bool,
        upsert_seed: Document | None,
        selector_filter: Filter | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
        bypass_document_validation: bool = False,
    ) -> UpdateResult[DocumentId]:
        semantics = compile_update_semantics(
            operation,
            dialect=dialect,
            selector_filter=selector_filter,
        )
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._purge_expired_documents_sync(conn, db_name, coll_name, context=context)
                selected: tuple[str, Document] | None = None
                sql_selection_supported = False
                collection_options = self._collection_options_or_empty_sync(
                    conn,
                    db_name,
                    coll_name,
                )
                try:
                    if self._dialect_requires_python_fallback(semantics.dialect):
                        raise NotImplementedError("Custom dialect requires Python fallback")
                    if semantics.collation is not None:
                        raise NotImplementedError("Collation requires Python update fallback")
                    selected = self._select_first_document_for_plan(db_name, coll_name, semantics.query_plan)
                    sql_selection_supported = True
                except (NotImplementedError, TypeError):
                    pass

                if selected is None and not sql_selection_supported:
                    for storage_key, document in self._load_documents(db_name, coll_name):
                        if not QueryEngine.match_plan(
                            document,
                            semantics.query_plan,
                            dialect=semantics.dialect,
                            collation=semantics.collation,
                        ):
                            continue
                        selected = (storage_key, document)
                        break

                if selected is not None:
                    storage_key, original_document = selected
                    document = deepcopy(original_document)
                    modified = semantics.compiled_update_plan.apply(document)
                    if not modified:
                        return UpdateResult(matched_count=1, modified_count=0)
                    if not bypass_document_validation:
                        enforce_collection_document_validation(
                            document,
                            options=collection_options,
                            original_document=original_document,
                            dialect=semantics.dialect,
                        )
                    self._validate_document_against_unique_indexes(
                        db_name,
                        coll_name,
                        document,
                        exclude_storage_key=storage_key,
                    )
                    indexes = self._load_indexes(db_name, coll_name)
                    search_indexes = self._load_search_index_rows(db_name, coll_name)

                    try:
                        if self._dialect_requires_python_fallback(semantics.dialect):
                            raise NotImplementedError("Custom dialect requires Python fallback")
                        if operation.array_filters is not None:
                            raise NotImplementedError("array_filters require Python update fallback")
                        if not isinstance(semantics.compiled_update_plan, CompiledUpdatePlan):
                            raise NotImplementedError("Aggregation pipeline updates require Python update fallback")
                        update_sql, update_params = translate_compiled_update_plan(
                            semantics.compiled_update_plan,
                            current_document=original_document,
                        )
                        self._begin_write(conn, context)
                        conn.execute(
                            f"""
                            UPDATE documents
                            SET document = {update_sql}
                            WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                            """,
                            (*update_params, db_name, coll_name, storage_key),
                        )
                        self._rebuild_multikey_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                        self._rebuild_scalar_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                        self._replace_search_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            search_indexes=search_indexes,
                        )
                        self._commit_write(conn, context)
                        self._invalidate_collection_features_cache(db_name, coll_name)
                        return UpdateResult(matched_count=1, modified_count=1)
                    except (NotImplementedError, TypeError):
                        self._rollback_write(conn, context)
                    except sqlite3.IntegrityError as exc:
                        self._rollback_write(conn, context)
                        raise DuplicateKeyError(str(exc)) from exc

                    try:
                        self._begin_write(conn, context)
                        conn.execute(
                            """
                            UPDATE documents
                            SET document = ?
                            WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                            """,
                            (self._serialize_document(document), db_name, coll_name, storage_key),
                        )
                        self._rebuild_multikey_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                        self._rebuild_scalar_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                        self._replace_search_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            search_indexes=search_indexes,
                        )
                        self._commit_write(conn, context)
                        self._invalidate_collection_features_cache(db_name, coll_name)
                        return UpdateResult(matched_count=1, modified_count=1)
                    except sqlite3.IntegrityError as exc:
                        self._rollback_write(conn, context)
                        raise DuplicateKeyError(str(exc)) from exc

                if not upsert:
                    return UpdateResult(matched_count=0, modified_count=0)

                new_doc = deepcopy(upsert_seed or {})
                semantics.compiled_upsert_plan.apply(new_doc)
                if "_id" not in new_doc:
                    new_doc["_id"] = ObjectId()
                if not bypass_document_validation:
                    enforce_collection_document_validation(
                        new_doc,
                        options=collection_options,
                        original_document=None,
                        is_upsert_insert=True,
                        dialect=semantics.dialect,
                    )
                self._validate_document_against_unique_indexes(db_name, coll_name, new_doc)

                storage_key = self._storage_key(new_doc["_id"])
                indexes = self._load_indexes(db_name, coll_name)
                search_indexes = self._load_search_index_rows(db_name, coll_name)
                try:
                    self._begin_write(conn, context)
                    conn.execute(
                        """
                        INSERT INTO documents (db_name, coll_name, storage_key, document)
                        VALUES (?, ?, ?, ?)
                        """,
                        (db_name, coll_name, storage_key, self._serialize_document(new_doc)),
                    )
                    self._rebuild_multikey_entries_for_document(
                        conn,
                        db_name,
                        coll_name,
                        storage_key,
                        new_doc,
                        indexes,
                    )
                    self._rebuild_scalar_entries_for_document(
                        conn,
                        db_name,
                        coll_name,
                        storage_key,
                        new_doc,
                        indexes,
                    )
                    self._replace_search_entries_for_document(
                        conn,
                        db_name,
                        coll_name,
                        storage_key,
                        new_doc,
                        search_indexes=search_indexes,
                    )
                    self._commit_write(conn, context)
                    self._invalidate_collection_features_cache(db_name, coll_name)
                except sqlite3.IntegrityError as exc:
                    self._rollback_write(conn, context)
                    raise DuplicateKeyError(str(exc)) from exc

                return UpdateResult(
                    matched_count=0,
                    modified_count=0,
                    upserted_id=new_doc["_id"],
                )

    @override
    async def delete_with_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> DeleteResult:
        return await self._run_blocking(
            self._delete_matching_document_sync,
            db_name,
            coll_name,
            operation.filter_spec,
            operation.plan,
            context,
            dialect,
            operation.collation,
        )

    @override
    async def count_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> int:
        return await self._run_blocking(
            self._count_matching_documents_sync,
            db_name,
            coll_name,
            semantics,
            None,
            context,
            None,
        )

    @override
    async def create_index(
        self,
        db_name: str,
        coll_name: str,
        keys: IndexKeySpec,
        *,
        unique: bool = False,
        name: str | None = None,
        sparse: bool = False,
        partial_filter_expression: Filter | None = None,
        expire_after_seconds: int | None = None,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> str:
        return await self._run_blocking(
            self._create_index_sync,
            db_name,
            coll_name,
            keys,
            unique,
            name,
            sparse,
            partial_filter_expression,
            expire_after_seconds,
            max_time_ms,
            context,
        )

    @override
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[IndexDocument]:
        return await self._run_blocking(self._list_indexes_sync, db_name, coll_name, context)

    @override
    async def index_information(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> IndexInformation:
        return await self._run_blocking(self._index_information_sync, db_name, coll_name, context)

    @override
    async def drop_index(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(self._drop_index_sync, db_name, coll_name, index_or_name, context)

    @override
    async def drop_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(self._drop_indexes_sync, db_name, coll_name, context)

    @override
    async def create_search_index(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> str:
        return await self._run_blocking(
            self._create_search_index_sync,
            db_name,
            coll_name,
            definition,
            max_time_ms,
            context,
        )

    @override
    async def list_search_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        name: str | None = None,
        context: ClientSession | None = None,
    ) -> list[SearchIndexDocument]:
        return await self._run_blocking(
            self._list_search_indexes_sync,
            db_name,
            coll_name,
            name,
            context,
        )

    @override
    async def update_search_index(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        definition: Document,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(
            self._update_search_index_sync,
            db_name,
            coll_name,
            name,
            definition,
            max_time_ms,
            context,
        )

    @override
    async def drop_search_index(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(
            self._drop_search_index_sync,
            db_name,
            coll_name,
            name,
            max_time_ms,
            context,
        )

    async def search_documents(
        self,
        db_name: str,
        coll_name: str,
        operator: str,
        spec: object,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> list[Document]:
        return await self._run_blocking(
            self._search_documents_sync,
            db_name,
            coll_name,
            operator,
            spec,
            max_time_ms,
            context,
        )

    async def explain_search_documents(
        self,
        db_name: str,
        coll_name: str,
        operator: str,
        spec: object,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation:
        return await self._run_blocking(
            self._explain_search_documents_sync,
            db_name,
            coll_name,
            operator,
            spec,
            max_time_ms,
            context,
        )

    @override
    async def explain_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation:
        self._record_operation_metadata(
            context,
            operation="explain_query_plan",
            comment=semantics.comment,
            max_time_ms=semantics.max_time_ms,
            hint=semantics.hint,
        )
        hinted_index = await self._run_blocking(
            self._resolve_hint_index,
            db_name,
            coll_name,
            semantics.hint,
        )
        execution_plan = await self.plan_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
        )
        details: object
        if isinstance(execution_plan, SQLiteReadExecutionPlan) and execution_plan.use_sql:
            sql_details = await self._run_blocking(
                self._explain_query_plan_sync,
                db_name,
                coll_name,
                semantics,
                hint=semantics.hint,
            )
            if execution_plan.fallback_reason is None:
                details = sql_details
            else:
                details = {
                    "engine_details": sql_details,
                    "fallback_reason": execution_plan.fallback_reason,
                }
        else:
            details = {"fallback_reason": execution_plan.fallback_reason}
        virtual_details = describe_virtual_index_usage(
            await self._run_blocking(self._list_indexes_sync, db_name, coll_name, context),
            semantics.query_plan,
            hinted_index_name=None if hinted_index is None else hinted_index["name"],
            dialect=semantics.dialect,
        )
        if virtual_details is not None:
            if isinstance(details, dict):
                details = {**details, **virtual_details}
            else:
                details = {"engine_details": details, **virtual_details}
        return build_query_plan_explanation(
            engine="sqlite",
            strategy=execution_plan.strategy,
            semantics=semantics,
            details=details,
            hinted_index=None if hinted_index is None else hinted_index["name"],
            execution_lineage=execution_plan.execution_lineage,
            physical_plan=execution_plan.physical_plan,
            fallback_reason=execution_plan.fallback_reason,
        )

    @override
    async def list_databases(self, *, context: ClientSession | None = None) -> list[str]:
        return await self._run_blocking(self._list_databases_sync, context)

    async def drop_database(
        self,
        db_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(self._drop_database_sync, db_name, context)

    @override
    async def list_collections(self, db_name: str, *, context: ClientSession | None = None) -> list[str]:
        return await self._run_blocking(self._list_collections_sync, db_name, context)

    @override
    async def collection_options(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        return await self._run_blocking(
            self._collection_options_sync,
            db_name,
            coll_name,
            context,
        )

    @override
    async def create_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        options: dict[str, object] | None = None,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(
            self._create_collection_sync,
            db_name,
            coll_name,
            options,
            context,
        )

    @override
    async def rename_collection(
        self,
        db_name: str,
        coll_name: str,
        new_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(
            self._rename_collection_sync,
            db_name,
            coll_name,
            new_name,
            context,
        )

    @override
    async def drop_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(self._drop_collection_sync, db_name, coll_name, context)
