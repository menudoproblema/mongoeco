import asyncio
import atexit
import contextvars
import datetime
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

from collections.abc import AsyncIterable, Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext, suppress
from copy import deepcopy
from dataclasses import dataclass, replace
from decimal import Decimal
from functools import partial, wraps
from pathlib import Path
from typing import Any, override

from mongoeco.api.operations import (
    FindOperation,
    UpdateOperation,
    compile_update_operation,
)
from mongoeco.compat import (
    MONGODB_DIALECT_70,
    MongoDialect,
    MongoDialect70,
    MongoDialect80,
)
from mongoeco.core.aggregation.cost import AggregationCostPolicy
from mongoeco.core.bson_ordering import (
    SQLITE_SORT_BUCKET_WEIGHTS,
    bson_engine_key,
    bson_numeric_index_key,
)
from mongoeco.core.bson_scalars import utc_bson_now
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import (
    normalize_collation,
    values_equal_with_collation,
)
from mongoeco.core.expression_context import (
    current_execution_now,
    ensure_expression_context,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.identity import (
    assert_document_kept_storage_key,
    assert_document_matches_storage_key,
    assert_valid_root_document_id,
    canonical_document_id,
    document_matches_root_id_lookup,
    materialize_merge_insert_document,
    materialize_merge_update_document,
)
from mongoeco.core.json_compat import json_dumps_compact, json_loads
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.operators import CompiledUpdatePlan
from mongoeco.core.paths import get_document_value
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    ElemMatchCondition,
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
from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
    legacy_document_from_runtime_state,
)
from mongoeco.core.search import (
    SearchQuery,
    SearchVectorQuery,
    attach_text_score,
    classic_text_score,
    collect_search_metadata,
    resolve_classic_text_index_for_hint,
)
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_models import (
    SearchCollectorPlan,
    SearchExecutionMode,
    SearchExecutionOutcome,
    SearchExecutionTrace,
    SearchExplainVerbosity,
)
from mongoeco.core.sorting import sort_documents
from mongoeco.engines._active_operations import LocalActiveOperationRegistry
from mongoeco.engines._change_dispatch import ConsumerDispatchCoordinator
from mongoeco.engines._runtime_metrics import LocalRuntimeMetrics
from mongoeco.engines._shared_ttl import (
    coerce_ttl_datetime,
    document_expired_by_ttl,
)
from mongoeco.engines._sqlite_admin_runtime import SQLiteAdminRuntime
from mongoeco.engines._sqlite_explain_contract import (
    sqlite_planning_issues,
    sqlite_pushdown_details,
    sqlite_pushdown_followup_hints,
)
from mongoeco.engines._sqlite_fast_paths import (
    build_scalar_sort_select_sql as _sqlite_build_scalar_sort_select_sql,
    build_select_sql as _sqlite_build_select_sql,
)
from mongoeco.engines._sqlite_index_admin import (
    build_index_information as _sqlite_build_index_information,
    create_index as _sqlite_create_index,
    drop_all_indexes as _sqlite_drop_all_indexes,
    drop_index as _sqlite_drop_index,
    list_index_documents as _sqlite_list_index_documents,
)
from mongoeco.engines._sqlite_index_runtime import (
    backfill_scalar_indexes_sync as _sqlite_runtime_backfill_scalar_indexes_sync,
    ensure_multikey_physical_indexes_sync as _sqlite_runtime_ensure_multikey_physical_indexes_sync,
    ensure_scalar_physical_indexes_sync as _sqlite_runtime_ensure_scalar_physical_indexes_sync,
    rebuild_multikey_entries_for_document as _sqlite_runtime_rebuild_multikey_entries_for_document,
    rebuild_scalar_entries_for_document as _sqlite_runtime_rebuild_scalar_entries_for_document,
    replace_multikey_entries_for_index_for_document as _sqlite_runtime_replace_multikey_entries_for_index_for_document,
    replace_scalar_entries_for_index_for_document as _sqlite_runtime_replace_scalar_entries_for_index_for_document,
)
from mongoeco.engines._sqlite_modify_ops import (
    delete_matching_document as _sqlite_delete_matching_document,
    update_with_operation as _sqlite_update_with_operation,
)
from mongoeco.engines._sqlite_namespace_admin import (
    create_collection as _sqlite_create_collection,
    drop_collection as _sqlite_drop_collection,
    rename_collection as _sqlite_rename_collection,
)
from mongoeco.engines._sqlite_outbox import (
    acquire_consumer_lease as _sqlite_acquire_consumer_lease,
    append_change as _sqlite_append_change,
    checkpoint_consumer as _sqlite_checkpoint_consumer,
    compact_change_outbox as _sqlite_compact_change_outbox,
    consumer_checkpoint as _sqlite_consumer_checkpoint,
    ensure_change_outbox_schema,
    expire_ephemeral_consumers as _sqlite_expire_ephemeral_consumers,
    outbox_info as _sqlite_outbox_info,
    read_committed_changes as _sqlite_read_committed_changes,
    register_consumer as _sqlite_register_consumer,
    release_consumer_lease as _sqlite_release_consumer_lease,
    renew_consumer_lease as _sqlite_renew_consumer_lease,
    renew_ephemeral_consumers as _sqlite_renew_ephemeral_consumers,
    unregister_consumer as _sqlite_unregister_consumer,
)
from mongoeco.engines._sqlite_plan_heuristics import (
    plan_has_array_traversing_paths as _sqlite_plan_has_array_traversing_paths,
    plan_requires_python_for_dbref_paths as _sqlite_plan_requires_python_for_dbref_paths,
    plan_requires_python_for_tagged_type as _sqlite_plan_requires_python_for_tagged_type,
    sort_requires_python as _sqlite_sort_requires_python,
)
from mongoeco.engines._sqlite_read_fast_path_runtime import (
    build_scalar_indexed_top_level_equals_sql as _sqlite_runtime_build_scalar_indexed_top_level_equals_sql,
    build_scalar_indexed_top_level_range_sql as _sqlite_runtime_build_scalar_indexed_top_level_range_sql,
    can_use_scalar_range_fast_path as _sqlite_runtime_can_use_scalar_range_fast_path,
    find_scalar_fast_path_index as _sqlite_runtime_find_scalar_fast_path_index,
    select_first_document_for_plan as _sqlite_runtime_select_first_document_for_plan,
    select_first_document_for_scalar_index as _sqlite_runtime_select_first_document_for_scalar_index,
    select_first_document_for_scalar_range as _sqlite_runtime_select_first_document_for_scalar_range,
)
from mongoeco.engines._sqlite_read_ops import (
    get_document as _sqlite_get_document,
    require_sql_execution_plan as _sqlite_require_sql_execution_plan,
)
from mongoeco.engines._sqlite_read_runtime import (
    compile_read_execution_plan as _sqlite_runtime_compile_read_execution_plan,
    explain_query_plan_sync as _sqlite_runtime_explain_query_plan_sync,
    plan_find_semantics_sync as _sqlite_runtime_plan_find_semantics_sync,
)
from mongoeco.engines._sqlite_runtime import (
    SQLiteCacheState,
    SQLiteRuntimeState,
)
from mongoeco.engines._sqlite_search_runtime import (
    collect_search_metadata_pushdown_sync as _sqlite_collect_search_metadata_pushdown_sync,
    create_search_index_sync as _sqlite_create_search_index_sync,
    delete_search_entries_for_storage_key as _sqlite_delete_search_entries_for_storage_key,
    drop_search_backend_sync as _sqlite_drop_search_backend_sync,
    drop_search_index_sync as _sqlite_drop_search_index_sync,
    ensure_search_backend_sync as _sqlite_ensure_search_backend_sync,
    ensure_vector_search_backend_sync as _sqlite_ensure_vector_search_backend_sync,
    exact_vector_hits_sync as _sqlite_exact_vector_hits_sync,
    execute_sqlite_search_query as _sqlite_execute_search_query,
    explain_search_documents_sync as _sqlite_explain_search_documents_sync,
    list_search_indexes_sync as _sqlite_list_search_indexes_sync,
    load_search_index_rows as _sqlite_runtime_load_search_index_rows,
    load_search_indexes as _sqlite_runtime_load_search_indexes,
    pending_search_index_ready_at as _sqlite_pending_search_index_ready_at,
    plan_search_documents_sync as _sqlite_plan_search_documents_sync,
    replace_search_entries_for_document as _sqlite_replace_search_entries_for_document,
    search_documents_sync as _sqlite_search_documents_sync,
    search_index_is_ready_sync as _sqlite_search_index_is_ready_sync,
    update_search_index_sync as _sqlite_update_search_index_sync,
)
from mongoeco.engines._sqlite_session_runtime import SQLiteSessionRuntime
from mongoeco.engines._sqlite_vector_backend import (
    SQLiteVectorBackendState,
    vector_backend_stats_document,
)
from mongoeco.engines._sqlite_write_ops import (
    delete_document as _sqlite_delete_document,
    put_document as _sqlite_put_document,
    put_documents_bulk as _sqlite_put_documents_bulk,
)
from mongoeco.engines._sqlite_write_scope import sqlite_write_scope
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.capabilities import (
    EngineCapabilities,
    SearchEngineCapabilities,
)
from mongoeco.engines.profiling import EngineProfiler
from mongoeco.engines.results import (
    CommittedChange,
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
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
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.engines.sqlite_planner import (
    SQLiteReadExecutionPlan,
)
from mongoeco.engines.sqlite_query import (
    SQLiteQueryTranslator,
    _normalize_comparable_value,
    _translate_all_condition,
    _translate_elem_match_condition,
    _translate_equals_scalar_only,
    _translate_same_type_comparison,
    _translate_scalar_equals,
    _translate_scalar_or_array_same_type_comparison,
    json_path_for_field,
    path_array_prefixes,
    translate_compiled_update_plan,
    translate_query_plan,
    type_expression_sql,
    value_expression_sql,
)
from mongoeco.engines.virtual_indexes import (
    describe_virtual_index_usage,
    document_in_virtual_index,
    normalize_partial_filter_expression,
    query_can_use_index,
)
from mongoeco.errors import (
    DuplicateKeyError,
    InvalidOperation,
    OperationFailure,
)
from mongoeco.session import ClientSession
from mongoeco.types import (
    ArrayFilters,
    CollationDocument,
    DBRef,
    Decimal128,
    Document,
    DocumentId,
    EngineIndexRecord,
    ExecutionLineageStep,
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
    is_ordered_index_spec,
    normalize_index_keys,
)


_SQLITE_SHARED_EXECUTOR_LOCK = threading.Lock()
_SQLITE_SHARED_EXECUTORS: dict[int, ThreadPoolExecutor] = {}
_SQLITE_CHANGE_DISPATCH = ConsumerDispatchCoordinator()
_ASYNC_SCAN_QUEUE_BATCH_SIZE = 64
_CHANGE_DISPATCH_LEASE_TTL_SECONDS = 30.0
_CHANGE_DISPATCH_HEARTBEAT_SECONDS = 10.0
_CHANGE_DISPATCH_RETRY_SECONDS = 0.01
_EPHEMERAL_CHANGE_CONSUMER_TTL_SECONDS = 3_600.0
_EPHEMERAL_CHANGE_CONSUMER_HEARTBEAT_SECONDS = 60.0
_EPHEMERAL_CHANGE_CONSUMER_STOP_TIMEOUT_SECONDS = 6.0


@dataclass(frozen=True, slots=True)
class _ScanProducerError:
    error: BaseException


class _ChangeDispatchHeartbeat:
    """Supervised state shared with the lease heartbeat thread."""

    def __init__(self) -> None:
        self.stop = threading.Event()
        self.lost = threading.Event()
        self._lock = threading.Lock()
        self._error: BaseException | None = None

    def fail(self, error: BaseException) -> None:
        with self._lock:
            if self._error is None:
                self._error = error
        self.lost.set()

    def raise_if_failed(self) -> None:
        with self._lock:
            error = self._error
        if error is not None:
            raise error
        if self.lost.is_set():
            message = "change consumer dispatch lease was lost"
            raise OperationFailure(message)


class _EphemeralRegistrationHeartbeat:
    """Lifecycle state for the engine-owned registration heartbeat."""

    def __init__(self) -> None:
        self.stop = threading.Event()
        self._lock = threading.Lock()
        self._error: BaseException | None = None

    def fail(self, error: BaseException) -> None:
        with self._lock:
            if self._error is None:
                self._error = error

    def error(self) -> BaseException | None:
        with self._lock:
            return self._error


def _shutdown_sqlite_shared_executors() -> None:
    with _SQLITE_SHARED_EXECUTOR_LOCK:
        executors = list(_SQLITE_SHARED_EXECUTORS.values())
        _SQLITE_SHARED_EXECUTORS.clear()
    for executor in executors:
        executor.shutdown(wait=True, cancel_futures=True)


def _cleanup_failed_connect(
    connect: Callable[["SQLiteEngine"], None],
) -> Callable[["SQLiteEngine"], None]:
    @wraps(connect)
    def guarded(engine: "SQLiteEngine") -> None:
        try:
            connect(engine)
        except BaseException:
            engine._abort_failed_connect_sync()
            raise

    return guarded


atexit.register(_shutdown_sqlite_shared_executors)


class SQLiteEngine(AsyncStorageEngine):
    capabilities = EngineCapabilities(
        injected_clock=True,
        explicit_read_snapshots=True,
        change_delivery="transactional-outbox",
        search=SearchEngineCapabilities(
            metadata_collectors=True,
            highlight=True,
            explain_verbosity=True,
            operators=frozenset({"$search", "$vectorSearch"}),
            vector_similarities=frozenset({"cosine", "dotProduct", "euclidean"}),
        ),
    )
    """Motor SQLite async-first usando la stdlib como backend persistente."""

    _PROFILE_COLLECTION_NAME = "system.profile"

    def __init__(
        self,
        path: str = ":memory:",
        codec: type[DocumentCodec] = DocumentCodec,
        executor_workers: int | None = None,
        aggregation_materialization_limit: int | None = 50_000,
        simulate_search_index_latency: float = 0.0,
        change_outbox_max_entries: int = 10_000,
    ):
        self._path = path
        self._codec = codec
        self._runtime_state = SQLiteRuntimeState()
        self._cache_state = SQLiteCacheState()
        self._lock = threading.RLock()
        if executor_workers is not None and executor_workers < 1:
            raise ValueError("executor_workers must be positive")
        if (
            not isinstance(change_outbox_max_entries, int)
            or isinstance(change_outbox_max_entries, bool)
            or change_outbox_max_entries < 1
        ):
            raise ValueError("change_outbox_max_entries must be positive")
        self._executor_workers = executor_workers or (
            min(32, max(4, (os.cpu_count() or 1) + 4)) if path != ":memory:" else 1
        )
        self._use_shared_executor = executor_workers is None
        self._profiler = EngineProfiler("sqlite")
        self._runtime_metrics = LocalRuntimeMetrics()
        self._active_operations = LocalActiveOperationRegistry()
        self._admin_runtime = SQLiteAdminRuntime(self)
        self._session_runtime = SQLiteSessionRuntime(self)
        self._change_outbox_checkpoints: dict[str, int] = {}
        self._registered_change_consumers: dict[str, bool] = {}
        self._change_delivery_connection: sqlite3.Connection | None = None
        self._pending_connection: sqlite3.Connection | None = None
        self._change_delivery_lock = threading.RLock()
        self._change_dispatch_owner = uuid.uuid4().hex
        self._registration_heartbeat_guard = threading.Lock()
        self._registration_heartbeat_state: _EphemeralRegistrationHeartbeat | None = (
            None
        )
        self._registration_heartbeat_thread: threading.Thread | None = None
        self._change_dispatch_scope = (
            self._change_dispatch_owner
            if path == ":memory:"
            else str(Path(path).expanduser().resolve())
        )
        self._change_outbox_max_entries = change_outbox_max_entries
        self._mvcc_version = 0
        self.aggregation_cost_policy = (
            None
            if aggregation_materialization_limit is None
            else AggregationCostPolicy(
                max_materialized_documents=aggregation_materialization_limit,
                require_spill_for_blocking_stages=True,
            )
        )
        self._simulate_search_index_latency = max(
            0.0,
            float(simulate_search_index_latency),
        )
        self._sql_translator = SQLiteQueryTranslator()

    @property
    def _connection(self) -> sqlite3.Connection | None:
        return self._runtime_state.connection

    @_connection.setter
    def _connection(self, value: sqlite3.Connection | None) -> None:
        self._runtime_state.connection = value

    @property
    def _connection_count(self) -> int:
        return self._runtime_state.connection_count

    @_connection_count.setter
    def _connection_count(self, value: int) -> None:
        self._runtime_state.connection_count = value

    @property
    def _transaction_owner_session_id(self) -> str | None:
        return self._runtime_state.transaction_owner_session_id

    @_transaction_owner_session_id.setter
    def _transaction_owner_session_id(self, value: str | None) -> None:
        self._runtime_state.transaction_owner_session_id = value

    @property
    def _scan_condition(self) -> threading.Condition:
        return self._runtime_state.scan_condition

    @property
    def _active_scan_count(self) -> int:
        return self._runtime_state.active_scan_count

    @_active_scan_count.setter
    def _active_scan_count(self, value: int) -> None:
        self._runtime_state.active_scan_count = value

    @property
    def _thread_local(self) -> threading.local:
        return self._runtime_state.thread_local

    @property
    def _executor(self) -> ThreadPoolExecutor | None:
        return self._runtime_state.executor

    @_executor.setter
    def _executor(self, value: ThreadPoolExecutor | None) -> None:
        self._runtime_state.executor = value

    @property
    def _owns_executor(self) -> bool:
        return self._runtime_state.owns_executor

    @_owns_executor.setter
    def _owns_executor(self, value: bool) -> None:
        self._runtime_state.owns_executor = value

    @property
    def _fts5_available(self) -> bool | None:
        return self._runtime_state.fts5_available

    @_fts5_available.setter
    def _fts5_available(self, value: bool | None) -> None:
        self._runtime_state.fts5_available = value

    @property
    def _index_cache(
        self,
    ) -> dict[tuple[str, str], tuple[int, list[EngineIndexRecord]]]:
        return self._cache_state.index_cache

    @property
    def _index_metadata_versions(self) -> dict[tuple[str, str], int]:
        return self._cache_state.index_metadata_versions

    @property
    def _collection_id_cache(self) -> dict[tuple[str, str], int]:
        return self._cache_state.collection_id_cache

    @property
    def _collection_features_cache(
        self,
    ) -> dict[tuple[str, str, str], bool | str]:
        return self._cache_state.collection_features_cache

    @property
    def _ensured_multikey_physical_indexes(self) -> set[str]:
        return self._cache_state.ensured_multikey_physical_indexes

    @property
    def _ensured_search_backends(self) -> set[str]:
        return self._cache_state.ensured_search_backends

    @property
    def _vector_search_backends(
        self,
    ) -> dict[tuple[str, str], SQLiteVectorBackendState]:
        return self._cache_state.vector_search_backends

    @property
    def _search_backend_versions(self) -> dict[tuple[str, str], int]:
        return self._cache_state.search_backend_versions

    @property
    def _materialized_search_entry_cache(
        self,
    ) -> dict[
        tuple[str, str, str, int],
        dict[str, tuple[tuple[tuple[str, str], ...], object]],
    ]:
        return self._cache_state.materialized_search_entry_cache

    def _clear_compound_search_caches(
        self,
        db_name: str | None = None,
        coll_name: str | None = None,
    ) -> None:
        for attr_name in (
            "_compound_should_score_cache",
            "_compound_rank_cache",
            "_compound_topk_prefilter_cache",
        ):
            cache = getattr(self, attr_name, None)
            if cache is None:
                continue
            if db_name is None:
                cache.clear()
                continue
            stale_keys = [
                key
                for key in cache
                if key[0] == db_name and (coll_name is None or key[1] == coll_name)
            ]
            for key in stale_keys:
                cache.pop(key, None)

    def _runtime_diagnostics_info(self) -> dict[str, object]:
        declared_search_index_count = 0
        pending_search_index_count = 0
        declared_vector_index_count = 0
        pending_vector_index_count = 0
        connection = self._connection
        change_delivery_connection = self._change_delivery_connection
        change_delivery_info: dict[str, object] = {
            "maxEntries": self._change_outbox_max_entries,
            "pendingEntries": 0,
            "consumerCount": 0,
            "prunedThrough": 0,
            "registrationHeartbeatActive": False,
            "registrationHeartbeatHealthy": True,
        }
        with self._registration_heartbeat_guard:
            heartbeat_thread = self._registration_heartbeat_thread
            heartbeat_state = self._registration_heartbeat_state
            change_delivery_info["registrationHeartbeatActive"] = bool(
                heartbeat_thread is not None and heartbeat_thread.is_alive(),
            )
            change_delivery_info["registrationHeartbeatHealthy"] = bool(
                heartbeat_state is None or heartbeat_state.error() is None,
            )
        if change_delivery_connection is not None or connection is not None:
            try:
                change_lock = (
                    self._change_delivery_lock
                    if change_delivery_connection is not None
                    else self._lock
                )
                with change_lock:
                    change_delivery_info.update(
                        _sqlite_outbox_info(
                            change_delivery_connection or connection,
                        ),
                    )
            except sqlite3.OperationalError:
                pass
            try:
                row = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS declared_count,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN ready_at_epoch IS NOT NULL AND ready_at_epoch > ?
                                    THEN 1
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS pending_count
                    FROM search_indexes
                    """,
                    (time.time(),),
                ).fetchone()
            except sqlite3.OperationalError:
                row = None
            if row is not None:
                declared_search_index_count = int(row[0] or 0)
                pending_search_index_count = int(row[1] or 0)
            try:
                vector_row = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS declared_count,
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN ready_at_epoch IS NOT NULL AND ready_at_epoch > ?
                                    THEN 1
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS pending_count
                    FROM search_indexes
                    WHERE index_type = 'vectorSearch'
                    """,
                    (time.time(),),
                ).fetchone()
            except sqlite3.OperationalError:
                vector_row = None
            if vector_row is not None:
                declared_vector_index_count = int(vector_row[0] or 0)
                pending_vector_index_count = int(vector_row[1] or 0)
        materialized_vector_backends = tuple(
            vector_backend_stats_document(state)
            for state in self._vector_search_backends.values()
        )
        return {
            "planner": {
                "engine": "sqlite",
                "pushdownModes": ["sql", "hybrid", "python"],
                "hybridSortFallback": True,
                "observed": self._runtime_metrics.planner_snapshot(),
            },
            "search": {
                "backend": "fts5-or-usearch-or-python-fallback",
                "fts5Available": self._fts5_available,
                "declaredIndexCount": declared_search_index_count,
                "pendingIndexCount": pending_search_index_count,
                "vectorBackend": "usearch",
                "declaredVectorIndexCount": declared_vector_index_count,
                "pendingVectorIndexCount": pending_vector_index_count,
                "materializedVectorBackendCount": len(
                    materialized_vector_backends,
                ),
                "materializedVectorBackends": list(
                    materialized_vector_backends,
                ),
                "ensuredBackendCount": len(self._ensured_search_backends),
                "simulatedIndexLatencySeconds": self._simulate_search_index_latency,
            },
            "caches": {
                "indexCacheEntries": len(self._index_cache),
                "collectionIdCacheEntries": len(self._collection_id_cache),
                "collectionFeatureCacheEntries": len(
                    self._collection_features_cache,
                ),
                "indexMetadataVersionEntries": len(
                    self._index_metadata_versions,
                ),
                "ensuredMultikeyPhysicalIndexCount": len(
                    self._ensured_multikey_physical_indexes,
                ),
                "vectorSearchBackendCount": len(self._vector_search_backends),
            },
            "changeDelivery": change_delivery_info,
        }

    def _ensure_executor(self) -> ThreadPoolExecutor:
        executor = self._executor
        if executor is None:
            if self._use_shared_executor:
                with _SQLITE_SHARED_EXECUTOR_LOCK:
                    executor = _SQLITE_SHARED_EXECUTORS.get(
                        self._executor_workers,
                    )
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
        # SQLite performs the semantic work in a worker thread.  ContextVars
        # do not cross that boundary implicitly, so copy the command context
        # (notably the injected clock) for each submitted job.
        context = contextvars.copy_context()
        return await loop.run_in_executor(
            self._ensure_executor(),
            context.run,
            partial(func, *args, **kwargs),
        )

    def _engine_key(self) -> str:
        return f"sqlite:{id(self)}"

    def _invalidate_index_cache(
        self,
        db_name: str | None = None,
        coll_name: str | None = None,
    ) -> None:
        if db_name is None or coll_name is None:
            self._index_cache.clear()
            return
        self._index_cache.pop((db_name, coll_name), None)

    def _invalidate_collection_id_cache(
        self,
        db_name: str | None = None,
        coll_name: str | None = None,
    ) -> None:
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
            key for key in self._collection_features_cache if key[:2] == prefix
        ]
        for key in stale_keys:
            self._collection_features_cache.pop(key, None)

    def _index_cache_version(self, db_name: str, coll_name: str) -> int:
        return self._index_metadata_versions.get((db_name, coll_name), 0)

    def _mark_index_metadata_changed(
        self,
        db_name: str,
        coll_name: str,
    ) -> None:
        cache_key = (db_name, coll_name)
        self._index_metadata_versions[cache_key] = (
            self._index_metadata_versions.get(cache_key, 0) + 1
        )
        self._invalidate_index_cache(db_name, coll_name)

    def _ensure_multikey_physical_indexes_sync(
        self,
        conn: sqlite3.Connection,
        indexes: list[EngineIndexRecord],
    ) -> None:
        _sqlite_runtime_ensure_multikey_physical_indexes_sync(
            self,
            conn,
            indexes,
        )

    def _ensure_scalar_physical_indexes_sync(
        self,
        conn: sqlite3.Connection,
        indexes: list[EngineIndexRecord],
    ) -> None:
        _sqlite_runtime_ensure_scalar_physical_indexes_sync(
            self,
            conn,
            indexes,
        )

    @staticmethod
    def _multikey_type_score(element_type: str) -> int:
        if element_type == "undefined":
            return 0
        return SQLITE_SORT_BUCKET_WEIGHTS.get(
            element_type,
            SQLITE_SORT_BUCKET_WEIGHTS["fallback"],
        )

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
        self._session_runtime.ensure_session_can_use_engine(context)
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
        self._runtime_metrics.record(op)
        self._admin_runtime.record_profile_event(
            db_name,
            op=op,
            command=command,
            duration_micros=duration_micros,
            execution_lineage=execution_lineage,
            fallback_reason=fallback_reason,
            ok=ok,
            errmsg=errmsg,
        )

    def _profile_is_active(
        self,
        db_name: str,
        *,
        duration_micros: int | None = None,
    ) -> bool:
        return self._profiler.is_active(
            db_name,
            duration_micros=duration_micros,
        )

    def _record_runtime_opcounter(self, op: str, *, amount: int = 1) -> None:
        self._runtime_metrics.record(op, amount=amount)

    def _runtime_opcounters_snapshot(self) -> dict[str, int]:
        return self._runtime_metrics.snapshot()

    def _begin_active_operation(
        self,
        *,
        command_name: str,
        operation_type: str,
        namespace: str | None,
        session_id: str | None = None,
        cursor_id: int | None = None,
        connection_id: int | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        killable: bool = True,
        metadata: dict[str, object] | None = None,
    ):
        return self._active_operations.begin(
            command_name=command_name,
            operation_type=operation_type,
            namespace=namespace,
            session_id=session_id,
            cursor_id=cursor_id,
            connection_id=connection_id,
            comment=comment,
            max_time_ms=max_time_ms,
            killable=killable,
            metadata=metadata,
        )

    def _complete_active_operation(self, opid: str) -> None:
        self._active_operations.complete(opid)

    def _snapshot_active_operations(self) -> list[dict[str, object]]:
        return self._active_operations.snapshot()

    def _cancel_active_operation(self, opid: str) -> bool:
        return self._active_operations.cancel(opid)

    def _is_profile_namespace(self, coll_name: str) -> bool:
        return self._admin_runtime.is_profile_namespace(coll_name)

    @override
    def create_session_state(self, session: ClientSession) -> None:
        self._session_runtime.create_session_state(session)

    def _sync_session_state(
        self,
        session: ClientSession,
        *,
        transaction_active: bool | None = None,
    ) -> None:
        self._session_runtime.sync_session_state(
            session,
            transaction_active=transaction_active,
        )

    def _start_session_transaction(self, session: ClientSession) -> None:
        self._session_runtime.start_session_transaction(session)

    def _commit_session_transaction(self, session: ClientSession) -> None:
        self._session_runtime.commit_session_transaction(session)

    def _abort_session_transaction(self, session: ClientSession) -> None:
        self._session_runtime.abort_session_transaction(session)

    @contextmanager
    def _bind_connection(self, conn: sqlite3.Connection):
        with self._session_runtime.bind_connection(conn):
            yield

    def _session_owns_transaction(self, context: ClientSession | None) -> bool:
        return self._session_runtime.session_owns_transaction(context)

    def _ensure_session_can_use_engine(
        self,
        context: ClientSession | None,
    ) -> None:
        self._session_runtime.ensure_session_can_use_engine(context)

    def _require_connection(
        self,
        context: ClientSession | None = None,
    ) -> sqlite3.Connection:
        return self._session_runtime.require_connection(context)

    def _begin_write(
        self,
        conn: sqlite3.Connection,
        context: ClientSession | None,
    ) -> None:
        self._session_runtime.begin_write(conn, context)

    def _commit_write(
        self,
        conn: sqlite3.Connection,
        context: ClientSession | None,
    ) -> None:
        self._session_runtime.commit_write(conn, context)

    def _rollback_write(
        self,
        conn: sqlite3.Connection,
        context: ClientSession | None,
    ) -> None:
        self._session_runtime.rollback_write(conn, context)

    def _dialect_requires_python_fallback(self, dialect: MongoDialect) -> bool:
        return type(dialect) not in (MongoDialect70, MongoDialect80)

    _coerce_ttl_datetime = staticmethod(coerce_ttl_datetime)

    def _document_expired_by_ttl(
        self,
        document: Document,
        index: EngineIndexRecord,
        *,
        now: datetime.datetime,
    ) -> bool:
        if not document_in_virtual_index(document, index):
            return False
        return document_expired_by_ttl(
            document,
            index,
            now=now,
            extract_values=QueryEngine.extract_values,
        )

    @staticmethod
    def _ttl_now() -> datetime.datetime:
        return (current_execution_now() or utc_bson_now()).replace(
            tzinfo=datetime.UTC,
        )

    def _purge_expired_documents_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None,
        now: datetime.datetime,
    ) -> int:
        self._ensure_session_can_use_engine(context)
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
        expired: list[tuple[str, Document]] = []
        for storage_key, document in self._load_documents(db_name, coll_name):
            if any(
                self._document_expired_by_ttl(document, index, now=now)
                for index in ttl_indexes
            ):
                assert_document_matches_storage_key(
                    document,
                    storage_key,
                    storage_key_for_id=self._storage_key,
                )
                expired.append((storage_key, document))
        if not expired:
            return 0
        with sqlite_write_scope(
            conn,
            begin_write=lambda current: self._begin_write(current, context),
            commit_write=lambda current: self._commit_write(current, context),
            rollback_write=lambda current: self._rollback_write(
                current,
                context,
            ),
        ):
            for storage_key, _document in expired:
                conn.execute(
                    """
                    DELETE FROM documents
                    WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                    """,
                    (db_name, coll_name, storage_key),
                )
                self._delete_multikey_entries_for_storage_key(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                )
                self._delete_scalar_entries_for_storage_key(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                )
                self._delete_search_entries_for_storage_key(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                )
        self._invalidate_collection_features_cache(db_name, coll_name)
        return len(expired)

    def _assert_expired_documents_have_stable_identity_sync(
        self,
        db_name: str,
        coll_name: str,
        ttl_indexes: list[EngineIndexRecord],
    ) -> None:
        effective_ttl_indexes = [
            index for index in ttl_indexes if index.expire_after_seconds is not None
        ]
        if not effective_ttl_indexes:
            return
        now = self._ttl_now()
        for storage_key, document in self._load_documents(db_name, coll_name):
            if not any(
                self._document_expired_by_ttl(document, index, now=now)
                for index in effective_ttl_indexes
            ):
                continue
            assert_document_matches_storage_key(
                document,
                storage_key,
                storage_key_for_id=self._storage_key,
            )

    def _storage_key(self, value: Any) -> str:
        return repr(canonical_document_id(value))

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _physical_index_name(
        self,
        db_name: str,
        coll_name: str,
        index_name: str,
    ) -> str:
        digest = hashlib.sha1(
            f"{db_name}:{coll_name}:{index_name}".encode(),
        ).hexdigest()[:16]
        return f"idx_{digest}"

    def _physical_multikey_index_name(
        self,
        db_name: str,
        coll_name: str,
        index_name: str,
    ) -> str:
        digest = hashlib.sha1(
            f"{db_name}:{coll_name}:{index_name}:multikey".encode(),
        ).hexdigest()[:16]
        return f"mkidx_{digest}"

    def _physical_scalar_index_name(
        self,
        db_name: str,
        coll_name: str,
        index_name: str,
    ) -> str:
        digest = hashlib.sha1(
            f"{db_name}:{coll_name}:{index_name}:scalar".encode(),
        ).hexdigest()[:16]
        return f"scidx_{digest}"

    def _physical_search_index_name(
        self,
        db_name: str,
        coll_name: str,
        index_name: str,
    ) -> str:
        digest = hashlib.sha1(
            f"{db_name}:{coll_name}:{index_name}:search".encode(),
        ).hexdigest()[:16]
        return f"fts_{digest}"

    def _mark_search_backend_changed(
        self,
        db_name: str,
        coll_name: str,
    ) -> None:
        cache_key = (db_name, coll_name)
        self._search_backend_versions[cache_key] = (
            self._search_backend_versions.get(cache_key, 0) + 1
        )
        self._clear_compound_search_caches(db_name, coll_name)
        stale_keys = [
            key
            for key, state in self._vector_search_backends.items()
            if state.db_name == db_name and state.coll_name == coll_name
        ]
        for key in stale_keys:
            self._vector_search_backends.pop(key, None)
        stale_entry_keys = [
            key
            for key in self._materialized_search_entry_cache
            if key[0] == db_name and key[1] == coll_name
        ]
        for key in stale_entry_keys:
            self._materialized_search_entry_cache.pop(key, None)

    def _search_backend_version(self, db_name: str, coll_name: str) -> int:
        return self._search_backend_versions.get((db_name, coll_name), 0)

    def _clear_search_backend_state_for_database(self, db_name: str) -> None:
        self._clear_compound_search_caches(db_name)
        stale_version_keys = [
            key for key in self._search_backend_versions if key[0] == db_name
        ]
        for key in stale_version_keys:
            self._search_backend_versions.pop(key, None)
        stale_backend_keys = [
            key
            for key, state in self._vector_search_backends.items()
            if state.db_name == db_name
        ]
        for key in stale_backend_keys:
            self._vector_search_backends.pop(key, None)
        stale_entry_keys = [
            key for key in self._materialized_search_entry_cache if key[0] == db_name
        ]
        for key in stale_entry_keys:
            self._materialized_search_entry_cache.pop(key, None)

    def _sqlite_table_exists(
        self,
        conn: sqlite3.Connection,
        table_name: str,
    ) -> bool:
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
                row[0] for row in conn.execute("PRAGMA compile_options").fetchall()
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
        collation: CollationDocument | None = None,
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
                    if index.get("hidden"):
                        raise OperationFailure(
                            "hint does not correspond to a usable index for this query",
                        )
                    if plan is not None and not query_can_use_index(
                        index,
                        plan,
                        dialect=dialect,
                        collation=collation,
                    ):
                        raise OperationFailure(
                            "hint does not correspond to a usable index for this query",
                        )
                    return deepcopy(index)
            elif index["key"] == normalized_hint:
                if index.get("hidden"):
                    raise OperationFailure(
                        "hint does not correspond to a usable index for this query",
                    )
                if plan is not None and not query_can_use_index(
                    index,
                    plan,
                    dialect=dialect,
                    collation=collation,
                ):
                    raise OperationFailure(
                        "hint does not correspond to a usable index for this query",
                    )
                return deepcopy(index)

        raise OperationFailure("hint does not correspond to an existing index")

    def _serialize_document(self, document: Document) -> str:
        return json_dumps_compact(
            self._codec.encode(document),
            sort_keys=False,
        )

    def _deserialize_document(
        self,
        payload: str,
        *,
        preserve_bson_wrappers: bool = True,
    ) -> Document:
        parsed = json_loads(payload)
        if DocumentCodec._MARKER not in payload:
            return parsed
        return self._decode_codec_payload(
            parsed,
            preserve_bson_wrappers=preserve_bson_wrappers,
        )

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
            try:
                cursor.close()
            except sqlite3.ProgrammingError:
                # The connection may already be closed if the generator is
                # finalized after engine shutdown.
                pass

    def _load_documents_by_storage_keys(
        self,
        db_name: str,
        coll_name: str,
        storage_keys: list[str],
    ) -> dict[str, Document]:
        if not storage_keys:
            return {}
        placeholders = ", ".join("?" for _ in storage_keys)
        conn = self._require_connection()
        cursor = conn.execute(
            f"""
            SELECT storage_key, document
            FROM documents
            WHERE db_name = ? AND coll_name = ? AND storage_key IN ({placeholders})
            """,
            (db_name, coll_name, *storage_keys),
        )
        try:
            return {
                storage_key: self._deserialize_document(document)
                for storage_key, document in cursor
            }
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
        return _sqlite_build_select_sql(
            db_name=db_name,
            coll_name=coll_name,
            plan=plan,
            select_clause=select_clause,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            build_scalar_sort_select_sql_fn=self._build_scalar_sort_select_sql,
            resolve_hint_index=lambda current_db_name, current_coll_name, current_hint: (
                self._resolve_hint_index(
                    current_db_name,
                    current_coll_name,
                    current_hint,
                    plan=plan,
                    dialect=dialect,
                )
            ),
            translate_query_plan_with_multikey=self._translate_query_plan_with_multikey,
            build_select_statement=self._sql_translator.build_select_statement,
            quote_identifier=self._quote_identifier,
        )

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
        total_count, numeric_count, string_count, bool_count = (
            int(value or 0) for value in row
        )
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

    def _resolve_select_clause_for_scalar_sort(
        self,
        select_clause: str,
    ) -> str:
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
        return _sqlite_build_scalar_sort_select_sql(
            db_name=db_name,
            coll_name=coll_name,
            plan=plan,
            select_clause=select_clause,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            field_traverses_array_in_collection=self._field_traverses_array_in_collection,
            field_contains_tagged_bytes_in_collection=self._field_contains_tagged_bytes_in_collection,
            field_contains_tagged_undefined_in_collection=self._field_contains_tagged_undefined_in_collection,
            field_has_uniform_scalar_sort_type_in_collection=self._field_has_uniform_scalar_sort_type_in_collection,
            translate_query_plan_with_multikey=self._translate_query_plan_with_multikey,
            find_scalar_sort_index=self._find_scalar_sort_index,
            lookup_collection_id=lambda current_db_name, current_coll_name: (
                self._lookup_collection_id(
                    self._require_connection(),
                    current_db_name,
                    current_coll_name,
                )
            ),
            quote_identifier=self._quote_identifier,
            value_expression_sql=value_expression_sql,
        )

    @staticmethod
    def _normalize_multikey_number(value: float) -> str:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise NotImplementedError(
                "NaN and infinity are not supported in SQLite multikey indexes",
            )
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
    def _extract_multikey_entries(
        cls,
        document: Document,
        field: str,
    ) -> set[tuple[str, str]]:
        found, value = get_document_value(document, field)
        if not found or not isinstance(value, list):
            return set()
        entries: set[tuple[str, str]] = set()
        for item in value:
            signature = cls._multikey_value_signature(item)
            if signature is not None:
                entries.add(signature)
        return entries

    def _unique_index_keys(
        self,
        document: Document,
        fields: list[str],
    ) -> set[tuple[object, ...]]:
        return {
            tuple(self._typed_engine_key(value) for value in key)
            for key in self._unique_index_values(document, fields)
        }

    def _unique_index_values(
        self,
        document: Document,
        fields: list[str],
    ) -> list[tuple[Any, ...]]:
        if len(fields) == 1:
            values = QueryEngine.extract_values(document, fields[0])
            return [(value,) for value in values or [None]]

        array_source_paths = self._compound_multikey_array_paths(
            document,
            fields,
        )
        if len(array_source_paths) == 1:
            array_path = next(iter(array_source_paths))
            array_values = [
                value
                for value in self._values_at_index_path(document, array_path)
                if isinstance(value, list)
            ]
            if array_values:
                keys: list[tuple[Any, ...]] = []
                for array_value in array_values:
                    for element in array_value:
                        candidates_per_field: list[list[Any]] = []
                        for field in fields:
                            if field == array_path:
                                candidates_per_field.append([element])
                            elif field.startswith(f"{array_path}."):
                                suffix = field.removeprefix(f"{array_path}.")
                                candidates_per_field.append(
                                    QueryEngine.extract_values(element, suffix)
                                    or [None],
                                )
                            else:
                                candidates_per_field.append(
                                    QueryEngine.extract_values(document, field)
                                    or [None],
                                )
                        element_keys: list[tuple[Any, ...]] = [()]
                        for candidates in candidates_per_field:
                            element_keys = [
                                (*key, candidate)
                                for key in element_keys
                                for candidate in candidates
                            ]
                        keys.extend(element_keys)
                return keys

        keys: list[tuple[Any, ...]] = [()]
        for field in fields:
            keys = [
                (*key, value)
                for key in keys
                for value in (QueryEngine.extract_values(document, field) or [None])
            ]
        return keys

    def _unique_index_conflict(
        self,
        candidate: Document,
        existing: Document,
        index: EngineIndexRecord,
    ) -> tuple[object, ...] | None:
        fields = index["fields"]
        if index.get("collation") is None:
            duplicate_keys = self._unique_index_keys(
                candidate,
                fields,
            ).intersection(self._unique_index_keys(existing, fields))
            return next(iter(duplicate_keys)) if duplicate_keys else None

        collation = normalize_collation(index["collation"])
        for candidate_key in self._unique_index_values(candidate, fields):
            for existing_key in self._unique_index_values(existing, fields):
                if all(
                    values_equal_with_collation(
                        candidate_value,
                        existing_value,
                        dialect=MONGODB_DIALECT_70,
                        collation=collation,
                    )
                    for candidate_value, existing_value in zip(
                        candidate_key,
                        existing_key,
                        strict=True,
                    )
                ):
                    return candidate_key
        return None

    def _unique_index_uses_multikey_values(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        index: EngineIndexRecord,
    ) -> bool:
        for field in index["fields"]:
            if self._array_paths_for_index_field(document, field):
                return True
            if self._field_traverses_array_in_collection(
                db_name,
                coll_name,
                field,
            ):
                return True
            if self._field_is_top_level_array_in_collection(
                db_name,
                coll_name,
                field,
            ):
                return True
        return False

    @staticmethod
    def _array_paths_for_index_field(
        document: Document,
        field: str,
    ) -> set[str]:
        array_paths: set[str] = set()
        parts = field.split(".")

        def collect(value: Any, position: int, path: str) -> None:
            if isinstance(value, DBRef):
                value = value.as_document()
            if isinstance(value, list):
                if position == len(parts):
                    array_paths.add(path)
                    return
                part = parts[position]
                if part.isdigit():
                    item_index = int(part)
                    if item_index < len(value):
                        next_path = f"{path}.{part}" if path else part
                        collect(value[item_index], position + 1, next_path)
                    return
                array_paths.add(path)
                for item in value:
                    collect(item, position, path)
                return
            if position == len(parts) or not isinstance(value, dict):
                return
            part = parts[position]
            if part not in value:
                return
            next_path = f"{path}.{part}" if path else part
            collect(value[part], position + 1, next_path)

        collect(document, 0, "")
        return array_paths

    def _compound_multikey_array_paths(
        self,
        document: Document,
        fields: list[str],
    ) -> set[str]:
        source_paths: set[str] = set()
        for field in fields:
            array_paths = self._array_paths_for_index_field(document, field)
            if array_paths:
                source_paths.add(
                    min(array_paths, key=lambda path: len(path.split("."))),
                )
        return source_paths

    @staticmethod
    def _values_at_index_path(document: Document, path: str) -> list[Any]:
        values: list[Any] = [document]
        for part in path.split("."):
            next_values: list[Any] = []
            for value in values:
                if isinstance(value, DBRef):
                    value = value.as_document()
                if isinstance(value, list):
                    if part.isdigit():
                        item_index = int(part)
                        if item_index < len(value):
                            next_values.append(value[item_index])
                    else:
                        for item in value:
                            if isinstance(item, DBRef):
                                item = item.as_document()
                            if isinstance(item, dict) and part in item:
                                next_values.append(item[part])
                elif isinstance(value, dict) and part in value:
                    next_values.append(value[part])
            values = next_values
        return values

    def _validate_compound_multikey_shape(
        self,
        document: Document,
        index: EngineIndexRecord,
    ) -> None:
        fields = index["fields"]
        if len(fields) < 2:
            return

        if len(self._compound_multikey_array_paths(document, fields)) > 1:
            raise OperationFailure(
                "compound indexes do not support more than one array field",
            )

    @staticmethod
    def _supports_multikey_index(fields: list[str], unique: bool) -> bool:
        return not unique and len(fields) == 1 and not path_array_prefixes(fields[0])

    @staticmethod
    def _supports_scalar_index(index: EngineIndexRecord) -> bool:
        return len(index["fields"]) == 1 and is_ordered_index_spec(
            index["key"],
        )

    def _find_multikey_index(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> EngineIndexRecord | None:
        for index in self._load_indexes(db_name, coll_name):
            if not index.get("multikey"):
                continue
            if index["fields"] == [field]:
                return index
        return None

    def _find_scalar_index(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> EngineIndexRecord | None:
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
        return (
            element_type,
            SQLiteEngine._multikey_type_score(element_type),
            element_key,
        )

    def _find_scalar_fast_path_index(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> EngineIndexRecord | None:
        return _sqlite_runtime_find_scalar_fast_path_index(
            self,
            db_name,
            coll_name,
            field,
        )

    def _can_use_scalar_range_fast_path(
        self,
        db_name: str,
        coll_name: str,
        field: str,
        value: object,
    ) -> tuple[EngineIndexRecord, int, str] | None:
        return _sqlite_runtime_can_use_scalar_range_fast_path(
            self,
            db_name,
            coll_name,
            field,
            value,
        )

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
        physical_name = self._quote_identifier(
            str(index["multikey_physical_name"]),
        )
        clauses: list[str] = []
        params: list[object] = []
        for element_type, element_key in signatures:
            clauses.append(
                "EXISTS ("
                f"SELECT 1 FROM multikey_entries INDEXED BY {physical_name} "
                "WHERE collection_id = ? AND index_name = ? "
                "AND storage_key = documents.storage_key "
                "AND element_type = ? AND element_key = ?)",
            )
            params.extend(
                [collection_id, index["name"], element_type, element_key],
            )
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
        physical_name = self._quote_identifier(
            str(index["multikey_physical_name"]),
        )
        type_score = self._multikey_type_score(element_type)
        if operator in (">", ">="):
            comparison_sql = (
                f"(type_score > ? OR (type_score = ? AND element_key {operator} ?))"
            )
            params_tail = [type_score, type_score, element_key]
        else:
            comparison_sql = (
                f"(type_score < ? OR (type_score = ? AND element_key {operator} ?))"
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
        return f"(({scalar_sql}) OR {array_sql})", [
            *scalar_params,
            *array_params,
        ]

    def _field_has_comparison_type_mismatch_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
        expected_type: str,
    ) -> bool:
        feature_key = (
            db_name,
            coll_name,
            f"comparison_mismatch:{field}:{expected_type}",
        )
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

    def _field_has_scalar_or_array_element_type_mismatch_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
        expected_type: str,
    ) -> bool:
        feature_key = (
            db_name,
            coll_name,
            f"scalar_or_array_comparison_mismatch:{field}:{expected_type}",
        )
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        if expected_type == "number":
            scalar_allowed = "'integer', 'real', 'array'"
            element_predicate = "json_each.type NOT IN ('integer', 'real')"
        elif expected_type == "string":
            scalar_allowed = "'text', 'array'"
            element_predicate = "json_each.type != 'text'"
        elif expected_type == "bool":
            scalar_allowed = "'true', 'false', 'array'"
            element_predicate = "json_each.type NOT IN ('true', 'false')"
        else:
            self._collection_features_cache[feature_key] = True
            return True
        conn = self._require_connection()
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        row = conn.execute(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND (
                    (json_type(document, {path_literal}) IS NOT NULL
                     AND json_type(document, {path_literal}) NOT IN ({scalar_allowed}))
                 OR (json_type(document, {path_literal}) = 'array'
                     AND EXISTS (
                         SELECT 1
                         FROM json_each(document, {path_literal})
                         WHERE {element_predicate}
                     ))
              )
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
        plan: (
            GreaterThanCondition
            | GreaterThanOrEqualCondition
            | LessThanCondition
            | LessThanOrEqualCondition
        ),
        index: EngineIndexRecord,
        *,
        operator: str,
    ) -> tuple[str, list[object]]:
        signature = self._multikey_value_signature(plan.value)
        if signature is None or signature[0] != "number":
            return translate_query_plan(plan)
        scalar_sql, scalar_params = _translate_same_type_comparison(
            operator,
            plan.field,
            plan.value,
        )
        array_sql, array_params = self._translate_multikey_ordered_exists_clause(
            collection_id,
            db_name,
            coll_name,
            index,
            element_type=signature[0],
            element_key=signature[1],
            operator=operator,
        )
        return f"(({scalar_sql}) OR {array_sql})", [
            *scalar_params,
            *array_params,
        ]

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
                if not self._field_is_top_level_array_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                ):
                    return _translate_equals_scalar_only(
                        plan.field,
                        plan.value,
                        null_matches_undefined=plan.null_matches_undefined,
                    )
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(
                self._require_connection(),
                db_name,
                coll_name,
            )
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_equals_with_multikey(
                collection_id,
                db_name,
                coll_name,
                plan,
                index,
            )
        if isinstance(plan, InCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(
                self._require_connection(),
                db_name,
                coll_name,
            )
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_in_with_multikey(
                collection_id,
                db_name,
                coll_name,
                plan,
                index,
            )
        if isinstance(plan, AllCondition):
            try:
                return _translate_all_condition(plan.field, plan.values)
            except NotImplementedError:
                return translate_query_plan(plan)
        if isinstance(plan, ElemMatchCondition):
            try:
                return _translate_elem_match_condition(
                    plan.field,
                    plan.compiled_plan,
                    wrap_value=plan.wrap_value,
                )
            except NotImplementedError:
                return translate_query_plan(plan)
        if isinstance(plan, GreaterThanCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if (
                    "." not in plan.field
                    and self._field_is_top_level_array_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                    )
                    and not self._field_has_scalar_or_array_element_type_mismatch_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                        expected_type,
                    )
                ):
                    return _translate_scalar_or_array_same_type_comparison(
                        ">",
                        plan.field,
                        plan.value,
                    )
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison(
                        ">",
                        plan.field,
                        plan.value,
                    )
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(
                self._require_connection(),
                db_name,
                coll_name,
            )
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(
                collection_id,
                db_name,
                coll_name,
                plan,
                index,
                operator=">",
            )
        if isinstance(plan, GreaterThanOrEqualCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if (
                    "." not in plan.field
                    and self._field_is_top_level_array_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                    )
                    and not self._field_has_scalar_or_array_element_type_mismatch_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                        expected_type,
                    )
                ):
                    return _translate_scalar_or_array_same_type_comparison(
                        ">=",
                        plan.field,
                        plan.value,
                    )
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison(
                        ">=",
                        plan.field,
                        plan.value,
                    )
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(
                self._require_connection(),
                db_name,
                coll_name,
            )
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(
                collection_id,
                db_name,
                coll_name,
                plan,
                index,
                operator=">=",
            )
        if isinstance(plan, LessThanCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if (
                    "." not in plan.field
                    and self._field_is_top_level_array_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                    )
                    and not self._field_has_scalar_or_array_element_type_mismatch_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                        expected_type,
                    )
                ):
                    return _translate_scalar_or_array_same_type_comparison(
                        "<",
                        plan.field,
                        plan.value,
                    )
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison(
                        "<",
                        plan.field,
                        plan.value,
                    )
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(
                self._require_connection(),
                db_name,
                coll_name,
            )
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(
                collection_id,
                db_name,
                coll_name,
                plan,
                index,
                operator="<",
            )
        if isinstance(plan, LessThanOrEqualCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                expected_type, _ = _normalize_comparable_value(plan.value)
                if (
                    "." not in plan.field
                    and self._field_is_top_level_array_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                    )
                    and not self._field_has_scalar_or_array_element_type_mismatch_in_collection(
                        db_name,
                        coll_name,
                        plan.field,
                        expected_type,
                    )
                ):
                    return _translate_scalar_or_array_same_type_comparison(
                        "<=",
                        plan.field,
                        plan.value,
                    )
                if not self._field_has_comparison_type_mismatch_in_collection(
                    db_name,
                    coll_name,
                    plan.field,
                    expected_type,
                ):
                    return _translate_same_type_comparison(
                        "<=",
                        plan.field,
                        plan.value,
                    )
                return translate_query_plan(plan)
            collection_id = self._lookup_collection_id(
                self._require_connection(),
                db_name,
                coll_name,
            )
            if collection_id is None:
                return translate_query_plan(plan)
            return self._translate_range_with_multikey(
                collection_id,
                db_name,
                coll_name,
                plan,
                index,
                operator="<=",
            )
        if isinstance(plan, AndCondition):
            fragments = [
                self._translate_query_plan_with_multikey(
                    db_name,
                    coll_name,
                    clause,
                )
                for clause in plan.clauses
            ]
            sql = " AND ".join(f"({fragment_sql})" for fragment_sql, _ in fragments)
            params: list[object] = []
            for _, fragment_params in fragments:
                params.extend(fragment_params)
            return sql, params
        if isinstance(plan, OrCondition):
            fragments = [
                self._translate_query_plan_with_multikey(
                    db_name,
                    coll_name,
                    clause,
                )
                for clause in plan.clauses
            ]
            sql = " OR ".join(f"({fragment_sql})" for fragment_sql, _ in fragments)
            params: list[object] = []
            for _, fragment_params in fragments:
                params.extend(fragment_params)
            return sql, params
        if isinstance(plan, NotCondition):
            clause_sql, clause_params = self._translate_query_plan_with_multikey(
                db_name,
                coll_name,
                plan.clause,
            )
            return f"NOT COALESCE(({clause_sql}), 0)", clause_params
        return translate_query_plan(plan)

    def _sort_requires_python(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
        sort: SortSpec | None,
    ) -> bool:
        return _sqlite_sort_requires_python(
            db_name=db_name,
            coll_name=coll_name,
            plan=plan,
            sort=sort,
            plan_has_array_traversing_paths=self._plan_has_array_traversing_paths,
            translate_query_plan=translate_query_plan,
            field_traverses_array_in_collection=self._field_traverses_array_in_collection,
            field_contains_tagged_bytes_in_collection=self._field_contains_tagged_bytes_in_collection,
            field_contains_tagged_undefined_in_collection=self._field_contains_tagged_undefined_in_collection,
            fetchone=lambda sql, params: (
                self._require_connection().execute(sql, params).fetchone()
            ),
            type_expression_sql=type_expression_sql,
            json_path_for_field=json_path_for_field,
        )

    @staticmethod
    def _plan_fields(plan: QueryNode) -> set[str]:
        if isinstance(plan, MatchAll):
            return set()
        if hasattr(plan, "field"):
            field = plan.field
            if isinstance(field, str):
                return {field}
        if hasattr(plan, "clause"):
            return SQLiteEngine._plan_fields(plan.clause)
        if hasattr(plan, "clauses"):
            fields: set[str] = set()
            for clause in plan.clauses:
                fields.update(SQLiteEngine._plan_fields(clause))
            return fields
        return set()

    def _field_traverses_array_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> bool:
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

    def _plan_has_array_traversing_paths(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> bool:
        return _sqlite_plan_has_array_traversing_paths(
            db_name=db_name,
            coll_name=coll_name,
            plan=plan,
            plan_fields=self._plan_fields,
            field_traverses_array_in_collection=self._field_traverses_array_in_collection,
        )

    def _field_traverses_dbref_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> bool:
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
            tagged_type_path = (
                json_path_for_field(prefix)
                + f'."{DocumentCodec._MARKER}".{DocumentCodec._TYPE}'
            )
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

    def _plan_requires_python_for_dbref_paths(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> bool:
        return _sqlite_plan_requires_python_for_dbref_paths(
            db_name=db_name,
            coll_name=coll_name,
            plan=plan,
            plan_fields=self._plan_fields,
            field_traverses_dbref_in_collection=self._field_traverses_dbref_in_collection,
        )

    @staticmethod
    def _comparison_fields(plan: QueryNode) -> set[str]:
        if isinstance(
            plan,
            (
                GreaterThanCondition,
                GreaterThanOrEqualCondition,
                LessThanCondition,
                LessThanOrEqualCondition,
            ),
        ):
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
        tagged_type_path = (
            json_path_for_field(field)
            + f'."{DocumentCodec._MARKER}".{DocumentCodec._TYPE}'
        )
        row = conn.execute(
            """
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

    def _field_contains_tagged_bytes_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> bool:
        return self._field_contains_tagged_value_type_in_collection(
            db_name,
            coll_name,
            field,
            "bytes",
        )

    def _field_contains_tagged_undefined_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> bool:
        return self._field_contains_tagged_value_type_in_collection(
            db_name,
            coll_name,
            field,
            "undefined",
        )

    def _plan_requires_python_for_bytes(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> bool:
        return _sqlite_plan_requires_python_for_tagged_type(
            db_name=db_name,
            coll_name=coll_name,
            plan=plan,
            comparison_fields=self._comparison_fields,
            field_contains_tagged_type_in_collection=self._field_contains_tagged_bytes_in_collection,
        )

    def _plan_requires_python_for_undefined(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> bool:
        return _sqlite_plan_requires_python_for_tagged_type(
            db_name=db_name,
            coll_name=coll_name,
            plan=plan,
            comparison_fields=self._comparison_fields,
            field_contains_tagged_type_in_collection=self._field_contains_tagged_undefined_in_collection,
        )

    @staticmethod
    def _plan_contains_decimal_operand(plan: object) -> bool:
        pending = [plan]
        seen: set[int] = set()
        while pending:
            current = pending.pop()
            if id(current) in seen:
                continue
            seen.add(id(current))
            if isinstance(current, (Decimal, Decimal128)):
                return True
            if isinstance(current, dict):
                pending.extend(current.values())
                continue
            if isinstance(current, (list, tuple, set, frozenset)):
                pending.extend(current)
                continue
            for attribute in (
                "value",
                "values",
                "clause",
                "clauses",
                "lower",
                "upper",
                "divisor",
                "remainder",
            ):
                if hasattr(current, attribute):
                    pending.append(getattr(current, attribute))
        return False

    def _plan_requires_python_for_decimal_numeric(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> bool:
        if self._plan_contains_decimal_operand(plan):
            return True
        try:
            return any(
                self._field_contains_tagged_value_type_in_collection(
                    db_name,
                    coll_name,
                    field,
                    value_type,
                )
                for field in self._plan_fields(plan)
                for value_type in (
                    "decimal",
                    "decimal128",
                    "decimal128_public",
                )
            )
        except RuntimeError:
            # Pure planner tests may compile against an unconnected empty engine.
            return False

    def _field_is_top_level_array_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> bool:
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

    def _field_contains_real_numeric_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> bool:
        feature_key = (db_name, coll_name, f"contains_real_numeric:{field}")
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        conn = self._require_connection()
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        row = conn.execute(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND json_type(document, {path_literal}) = 'real'
            LIMIT 1
            """,
            (db_name, coll_name),
        ).fetchone()
        result = row is not None
        self._collection_features_cache[feature_key] = result
        return result

    def _field_contains_non_ascii_text_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
    ) -> bool:
        feature_key = (db_name, coll_name, f"contains_non_ascii_text:{field}")
        if feature_key in self._collection_features_cache:
            return self._collection_features_cache[feature_key]
        conn = self._require_connection()
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        rows = conn.execute(
            f"""
            SELECT json_extract(document, {path_literal})
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND json_type(document, {path_literal}) = 'text'
            """,
            (db_name, coll_name),
        ).fetchall()
        result = any(isinstance(row[0], str) and not row[0].isascii() for row in rows)
        self._collection_features_cache[feature_key] = result
        return result

    def _plan_requires_python_for_array_comparisons(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> bool:
        def requires_python(node: QueryNode) -> bool:
            if isinstance(
                node,
                (
                    GreaterThanCondition,
                    GreaterThanOrEqualCondition,
                    LessThanCondition,
                    LessThanOrEqualCondition,
                ),
            ):
                if not self._field_is_top_level_array_in_collection(
                    db_name,
                    coll_name,
                    node.field,
                ):
                    return False
                if "." in node.field:
                    return True
                try:
                    expected_type, _ = _normalize_comparable_value(node.value)
                except NotImplementedError:
                    return True
                return (
                    self._field_has_scalar_or_array_element_type_mismatch_in_collection(
                        db_name,
                        coll_name,
                        node.field,
                        expected_type,
                    )
                )
            if isinstance(node, NotCondition):
                return requires_python(node.clause)
            if isinstance(node, (AndCondition, OrCondition)):
                return any(requires_python(clause) for clause in node.clauses)
            return False

        return requires_python(plan)

    @staticmethod
    def _document_traverses_array_on_field(
        document: Document,
        field: str,
    ) -> bool:
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
        skip_id_check: bool = False,
        scan_payload_id: bool = False,
    ) -> None:
        conn = self._require_connection()
        collection_id = self._lookup_collection_id(conn, db_name, coll_name)
        if "_id" in document and not skip_id_check:
            document_id_storage_key = self._storage_key(document["_id"])
            if exclude_storage_key != document_id_storage_key:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM documents
                    WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                    LIMIT 1
                    """,
                    (db_name, coll_name, document_id_storage_key),
                ).fetchone()
                if row is not None:
                    raise DuplicateKeyError(
                        f"Duplicate key: _id={document['_id']}",
                    )
            if scan_payload_id:
                for storage_key, existing_document in self._load_documents(
                    db_name,
                    coll_name,
                ):
                    if (
                        exclude_storage_key is not None
                        and storage_key == exclude_storage_key
                    ):
                        continue
                    if (
                        "_id" in existing_document
                        and self._storage_key(existing_document["_id"])
                        == document_id_storage_key
                    ):
                        raise DuplicateKeyError(
                            f"Duplicate key: _id={document['_id']}",
                        )
        indexes = self._load_indexes(db_name, coll_name)
        for index in indexes:
            if document_in_virtual_index(document, index):
                self._validate_compound_multikey_shape(document, index)

        for index in indexes:
            if not index["unique"]:
                continue
            if not document_in_virtual_index(document, index):
                continue
            fields = index["fields"]
            if index.get(
                "collation",
            ) is not None or self._unique_index_uses_multikey_values(
                db_name,
                coll_name,
                document,
                index,
            ):
                for storage_key, existing_document in self._load_documents(
                    db_name,
                    coll_name,
                ):
                    if (
                        exclude_storage_key is not None
                        and storage_key == exclude_storage_key
                    ):
                        continue
                    if not document_in_virtual_index(existing_document, index):
                        continue
                    duplicate_key = self._unique_index_conflict(
                        document,
                        existing_document,
                        index,
                    )
                    if duplicate_key is not None:
                        raise DuplicateKeyError(
                            f"Duplicate key for unique index '{index['name']}': {fields}={duplicate_key!r}",
                        )
                continue
            scalar_conflict = self._unique_index_conflict_via_scalar_entries(
                conn,
                collection_id,
                index,
                document,
                exclude_storage_key=exclude_storage_key,
            )
            if scalar_conflict is True:
                raise DuplicateKeyError(
                    f"Duplicate key for unique index '{index['name']}': {fields}={tuple(self._index_value(document, field) for field in fields)!r}",
                )
            if scalar_conflict is False:
                continue
            key = tuple(self._index_value(document, field) for field in fields)
            for storage_key, existing_document in self._load_documents(
                db_name,
                coll_name,
            ):
                if (
                    exclude_storage_key is not None
                    and storage_key == exclude_storage_key
                ):
                    continue
                if not document_in_virtual_index(existing_document, index):
                    continue
                existing_key = tuple(
                    self._index_value(existing_document, field) for field in fields
                )
                if existing_key == key:
                    raise DuplicateKeyError(
                        f"Duplicate key for unique index '{index['name']}': {fields}={key!r}",
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

    def _select_first_document_for_plan(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
        *,
        hint: str | IndexKeySpec | None = None,
        context: ClientSession | None = None,
    ) -> tuple[str, Document] | None:
        conn = self._require_connection(context)
        with self._bind_connection(conn):
            return _sqlite_runtime_select_first_document_for_plan(
                self,
                db_name,
                coll_name,
                plan,
                hint=hint,
            )

    def _select_first_document_for_scalar_index(
        self,
        db_name: str,
        coll_name: str,
        *,
        field: str,
        value: Any,
        null_matches_undefined: bool = False,
    ) -> tuple[str, Document] | None:
        return _sqlite_runtime_select_first_document_for_scalar_index(
            self,
            db_name,
            coll_name,
            field=field,
            value=value,
            null_matches_undefined=null_matches_undefined,
        )

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
        return _sqlite_runtime_build_scalar_indexed_top_level_range_sql(
            self,
            db_name,
            coll_name,
            field=field,
            value=value,
            operator=operator,
            index_name=index_name,
            physical_name=physical_name,
            limit=limit,
        )

    def _select_first_document_for_scalar_range(
        self,
        db_name: str,
        coll_name: str,
        *,
        field: str,
        value: Any,
        operator: str,
    ) -> tuple[str, Document] | None:
        return _sqlite_runtime_select_first_document_for_scalar_range(
            self,
            db_name,
            coll_name,
            field=field,
            value=value,
            operator=operator,
        )

    def _compile_read_execution_plan(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        select_clause: str = "document",
        hint: str | IndexKeySpec | None = None,
    ) -> SQLiteReadExecutionPlan:
        return _sqlite_runtime_compile_read_execution_plan(
            self,
            db_name,
            coll_name,
            semantics,
            select_clause=select_clause,
            hint=hint,
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
        semantics = compile_find_semantics_from_operation(
            operation,
            dialect=dialect,
        )
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
        return _sqlite_runtime_plan_find_semantics_sync(
            self,
            db_name,
            coll_name,
            semantics,
        )

    async def plan_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> EngineReadExecutionPlan:
        self._ensure_session_can_use_engine(context)
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
        return _sqlite_runtime_build_scalar_indexed_top_level_equals_sql(
            self,
            db_name,
            coll_name,
            field=field,
            value=value,
            index_name=index_name,
            physical_name=physical_name,
            limit=limit,
            null_matches_undefined=null_matches_undefined,
        )

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
        with self._lock:
            return _sqlite_runtime_explain_query_plan_sync(
                self,
                db_name,
                coll_name,
                semantics_or_filter_spec,
                plan=plan,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                max_time_ms=max_time_ms,
                dialect=dialect,
            )

    def _load_indexes(
        self,
        db_name: str,
        coll_name: str,
    ) -> list[EngineIndexRecord]:
        cache_key = (db_name, coll_name)
        cached = self._index_cache.get(cache_key)
        cache_version = self._index_cache_version(db_name, coll_name)
        if cached is not None and cached[0] == cache_version:
            return deepcopy(cached[1])
        conn = self._require_connection()
        cursor = conn.execute(
            """
            SELECT name, physical_name, fields, keys, unique_flag, sparse_flag, hidden_flag, collation_json, partial_filter_json, expire_after_seconds, text_weights_json, default_language, language_override, min_value, max_value, bucket_size, multikey_flag, multikey_physical_name, scalar_physical_name
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
                hidden_flag = 0
                collation_json = None
                expire_after_seconds = None
                text_weights_json = None
                default_language = None
                language_override = None
                min_value = None
                max_value = None
                bucket_size = None
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
                    expire_after_seconds,
                    multikey_flag,
                    multikey_physical_name,
                ) = row
                hidden_flag = 0
                collation_json = None
                text_weights_json = None
                default_language = None
                language_override = None
                min_value = None
                max_value = None
                bucket_size = None
                scalar_physical_name = None
            elif len(row) == 11:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    hidden_flag,
                    partial_filter_json,
                    expire_after_seconds,
                    multikey_flag,
                    multikey_physical_name,
                ) = row
                collation_json = None
                text_weights_json = None
                default_language = None
                language_override = None
                min_value = None
                max_value = None
                bucket_size = None
                scalar_physical_name = None
            elif len(row) == 12:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    hidden_flag,
                    partial_filter_json,
                    expire_after_seconds,
                    multikey_flag,
                    multikey_physical_name,
                    scalar_physical_name,
                ) = row
                collation_json = None
                text_weights_json = None
                default_language = None
                language_override = None
                min_value = None
                max_value = None
                bucket_size = None
            elif len(row) == 13:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    hidden_flag,
                    collation_json,
                    partial_filter_json,
                    expire_after_seconds,
                    multikey_flag,
                    multikey_physical_name,
                    scalar_physical_name,
                ) = row
                text_weights_json = None
                default_language = None
                language_override = None
                min_value = None
                max_value = None
                bucket_size = None
            elif len(row) == 16:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    hidden_flag,
                    collation_json,
                    partial_filter_json,
                    expire_after_seconds,
                    text_weights_json,
                    default_language,
                    language_override,
                    multikey_flag,
                    multikey_physical_name,
                    scalar_physical_name,
                ) = row
                min_value = None
                max_value = None
                bucket_size = None
            elif len(row) == 19:
                (
                    name,
                    physical_name,
                    fields,
                    keys,
                    unique_flag,
                    sparse_flag,
                    hidden_flag,
                    collation_json,
                    partial_filter_json,
                    expire_after_seconds,
                    text_weights_json,
                    default_language,
                    language_override,
                    min_value,
                    max_value,
                    bucket_size,
                    multikey_flag,
                    multikey_physical_name,
                    scalar_physical_name,
                ) = row
            else:
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}",
                )
            try:
                parsed_fields = json_loads(fields)
            except json.JSONDecodeError as exc:
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                ) from exc
            if not isinstance(parsed_fields, list) or not all(
                isinstance(field, str) for field in parsed_fields
            ):
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                )
            if keys is None:
                parsed_keys = [(field, 1) for field in parsed_fields]
            else:
                try:
                    parsed_keys = normalize_index_keys(json_loads(keys))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                    ) from exc
            collation: Filter | None = None
            if collation_json is not None:
                try:
                    parsed_collation = json_loads(collation_json)
                except json.JSONDecodeError as exc:
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                    ) from exc
                if not isinstance(parsed_collation, dict):
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                    )
                collation = parsed_collation
            partial_filter_expression: Filter | None = None
            if partial_filter_json is not None:
                try:
                    partial_filter_expression = normalize_partial_filter_expression(
                        DocumentCodec.decode(
                            json_loads(partial_filter_json),
                        ),
                    )
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                    ) from exc
            text_weights: dict[str, int] | None = None
            if text_weights_json is not None:
                try:
                    parsed_weights = json_loads(text_weights_json)
                except json.JSONDecodeError as exc:
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                    ) from exc
                if (
                    not isinstance(parsed_weights, dict)
                    or not all(
                        isinstance(field, str) and field for field in parsed_weights
                    )
                    or not all(
                        isinstance(weight, int)
                        and not isinstance(weight, bool)
                        and weight > 0
                        for weight in parsed_weights.values()
                    )
                ):
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                    )
                text_weights = {
                    field: int(weight) for field, weight in parsed_weights.items()
                }
            if default_language is not None and (
                not isinstance(default_language, str) or not default_language
            ):
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                )
            if language_override is not None and (
                not isinstance(language_override, str) or not language_override
            ):
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}",
                )
            indexes.append(
                EngineIndexRecord(
                    name=name,
                    physical_name=physical_name
                    or self._physical_index_name(db_name, coll_name, name),
                    fields=parsed_fields,
                    key=parsed_keys,
                    unique=bool(unique_flag),
                    sparse=bool(sparse_flag),
                    hidden=bool(hidden_flag),
                    collation=collation,
                    partial_filter_expression=partial_filter_expression,
                    expire_after_seconds=(
                        int(expire_after_seconds)
                        if expire_after_seconds is not None
                        else None
                    ),
                    weights=text_weights,
                    default_language=default_language,
                    language_override=language_override,
                    min_value=min_value,
                    max_value=max_value,
                    bucket_size=bucket_size,
                    multikey=bool(multikey_flag),
                    multikey_physical_name=multikey_physical_name
                    or self._physical_multikey_index_name(
                        db_name,
                        coll_name,
                        name,
                    ),
                    scalar_physical_name=scalar_physical_name
                    or (
                        self._physical_scalar_index_name(
                            db_name,
                            coll_name,
                            name,
                        )
                        if len(parsed_fields) == 1
                        else None
                    ),
                ),
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
            for element_type, element_key in self._extract_multikey_entries(
                document,
                field,
            ):
                rows.append(
                    (
                        str(index["name"]),
                        element_type,
                        self._multikey_type_score(element_type),
                        element_key,
                    ),
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
            signature = self._scalar_value_signature_for_document(
                document,
                index["fields"][0],
            )
            if signature is None:
                continue
            rows.append(
                (
                    str(index["name"]),
                    signature[0],
                    self._multikey_type_score(signature[0]),
                    signature[1],
                ),
            )
        return rows

    def _load_search_index_rows(
        self,
        db_name: str,
        coll_name: str,
        *,
        name: str | None = None,
    ) -> list[tuple[SearchIndexDefinition, str | None, float | None]]:
        return _sqlite_runtime_load_search_index_rows(
            self,
            db_name,
            coll_name,
            name=name,
        )

    def _load_search_indexes(
        self,
        db_name: str,
        coll_name: str,
    ) -> list[SearchIndexDefinition]:
        return _sqlite_runtime_load_search_indexes(self, db_name, coll_name)

    def _pending_search_index_ready_at(self) -> float | None:
        return _sqlite_pending_search_index_ready_at(self)

    def _search_index_is_ready_sync(
        self,
        ready_at_epoch: float | None,
    ) -> bool:
        return _sqlite_search_index_is_ready_sync(ready_at_epoch)

    def _drop_search_backend_sync(
        self,
        conn: sqlite3.Connection,
        physical_name: str | None,
    ) -> None:
        _sqlite_drop_search_backend_sync(self, conn, physical_name)

    def _ensure_search_backend_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        physical_name: str | None,
    ) -> str | None:
        return _sqlite_ensure_search_backend_sync(
            self,
            conn,
            db_name,
            coll_name,
            definition,
            physical_name,
        )

    def _ensure_vector_search_backend_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        physical_name: str,
        path: str,
    ) -> SQLiteVectorBackendState:
        return _sqlite_ensure_vector_search_backend_sync(
            self,
            conn,
            db_name,
            coll_name,
            definition,
            physical_name,
            path,
        )

    def _delete_search_entries_for_storage_key(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        search_indexes: (
            list[tuple[SearchIndexDefinition, str | None, float | None]] | None
        ) = None,
    ) -> None:
        _sqlite_delete_search_entries_for_storage_key(
            self,
            conn,
            db_name,
            coll_name,
            storage_key,
            search_indexes=search_indexes,
        )

    def _replace_search_entries_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        search_indexes: (
            list[tuple[SearchIndexDefinition, str | None, float | None]] | None
        ) = None,
    ) -> None:
        _sqlite_replace_search_entries_for_document(
            self,
            conn,
            db_name,
            coll_name,
            storage_key,
            document,
            search_indexes=search_indexes,
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
        _sqlite_runtime_rebuild_multikey_entries_for_document(
            self,
            conn,
            db_name,
            coll_name,
            storage_key,
            document,
            indexes,
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
        _sqlite_runtime_rebuild_scalar_entries_for_document(
            self,
            conn,
            db_name,
            coll_name,
            storage_key,
            document,
            indexes,
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
        _sqlite_runtime_replace_multikey_entries_for_index_for_document(
            self,
            conn,
            db_name,
            coll_name,
            storage_key,
            document,
            index,
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
        _sqlite_runtime_replace_scalar_entries_for_index_for_document(
            self,
            conn,
            db_name,
            coll_name,
            storage_key,
            document,
            index,
        )
        self._mark_search_backend_changed(db_name, coll_name)

    def _backfill_scalar_indexes_sync(self, conn: sqlite3.Connection) -> None:
        _sqlite_runtime_backfill_scalar_indexes_sync(self, conn)

    def _abort_failed_connect_sync(self) -> None:
        with self._lock:
            connections = (
                self._pending_connection,
                self._connection,
                self._change_delivery_connection,
            )
            self._pending_connection = None
            self._connection = None
            self._change_delivery_connection = None
            self._connection_count = 0
            self._transaction_owner_session_id = None
            self._registered_change_consumers.clear()
            self._change_outbox_checkpoints.clear()
            self._invalidate_index_cache()
            self._invalidate_collection_id_cache()
            self._invalidate_collection_features_cache()
            self._ensured_search_backends.clear()
            self._vector_search_backends.clear()
            self._search_backend_versions.clear()
            self._materialized_search_entry_cache.clear()
            self._clear_compound_search_caches()
            self._fts5_available = None
        closed: set[int] = set()
        for connection in connections:
            if connection is None or id(connection) in closed:
                continue
            closed.add(id(connection))
            with suppress(sqlite3.Error):
                connection.close()

    @_cleanup_failed_connect
    def _connect_sync(self) -> None:  # noqa: PLR0912, PLR0915
        with self._lock:
            if self._connection_count == 0:
                connection = self._create_sqlite_connection()
                self._pending_connection = connection
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS collections (
                        collection_id INTEGER,
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        options_json TEXT NOT NULL DEFAULT '{}',
                        PRIMARY KEY (db_name, coll_name)
                    )
                    """,
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
                    """,
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
                        hidden_flag INTEGER NOT NULL DEFAULT 0,
                        collation_json TEXT,
                        partial_filter_json TEXT,
                        expire_after_seconds INTEGER,
                        text_weights_json TEXT,
                        default_language TEXT,
                        language_override TEXT,
                        min_value REAL,
                        max_value REAL,
                        bucket_size REAL,
                        multikey_flag INTEGER NOT NULL DEFAULT 0,
                        multikey_physical_name TEXT,
                        PRIMARY KEY (db_name, coll_name, name)
                    )
                    """,
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
                    """,
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
                    """,
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
                    """,
                )
                ensure_change_outbox_schema(connection)
                _sqlite_expire_ephemeral_consumers(connection)
                collection_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(collections)",
                    ).fetchall()
                }
                if "options_json" not in collection_columns:
                    connection.execute(
                        "ALTER TABLE collections ADD COLUMN options_json TEXT NOT NULL DEFAULT '{}'",
                    )
                if "collection_id" not in collection_columns:
                    connection.execute(
                        "ALTER TABLE collections ADD COLUMN collection_id INTEGER",
                    )
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(indexes)",
                    ).fetchall()
                }
                if "physical_name" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN physical_name TEXT",
                    )
                if "keys" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN keys TEXT",
                    )
                if "sparse_flag" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN sparse_flag INTEGER NOT NULL DEFAULT 0",
                    )
                if "partial_filter_json" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN partial_filter_json TEXT",
                    )
                if "hidden_flag" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN hidden_flag INTEGER NOT NULL DEFAULT 0",
                    )
                if "collation_json" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN collation_json TEXT",
                    )
                if "expire_after_seconds" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN expire_after_seconds INTEGER",
                    )
                if "text_weights_json" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN text_weights_json TEXT",
                    )
                if "default_language" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN default_language TEXT",
                    )
                if "language_override" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN language_override TEXT",
                    )
                if "min_value" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN min_value REAL",
                    )
                if "max_value" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN max_value REAL",
                    )
                if "bucket_size" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN bucket_size REAL",
                    )
                if "multikey_flag" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN multikey_flag INTEGER NOT NULL DEFAULT 0",
                    )
                if "multikey_physical_name" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN multikey_physical_name TEXT",
                    )
                if "scalar_physical_name" not in columns:
                    connection.execute(
                        "ALTER TABLE indexes ADD COLUMN scalar_physical_name TEXT",
                    )
                search_index_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(search_indexes)",
                    ).fetchall()
                }
                if "physical_name" not in search_index_columns:
                    connection.execute(
                        "ALTER TABLE search_indexes ADD COLUMN physical_name TEXT",
                    )
                if "ready_at_epoch" not in search_index_columns:
                    connection.execute(
                        "ALTER TABLE search_indexes ADD COLUMN ready_at_epoch REAL",
                    )
                multikey_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(multikey_entries)",
                    ).fetchall()
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
                        """,
                    )
                    select_type_score = (
                        "multikey_entries.type_score"
                        if "type_score" in multikey_columns
                        else "100"
                    )
                    join_db_name = (
                        "multikey_entries.db_name"
                        if "db_name" in multikey_columns
                        else "collections.db_name"
                    )
                    join_coll_name = (
                        "multikey_entries.coll_name"
                        if "coll_name" in multikey_columns
                        else "collections.coll_name"
                    )
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
                        """,
                    )
                    connection.execute("DROP TABLE multikey_entries")
                    connection.execute(
                        "ALTER TABLE multikey_entries_new RENAME TO multikey_entries",
                    )
                scalar_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(scalar_index_entries)",
                    ).fetchall()
                }
                if (
                    not scalar_columns
                    or "collection_id" not in scalar_columns
                    or "type_score" not in scalar_columns
                    or "db_name" in scalar_columns
                    or "coll_name" in scalar_columns
                ):
                    connection.execute(
                        "DROP TABLE IF EXISTS scalar_index_entries_new",
                    )
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
                        """,
                    )
                    if scalar_columns:
                        select_type_score = (
                            "scalar_index_entries.type_score"
                            if "type_score" in scalar_columns
                            else "100"
                        )
                        join_db_name = (
                            "scalar_index_entries.db_name"
                            if "db_name" in scalar_columns
                            else "collections.db_name"
                        )
                        join_coll_name = (
                            "scalar_index_entries.coll_name"
                            if "coll_name" in scalar_columns
                            else "collections.coll_name"
                        )
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
                            """,
                        )
                    connection.execute(
                        "DROP TABLE IF EXISTS scalar_index_entries",
                    )
                    connection.execute(
                        "ALTER TABLE scalar_index_entries_new RENAME TO scalar_index_entries",
                    )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO collections (db_name, coll_name, options_json)
                    SELECT db_name, coll_name, '{}' FROM documents
                    UNION
                    SELECT db_name, coll_name, '{}' FROM indexes
                    UNION
                    SELECT db_name, coll_name, '{}' FROM search_indexes
                    """,
                )
                connection.execute(
                    """
                    UPDATE collections
                    SET collection_id = rowid
                    WHERE collection_id IS NULL OR collection_id = 0
                    """,
                )
                connection.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_collections_collection_id
                    ON collections (collection_id)
                    """,
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scalar_index_entries_storage
                    ON scalar_index_entries (collection_id, storage_key)
                    """,
                )
                search_index_rows = connection.execute(
                    """
                    SELECT db_name, coll_name, name
                    FROM search_indexes
                    WHERE physical_name IS NULL OR physical_name = ''
                    """,
                ).fetchall()
                for db_name, coll_name, name in search_index_rows:
                    connection.execute(
                        """
                        UPDATE search_indexes
                        SET physical_name = ?
                        WHERE db_name = ? AND coll_name = ? AND name = ?
                        """,
                        (
                            self._physical_search_index_name(
                                db_name,
                                coll_name,
                                name,
                            ),
                            db_name,
                            coll_name,
                            name,
                        ),
                    )
                self._connection = connection
                with self._bind_connection(connection):
                    self._backfill_scalar_indexes_sync(connection)
                connection.commit()
                if self._path != ":memory:":
                    change_delivery_connection = self._create_sqlite_connection()
                    self._change_delivery_connection = change_delivery_connection
                    ensure_change_outbox_schema(
                        change_delivery_connection,
                    )
                    _sqlite_expire_ephemeral_consumers(
                        change_delivery_connection,
                    )
                    change_delivery_connection.commit()
                self._ensured_multikey_physical_indexes.clear()
                self._ensured_search_backends.clear()
                self._vector_search_backends.clear()
                self._search_backend_versions.clear()
                self._materialized_search_entry_cache.clear()
                self._clear_compound_search_caches()
                self._fts5_available = None
                self._invalidate_index_cache()
                self._invalidate_collection_id_cache()
                self._invalidate_collection_features_cache()
                self._pending_connection = None
            self._connection_count += 1

    def _create_sqlite_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, check_same_thread=False)
        try:
            if self._path != ":memory:":
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=NORMAL")
        except BaseException:
            with suppress(sqlite3.Error):
                connection.close()
            raise
        return connection

    @contextmanager
    def _change_delivery_storage(
        self,
        *,
        allow_transactional_registration: bool = False,
    ):
        dedicated = self._change_delivery_connection
        lock = self._change_delivery_lock if dedicated is not None else self._lock
        with lock:
            conn = dedicated or self._connection
            if conn is None:
                message = "SQLiteEngine must be connected for change delivery"
                raise RuntimeError(message)
            if (
                dedicated is None
                and conn.in_transaction
                and not allow_transactional_registration
            ):
                message = (
                    "change delivery control plane cannot share an active "
                    "SQLite data transaction"
                )
                raise InvalidOperation(message)
            yield conn

    def register_change_consumer(
        self,
        consumer_id: str,
        *,
        initial_checkpoint: int | None = None,
        durable: bool = False,
    ) -> int:
        if not durable:
            self._ensure_ephemeral_registration_heartbeat()
        with self._change_delivery_storage(
            allow_transactional_registration=True,
        ) as conn:
            was_in_transaction = conn.in_transaction
            checkpoint = _sqlite_register_consumer(
                conn,
                consumer_id,
                initial_checkpoint=initial_checkpoint,
                durable=durable,
                owner_instance=(None if durable else self._change_dispatch_owner),
                ephemeral_ttl_seconds=(
                    None if durable else _EPHEMERAL_CHANGE_CONSUMER_TTL_SECONDS
                ),
            )
            if not was_in_transaction:
                conn.commit()
        with self._lock:
            self._change_outbox_checkpoints[consumer_id] = checkpoint
            self._registered_change_consumers[consumer_id] = durable
        return checkpoint

    def _ensure_ephemeral_registration_heartbeat(self) -> None:
        if self._path == ":memory:":
            return
        with self._registration_heartbeat_guard:
            thread = self._registration_heartbeat_thread
            state = self._registration_heartbeat_state
            if thread is not None and thread.is_alive():
                if state is not None and state.error() is not None:
                    raise state.error()
                return
            if state is not None and state.error() is not None:
                raise state.error()
            state = _EphemeralRegistrationHeartbeat()
            thread = threading.Thread(
                target=self._maintain_ephemeral_registrations,
                args=(state,),
                daemon=True,
                name="mongoeco-outbox-registration",
            )
            self._registration_heartbeat_state = state
            self._registration_heartbeat_thread = thread
            thread.start()

    def _maintain_ephemeral_registrations(
        self,
        state: _EphemeralRegistrationHeartbeat,
    ) -> None:
        connection: sqlite3.Connection | None = None
        try:
            connection = self._create_sqlite_connection()
            while not state.stop.wait(
                _EPHEMERAL_CHANGE_CONSUMER_HEARTBEAT_SECONDS,
            ):
                _sqlite_renew_ephemeral_consumers(
                    connection,
                    owner_instance=self._change_dispatch_owner,
                    now_epoch=time.time(),
                    ttl_seconds=_EPHEMERAL_CHANGE_CONSUMER_TTL_SECONDS,
                )
                connection.commit()
        except BaseException as exc:
            state.fail(exc)
        finally:
            if connection is not None:
                connection.close()

    def _stop_ephemeral_registration_heartbeat(
        self,
    ) -> BaseException | None:
        with self._registration_heartbeat_guard:
            state = self._registration_heartbeat_state
            thread = self._registration_heartbeat_thread
            if state is None or thread is None:
                return None
            state.stop.set()
        thread.join(
            timeout=_EPHEMERAL_CHANGE_CONSUMER_STOP_TIMEOUT_SECONDS,
        )
        if thread.is_alive():
            error: BaseException | None = OperationFailure(
                "ephemeral registration heartbeat did not terminate",
            )
        else:
            error = state.error()
        with self._registration_heartbeat_guard:
            if self._registration_heartbeat_thread is thread:
                self._registration_heartbeat_thread = None
                self._registration_heartbeat_state = None
        return error

    def dispatch_committed_changes(
        self,
        consumer_id: str,
        consumer: Callable[[CommittedChange], None],
    ) -> None:
        dispatch_key = f"{self._change_dispatch_scope}\0{consumer_id}"
        with _SQLITE_CHANGE_DISPATCH.hold(dispatch_key):
            generation = self._acquire_change_dispatch_lease(consumer_id)
            heartbeat_state = _ChangeDispatchHeartbeat()
            heartbeat = threading.Thread(
                target=self._maintain_change_dispatch_lease,
                args=(
                    consumer_id,
                    generation,
                    heartbeat_state,
                ),
                daemon=True,
                name=f"mongoeco-outbox-{consumer_id}",
            )
            heartbeat.start()
            dispatch_error: BaseException | None = None
            try:
                self._dispatch_committed_changes_serial(
                    consumer_id,
                    consumer,
                    generation=generation,
                    heartbeat=heartbeat_state,
                )
            except BaseException as exc:
                dispatch_error = exc
            finally:
                heartbeat_state.stop.set()
                heartbeat.join(timeout=_CHANGE_DISPATCH_HEARTBEAT_SECONDS + 1)
                if heartbeat.is_alive():
                    heartbeat_state.fail(
                        OperationFailure(
                            "change dispatch heartbeat did not terminate",
                        ),
                    )
                else:
                    try:
                        self._release_change_dispatch_lease(
                            consumer_id,
                            generation,
                        )
                    except BaseException as exc:
                        if dispatch_error is None:
                            dispatch_error = exc
            if dispatch_error is not None:
                raise dispatch_error
            heartbeat_state.raise_if_failed()

    def _acquire_change_dispatch_lease(
        self,
        consumer_id: str,
    ) -> int:
        while True:
            with self._change_delivery_storage() as conn:
                generation = _sqlite_acquire_consumer_lease(
                    conn,
                    consumer_id,
                    owner=self._change_dispatch_owner,
                    now_epoch=time.time(),
                    ttl_seconds=_CHANGE_DISPATCH_LEASE_TTL_SECONDS,
                )
                conn.commit()
                if generation is not None:
                    return generation
                # Distinguish lease contention from a retired consumer.
                _sqlite_consumer_checkpoint(conn, consumer_id)
            time.sleep(_CHANGE_DISPATCH_RETRY_SECONDS)

    def _maintain_change_dispatch_lease(
        self,
        consumer_id: str,
        generation: int,
        heartbeat: _ChangeDispatchHeartbeat,
    ) -> None:
        try:
            while not heartbeat.stop.wait(
                _CHANGE_DISPATCH_HEARTBEAT_SECONDS,
            ):
                with self._change_delivery_storage() as conn:
                    renewed = _sqlite_renew_consumer_lease(
                        conn,
                        consumer_id,
                        (self._change_dispatch_owner, generation),
                        now_epoch=time.time(),
                        ttl_seconds=_CHANGE_DISPATCH_LEASE_TTL_SECONDS,
                    )
                    conn.commit()
                if not renewed:
                    message = "change consumer dispatch lease was lost"
                    heartbeat.fail(OperationFailure(message))
                    return
        except BaseException as exc:
            heartbeat.fail(exc)

    def _release_change_dispatch_lease(
        self,
        consumer_id: str,
        generation: int,
    ) -> None:
        try:
            with self._change_delivery_storage() as conn:
                _sqlite_release_consumer_lease(
                    conn,
                    consumer_id,
                    owner=self._change_dispatch_owner,
                    generation=generation,
                )
                conn.commit()
        except RuntimeError:
            return

    def _dispatch_committed_changes_serial(
        self,
        consumer_id: str,
        consumer: Callable[[CommittedChange], None],
        *,
        generation: int,
        heartbeat: _ChangeDispatchHeartbeat,
    ) -> None:
        with self._change_delivery_storage() as conn:
            checkpoint = _sqlite_consumer_checkpoint(conn, consumer_id)
            through_sequence = _sqlite_outbox_info(conn)["newestSequence"]

        while checkpoint < through_sequence:
            with self._change_delivery_storage() as conn:
                checkpoint = _sqlite_consumer_checkpoint(conn, consumer_id)
                changes = _sqlite_read_committed_changes(
                    conn,
                    after_sequence=checkpoint,
                    through_sequence=through_sequence,
                    deserialize_document=self._deserialize_document,
                )
            if not changes:
                break
            for change in changes:
                heartbeat.raise_if_failed()
                consumer(change.for_delivery())
                heartbeat.raise_if_failed()
                with self._change_delivery_storage() as conn:
                    _sqlite_checkpoint_consumer(
                        conn,
                        consumer_id,
                        change.sequence,
                        lease_owner=self._change_dispatch_owner,
                        lease_generation=generation,
                    )
                    conn.commit()
                with self._lock:
                    self._change_outbox_checkpoints[consumer_id] = change.sequence
                    checkpoint = change.sequence

        heartbeat.raise_if_failed()
        with self._change_delivery_storage() as conn:
            _sqlite_compact_change_outbox(
                conn,
                max_entries=self._change_outbox_max_entries,
            )
            conn.commit()

    def unregister_change_consumer(
        self,
        consumer_id: str,
        *,
        retire_durable: bool = False,
    ) -> None:
        try:
            with self._change_delivery_storage() as conn:
                _sqlite_unregister_consumer(
                    conn,
                    consumer_id,
                    include_durable=retire_durable,
                )
                _sqlite_compact_change_outbox(
                    conn,
                    max_entries=self._change_outbox_max_entries,
                )
                conn.commit()
        except RuntimeError:
            pass
        with self._lock:
            self._change_outbox_checkpoints.pop(consumer_id, None)
            self._registered_change_consumers.pop(consumer_id, None)
        dispatch_key = f"{self._change_dispatch_scope}\0{consumer_id}"
        _SQLITE_CHANGE_DISPATCH.retire(dispatch_key)

    def _can_use_dedicated_reader(self, context: ClientSession | None) -> bool:
        self._ensure_session_can_use_engine(context)
        return self._path != ":memory:" and not self._session_owns_transaction(
            context,
        )

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
            (
                db_name,
                coll_name,
                json_dumps_compact(options or {}, sort_keys=True),
            ),
        )
        self._lookup_collection_id(conn, db_name, coll_name, create=True)

    def _collection_exists_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
    ) -> bool:
        return self._admin_runtime.collection_exists(conn, db_name, coll_name)

    def _collection_options_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        with self._lock:
            return self._admin_runtime.collection_options(
                db_name,
                coll_name,
                context=context,
            )

    def _collection_options_or_empty_sync(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
    ) -> dict[str, object]:
        return self._admin_runtime.collection_options_or_empty(
            conn,
            db_name,
            coll_name,
        )

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
        change_delivery_connection: sqlite3.Connection | None = None
        heartbeat_error: BaseException | None = None
        with self._lock:
            if self._connection_count == 0:
                return
            self._connection_count -= 1
            if self._connection_count != 0:
                return
            heartbeat_error = self._stop_ephemeral_registration_heartbeat()
            with self._scan_condition:
                while self._active_scan_count > 0:
                    self._scan_condition.wait()
            connection = self._connection
            change_delivery_connection = self._change_delivery_connection
            control_connection = change_delivery_connection or connection
            if control_connection is not None:
                control_lock = (
                    self._change_delivery_lock
                    if change_delivery_connection is not None
                    else nullcontext()
                )
                with control_lock:
                    for consumer_id, durable in tuple(
                        self._registered_change_consumers.items(),
                    ):
                        if not durable:
                            _sqlite_unregister_consumer(
                                control_connection,
                                consumer_id,
                                include_durable=False,
                            )
                    _sqlite_compact_change_outbox(
                        control_connection,
                        max_entries=self._change_outbox_max_entries,
                    )
                    control_connection.commit()
            self._registered_change_consumers.clear()
            self._change_outbox_checkpoints.clear()
            self._connection = None
            self._change_delivery_connection = None
            self._transaction_owner_session_id = None
            self._invalidate_index_cache()
            self._invalidate_collection_id_cache()
            self._invalidate_collection_features_cache()
            self._ensured_search_backends.clear()
            self._vector_search_backends.clear()
            self._search_backend_versions.clear()
            self._materialized_search_entry_cache.clear()
            self._clear_compound_search_caches()
            self._fts5_available = None
        if connection is not None:
            connection.close()
        if change_delivery_connection is not None:
            change_delivery_connection.close()
        if heartbeat_error is not None:
            raise heartbeat_error

    def _profile_documents(self, db_name: str) -> list[Document]:
        profile_documents = getattr(
            self._admin_runtime,
            "profile_namespace_documents",
            None,
        )
        if callable(profile_documents):
            return profile_documents(db_name)
        return self._admin_runtime.profile_documents(db_name)

    def _put_document_sync(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        overwrite: bool,
        context: ClientSession | None,
        *,
        bypass_document_validation: bool = False,
        operation_context: OperationContext | None = None,
    ) -> InsertOutcome:
        effective_dialect = (
            operation_context.dialect
            if operation_context is not None
            else MONGODB_DIALECT_70
        )
        if "_id" in document:
            assert_valid_root_document_id(document["_id"])
        storage_key = self._storage_key(document.get("_id"))
        serialized_document = self._serialize_document(document)

        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                with sqlite_write_scope(
                    conn,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                ):
                    applied = _sqlite_put_document(
                        conn,
                        db_name=db_name,
                        coll_name=coll_name,
                        document=document,
                        overwrite=overwrite,
                        bypass_document_validation=bypass_document_validation,
                        dialect=effective_dialect,
                        storage_key=storage_key,
                        serialized_document=serialized_document,
                        purge_expired_documents=lambda current, current_db_name, current_coll_name: (
                            self._purge_expired_documents_sync(
                                current,
                                current_db_name,
                                current_coll_name,
                                context=context,
                                now=(
                                    self._ttl_now()
                                    if operation_context is None
                                    else operation_context.expressions.now.replace(
                                        tzinfo=datetime.UTC,
                                    )
                                ),
                            )
                        ),
                        begin_write=lambda current: None,
                        rollback_write=lambda current: None,
                        commit_write=lambda current: None,
                        collection_options_or_empty=self._collection_options_or_empty_sync,
                        load_existing_document_for_storage_key=self._load_existing_document_for_storage_key,
                        ensure_collection_row=self._ensure_collection_row,
                        validate_document_against_unique_indexes=lambda current_db_name, current_coll_name, current_document, exclude_storage_key, skip_id_check=False, scan_payload_id=False: (
                            self._validate_document_against_unique_indexes(
                                current_db_name,
                                current_coll_name,
                                current_document,
                                exclude_storage_key=exclude_storage_key,
                                skip_id_check=skip_id_check,
                                scan_payload_id=scan_payload_id,
                            )
                        ),
                        load_indexes=self._load_indexes,
                        rebuild_multikey_entries_for_document=self._rebuild_multikey_entries_for_document,
                        supports_scalar_index=self._supports_scalar_index,
                        rebuild_scalar_entries_for_document=self._rebuild_scalar_entries_for_document,
                        load_search_index_rows=lambda current_db_name, current_coll_name: (
                            self._load_search_index_rows(
                                current_db_name,
                                current_coll_name,
                            )
                        ),
                        replace_search_entries_for_document=lambda current, current_db_name, current_coll_name, current_storage_key, current_document, search_indexes: (
                            self._replace_search_entries_for_document(
                                current,
                                current_db_name,
                                current_coll_name,
                                current_storage_key,
                                current_document,
                                search_indexes=search_indexes,
                            )
                        ),
                        invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                    )
                    commit_sequence = None
                    if applied and "_id" in document:
                        commit_sequence = _sqlite_append_change(
                            conn,
                            context=operation_context,
                            operation_type="insert",
                            db_name=db_name,
                            coll_name=coll_name,
                            document_key={"_id": deepcopy(document["_id"])},
                            full_document=deepcopy(document),
                            serialize_document=self._serialize_document,
                            max_entries=self._change_outbox_max_entries,
                        )
                return InsertOutcome(
                    applied=applied,
                    document=deepcopy(document) if applied else None,
                    commit_sequence=commit_sequence,
                )

    def _merge_document_sync(  # noqa: PLR0912, PLR0913, PLR0915
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        *,
        when_matched: str,
        when_not_matched: str,
        context: ClientSession | None,
        on_commit: Callable[[MergeOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> MergeOutcome:
        if "_id" not in document:
            raise ValueError("merge_document requires an _id")
        assert_valid_root_document_id(document["_id"])
        storage_key = self._storage_key(document["_id"])
        effective_dialect = (
            operation_context.dialect
            if operation_context is not None
            else MONGODB_DIALECT_70
        )

        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._purge_expired_documents_sync(
                    conn,
                    db_name,
                    coll_name,
                    context=context,
                    now=(
                        self._ttl_now()
                        if operation_context is None
                        else operation_context.expressions.now.replace(
                            tzinfo=datetime.UTC,
                        )
                    ),
                )
                with sqlite_write_scope(
                    conn,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                ):
                    original_document = self._load_existing_document_for_storage_key(
                        conn,
                        db_name,
                        coll_name,
                        storage_key,
                    )
                    if original_document is None:
                        for (
                            candidate_key,
                            candidate_document,
                        ) in self._load_documents(
                            db_name,
                            coll_name,
                        ):
                            if document_matches_root_id_lookup(
                                DocumentCodec.to_public(candidate_document),
                                document["_id"],
                                dialect=effective_dialect,
                            ):
                                assert_document_matches_storage_key(
                                    candidate_document,
                                    candidate_key,
                                    storage_key_for_id=self._storage_key,
                                )
                                original_document = candidate_document
                                storage_key = candidate_key
                                break
                    if original_document is not None:
                        assert_document_matches_storage_key(
                            original_document,
                            storage_key,
                            storage_key_for_id=self._storage_key,
                        )
                        if when_matched in {"keepExisting", "fail"}:
                            return MergeOutcome(
                                matched=True,
                                applied=False,
                                before_document=deepcopy(original_document),
                                after_document=deepcopy(original_document),
                            )
                        if when_matched == "replace":
                            next_document = deepcopy(document)
                            operation_type = "replace"
                        else:
                            next_document = materialize_merge_update_document(
                                original_document,
                                document,
                            )
                            operation_type = "update"
                        assert_document_kept_storage_key(
                            next_document,
                            storage_key,
                            storage_key_for_id=self._storage_key,
                        )
                        if effective_dialect.values_equal(
                            next_document,
                            original_document,
                        ):
                            return MergeOutcome(
                                matched=True,
                                applied=False,
                                operation_type=operation_type,
                                before_document=deepcopy(original_document),
                                after_document=deepcopy(original_document),
                            )
                    else:
                        if when_not_matched != "insert":
                            return MergeOutcome(
                                matched=False,
                                applied=False,
                            )
                        next_document = (
                            materialize_merge_insert_document(document)
                            if when_matched == "merge"
                            else deepcopy(document)
                        )
                        operation_type = "insert"

                    collection_options = self._collection_options_or_empty_sync(
                        conn,
                        db_name,
                        coll_name,
                    )
                    enforce_collection_document_validation(
                        next_document,
                        options=collection_options,
                        original_document=original_document,
                        dialect=effective_dialect,
                    )
                    self._ensure_collection_row(
                        conn,
                        db_name,
                        coll_name,
                        options=collection_options,
                    )
                    self._validate_document_against_unique_indexes(
                        db_name,
                        coll_name,
                        next_document,
                        exclude_storage_key=(
                            storage_key if original_document is not None else None
                        ),
                        skip_id_check=False,
                        scan_payload_id=True,
                    )
                    conn.execute(
                        """
                        INSERT INTO documents (db_name, coll_name, storage_key, document)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(db_name, coll_name, storage_key)
                        DO UPDATE SET document = excluded.document
                        """,
                        (
                            db_name,
                            coll_name,
                            storage_key,
                            self._serialize_document(next_document),
                        ),
                    )
                    indexes = self._load_indexes(db_name, coll_name)
                    if any(index.get("multikey") for index in indexes):
                        self._rebuild_multikey_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            next_document,
                            indexes,
                        )
                    if any(self._supports_scalar_index(index) for index in indexes):
                        self._rebuild_scalar_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            next_document,
                            indexes,
                        )
                    search_indexes = self._load_search_index_rows(
                        db_name,
                        coll_name,
                    )
                    if search_indexes:
                        self._replace_search_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            next_document,
                            search_indexes=search_indexes,
                        )
                    self._invalidate_collection_features_cache(
                        db_name,
                        coll_name,
                    )

                    outcome = MergeOutcome(
                        matched=original_document is not None,
                        applied=True,
                        operation_type=operation_type,
                        before_document=deepcopy(original_document),
                        after_document=deepcopy(next_document),
                    )
                    commit_sequence = _sqlite_append_change(
                        conn,
                        context=operation_context,
                        operation_type=operation_type,
                        db_name=db_name,
                        coll_name=coll_name,
                        document_key={"_id": deepcopy(next_document["_id"])},
                        full_document=deepcopy(next_document),
                        serialize_document=self._serialize_document,
                        max_entries=self._change_outbox_max_entries,
                    )
                    outcome = replace(outcome, commit_sequence=commit_sequence)
                if on_commit is not None:
                    on_commit(outcome)
                return outcome

    def _snapshot_bulk_insert_validation_options_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> dict[str, object]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                return self._collection_options_or_empty_sync(
                    conn,
                    db_name,
                    coll_name,
                )

    def _insert_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        prepared_documents: list[tuple[str, str, list[tuple[str, str, int, str]]]],
        snapshot_indexes: list[EngineIndexRecord],
        *,
        context: ClientSession | None,
        bypass_document_validation: bool = False,
        snapshot_options: dict[str, object] | None = None,
        operation_context: OperationContext | None = None,
    ) -> tuple[InsertOutcome, ...]:
        effective_dialect = (
            operation_context.dialect
            if operation_context is not None
            else MONGODB_DIALECT_70
        )
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                with sqlite_write_scope(
                    conn,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                ):
                    results = _sqlite_put_documents_bulk(
                        conn,
                        db_name=db_name,
                        coll_name=coll_name,
                        documents=documents,
                        prepared_documents=prepared_documents,
                        snapshot_indexes=snapshot_indexes,
                        bypass_document_validation=bypass_document_validation,
                        dialect=effective_dialect,
                        snapshot_options=snapshot_options,
                        purge_expired_documents=lambda current, current_db_name, current_coll_name: (
                            self._purge_expired_documents_sync(
                                current,
                                current_db_name,
                                current_coll_name,
                                context=context,
                                now=(
                                    self._ttl_now()
                                    if operation_context is None
                                    else operation_context.expressions.now.replace(
                                        tzinfo=datetime.UTC,
                                    )
                                ),
                            )
                        ),
                        collection_options_or_empty=self._collection_options_or_empty_sync,
                        load_indexes=self._load_indexes,
                        load_search_index_rows=lambda current_db_name, current_coll_name: (
                            self._load_search_index_rows(
                                current_db_name,
                                current_coll_name,
                            )
                        ),
                        begin_write=lambda current: None,
                        ensure_collection_row=self._ensure_collection_row,
                        lookup_collection_id=lambda current, current_db_name, current_coll_name, create: (
                            self._lookup_collection_id(
                                current,
                                current_db_name,
                                current_coll_name,
                                create=create,
                            )
                        ),
                        validate_document_against_unique_indexes=lambda current_db_name, current_coll_name, current_document, exclude_storage_key, skip_id_check=False, scan_payload_id=False: (
                            self._validate_document_against_unique_indexes(
                                current_db_name,
                                current_coll_name,
                                current_document,
                                exclude_storage_key=exclude_storage_key,
                                skip_id_check=skip_id_check,
                                scan_payload_id=scan_payload_id,
                            )
                        ),
                        delete_multikey_entries_for_storage_key=self._delete_multikey_entries_for_storage_key,
                        delete_scalar_entries_for_storage_key=self._delete_scalar_entries_for_storage_key,
                        build_multikey_rows_for_document=self._build_multikey_rows_for_document,
                        ensure_multikey_physical_indexes=self._ensure_multikey_physical_indexes_sync,
                        build_scalar_rows_for_document=self._build_scalar_rows_for_document,
                        ensure_scalar_physical_indexes=self._ensure_scalar_physical_indexes_sync,
                        replace_search_entries_for_document=lambda current, current_db_name, current_coll_name, current_storage_key, current_document, search_indexes: (
                            self._replace_search_entries_for_document(
                                current,
                                current_db_name,
                                current_coll_name,
                                current_storage_key,
                                current_document,
                                search_indexes=search_indexes,
                            )
                        ),
                        commit_write=lambda current: None,
                        rollback_write=lambda current: None,
                        invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                    )
                    outcomes: list[InsertOutcome] = []
                    for event_index, (document, applied) in enumerate(
                        zip(documents, results, strict=False),
                    ):
                        commit_sequence = None
                        if applied and "_id" in document:
                            commit_sequence = _sqlite_append_change(
                                conn,
                                context=operation_context,
                                event_index=event_index,
                                operation_type="insert",
                                db_name=db_name,
                                coll_name=coll_name,
                                document_key={
                                    "_id": deepcopy(document["_id"]),
                                },
                                full_document=deepcopy(document),
                                serialize_document=self._serialize_document,
                                max_entries=self._change_outbox_max_entries,
                            )
                        outcomes.append(
                            InsertOutcome(
                                applied=applied,
                                document=deepcopy(document) if applied else None,
                                commit_sequence=commit_sequence,
                            ),
                        )
                return tuple(outcomes)

    def _put_documents_bulk_sync(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        prepared_documents: list[tuple[str, str, list[tuple[str, str, int, str]]]],
        snapshot_indexes: list[EngineIndexRecord],
        *,
        context: ClientSession | None,
        bypass_document_validation: bool = False,
        snapshot_options: dict[str, object] | None = None,
    ) -> list[bool]:
        return [
            outcome.applied
            for outcome in self._insert_documents_sync(
                db_name,
                coll_name,
                documents,
                prepared_documents,
                snapshot_indexes,
                context=context,
                bypass_document_validation=bypass_document_validation,
                snapshot_options=snapshot_options,
            )
        ]

    def _prepare_bulk_document_sync(
        self,
        document: Document,
    ) -> tuple[str, str]:
        if "_id" in document:
            assert_valid_root_document_id(document["_id"])
        return self._storage_key(
            document.get("_id"),
        ), self._serialize_document(document)

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
                    self._collection_options_or_empty_sync(
                        conn,
                        db_name,
                        coll_name,
                    ),
                    self._load_indexes(db_name, coll_name),
                )

    def _prepare_bulk_document_with_indexes_sync(
        self,
        document: Document,
        indexes: list[EngineIndexRecord],
    ) -> tuple[str, str, list[tuple[str, str, int, str]]]:
        storage_key, serialized_document = self._prepare_bulk_document_sync(
            document,
        )
        return (
            storage_key,
            serialized_document,
            self._build_multikey_rows_for_document(
                storage_key,
                document,
                indexes,
            ),
        )

    def _get_document_sync(
        self,
        db_name: str,
        coll_name: str,
        doc_id: DocumentId,
        projection: Projection | None,
        *,
        operation_context: OperationContext | None = None,
    ) -> Document | None:
        operation_context = operation_context or OperationContext.create(
            dialect=MONGODB_DIALECT_70,
        )
        context = operation_context.session
        self._ensure_session_can_use_engine(context)
        effective_dialect = operation_context.dialect
        if self._is_profile_namespace(coll_name):
            profile_document = getattr(
                self._admin_runtime,
                "profile_namespace_document",
                None,
            )
            if callable(profile_document):
                return profile_document(
                    db_name,
                    doc_id,
                    projection=projection,
                    dialect=effective_dialect,
                )
            document = self._admin_runtime.profile_document(db_name, doc_id)
            if document is None:
                return None
            return apply_projection(
                document,
                projection,
                dialect=effective_dialect,
            )
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._purge_expired_documents_sync(
                    conn,
                    db_name,
                    coll_name,
                    context=context,
                    now=operation_context.expressions.now.replace(
                        tzinfo=datetime.UTC,
                    ),
                )
                return _sqlite_get_document(
                    conn,
                    db_name=db_name,
                    coll_name=coll_name,
                    doc_id=doc_id,
                    projection=projection,
                    dialect=effective_dialect,
                    storage_key=self._storage_key(doc_id),
                    deserialize_document=lambda payload: DocumentCodec.to_public(
                        self._deserialize_document(payload),
                    ),
                )

    def _delete_document_sync(
        self,
        db_name: str,
        coll_name: str,
        doc_id: DocumentId,
        context: ClientSession | None,
    ) -> bool:
        self._ensure_session_can_use_engine(context)
        if self._is_profile_namespace(coll_name):
            return False
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                document = _sqlite_get_document(
                    conn,
                    db_name=db_name,
                    coll_name=coll_name,
                    doc_id=doc_id,
                    projection=None,
                    dialect=MONGODB_DIALECT_70,
                    storage_key=self._storage_key(doc_id),
                    deserialize_document=lambda payload: DocumentCodec.to_public(
                        self._deserialize_document(payload),
                    ),
                )
                if document is None:
                    return False
                if "_id" in document:
                    assert_valid_root_document_id(document["_id"])
                return _sqlite_delete_document(
                    conn,
                    db_name=db_name,
                    coll_name=coll_name,
                    storage_key=self._storage_key(doc_id),
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                    delete_multikey_entries_for_storage_key=self._delete_multikey_entries_for_storage_key,
                    delete_scalar_entries_for_storage_key=self._delete_scalar_entries_for_storage_key,
                    delete_search_entries_for_storage_key=self._delete_search_entries_for_storage_key,
                    invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                )

    def _iter_documents_for_classic_text_query_sync(
        self,
        db_name: str,
        coll_name: str,
        documents: Any,
        *,
        semantics: EngineFindSemantics,
        context: ClientSession | None = None,
    ):
        self._ensure_session_can_use_engine(context)
        text_query = semantics.text_query
        if text_query is None:
            return documents
        indexes = self._load_indexes(db_name, coll_name)
        index_name, fields = resolve_classic_text_index_for_hint(
            indexes,
            semantics.hint,
        )
        matched_index = next(
            (index for index in indexes if index.name == index_name),
            None,
        )
        weights = matched_index.weights if matched_index is not None else None

        def _iter():
            for document in documents:
                score = classic_text_score(
                    document,
                    field=fields,
                    query=text_query,
                    weights=weights,
                )
                if score is None:
                    continue
                yield attach_text_score(document, score)

        return _iter()

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
        self._ensure_session_can_use_engine(context)
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
            documents_iter = self._iter_documents_for_classic_text_query_sync(
                db_name,
                coll_name,
                iter(self._profile_documents(db_name)),
                semantics=python_semantics,
                context=context,
            )
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
            documents = finalize_documents(
                documents,
                python_semantics,
                apply_sort_phase=False,
            )
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
                        now=semantics.variables.now.replace(
                            tzinfo=datetime.UTC,
                        ),
                    )
        if (
            semantics.text_query is None
            and semantics.sort is None
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
                    context=context,
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
            with (
                self._bind_connection(active_connection)
                if active_connection is not None
                else nullcontext()
            ):
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
                    self._runtime_metrics.record_planner(
                        execution_plan.mode.value,
                        fallback_reason=execution_plan.fallback_reason,
                    )
                    sql, sql_params = _sqlite_require_sql_execution_plan(
                        execution_plan,
                    )
                    if active_connection is None:
                        with self._lock:
                            active_connection = self._require_connection(
                                context,
                            )
                    cursor = active_connection.execute(sql, tuple(sql_params))
                    try:
                        if (
                            execution_plan.apply_python_sort
                            or execution_plan.apply_python_residual
                        ):
                            documents = finalize_documents(
                                (
                                    self._deserialize_document(payload)
                                    for (payload,) in cursor
                                ),
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
                    documents_iter = self._iter_documents_for_classic_text_query_sync(
                        db_name,
                        coll_name,
                        (
                            document
                            for _, document in self._load_documents(
                                db_name,
                                coll_name,
                            )
                        ),
                        semantics=python_semantics,
                        context=context,
                    )
                    try:
                        if python_semantics.sort is None:
                            for document in stream_finalize_documents(
                                iter_filtered_documents(
                                    documents_iter,
                                    python_semantics,
                                ),
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
        ).result

    def _delete_matching_document_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
        collation: CollationDocument | None = None,
        variables: Mapping[str, object] | None = None,
        selector_filter: Filter | None = None,
        sort: SortSpec | None = None,
        hint: str | IndexKeySpec | None = None,
        on_commit: Callable[[DeleteOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> DeleteOutcome:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                with sqlite_write_scope(
                    conn,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                ):
                    effective_dialect = (
                        operation_context.dialect
                        if operation_context is not None
                        else (dialect or MONGODB_DIALECT_70)
                    )
                    effective_collation = (
                        operation_context.collation
                        if operation_context is not None
                        else collation
                    )
                    effective_variables = (
                        operation_context.expressions
                        if operation_context is not None
                        else variables
                    )
                    query_plan = ensure_query_plan(
                        filter_spec,
                        plan,
                        dialect=effective_dialect,
                    )
                    self._resolve_hint_index(
                        db_name,
                        coll_name,
                        hint,
                        plan=query_plan,
                        dialect=effective_dialect,
                        collation=normalize_collation(effective_collation),
                    )
                    result = _sqlite_delete_matching_document(
                        db_name=db_name,
                        coll_name=coll_name,
                        filter_spec=filter_spec,
                        selector_filter=selector_filter,
                        plan=plan,
                        dialect=effective_dialect,
                        collation=effective_collation,
                        variables=effective_variables,
                        compile_find_semantics=compile_find_semantics,
                        ensure_query_plan=lambda current_filter, current_plan: (
                            ensure_query_plan(
                                current_filter,
                                current_plan,
                                dialect=effective_dialect,
                            )
                        ),
                        require_connection=lambda: conn,
                        purge_expired_documents=lambda current, current_db_name, current_coll_name: (
                            self._purge_expired_documents_sync(
                                current,
                                current_db_name,
                                current_coll_name,
                                context=context,
                                now=ensure_expression_context(
                                    effective_variables,
                                ).now.replace(tzinfo=datetime.UTC),
                            )
                        ),
                        select_first_document_for_plan=self._select_first_document_for_plan,
                        storage_key_for_id=self._storage_key,
                        begin_write=lambda current: None,
                        commit_write=lambda current: None,
                        rollback_write=lambda current: None,
                        delete_multikey_entries_for_storage_key=self._delete_multikey_entries_for_storage_key,
                        delete_scalar_entries_for_storage_key=self._delete_scalar_entries_for_storage_key,
                        delete_search_entries_for_storage_key=self._delete_search_entries_for_storage_key,
                        load_documents=self._load_documents,
                        match_plan=lambda document, query_plan, current_dialect, current_collation, variables: (
                            QueryEngine.match_plan(
                                document,
                                query_plan,
                                dialect=current_dialect,
                                collation=current_collation,
                                variables=variables,
                            )
                        ),
                        invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                        sort=sort,
                    )
                    if (
                        result.result.deleted_count > 0
                        and result.deleted_document is not None
                    ):
                        change_context = operation_context
                        if (
                            change_context is not None
                            and "_id" not in result.deleted_document
                        ):
                            change_context = change_context.for_unpublishable_change()
                        commit_sequence = _sqlite_append_change(
                            conn,
                            context=change_context,
                            operation_type="delete",
                            db_name=db_name,
                            coll_name=coll_name,
                            document_key=(
                                {
                                    "_id": deepcopy(
                                        result.deleted_document["_id"],
                                    ),
                                }
                                if "_id" in result.deleted_document
                                else {}
                            ),
                            full_document=None,
                            serialize_document=self._serialize_document,
                            max_entries=self._change_outbox_max_entries,
                        )
                        result = replace(
                            result,
                            commit_sequence=commit_sequence,
                        )
                if result.result.deleted_count and on_commit is not None:
                    on_commit(result)
                return result

    def _count_matching_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        semantics_or_filter_spec: EngineFindSemantics | Filter,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
    ) -> int:
        self._ensure_session_can_use_engine(context)
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
                    self._purge_expired_documents_sync(
                        conn,
                        db_name,
                        coll_name,
                        context=context,
                        now=self._ttl_now(),
                    )
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
                        for _, document in self._load_documents(
                            db_name,
                            coll_name,
                        )
                        if QueryEngine.match_plan(
                            document,
                            semantics.query_plan,
                            dialect=effective_dialect,
                            collation=semantics.collation,
                            variables=semantics.variables,
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
        hidden: bool,
        collation: Filter | None,
        partial_filter_expression: Filter | None,
        expire_after_seconds: int | None,
        weights: dict[str, int] | None,
        default_language: str | None,
        language_override: str | None,
        min_value: float | None,
        max_value: float | None,
        bucket_size: float | None,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> str:
        deadline = operation_deadline(max_time_ms)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                return _sqlite_create_index(
                    conn,
                    db_name=db_name,
                    coll_name=coll_name,
                    keys=keys,
                    unique=unique,
                    name=name,
                    sparse=sparse,
                    hidden=hidden,
                    collation=collation,
                    partial_filter_expression=partial_filter_expression,
                    expire_after_seconds=expire_after_seconds,
                    weights=weights,
                    default_language=default_language,
                    language_override=language_override,
                    min_value=min_value,
                    max_value=max_value,
                    bucket_size=bucket_size,
                    deadline=deadline,
                    enforce_deadline_fn=enforce_deadline,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                    purge_expired_documents=lambda current, current_db_name, current_coll_name: (
                        self._purge_expired_documents_sync(
                            current,
                            current_db_name,
                            current_coll_name,
                            context=context,
                            now=self._ttl_now(),
                        )
                    ),
                    mark_index_metadata_changed=self._mark_index_metadata_changed,
                    invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                    load_indexes=self._load_indexes,
                    supports_multikey_index=self._supports_multikey_index,
                    physical_index_name=self._physical_index_name,
                    physical_multikey_index_name=self._physical_multikey_index_name,
                    physical_scalar_index_name=self._physical_scalar_index_name,
                    is_builtin_id_index=self._is_builtin_id_index,
                    validate_ttl_index_candidates=lambda ttl_indexes: (
                        self._assert_expired_documents_have_stable_identity_sync(
                            db_name,
                            coll_name,
                            ttl_indexes,
                        )
                    ),
                    replace_multikey_entries_for_document=self._replace_multikey_entries_for_index_for_document,
                    replace_scalar_entries_for_document=self._replace_scalar_entries_for_index_for_document,
                    load_documents=self._load_documents,
                    validate_compound_multikey_document=self._validate_compound_multikey_shape,
                    unique_index_conflict=self._unique_index_conflict,
                    quote_identifier=self._quote_identifier,
                )

    def _list_indexes_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> list[IndexDocument]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = deepcopy(self._load_indexes(db_name, coll_name))
        return _sqlite_list_index_documents(indexes)

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
        return _sqlite_build_index_information(indexes)

    def _drop_index_sync(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        context: ClientSession | None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                _sqlite_drop_index(
                    conn,
                    db_name=db_name,
                    coll_name=coll_name,
                    index_or_name=index_or_name,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                    load_indexes=self._load_indexes,
                    lookup_collection_id=lambda current, current_db_name, current_coll_name: (
                        self._lookup_collection_id(
                            current,
                            current_db_name,
                            current_coll_name,
                        )
                    ),
                    quote_identifier=self._quote_identifier,
                    mark_index_metadata_changed=self._mark_index_metadata_changed,
                    invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                    is_builtin_id_index=self._is_builtin_id_index,
                )

    def _drop_database_sync(
        self,
        db_name: str,
        context: ClientSession | None = None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                self._admin_runtime.drop_database(db_name, context=context)
            self._clear_search_backend_state_for_database(db_name)

    def _clear_index_metadata_versions_for_database(
        self,
        db_name: str,
    ) -> None:
        self._admin_runtime.clear_index_metadata_versions_for_database(db_name)

    def _drop_indexes_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                _sqlite_drop_all_indexes(
                    conn,
                    db_name=db_name,
                    coll_name=coll_name,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                    load_indexes=self._load_indexes,
                    lookup_collection_id=lambda current, current_db_name, current_coll_name: (
                        self._lookup_collection_id(
                            current,
                            current_db_name,
                            current_coll_name,
                        )
                    ),
                    quote_identifier=self._quote_identifier,
                    mark_index_metadata_changed=self._mark_index_metadata_changed,
                    invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                )

    def _create_search_index_sync(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> str:
        with self._lock:
            return _sqlite_create_search_index_sync(
                self,
                db_name,
                coll_name,
                definition,
                max_time_ms,
                context,
            )

    def _list_search_indexes_sync(
        self,
        db_name: str,
        coll_name: str,
        name: str | None,
        context: ClientSession | None,
    ) -> list[SearchIndexDocument]:
        with self._lock:
            return _sqlite_list_search_indexes_sync(
                self,
                db_name,
                coll_name,
                name,
                context,
            )

    def _update_search_index_sync(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        definition: Document,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> None:
        with self._lock:
            _sqlite_update_search_index_sync(
                self,
                db_name,
                coll_name,
                name,
                definition,
                max_time_ms,
                context,
            )

    def _drop_search_index_sync(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> None:
        with self._lock:
            _sqlite_drop_search_index_sync(
                self,
                db_name,
                coll_name,
                name,
                max_time_ms,
                context,
            )

    def _exact_vector_hits_sync(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        query: SearchVectorQuery,
        *,
        candidate_storage_keys: list[str] | None = None,
    ) -> list[tuple[float, Document]]:
        return _sqlite_exact_vector_hits_sync(
            self,
            db_name,
            coll_name,
            definition,
            query,
            candidate_storage_keys=candidate_storage_keys,
        )

    def _execute_sqlite_search_query(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        query: SearchQuery,
        physical_name: str | None,
        deadline: float | None,
        result_limit_hint: int | None = None,
        downstream_filter_spec: dict[str, object] | None = None,
    ) -> list[Document]:
        return _sqlite_execute_search_query(
            self,
            conn,
            db_name,
            coll_name,
            definition,
            query,
            physical_name,
            deadline,
            result_limit_hint,
            downstream_filter_spec,
        )

    def _search_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        operator: str,
        spec: object,
        max_time_ms: int | None,
        context: ClientSession | None,
        result_limit_hint: int | None = None,
        downstream_filter_spec: dict[str, object] | None = None,
        query: SearchQuery | None = None,
        operation_context: OperationContext | None = None,
    ) -> list[Document]:
        with self._lock:
            return _sqlite_search_documents_sync(
                self,
                db_name,
                coll_name,
                operator,
                spec,
                max_time_ms,
                context,
                result_limit_hint,
                downstream_filter_spec,
                query,
                operation_context,
            )

    def _collect_search_metadata_pushdown_sync(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
    ):
        with self._lock:
            return _sqlite_collect_search_metadata_pushdown_sync(
                self,
                db_name,
                coll_name,
                request,
            )

    def _explain_search_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        operator: str,
        spec: object,
        max_time_ms: int | None,
        context: ClientSession | None,
        result_limit_hint: int | None = None,
        downstream_filter_spec: dict[str, object] | None = None,
        operation_context: OperationContext | None = None,
    ) -> QueryPlanExplanation:
        with self._lock:
            return _sqlite_explain_search_documents_sync(
                self,
                db_name,
                coll_name,
                operator,
                spec,
                max_time_ms,
                context,
                result_limit_hint,
                downstream_filter_spec,
                operation_context,
            )

    def _plan_search_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
    ) -> QueryPlanExplanation:
        with self._lock:
            return _sqlite_plan_search_documents_sync(
                self,
                db_name,
                coll_name,
                request,
            )

    def _list_databases_sync(
        self,
        context: ClientSession | None = None,
    ) -> list[str]:
        with self._lock:
            return self._admin_runtime.list_databases(context=context)

    def _list_collections_sync(
        self,
        db_name: str,
        context: ClientSession | None = None,
    ) -> list[str]:
        with self._lock:
            return self._admin_runtime.list_collections(
                db_name,
                context=context,
            )

    def _create_collection_sync(
        self,
        db_name: str,
        coll_name: str,
        options: dict[str, object] | None = None,
        context: ClientSession | None = None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            _sqlite_create_collection(
                conn=conn,
                db_name=db_name,
                coll_name=coll_name,
                options=options,
                begin_write=lambda current: self._begin_write(
                    current,
                    context,
                ),
                commit_write=lambda current: self._commit_write(
                    current,
                    context,
                ),
                rollback_write=lambda current: self._rollback_write(
                    current,
                    context,
                ),
                collection_exists=self._collection_exists_sync,
                ensure_collection_row=self._ensure_collection_row,
            )

    def _rename_collection_sync(
        self,
        db_name: str,
        coll_name: str,
        new_name: str,
        context: ClientSession | None = None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            _sqlite_rename_collection(
                conn=conn,
                db_name=db_name,
                coll_name=coll_name,
                new_name=new_name,
                begin_write=lambda current: self._begin_write(
                    current,
                    context,
                ),
                commit_write=lambda current: self._commit_write(
                    current,
                    context,
                ),
                rollback_write=lambda current: self._rollback_write(
                    current,
                    context,
                ),
                collection_exists=self._collection_exists_sync,
                mark_index_metadata_changed=self._mark_index_metadata_changed,
                invalidate_collection_id_cache=self._invalidate_collection_id_cache,
                invalidate_collection_features_cache=self._invalidate_collection_features_cache,
            )

    def _drop_collection_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None = None,
    ) -> None:
        self._ensure_session_can_use_engine(context)
        if self._is_profile_namespace(coll_name):
            self._admin_runtime.clear_profile_namespace(db_name)
            return
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                _sqlite_drop_collection(
                    conn=conn,
                    db_name=db_name,
                    coll_name=coll_name,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                    load_indexes=self._load_indexes,
                    load_search_index_rows=self._load_search_index_rows,
                    lookup_collection_id=self._lookup_collection_id,
                    quote_identifier=self._quote_identifier,
                    drop_search_backend=self._drop_search_backend_sync,
                    mark_index_metadata_changed=self._mark_index_metadata_changed,
                    invalidate_collection_id_cache=self._invalidate_collection_id_cache,
                    invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                )

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
        self._ensure_session_can_use_engine(context)
        return self._admin_runtime.set_profiling_level(
            db_name,
            level,
            slow_ms=slow_ms,
        )

    @override
    async def insert_document(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        overwrite: bool = True,
        *,
        context: ClientSession | None = None,
        bypass_document_validation: bool = False,
        on_commit: Callable[[InsertOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> InsertOutcome:
        if operation_context is not None:
            context = operation_context.session
        outcome = await self._run_blocking(
            self._put_document_sync,
            db_name,
            coll_name,
            document,
            overwrite,
            context,
            bypass_document_validation=bypass_document_validation,
            operation_context=operation_context,
        )
        if outcome and on_commit is not None:
            on_commit(outcome)
        return outcome

    async def insert_documents(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        *,
        context: ClientSession | None = None,
        bypass_document_validation: bool = False,
        on_commit: Callable[[InsertOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> tuple[InsertOutcome, ...]:
        if operation_context is not None:
            context = operation_context.session
        outcomes = await self._prepare_and_insert_documents(
            db_name,
            coll_name,
            documents,
            context=context,
            bypass_document_validation=bypass_document_validation,
            operation_context=operation_context,
        )
        if on_commit is not None:
            for outcome in outcomes:
                if outcome:
                    on_commit(outcome)
        return outcomes

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
        on_commit: Callable[[Document], None] | None = None,
    ) -> bool:
        outcome = await self._run_blocking(
            self._put_document_sync,
            db_name,
            coll_name,
            document,
            overwrite,
            context,
            bypass_document_validation=bypass_document_validation,
        )
        if outcome and on_commit is not None and outcome.document is not None:
            on_commit(outcome.document)
        return outcome.applied

    @override
    async def merge_document(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        *,
        when_matched: str,
        when_not_matched: str,
        context: ClientSession | None = None,
        on_commit: Callable[[MergeOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> MergeOutcome:
        if operation_context is not None:
            context = operation_context.session
        self._ensure_session_can_use_engine(context)
        return await self._run_blocking(
            self._merge_document_sync,
            db_name,
            coll_name,
            document,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            context=context,
            on_commit=on_commit,
            operation_context=operation_context,
        )

    async def put_documents_bulk(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        *,
        context: ClientSession | None = None,
        bypass_document_validation: bool = False,
        on_commit: Callable[[Document], None] | None = None,
    ) -> list[bool]:
        outcomes = await self._prepare_and_insert_documents(
            db_name,
            coll_name,
            documents,
            context=context,
            bypass_document_validation=bypass_document_validation,
        )
        if on_commit is not None:
            for outcome in outcomes:
                if outcome.document is not None:
                    on_commit(outcome.document)
        return [outcome.applied for outcome in outcomes]

    async def _prepare_and_insert_documents(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        *,
        context: ClientSession | None,
        bypass_document_validation: bool,
        operation_context: OperationContext | None = None,
    ) -> tuple[InsertOutcome, ...]:
        snapshot_options: dict[str, object] | None = None
        snapshot_indexes: list[EngineIndexRecord] = []
        effective_dialect = (
            operation_context.dialect
            if operation_context is not None
            else MONGODB_DIALECT_70
        )
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
                    contextvars.copy_context().run,
                    partial(
                        enforce_collection_document_validation,
                        document,
                        options=snapshot_options,
                        original_document=None,
                        dialect=effective_dialect,
                    ),
                )
                for document in documents
            ]
            await asyncio.gather(*validations)
        prepared_documents = await asyncio.gather(
            *[
                loop.run_in_executor(
                    self._ensure_executor(),
                    contextvars.copy_context().run,
                    partial(
                        self._prepare_bulk_document_with_indexes_sync,
                        document,
                        snapshot_indexes,
                    ),
                )
                for document in documents
            ],
        )
        return await self._run_blocking(
            self._insert_documents_sync,
            db_name,
            coll_name,
            documents,
            list(prepared_documents),
            snapshot_indexes,
            context=context,
            bypass_document_validation=bypass_document_validation,
            snapshot_options=snapshot_options,
            operation_context=operation_context,
        )

    @override
    async def get_document(
        self,
        db_name: str,
        coll_name: str,
        doc_id: DocumentId,
        *,
        projection: Projection | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
        operation_context: OperationContext | None = None,
    ) -> Document | None:
        operation_context = operation_context or OperationContext.create(
            dialect=dialect or MONGODB_DIALECT_70,
            session=context,
        )
        return await self._run_blocking(
            self._get_document_sync,
            db_name,
            coll_name,
            doc_id,
            projection,
            operation_context=operation_context,
        )

    @override
    async def delete_document(
        self,
        db_name: str,
        coll_name: str,
        doc_id: DocumentId,
        *,
        context: ClientSession | None = None,
    ) -> bool:
        return await self._run_blocking(
            self._delete_document_sync,
            db_name,
            coll_name,
            doc_id,
            context,
        )

    @override
    def open_read_snapshot(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        operation_context: OperationContext,
    ) -> ReadSnapshot:
        return ReadSnapshot(
            self.scan_find_semantics(
                db_name,
                coll_name,
                semantics,
                context=operation_context.session,
            ),
            policy=SnapshotPolicy.STABLE,
            operation_id=operation_context.operation_id,
        )

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

            items: queue.Queue[object] = queue.Queue(maxsize=2)
            sentinel = object()
            stop_event = threading.Event()

            def _enqueue(item: object) -> bool:
                while not stop_event.is_set():
                    try:
                        items.put(item, timeout=0.05)
                        return True
                    except queue.Full:
                        continue
                return False

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
                            if not _enqueue(batch):
                                break
                            batch = []
                except Exception as exc:
                    if batch:
                        _enqueue(batch)
                        batch = []
                    _enqueue(_ScanProducerError(exc))
                finally:
                    if batch:
                        _enqueue(batch)
                    with self._scan_condition:
                        self._active_scan_count -= 1
                        self._scan_condition.notify_all()
                    _enqueue(sentinel)

            producer = asyncio.create_task(self._run_blocking(_produce))
            try:
                while True:
                    try:
                        item = await self._run_blocking(
                            items.get,
                            True,
                            0.05,
                        )
                    except queue.Empty:
                        if producer.done():
                            break
                        continue
                    if item is sentinel:
                        break
                    if isinstance(item, _ScanProducerError):
                        raise item.error
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
        replacement_document: Document | None = None,
        on_commit: Callable[[MutationOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> MutationOutcome:
        if operation_context is not None:
            context = operation_context.session
        if (
            operation.compiled_update_plan is None
            or operation.compiled_upsert_plan is None
        ):
            raise OperationFailure(
                "update operation does not include a compiled update plan",
            )
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
            replacement_document,
            on_commit,
            operation_context,
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
        replacement_document: Document | None = None,
        on_commit: Callable[[MutationOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> MutationOutcome:
        if operation_context is not None:
            operation = operation.bind(operation_context)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                with sqlite_write_scope(
                    conn,
                    begin_write=lambda current: self._begin_write(
                        current,
                        context,
                    ),
                    commit_write=lambda current: self._commit_write(
                        current,
                        context,
                    ),
                    rollback_write=lambda current: self._rollback_write(
                        current,
                        context,
                    ),
                ):
                    semantics = compile_update_semantics(
                        operation,
                        dialect=dialect,
                        selector_filter=selector_filter,
                    )
                    self._resolve_hint_index(
                        db_name,
                        coll_name,
                        semantics.hint,
                        plan=semantics.query_plan,
                        dialect=semantics.dialect,
                        collation=semantics.collation,
                    )
                    result = _sqlite_update_with_operation(
                        db_name=db_name,
                        coll_name=coll_name,
                        operation=operation,
                        upsert=upsert,
                        upsert_seed=upsert_seed,
                        selector_filter=selector_filter,
                        dialect=dialect,
                        bypass_document_validation=bypass_document_validation,
                        compile_update_semantics=compile_update_semantics,
                        require_connection=lambda: conn,
                        purge_expired_documents=lambda current, current_db_name, current_coll_name: (
                            self._purge_expired_documents_sync(
                                current,
                                current_db_name,
                                current_coll_name,
                                context=context,
                                now=semantics.variables.now.replace(
                                    tzinfo=datetime.UTC,
                                ),
                            )
                        ),
                        collection_options_or_empty=self._collection_options_or_empty_sync,
                        dialect_requires_python_fallback=self._dialect_requires_python_fallback,
                        select_first_document_for_plan=self._select_first_document_for_plan,
                        load_documents=self._load_documents,
                        match_plan=lambda document, query_plan, current_dialect, current_collation, variables: (
                            QueryEngine.match_plan(
                                document,
                                query_plan,
                                dialect=current_dialect,
                                collation=current_collation,
                                variables=variables,
                            )
                        ),
                        enforce_collection_document_validation=enforce_collection_document_validation,
                        validate_document_against_unique_indexes=lambda current_db_name, current_coll_name, document, exclude_storage_key, skip_id_check=False, scan_payload_id=False: (
                            self._validate_document_against_unique_indexes(
                                current_db_name,
                                current_coll_name,
                                document,
                                exclude_storage_key=exclude_storage_key,
                                skip_id_check=skip_id_check,
                                scan_payload_id=scan_payload_id,
                            )
                        ),
                        load_indexes=self._load_indexes,
                        load_search_index_rows=lambda current_db_name, current_coll_name: (
                            self._load_search_index_rows(
                                current_db_name,
                                current_coll_name,
                            )
                        ),
                        begin_write=lambda current: None,
                        commit_write=lambda current: None,
                        rollback_write=lambda current: None,
                        translate_compiled_update_plan=lambda compiled_update_plan, current_document: (
                            translate_compiled_update_plan(
                                compiled_update_plan,
                                current_document=current_document,
                            )
                        ),
                        compiled_update_plan_type=CompiledUpdatePlan,
                        rebuild_multikey_entries_for_document=self._rebuild_multikey_entries_for_document,
                        rebuild_scalar_entries_for_document=self._rebuild_scalar_entries_for_document,
                        replace_search_entries_for_document=lambda current, current_db_name, current_coll_name, storage_key, document, search_indexes: (
                            self._replace_search_entries_for_document(
                                current,
                                current_db_name,
                                current_coll_name,
                                storage_key,
                                document,
                                search_indexes=search_indexes,
                            )
                        ),
                        serialize_document=self._serialize_document,
                        storage_key_for_id=self._storage_key,
                        new_object_id=ObjectId,
                        invalidate_collection_features_cache=self._invalidate_collection_features_cache,
                        replacement_document=replacement_document,
                    )
                    if result.after_document is not None and (
                        result.result.upserted_id is not None
                        or result.result.modified_count > 0
                    ):
                        change_context = operation_context
                        if (
                            change_context is not None
                            and "_id" not in result.after_document
                        ):
                            change_context = change_context.for_unpublishable_change()
                        commit_sequence = _sqlite_append_change(
                            conn,
                            context=change_context,
                            operation_type=(
                                "insert"
                                if result.result.upserted_id is not None
                                else (
                                    operation_context.change_operation_type
                                    if operation_context is not None
                                    and operation_context.change_operation_type
                                    is not None
                                    else "update"
                                )
                            ),
                            db_name=db_name,
                            coll_name=coll_name,
                            document_key=(
                                {
                                    "_id": deepcopy(
                                        result.after_document["_id"],
                                    ),
                                }
                                if "_id" in result.after_document
                                else {}
                            ),
                            full_document=deepcopy(result.after_document),
                            serialize_document=self._serialize_document,
                            max_entries=self._change_outbox_max_entries,
                        )
                        result = replace(
                            result,
                            commit_sequence=commit_sequence,
                        )
                if result.after_document is not None and on_commit is not None:
                    on_commit(result)
                return result

    @override
    async def delete_with_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        *,
        selector_filter: Filter | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
        on_commit: Callable[[DeleteOutcome], None] | None = None,
        operation_context: OperationContext | None = None,
    ) -> DeleteOutcome:
        if operation_context is not None:
            context = operation_context.session
        return await self._run_blocking(
            self._delete_matching_document_sync,
            db_name,
            coll_name,
            operation.filter_spec,
            operation.plan,
            context,
            dialect,
            operation.collation,
            operation.let,
            selector_filter,
            operation.sort,
            operation.hint,
            on_commit,
            operation_context,
        )

    @override
    async def count_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
        operation_context: OperationContext | None = None,
    ) -> int:
        if operation_context is not None:
            context = operation_context.session
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
        hidden: bool = False,
        collation: Filter | None = None,
        partial_filter_expression: Filter | None = None,
        expire_after_seconds: int | None = None,
        weights: dict[str, int] | None = None,
        default_language: str | None = None,
        language_override: str | None = None,
        min_value: float | None = None,
        max_value: float | None = None,
        bucket_size: float | None = None,
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
            hidden,
            collation,
            partial_filter_expression,
            expire_after_seconds,
            weights,
            default_language,
            language_override,
            min_value,
            max_value,
            bucket_size,
            max_time_ms,
            context,
        )

    @override
    async def list_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> list[IndexDocument]:
        return await self._run_blocking(
            self._list_indexes_sync,
            db_name,
            coll_name,
            context,
        )

    @override
    async def index_information(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> IndexInformation:
        return await self._run_blocking(
            self._index_information_sync,
            db_name,
            coll_name,
            context,
        )

    @override
    async def drop_index(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(
            self._drop_index_sync,
            db_name,
            coll_name,
            index_or_name,
            context,
        )

    @override
    async def drop_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(
            self._drop_indexes_sync,
            db_name,
            coll_name,
            context,
        )

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
        result_limit_hint: int | None = None,
        downstream_filter_spec: dict[str, object] | None = None,
    ) -> list[Document]:
        result = await self._run_blocking(
            self._search_documents_sync,
            db_name,
            coll_name,
            operator,
            spec,
            max_time_ms,
            context,
            result_limit_hint,
            downstream_filter_spec,
        )
        return [
            legacy_document_from_runtime_state(document)
            if isinstance(document, RuntimeDocumentState)
            else document
            for document in result
        ]

    async def execute_search(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
    ) -> SearchExecutionOutcome:
        if request.mode is SearchExecutionMode.METADATA:
            pushdown = await self._run_blocking(
                self._collect_search_metadata_pushdown_sync,
                db_name,
                coll_name,
                request,
            )
            if pushdown is not None:
                return SearchExecutionOutcome(
                    metadata=pushdown.metadata,
                    trace=SearchExecutionTrace(
                        backend="sqlite",
                        operation_id=request.operation_context.operation_id,
                        snapshot_captured=True,
                        matched_count=pushdown.candidate_count,
                        candidate_count=pushdown.candidate_count,
                        documents_scanned=0,
                        collector_document_count=pushdown.candidate_count,
                        collector_backend="sqlite",
                        collector_pushdown=True,
                        collector_count=pushdown.collector_count,
                        collector_plan=SearchCollectorPlan(
                            backend="sqlite",
                            pushed_down=True,
                            candidate_exact=True,
                            count_strategy="candidate-set",
                            facet_strategy="group-by-distinct-storage-key",
                        ),
                    ),
                )
        documents = await self._run_blocking(
            self._search_documents_sync,
            db_name,
            coll_name,
            request.effective_operator,
            request.effective_specification,
            request.max_time_ms,
            request.operation_context.session,
            request.result_limit_hint,
            request.downstream_filter_spec,
            request.query,
            request.operation_context,
        )
        if request.mode is SearchExecutionMode.METADATA:
            return SearchExecutionOutcome(
                metadata=collect_search_metadata(
                    documents,
                    query=request.query,
                ),
                trace=SearchExecutionTrace(
                    backend="sqlite",
                    operation_id=request.operation_context.operation_id,
                    snapshot_captured=True,
                    matched_count=len(documents),
                    collector_document_count=len(documents),
                    collector_backend="semantic-core",
                    collector_count=(
                        (1 if request.query.stage_options.count else 0)
                        + (
                            len(request.query.stage_options.facet.definitions)
                            if request.query.stage_options.facet
                            else 0
                        )
                    ),
                    collector_plan=SearchCollectorPlan(
                        backend="semantic-core",
                        pushed_down=False,
                        candidate_exact=None,
                        count_strategy="materialized-outcome",
                        facet_strategy="semantic-accumulator",
                        fallback_reason="pushdown-not-provably-exact",
                    ),
                ),
            )
        return SearchExecutionOutcome.from_documents(
            documents,
            backend="sqlite",
            operation_id=request.operation_context.operation_id,
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
        result_limit_hint: int | None = None,
        downstream_filter_spec: dict[str, object] | None = None,
    ) -> QueryPlanExplanation:
        return await self._run_blocking(
            self._explain_search_documents_sync,
            db_name,
            coll_name,
            operator,
            spec,
            max_time_ms,
            context,
            result_limit_hint,
            downstream_filter_spec,
        )

    async def explain_search(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
        verbosity: SearchExplainVerbosity,
    ) -> QueryPlanExplanation:
        plan = await self._run_blocking(
            self._plan_search_documents_sync,
            db_name,
            coll_name,
            request,
        )
        if verbosity is SearchExplainVerbosity.QUERY_PLANNER:
            return plan
        if (plan.details or {}).get("status") != "READY":
            message = f"search index [{request.query.index_name}] is not ready"
            raise OperationFailure(message)
        if request.mode is SearchExecutionMode.HITS:
            explanation = await self._run_blocking(
                self._explain_search_documents_sync,
                db_name,
                coll_name,
                request.operator,
                request.specification,
                request.max_time_ms,
                request.operation_context.session,
                request.result_limit_hint,
                request.downstream_filter_spec,
                request.operation_context,
            )
            details = dict(explanation.details or {})
            details.update(
                {
                    "contractVersion": "search-v1",
                    "verbosity": verbosity.value,
                    "executionStats": SearchExecutionTrace.from_explain_details(
                        details,
                        default_backend="sqlite",
                        operation_id=request.operation_context.operation_id,
                    ).to_document(),
                },
            )
            return replace(explanation, details=details)
        outcome = await self.execute_search(db_name, coll_name, request)
        details = dict(plan.details or {})
        trace = outcome.trace
        details.update(
            {
                "contractVersion": "search-v1",
                "verbosity": verbosity.value,
                "executionStats": trace.to_document() if trace is not None else None,
            },
        )
        if request.mode is SearchExecutionMode.METADATA:
            collector_pushdown = dict(details.get("collectorPushdown") or {})
            if trace is not None:
                collector_pushdown.update(
                    {
                        "backend": trace.collector_backend,
                        "pushedDown": trace.collector_pushdown,
                    },
                )
            details["collectorPlan"] = (
                trace.collector_plan.to_document()
                if trace is not None and trace.collector_plan is not None
                else None
            )
            details["collectorPushdown"] = collector_pushdown
        return replace(plan, details=details)

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
        text_index_name: str | None = None
        text_fields: tuple[str, ...] | None = None
        text_index_metadata: EngineIndexRecord | None = None
        if semantics.text_query is None:
            hinted_index = await self._run_blocking(
                self._resolve_hint_index,
                db_name,
                coll_name,
                semantics.hint,
            )
        else:
            text_indexes = await self._run_blocking(
                self._load_indexes,
                db_name,
                coll_name,
            )
            text_index_name, text_fields = resolve_classic_text_index_for_hint(
                text_indexes,
                semantics.hint,
            )
            text_index_metadata = next(
                (index for index in text_indexes if index.name == text_index_name),
                None,
            )
            hinted_index = text_index_metadata if semantics.hint is not None else None
        execution_plan = await self.plan_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
        )
        pushdown_details = sqlite_pushdown_details(execution_plan)
        details: object
        if (
            isinstance(execution_plan, SQLiteReadExecutionPlan)
            and execution_plan.use_sql
        ):
            sql_details = await self._run_blocking(
                self._explain_query_plan_sync,
                db_name,
                coll_name,
                semantics,
                hint=semantics.hint,
            )
            details = {
                "engine_details": sql_details,
                "pushdown": pushdown_details,
            }
            if execution_plan.fallback_reason is not None:
                details["fallback_reason"] = execution_plan.fallback_reason
        else:
            details = {
                "pushdown": pushdown_details,
                "fallback_reason": execution_plan.fallback_reason,
            }
        virtual_details = describe_virtual_index_usage(
            await self._run_blocking(
                self._list_indexes_sync,
                db_name,
                coll_name,
                context,
            ),
            semantics.query_plan,
            hinted_index_name=None if hinted_index is None else hinted_index["name"],
            dialect=semantics.dialect,
            collation=semantics.collation,
        )
        if virtual_details is not None:
            if isinstance(details, dict):
                details = {**details, **virtual_details}
        if semantics.text_query is not None:
            assert text_index_name is not None and text_fields is not None
            text_details = {
                "textQuery": {
                    "backend": "python",
                    "index": text_index_name,
                    "fields": list(text_fields),
                    "rawQuery": semantics.text_query.raw_query,
                    "terms": list(semantics.text_query.terms),
                    "tokenizer": "lowercase+punctuation-split",
                    "caseSensitive": semantics.text_query.case_sensitive,
                    "diacriticSensitive": semantics.text_query.diacritic_sensitive,
                },
            }
            if semantics.text_query.required_phrases:
                text_details["textQuery"]["requiredPhrases"] = [
                    " ".join(phrase) for phrase in semantics.text_query.required_phrases
                ]
            if semantics.text_query.excluded_terms:
                text_details["textQuery"]["excludedTerms"] = list(
                    semantics.text_query.excluded_terms,
                )
            if semantics.text_query.excluded_phrases:
                text_details["textQuery"]["excludedPhrases"] = [
                    " ".join(phrase) for phrase in semantics.text_query.excluded_phrases
                ]
            if semantics.text_query.language is not None:
                text_details["textQuery"]["language"] = semantics.text_query.language
            if text_index_metadata is not None:
                if text_index_metadata.weights is not None:
                    text_details["textQuery"]["weights"] = deepcopy(
                        text_index_metadata.weights,
                    )
                if text_index_metadata.default_language is not None:
                    text_details["textQuery"]["defaultLanguage"] = (
                        text_index_metadata.default_language
                    )
                if text_index_metadata.language_override is not None:
                    text_details["textQuery"]["languageOverride"] = (
                        text_index_metadata.language_override
                    )
            if isinstance(details, dict):
                details = {**details, **text_details}
        planning_issues = sqlite_planning_issues(
            execution_plan.fallback_reason,
        )
        pushdown_hints = sqlite_pushdown_followup_hints(
            semantics.query_plan,
            execution_plan.fallback_reason,
        )
        if pushdown_hints and isinstance(details, dict):
            details = {**details, "pushdown_hints": pushdown_hints}
        return build_query_plan_explanation(
            engine="sqlite",
            strategy=execution_plan.strategy,
            semantics=semantics,
            details=details,
            hinted_index=None if hinted_index is None else hinted_index["name"],
            execution_lineage=execution_plan.execution_lineage,
            physical_plan=execution_plan.physical_plan,
            fallback_reason=execution_plan.fallback_reason,
            planning_issues=planning_issues,
        )

    @override
    async def list_databases(
        self,
        *,
        context: ClientSession | None = None,
    ) -> list[str]:
        return await self._run_blocking(self._list_databases_sync, context)

    async def drop_database(
        self,
        db_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await self._run_blocking(self._drop_database_sync, db_name, context)

    @override
    async def list_collections(
        self,
        db_name: str,
        *,
        context: ClientSession | None = None,
    ) -> list[str]:
        return await self._run_blocking(
            self._list_collections_sync,
            db_name,
            context,
        )

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
        await self._run_blocking(
            self._drop_collection_sync,
            db_name,
            coll_name,
            context,
        )
