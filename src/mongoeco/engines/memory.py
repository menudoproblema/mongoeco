import asyncio
from contextlib import AsyncExitStack
import datetime
from dataclasses import dataclass
import heapq
import inspect
import math
import numpy as np
import threading
import time
import uuid
from copy import deepcopy
from typing import Any, AsyncIterable, override

from mongoeco.api.operations import FindOperation, UpdateOperation
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_ordering import bson_engine_key
from mongoeco.core.collation import normalize_collation
from mongoeco.core.aggregation.cost import AggregationCostPolicy
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines._shared_namespace_admin import (
    merge_profile_collection_names,
    merge_profile_database_names,
    namespace_collection_names,
    namespace_collection_options,
    namespace_database_names,
)
from mongoeco.engines._shared_search_admin import (
    build_search_index_documents,
    ensure_search_index_query_supported,
    normalize_search_index_definition,
    search_index_not_found,
)
from mongoeco.engines._active_operations import LocalActiveOperationRegistry
from mongoeco.engines._memory_search_runtime import (
    execute_search_documents as _execute_memory_search_documents,
    explain_search_documents as _explain_memory_search_documents,
)
from mongoeco.engines._memory_vector_runtime import (
    MaterializedVectorIndex as _RuntimeMaterializedVectorIndex,
    build_materialized_vector_index as _build_memory_vector_index,
    candidate_positions_for_vector_filter as _memory_candidate_positions_for_vector_filter,
    vector_scores_for_rows as _memory_vector_scores_for_rows,
    vector_scores_for_positions as _memory_vector_scores_for_positions,
)
from mongoeco.engines._runtime_metrics import LocalRuntimeMetrics
from mongoeco.engines._shared_ttl import coerce_ttl_datetime, document_expired_by_ttl
from mongoeco.engines.mvcc import MemoryMvccState
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
    iter_filtered_documents,
    finalize_documents,
    stream_finalize_documents,
)
from mongoeco.engines.virtual_indexes import (
    describe_virtual_index_usage,
    document_in_virtual_index,
    normalize_partial_filter_expression,
    query_can_use_index,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.paths import get_document_value
from mongoeco.core.projections import apply_projection
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.query_plan import QueryNode, ensure_query_plan
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.search import (
    attach_text_score,
    classic_text_score,
    ClassicTextQuery,
    MaterializedSearchDocument,
    materialize_search_document,
    SearchCompoundQuery,
    search_compound_ranking,
    SearchNearQuery,
    SearchVectorQuery,
    build_search_index_document,
    compile_search_stage,
    is_text_search_query,
    matches_search_query,
    resolve_classic_text_index,
    search_near_distance,
    search_query_explain_details,
    validate_search_index_definition,
    vector_field_specs,
    vector_field_paths,
)
from mongoeco.core.search_filter_prefilter import (
    collect_filterable_values as _shared_collect_filterable_values,
    filter_value_key as _shared_filter_value_key,
    matches_candidateable_filter as _shared_matches_candidateable_filter,
    value_key_matches_range as _shared_value_key_matches_range,
)
from mongoeco.core.aggregation import AggregationSpillPolicy
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.session import EngineTransactionContext
from mongoeco.types import (
    ArrayFilters, CollationDocument, DeleteResult, Document, DocumentId, ExecutionLineageStep, Filter, IndexInformation, IndexDocument, IndexKeySpec, ObjectId,
    PhysicalPlanStep,
    ProfilingCommandResult,
    Projection, QueryPlanExplanation, SortSpec, Update, UpdateResult, default_index_name,
    default_id_index_definition, default_id_index_document, default_id_index_information, index_fields,
    EngineIndexRecord, SearchIndexDefinition, SearchIndexDocument, is_ordered_index_spec,
    normalize_index_keys, special_index_directions,
)


class _AsyncLock:
    """Lock compatible con asyncio para uso dentro del motor."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "_AsyncLock":
        await self._lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._lock.release()
        return False


class _StoredDocument:
    __slots__ = ("payload", "decoded_wrapped", "decoded_public")

    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.decoded_wrapped: Document | None = None
        self.decoded_public: Document | None = None


type _FilterValueKey = tuple[str, object]


@dataclass(frozen=True, slots=True)
class _MaterializedVectorDocument:
    document: Document
    vectors_by_path: dict[str, tuple[float, ...]]
    exists_paths: frozenset[str]
    scalar_values: dict[str, tuple[_FilterValueKey, ...]]


@dataclass(frozen=True, slots=True)
class _MaterializedVectorIndex:
    documents: tuple[_MaterializedVectorDocument, ...]
    vector_paths: tuple[str, ...]
    vector_specs: dict[str, dict[str, object]]
    valid_vector_counts: dict[str, int]
    vector_matrices: dict[str, np.ndarray]
    vector_row_positions: dict[str, tuple[int, ...]]
    vector_row_index_by_position: dict[str, dict[int, int]]
    exists_filter_index: dict[str, frozenset[int]]
    scalar_filter_index: dict[str, dict[_FilterValueKey, frozenset[int]]]
    scalar_values_by_path: dict[str, tuple[tuple[int, _FilterValueKey], ...]]


def _filter_value_key(value: object) -> _FilterValueKey | None:
    return _shared_filter_value_key(value)


def _collect_filterable_values(
    value: object,
    *,
    prefix: str = "",
) -> list[tuple[str, set[_FilterValueKey]]]:
    return _shared_collect_filterable_values(value, prefix=prefix)


def _build_materialized_vector_index(
    documents: list[Document],
    definition: SearchIndexDefinition,
) -> _MaterializedVectorIndex:
    return _build_memory_vector_index(documents, definition)


def _value_key_matches_range(value_key: _FilterValueKey, clause: dict[str, object]) -> bool:
    return _shared_value_key_matches_range(value_key, clause)


def _candidate_positions_for_vector_filter(
    vector_index: _MaterializedVectorIndex,
    *,
    filter_spec: dict[str, object] | None,
    candidate_positions: tuple[int, ...] | list[int] | None = None,
) -> tuple[list[int] | None, dict[str, object] | None]:
    return _memory_candidate_positions_for_vector_filter(
        vector_index,
        filter_spec=filter_spec,
        candidate_positions=candidate_positions,
    )


def _matches_candidateable_filter(document: Document, filter_spec: dict[str, object]) -> bool | None:
    return _shared_matches_candidateable_filter(document, filter_spec)


def _vector_scores_for_positions(
    vector_index: _MaterializedVectorIndex,
    *,
    query: SearchVectorQuery,
    candidate_positions: list[int],
    limit: int | None,
) -> list[tuple[float, int]]:
    return _memory_vector_scores_for_positions(
        vector_index,
        query=query,
        candidate_positions=candidate_positions,
        limit=limit,
    )


def _vector_scores_for_rows(
    vector_index: _MaterializedVectorIndex,
    *,
    query: SearchVectorQuery,
    candidate_rows: list[int],
    limit: int | None,
) -> list[tuple[float, int]]:
    return _memory_vector_scores_for_rows(
        vector_index,
        query=query,
        candidate_rows=candidate_rows,
        limit=limit,
    )


class MemoryEngine(AsyncStorageEngine):
    """Motor de almacenamiento en memoria ultra-rápido."""

    _PROFILE_COLLECTION_NAME = "system.profile"

    def __init__(
        self,
        codec: type[DocumentCodec] = DocumentCodec,
        *,
        aggregation_spill_threshold: int | None = None,
        aggregation_materialization_limit: int | None = 50_000,
        simulate_search_index_latency: float = 0.0,
    ):
        self._storage: dict[str, dict[str, dict[Any, Any]]] = {}
        self._locks: dict[str, _AsyncLock] = {}
        self._indexes: dict[str, dict[str, list[EngineIndexRecord]]] = {}
        self._index_data: dict[str, dict[str, dict[str, dict[tuple[Any, ...], set[Any]]]]] = {}
        self._search_indexes: dict[str, dict[str, list[SearchIndexDefinition]]] = {}
        self._search_index_ready_at: dict[tuple[str, str, str], float] = {}
        self._search_document_cache: dict[
            tuple[str, str, str],
            tuple[SearchIndexDefinition, list[tuple[Document, MaterializedSearchDocument]]],
        ] = {}
        self._vector_document_cache: dict[
            tuple[str, str, str],
            tuple[SearchIndexDefinition, _MaterializedVectorIndex],
        ] = {}
        self._collections: dict[str, set[str]] = {}
        self._collection_options: dict[str, dict[str, Document]] = {}
        self._meta_lock = threading.Lock()
        self._connection_count = 0
        self._codec = codec
        self._profiler = EngineProfiler("memory")
        self._runtime_metrics = LocalRuntimeMetrics()
        self._active_operations = LocalActiveOperationRegistry()
        self._mvcc_version = 0
        self._mvcc_states: dict[str, MemoryMvccState] = {}
        self.aggregation_spill_policy = (
            None
            if aggregation_spill_threshold is None
            else AggregationSpillPolicy(
                threshold=aggregation_spill_threshold,
                codec=codec,
            )
        )
        self.aggregation_cost_policy = (
            None
            if aggregation_materialization_limit is None
            else AggregationCostPolicy(
                max_materialized_documents=aggregation_materialization_limit,
                require_spill_for_blocking_stages=True,
            )
        )
        self._simulate_search_index_latency = max(0.0, float(simulate_search_index_latency))

    def _mark_search_index_pending(self, db_name: str, coll_name: str, index_name: str) -> None:
        if self._simulate_search_index_latency <= 0:
            self._search_index_ready_at.pop((db_name, coll_name, index_name), None)
            return
        self._search_index_ready_at[(db_name, coll_name, index_name)] = (
            time.time() + self._simulate_search_index_latency
        )

    def _search_index_is_ready(self, db_name: str, coll_name: str, index_name: str) -> bool:
        ready_at = self._search_index_ready_at.get((db_name, coll_name, index_name))
        if ready_at is None:
            return True
        if time.time() >= ready_at:
            self._search_index_ready_at.pop((db_name, coll_name, index_name), None)
            return True
        return False

    def _clear_search_runtime_cache(self) -> None:
        self._search_document_cache.clear()
        self._vector_document_cache.clear()

    def _invalidate_search_runtime_cache(
        self,
        db_name: str,
        coll_name: str,
        *,
        index_name: str | None = None,
    ) -> None:
        if index_name is not None:
            self._search_document_cache.pop((db_name, coll_name, index_name), None)
            self._vector_document_cache.pop((db_name, coll_name, index_name), None)
            return
        stale_keys = [
            key
            for key in self._search_document_cache
            if key[0] == db_name and key[1] == coll_name
        ]
        for key in stale_keys:
            self._search_document_cache.pop(key, None)
        stale_vector_keys = [
            key
            for key in self._vector_document_cache
            if key[0] == db_name and key[1] == coll_name
        ]
        for key in stale_vector_keys:
            self._vector_document_cache.pop(key, None)

    def _materialized_search_documents(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        *,
        context: ClientSession | None,
    ) -> list[tuple[Document, MaterializedSearchDocument]]:
        if context is not None:
            return [
                (document, materialize_search_document(document, definition))
                for document in [
                    self._decode_storage_document(payload)
                    for payload in self._storage_view(context).get(db_name, {}).get(coll_name, {}).values()
                ]
            ]
        cache_key = (db_name, coll_name, definition.name)
        cached = self._search_document_cache.get(cache_key)
        if cached is not None and cached[0] == definition:
            return cached[1]
        materialized = [
            (document, materialize_search_document(document, definition))
            for document in [
                self._decode_storage_document(payload)
                for payload in self._storage_view(None).get(db_name, {}).get(coll_name, {}).values()
            ]
        ]
        self._search_document_cache[cache_key] = (definition, materialized)
        return materialized

    def _materialized_vector_index(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        *,
        context: ClientSession | None,
    ) -> _RuntimeMaterializedVectorIndex:
        documents = [
            self._decode_storage_document(payload)
            for payload in self._storage_view(context).get(db_name, {}).get(coll_name, {}).values()
        ]
        if context is not None:
            return _build_memory_vector_index(documents, definition)
        cache_key = (db_name, coll_name, definition.name)
        cached = self._vector_document_cache.get(cache_key)
        if cached is not None and cached[0] == definition:
            return cached[1]
        materialized = _build_memory_vector_index(documents, definition)
        self._vector_document_cache[cache_key] = (definition, materialized)
        return materialized

    def _runtime_diagnostics_info(self) -> dict[str, object]:
        declared_search_index_count = sum(
            len(indexes)
            for collections in self._search_indexes.values()
            for indexes in collections.values()
        )
        tracked_collection_count = sum(len(collections) for collections in self._collections.values())
        collection_option_namespace_count = sum(
            len(collections)
            for collections in self._collection_options.values()
        )
        index_data_collection_count = sum(
            len(collections)
            for collections in self._index_data.values()
        )
        return {
            "planner": {
                "engine": "python",
                "pushdownModes": ["python"],
                "hybridSortFallback": False,
            },
            "search": {
                "backend": "python",
                "declaredIndexCount": declared_search_index_count,
                "pendingIndexCount": len(self._search_index_ready_at),
                "simulatedIndexLatencySeconds": self._simulate_search_index_latency,
                "fts5Available": None,
            },
            "caches": {
                "trackedCollections": tracked_collection_count,
                "collectionOptionNamespaces": collection_option_namespace_count,
                "indexDataCollections": index_data_collection_count,
            },
        }

    _coerce_ttl_datetime = staticmethod(coerce_ttl_datetime)

    def _document_expired_by_ttl(
        self,
        document: Document,
        index: EngineIndexRecord,
        *,
        now: datetime.datetime,
    ) -> bool:
        return document_expired_by_ttl(
            document,
            index,
            now=now,
            extract_values=QueryEngine.extract_values,
        )

    def _purge_expired_documents_locked(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
        indexes_view: dict[str, dict[str, list[EngineIndexRecord]]] | None = None,
        index_data_view: dict[str, dict[str, dict[str, dict[tuple[Any, ...], set[Any]]]]] | None = None,
        storage_view: dict[str, dict[str, dict[Any, Any]]] | None = None,
    ) -> int:
        indexes_source = indexes_view if indexes_view is not None else self._indexes_view(context)
        storage_source = storage_view if storage_view is not None else self._storage_view(context)
        index_data_source = index_data_view if index_data_view is not None else self._index_data_view(context)
        coll_indexes = indexes_source.get(db_name, {}).get(coll_name, [])
        ttl_indexes = [index for index in coll_indexes if index.expire_after_seconds is not None]
        if not ttl_indexes:
            return 0
        coll = storage_source.get(db_name, {}).get(coll_name, {})
        if not coll:
            return 0
        now = datetime.datetime.now(datetime.timezone.utc)
        expired_storage_keys: list[Any] = []
        for storage_key, data in coll.items():
            document = self._borrow_storage_document(data)
            if any(
                self._document_expired_by_ttl(document, index, now=now)
                for index in ttl_indexes
            ):
                expired_storage_keys.append(storage_key)
        for storage_key in expired_storage_keys:
            document = self._borrow_storage_document(coll[storage_key])
            self._update_indexes_locked(
                db_name,
                coll_name,
                storage_key,
                document,
                action="delete",
                index_data_view=index_data_source,
                indexes_view=indexes_source,
            )
            del coll[storage_key]
        return len(expired_storage_keys)

    def _decode_storage_document(
        self,
        payload: object,
        *,
        preserve_bson_wrappers: bool = True,
    ) -> Document:
        if isinstance(payload, _StoredDocument):
            if preserve_bson_wrappers:
                return self._copy_document_containers(self._borrow_storage_document(payload))
            payload = payload.payload
        return self._decode_codec_payload(payload, preserve_bson_wrappers=preserve_bson_wrappers)

    def _borrow_storage_document(
        self,
        payload: object,
        *,
        preserve_bson_wrappers: bool = True,
    ) -> Document:
        if isinstance(payload, _StoredDocument):
            if preserve_bson_wrappers and payload.decoded_wrapped is not None:
                return payload.decoded_wrapped
            raw_payload = payload.payload
        else:
            raw_payload = payload
        decoded = self._decode_codec_payload(raw_payload, preserve_bson_wrappers=preserve_bson_wrappers)
        if isinstance(payload, _StoredDocument) and preserve_bson_wrappers:
            payload.decoded_wrapped = decoded
        return decoded

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

    def _borrow_public_storage_document(self, payload: object) -> Document:
        if isinstance(payload, _StoredDocument):
            if payload.decoded_public is not None:
                return payload.decoded_public
            public_document = DocumentCodec.to_public(self._borrow_storage_document(payload))
            payload.decoded_public = public_document
            return public_document
        return DocumentCodec.to_public(self._borrow_storage_document(payload, preserve_bson_wrappers=False))

    def _encode_storage_document(self, document: Document) -> _StoredDocument:
        return _StoredDocument(self._codec.encode(document))

    def _copy_document_containers(self, value: object) -> object:
        if isinstance(value, dict):
            return {
                key: self._copy_document_containers(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._copy_document_containers(item) for item in value]
        return value

    @override
    def create_session_state(self, session: ClientSession) -> None:
        engine_key = f"memory:{id(self)}"
        session.bind_engine_context(
            EngineTransactionContext(
                engine_key=engine_key,
                connected=self._connection_count > 0,
                supports_transactions=True,
                transaction_active=False,
                metadata={"snapshot_version": self._mvcc_version},
            )
        )
        session.register_transaction_hooks(
            engine_key,
            start=self._start_session_transaction,
            commit=self._commit_session_transaction,
            abort=self._abort_session_transaction,
        )

    def _engine_key(self) -> str:
        return f"memory:{id(self)}"

    def _sync_session_state(
        self,
        session: ClientSession,
        *,
        transaction_active: bool | None = None,
        snapshot_version: int | None = None,
    ) -> None:
        state = session.get_engine_context(self._engine_key())
        if state is None:
            return
        state.connected = self._connection_count > 0
        if transaction_active is not None:
            state.transaction_active = transaction_active
        if snapshot_version is not None:
            state.metadata["snapshot_version"] = snapshot_version

    def _start_session_transaction(self, session: ClientSession) -> None:
        with self._meta_lock:
            snapshot = MemoryMvccState.capture(
                snapshot_version=self._mvcc_version,
                storage=self._storage,
                indexes=self._indexes,
                index_data=self._index_data,
                search_indexes=self._search_indexes,
                collections=self._collections,
                collection_options=self._collection_options,
            )
            self._mvcc_states[session.session_id] = snapshot
        self._sync_session_state(
            session,
            transaction_active=True,
            snapshot_version=snapshot.snapshot_version,
        )

    def _commit_session_transaction(self, session: ClientSession) -> None:
        with self._meta_lock:
            snapshot = self._mvcc_states.pop(session.session_id, None)
            if snapshot is not None:
                if snapshot.snapshot_version != self._mvcc_version:
                    raise OperationFailure("Write conflict during transaction commit")
                self._storage = snapshot.storage
                self._indexes = snapshot.indexes
                self._index_data = snapshot.index_data
                self._search_indexes = snapshot.search_indexes
                self._collections = snapshot.collections
                self._collection_options = snapshot.collection_options
                self._mvcc_version += 1
                self._clear_search_runtime_cache()
        self._sync_session_state(
            session,
            transaction_active=False,
            snapshot_version=self._mvcc_version,
        )

    def _abort_session_transaction(self, session: ClientSession) -> None:
        with self._meta_lock:
            self._mvcc_states.pop(session.session_id, None)
        self._sync_session_state(
            session,
            transaction_active=False,
            snapshot_version=self._mvcc_version,
        )

    def _active_mvcc_state(self, context: ClientSession | None) -> MemoryMvccState | None:
        if context is None or not context.in_transaction:
            return None
        return self._mvcc_states.get(context.session_id)

    def _storage_view(self, context: ClientSession | None) -> dict[str, dict[str, dict[Any, Any]]]:
        state = self._active_mvcc_state(context)
        return self._storage if state is None else state.storage

    def _indexes_view(self, context: ClientSession | None) -> dict[str, dict[str, list[EngineIndexRecord]]]:
        state = self._active_mvcc_state(context)
        return self._indexes if state is None else state.indexes

    def _collections_view(self, context: ClientSession | None) -> dict[str, set[str]]:
        state = self._active_mvcc_state(context)
        return self._collections if state is None else state.collections

    def _search_indexes_view(self, context: ClientSession | None) -> dict[str, dict[str, list[SearchIndexDefinition]]]:
        state = self._active_mvcc_state(context)
        return self._search_indexes if state is None else state.search_indexes

    def _index_data_view(self, context: ClientSession | None) -> dict[str, dict[str, dict[str, dict[tuple[Any, ...], set[Any]]]]]:
        state = self._active_mvcc_state(context)
        return self._index_data if state is None else state.index_data

    def _rebuild_index_data_locked(self, db_name: str, coll_name: str, index: EngineIndexRecord, *, index_data_view: dict | None = None, storage_view: dict | None = None) -> None:
        if index_data_view is None:
            index_data_view = self._index_data
        if storage_view is None:
            storage_view = self._storage

        db_data = index_data_view.setdefault(db_name, {})
        coll_data = db_data.setdefault(coll_name, {})
        index_map = coll_data.setdefault(index["name"], {})
        index_map.clear()

        coll_storage = storage_view.get(db_name, {}).get(coll_name, {})
        for storage_key, data in coll_storage.items():
            doc = self._borrow_storage_document(data)
            if not document_in_virtual_index(doc, index):
                continue
            key = self._index_key(doc, index["fields"])
            index_map.setdefault(key, set()).add(storage_key)

    def _update_indexes_locked(self, db_name: str, coll_name: str, storage_key: Any, document: Document, *, action: str = "insert", index_data_view: dict | None = None, indexes_view: dict | None = None) -> None:
        if indexes_view is None:
            indexes_view = self._indexes
        if index_data_view is None:
            index_data_view = self._index_data

        indexes = indexes_view.get(db_name, {}).get(coll_name, [])
        if not indexes:
            return

        db_data = index_data_view.setdefault(db_name, {})
        coll_data = db_data.setdefault(coll_name, {})

        for index in indexes:
            if not document_in_virtual_index(document, index):
                continue
            index_map = coll_data.setdefault(index["name"], {})
            key = self._index_key(document, index["fields"])
            if action == "insert":
                index_map.setdefault(key, set()).add(storage_key)
            else:
                if key in index_map:
                    index_map[key].discard(storage_key)
                    if not index_map[key]:
                        del index_map[key]

    def _collection_options_view(self, context: ClientSession | None) -> dict[str, dict[str, Document]]:
        state = self._active_mvcc_state(context)
        return self._collection_options if state is None else state.collection_options

    def _lock_key(self, db: str, coll: str) -> str:
        return f"{db}.{coll}"

    def _get_lock(self, db: str, coll: str) -> _AsyncLock:
        key = f"{db}.{coll}"
        with self._meta_lock:
            return self._locks.setdefault(key, _AsyncLock())

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
            f"memory:{id(self)}",
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
        return coll_name == self._PROFILE_COLLECTION_NAME

    def _profile_documents(self, db_name: str) -> list[Document]:
        return self._profiler.list_entries(db_name)

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
                return EngineIndexRecord(
                    name="_id_",
                    physical_name=None,
                    fields=["_id"],
                    key=[("_id", 1)],
                    unique=True,
                )
        else:
            normalized_hint = normalize_index_keys(hint)
            if self._is_builtin_id_index(normalized_hint):
                return EngineIndexRecord(
                    name="_id_",
                    physical_name=None,
                    fields=["_id"],
                    key=[("_id", 1)],
                    unique=True,
                )

        if indexes is None:
            indexes = deepcopy(self._indexes.get(db_name, {}).get(coll_name, []))

        for index in indexes:
            if isinstance(hint, str):
                if index["name"] == hint:
                    if index.get("hidden"):
                        raise OperationFailure("hint does not correspond to a usable index for this query")
                    if plan is not None and not query_can_use_index(index, plan, dialect=dialect):
                        raise OperationFailure("hint does not correspond to a usable index for this query")
                    return deepcopy(index)
            else:
                if index["key"] == normalized_hint:
                    if index.get("hidden"):
                        raise OperationFailure("hint does not correspond to a usable index for this query")
                    if plan is not None and not query_can_use_index(index, plan, dialect=dialect):
                        raise OperationFailure("hint does not correspond to a usable index for this query")
                    return deepcopy(index)

        raise OperationFailure("hint does not correspond to an existing index")

    def _storage_key(self, value: Any) -> Any:
        return self._typed_engine_key(value)

    def _register_collection_locked(
        self,
        db_name: str,
        coll_name: str,
        *,
        options: Document | None = None,
        collections: dict[str, set[str]] | None = None,
        collection_options: dict[str, dict[str, Document]] | None = None,
    ) -> None:
        target_collections = self._collections if collections is None else collections
        target_options = self._collection_options if collection_options is None else collection_options
        target_collections.setdefault(db_name, set()).add(coll_name)
        db_options = target_options.setdefault(db_name, {})
        db_options.setdefault(coll_name, deepcopy(options or {}))

    def _prune_collection_registry_locked(
        self,
        db_name: str,
        coll_name: str,
        *,
        collections: dict[str, set[str]] | None = None,
        collection_options: dict[str, dict[str, Document]] | None = None,
    ) -> None:
        target_collections = self._collections if collections is None else collections
        target_options = self._collection_options if collection_options is None else collection_options
        collections = target_collections.get(db_name)
        if collections is None:
            return
        collections.discard(coll_name)
        if not collections:
            del target_collections[db_name]
        db_options = target_options.get(db_name)
        if db_options is not None:
            db_options.pop(coll_name, None)
            if not db_options:
                del target_options[db_name]

    def _prune_index_data_locked(
        self,
        db_name: str,
        coll_name: str,
        *,
        index_name: str | None = None,
        index_data: dict[str, dict[str, dict[str, dict[tuple[Any, ...], set[Any]]]]] | None = None,
    ) -> None:
        target_index_data = self._index_data if index_data is None else index_data
        db_index_data = target_index_data.get(db_name)
        if db_index_data is None:
            return
        coll_index_data = db_index_data.get(coll_name)
        if coll_index_data is None:
            return
        if index_name is None:
            db_index_data.pop(coll_name, None)
        else:
            coll_index_data.pop(index_name, None)
            if not coll_index_data:
                db_index_data.pop(coll_name, None)
        if not db_index_data:
            target_index_data.pop(db_name, None)

    def _namespace_exists_locked(self, db_name: str, coll_name: str) -> bool:
        return (
            coll_name in self._collections.get(db_name, set())
            or coll_name in self._storage.get(db_name, {})
            or coll_name in self._indexes.get(db_name, {})
        )

    def _collection_options_snapshot_locked(
        self,
        db_name: str,
        coll_name: str,
        *,
        collection_options: dict[str, dict[str, Document]] | None = None,
    ) -> Document:
        target_options = self._collection_options if collection_options is None else collection_options
        return deepcopy(target_options.get(db_name, {}).get(coll_name, {}))

    def _typed_engine_key(self, value: Any) -> Any:
        return bson_engine_key(value)

    def _index_value(self, document: Document, field: str) -> Any:
        values = QueryEngine.extract_values(document, field)
        if not values:
            return None

        primary = values[0]
        return self._typed_engine_key(primary)

    def _index_key(self, document: Document, fields: list[str]) -> tuple[Any, ...]:
        return tuple(self._index_value(document, field) for field in fields)

    @staticmethod
    def _is_builtin_id_index(keys: IndexKeySpec) -> bool:
        return keys == [("_id", 1)]

    def _ensure_unique_indexes(
        self,
        db_name: str,
        coll_name: str,
        candidate: Document,
        *,
        exclude_storage_key: Any | None = None,
        indexes_view: dict[str, dict[str, list[EngineIndexRecord]]] | None = None,
        storage_view: dict[str, dict[str, dict[Any, Any]]] | None = None,
        index_data_view: dict | None = None,
    ) -> None:
        indexes = (self._indexes if indexes_view is None else indexes_view).get(db_name, {}).get(coll_name, [])
        coll = (self._storage if storage_view is None else storage_view).get(db_name, {}).get(coll_name, {})
        normalized_exclude = exclude_storage_key
        if exclude_storage_key is not None and exclude_storage_key not in coll:
            normalized_exclude = self._storage_key(exclude_storage_key)

        for index in indexes:
            if not index.get("unique"):
                continue
            if not document_in_virtual_index(candidate, index):
                continue

            fields = index["fields"]
            candidate_key = self._index_key(candidate, fields)

            # Optimización: usar datos del índice si están disponibles
            index_map = (index_data_view if index_data_view is not None else self._index_data).get(db_name, {}).get(coll_name, {}).get(index["name"])
            if index_map is not None:
                if candidate_key in index_map:
                    matches = index_map[candidate_key]
                    if normalized_exclude is None or any(m != normalized_exclude for m in matches):
                        raise DuplicateKeyError(
                            f"Duplicate key for unique index '{index['name']}': {fields}={candidate_key!r}"
                        )
                continue

            # Caída a escaneo lento (solo si no hay datos de índice)
            for storage_key, data in coll.items():
                if normalized_exclude is not None and storage_key == normalized_exclude:
                    continue
                existing = self._borrow_storage_document(data)
                if not document_in_virtual_index(existing, index):
                    continue
                existing_key = self._index_key(existing, fields)
                if existing_key == candidate_key:
                    raise DuplicateKeyError(
                        f"Duplicate key for unique index '{index['name']}': {fields}={candidate_key!r}"
                    )

    @override
    async def connect(self) -> None:
        with self._meta_lock:
            self._connection_count += 1

    @override
    async def disconnect(self) -> None:
        with self._meta_lock:
            if self._connection_count == 0:
                return
            self._connection_count -= 1
            if self._connection_count != 0:
                return
            self._storage.clear()
            self._indexes.clear()
            self._index_data.clear()
            self._search_indexes.clear()
            self._search_index_ready_at.clear()
            self._clear_search_runtime_cache()
            self._collections.clear()
            self._collection_options.clear()
            self._locks.clear()
            self._mvcc_states.clear()

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
        results = await self.put_documents_bulk(
            db_name,
            coll_name,
            [document],
            overwrite=overwrite,
            context=context,
            bypass_document_validation=bypass_document_validation,
        )
        return results[0]

    async def put_documents_bulk(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        overwrite: bool = True,
        *,
        context: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ) -> list[bool]:
        async with self._get_lock(db_name, coll_name):
            storage = self._storage_view(context)
            collections = self._collections_view(context)
            option_store = self._collection_options_view(context)
            indexes_view = self._indexes_view(context)
            index_data_view = self._index_data_view(context)
            self._purge_expired_documents_locked(
                db_name,
                coll_name,
                context=context,
                indexes_view=indexes_view,
                index_data_view=index_data_view,
                storage_view=storage,
            )

            with self._meta_lock:
                db = storage.setdefault(db_name, {})
                coll = db.setdefault(coll_name, {})
                self._register_collection_locked(
                    db_name,
                    coll_name,
                    collections=collections,
                    collection_options=option_store,
                )
                collection_options = self._collection_options_snapshot_locked(
                    db_name,
                    coll_name,
                    collection_options=option_store,
                )

            results: list[bool] = []
            modified_any = False
            for document in documents:
                doc_id = document.get("_id")
                storage_key = self._storage_key(doc_id)
                original_document = None
                if overwrite and storage_key in coll:
                    original_document = self._borrow_storage_document(coll[storage_key])
                if not overwrite and storage_key in coll:
                    results.append(False)
                    continue

                if not bypass_document_validation:
                    enforce_collection_document_validation(
                        document,
                        options=collection_options,
                        original_document=original_document,
                        dialect=MONGODB_DIALECT_70,
                    )

                self._ensure_unique_indexes(
                    db_name,
                    coll_name,
                    document,
                    exclude_storage_key=storage_key if overwrite else None,
                    indexes_view=indexes_view,
                    storage_view=storage,
                    index_data_view=index_data_view,
                )

                if original_document is not None:
                    self._update_indexes_locked(
                        db_name,
                        coll_name,
                        storage_key,
                        original_document,
                        action="delete",
                        index_data_view=index_data_view,
                        indexes_view=indexes_view,
                    )

                coll[storage_key] = self._encode_storage_document(document)
                self._update_indexes_locked(
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    action="insert",
                    index_data_view=index_data_view,
                    indexes_view=indexes_view,
                )
                results.append(True)
                modified_any = True
            if modified_any:
                self._invalidate_search_runtime_cache(db_name, coll_name)
            return results

    @override
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        effective_dialect = dialect or MONGODB_DIALECT_70
        if self._is_profile_namespace(coll_name):
            document = self._profiler.get_entry(db_name, doc_id)
            if document is None:
                return None
            return apply_projection(document, projection, dialect=effective_dialect)
        async with self._get_lock(db_name, coll_name):
            self._purge_expired_documents_locked(db_name, coll_name, context=context)
            storage_key = self._storage_key(doc_id)
            data = self._storage_view(context).get(db_name, {}).get(coll_name, {}).get(storage_key)
        if data is None:
            return None
        return apply_projection(
            self._copy_document_containers(self._borrow_public_storage_document(data)),
            projection,
            dialect=effective_dialect,
        )

    @override
    async def delete_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, context: ClientSession | None = None) -> bool:
        if self._is_profile_namespace(coll_name):
            return False
        async with self._get_lock(db_name, coll_name):
            coll = self._storage_view(context).get(db_name, {}).get(coll_name, {})
            indexes_view = self._indexes_view(context)
            index_data_view = self._index_data_view(context)
            self._purge_expired_documents_locked(
                db_name,
                coll_name,
                context=context,
                indexes_view=indexes_view,
                index_data_view=index_data_view,
                storage_view=self._storage_view(context),
            )
            storage_key = self._storage_key(doc_id)
            if storage_key in coll:
                document = self._borrow_storage_document(coll[storage_key])
                self._update_indexes_locked(
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    action="delete",
                    index_data_view=index_data_view,
                    indexes_view=indexes_view,
                )
                del coll[storage_key]
                self._invalidate_search_runtime_cache(db_name, coll_name)
                return True
            return False

    @override
    def scan_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        async def _scan():
            deadline = semantics.deadline
            self._record_operation_metadata(
                context,
                operation="scan_collection",
                comment=semantics.comment,
                max_time_ms=semantics.max_time_ms,
                hint=semantics.hint,
            )
            enforce_deadline(deadline)

            if self._is_profile_namespace(coll_name):
                documents = self._iter_documents_for_classic_text_query(
                    self._profile_documents(db_name),
                    indexes=[],
                    semantics=semantics,
                )
                documents = filter_documents(documents, semantics)
                documents = finalize_documents(documents, semantics)
                for document in documents:
                    enforce_deadline(deadline)
                    yield document
                return

            async with self._get_lock(db_name, coll_name):
                coll = self._storage_view(context).get(db_name, {}).get(coll_name, {})
                indexes = self._indexes_view(context).get(db_name, {}).get(coll_name, [])
                index_data = self._index_data_view(context).get(db_name, {}).get(coll_name, {})
                self._purge_expired_documents_locked(
                    db_name,
                    coll_name,
                    context=context,
                    indexes_view=self._indexes_view(context),
                    index_data_view=self._index_data_view(context),
                    storage_view=self._storage_view(context),
                )
                self._resolve_hint_index(
                    db_name,
                    coll_name,
                    semantics.hint,
                    indexes=indexes,
                    plan=semantics.query_plan,
                    dialect=semantics.dialect,
                )

                # Intento de optimización: Point lookup por _id
                from mongoeco.core.query_plan import EqualsCondition, AndCondition
                id_lookup = None
                if isinstance(semantics.query_plan, EqualsCondition) and semantics.query_plan.field == "_id":
                    id_lookup = semantics.query_plan.value
                elif isinstance(semantics.query_plan, AndCondition):
                    for clause in semantics.query_plan.clauses:
                        if isinstance(clause, EqualsCondition) and clause.field == "_id":
                            id_lookup = clause.value
                            break

                if id_lookup is not None:
                    storage_key = self._storage_key(id_lookup)
                    data = coll.get(storage_key)
                    document_source = [self._borrow_storage_document(data)] if data is not None else []
                else:
                    # Intento de optimización: Otros índices (igualdad simple)
                    target_index_match = None
                    target_key = None

                    def find_usable_index(node):
                        if isinstance(node, EqualsCondition):
                            for idx in indexes:
                                if idx["key"] == [(node.field, 1)]:
                                    return idx, self._index_key({node.field: node.value}, idx["fields"])
                        elif isinstance(node, AndCondition):
                            for clause in node.clauses:
                                idx, key = find_usable_index(clause)
                                if idx:
                                    return idx, key
                        return None, None

                    target_index_match, target_key = find_usable_index(semantics.query_plan)
                    if target_index_match and target_index_match["name"] in index_data:
                        storage_keys = index_data[target_index_match["name"]].get(target_key, set())
                        if len(storage_keys) <= 1:
                            document_source = (
                                self._borrow_storage_document(coll[sk])
                                for sk in storage_keys
                                if sk in coll
                            )
                        else:
                            document_source = (
                                self._borrow_storage_document(data)
                                for sk, data in coll.items()
                                if sk in storage_keys
                            )
                    else:
                        # Scan completo (lento)
                        enforce_deadline(deadline)
                        document_source = (
                            self._borrow_storage_document(data)
                            for data in coll.values()
                        )

                # Pipeline de procesamiento perezoso (streaming)
                document_source = self._iter_documents_for_classic_text_query(
                    document_source,
                    indexes=indexes,
                    semantics=semantics,
                )
                filtered = iter_filtered_documents(document_source, semantics)

                # Si no hay ordenación, podemos hacer streaming real y parar tras el limit.
                if not semantics.sort:
                    final_stream = stream_finalize_documents(
                        (self._copy_document_containers(DocumentCodec.to_public(document)) for document in filtered),
                        semantics,
                        emit_public_documents=False,
                    )
                    for document in final_stream:
                        enforce_deadline(deadline)
                        yield document
                    return

                # Si hay sort, ordenamos/sliceamos sobre documentos prestados y
                # solo publicamos/copamos el subconjunto final que se devuelve.
                documents = finalize_documents(
                    filtered,
                    semantics,
                    apply_sort_phase=True,
                    emit_public_documents=False,
                )

            for document in documents:
                enforce_deadline(deadline)
                yield self._copy_document_containers(DocumentCodec.to_public(document))
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
        semantics = compile_update_semantics(
            operation,
            dialect=dialect,
            selector_filter=selector_filter,
        )
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                coll = self._storage_view(context).get(db_name, {}).get(coll_name)
                indexes_view = self._indexes_view(context)
                index_data_view = self._index_data_view(context)
                storage_view = self._storage_view(context)
                collection_options = self._collection_options_snapshot_locked(
                    db_name,
                    coll_name,
                    collection_options=self._collection_options_view(context),
                )
            self._purge_expired_documents_locked(
                db_name,
                coll_name,
                context=context,
                indexes_view=indexes_view,
                index_data_view=index_data_view,
                storage_view=storage_view,
            )
            if coll is None:
                coll = {}

            for storage_key, data in list(coll.items()):
                borrowed_document = self._borrow_storage_document(data)
                if not QueryEngine.match_plan(
                    borrowed_document,
                    semantics.query_plan,
                    dialect=semantics.dialect,
                    collation=semantics.collation,
                ):
                    continue

                document = deepcopy(borrowed_document)
                original_document = deepcopy(borrowed_document)
                modified = semantics.compiled_update_plan.apply(document)
                if not bypass_document_validation:
                    enforce_collection_document_validation(
                        document,
                        options=collection_options,
                        original_document=original_document,
                        dialect=semantics.dialect,
                    )
                self._ensure_unique_indexes(
                    db_name,
                    coll_name,
                    document,
                    exclude_storage_key=storage_key,
                    indexes_view=indexes_view,
                    storage_view=storage_view,
                    index_data_view=index_data_view,
                )
                self._update_indexes_locked(
                    db_name,
                    coll_name,
                    storage_key,
                    original_document,
                    action="delete",
                    index_data_view=index_data_view,
                    indexes_view=indexes_view,
                )
                coll[storage_key] = self._encode_storage_document(document)
                self._update_indexes_locked(
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    action="insert",
                    index_data_view=index_data_view,
                    indexes_view=indexes_view,
                )
                self._invalidate_search_runtime_cache(db_name, coll_name)
                return UpdateResult(
                    matched_count=1,
                    modified_count=1 if modified else 0,
                )

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

            with self._meta_lock:
                db = self._storage_view(context).setdefault(db_name, {})
                coll = db.setdefault(coll_name, {})
                self._register_collection_locked(
                    db_name,
                    coll_name,
                    collections=self._collections_view(context),
                    collection_options=self._collection_options_view(context),
                )

            storage_key = self._storage_key(new_doc["_id"])
            if storage_key in coll:
                raise DuplicateKeyError(f"Duplicate key: _id={new_doc['_id']}")

            self._ensure_unique_indexes(
                db_name,
                coll_name,
                new_doc,
                indexes_view=indexes_view,
                storage_view=storage_view,
                index_data_view=index_data_view,
            )
            coll[storage_key] = self._encode_storage_document(new_doc)
            self._update_indexes_locked(
                db_name,
                coll_name,
                storage_key,
                new_doc,
                action="insert",
                index_data_view=index_data_view,
                indexes_view=indexes_view,
            )
            self._invalidate_search_runtime_cache(db_name, coll_name)
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
        effective_dialect = dialect or MONGODB_DIALECT_70
        query_plan = ensure_query_plan(operation.filter_spec, operation.plan, dialect=effective_dialect)
        effective_collation = normalize_collation(operation.collation)
        async with self._get_lock(db_name, coll_name):
            coll = self._storage_view(context).get(db_name, {}).get(coll_name, {})
            indexes_view = self._indexes_view(context)
            index_data_view = self._index_data_view(context)
            self._purge_expired_documents_locked(
                db_name,
                coll_name,
                context=context,
                indexes_view=indexes_view,
                index_data_view=index_data_view,
                storage_view=self._storage_view(context),
            )
            for storage_key, data in list(coll.items()):
                document = self._borrow_storage_document(data)
                if not QueryEngine.match_plan(
                    document,
                    query_plan,
                    dialect=effective_dialect,
                    collation=effective_collation,
                ):
                    continue
                self._update_indexes_locked(
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    action="delete",
                    index_data_view=index_data_view,
                    indexes_view=indexes_view,
                )
                del coll[storage_key]
                self._invalidate_search_runtime_cache(db_name, coll_name)
                return DeleteResult(deleted_count=1)
            return DeleteResult(deleted_count=0)

    @override
    async def count_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> int:
        count = 0
        async for _ in self.scan_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
        ):
            count += 1
        return count

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
        partial_filter_expression: Filter | None = None,
        expire_after_seconds: int | None = None,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
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
        if not isinstance(hidden, bool):
            raise TypeError("hidden must be a bool")
        if special_directions:
            if len(normalized_keys) != 1:
                raise OperationFailure("special index types currently require a single-field key pattern")
            if unique:
                raise OperationFailure(f"{special_directions[0]} indexes do not support unique")
        if self._is_builtin_id_index(normalized_keys):
            if (
                name not in (None, "_id_")
                or sparse
                or hidden
                or partial_filter_expression is not None
                or expire_after_seconds is not None
                or not unique
            ):
                raise OperationFailure("Conflicting index definition for '_id_'")
            return "_id_"
        if index_name == "_id_":
            raise OperationFailure("Conflicting index definition for '_id_'")
        async with self._get_lock(db_name, coll_name):
            indexes_view = self._indexes_view(context)
            index_data_view = self._index_data_view(context)
            storage_view = self._storage_view(context)
            collections = self._collections_view(context)
            option_store = self._collection_options_view(context)
            enforce_deadline(deadline)
            with self._meta_lock:
                db_indexes = indexes_view.setdefault(db_name, {})
                coll_indexes = db_indexes.setdefault(coll_name, [])
                self._register_collection_locked(
                    db_name,
                    coll_name,
                    collections=collections,
                    collection_options=option_store,
                )

            for index in coll_indexes:
                enforce_deadline(deadline)
                if index["name"] == index_name:
                    if (
                        index["key"] != normalized_keys
                        or index["unique"] != unique
                        or index.get("sparse") != sparse
                        or index.get("hidden") != hidden
                        or index.get("partial_filter_expression") != partial_filter_expression
                        or index.get("expire_after_seconds") != expire_after_seconds
                    ):
                        raise OperationFailure(
                            f"Conflicting index definition for '{index_name}'"
                        )
                    return index_name
                if index["key"] == normalized_keys:
                    if (
                        index["unique"] != unique
                        or index.get("sparse") != sparse
                        or index.get("hidden") != hidden
                        or index.get("partial_filter_expression") != partial_filter_expression
                        or index.get("expire_after_seconds") != expire_after_seconds
                    ):
                        raise OperationFailure(
                            f"Conflicting index definition for key pattern '{normalized_keys!r}'"
                        )
                    continue

            new_index = EngineIndexRecord(
                name=index_name,
                fields=fields.copy(),
                key=deepcopy(normalized_keys),
                unique=unique,
                sparse=sparse,
                hidden=hidden,
                partial_filter_expression=deepcopy(partial_filter_expression),
                expire_after_seconds=expire_after_seconds,
            )

            if unique:
                seen: set[tuple[Any, ...]] = set()
                coll = storage_view.get(db_name, {}).get(coll_name, {})
                for data in coll.values():
                    enforce_deadline(deadline)
                    document = self._borrow_storage_document(data)
                    if not document_in_virtual_index(document, new_index):
                        continue
                    key = self._index_key(document, fields)
                    if key in seen:
                        raise DuplicateKeyError(
                            f"Duplicate key for unique index '{index_name}': {fields}={key!r}"
                        )
                    seen.add(key)

            enforce_deadline(deadline)
            coll_indexes.append(new_index)
            if is_ordered_index_spec(normalized_keys):
                self._rebuild_index_data_locked(
                    db_name,
                    coll_name,
                    new_index,
                    index_data_view=index_data_view,
                    storage_view=storage_view,
                )
            self._purge_expired_documents_locked(
                db_name,
                coll_name,
                context=context,
                indexes_view=indexes_view,
                index_data_view=index_data_view,
                storage_view=storage_view,
            )
        return index_name

    @override
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[IndexDocument]:
        async with self._get_lock(db_name, coll_name):
            indexes = self._indexes_view(context).get(db_name, {}).get(coll_name, [])
        result = [default_id_index_definition().to_list_document()]
        result.extend(
            index.to_definition().to_list_document()
            for index in deepcopy(indexes)
        )
        return result

    @override
    async def index_information(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> IndexInformation:
        async with self._get_lock(db_name, coll_name):
            indexes = deepcopy(self._indexes_view(context).get(db_name, {}).get(coll_name, []))
        result = default_id_index_information()
        result.update(
            {
                index.name: index.to_definition().to_information_entry()
                for index in indexes
            }
        )
        return result

    @override
    async def drop_index(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        *,
        context: ClientSession | None = None,
    ) -> None:
        async with self._get_lock(db_name, coll_name):
            indexes_view = self._indexes_view(context)
            index_data_view = self._index_data_view(context)
            indexes = indexes_view.get(db_name, {}).get(coll_name, [])
            normalized_keys: IndexKeySpec | None = None
            target_name: str | None = None
            if isinstance(index_or_name, str):
                if index_or_name == "_id_":
                    raise OperationFailure("cannot drop _id index")
                target_name = index_or_name
            else:
                normalized_keys = normalize_index_keys(index_or_name)
                if self._is_builtin_id_index(normalized_keys):
                    raise OperationFailure("cannot drop _id index")
                matches = [index for index in indexes if index["key"] == normalized_keys]
                if not matches:
                    raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")
                if len(matches) > 1:
                    raise OperationFailure(
                        f"multiple indexes found with key pattern {normalized_keys!r}; drop by name instead"
                    )
                target_name = str(matches[0]["name"])
            for idx, index in enumerate(indexes):
                if index["name"] == target_name:
                    del indexes[idx]
                    self._prune_index_data_locked(
                        db_name,
                        coll_name,
                        index_name=target_name,
                        index_data=index_data_view,
                    )
                    break
            else:
                if isinstance(index_or_name, str):
                    raise OperationFailure(f"index not found with name [{index_or_name}]")
                raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")

            if db_name in indexes_view and coll_name in indexes_view[db_name] and not indexes_view[db_name][coll_name]:
                del indexes_view[db_name][coll_name]
                if not indexes_view[db_name]:
                    del indexes_view[db_name]

    async def drop_database(
        self,
        db_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        lock_names: list[str] = []
        storage = self._storage_view(context)
        indexes = self._indexes_view(context)
        search_indexes = self._search_indexes_view(context)
        collections = self._collections_view(context)
        options = self._collection_options_view(context)
        with self._meta_lock:
            lock_names = sorted(
                set(storage.get(db_name, {}).keys())
                | set(indexes.get(db_name, {}).keys())
                | set(search_indexes.get(db_name, {}).keys())
                | set(collections.get(db_name, set()))
                | set(options.get(db_name, {}).keys())
            )
        async with AsyncExitStack() as stack:
            for coll_name in lock_names:
                await stack.enter_async_context(self._get_lock(db_name, coll_name))
            storage = self._storage_view(context)
            indexes = self._indexes_view(context)
            index_data = self._index_data_view(context)
            search_indexes = self._search_indexes_view(context)
            collections = self._collections_view(context)
            options = self._collection_options_view(context)
            with self._meta_lock:
                storage.pop(db_name, None)
                indexes.pop(db_name, None)
                index_data.pop(db_name, None)
                search_indexes.pop(db_name, None)
                collections.pop(db_name, None)
                options.pop(db_name, None)
                ready_keys = [key for key in self._search_index_ready_at if key[0] == db_name]
                for key in ready_keys:
                    del self._search_index_ready_at[key]
                stale_cache_keys = [
                    key for key in self._search_document_cache if key[0] == db_name
                ]
                for key in stale_cache_keys:
                    del self._search_document_cache[key]
                stale_vector_cache_keys = [
                    key for key in self._vector_document_cache if key[0] == db_name
                ]
                for key in stale_vector_cache_keys:
                    del self._vector_document_cache[key]
        self._profiler.clear(db_name)

    @override
    async def drop_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        async with self._get_lock(db_name, coll_name):
            indexes_view = self._indexes_view(context)
            index_data_view = self._index_data_view(context)
            if db_name in indexes_view and coll_name in indexes_view[db_name]:
                del indexes_view[db_name][coll_name]
                if not indexes_view[db_name]:
                    del indexes_view[db_name]
            self._prune_index_data_locked(
                db_name,
                coll_name,
                index_data=index_data_view,
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
        deadline = operation_deadline(max_time_ms)
        normalized_definition = normalize_search_index_definition(definition)
        async with self._get_lock(db_name, coll_name):
            search_indexes = self._search_indexes_view(context)
            collections = self._collections_view(context)
            option_store = self._collection_options_view(context)
            enforce_deadline(deadline)
            with self._meta_lock:
                self._register_collection_locked(
                    db_name,
                    coll_name,
                    collections=collections,
                    collection_options=option_store,
                )
                db_search_indexes = search_indexes.setdefault(db_name, {})
                coll_search_indexes = db_search_indexes.setdefault(coll_name, [])
                for existing in coll_search_indexes:
                    if existing.name == normalized_definition.name:
                        if existing != normalized_definition:
                            raise OperationFailure(
                                f"Conflicting search index definition for '{normalized_definition.name}'"
                            )
                        return normalized_definition.name
                coll_search_indexes.append(normalized_definition)
                self._mark_search_index_pending(db_name, coll_name, normalized_definition.name)
                self._invalidate_search_runtime_cache(
                    db_name,
                    coll_name,
                    index_name=normalized_definition.name,
                )
                return normalized_definition.name

    @override
    async def list_search_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        name: str | None = None,
        context: ClientSession | None = None,
    ) -> list[SearchIndexDocument]:
        async with self._get_lock(db_name, coll_name):
            indexes = deepcopy(
                self._search_indexes_view(context).get(db_name, {}).get(coll_name, [])
            )
        return build_search_index_documents(
            indexes,
            get_name=lambda index: index.name,
            get_definition=lambda index: index,
            get_ready_at_epoch=lambda index: self._search_index_ready_at.get((db_name, coll_name, index.name)),
            is_ready=lambda ready_at_epoch: ready_at_epoch is None or ready_at_epoch <= time.time(),
            name=name,
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
        deadline = operation_deadline(max_time_ms)
        async with self._get_lock(db_name, coll_name):
            search_indexes = self._search_indexes_view(context)
            coll_search_indexes = search_indexes.get(db_name, {}).get(coll_name, [])
            enforce_deadline(deadline)
            for idx, existing in enumerate(coll_search_indexes):
                if existing.name == name:
                    normalized_definition = validate_search_index_definition(
                        definition,
                        index_type=existing.index_type,
                    )
                    coll_search_indexes[idx] = SearchIndexDefinition(
                        normalized_definition,
                        name=name,
                        index_type=existing.index_type,
                    )
                    self._mark_search_index_pending(db_name, coll_name, name)
                    self._invalidate_search_runtime_cache(
                        db_name,
                        coll_name,
                        index_name=name,
                    )
                    return
        raise OperationFailure(f"search index not found with name [{name}]")

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
        deadline = operation_deadline(max_time_ms)
        async with self._get_lock(db_name, coll_name):
            search_indexes = self._search_indexes_view(context)
            coll_search_indexes = search_indexes.get(db_name, {}).get(coll_name, [])
            enforce_deadline(deadline)
            for idx, existing in enumerate(coll_search_indexes):
                if existing.name == name:
                    del coll_search_indexes[idx]
                    self._search_index_ready_at.pop((db_name, coll_name, name), None)
                    self._invalidate_search_runtime_cache(
                        db_name,
                        coll_name,
                        index_name=name,
                    )
                    if not coll_search_indexes:
                        del search_indexes[db_name][coll_name]
                    if not search_indexes[db_name]:
                        del search_indexes[db_name]
                    return
        raise OperationFailure(f"search index not found with name [{name}]")

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
        deadline = operation_deadline(max_time_ms)
        result = await _execute_memory_search_documents(
            self,
            db_name,
            coll_name,
            operator,
            spec,
            context=context,
            result_limit_hint=result_limit_hint,
            downstream_filter_spec=downstream_filter_spec,
        )
        enforce_deadline(deadline)
        return result

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
        return await _explain_memory_search_documents(
            self,
            db_name,
            coll_name,
            operator,
            spec,
            max_time_ms=max_time_ms,
            context=context,
            result_limit_hint=result_limit_hint,
            downstream_filter_spec=downstream_filter_spec,
        )

    def _iter_documents_for_classic_text_query(
        self,
        documents: Any,
        *,
        indexes: list[EngineIndexRecord],
        semantics: EngineFindSemantics,
    ):
        text_query = semantics.text_query
        if text_query is None:
            return documents
        _index_name, field = resolve_classic_text_index(indexes)

        def _iter():
            for document in documents:
                score = classic_text_score(document, field=field, query=text_query)
                if score is None:
                    continue
                yield attach_text_score(document, score)

        return _iter()

    @override
    async def explain_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation:
        deadline = semantics.deadline
        self._record_operation_metadata(
            context,
            operation="explain_query_plan",
            comment=semantics.comment,
            max_time_ms=semantics.max_time_ms,
            hint=semantics.hint,
        )
        enforce_deadline(deadline)
        async with self._get_lock(db_name, coll_name):
            indexes = deepcopy(self._indexes.get(db_name, {}).get(coll_name, []))
        hinted_index = self._resolve_hint_index(
            db_name,
            coll_name,
            semantics.hint,
            indexes=indexes,
            plan=semantics.query_plan,
            dialect=semantics.dialect,
        )
        enforce_deadline(deadline)
        execution_plan = await self.plan_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
        )
        details = describe_virtual_index_usage(
            indexes,
            semantics.query_plan,
            hinted_index_name=None if hinted_index is None else hinted_index["name"],
            dialect=semantics.dialect,
        )
        if semantics.text_query is not None:
            text_index_name, text_field = resolve_classic_text_index(indexes)
            text_details = {
                "textQuery": {
                    "backend": "python",
                    "index": text_index_name,
                    "field": text_field,
                    "rawQuery": semantics.text_query.raw_query,
                    "terms": list(semantics.text_query.terms),
                    "tokenizer": "lowercase+punctuation-split",
                    "caseSensitive": False,
                    "diacriticSensitive": False,
                }
            }
            details = {**details, **text_details} if isinstance(details, dict) else text_details
        return build_query_plan_explanation(
            engine="memory",
            strategy=execution_plan.strategy,
            semantics=semantics,
            details=details,
            hinted_index=None if hinted_index is None else hinted_index["name"],
            indexes=indexes,
            execution_lineage=execution_plan.execution_lineage,
            physical_plan=execution_plan.physical_plan,
            fallback_reason=execution_plan.fallback_reason,
        )

    @override
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

    @override
    async def plan_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> EngineReadExecutionPlan:
        del db_name, coll_name, context
        lineage = [
            ExecutionLineageStep(runtime="python", phase="scan", detail="engine scan"),
        ]
        if semantics.text_query is not None:
            lineage.append(ExecutionLineageStep(runtime="python", phase="text", detail="classic text filter"))
        lineage.append(ExecutionLineageStep(runtime="python", phase="filter", detail="semantic core"))
        if semantics.sort:
            lineage.append(ExecutionLineageStep(runtime="python", phase="sort", detail="semantic core"))
        if semantics.projection is not None:
            lineage.append(ExecutionLineageStep(runtime="python", phase="project", detail="semantic core"))
        if semantics.skip or semantics.limit is not None:
            lineage.append(ExecutionLineageStep(runtime="python", phase="slice", detail="semantic core"))
        return EngineReadExecutionPlan(
            semantics=semantics,
            strategy="python-text" if semantics.text_query is not None else "python",
            execution_lineage=tuple(lineage),
            physical_plan=(
                PhysicalPlanStep(runtime="python", operation="scan"),
                *(
                    (PhysicalPlanStep(runtime="python", operation="text"),)
                    if semantics.text_query is not None
                    else ()
                ),
                PhysicalPlanStep(runtime="python", operation="filter"),
                *(
                    (PhysicalPlanStep(runtime="python", operation="sort"),)
                    if semantics.sort
                    else ()
                ),
                *(
                    (PhysicalPlanStep(runtime="python", operation="project"),)
                    if semantics.projection is not None
                    else ()
                ),
                *(
                    (PhysicalPlanStep(runtime="python", operation="slice"),)
                    if semantics.skip or semantics.limit is not None
                    else ()
                ),
            ),
        )

    @override
    async def list_databases(self, *, context: ClientSession | None = None) -> list[str]:
        storage = self._storage_view(context)
        indexes = self._indexes_view(context)
        search_indexes = self._search_indexes_view(context)
        collections = self._collections_view(context)
        options = self._collection_options_view(context)
        with self._meta_lock:
            names = namespace_database_names(
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
                options=options,
            )
        return merge_profile_database_names(names, self._profiler)

    @override
    async def list_collections(self, db_name: str, *, context: ClientSession | None = None) -> list[str]:
        storage = self._storage_view(context)
        indexes = self._indexes_view(context)
        search_indexes = self._search_indexes_view(context)
        collections = self._collections_view(context)
        with self._meta_lock:
            names = namespace_collection_names(
                db_name,
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
            )
        return merge_profile_collection_names(
            names,
            db_name=db_name,
            profiler=self._profiler,
            profile_collection_name=self._PROFILE_COLLECTION_NAME,
        )

    @override
    async def collection_options(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        collections = self._collections_view(context)
        storage = self._storage_view(context)
        indexes = self._indexes_view(context)
        search_indexes = self._search_indexes_view(context)
        options = self._collection_options_view(context)
        with self._meta_lock:
            return namespace_collection_options(
                db_name,
                coll_name,
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
                options=options,
                profiler=self._profiler,
                profile_collection_name=self._PROFILE_COLLECTION_NAME,
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
        async with self._get_lock(db_name, coll_name):
            collections = self._collections_view(context)
            storage = self._storage_view(context)
            indexes = self._indexes_view(context)
            search_indexes = self._search_indexes_view(context)
            option_store = self._collection_options_view(context)
            with self._meta_lock:
                if (
                    coll_name in collections.get(db_name, set())
                    or coll_name in storage.get(db_name, {})
                    or coll_name in indexes.get(db_name, {})
                    or coll_name in search_indexes.get(db_name, {})
                ):
                    raise CollectionInvalid(f"collection '{coll_name}' already exists")
                self._register_collection_locked(
                    db_name,
                    coll_name,
                    options=deepcopy(options or {}),
                    collections=collections,
                    collection_options=option_store,
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
        if coll_name == new_name:
            raise CollectionInvalid("collection names must differ")
        lock_names = sorted({coll_name, new_name})
        async with AsyncExitStack() as stack:
            for name in lock_names:
                await stack.enter_async_context(self._get_lock(db_name, name))
            storage = self._storage_view(context)
            indexes = self._indexes_view(context)
            index_data = self._index_data_view(context)
            search_indexes = self._search_indexes_view(context)
            collections = self._collections_view(context)
            option_store = self._collection_options_view(context)
            with self._meta_lock:
                if (
                    coll_name not in collections.get(db_name, set())
                    and coll_name not in storage.get(db_name, {})
                    and coll_name not in indexes.get(db_name, {})
                    and coll_name not in search_indexes.get(db_name, {})
                ):
                    raise CollectionInvalid(f"collection '{coll_name}' does not exist")
                if (
                    new_name in collections.get(db_name, set())
                    or new_name in storage.get(db_name, {})
                    or new_name in indexes.get(db_name, {})
                    or new_name in search_indexes.get(db_name, {})
                ):
                    raise CollectionInvalid(f"collection '{new_name}' already exists")

                db_storage = storage.get(db_name)
                if db_storage is not None and coll_name in db_storage:
                    db_storage[new_name] = db_storage.pop(coll_name)

                db_indexes = indexes.get(db_name)
                if db_indexes is not None and coll_name in db_indexes:
                    db_indexes[new_name] = db_indexes.pop(coll_name)

                db_index_data = index_data.get(db_name)
                if db_index_data is not None and coll_name in db_index_data:
                    db_index_data[new_name] = db_index_data.pop(coll_name)

                db_search_indexes = search_indexes.get(db_name)
                if db_search_indexes is not None and coll_name in db_search_indexes:
                    db_search_indexes[new_name] = db_search_indexes.pop(coll_name)
                ready_at = self._search_index_ready_at
                for key in [key for key in ready_at if key[0] == db_name and key[1] == coll_name]:
                    ready_at[(db_name, new_name, key[2])] = ready_at.pop(key)
                stale_cache_keys = [
                    key
                    for key in self._search_document_cache
                    if key[0] == db_name and key[1] == coll_name
                ]
                for key in stale_cache_keys:
                    self._search_document_cache[(db_name, new_name, key[2])] = self._search_document_cache.pop(key)
                stale_vector_cache_keys = [
                    key
                    for key in self._vector_document_cache
                    if key[0] == db_name and key[1] == coll_name
                ]
                for key in stale_vector_cache_keys:
                    self._vector_document_cache[(db_name, new_name, key[2])] = self._vector_document_cache.pop(key)

                db_options = option_store.get(db_name)
                if db_options is not None and coll_name in db_options:
                    db_options[new_name] = db_options.pop(coll_name)

                self._prune_collection_registry_locked(
                    db_name,
                    coll_name,
                    collections=collections,
                    collection_options=option_store,
                )
                self._register_collection_locked(
                    db_name,
                    new_name,
                    options=deepcopy(
                        option_store.get(db_name, {}).get(new_name, {})
                    ),
                    collections=collections,
                    collection_options=option_store,
                )

    @override
    async def drop_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        if self._is_profile_namespace(coll_name):
            self._profiler.clear(db_name)
            return
        async with self._get_lock(db_name, coll_name):
            storage = self._storage_view(context)
            indexes = self._indexes_view(context)
            index_data = self._index_data_view(context)
            search_indexes = self._search_indexes_view(context)
            collections = self._collections_view(context)
            option_store = self._collection_options_view(context)
            with self._meta_lock:
                self._prune_collection_registry_locked(
                    db_name,
                    coll_name,
                    collections=collections,
                    collection_options=option_store,
                )
                if db_name in storage and coll_name in storage[db_name]:
                    del storage[db_name][coll_name]
                    if not storage[db_name]:
                        del storage[db_name]
                if db_name in indexes and coll_name in indexes[db_name]:
                    del indexes[db_name][coll_name]
                    if not indexes[db_name]:
                        del indexes[db_name]
                self._prune_index_data_locked(
                    db_name,
                    coll_name,
                    index_data=index_data,
                )
                if db_name in search_indexes and coll_name in search_indexes[db_name]:
                    del search_indexes[db_name][coll_name]
                    if not search_indexes[db_name]:
                        del search_indexes[db_name]
                ready_at = self._search_index_ready_at
                for key in [key for key in ready_at if key[0] == db_name and key[1] == coll_name]:
                    del ready_at[key]
                self._invalidate_search_runtime_cache(db_name, coll_name)
