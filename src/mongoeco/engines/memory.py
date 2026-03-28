import asyncio
from contextlib import AsyncExitStack
import datetime
import threading
import time
import uuid
from copy import deepcopy
from typing import Any, AsyncIterable, override

from mongoeco.api.operations import FindOperation, UpdateOperation, compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_ordering import bson_engine_key
from mongoeco.core.collation import normalize_collation
from mongoeco.engines.base import AsyncStorageEngine
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
    finalize_documents,
)
from mongoeco.engines.virtual_indexes import (
    describe_virtual_index_usage,
    document_in_virtual_index,
    normalize_partial_filter_expression,
    query_can_use_index,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.projections import apply_projection
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.query_plan import QueryNode, ensure_query_plan
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.search import (
    build_search_index_document,
    compile_search_stage,
    matches_search_text_query,
    validate_search_index_definition,
)
from mongoeco.core.aggregation import AggregationSpillPolicy
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.session import EngineTransactionContext
from mongoeco.types import (
    ArrayFilters, CollationDocument, DeleteResult, Document, DocumentId, ExecutionLineageStep, Filter, IndexInformation, IndexDocument, IndexKeySpec, ObjectId,
    ProfilingCommandResult,
    Projection, QueryPlanExplanation, SortSpec, Update, UpdateResult, default_index_name,
    default_id_index_definition, default_id_index_document, default_id_index_information, index_fields,
    EngineIndexRecord, SearchIndexDefinition, SearchIndexDocument, normalize_index_keys,
)


class _AsyncThreadLock:
    """Lock compatible con hilos que no bloquea directamente el event loop al adquirir."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    async def __aenter__(self) -> "_AsyncThreadLock":
        await asyncio.to_thread(self._lock.acquire)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._lock.release()
        return False


class MemoryEngine(AsyncStorageEngine):
    """Motor de almacenamiento en memoria ultra-rápido."""

    _PROFILE_COLLECTION_NAME = "system.profile"

    def __init__(
        self,
        codec: type[DocumentCodec] = DocumentCodec,
        *,
        aggregation_spill_threshold: int | None = None,
    ):
        self._storage: dict[str, dict[str, dict[Any, Any]]] = {}
        self._locks: dict[str, _AsyncThreadLock] = {}
        self._indexes: dict[str, dict[str, list[EngineIndexRecord]]] = {}
        self._search_indexes: dict[str, dict[str, list[SearchIndexDefinition]]] = {}
        self._collections: dict[str, set[str]] = {}
        self._collection_options: dict[str, dict[str, Document]] = {}
        self._meta_lock = threading.Lock()
        self._connection_count = 0
        self._codec = codec
        self._profiler = EngineProfiler("memory")
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

    def _decode_storage_document(
        self,
        payload: object,
        *,
        preserve_bson_wrappers: bool = True,
    ) -> Document:
        try:
            return self._codec.decode(payload, preserve_bson_wrappers=preserve_bson_wrappers)
        except TypeError:
            return self._codec.decode(payload)

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
                self._storage = snapshot.storage
                self._indexes = snapshot.indexes
                self._search_indexes = snapshot.search_indexes
                self._collections = snapshot.collections
                self._collection_options = snapshot.collection_options
                self._mvcc_version += 1
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

    def _collection_options_view(self, context: ClientSession | None) -> dict[str, dict[str, Document]]:
        state = self._active_mvcc_state(context)
        return self._collection_options if state is None else state.collection_options

    def _lock_key(self, db: str, coll: str) -> str:
        return f"{db}.{coll}"

    def _get_lock(self, db: str, coll: str) -> _AsyncThreadLock:
        key = f"{db}.{coll}"
        with self._meta_lock:
            return self._locks.setdefault(key, _AsyncThreadLock())

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
                    if plan is not None and not query_can_use_index(index, plan, dialect=dialect):
                        raise OperationFailure("hint does not correspond to a usable index for this query")
                    return deepcopy(index)
            else:
                if index["key"] == normalized_hint:
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
            for storage_key, data in coll.items():
                if normalized_exclude is not None and storage_key == normalized_exclude:
                    continue
                existing = self._decode_storage_document(data)
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
            self._search_indexes.clear()
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
        async with self._get_lock(db_name, coll_name):
            storage = self._storage_view(context)
            collections = self._collections_view(context)
            option_store = self._collection_options_view(context)
            indexes_view = self._indexes_view(context)
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

            doc_id = document.get("_id")
            storage_key = self._storage_key(doc_id)
            original_document = None
            if overwrite and storage_key in coll:
                original_document = self._decode_storage_document(coll[storage_key])
            if not overwrite and storage_key in coll:
                return False

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
            )

            coll[storage_key] = self._codec.encode(document)
            return True

    @override
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        effective_dialect = dialect or MONGODB_DIALECT_70
        if self._is_profile_namespace(coll_name):
            document = self._profiler.get_entry(db_name, doc_id)
            if document is None:
                return None
            return apply_projection(document, projection, dialect=effective_dialect)
        async with self._get_lock(db_name, coll_name):
            storage_key = self._storage_key(doc_id)
            data = self._storage_view(context).get(db_name, {}).get(coll_name, {}).get(storage_key)
        if data is None:
            return None
        return apply_projection(
            DocumentCodec.to_public(self._decode_storage_document(data)),
            projection,
            dialect=effective_dialect,
        )

    @override
    async def delete_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, context: ClientSession | None = None) -> bool:
        if self._is_profile_namespace(coll_name):
            return False
        async with self._get_lock(db_name, coll_name):
            coll = self._storage_view(context).get(db_name, {}).get(coll_name, {})
            storage_key = self._storage_key(doc_id)
            if storage_key in coll:
                del coll[storage_key]
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
                documents = filter_documents(self._profile_documents(db_name), semantics)
                documents = finalize_documents(documents, semantics)
                for document in documents:
                    enforce_deadline(deadline)
                    yield document
                return

            async with self._get_lock(db_name, coll_name):
                coll = self._storage_view(context).get(db_name, {}).get(coll_name, {})
                indexes = deepcopy(self._indexes_view(context).get(db_name, {}).get(coll_name, []))
                self._resolve_hint_index(
                    db_name,
                    coll_name,
                    semantics.hint,
                    indexes=indexes,
                    plan=semantics.query_plan,
                    dialect=semantics.dialect,
                )
                enforce_deadline(deadline)
                documents = [
                    self._decode_storage_document(data)
                    for data in list(coll.values())
                ]

            documents = filter_documents(documents, semantics)
            documents = finalize_documents(documents, semantics)

            for document in documents:
                enforce_deadline(deadline)
                yield document
        return _scan()

    @override
    def scan_collection(self, db_name: str, coll_name: str, filter_spec: Filter | None = None, *, plan: QueryNode | None = None, projection: Projection | None = None, collation: CollationDocument | None = None, sort: SortSpec | None = None, skip: int = 0, limit: int | None = None, hint: str | IndexKeySpec | None = None, comment: object | None = None, max_time_ms: int | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> AsyncIterable[Document]:
        semantics = compile_find_semantics(
            filter_spec,
            plan=plan,
            projection=projection,
            collation=collation,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            dialect=dialect,
        )
        return self.scan_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
        )

    @override
    def scan_find_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        return self.scan_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics_from_operation(operation, dialect=dialect),
            context=context,
        )

    @override
    async def update_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, update_spec: Update, upsert: bool = False, upsert_seed: Document | None = None, *, selector_filter: Filter | None = None, array_filters: ArrayFilters | None = None, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None, bypass_document_validation: bool = False) -> UpdateResult[DocumentId]:
        operation = compile_update_operation(
            filter_spec,
            array_filters=array_filters,
            dialect=dialect or MONGODB_DIALECT_70,
            plan=plan,
            update_spec=update_spec,
        )
        return await self.update_with_operation(
            db_name,
            coll_name,
            operation,
            upsert=upsert,
            upsert_seed=upsert_seed,
            selector_filter=selector_filter,
            dialect=dialect,
            context=context,
            bypass_document_validation=bypass_document_validation,
        )

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
                collection_options = self._collection_options_snapshot_locked(
                    db_name,
                    coll_name,
                    collection_options=self._collection_options_view(context),
                )
            if coll is None:
                coll = {}

            for storage_key, data in list(coll.items()):
                document = self._decode_storage_document(data)
                if not QueryEngine.match_plan(
                    document,
                    semantics.query_plan,
                    dialect=semantics.dialect,
                    collation=semantics.collation,
                ):
                    continue

                original_document = deepcopy(document)
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
                    indexes_view=self._indexes_view(context),
                    storage_view=self._storage_view(context),
                )
                coll[storage_key] = self._codec.encode(document)
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
                indexes_view=self._indexes_view(context),
                storage_view=self._storage_view(context),
            )
            coll[storage_key] = self._codec.encode(new_doc)
            return UpdateResult(
                matched_count=0,
                modified_count=0,
                upserted_id=new_doc["_id"],
            )

    @override
    async def delete_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, collation: CollationDocument | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> DeleteResult:
        effective_dialect = dialect or MONGODB_DIALECT_70
        query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        async with self._get_lock(db_name, coll_name):
            coll = self._storage_view(context).get(db_name, {}).get(coll_name, {})
            for storage_key, data in list(coll.items()):
                document = self._decode_storage_document(data)
                if not QueryEngine.match_plan(
                    document,
                    query_plan,
                    dialect=effective_dialect,
                    collation=normalize_collation(collation),
                ):
                    continue
                del coll[storage_key]
                return DeleteResult(deleted_count=1)
            return DeleteResult(deleted_count=0)

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
        return await self.delete_matching_document(
            db_name,
            coll_name,
            operation.filter_spec,
            plan=operation.plan,
            collation=operation.collation,
            dialect=dialect,
            context=context,
        )

    @override
    async def count_matching_documents(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, collation: CollationDocument | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> int:
        semantics = compile_find_semantics(
            filter_spec,
            plan=plan,
            collation=collation,
            dialect=dialect,
        )
        return await self.count_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
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
    async def count_find_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> int:
        return await self.count_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics_from_operation(operation, dialect=dialect),
            context=context,
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
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> str:
        normalized_keys = normalize_index_keys(keys)
        partial_filter_expression = normalize_partial_filter_expression(partial_filter_expression)
        fields = index_fields(normalized_keys)
        index_name = name or default_index_name(normalized_keys)
        deadline = operation_deadline(max_time_ms)
        if self._is_builtin_id_index(normalized_keys):
            if name not in (None, "_id_") or sparse or partial_filter_expression is not None or not unique:
                raise OperationFailure("Conflicting index definition for '_id_'")
            return "_id_"
        if index_name == "_id_":
            raise OperationFailure("Conflicting index definition for '_id_'")
        async with self._get_lock(db_name, coll_name):
            indexes_view = self._indexes_view(context)
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
                        or index.get("partial_filter_expression") != partial_filter_expression
                    ):
                        raise OperationFailure(
                            f"Conflicting index definition for '{index_name}'"
                        )
                    return index_name
                if index["key"] == normalized_keys:
                    if (
                        index["unique"] != unique
                        or index.get("sparse") != sparse
                        or index.get("partial_filter_expression") != partial_filter_expression
                    ):
                        raise OperationFailure(
                            f"Conflicting index definition for key pattern '{normalized_keys!r}'"
                        )
                    return index["name"]

            if unique:
                seen: set[tuple[Any, ...]] = set()
                coll = storage_view.get(db_name, {}).get(coll_name, {})
                candidate_index = EngineIndexRecord(
                    name=index_name,
                    fields=fields.copy(),
                    key=deepcopy(normalized_keys),
                    unique=unique,
                    sparse=sparse,
                    partial_filter_expression=deepcopy(partial_filter_expression),
                )
                for data in coll.values():
                    enforce_deadline(deadline)
                    document = self._decode_storage_document(data)
                    if not document_in_virtual_index(document, candidate_index):
                        continue
                    key = self._index_key(document, fields)
                    if key in seen:
                        raise DuplicateKeyError(
                            f"Duplicate key for unique index '{index_name}': {fields}={key!r}"
                        )
                    seen.add(key)

            enforce_deadline(deadline)
            coll_indexes.append(
                EngineIndexRecord(
                    name=index_name,
                    fields=fields.copy(),
                    key=deepcopy(normalized_keys),
                    unique=unique,
                    sparse=sparse,
                    partial_filter_expression=deepcopy(partial_filter_expression),
                )
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
        target_name: str | None = None
        if isinstance(index_or_name, str):
            if index_or_name == "_id_":
                raise OperationFailure("cannot drop _id index")
            target_name = index_or_name
        else:
            normalized_keys = normalize_index_keys(index_or_name)
            if self._is_builtin_id_index(normalized_keys):
                raise OperationFailure("cannot drop _id index")
            target_name = default_index_name(normalized_keys)
        async with self._get_lock(db_name, coll_name):
            indexes_view = self._indexes_view(context)
            indexes = indexes_view.get(db_name, {}).get(coll_name, [])
            for idx, index in enumerate(indexes):
                if index["name"] == target_name:
                    del indexes[idx]
                    break
            else:
                if isinstance(index_or_name, str):
                    raise OperationFailure(f"index not found with name [{index_or_name}]")
                raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")

            if db_name in indexes_view and coll_name in indexes_view[db_name] and not indexes_view[db_name][coll_name]:
                del indexes_view[db_name][coll_name]
                if not indexes_view[db_name]:
                    del indexes_view[db_name]

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
            if db_name in indexes_view and coll_name in indexes_view[db_name]:
                del indexes_view[db_name][coll_name]
                if not indexes_view[db_name]:
                    del indexes_view[db_name]

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
        normalized_definition = SearchIndexDefinition(
            validate_search_index_definition(
                definition.definition,
                index_type=definition.index_type,
            ),
            name=definition.name,
            index_type=definition.index_type,
        )
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
        documents = [
            build_search_index_document(index)
            for index in sorted(indexes, key=lambda item: item.name)
        ]
        if name is None:
            return documents
        return [document for document in documents if document["name"] == name]

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
    ) -> list[Document]:
        deadline = operation_deadline(max_time_ms)
        query = compile_search_stage(operator, spec)
        async with self._get_lock(db_name, coll_name):
            indexes = deepcopy(
                self._search_indexes_view(context).get(db_name, {}).get(coll_name, [])
            )
            definition = next((item for item in indexes if item.name == query.index_name), None)
            if definition is None:
                raise OperationFailure(f"search index not found with name [{query.index_name}]")
            documents = [
                self._decode_storage_document(payload)
                for payload in self._storage_view(context).get(db_name, {}).get(coll_name, {}).values()
            ]
        enforce_deadline(deadline)
        return [
            document
            for document in documents
            if matches_search_text_query(
                document,
                definition=definition,
                query=query,
            )
        ]

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
        query = compile_search_stage(operator, spec)
        async with self._get_lock(db_name, coll_name):
            indexes = deepcopy(
                self._search_indexes_view(context).get(db_name, {}).get(coll_name, [])
            )
        definition = next((item for item in indexes if item.name == query.index_name), None)
        if definition is None:
            raise OperationFailure(f"search index not found with name [{query.index_name}]")
        return QueryPlanExplanation(
            engine="memory",
            strategy="search",
            plan="python-search-scan",
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
                "backend": "python",
                "definition": build_search_index_document(definition),
            },
        )

    @override
    async def explain_query_plan(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter | None = None,
        *,
        plan: QueryNode | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: str | IndexKeySpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation:
        semantics = compile_find_semantics(
            filter_spec,
            plan=plan,
            collation=collation,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            dialect=dialect,
        )
        return await self.explain_find_semantics(
            db_name,
            coll_name,
            semantics,
            context=context,
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
        execution_plan = await self.plan_find_execution(
            db_name,
            coll_name,
            FindOperation(
                filter_spec=semantics.filter_spec or {},
                projection=semantics.projection,
                collation=None if semantics.collation is None else semantics.collation.to_document(),
                sort=semantics.sort,
                skip=semantics.skip,
                limit=semantics.limit,
                hint=semantics.hint,
                comment=semantics.comment,
                max_time_ms=semantics.max_time_ms,
                batch_size=None,
                plan=semantics.query_plan,
            ),
            dialect=semantics.dialect,
            context=context,
        )
        details = describe_virtual_index_usage(
            indexes,
            semantics.query_plan,
            hinted_index_name=None if hinted_index is None else hinted_index["name"],
            dialect=semantics.dialect,
        )
        return build_query_plan_explanation(
            engine="memory",
            strategy=execution_plan.strategy,
            semantics=semantics,
            details=details,
            hinted_index=None if hinted_index is None else hinted_index["name"],
            indexes=indexes,
            execution_lineage=execution_plan.execution_lineage,
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
        semantics = compile_find_semantics(
            operation.filter_spec,
            plan=operation.plan,
            projection=operation.projection,
            sort=operation.sort,
            skip=operation.skip,
            limit=operation.limit,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            dialect=dialect,
        )
        lineage = [
            ExecutionLineageStep(runtime="python", phase="scan", detail="engine scan"),
            ExecutionLineageStep(runtime="python", phase="filter", detail="semantic core"),
        ]
        if semantics.sort:
            lineage.append(ExecutionLineageStep(runtime="python", phase="sort", detail="semantic core"))
        if semantics.projection is not None:
            lineage.append(ExecutionLineageStep(runtime="python", phase="project", detail="semantic core"))
        if semantics.skip or semantics.limit is not None:
            lineage.append(ExecutionLineageStep(runtime="python", phase="slice", detail="semantic core"))
        return EngineReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=tuple(lineage),
        )

    @override
    async def explain_find_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation:
        return await self.explain_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics_from_operation(operation, dialect=dialect),
            context=context,
        )

    @override
    async def list_databases(self, *, context: ClientSession | None = None) -> list[str]:
        storage = self._storage_view(context)
        indexes = self._indexes_view(context)
        search_indexes = self._search_indexes_view(context)
        collections = self._collections_view(context)
        options = self._collection_options_view(context)
        with self._meta_lock:
            names = sorted(
                set(storage.keys())
                | set(indexes.keys())
                | set(search_indexes.keys())
                | set(collections.keys())
                | set(options.keys())
            )
        for db_name in list(self._profiler._settings.keys()):
            if db_name not in names:
                names.append(db_name)
        return sorted(names)

    @override
    async def list_collections(self, db_name: str, *, context: ClientSession | None = None) -> list[str]:
        storage = self._storage_view(context)
        indexes = self._indexes_view(context)
        search_indexes = self._search_indexes_view(context)
        collections = self._collections_view(context)
        with self._meta_lock:
            names = sorted(
                set(storage.get(db_name, {}).keys())
                | set(indexes.get(db_name, {}).keys())
                | set(search_indexes.get(db_name, {}).keys())
                | set(collections.get(db_name, set()))
            )
        if self._profiler.namespace_visible(db_name):
            names = sorted(set(names) | {self._PROFILE_COLLECTION_NAME})
        return names

    @override
    async def collection_options(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        if self._is_profile_namespace(coll_name) and self._profiler.namespace_visible(db_name):
            return {}
        collections = self._collections_view(context)
        storage = self._storage_view(context)
        indexes = self._indexes_view(context)
        search_indexes = self._search_indexes_view(context)
        options = self._collection_options_view(context)
        with self._meta_lock:
            if not (
                coll_name in collections.get(db_name, set())
                or coll_name in storage.get(db_name, {})
                or coll_name in indexes.get(db_name, {})
                or coll_name in search_indexes.get(db_name, {})
            ):
                raise CollectionInvalid(f"collection '{coll_name}' does not exist")
            return deepcopy(options.get(db_name, {}).get(coll_name, {}))

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

                db_search_indexes = search_indexes.get(db_name)
                if db_search_indexes is not None and coll_name in db_search_indexes:
                    db_search_indexes[new_name] = db_search_indexes.pop(coll_name)

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
                if db_name in search_indexes and coll_name in search_indexes[db_name]:
                    del search_indexes[db_name][coll_name]
                    if not search_indexes[db_name]:
                        del search_indexes[db_name]
