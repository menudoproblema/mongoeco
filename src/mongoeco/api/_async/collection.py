from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
import time

from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api.argument_validation import (
    HintSpec,
    normalize_sort_spec as _normalize_sort_spec,
    validate_batch_size as _validate_batch_size,
    validate_hint_spec as _validate_hint_spec,
    validate_max_time_ms as _validate_max_time_ms,
)
from mongoeco.change_streams import AsyncChangeStreamCursor, ChangeStreamHub, ChangeStreamScope
from mongoeco.api.public_api import (
    ARG_UNSET,
    COLLECTION_COUNT_DOCUMENTS_SPEC,
    COLLECTION_DELETE_MANY_SPEC,
    COLLECTION_DELETE_ONE_SPEC,
    COLLECTION_DISTINCT_SPEC,
    COLLECTION_FIND_ONE_AND_DELETE_SPEC,
    COLLECTION_FIND_ONE_AND_REPLACE_SPEC,
    COLLECTION_FIND_ONE_AND_UPDATE_SPEC,
    COLLECTION_FIND_ONE_SPEC,
    COLLECTION_FIND_RAW_BATCHES_SPEC,
    COLLECTION_FIND_SPEC,
    COLLECTION_REPLACE_ONE_SPEC,
    COLLECTION_UPDATE_MANY_SPEC,
    COLLECTION_UPDATE_ONE_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    UpdateOperation,
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.api._async.cursor import AsyncCursor, _operation_issue_message
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.search_index_cursor import AsyncSearchIndexCursor
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
    resolve_mongodb_dialect_resolution,
    resolve_pymongo_profile_resolution,
)
from mongoeco.core.aggregation import Pipeline
from mongoeco.core.collation import normalize_collation
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.core.upserts import seed_upsert_document
from mongoeco.core.validation import is_document, is_filter, is_projection, is_update
from mongoeco.session import ClientSession
from mongoeco.types import (
    ArrayFilters, BulkWriteErrorDetails, BulkWriteResult, CodecOptions, CollationDocument, DeleteResult, Document, DocumentId, Filter,
    IndexInformation, IndexKeySpec, IndexModel, InsertManyResult, InsertOne, InsertOneResult, ObjectId, Projection,
    PlanningMode, ReadConcern, ReadPreference, ReplaceOne, ReturnDocument, SearchIndexDefinition, SearchIndexDocument,
    SearchIndexModel, SortSpec, Update, UpdateMany, UpdateOne, UpdateResult,
    UpsertedWriteEntry, WriteConcern, WriteErrorEntry, WriteModel, DeleteOne, DeleteMany,
    normalize_codec_options, normalize_index_keys,
    normalize_read_concern, normalize_read_preference, normalize_write_concern,
)
from mongoeco.errors import BulkWriteError, DuplicateKeyError, InvalidOperation, OperationFailure, WriteError

_FILTER_UNSET = ARG_UNSET
_UPDATE_UNSET = ARG_UNSET


@dataclass(slots=True)
class _PreparedBulkWriteRequest:
    index: int
    request: WriteModel
    insert_document: Document | None = None
    replacement_document: Document | None = None
    preparation_error: Exception | None = None


class _BulkWriteContext:
    def __init__(
        self,
        collection: "AsyncCollection",
        requests: list[WriteModel],
    ) -> None:
        self._collection = collection
        self._requests = requests

    async def prepare(self) -> list[_PreparedBulkWriteRequest]:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(None, partial(self._prepare_request, index, request))
            for index, request in enumerate(self._requests)
        ]
        return list(await asyncio.gather(*tasks))

    def _prepare_request(self, index: int, request: WriteModel) -> _PreparedBulkWriteRequest:
        collection = self._collection
        try:
            collection._validate_bulk_write_request_against_profile(request)
            if isinstance(request, InsertOne):
                original = collection._require_document(request.document)
                if "_id" not in original:
                    original["_id"] = ObjectId()
                return _PreparedBulkWriteRequest(
                    index=index,
                    request=request,
                    insert_document=deepcopy(original),
                )
            if isinstance(request, ReplaceOne):
                collection._normalize_filter(request.filter)
                collection._normalize_hint(request.hint)
                if request.sort is not None:
                    collection._normalize_sort(request.sort)
                if request.let is not None:
                    collection._normalize_let(request.let)
                return _PreparedBulkWriteRequest(
                    index=index,
                    request=request,
                    replacement_document=deepcopy(collection._require_replacement(request.replacement)),
                )
            if isinstance(request, (UpdateOne, UpdateMany)):
                collection._normalize_filter(request.filter)
                collection._require_update(request.update)
                collection._normalize_hint(request.hint)
                if request.array_filters is not None:
                    collection._normalize_array_filters(request.array_filters)
                if request.let is not None:
                    collection._normalize_let(request.let)
                if isinstance(request, UpdateOne) and request.sort is not None:
                    collection._normalize_sort(request.sort)
                return _PreparedBulkWriteRequest(index=index, request=request)
            if isinstance(request, (DeleteOne, DeleteMany)):
                collection._normalize_filter(request.filter)
                collection._normalize_hint(request.hint)
                if request.let is not None:
                    collection._normalize_let(request.let)
                return _PreparedBulkWriteRequest(index=index, request=request)
            raise TypeError("bulk_write requests must be write model instances")
        except (TypeError, ValueError, OperationFailure, WriteError) as exc:
            return _PreparedBulkWriteRequest(index=index, request=request, preparation_error=exc)


class AsyncCollection:
    """Representa una colección de MongoDB."""

    def __init__(
        self,
        engine: AsyncStorageEngine,
        db_name: str,
        collection_name: str,
        *,
        mongodb_dialect: MongoDialect | str | None = None,
        mongodb_dialect_resolution: MongoDialectResolution | None = None,
        pymongo_profile: PyMongoProfile | str | None = None,
        pymongo_profile_resolution: PyMongoProfileResolution | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        planning_mode: PlanningMode = PlanningMode.STRICT,
        change_hub: ChangeStreamHub | None = None,
        change_stream_history_size: int | None = 10_000,
        change_stream_journal_path: str | None = None,
        change_stream_journal_fsync: bool = False,
        change_stream_journal_max_bytes: int | None = 1_048_576,
    ):
        self._engine = engine
        self._db_name = db_name
        self._collection_name = collection_name
        self._mongodb_dialect_resolution = (
            mongodb_dialect_resolution
            if mongodb_dialect_resolution is not None
            else resolve_mongodb_dialect_resolution(mongodb_dialect)
        )
        self._mongodb_dialect = self._mongodb_dialect_resolution.resolved_dialect
        self._pymongo_profile_resolution = (
            pymongo_profile_resolution
            if pymongo_profile_resolution is not None
            else resolve_pymongo_profile_resolution(pymongo_profile)
        )
        self._pymongo_profile = self._pymongo_profile_resolution.resolved_profile
        self._write_concern = normalize_write_concern(write_concern)
        self._read_concern = normalize_read_concern(read_concern)
        self._read_preference = normalize_read_preference(read_preference)
        self._codec_options = normalize_codec_options(codec_options)
        self._planning_mode = planning_mode
        self._change_stream_history_size = change_stream_history_size
        self._change_stream_journal_path = change_stream_journal_path
        self._change_stream_journal_fsync = change_stream_journal_fsync
        self._change_stream_journal_max_bytes = change_stream_journal_max_bytes
        self._change_hub = (
            ChangeStreamHub(
                max_retained_events=change_stream_history_size,
                journal_path=change_stream_journal_path,
                journal_fsync=change_stream_journal_fsync,
                journal_max_log_bytes=change_stream_journal_max_bytes,
            )
            if change_hub is None
            else change_hub
        )

    def with_options(
        self,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        planning_mode: PlanningMode | None = None,
    ) -> "AsyncCollection":
        return type(self)(
            self._engine,
            self._db_name,
            self._collection_name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
            write_concern=self._write_concern if write_concern is None else write_concern,
            read_concern=self._read_concern if read_concern is None else read_concern,
            read_preference=self._read_preference if read_preference is None else read_preference,
            codec_options=self._codec_options if codec_options is None else codec_options,
            planning_mode=self._planning_mode if planning_mode is None else planning_mode,
            change_hub=self._change_hub,
            change_stream_history_size=self._change_stream_history_size,
            change_stream_journal_path=self._change_stream_journal_path,
            change_stream_journal_fsync=self._change_stream_journal_fsync,
            change_stream_journal_max_bytes=self._change_stream_journal_max_bytes,
        )

    def __getattr__(self, name: str) -> "AsyncCollection":
        if name.startswith("_"):
            raise AttributeError(name)
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> "AsyncCollection":
        if not isinstance(name, str) or not name:
            raise TypeError("subcollection name must be a non-empty string")
        return self.database.get_collection(
            f"{self._collection_name}.{name}",
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
        ).with_options(planning_mode=self._planning_mode)

    @property
    def planning_mode(self) -> PlanningMode:
        return self._planning_mode

    @staticmethod
    def _require_document(document: object) -> Document:
        if not is_document(document):
            raise TypeError("document must be a dict")
        return document

    @classmethod
    def _require_documents(cls, documents: object) -> list[Document]:
        if (
            not isinstance(documents, Iterable)
            or isinstance(documents, (str, bytes, bytearray, dict))
        ):
            raise TypeError("documents must be a non-empty iterable of documents")
        normalized = list(documents)
        if not normalized:
            raise ValueError("documents must not be empty")
        return [cls._require_document(document) for document in normalized]

    @staticmethod
    def _normalize_filter(filter_spec: object | None) -> Filter:
        if filter_spec is None:
            return {}
        if not is_filter(filter_spec):
            raise TypeError("filter_spec must be a dict")
        return filter_spec

    @staticmethod
    def _normalize_projection(projection: object | None) -> Projection | None:
        if projection is None:
            return None
        if not is_projection(projection):
            raise TypeError("projection must be a dict")
        return projection

    @staticmethod
    def _require_write_requests(requests: object) -> list[WriteModel]:
        if not isinstance(requests, list):
            raise TypeError("requests must be a list of write models")
        normalized = list(requests)
        if not normalized:
            raise ValueError("requests must not be empty")
        supported = (InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany)
        if not all(isinstance(request, supported) for request in normalized):
            raise TypeError("bulk_write requests must be write model instances")
        return normalized

    @staticmethod
    def _require_update(update_spec: object) -> Update:
        if isinstance(update_spec, list):
            if not update_spec:
                raise ValueError("update_spec must not be empty")
            for stage in update_spec:
                if not is_document(stage):
                    raise TypeError("update pipeline stages must be dicts")
                if len(stage) != 1:
                    raise ValueError("update pipeline stages must be single-key documents")
                operator = next(iter(stage))
                if not isinstance(operator, str) or not operator.startswith("$"):
                    raise ValueError("update pipeline stages must start with '$'")
            return update_spec
        if not is_update(update_spec):
            raise TypeError("update_spec must be a dict or list")
        if not update_spec:
            raise ValueError("update_spec must not be empty")
        if not all(isinstance(key, str) and key.startswith("$") for key in update_spec):
            raise ValueError("update_spec must contain only update operators")
        for operator, params in update_spec.items():
            if not is_document(params):
                raise TypeError(f"{operator} value must be a dict")
        return update_spec

    @staticmethod
    def _require_replacement(replacement: object) -> Document:
        if not is_document(replacement):
            raise TypeError("replacement must be a dict")
        if any(isinstance(key, str) and key.startswith("$") for key in replacement):
            raise ValueError("replacement must not contain update operators")
        return replacement

    @staticmethod
    def _normalize_hint(hint: object | None) -> HintSpec | None:
        if hint is None:
            return None
        _validate_hint_spec(hint)
        return hint if isinstance(hint, str) else _normalize_sort_spec(hint)

    @staticmethod
    def _normalize_sort(sort: object | None) -> SortSpec | None:
        return _normalize_sort_spec(sort)

    @staticmethod
    def _normalize_batch_size(batch_size: object | None) -> int | None:
        if batch_size is None:
            return None
        _validate_batch_size(batch_size)
        return batch_size

    @staticmethod
    def _normalize_collation(collation: object | None) -> CollationDocument | None:
        normalized = normalize_collation(collation)
        if normalized is None:
            return None
        return normalized.to_document()

    @staticmethod
    def _normalize_max_time_ms(max_time_ms: object | None) -> int | None:
        if max_time_ms is None:
            return None
        _validate_max_time_ms(max_time_ms)
        return max_time_ms

    @staticmethod
    def _normalize_let(let: object | None) -> dict[str, object] | None:
        if let is None:
            return None
        if not isinstance(let, dict):
            raise TypeError("let must be a dict")
        return let

    @staticmethod
    def _normalize_array_filters(array_filters: object | None) -> ArrayFilters | None:
        if array_filters is None:
            return None
        if not isinstance(array_filters, list):
            raise TypeError("array_filters must be a list of dicts")
        if not all(is_filter(item) for item in array_filters):
            raise TypeError("array_filters must be a list of dicts")
        return array_filters

    @staticmethod
    def _normalize_index_keys(keys: object) -> IndexKeySpec:
        return normalize_index_keys(keys)

    @staticmethod
    def _normalize_expire_after_seconds(value: object | None) -> int | None:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise TypeError("expire_after_seconds must be a non-negative int or None")
        return value

    @classmethod
    def _normalize_index_models(cls, indexes: object) -> list[IndexModel]:
        if not isinstance(indexes, list):
            raise TypeError("indexes must be a list of IndexModel instances")
        normalized = [cls._normalize_index_model(index) for index in indexes]
        if not normalized:
            raise ValueError("indexes must not be empty")
        return normalized

    @staticmethod
    def _normalize_index_model(model: object) -> IndexModel:
        if isinstance(model, IndexModel):
            return model
        document = getattr(model, "document", None)
        if not isinstance(document, dict):
            raise TypeError("indexes must contain only IndexModel instances")
        if "key" not in document:
            raise TypeError("index model document must contain 'key'")
        unsupported = set(document) - {
            "key",
            "name",
            "unique",
            "sparse",
            "partialFilterExpression",
            "partial_filter_expression",
            "expireAfterSeconds",
            "expire_after_seconds",
        }
        if unsupported:
            unsupported_names = ", ".join(sorted(unsupported))
            raise TypeError(f"unsupported IndexModel options: {unsupported_names}")
        kwargs: dict[str, object] = {}
        for field in (
            "name",
            "unique",
            "sparse",
            "partialFilterExpression",
            "partial_filter_expression",
            "expireAfterSeconds",
            "expire_after_seconds",
        ):
            if field in document:
                kwargs[field] = document[field]
        return IndexModel(document["key"], **kwargs)

    @staticmethod
    def _normalize_search_index_model(model: object) -> SearchIndexModel:
        if isinstance(model, SearchIndexModel):
            return model
        if isinstance(model, dict):
            return SearchIndexModel(model)
        raise TypeError("model must be a SearchIndexModel or a dict definition")

    @classmethod
    def _normalize_search_index_models(cls, indexes: object) -> list[SearchIndexModel]:
        if not isinstance(indexes, list):
            raise TypeError("indexes must be a list of SearchIndexModel instances")
        normalized = [cls._normalize_search_index_model(index) for index in indexes]
        if not normalized:
            raise ValueError("indexes must not be empty")
        return normalized

    @staticmethod
    def _normalize_search_index_name(name: object) -> str:
        if not isinstance(name, str) or not name:
            raise TypeError("name must be a non-empty string")
        return name

    @staticmethod
    def _normalize_return_document(value: object | None) -> ReturnDocument:
        if value is None:
            return ReturnDocument.BEFORE
        if isinstance(value, ReturnDocument):
            return value
        if isinstance(value, bool):
            return ReturnDocument.AFTER if value else ReturnDocument.BEFORE
        enum_name = getattr(value, "name", None)
        if isinstance(enum_name, str):
            normalized_name = enum_name.upper()
            if normalized_name == "BEFORE":
                return ReturnDocument.BEFORE
            if normalized_name == "AFTER":
                return ReturnDocument.AFTER
        raise TypeError("return_document must be a ReturnDocument value")

    def _record_operation_metadata(
        self,
        *,
        operation: str,
        comment: object | None = None,
        max_time_ms: int | None = None,
        hint: HintSpec | None = None,
        session: ClientSession | None = None,
    ) -> None:
        if session is None:
            return
        recorder = getattr(self._engine, "_record_operation_metadata", None)
        if callable(recorder):
            recorder(
                session,
                operation=operation,
                comment=comment,
                max_time_ms=max_time_ms,
                hint=hint,
            )
        observe_operation = getattr(session, "observe_operation", None)
        if callable(observe_operation):
            observe_operation()

    async def _profile_operation(
        self,
        *,
        op: str,
        command: dict[str, object],
        duration_ns: int,
        operation: FindOperation | None = None,
        errmsg: str | None = None,
    ) -> None:
        if self._collection_name == "system.profile":
            return
        recorder = getattr(self._engine, "_record_profile_event", None)
        if not callable(recorder):
            return
        execution_lineage: tuple[object, ...] = ()
        fallback_reason: str | None = None
        if operation is not None:
            planner = getattr(self._engine, "plan_find_execution", None)
            if callable(planner):
                try:
                    execution_plan = await planner(
                        self._db_name,
                        self._collection_name,
                        operation,
                        dialect=self._mongodb_dialect,
                        context=None,
                    )
                    execution_lineage = execution_plan.execution_lineage
                    fallback_reason = execution_plan.fallback_reason
                except Exception:
                    execution_lineage = ()
                    fallback_reason = None
        recorder(
            self._db_name,
            op=op,
            command=command,
            duration_micros=max(1, duration_ns // 1000),
            execution_lineage=tuple(execution_lineage),
            fallback_reason=fallback_reason,
            ok=0.0 if errmsg is not None else 1.0,
            errmsg=errmsg,
        )

    async def _document_by_id(
        self,
        document_id: DocumentId,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._engine.get_document(
            self._db_name,
            self._collection_name,
            document_id,
            dialect=self._mongodb_dialect,
            context=session,
        )

    def _publish_change_event(
        self,
        *,
        operation_type: str,
        document_key: Document,
        full_document: Document | None = None,
        update_description: dict[str, object] | None = None,
    ) -> None:
        if self._change_hub is None:
            return
        self._change_hub.publish(
            operation_type=operation_type,
            db_name=self._db_name,
            coll_name=self._collection_name,
            document_key=document_key,
            full_document=full_document,
            update_description=update_description,
        )

    async def _engine_update_with_operation(
        self,
        operation: UpdateOperation,
        *,
        upsert: bool = False,
        upsert_seed: Document | None = None,
        selector_filter: Filter | None = None,
        session: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ) -> UpdateResult[DocumentId]:
        self._ensure_operation_executable(operation)
        started_at = time.perf_counter_ns()
        method = getattr(self._engine, "update_with_operation", None)
        try:
            if not callable(method):
                raise TypeError("engine must implement update_with_operation")
            result = await method(
                self._db_name,
                self._collection_name,
                operation,
                upsert=upsert,
                upsert_seed=upsert_seed,
                selector_filter=selector_filter,
                dialect=self._mongodb_dialect,
                context=session,
                bypass_document_validation=bypass_document_validation,
            )
        except Exception as exc:
            await self._profile_operation(
                op="update",
                command={
                    "update": self._collection_name,
                    "q": operation.filter_spec,
                    "u": deepcopy(operation.update_spec or {}),
                    "upsert": upsert,
                    "bypassDocumentValidation": bypass_document_validation,
                },
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        await self._profile_operation(
            op="update",
            command={
                "update": self._collection_name,
                "q": operation.filter_spec,
                "u": deepcopy(operation.update_spec or {}),
                "upsert": upsert,
                "bypassDocumentValidation": bypass_document_validation,
            },
            duration_ns=time.perf_counter_ns() - started_at,
        )
        return result

    def _engine_scan_with_operation(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        self._ensure_operation_executable(operation)
        from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

        semantics = compile_find_semantics_from_operation(
            operation,
            dialect=self._mongodb_dialect,
        )
        return self._engine.scan_find_semantics(
            self._db_name,
            self._collection_name,
            semantics,
            context=session,
        )

    async def _engine_delete_with_operation(
        self,
        operation: UpdateOperation,
        *,
        session: ClientSession | None = None,
    ) -> DeleteResult:
        self._ensure_operation_executable(operation)
        started_at = time.perf_counter_ns()
        try:
            result = await self._engine.delete_with_operation(
                self._db_name,
                self._collection_name,
                operation,
                dialect=self._mongodb_dialect,
                context=session,
            )
        except Exception as exc:
            await self._profile_operation(
                op="remove",
                command={"delete": self._collection_name, "q": operation.filter_spec},
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        await self._profile_operation(
            op="remove",
            command={"delete": self._collection_name, "q": operation.filter_spec},
            duration_ns=time.perf_counter_ns() - started_at,
        )
        return result

    async def _engine_count_with_operation(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> int:
        self._ensure_operation_executable(operation)
        started_at = time.perf_counter_ns()
        from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

        semantics = compile_find_semantics_from_operation(
            operation,
            dialect=self._mongodb_dialect,
        )
        count = await self._engine.count_find_semantics(
            self._db_name,
            self._collection_name,
            semantics,
            context=session,
        )
        await self._profile_operation(
            op="command",
            command={"count": self._collection_name, "query": operation.filter_spec},
            duration_ns=time.perf_counter_ns() - started_at,
            operation=operation,
        )
        return count

    @staticmethod
    def _can_use_direct_id_lookup(filter_spec: Filter) -> bool:
        if len(filter_spec) != 1 or "_id" not in filter_spec:
            return False

        id_selector = filter_spec["_id"]
        return not (
            isinstance(id_selector, dict)
            and any(isinstance(key, str) and key.startswith("$") for key in id_selector)
        )

    async def _select_first_document(
        self,
        filter_spec: Filter,
        *,
        plan: QueryNode | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        operation = compile_find_operation(
            filter_spec,
            collation=collation,
            sort=sort,
            limit=1,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            dialect=self._mongodb_dialect,
            plan=plan,
            planning_mode=self._planning_mode,
        )
        return await self._build_cursor(operation, session=session).first()

    def _ensure_operation_executable(self, operation: FindOperation | UpdateOperation | AggregateOperation) -> None:
        if operation.planning_issues:
            raise OperationFailure(_operation_issue_message(operation))

    def _build_cursor(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> AsyncCursor:
        return AsyncCursor(
            self,
            operation.filter_spec,
            operation.plan,
            operation.projection,
            collation=operation.collation,
            sort=operation.sort,
            skip=operation.skip,
            limit=operation.limit,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            batch_size=operation.batch_size,
            session=session,
        )

    async def _put_replacement_document(
        self,
        document: Document,
        *,
        overwrite: bool,
        session: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ) -> None:
        success = await self._engine.put_document(
            self._db_name,
            self._collection_name,
            document,
            overwrite=overwrite,
            context=session,
            bypass_document_validation=bypass_document_validation,
        )
        if not success:
            raise DuplicateKeyError(f"Duplicate key: _id={document['_id']}")

    def _build_upsert_replacement_document(
        self,
        filter_spec: Filter,
        replacement: Document,
    ) -> Document:
        seeded: Document = {}
        seed_upsert_document(seeded, filter_spec)
        if "_id" in seeded and "_id" in replacement:
            if not self._mongodb_dialect.values_equal(seeded["_id"], replacement["_id"]):
                raise OperationFailure("The _id field cannot conflict with the replacement filter during upsert")
        document = deepcopy(seeded)
        document.update(deepcopy(replacement))
        if "_id" not in document:
            document["_id"] = ObjectId()
        return document

    @staticmethod
    def _materialize_replacement_document(selected: Document, replacement: Document) -> Document:
        if "_id" in replacement:
            return deepcopy(replacement)

        replacement_items = [(key, deepcopy(value)) for key, value in replacement.items()]
        replacement_document: Document = {}
        inserted_id = False
        id_position = list(selected).index("_id") if "_id" in selected else 0

        for index in range(len(replacement_items) + 1):
            if index == id_position and not inserted_id:
                replacement_document["_id"] = deepcopy(selected["_id"])
                inserted_id = True
            if index < len(replacement_items):
                key, value = replacement_items[index]
                replacement_document[key] = value

        if not inserted_id:
            replacement_document["_id"] = deepcopy(selected["_id"])
        return replacement_document

    def _validate_bulk_write_request_against_profile(self, request: WriteModel) -> None:
        if (
            isinstance(request, (UpdateOne, ReplaceOne))
            and request.sort is not None
            and not self._pymongo_profile.supports_update_one_sort()
        ):
            raise TypeError(
                f"sort is not supported by PyMongo profile {self._pymongo_profile.key} "
                f"for {type(request).__name__} in bulk_write()"
            )

    async def insert_one(
        self,
        document: Document,
        *,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
    ) -> InsertOneResult[DocumentId]:
        original = self._require_document(document)
        if "_id" not in original:
            original["_id"] = ObjectId()
        doc = deepcopy(original)

        started_at = time.perf_counter_ns()
        try:
            success = await self._engine.put_document(
                self._db_name,
                self._collection_name,
                doc,
                overwrite=False,
                context=session,
                bypass_document_validation=bypass_document_validation,
            )
        except Exception as exc:
            await self._profile_operation(
                op="insert",
                command={
                    "insert": self._collection_name,
                    "documents": [deepcopy(doc)],
                    "bypassDocumentValidation": bypass_document_validation,
                },
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        if not success:
            raise DuplicateKeyError(f"Duplicate key: _id={doc['_id']}")
        await self._profile_operation(
            op="insert",
            command={
                "insert": self._collection_name,
                "documents": [deepcopy(doc)],
                "bypassDocumentValidation": bypass_document_validation,
            },
            duration_ns=time.perf_counter_ns() - started_at,
        )
        if session is not None:
            session.observe_operation()
        self._publish_change_event(
            operation_type="insert",
            document_key={"_id": deepcopy(doc["_id"])},
            full_document=deepcopy(doc),
        )
        return InsertOneResult(inserted_id=doc["_id"])

    async def insert_many(
        self,
        documents: list[Document],
        *,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
    ) -> InsertManyResult[DocumentId]:
        inserted_ids: list[DocumentId] = []
        command_documents: list[Document] = []
        started_at = time.perf_counter_ns()
        normalized_documents: list[Document] = []
        for original in self._require_documents(documents):
            if "_id" not in original:
                original["_id"] = ObjectId()
            doc = deepcopy(original)
            normalized_documents.append(doc)
            command_documents.append(deepcopy(doc))

        bulk_put = getattr(self._engine, "put_documents_bulk", None)
        if callable(bulk_put):
            try:
                results = list(await bulk_put(
                    self._db_name,
                    self._collection_name,
                    normalized_documents,
                    context=session,
                    bypass_document_validation=bypass_document_validation,
                ))
            except Exception as exc:
                await self._profile_operation(
                    op="insert",
                    command={
                        "insert": self._collection_name,
                        "documents": command_documents,
                        "bypassDocumentValidation": bypass_document_validation,
                    },
                    duration_ns=time.perf_counter_ns() - started_at,
                    errmsg=str(exc),
                )
                raise
            if len(results) != len(normalized_documents):
                raise RuntimeError(
                    "bulk insert engine returned a result count different from the number of documents"
                )
            for doc, success in zip(normalized_documents, results, strict=True):
                if not success:
                    raise DuplicateKeyError(f"Duplicate key: _id={doc['_id']}")
                inserted_ids.append(doc["_id"])
            await self._profile_operation(
                op="insert",
                command={
                    "insert": self._collection_name,
                    "documents": command_documents,
                    "bypassDocumentValidation": bypass_document_validation,
                },
                duration_ns=time.perf_counter_ns() - started_at,
            )
            if session is not None:
                session.observe_operation()
            for inserted in command_documents[: len(inserted_ids)]:
                self._publish_change_event(
                    operation_type="insert",
                    document_key={"_id": deepcopy(inserted["_id"])},
                    full_document=deepcopy(inserted),
                )
            return InsertManyResult(inserted_ids=inserted_ids)

        for doc in normalized_documents:
            try:
                success = await self._engine.put_document(
                    self._db_name,
                    self._collection_name,
                    doc,
                    overwrite=False,
                    context=session,
                    bypass_document_validation=bypass_document_validation,
                )
            except Exception as exc:
                await self._profile_operation(
                    op="insert",
                    command={
                        "insert": self._collection_name,
                        "documents": command_documents,
                        "bypassDocumentValidation": bypass_document_validation,
                    },
                    duration_ns=time.perf_counter_ns() - started_at,
                    errmsg=str(exc),
                )
                raise
            if not success:
                raise DuplicateKeyError(f"Duplicate key: _id={doc['_id']}")
            inserted_ids.append(doc["_id"])

        await self._profile_operation(
            op="insert",
            command={
                "insert": self._collection_name,
                "documents": command_documents,
                "bypassDocumentValidation": bypass_document_validation,
            },
            duration_ns=time.perf_counter_ns() - started_at,
        )
        if session is not None:
            session.observe_operation()
        for inserted in command_documents:
            self._publish_change_event(
                operation_type="insert",
                document_key={"_id": deepcopy(inserted["_id"])},
                full_document=deepcopy(inserted),
            )
        return InsertManyResult(inserted_ids=inserted_ids)

    async def bulk_write(
        self,
        requests: list[WriteModel],
        *,
        ordered: bool = True,
        bypass_document_validation: bool = False,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> BulkWriteResult[DocumentId]:
        requests = self._require_write_requests(requests)
        if not isinstance(ordered, bool):
            raise TypeError("ordered must be a bool")
        let = self._normalize_let(let)
        prepared_requests = await _BulkWriteContext(self, requests).prepare()

        inserted_count = 0
        matched_count = 0
        modified_count = 0
        deleted_count = 0
        upserted_ids: dict[int, DocumentId] = {}
        write_errors: list[WriteErrorEntry] = []

        for prepared in prepared_requests:
            index = prepared.index
            request = prepared.request
            if prepared.preparation_error is not None:
                exc = prepared.preparation_error
                if ordered:
                    raise exc
                write_errors.append(
                    WriteErrorEntry(
                        index=index,
                        code=getattr(exc, "code", None),
                        errmsg=str(exc),
                        operation=request.__class__.__name__,
                    )
                )
                continue
            try:
                if isinstance(request, InsertOne):
                    insert_kwargs = {"session": session}
                    if bypass_document_validation:
                        insert_kwargs["bypass_document_validation"] = True
                    await self.insert_one(prepared.insert_document or request.document, **insert_kwargs)
                    inserted_count += 1
                elif isinstance(request, UpdateOne):
                    update_one_kwargs = {
                        "sort": request.sort,
                        "array_filters": request.array_filters,
                        "hint": request.hint,
                        "comment": request.comment if request.comment is not None else comment,
                        "let": request.let if request.let is not None else let,
                        "session": session,
                    }
                    if bypass_document_validation:
                        update_one_kwargs["bypass_document_validation"] = True
                    result = await self.update_one(
                        request.filter,
                        request.update,
                        request.upsert,
                        **update_one_kwargs,
                    )
                    matched_count += result.matched_count
                    modified_count += result.modified_count
                    if result.upserted_id is not None:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, UpdateMany):
                    update_many_kwargs = {
                        "array_filters": request.array_filters,
                        "hint": request.hint,
                        "comment": request.comment if request.comment is not None else comment,
                        "let": request.let if request.let is not None else let,
                        "session": session,
                    }
                    if bypass_document_validation:
                        update_many_kwargs["bypass_document_validation"] = True
                    result = await self.update_many(
                        request.filter,
                        request.update,
                        request.upsert,
                        **update_many_kwargs,
                    )
                    matched_count += result.matched_count
                    modified_count += result.modified_count
                    if result.upserted_id is not None:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, ReplaceOne):
                    replace_one_kwargs = {
                        "sort": request.sort,
                        "hint": request.hint,
                        "comment": request.comment if request.comment is not None else comment,
                        "let": request.let if request.let is not None else let,
                        "session": session,
                    }
                    if bypass_document_validation:
                        replace_one_kwargs["bypass_document_validation"] = True
                    result = await self.replace_one(
                        request.filter,
                        prepared.replacement_document or request.replacement,
                        request.upsert,
                        **replace_one_kwargs,
                    )
                    matched_count += result.matched_count
                    modified_count += result.modified_count
                    if result.upserted_id is not None:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, DeleteOne):
                    result = await self.delete_one(
                        request.filter,
                        hint=request.hint,
                        comment=request.comment if request.comment is not None else comment,
                        let=request.let if request.let is not None else let,
                        session=session,
                    )
                    deleted_count += result.deleted_count
                elif isinstance(request, DeleteMany):
                    result = await self.delete_many(
                        request.filter,
                        hint=request.hint,
                        comment=request.comment if request.comment is not None else comment,
                        let=request.let if request.let is not None else let,
                        session=session,
                    )
                    deleted_count += result.deleted_count
            except (WriteError, OperationFailure, TypeError, ValueError) as exc:
                write_errors.append(
                    WriteErrorEntry(
                        index=index,
                        code=getattr(exc, "code", None),
                        errmsg=str(exc),
                        operation=request.__class__.__name__,
                    )
                )
                if ordered:
                    break

        result = BulkWriteResult(
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=modified_count,
            deleted_count=deleted_count,
            upserted_count=len(upserted_ids),
            upserted_ids=upserted_ids,
        )
        if write_errors:
            raise BulkWriteError(
                "bulk write failed",
                details=BulkWriteErrorDetails(
                    write_errors=write_errors,
                    inserted_count=result.inserted_count,
                    matched_count=result.matched_count,
                    modified_count=result.modified_count,
                    removed_count=result.deleted_count,
                    upserted=[
                        UpsertedWriteEntry(index=op_index, document_id=upserted_id)
                        for op_index, upserted_id in upserted_ids.items()
                    ],
                ).to_document(),
            )
        self._record_operation_metadata(
            operation="bulk_write",
            comment=comment,
            session=session,
        )
        return result

    async def find_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        operation = compile_find_operation(
            options.get("filter_spec"),
            projection=options.get("projection"),
            collation=options.get("collation"),
            sort=options.get("sort"),
            skip=options.get("skip", 0),
            limit=1,
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        doc = None
        started_at = time.perf_counter_ns()
        try:
            if (
                operation.collation is None
                and operation.sort is None
                and operation.skip == 0
                and operation.hint is None
                and operation.comment is None
                and operation.max_time_ms is None
                and self._can_use_direct_id_lookup(operation.filter_spec)
            ):
                doc = await self._engine.get_document(
                    self._db_name,
                    self._collection_name,
                    operation.filter_spec["_id"],
                    projection=operation.projection,
                    dialect=self._mongodb_dialect,
                    context=options.get("session"),
                )
            else:
                async for d in self._engine_scan_with_operation(
                    operation,
                    session=options.get("session"),
                ):
                    doc = d
                    break
        except Exception as exc:
            await self._profile_operation(
                op="query",
                command={"find": self._collection_name, "filter": operation.filter_spec},
                duration_ns=time.perf_counter_ns() - started_at,
                operation=operation,
                errmsg=str(exc),
            )
            raise
        await self._profile_operation(
            op="query",
            command={"find": self._collection_name, "filter": operation.filter_spec},
            duration_ns=time.perf_counter_ns() - started_at,
            operation=operation,
        )

        return doc

    def find(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ):
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "skip": skip,
                "limit": limit,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "batch_size": batch_size,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        operation = compile_find_operation(
            options.get("filter_spec"),
            projection=options.get("projection"),
            collation=options.get("collation"),
            sort=options.get("sort"),
            skip=options.get("skip", 0),
            limit=options.get("limit"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            batch_size=options.get("batch_size"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        return self._build_cursor(
            operation,
            session=options.get("session"),
        )

    def aggregate(
        self,
        pipeline: Pipeline,
        *,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        allow_disk_use: bool | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> AsyncAggregationCursor:
        operation = compile_aggregate_operation(
            pipeline,
            collation=collation,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            batch_size=batch_size,
            allow_disk_use=allow_disk_use,
            let=let,
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        return self._build_aggregation_cursor(operation, session=session)

    def find_raw_batches(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> AsyncRawBatchCursor:
        from mongoeco.api._async.raw_batch_cursor import AsyncRawBatchCursor

        options = normalize_public_operation_arguments(
            COLLECTION_FIND_RAW_BATCHES_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "skip": skip,
                "limit": limit,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "batch_size": batch_size,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        cursor = self.find(
            options.get("filter_spec", _FILTER_UNSET),
            options.get("projection"),
            collation=options.get("collation"),
            sort=options.get("sort"),
            skip=options.get("skip", 0),
            limit=options.get("limit"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            batch_size=options.get("batch_size"),
            session=options.get("session"),
        )
        offset = 0

        async def _fetch(size: int) -> list[Document]:
            nonlocal offset
            page = await cursor._fetch_batch(offset, size)
            offset += len(page)
            return page

        return AsyncRawBatchCursor(_fetch, batch_size=batch_size)

    def aggregate_raw_batches(
        self,
        pipeline: Pipeline,
        *,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        allow_disk_use: bool | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> AsyncRawBatchCursor:
        from mongoeco.api._async.raw_batch_cursor import AsyncRawBatchCursor

        cursor = self.aggregate(
            pipeline,
            collation=collation,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            batch_size=batch_size,
            allow_disk_use=allow_disk_use,
            let=let,
            session=session,
        )
        iterator = cursor.__aiter__()

        async def _fetch(size: int) -> list[Document]:
            documents: list[Document] = []
            for _ in range(size):
                try:
                    documents.append(await iterator.__anext__())
                except StopAsyncIteration:
                    break
            return documents

        return AsyncRawBatchCursor(_fetch, batch_size=batch_size)

    def _build_aggregation_cursor(
        self,
        operation: AggregateOperation,
        *,
        session: ClientSession | None = None,
    ) -> AsyncAggregationCursor:
        return AsyncAggregationCursor(
            self,
            operation,
            session=session,
        )

    async def update_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        update_spec: Update | object = _UPDATE_UNSET,
        upsert: bool = False,
        *,
        filter: Filter | object = _FILTER_UNSET,
        update: Update | object = _UPDATE_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        array_filters: ArrayFilters | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> UpdateResult[DocumentId]:
        options = normalize_public_operation_arguments(
            COLLECTION_UPDATE_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "update_spec": update_spec,
                "upsert": upsert,
                "collation": collation,
                "sort": sort,
                "array_filters": array_filters,
                "hint": hint,
                "comment": comment,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, "update": update, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        update_spec = self._require_update(options["update_spec"])
        upsert = bool(options.get("upsert", False))
        bypass_document_validation = bool(options.get("bypass_document_validation", False))
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            sort=options.get("sort"),
            array_filters=options.get("array_filters"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            update_spec=update_spec,
            planning_mode=self._planning_mode,
        )
        event_selected_id: DocumentId | None = None
        if self._change_hub is not None and operation.sort is None and operation.hint is None:
            selected = await self._build_cursor(
                compile_find_selection_from_update_operation(
                    operation,
                    projection={"_id": 1},
                    limit=1,
                ),
                session=session,
            ).first()
            if selected is not None:
                event_selected_id = selected["_id"]
        if operation.sort is not None:
            selected = await self._build_cursor(
                compile_find_selection_from_update_operation(operation, limit=1),
                session=session,
            ).first()
            if selected is None and not upsert:
                return UpdateResult(matched_count=0, modified_count=0)
            if selected is not None:
                identity_filter = {'_id': selected['_id']}
                identity_plan = compile_filter(identity_filter, dialect=self._mongodb_dialect)
                result = await self._engine_update_with_operation(
                    operation.with_overrides(
                        filter_spec=identity_filter,
                        plan=identity_plan,
                        sort=None,
                        hint=None,
                    ),
                    upsert=False,
                    selector_filter=operation.filter_spec,
                    session=session,
                    bypass_document_validation=bypass_document_validation,
                )
                self._record_operation_metadata(
                    operation="update_one",
                    comment=operation.comment,
                    hint=operation.hint,
                    session=session,
                )
                updated = await self._document_by_id(selected["_id"], session=session)
                if updated is not None:
                    self._publish_change_event(
                        operation_type="update",
                        document_key={"_id": deepcopy(selected["_id"])},
                        full_document=deepcopy(updated),
                    )
                return result
            return await self._perform_upsert_update(
                operation.filter_spec,
                update_spec,
                session=session,
                array_filters=operation.array_filters,
                let=operation.let,
                bypass_document_validation=bypass_document_validation,
            )
        if operation.hint is not None:
            selected = await self._build_cursor(
                compile_find_selection_from_update_operation(
                    operation,
                    projection={"_id": 1},
                    limit=1,
                ),
                session=session,
            ).first()
            if selected is None:
                if upsert:
                    return await self._perform_upsert_update(
                        operation.filter_spec,
                        update_spec,
                        session=session,
                        array_filters=operation.array_filters,
                        let=operation.let,
                        bypass_document_validation=bypass_document_validation,
                    )
                return UpdateResult(matched_count=0, modified_count=0)
            identity_filter = {"_id": selected["_id"]}
            identity_plan = compile_filter(identity_filter, dialect=self._mongodb_dialect)
            result = await self._engine_update_with_operation(
                operation.with_overrides(
                    filter_spec=identity_filter,
                    plan=identity_plan,
                    hint=None,
                ),
                upsert=False,
                selector_filter=operation.filter_spec,
                session=session,
                bypass_document_validation=bypass_document_validation,
            )
            self._record_operation_metadata(
                operation="update_one",
                comment=operation.comment,
                hint=operation.hint,
                session=session,
            )
            updated = await self._document_by_id(selected["_id"], session=session)
            if updated is not None:
                self._publish_change_event(
                    operation_type="update",
                    document_key={"_id": deepcopy(selected["_id"])},
                    full_document=deepcopy(updated),
                )
            return result
        upsert_seed = None
        if upsert:
            upsert_seed = {}
            seed_upsert_document(upsert_seed, operation.filter_spec)

        result = await self._engine_update_with_operation(
            operation,
            upsert=upsert,
            upsert_seed=upsert_seed,
            selector_filter=operation.filter_spec,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        self._record_operation_metadata(
            operation="update_one",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        if result.upserted_id is not None:
            inserted = await self._document_by_id(result.upserted_id, session=session)
            if inserted is not None:
                self._publish_change_event(
                    operation_type="insert",
                    document_key={"_id": deepcopy(result.upserted_id)},
                    full_document=deepcopy(inserted),
                )
        elif event_selected_id is not None:
            updated = await self._document_by_id(event_selected_id, session=session)
            if updated is not None:
                self._publish_change_event(
                    operation_type="update",
                    document_key={"_id": deepcopy(event_selected_id)},
                    full_document=deepcopy(updated),
                )
        return result

    async def _perform_upsert_update(
        self,
        filter_spec: Filter,
        update_spec: Update,
        *,
        session: ClientSession | None = None,
        array_filters: ArrayFilters | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
    ) -> UpdateResult[DocumentId]:
        new_doc: Document = {}
        seed_upsert_document(new_doc, filter_spec)
        UpdateEngine.apply_update(
            new_doc,
            update_spec,
            dialect=self._mongodb_dialect,
            array_filters=array_filters,
            is_upsert_insert=True,
            variables=let,
        )
        if "_id" not in new_doc:
            new_doc["_id"] = ObjectId()
        await self._put_replacement_document(
            new_doc,
            overwrite=False,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        self._publish_change_event(
            operation_type="insert",
            document_key={"_id": deepcopy(new_doc["_id"])},
            full_document=deepcopy(new_doc),
        )
        return UpdateResult(
            matched_count=0,
            modified_count=0,
            upserted_id=new_doc["_id"],
        )

    async def update_many(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        update_spec: Update | object = _UPDATE_UNSET,
        upsert: bool = False,
        *,
        filter: Filter | object = _FILTER_UNSET,
        update: Update | object = _UPDATE_UNSET,
        collation: CollationDocument | None = None,
        array_filters: ArrayFilters | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> UpdateResult[DocumentId]:
        options = normalize_public_operation_arguments(
            COLLECTION_UPDATE_MANY_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "update_spec": update_spec,
                "upsert": upsert,
                "collation": collation,
                "array_filters": array_filters,
                "hint": hint,
                "comment": comment,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, "update": update, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        update_spec = self._require_update(options["update_spec"])
        upsert = bool(options.get("upsert", False))
        bypass_document_validation = bool(options.get("bypass_document_validation", False))
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            array_filters=options.get("array_filters"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            update_spec=update_spec,
            planning_mode=self._planning_mode,
        )
        matched_documents = await self._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={"_id": 1},
            ),
            session=session,
        ).to_list()
        if not matched_documents:
            if upsert:
                return await self.update_one(
                    operation.filter_spec,
                    update_spec,
                    upsert=True,
                    collation=operation.collation,
                    array_filters=operation.array_filters,
                    hint=operation.hint,
                    comment=operation.comment,
                    let=operation.let,
                    bypass_document_validation=bypass_document_validation,
                    session=session,
                )
            return UpdateResult(matched_count=0, modified_count=0)

        modified_count = 0
        for matched in matched_documents:
            identity_filter = {"_id": matched["_id"]}
            identity_plan = compile_filter(identity_filter, dialect=self._mongodb_dialect)
            result = await self._engine_update_with_operation(
                operation.with_overrides(
                    filter_spec=identity_filter,
                    plan=identity_plan,
                    hint=None,
                ),
                upsert=False,
                selector_filter=operation.filter_spec,
                session=session,
                bypass_document_validation=bypass_document_validation,
            )
            modified_count += result.modified_count
            updated = await self._document_by_id(matched["_id"], session=session)
            if updated is not None:
                self._publish_change_event(
                    operation_type="update",
                    document_key={"_id": deepcopy(matched["_id"])},
                    full_document=deepcopy(updated),
                )

        self._record_operation_metadata(
            operation="update_many",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        return UpdateResult(
            matched_count=len(matched_documents),
            modified_count=modified_count,
        )

    async def replace_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        replacement: Document | object = ARG_UNSET,
        upsert: bool = False,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> UpdateResult[DocumentId]:
        options = normalize_public_operation_arguments(
            COLLECTION_REPLACE_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "replacement": replacement,
                "upsert": upsert,
                "collation": collation,
                "sort": sort,
                "hint": hint,
                "comment": comment,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        replacement = self._require_replacement(options["replacement"])
        upsert = bool(options.get("upsert", False))
        bypass_document_validation = bool(options.get("bypass_document_validation", False))
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            sort=options.get("sort"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        selected = await self._select_first_document(
            operation.filter_spec,
            plan=operation.plan,
            collation=operation.collation,
            sort=operation.sort,
            hint=operation.hint,
            comment=operation.comment,
            session=session,
        )
        if selected is None:
            if not upsert:
                return UpdateResult(matched_count=0, modified_count=0)
            document = self._build_upsert_replacement_document(operation.filter_spec, replacement)
            await self._put_replacement_document(
                document,
                overwrite=False,
                session=session,
                bypass_document_validation=bypass_document_validation,
            )
            self._record_operation_metadata(
                operation="replace_one",
                comment=operation.comment,
                hint=operation.hint,
                session=session,
            )
            self._publish_change_event(
                operation_type="insert",
                document_key={"_id": deepcopy(document["_id"])},
                full_document=deepcopy(document),
            )
            return UpdateResult(
                matched_count=0,
                modified_count=0,
                upserted_id=document["_id"],
            )

        if "_id" in replacement and not self._mongodb_dialect.values_equal(replacement["_id"], selected["_id"]):
            raise OperationFailure("The _id field cannot be changed in a replacement document")
        document = self._materialize_replacement_document(selected, replacement)
        modified_count = 0 if self._mongodb_dialect.values_equal(selected, document) else 1
        await self._put_replacement_document(
            document,
            overwrite=True,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        self._record_operation_metadata(
            operation="replace_one",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        self._publish_change_event(
            operation_type="replace",
            document_key={"_id": deepcopy(document["_id"])},
            full_document=deepcopy(document),
        )
        return UpdateResult(matched_count=1, modified_count=modified_count)

    async def find_one_and_update(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        update_spec: Update | object = _UPDATE_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        update: Update | object = _UPDATE_UNSET,
        projection: Projection | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        array_filters: ArrayFilters | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_AND_UPDATE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "update_spec": update_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "upsert": upsert,
                "return_document": return_document,
                "array_filters": array_filters,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, "update": update, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        update_spec = self._require_update(options["update_spec"])
        projection = self._normalize_projection(options.get("projection"))
        return_document = self._normalize_return_document(options.get("return_document"))
        upsert = bool(options.get("upsert", False))
        bypass_document_validation = bool(options.get("bypass_document_validation", False))
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            sort=options.get("sort"),
            array_filters=options.get("array_filters"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            update_spec=update_spec,
            planning_mode=self._planning_mode,
        )
        before = await self._select_first_document(
            operation.filter_spec,
            plan=operation.plan,
            collation=operation.collation,
            sort=operation.sort,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            session=session,
        )
        if before is None:
            if not upsert:
                return None
            result = await self.update_one(
                operation.filter_spec,
                update_spec,
                upsert=True,
                collation=operation.collation,
                sort=operation.sort,
                array_filters=operation.array_filters,
                hint=operation.hint,
                comment=operation.comment,
                let=operation.let,
                bypass_document_validation=bypass_document_validation,
                session=session,
            )
            if return_document is ReturnDocument.BEFORE:
                return None
            return await self.find(
                {"_id": result.upserted_id},
                projection,
                collation=operation.collation,
                limit=1,
                hint=operation.hint,
                comment=operation.comment,
                max_time_ms=operation.max_time_ms,
                session=session,
            ).first()

        identity_filter = {"_id": before["_id"]}
        identity_plan = compile_filter(identity_filter, dialect=self._mongodb_dialect)
        await self._engine_update_with_operation(
            operation.with_overrides(
                filter_spec=identity_filter,
                plan=identity_plan,
                sort=None,
                hint=None,
            ),
            upsert=False,
            selector_filter=operation.filter_spec,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        after = await self._document_by_id(before["_id"], session=session)
        if after is not None:
            self._publish_change_event(
                operation_type="update",
                document_key={"_id": deepcopy(before["_id"])},
                full_document=deepcopy(after),
            )
        if return_document is ReturnDocument.BEFORE:
            return apply_projection(
                before,
                projection,
                selector_filter=operation.filter_spec,
                dialect=self._mongodb_dialect,
            )
        if after is None:
            return None
        return apply_projection(
            after,
            projection,
            selector_filter=operation.filter_spec,
            dialect=self._mongodb_dialect,
        )

    async def find_one_and_replace(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        replacement: Document | object = ARG_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_AND_REPLACE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "replacement": replacement,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "upsert": upsert,
                "return_document": return_document,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        replacement = self._require_replacement(options["replacement"])
        projection = self._normalize_projection(options.get("projection"))
        return_document = self._normalize_return_document(options.get("return_document"))
        upsert = bool(options.get("upsert", False))
        bypass_document_validation = bool(options.get("bypass_document_validation", False))
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            sort=options.get("sort"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )

        before = await self._select_first_document(
            operation.filter_spec,
            plan=operation.plan,
            collation=operation.collation,
            sort=operation.sort,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            session=session,
        )
        if before is None:
            if not upsert:
                return None
            result = await self.replace_one(
                operation.filter_spec,
                replacement,
                upsert=True,
                collation=operation.collation,
                sort=operation.sort,
                hint=operation.hint,
                comment=operation.comment,
                let=operation.let,
                bypass_document_validation=bypass_document_validation,
                session=session,
            )
            if return_document is ReturnDocument.BEFORE:
                return None
            return await self.find(
                {"_id": result.upserted_id},
                projection,
                collation=operation.collation,
                limit=1,
                hint=operation.hint,
                comment=operation.comment,
                max_time_ms=operation.max_time_ms,
                session=session,
            ).first()

        identity_filter = {"_id": before["_id"]}
        await self.replace_one(
            identity_filter,
            replacement,
            upsert=False,
            collation=operation.collation,
            bypass_document_validation=bypass_document_validation,
            session=session,
        )
        if return_document is ReturnDocument.BEFORE:
            return apply_projection(
                before,
                projection,
                selector_filter=operation.filter_spec,
                dialect=self._mongodb_dialect,
            )
        after = await self._document_by_id(before["_id"], session=session)
        if after is None:
            return None
        return apply_projection(
            after,
            projection,
            selector_filter=operation.filter_spec,
            dialect=self._mongodb_dialect,
        )

    async def find_one_and_delete(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_AND_DELETE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "let": let,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        projection = self._normalize_projection(options.get("projection"))
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            sort=options.get("sort"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )

        before = await self._select_first_document(
            operation.filter_spec,
            plan=operation.plan,
            collation=operation.collation,
            sort=operation.sort,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            session=session,
        )
        if before is None:
            return None

        await self._engine.delete_document(
            self._db_name,
            self._collection_name,
            before["_id"],
            context=session,
        )
        self._publish_change_event(
            operation_type="delete",
            document_key={"_id": deepcopy(before["_id"])},
        )
        return apply_projection(
            before,
            projection,
            selector_filter=operation.filter_spec,
            dialect=self._mongodb_dialect,
        )

    async def delete_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> DeleteResult:
        options = normalize_public_operation_arguments(
            COLLECTION_DELETE_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "let": let,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        event_selected_id: DocumentId | None = None
        if self._change_hub is not None and operation.hint is None:
            selected_for_event = await self._build_cursor(
                compile_find_selection_from_update_operation(
                    operation,
                    projection={"_id": 1},
                    limit=1,
                ),
                session=session,
            ).first()
            if selected_for_event is not None:
                event_selected_id = selected_for_event["_id"]
        if operation.hint is not None:
            selected = await self._build_cursor(
                compile_find_selection_from_update_operation(
                    operation,
                    projection={"_id": 1},
                    limit=1,
                ),
                session=session,
            ).first()
            if selected is None:
                return DeleteResult(deleted_count=0)
            deleted = await self._engine.delete_document(
                self._db_name,
                self._collection_name,
                selected["_id"],
                context=session,
            )
            self._record_operation_metadata(
                operation="delete_one",
                comment=operation.comment,
                hint=operation.hint,
                session=session,
            )
            if deleted:
                self._publish_change_event(
                    operation_type="delete",
                    document_key={"_id": deepcopy(selected["_id"])},
                )
            return DeleteResult(deleted_count=1 if deleted else 0)
        result = await self._engine_delete_with_operation(operation, session=session)
        self._record_operation_metadata(
            operation="delete_one",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        if result.deleted_count and event_selected_id is not None:
            self._publish_change_event(
                operation_type="delete",
                document_key={"_id": deepcopy(event_selected_id)},
            )
        return result

    async def delete_many(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> DeleteResult:
        options = normalize_public_operation_arguments(
            COLLECTION_DELETE_MANY_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "let": let,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        filter_spec = self._normalize_filter(options["filter_spec"])
        session = options.get("session")
        operation = compile_update_operation(
            filter_spec,
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        matched_documents = await self._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={"_id": 1},
            ),
            session=session,
        ).to_list()
        deleted_count = 0
        for matched in matched_documents:
            deleted = await self._engine.delete_document(
                self._db_name,
                self._collection_name,
                matched["_id"],
                context=session,
            )
            if deleted:
                deleted_count += 1
                self._publish_change_event(
                    operation_type="delete",
                    document_key={"_id": deepcopy(matched["_id"])},
                )
        self._record_operation_metadata(
            operation="delete_many",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        return DeleteResult(deleted_count=deleted_count)

    async def count_documents(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        skip: int = 0,
        limit: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> int:
        options = normalize_public_operation_arguments(
            COLLECTION_COUNT_DOCUMENTS_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "skip": skip,
                "limit": limit,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        operation = compile_find_operation(
            options["filter_spec"],
            projection={"_id": 1},
            collation=options.get("collation"),
            skip=options.get("skip", 0),
            limit=options.get("limit"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        count = await self._engine_count_with_operation(operation, session=options.get("session"))
        self._record_operation_metadata(
            operation="count_documents",
            comment=operation.comment,
            hint=operation.hint,
            max_time_ms=operation.max_time_ms,
            session=options.get("session"),
        )
        return count

    async def estimated_document_count(
        self,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> int:
        max_time_ms = self._normalize_max_time_ms(max_time_ms)
        count = len(
            await self.find(
                {},
                {"_id": 1},
                comment=comment,
                max_time_ms=max_time_ms,
                session=session,
            ).to_list()
        )
        self._record_operation_metadata(
            operation="estimated_document_count",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )
        return count

    async def distinct(
        self,
        key: str,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> list[object]:
        options = normalize_public_operation_arguments(
            COLLECTION_DISTINCT_SPEC,
            explicit={
                "key": key,
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._pymongo_profile,
        )
        if not isinstance(options["key"], str):
            raise TypeError("key must be a string")
        key = options["key"]
        normalized_collation = normalize_collation(options.get("collation"))
        distinct_values: list[object] = []
        async for document in self.find(
            options.get("filter_spec"),
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            session=options.get("session"),
        ):
            values = QueryEngine.extract_values(document, key)
            found, value = QueryEngine._get_field_value(document, key)
            candidates = _resolve_distinct_candidates(
                values,
                exact_found=found,
                exact_value=value,
            )
            for candidate in candidates:
                if not any(
                    QueryEngine._values_equal(
                        existing,
                        candidate,
                        dialect=self._mongodb_dialect,
                        collation=normalized_collation,
                    )
                    for existing in distinct_values
                ):
                    distinct_values.append(candidate)
        self._record_operation_metadata(
            operation="distinct",
            comment=options.get("comment"),
            hint=options.get("hint"),
            max_time_ms=options.get("max_time_ms"),
            session=options.get("session"),
        )
        return distinct_values

    async def create_index(
        self,
        keys: object,
        *,
        unique: bool = False,
        name: str | None = None,
        sparse: bool = False,
        partial_filter_expression: dict[str, object] | None = None,
        expire_after_seconds: int | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> str:
        normalized_keys = self._normalize_index_keys(keys)
        expire_after_seconds = self._normalize_expire_after_seconds(expire_after_seconds)
        max_time_ms = self._normalize_max_time_ms(max_time_ms)
        created_name = await self._engine.create_index(
            self._db_name,
            self._collection_name,
            normalized_keys,
            unique=unique,
            name=name,
            sparse=sparse,
            partial_filter_expression=partial_filter_expression,
            expire_after_seconds=expire_after_seconds,
            max_time_ms=max_time_ms,
            context=session,
        )
        self._record_operation_metadata(
            operation="create_index",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )
        return created_name

    async def create_indexes(
        self,
        indexes: object,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> list[str]:
        models = self._normalize_index_models(indexes)
        max_time_ms = self._normalize_max_time_ms(max_time_ms)
        deadline = operation_deadline(max_time_ms)
        existing = await self._engine.index_information(
            self._db_name,
            self._collection_name,
            context=session,
        )
        names: list[str] = []
        created_names: list[str] = []
        for index in models:
            try:
                enforce_deadline(deadline)
                name = await self._engine.create_index(
                    self._db_name,
                    self._collection_name,
                    index.keys,
                    unique=index.unique,
                    name=index.name,
                    sparse=index.sparse,
                    partial_filter_expression=index.partial_filter_expression,
                    expire_after_seconds=index.expire_after_seconds,
                    max_time_ms=None if deadline is None else max(
                        1,
                        int((deadline - time.monotonic()) * 1000),
                    ),
                    context=session,
                )
            except Exception:
                for created_name in reversed(created_names):
                    try:
                        await self._engine.drop_index(
                            self._db_name,
                            self._collection_name,
                            created_name,
                            context=session,
                        )
                    except Exception:
                        pass
                raise
            names.append(name)
            if name not in existing and name not in created_names:
                created_names.append(name)
        self._record_operation_metadata(
            operation="create_indexes",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )
        return names

    def list_indexes(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> AsyncIndexCursor:
        self._record_operation_metadata(
            operation="list_indexes",
            comment=comment,
            session=session,
        )
        return AsyncIndexCursor(
            lambda: self._engine.list_indexes(
                self._db_name,
                self._collection_name,
                context=session,
            )
        )

    async def index_information(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> IndexInformation:
        self._record_operation_metadata(
            operation="index_information",
            comment=comment,
            session=session,
        )
        return await self._engine.index_information(self._db_name, self._collection_name, context=session)

    async def drop_index(
        self,
        index_or_name: str | object,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> None:
        target: str | IndexKeySpec
        if isinstance(index_or_name, str):
            target = index_or_name
        else:
            target = self._normalize_index_keys(index_or_name)
        await self._engine.drop_index(self._db_name, self._collection_name, target, context=session)
        self._record_operation_metadata(
            operation="drop_index",
            comment=comment,
            session=session,
        )

    async def drop_indexes(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> None:
        await self._engine.drop_indexes(self._db_name, self._collection_name, context=session)
        self._record_operation_metadata(
            operation="drop_indexes",
            comment=comment,
            session=session,
        )

    async def create_search_index(
        self,
        model: object,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> str:
        normalized_model = self._normalize_search_index_model(model)
        max_time_ms = self._normalize_max_time_ms(max_time_ms)
        created_name = await self._engine.create_search_index(
            self._db_name,
            self._collection_name,
            normalized_model.definition_snapshot,
            max_time_ms=max_time_ms,
            context=session,
        )
        self._record_operation_metadata(
            operation="create_search_index",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )
        return created_name

    async def create_search_indexes(
        self,
        indexes: object,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> list[str]:
        models = self._normalize_search_index_models(indexes)
        max_time_ms = self._normalize_max_time_ms(max_time_ms)
        deadline = operation_deadline(max_time_ms)
        names: list[str] = []
        for model in models:
            enforce_deadline(deadline)
            remaining = None if deadline is None else max(1, int((deadline - time.monotonic()) * 1000))
            name = await self._engine.create_search_index(
                self._db_name,
                self._collection_name,
                model.definition_snapshot,
                max_time_ms=remaining,
                context=session,
            )
            names.append(name)
        self._record_operation_metadata(
            operation="create_search_indexes",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )
        return names

    def list_search_indexes(
        self,
        name: str | None = None,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> AsyncSearchIndexCursor:
        if name is not None:
            name = self._normalize_search_index_name(name)
        self._record_operation_metadata(
            operation="list_search_indexes",
            comment=comment,
            session=session,
        )
        return AsyncSearchIndexCursor(
            lambda: self._engine.list_search_indexes(
                self._db_name,
                self._collection_name,
                name=name,
                context=session,
            )
        )

    async def update_search_index(
        self,
        name: str,
        definition: Document,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> None:
        name = self._normalize_search_index_name(name)
        definition = self._require_document(definition)
        max_time_ms = self._normalize_max_time_ms(max_time_ms)
        await self._engine.update_search_index(
            self._db_name,
            self._collection_name,
            name,
            definition,
            max_time_ms=max_time_ms,
            context=session,
        )
        self._record_operation_metadata(
            operation="update_search_index",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    async def drop_search_index(
        self,
        name: str,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> None:
        name = self._normalize_search_index_name(name)
        max_time_ms = self._normalize_max_time_ms(max_time_ms)
        await self._engine.drop_search_index(
            self._db_name,
            self._collection_name,
            name,
            max_time_ms=max_time_ms,
            context=session,
        )
        self._record_operation_metadata(
            operation="drop_search_index",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    async def drop(self, *, session: ClientSession | None = None) -> None:
        await self._engine.drop_collection(
            self._db_name,
            self._collection_name,
            context=session,
        )
        self._publish_change_event(
            operation_type="invalidate",
            document_key={"_id": self.full_name},
        )

    async def rename(
        self,
        new_name: str,
        *,
        session: ClientSession | None = None,
    ) -> "AsyncCollection":
        if not isinstance(new_name, str) or not new_name:
            raise TypeError("new_name must be a non-empty string")
        await self._engine.rename_collection(
            self._db_name,
            self._collection_name,
            new_name,
            context=session,
        )
        return type(self)(
            self._engine,
            self._db_name,
            new_name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
        )

    async def options(self, *, session: ClientSession | None = None) -> dict[str, object]:
        return await self._engine.collection_options(
            self._db_name,
            self._collection_name,
            context=session,
        )

    def watch(
        self,
        pipeline: object | None = None,
        *,
        max_await_time_ms: int | None = None,
        resume_after: dict[str, object] | None = None,
        start_after: dict[str, object] | None = None,
        start_at_operation_time: int | None = None,
        full_document: str = "default",
        session: ClientSession | None = None,
    ) -> AsyncChangeStreamCursor:
        if session is not None:
            raise InvalidOperation("watch does not support explicit sessions")
        if max_await_time_ms is not None and (
            not isinstance(max_await_time_ms, int)
            or isinstance(max_await_time_ms, bool)
            or max_await_time_ms < 0
        ):
            raise TypeError("max_await_time_ms must be a non-negative integer")
        return AsyncChangeStreamCursor(
            self._change_hub,
            scope=ChangeStreamScope(db_name=self._db_name, coll_name=self._collection_name),
            pipeline=pipeline,
            max_await_time_ms=max_await_time_ms,
            resume_after=resume_after,
            start_after=start_after,
            start_at_operation_time=start_at_operation_time,
            full_document=full_document,
        )

    def change_stream_state(self) -> dict[str, object]:
        return self._change_hub.state.to_document()

    @property
    def name(self) -> str:
        return self._collection_name

    @property
    def full_name(self) -> str:
        return f"{self._db_name}.{self._collection_name}"

    @property
    def database(self):
        from mongoeco.api._async.client import AsyncDatabase

        return AsyncDatabase(
            self._engine,
            self._db_name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
            change_hub=self._change_hub,
            change_stream_history_size=self._change_stream_history_size,
            change_stream_journal_path=self._change_stream_journal_path,
        )

    @property
    def mongodb_dialect(self) -> MongoDialect:
        return self._mongodb_dialect

    @property
    def mongodb_dialect_resolution(self) -> MongoDialectResolution:
        return self._mongodb_dialect_resolution

    @property
    def pymongo_profile(self) -> PyMongoProfile:
        return self._pymongo_profile

    @property
    def pymongo_profile_resolution(self) -> PyMongoProfileResolution:
        return self._pymongo_profile_resolution

    @property
    def write_concern(self) -> WriteConcern:
        return self._write_concern

    @property
    def read_concern(self) -> ReadConcern:
        return self._read_concern

    @property
    def read_preference(self) -> ReadPreference:
        return self._read_preference

    @property
    def codec_options(self) -> CodecOptions:
        return self._codec_options

    @property
    def change_stream_journal_path(self) -> str | None:
        return self._change_stream_journal_path

    @property
    def change_stream_journal_fsync(self) -> bool:
        return self._change_stream_journal_fsync

    @property
    def change_stream_journal_max_bytes(self) -> int | None:
        return self._change_stream_journal_max_bytes


def _resolve_distinct_candidates(
    values: list[object],
    *,
    exact_found: bool,
    exact_value: object,
) -> list[object]:
    if not values:
        if not exact_found:
            return [None]
        if isinstance(exact_value, list):
            return []
        return [exact_value]
    if not exact_found or not isinstance(exact_value, list):
        return values
    if exact_value == [] and values == [[]]:
        return []
    if values and values[0] == exact_value:
        expanded_members = list(exact_value)
        if values[1:] == expanded_members:
            return expanded_members
    return values
