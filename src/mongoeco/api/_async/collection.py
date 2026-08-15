from __future__ import annotations

import re
import time

from collections.abc import (
    AsyncIterable,
    Callable,
    Iterable,
    Mapping,
)
from copy import deepcopy
from datetime import datetime
from mongoeco.api._async import (
    _collection_indexing,
    _collection_modify,
    _collection_reads,
)
from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.api._async._collection_bulk import (
    BulkWritePreparationContext as _BulkWriteContext,  # noqa: F401
    PreparedBulkWriteRequest as _PreparedBulkWriteRequest,  # noqa: F401
    execute_bulk_write,
    normalize_bulk_write_request,
)
from mongoeco.api._async._collection_runtime import (
    CollectionRuntimeCoordinator,
)
from mongoeco.api._async._collection_watch import (
    CollectionChangeStreamConfig,
    create_change_stream_hub,
    open_collection_change_stream,
)
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api._async.cursor import AsyncCursor  # noqa: TC001
from mongoeco.api._async.raw_batch_cursor import AsyncRawBatchCursor
from mongoeco.api._async.search_index_cursor import AsyncSearchIndexCursor  # noqa: TC001
from mongoeco.api.argument_validation import (
    HintSpec,
    normalize_sort_spec as _normalize_sort_spec,
    validate_batch_size as _validate_batch_size,
    validate_hint_spec as _validate_hint_spec,
    validate_max_time_ms as _validate_max_time_ms,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    UpdateOperation,
    _normalize_array_filters,
    compile_aggregate_operation,
    compile_find_operation,
)
from mongoeco.api.public_api import (
    ARG_UNSET,
    COLLECTION_FIND_RAW_BATCHES_SPEC,
    COLLECTION_FIND_SPEC,
    normalize_aggregate_operation_arguments,
    normalize_public_operation_arguments,
)
from mongoeco.change_streams import (  # noqa: TC001
    AsyncChangeStreamCursor,
    ChangeStreamHub,
)
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
    resolve_mongodb_dialect_resolution,
    resolve_pymongo_profile_resolution,
)
from mongoeco.core.aggregation import Pipeline
from mongoeco.core.bson_scalars import (
    normalize_utc_bson_datetime,
    utc_bson_now,
)
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import normalize_collation
from mongoeco.core.expression_context import (
    ExpressionExecutionContext,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.identity import assert_valid_root_document_id
from mongoeco.core.operation_context import (
    ChangeOperationType,
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import QueryNode  # noqa: TC001
from mongoeco.core.validation import (
    is_document,
    is_filter,
    is_projection,
    is_update,
)
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.results import (
    DeleteOutcome,
    FindAndModifyOutcome,
    InsertOutcome,
    MutationOutcome,
)
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.session import ClientSession  # noqa: TC001
from mongoeco.types import (
    ArrayFilters,
    BulkWriteResult,
    CodecOptions,
    CollationDocument,
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    IndexInformation,
    IndexKeySpec,
    IndexModel,
    InsertManyResult,
    InsertOneResult,
    ObjectId,
    PlanningMode,
    Projection,
    ReadConcern,
    ReadPreference,
    ReplaceOne,
    ReturnDocument,
    SearchIndexModel,
    SortSpec,
    Update,
    UpdateOne,
    UpdateResult,
    WriteConcern,
    WriteModel,
    normalize_codec_options,
    normalize_index_keys,
    normalize_read_concern,
    normalize_read_preference,
    normalize_write_concern,
)


_FILTER_UNSET = ARG_UNSET
_UPDATE_UNSET = ARG_UNSET
_LET_VARIABLE_RE = re.compile(
    r'^(?:[a-z]|[^\x00-\x7f])(?:[A-Za-z0-9_]|[^\x00-\x7f])*$'
)
_resolve_distinct_candidates = _collection_reads.resolve_distinct_candidates


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
        now_factory: Callable[[], datetime] | None = None,
    ):
        self._engine = engine
        self._now_factory = now_factory
        self._db_name = db_name
        self._collection_name = collection_name
        self._mongodb_dialect_resolution = (
            mongodb_dialect_resolution
            if mongodb_dialect_resolution is not None
            else resolve_mongodb_dialect_resolution(mongodb_dialect)
        )
        self._mongodb_dialect = (
            self._mongodb_dialect_resolution.resolved_dialect
        )
        self._pymongo_profile_resolution = (
            pymongo_profile_resolution
            if pymongo_profile_resolution is not None
            else resolve_pymongo_profile_resolution(pymongo_profile)
        )
        self._pymongo_profile = (
            self._pymongo_profile_resolution.resolved_profile
        )
        self._write_concern = normalize_write_concern(write_concern)
        self._read_concern = normalize_read_concern(read_concern)
        self._read_preference = normalize_read_preference(read_preference)
        self._codec_options = normalize_codec_options(codec_options)
        self._planning_mode = planning_mode
        self._change_stream_history_size = change_stream_history_size
        self._change_stream_journal_path = change_stream_journal_path
        self._change_stream_journal_fsync = change_stream_journal_fsync
        self._change_stream_journal_max_bytes = change_stream_journal_max_bytes
        self._change_hub = create_change_stream_hub(
            change_hub=change_hub,
            config=CollectionChangeStreamConfig(
                history_size=change_stream_history_size,
                journal_path=change_stream_journal_path,
                journal_fsync=change_stream_journal_fsync,
                journal_max_bytes=change_stream_journal_max_bytes,
            ),
        )
        self._runtime = CollectionRuntimeCoordinator(self)

    def with_options(
        self,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        planning_mode: PlanningMode | None = None,
    ) -> 'AsyncCollection':
        return type(self)(
            self._engine,
            self._db_name,
            self._collection_name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
            write_concern=self._write_concern
            if write_concern is None
            else write_concern,
            read_concern=self._read_concern
            if read_concern is None
            else read_concern,
            read_preference=self._read_preference
            if read_preference is None
            else read_preference,
            codec_options=self._codec_options
            if codec_options is None
            else codec_options,
            planning_mode=self._planning_mode
            if planning_mode is None
            else planning_mode,
            change_hub=self._change_hub,
            change_stream_history_size=self._change_stream_history_size,
            change_stream_journal_path=self._change_stream_journal_path,
            change_stream_journal_fsync=self._change_stream_journal_fsync,
            change_stream_journal_max_bytes=self._change_stream_journal_max_bytes,
            now_factory=self._now_factory,
        )

    @property
    def now_factory(self) -> Callable[[], datetime] | None:
        return self._now_factory

    def _resolve_now(self) -> datetime:
        if self._now_factory is None:
            return utc_bson_now()
        return normalize_utc_bson_datetime(self._now_factory())

    def _new_execution_context(self) -> ExpressionExecutionContext:
        now = self._resolve_now()
        return ExpressionExecutionContext(now=now)

    def _new_operation_context(
        self,
        *,
        session: ClientSession | None = None,
        collation: object | None = None,
        bindings: Mapping[str, object] | None = None,
        expressions: ExpressionExecutionContext | None = None,
        publication: ChangePublicationPolicy = ChangePublicationPolicy.DISABLED,
        change_operation_type: ChangeOperationType | None = None,
    ) -> OperationContext:
        if expressions is None and isinstance(
            bindings, ExpressionExecutionContext
        ):
            expressions = bindings
            bindings = None
        execution_context = expressions or self._new_execution_context()
        if bindings is not None:
            execution_context = execution_context.with_bindings(bindings)
        return OperationContext.create(
            dialect=self._mongodb_dialect,
            codec_options=self._codec_options,
            session=session,
            collation=collation,
            expressions=execution_context,
            publication=publication,
            change_operation_type=change_operation_type,
        )

    def __getattr__(self, name: str) -> 'AsyncCollection':
        if name.startswith('_'):
            raise AttributeError(name)
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> 'AsyncCollection':
        if not isinstance(name, str) or not name:
            raise TypeError('subcollection name must be a non-empty string')
        return self.database.get_collection(
            f'{self._collection_name}.{name}',
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
            raise TypeError('document must be a dict')
        return document

    @classmethod
    def _require_documents(cls, documents: object) -> list[Document]:
        if not isinstance(documents, Iterable) or isinstance(
            documents, (str, bytes, bytearray, dict)
        ):
            raise TypeError(
                'documents must be a non-empty iterable of documents'
            )
        normalized = list(documents)
        if not normalized:
            raise ValueError('documents must not be empty')
        return [cls._require_document(document) for document in normalized]

    @staticmethod
    def _normalize_filter(filter_spec: object | None) -> Filter:
        if filter_spec is None:
            return {}
        if not is_filter(filter_spec):
            raise TypeError('filter_spec must be a dict')
        return DocumentCodec.to_internal(filter_spec)

    @staticmethod
    def _normalize_projection(projection: object | None) -> Projection | None:
        if projection is None:
            return None
        if not is_projection(projection):
            raise TypeError('projection must be a dict')
        return DocumentCodec.to_internal(projection)

    @staticmethod
    def _require_write_requests(requests: object) -> list[WriteModel]:
        if not isinstance(requests, list):
            raise TypeError('requests must be a list of write models')
        raw_requests = list(requests)
        if not raw_requests:
            raise ValueError('requests must not be empty')
        return [
            normalize_bulk_write_request(request) for request in raw_requests
        ]

    @staticmethod
    def _require_update(update_spec: object) -> Update:
        if isinstance(update_spec, list):
            if not update_spec:
                raise ValueError('update_spec must not be empty')
            for stage in update_spec:
                if not is_document(stage):
                    raise TypeError('update pipeline stages must be dicts')
                if len(stage) != 1:
                    raise ValueError(
                        'update pipeline stages must be single-key documents'
                    )
                operator = next(iter(stage))
                if not isinstance(operator, str) or not operator.startswith(
                    '$'
                ):
                    raise ValueError(
                        "update pipeline stages must start with '$'"
                    )
            return DocumentCodec.to_internal(update_spec)
        if not is_update(update_spec):
            raise TypeError('update_spec must be a dict or list')
        if not update_spec:
            raise ValueError('update_spec must not be empty')
        if not all(
            isinstance(key, str) and key.startswith('$') for key in update_spec
        ):
            raise ValueError('update_spec must contain only update operators')
        for operator, params in update_spec.items():
            if not is_document(params):
                raise TypeError(f'{operator} value must be a dict')
        return DocumentCodec.to_internal(update_spec)

    @staticmethod
    def _require_replacement(replacement: object) -> Document:
        if not is_document(replacement):
            raise TypeError('replacement must be a dict')
        if any(
            isinstance(key, str) and key.startswith('$') for key in replacement
        ):
            raise ValueError('replacement must not contain update operators')
        return DocumentCodec.to_internal(replacement)

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
    def _normalize_collation(
        collation: object | None,
    ) -> CollationDocument | None:
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
    def _normalize_let(
        let: object | None,
    ) -> dict[str, object] | ExpressionExecutionContext | None:
        if let is None:
            return None
        if isinstance(let, ExpressionExecutionContext):
            return let
        if not isinstance(let, dict):
            raise TypeError('let must be a dict')
        for name in let:
            if not isinstance(name, str) or not _LET_VARIABLE_RE.match(name):
                raise OperationFailure(
                    '$lookup let variable names must begin with a lowercase letter or non-ascii character'
                )
        return DocumentCodec.to_internal(let)

    @staticmethod
    def _normalize_array_filters(
        array_filters: object | None,
    ) -> ArrayFilters | None:
        return _normalize_array_filters(array_filters)

    def _apply_codec_options_to_document(self, document: Document) -> Document:
        materialized = DocumentCodec.apply_codec_options(
            DocumentCodec.to_pymongo(document),
            codec_options=self._codec_options,
        )
        if not isinstance(materialized, dict):
            raise TypeError(
                'codec_options.document_class must produce dict-compatible documents'
            )
        return materialized

    def _apply_codec_options_to_optional_document(
        self,
        document: Document | None,
    ) -> Document | None:
        if document is None:
            return None
        return self._apply_codec_options_to_document(document)

    @staticmethod
    def _to_public_update_result(
        result: UpdateResult[DocumentId],
    ) -> UpdateResult[DocumentId]:
        return UpdateResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=DocumentCodec.to_pymongo(result.upserted_id),
            acknowledged=result.acknowledged,
        )

    @staticmethod
    def _normalize_index_keys(keys: object) -> IndexKeySpec:
        return normalize_index_keys(keys)

    @staticmethod
    def _normalize_expire_after_seconds(value: object | None) -> int | None:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise TypeError(
                'expire_after_seconds must be a non-negative int or None'
            )
        return value

    @classmethod
    def _normalize_index_models(cls, indexes: object) -> list[IndexModel]:
        if not isinstance(indexes, list):
            raise TypeError('indexes must be a list of IndexModel instances')
        normalized = [cls._normalize_index_model(index) for index in indexes]
        if not normalized:
            raise ValueError('indexes must not be empty')
        return normalized

    @staticmethod
    def _normalize_index_model(model: object) -> IndexModel:
        if isinstance(model, IndexModel):
            return model
        document = getattr(model, 'document', None)
        if not isinstance(document, dict):
            raise TypeError('indexes must contain only IndexModel instances')
        if 'key' not in document:
            raise TypeError("index model document must contain 'key'")
        unsupported = set(document) - {
            'key',
            'name',
            'unique',
            'sparse',
            'background',
            'hidden',
            'collation',
            'partialFilterExpression',
            'partial_filter_expression',
            'expireAfterSeconds',
            'expire_after_seconds',
            'weights',
            'wildcardProjection',
            'wildcard_projection',
            'defaultLanguage',
            'default_language',
            'languageOverride',
            'language_override',
        }
        if unsupported:
            unsupported_names = ', '.join(sorted(unsupported))
            raise TypeError(
                f'unsupported IndexModel options: {unsupported_names}'
            )
        kwargs: dict[str, object] = {}
        for field in (
            'name',
            'unique',
            'sparse',
            'background',
            'hidden',
            'collation',
            'partialFilterExpression',
            'partial_filter_expression',
            'expireAfterSeconds',
            'expire_after_seconds',
            'weights',
            'wildcardProjection',
            'wildcard_projection',
            'defaultLanguage',
            'default_language',
            'languageOverride',
            'language_override',
        ):
            if field in document:
                kwargs[field] = document[field]
        return IndexModel(document['key'], **kwargs)

    @staticmethod
    def _normalize_search_index_model(model: object) -> SearchIndexModel:
        if isinstance(model, SearchIndexModel):
            return model
        if isinstance(model, dict):
            return SearchIndexModel(model)
        raise TypeError(
            'model must be a SearchIndexModel or a dict definition'
        )

    @classmethod
    def _normalize_search_index_models(
        cls, indexes: object
    ) -> list[SearchIndexModel]:
        if not isinstance(indexes, list):
            raise TypeError(
                'indexes must be a list of SearchIndexModel instances'
            )
        normalized = [
            cls._normalize_search_index_model(index) for index in indexes
        ]
        if not normalized:
            raise ValueError('indexes must not be empty')
        return normalized

    @staticmethod
    def _normalize_search_index_name(name: object) -> str:
        if not isinstance(name, str) or not name:
            raise TypeError('name must be a non-empty string')
        return name

    @staticmethod
    def _normalize_return_document(value: object | None) -> ReturnDocument:
        if value is None:
            return ReturnDocument.BEFORE
        if isinstance(value, ReturnDocument):
            return value
        if isinstance(value, bool):
            return ReturnDocument.AFTER if value else ReturnDocument.BEFORE
        enum_name = getattr(value, 'name', None)
        if isinstance(enum_name, str):
            normalized_name = enum_name.upper()
            if normalized_name == 'BEFORE':
                return ReturnDocument.BEFORE
            if normalized_name == 'AFTER':
                return ReturnDocument.AFTER
        raise TypeError('return_document must be a ReturnDocument value')

    def _record_operation_metadata(
        self,
        *,
        operation: str,
        comment: object | None = None,
        max_time_ms: int | None = None,
        hint: HintSpec | None = None,
        session: ClientSession | None = None,
    ) -> None:
        self._runtime.record_operation_metadata(
            operation=operation,
            comment=comment,
            max_time_ms=max_time_ms,
            hint=hint,
            session=session,
        )

    @staticmethod
    def _ensure_session_active(session: object | None) -> None:
        CollectionRuntimeCoordinator.ensure_session_active(session)

    async def _profile_operation(
        self,
        *,
        op: str,
        command: dict[str, object] | None = None,
        command_factory: Callable[[], dict[str, object]] | None = None,
        duration_ns: int,
        operation: FindOperation | None = None,
        errmsg: str | None = None,
    ) -> None:
        await self._runtime.profile_operation(
            op=op,
            command=command,
            command_factory=command_factory,
            duration_ns=duration_ns,
            operation=operation,
            errmsg=errmsg,
        )

    async def _document_by_id(
        self,
        document_id: DocumentId,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._runtime.document_by_id(
            document_id,
            session=session,
        )

    def _publish_change_event(
        self,
        *,
        operation_type: str,
        document_key: Document,
        full_document: Document | None = None,
        update_description: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> None:
        self._runtime.publish_change_event(
            operation_type=operation_type,
            document_key=document_key,
            full_document=full_document,
            update_description=update_description,
            session=session,
        )

    def _should_publish_change_events(
        self,
        *,
        session: ClientSession | None = None,
    ) -> bool:
        if '_publish_change_event' in self.__dict__:
            return True
        return self._runtime.should_publish_change_events(session=session)

    def _mark_change_event_gap(self) -> None:
        self._runtime.mark_change_event_gap()

    async def _engine_update_with_operation(
        self,
        operation: UpdateOperation,
        *,
        upsert: bool = False,
        upsert_seed: Document | None = None,
        selector_filter: Filter | None = None,
        session: ClientSession | None = None,
        bypass_document_validation: bool = False,
        replacement_document: Document | None = None,
        publish_operation_type: str | None = None,
    ) -> MutationOutcome:
        return await self._runtime.engine_update_with_operation(
            operation,
            upsert=upsert,
            upsert_seed=upsert_seed,
            selector_filter=selector_filter,
            session=session,
            bypass_document_validation=bypass_document_validation,
            replacement_document=replacement_document,
            publish_operation_type=publish_operation_type,
        )

    def _engine_scan_with_operation(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        return self._runtime.engine_scan_with_operation(
            operation,
            session=session,
        )

    async def _engine_delete_with_operation(
        self,
        operation: UpdateOperation,
        *,
        selector_filter: Filter | None = None,
        session: ClientSession | None = None,
        publish_change_event: bool = False,
    ) -> DeleteOutcome:
        return await self._runtime.engine_delete_with_operation(
            operation,
            selector_filter=selector_filter,
            session=session,
            publish_change_event=publish_change_event,
        )

    async def _engine_count_with_operation(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> int:
        return await self._runtime.engine_count_with_operation(
            operation,
            session=session,
        )

    @staticmethod
    def _can_use_direct_id_lookup(filter_spec: Filter) -> bool:
        if len(filter_spec) != 1 or '_id' not in filter_spec:
            return False

        id_selector = filter_spec['_id']
        return not (
            isinstance(id_selector, dict)
            and any(
                isinstance(key, str) and key.startswith('$')
                for key in id_selector
            )
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
        variables=None,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._runtime.select_first_document(
            filter_spec,
            plan=plan,
            collation=collation,
            sort=sort,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            variables=variables,
            session=session,
        )

    def _ensure_operation_executable(
        self, operation: FindOperation | UpdateOperation | AggregateOperation
    ) -> None:
        self._runtime.ensure_operation_executable(operation)

    def _build_cursor(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
        apply_codec_options: bool = True,
        execution_variables=None,
    ) -> AsyncCursor:
        return self._runtime.build_cursor(
            operation,
            session=session,
            apply_codec_options=apply_codec_options,
            execution_variables=execution_variables,
        )

    def _build_upsert_replacement_document(
        self,
        filter_spec: Filter,
        replacement: Document,
    ) -> Document:
        return self._runtime.build_upsert_replacement_document(
            filter_spec,
            replacement,
        )

    @staticmethod
    def _materialize_replacement_document(
        selected: Document, replacement: Document
    ) -> Document:
        return CollectionRuntimeCoordinator.materialize_replacement_document(
            selected,
            replacement,
        )

    def _validate_bulk_write_request_against_profile(
        self, request: WriteModel
    ) -> None:
        if (
            isinstance(request, (UpdateOne, ReplaceOne))
            and request.sort is not None
            and not self._pymongo_profile.supports_update_one_sort()
        ):
            raise TypeError(
                f'sort is not supported by PyMongo profile {self._pymongo_profile.key} '
                f'for {type(request).__name__} in bulk_write()'
            )

    async def insert_one(
        self,
        document: Document,
        *,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        _execution_context: ExpressionExecutionContext | None = None,
    ) -> InsertOneResult[DocumentId]:
        self._ensure_session_active(session)
        context = self._new_operation_context(
            session=session,
            expressions=_execution_context,
            change_operation_type='insert',
            publication=(
                ChangePublicationPolicy.EMIT
                if self._should_publish_change_events(session=session)
                else ChangePublicationPolicy.RECORD_GAP
            ),
        )
        original = self._require_document(document)
        if '_id' not in original:
            original['_id'] = DocumentCodec.to_pymongo(ObjectId())
        assert_valid_root_document_id(original['_id'])
        doc = DocumentCodec.to_internal(deepcopy(original))

        def _insert_one_profile_command() -> dict[str, object]:
            return {
                'insert': self._collection_name,
                'documents': [deepcopy(doc)],
                'bypassDocumentValidation': bypass_document_validation,
            }

        started_at = time.perf_counter_ns()
        try:
            with track_active_operation(
                self._engine,
                command_name='insert',
                operation_type='write',
                namespace=f'{self._db_name}.{self._collection_name}',
                session=session,
                metadata={'kind': 'insert_one'},
                killable=False,
            ):
                outcome = await self._runtime.engine_insert_document(
                    doc,
                    overwrite=False,
                    session=session,
                    bypass_document_validation=bypass_document_validation,
                    operation_context=context,
                    on_commit=lambda committed: self._publish_insert_outcome(
                        committed,
                        session=session,
                    ),
                )
        except Exception as exc:
            await self._profile_operation(
                op='insert',
                command_factory=_insert_one_profile_command,
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        if not outcome:
            raise DuplicateKeyError(f'Duplicate key: _id={doc["_id"]}')
        self._notify_instrumented_insert_outcome(outcome, session=session)
        await self._profile_operation(
            op='insert',
            command_factory=_insert_one_profile_command,
            duration_ns=time.perf_counter_ns() - started_at,
        )
        if session is not None:
            session.observe_operation()
        return InsertOneResult(
            inserted_id=DocumentCodec.to_pymongo(doc['_id'])
        )

    def _publish_insert_change_events(
        self,
        documents: Iterable[Document],
        *,
        session: ClientSession | None,
    ) -> None:
        for document in documents:
            self._publish_change_event(
                operation_type='insert',
                document_key={'_id': deepcopy(document['_id'])},
                full_document=document,
                session=session,
            )

    def _publish_insert_outcome(
        self,
        outcome: InsertOutcome,
        *,
        session: ClientSession | None,
    ) -> None:
        if outcome.document is not None:
            self._publish_insert_change_events(
                [outcome.document],
                session=session,
            )

    def _notify_instrumented_insert_outcome(
        self,
        outcome: InsertOutcome,
        *,
        session: ClientSession | None,
    ) -> None:
        if (
            '_publish_change_event' in self.__dict__
            and self._runtime._engine_spi.capabilities.change_delivery
            in {'commit-sequence', 'transactional-outbox'}
        ):
            self._publish_insert_outcome(outcome, session=session)

    async def insert_many(
        self,
        documents: list[Document],
        *,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
    ) -> InsertManyResult[DocumentId]:
        self._ensure_session_active(session)
        execution_context = self._new_operation_context(
            session=session,
            change_operation_type='insert',
            publication=(
                ChangePublicationPolicy.EMIT
                if self._should_publish_change_events(session=session)
                else ChangePublicationPolicy.RECORD_GAP
            ),
        )
        inserted_ids: list[DocumentId] = []
        started_at = time.perf_counter_ns()
        normalized_documents: list[Document] = []
        for original in self._require_documents(documents):
            if '_id' not in original:
                original['_id'] = DocumentCodec.to_pymongo(ObjectId())
            assert_valid_root_document_id(original['_id'])
            doc = DocumentCodec.to_internal(deepcopy(original))
            normalized_documents.append(doc)

        def _insert_many_profile_command() -> dict[str, object]:
            return {
                'insert': self._collection_name,
                'documents': [deepcopy(doc) for doc in normalized_documents],
                'bypassDocumentValidation': bypass_document_validation,
            }

        try:
            try:
                with track_active_operation(
                    self._engine,
                    command_name='insert',
                    operation_type='write',
                    namespace=f'{self._db_name}.{self._collection_name}',
                    session=session,
                    metadata={
                        'kind': 'insert_many',
                        'documents': len(normalized_documents),
                    },
                    killable=False,
                ):
                    results = list(
                        await self._runtime.engine_insert_documents(
                            normalized_documents,
                            session=session,
                            bypass_document_validation=bypass_document_validation,
                            operation_context=execution_context,
                            on_commit=lambda committed: self._publish_insert_outcome(
                                committed,
                                session=session,
                            ),
                        )
                    )
                    for outcome in results:
                        self._notify_instrumented_insert_outcome(
                            outcome,
                            session=session,
                        )
            except NotImplementedError:
                raise
            except Exception as exc:
                await self._profile_operation(
                    op='insert',
                    command_factory=_insert_many_profile_command,
                    duration_ns=time.perf_counter_ns() - started_at,
                    errmsg=str(exc),
                )
                raise
            if len(results) > len(normalized_documents) or (
                len(results) != len(normalized_documents)
                and (not results or results[-1])
            ):
                raise RuntimeError(
                    'bulk insert engine returned a result count different from the number of documents'
                )
            for doc, success in zip(
                normalized_documents, results, strict=False
            ):
                if not success:
                    message = f'Duplicate key: _id={doc["_id"]}'
                    await self._profile_operation(
                        op='insert',
                        command_factory=_insert_many_profile_command,
                        duration_ns=time.perf_counter_ns() - started_at,
                        errmsg=message,
                    )
                    if inserted_ids and session is not None:
                        session.observe_operation()
                    raise DuplicateKeyError(message)
                inserted_ids.append(doc['_id'])
            await self._profile_operation(
                op='insert',
                command_factory=_insert_many_profile_command,
                duration_ns=time.perf_counter_ns() - started_at,
            )
            if session is not None:
                session.observe_operation()
            return InsertManyResult(
                inserted_ids=[
                    DocumentCodec.to_pymongo(document_id)
                    for document_id in inserted_ids
                ]
            )
        except NotImplementedError:
            pass

        for doc in normalized_documents:
            try:
                with track_active_operation(
                    self._engine,
                    command_name='insert',
                    operation_type='write',
                    namespace=f'{self._db_name}.{self._collection_name}',
                    session=session,
                    metadata={
                        'kind': 'insert_many',
                        'documents': len(normalized_documents),
                    },
                    killable=False,
                ):
                    outcome = await self._runtime.engine_insert_document(
                        doc,
                        overwrite=False,
                        session=session,
                        bypass_document_validation=bypass_document_validation,
                        operation_context=execution_context,
                        on_commit=lambda committed: self._publish_insert_outcome(
                            committed,
                            session=session,
                        ),
                    )
            except Exception as exc:
                await self._profile_operation(
                    op='insert',
                    command_factory=_insert_many_profile_command,
                    duration_ns=time.perf_counter_ns() - started_at,
                    errmsg=str(exc),
                )
                raise
            if not outcome:
                message = f'Duplicate key: _id={doc["_id"]}'
                await self._profile_operation(
                    op='insert',
                    command_factory=_insert_many_profile_command,
                    duration_ns=time.perf_counter_ns() - started_at,
                    errmsg=message,
                )
                if inserted_ids and session is not None:
                    session.observe_operation()
                raise DuplicateKeyError(message)
            self._notify_instrumented_insert_outcome(
                outcome,
                session=session,
            )
            inserted_ids.append(doc['_id'])

        await self._profile_operation(
            op='insert',
            command_factory=_insert_many_profile_command,
            duration_ns=time.perf_counter_ns() - started_at,
        )
        if session is not None:
            session.observe_operation()
        return InsertManyResult(
            inserted_ids=[
                DocumentCodec.to_pymongo(document_id)
                for document_id in inserted_ids
            ]
        )

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
        self._ensure_session_active(session)
        requests = self._require_write_requests(requests)
        if not isinstance(ordered, bool):
            raise TypeError('ordered must be a bool')
        let = self._normalize_let(let)
        result = await execute_bulk_write(
            self,
            requests,
            ordered=ordered,
            bypass_document_validation=bypass_document_validation,
            comment=comment,
            let=let,
            session=session,
        )
        self._record_operation_metadata(
            operation='bulk_write',
            comment=comment,
            session=session,
        )
        return BulkWriteResult(
            inserted_count=result.inserted_count,
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            deleted_count=result.deleted_count,
            upserted_count=result.upserted_count,
            upserted_ids={
                index: DocumentCodec.to_pymongo(document_id)
                for index, document_id in result.upserted_ids.items()
            },
            acknowledged=result.acknowledged,
        )

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
        return await _collection_reads.find_one(
            self,
            filter_spec,
            projection,
            filter=filter,
            collation=collation,
            session=session,
            extra_kwargs=kwargs,
        )

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
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ):
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_SPEC,
            explicit={
                'filter_spec': filter_spec,
                'projection': projection,
                'collation': collation,
                'sort': sort,
                'skip': skip,
                'limit': limit,
                'hint': hint,
                'comment': comment,
                'max_time_ms': max_time_ms,
                'batch_size': batch_size,
                'let': let,
                'session': session,
            },
            extra_kwargs={'filter': filter, **kwargs},
            profile=self._pymongo_profile,
        )
        operation = compile_find_operation(
            self._normalize_filter(options.get('filter_spec')),
            projection=self._normalize_projection(options.get('projection')),
            collation=options.get('collation'),
            sort=options.get('sort'),
            skip=options.get('skip', 0),
            limit=options.get('limit'),
            hint=options.get('hint'),
            comment=options.get('comment'),
            max_time_ms=options.get('max_time_ms'),
            batch_size=options.get('batch_size'),
            variables=self._normalize_let(options.get('let')),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        return self._build_cursor(
            operation,
            session=options.get('session'),
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
        allow_disk_use: object = ARG_UNSET,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> AsyncAggregationCursor:
        options = normalize_aggregate_operation_arguments(
            'AsyncCollection.aggregate',
            pipeline=pipeline,
            collation=collation,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            batch_size=batch_size,
            allow_disk_use=allow_disk_use,
            let=let,
            session=session,
            extra_kwargs=kwargs,
        )
        operation = compile_aggregate_operation(
            DocumentCodec.to_internal(options['pipeline']),
            collation=options.get('collation'),
            hint=options.get('hint'),
            comment=options.get('comment'),
            max_time_ms=options.get('max_time_ms'),
            batch_size=options.get('batch_size'),
            allow_disk_use=options.get('allow_disk_use'),
            let=self._normalize_let(options.get('let')),
            dialect=self._mongodb_dialect,
            planning_mode=self._planning_mode,
        )
        return self._build_aggregation_cursor(
            operation, session=options.get('session')
        )

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
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_RAW_BATCHES_SPEC,
            explicit={
                'filter_spec': filter_spec,
                'projection': projection,
                'collation': collation,
                'sort': sort,
                'skip': skip,
                'limit': limit,
                'hint': hint,
                'comment': comment,
                'max_time_ms': max_time_ms,
                'batch_size': batch_size,
                'session': session,
            },
            extra_kwargs={'filter': filter, **kwargs},
            profile=self._pymongo_profile,
        )
        cursor = self.find(
            options.get('filter_spec', _FILTER_UNSET),
            options.get('projection'),
            collation=options.get('collation'),
            sort=options.get('sort'),
            skip=options.get('skip', 0),
            limit=options.get('limit'),
            hint=options.get('hint'),
            comment=options.get('comment'),
            max_time_ms=options.get('max_time_ms'),
            batch_size=options.get('batch_size'),
            session=options.get('session'),
        )
        offset = 0

        async def _fetch(size: int) -> list[Document]:
            nonlocal offset
            page = await cursor._fetch_batch(offset, size)
            offset += len(page)
            return page

        return AsyncRawBatchCursor(
            _fetch,
            batch_size=batch_size,
            close=cursor.close,
        )

    def aggregate_raw_batches(
        self,
        pipeline: Pipeline,
        *,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        allow_disk_use: object = ARG_UNSET,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> AsyncRawBatchCursor:
        options = normalize_aggregate_operation_arguments(
            'AsyncCollection.aggregate_raw_batches',
            pipeline=pipeline,
            collation=collation,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            batch_size=batch_size,
            allow_disk_use=allow_disk_use,
            let=let,
            session=session,
            extra_kwargs=kwargs,
        )
        cursor = self.aggregate(
            options['pipeline'],
            collation=options.get('collation'),
            hint=options.get('hint'),
            comment=options.get('comment'),
            max_time_ms=options.get('max_time_ms'),
            batch_size=options.get('batch_size'),
            allow_disk_use=options.get('allow_disk_use'),
            let=options.get('let'),
            session=options.get('session'),
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

        return AsyncRawBatchCursor(
            _fetch,
            batch_size=batch_size,
            close=cursor.close,
        )

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
        result = await _collection_modify.update_one(
            self,
            filter_spec,
            update_spec,
            upsert,
            filter=filter,
            update=update,
            collation=collation,
            sort=sort,
            array_filters=array_filters,
            hint=hint,
            comment=comment,
            let=let,
            bypass_document_validation=bypass_document_validation,
            session=session,
            extra_kwargs=kwargs,
        )
        return self._to_public_update_result(result)

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
        result = await _collection_modify.update_many(
            self,
            filter_spec,
            update_spec,
            upsert,
            filter=filter,
            update=update,
            collation=collation,
            array_filters=array_filters,
            hint=hint,
            comment=comment,
            let=let,
            bypass_document_validation=bypass_document_validation,
            session=session,
            extra_kwargs=kwargs,
        )
        return self._to_public_update_result(result)

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
        result = await _collection_modify.replace_one(
            self,
            filter_spec,
            replacement,
            upsert,
            filter=filter,
            collation=collation,
            sort=sort,
            hint=hint,
            comment=comment,
            let=let,
            bypass_document_validation=bypass_document_validation,
            session=session,
            extra_kwargs=kwargs,
        )
        return self._to_public_update_result(result)

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
        outcome = await self._find_one_and_update_outcome(
            filter_spec,
            update_spec,
            filter=filter,
            update=update,
            projection=projection,
            collation=collation,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            array_filters=array_filters,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            let=let,
            bypass_document_validation=bypass_document_validation,
            session=session,
            **kwargs,
        )
        return outcome.value

    async def _find_one_and_update_outcome(
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
    ) -> FindAndModifyOutcome:
        result = await _collection_modify.find_one_and_update_outcome(
            self,
            filter_spec,
            update_spec,
            filter=filter,
            update=update,
            projection=projection,
            collation=collation,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            array_filters=array_filters,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            let=let,
            bypass_document_validation=bypass_document_validation,
            session=session,
            extra_kwargs=kwargs,
        )
        return FindAndModifyOutcome(
            captured=result.captured,
            value=self._apply_codec_options_to_optional_document(result.value),
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
        outcome = await self._find_one_and_replace_outcome(
            filter_spec,
            replacement,
            filter=filter,
            projection=projection,
            collation=collation,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            let=let,
            bypass_document_validation=bypass_document_validation,
            session=session,
            **kwargs,
        )
        return outcome.value

    async def _find_one_and_replace_outcome(
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
    ) -> FindAndModifyOutcome:
        result = await _collection_modify.find_one_and_replace_outcome(
            self,
            filter_spec,
            replacement,
            filter=filter,
            projection=projection,
            collation=collation,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            let=let,
            bypass_document_validation=bypass_document_validation,
            session=session,
            extra_kwargs=kwargs,
        )
        return FindAndModifyOutcome(
            captured=result.captured,
            value=self._apply_codec_options_to_optional_document(result.value),
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
        document = await _collection_modify.find_one_and_delete(
            self,
            filter_spec,
            filter=filter,
            projection=projection,
            collation=collation,
            sort=sort,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            let=let,
            session=session,
            extra_kwargs=kwargs,
        )
        return self._apply_codec_options_to_optional_document(document)

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
        return await _collection_modify.delete_one(
            self,
            filter_spec,
            filter=filter,
            collation=collation,
            hint=hint,
            comment=comment,
            let=let,
            session=session,
            extra_kwargs=kwargs,
        )

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
        return await _collection_modify.delete_many(
            self,
            filter_spec,
            filter=filter,
            collation=collation,
            hint=hint,
            comment=comment,
            let=let,
            session=session,
            extra_kwargs=kwargs,
        )

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
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> int:
        return await _collection_reads.count_documents(
            self,
            filter_spec,
            filter=filter,
            collation=collation,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            skip=skip,
            limit=limit,
            let=let,
            session=session,
            extra_kwargs=kwargs,
        )

    async def estimated_document_count(
        self,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> int:
        return await _collection_reads.estimated_document_count(
            self,
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

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
        return await _collection_reads.distinct(
            self,
            key,
            filter_spec,
            filter=filter,
            collation=collation,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
            extra_kwargs=kwargs,
        )

    async def create_index(
        self,
        keys: object,
        *,
        unique: bool = False,
        name: str | None = None,
        sparse: bool = False,
        background: bool = False,
        hidden: bool = False,
        collation: dict[str, object] | None = None,
        partial_filter_expression: dict[str, object] | None = None,
        expire_after_seconds: int | None = None,
        weights: dict[str, int] | None = None,
        wildcard_projection: dict[str, object] | None = None,
        default_language: str | None = None,
        language_override: str | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> str:
        return await _collection_indexing.create_index(
            self,
            keys,
            unique=unique,
            name=name,
            sparse=sparse,
            background=background,
            hidden=hidden,
            collation=collation,
            partial_filter_expression=partial_filter_expression,
            expire_after_seconds=expire_after_seconds,
            weights=weights,
            wildcard_projection=wildcard_projection,
            default_language=default_language,
            language_override=language_override,
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    async def create_indexes(
        self,
        indexes: object,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> list[str]:
        return await _collection_indexing.create_indexes(
            self,
            indexes,
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    def list_indexes(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ):
        return _collection_indexing.list_indexes(
            self,
            comment=comment,
            session=session,
        )

    async def index_information(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> IndexInformation:
        return await _collection_indexing.index_information(
            self,
            comment=comment,
            session=session,
        )

    async def drop_index(
        self,
        index_or_name: str | object,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> None:
        await _collection_indexing.drop_index(
            self,
            index_or_name,
            comment=comment,
            session=session,
        )

    async def drop_indexes(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> None:
        await _collection_indexing.drop_indexes(
            self,
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
        return await _collection_indexing.create_search_index(
            self,
            model,
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    async def create_search_indexes(
        self,
        indexes: object,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> list[str]:
        return await _collection_indexing.create_search_indexes(
            self,
            indexes,
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    def list_search_indexes(
        self,
        name: str | None = None,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> AsyncSearchIndexCursor:
        return _collection_indexing.list_search_indexes(
            self,
            name=name,
            comment=comment,
            session=session,
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
        await _collection_indexing.update_search_index(
            self,
            name,
            definition,
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
        await _collection_indexing.drop_search_index(
            self,
            name,
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    async def drop(self, *, session: ClientSession | None = None) -> None:
        self._ensure_session_active(session)
        await self._engine.drop_collection(
            self._db_name,
            self._collection_name,
            context=session,
        )
        self._publish_change_event(
            operation_type='invalidate',
            document_key={'_id': self.full_name},
            session=session,
        )

    async def rename(
        self,
        new_name: str,
        *,
        session: ClientSession | None = None,
    ) -> 'AsyncCollection':
        self._ensure_session_active(session)
        if not isinstance(new_name, str) or not new_name:
            raise TypeError('new_name must be a non-empty string')
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
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
            planning_mode=self._planning_mode,
            change_hub=self._change_hub,
            change_stream_history_size=self._change_stream_history_size,
            change_stream_journal_path=self._change_stream_journal_path,
            change_stream_journal_fsync=self._change_stream_journal_fsync,
            change_stream_journal_max_bytes=self._change_stream_journal_max_bytes,
            now_factory=self._now_factory,
        )

    async def options(
        self, *, session: ClientSession | None = None
    ) -> dict[str, object]:
        self._ensure_session_active(session)
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
        full_document: str = 'default',
        session: ClientSession | None = None,
    ) -> AsyncChangeStreamCursor:
        return open_collection_change_stream(
            self,
            pipeline=pipeline,
            max_await_time_ms=max_await_time_ms,
            resume_after=resume_after,
            start_after=start_after,
            start_at_operation_time=start_at_operation_time,
            full_document=full_document,
            session=session,
        )

    def change_stream_state(self) -> dict[str, object]:
        return self._change_hub.state.to_document()

    def change_stream_backend_info(self) -> dict[str, object]:
        return self._change_hub.backend_info.to_document()

    @property
    def name(self) -> str:
        return self._collection_name

    @property
    def full_name(self) -> str:
        return f'{self._db_name}.{self._collection_name}'

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
            change_stream_journal_fsync=self._change_stream_journal_fsync,
            change_stream_journal_max_bytes=self._change_stream_journal_max_bytes,
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
    def change_stream_history_size(self) -> int | None:
        return self._change_stream_history_size

    @property
    def change_stream_journal_path(self) -> str | None:
        return self._change_stream_journal_path

    @property
    def change_stream_journal_fsync(self) -> bool:
        return self._change_stream_journal_fsync

    @property
    def change_stream_journal_max_bytes(self) -> int | None:
        return self._change_stream_journal_max_bytes
