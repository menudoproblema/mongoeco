from typing import TYPE_CHECKING

from mongoeco.api.admin_parsing import (
    FindAndModifyCommandOptions,
    normalize_command_batch_size,
    normalize_command_document,
    normalize_command_hint,
    normalize_command_max_time_ms,
    normalize_command_ordered,
    normalize_command_projection,
    normalize_command_scale,
    normalize_command_sort_document,
    normalize_delete_specs,
    normalize_filter_document,
    normalize_index_models_from_command,
    normalize_insert_documents,
    normalize_list_collections_options,
    normalize_list_databases_options,
    normalize_namespace,
    normalize_update_specs,
    normalize_validate_command_options,
    require_collection_name,
    resolve_collection_reference,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.api._async.database_commands import AsyncDatabaseCommandService
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.core.aggregation import _bson_document_size
from mongoeco.core.collation import normalize_collation
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.semantic_core import compile_collection_validation_semantics
from mongoeco.errors import BulkWriteError, CollectionInvalid, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    BulkWriteErrorDetails,
    CollectionStatsSnapshot,
    CollectionListingDocument,
    CollectionListingSnapshot,
    CollectionStatsDocument,
    CollectionValidationDocument,
    CollectionValidationSnapshot,
    CommandCursorResult,
    CountCommandResult,
    CreateIndexesCommandResult,
    DatabaseStatsSnapshot,
    DatabaseListingDocument,
    DatabaseListingSnapshot,
    DatabaseStatsDocument,
    DistinctCommandResult,
    DropDatabaseCommandResult,
    DropIndexesCommandResult,
    Document,
    Filter,
    FindAndModifyCommandResult,
    FindAndModifyLastErrorObject,
    IndexModel,
    ListDatabasesCommandResult,
    NamespaceOkResult,
    OkResult,
    Projection,
    ReturnDocument,
    UpsertedWriteEntry,
    WriteErrorEntry,
    WriteCommandResult,
)

if TYPE_CHECKING:
    from mongoeco.api._async.client import AsyncDatabase
    from mongoeco.compat import MongoDialect


class AsyncDatabaseAdminService:
    def __init__(self, database: "AsyncDatabase"):
        self._database = database
        self._commands = AsyncDatabaseCommandService(self)

    @property
    def _engine(self) -> AsyncStorageEngine:
        return self._database._engine

    @property
    def _db_name(self) -> str:
        return self._database._db_name

    @property
    def _mongodb_dialect(self) -> "MongoDialect":
        return self._database._mongodb_dialect

    @staticmethod
    def _normalize_filter(filter_spec: object | None) -> Filter:
        return normalize_filter_document(filter_spec)

    async def list_collection_names(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> list[str]:
        if filter_spec is None:
            return await self._engine.list_collections(self._db_name, context=session)
        documents = await self.list_collections(filter_spec, session=session).to_list()
        return [str(document["name"]) for document in documents]

    async def _list_collection_snapshots(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[CollectionListingSnapshot]:
        names = await self._engine.list_collections(self._db_name, context=session)
        snapshots: list[CollectionListingSnapshot] = []
        for name in names:
            snapshots.append(
                CollectionListingSnapshot(
                    name=name,
                    options=await self._engine.collection_options(
                        self._db_name,
                        name,
                        context=session,
                    ),
                )
            )
        return snapshots

    def list_collections(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> AsyncListingCursor:
        normalized_filter = self._normalize_filter(filter_spec)

        async def _load() -> list[CollectionListingDocument]:
            documents = [
                snapshot.to_document()
                for snapshot in await self._list_collection_snapshots(session=session)
            ]
            return [
                document
                for document in documents
                if QueryEngine.match(
                    document,
                    normalized_filter,
                    dialect=self._mongodb_dialect,
                )
            ]

        return AsyncListingCursor(_load)

    async def create_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
        **options: object,
    ):
        normalized_options = dict(options)
        compile_collection_validation_semantics(
            normalized_options,
            dialect=self._mongodb_dialect,
        )
        await self._engine.create_collection(
            self._db_name,
            name,
            options=normalized_options,
            context=session,
        )
        return self._database.get_collection(name)

    async def drop_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
    ) -> None:
        await self._engine.drop_collection(self._db_name, name, context=session)

    @staticmethod
    def _normalize_command(command: object, kwargs: dict[str, object]) -> dict[str, object]:
        return normalize_command_document(command, kwargs)

    @staticmethod
    def _require_collection_name(value: object, field_name: str) -> str:
        return require_collection_name(value, field_name)

    @staticmethod
    def _resolve_collection_reference(value: object, field_name: str) -> str:
        return resolve_collection_reference(value, field_name)

    @staticmethod
    def _normalize_index_models_from_command(indexes: object) -> list[IndexModel]:
        return normalize_index_models_from_command(indexes)

    @staticmethod
    def _normalize_sort_document(sort: object | None) -> list[tuple[str, int]] | None:
        return normalize_command_sort_document(sort)

    @staticmethod
    def _normalize_hint_from_command(hint: object | None) -> object | None:
        return normalize_command_hint(hint)

    @staticmethod
    def _normalize_max_time_ms_from_command(max_time_ms: object | None) -> int | None:
        return normalize_command_max_time_ms(max_time_ms)

    @staticmethod
    def _normalize_projection_from_command(projection: object | None) -> dict[str, object] | None:
        return normalize_command_projection(projection)

    @staticmethod
    def _normalize_batch_size_from_command(batch_size: object | None) -> int | None:
        return normalize_command_batch_size(batch_size)

    def _compile_command_find_operation(
        self,
        spec: dict[str, object],
        *,
        collection_field: str,
        filter_field: str = "filter",
        projection_field: str = "projection",
        sort_field: str = "sort",
        hint_field: str = "hint",
        comment_field: str = "comment",
        max_time_ms_field: str = "maxTimeMS",
        skip_field: str = "skip",
        limit_field: str = "limit",
        batch_size_field: str | None = "batchSize",
        default_projection: Projection | None = None,
        default_limit: int | None = None,
    ) -> tuple[str, FindOperation]:
        collection_name = self._require_collection_name(
            spec.get(collection_field),
            collection_field,
        )
        skip = spec.get(skip_field, 0)
        if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
            raise TypeError("skip must be a non-negative integer")
        limit = spec.get(limit_field, default_limit)
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 0
        ):
            raise TypeError("limit must be a non-negative integer")
        batch_size = (
            None
            if batch_size_field is None
            else self._normalize_batch_size_from_command(spec.get(batch_size_field))
        )
        return (
            collection_name,
            compile_find_operation(
                self._normalize_filter(spec.get(filter_field)),
                projection=normalize_command_projection(
                    spec.get(projection_field, default_projection)
                ),
                collation=spec.get("collation"),
                sort=normalize_command_sort_document(spec.get(sort_field)),
                skip=skip,
                limit=limit,
                hint=normalize_command_hint(spec.get(hint_field)),
                comment=spec.get(comment_field),
                max_time_ms=normalize_command_max_time_ms(
                    spec.get(max_time_ms_field)
                ),
                batch_size=batch_size,
                dialect=self._mongodb_dialect,
            ),
        )

    def _compile_command_aggregate_operation(
        self,
        spec: dict[str, object],
        *,
        collection_field: str = "aggregate",
        pipeline_field: str = "pipeline",
        cursor_field: str = "cursor",
        batch_size_field: str = "batchSize",
        hint_field: str = "hint",
        comment_field: str = "comment",
        max_time_ms_field: str = "maxTimeMS",
        allow_disk_use_field: str = "allowDiskUse",
        let_field: str = "let",
    ) -> tuple[str, AggregateOperation]:
        collection_name = self._require_collection_name(
            spec.get(collection_field),
            collection_field,
        )
        pipeline = spec.get(pipeline_field)
        if not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")
        cursor_spec = spec.get(cursor_field, {})
        if not isinstance(cursor_spec, dict):
            raise TypeError("cursor must be a document")
        return (
            collection_name,
            compile_aggregate_operation(
                pipeline,
                collation=spec.get("collation"),
                hint=self._normalize_hint_from_command(spec.get(hint_field)),
                comment=spec.get(comment_field),
                max_time_ms=self._normalize_max_time_ms_from_command(
                    spec.get(max_time_ms_field)
                ),
                batch_size=self._normalize_batch_size_from_command(
                    cursor_spec.get(batch_size_field)
                ),
                allow_disk_use=spec.get(allow_disk_use_field),
                let=spec.get(let_field),
            ),
        )

    def _compile_command_update_selection_operation(
        self,
        update_spec: dict[str, object],
        *,
        comment: object | None,
        max_time_ms: object | None,
        limit: int | None,
    ) -> FindOperation:
        return compile_find_selection_from_update_operation(
            compile_update_operation(
                self._normalize_filter(update_spec.get("q")),
                collation=update_spec.get("collation"),
                hint=self._normalize_hint_from_command(update_spec.get("hint")),
                comment=comment,
                max_time_ms=self._normalize_max_time_ms_from_command(max_time_ms),
                let=update_spec.get("let"),
                dialect=self._mongodb_dialect,
            ),
            projection={"_id": 1},
            limit=limit,
        )

    def _compile_command_delete_selection_operation(
        self,
        delete_spec: dict[str, object],
        *,
        comment: object | None,
        max_time_ms: object | None,
        limit: int | None,
    ) -> FindOperation:
        return compile_find_selection_from_update_operation(
            compile_update_operation(
                self._normalize_filter(delete_spec.get("q")),
                collation=delete_spec.get("collation"),
                hint=self._normalize_hint_from_command(delete_spec.get("hint")),
                comment=comment,
                max_time_ms=self._normalize_max_time_ms_from_command(max_time_ms),
                let=delete_spec.get("let"),
                dialect=self._mongodb_dialect,
            ),
            projection={"_id": 1},
            limit=limit,
        )

    def _compile_command_count_operation(
        self,
        spec: dict[str, object],
    ) -> tuple[str, FindOperation]:
        return self._compile_command_find_operation(
            spec,
            collection_field="count",
            filter_field="query",
            default_projection={"_id": 1},
        )

    def _compile_command_distinct_operation(
        self,
        spec: dict[str, object],
    ) -> tuple[str, str, FindOperation]:
        collection_name = self._require_collection_name(
            spec.get("distinct"),
            "distinct",
        )
        key = spec.get("key")
        if not isinstance(key, str) or not key:
            raise TypeError("key must be a non-empty string")
        _, operation = self._compile_command_find_operation(
            spec,
            collection_field="distinct",
            filter_field="query",
            batch_size_field=None,
        )
        return collection_name, key, operation

    def _compile_find_and_modify_selection_operation(
        self,
        options: FindAndModifyCommandOptions,
        *,
        projection: Projection | None = None,
        limit: int | None = 1,
    ) -> FindOperation:
        return compile_find_operation(
            options.query,
            projection=projection,
            sort=options.sort,
            limit=limit,
            hint=options.hint,
            comment=options.comment,
            max_time_ms=options.max_time_ms,
            dialect=self._mongodb_dialect,
        )

    def _compile_id_lookup_operation(
        self,
        document_id: object,
        *,
        projection: Projection | None = None,
    ) -> FindOperation:
        return compile_find_operation(
            {"_id": document_id},
            projection=projection,
            limit=1,
            dialect=self._mongodb_dialect,
        )

    async def _first_with_operation(
        self,
        collection_name: str,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._database.get_collection(collection_name)._build_cursor(
            operation,
            session=session,
        ).first()

    @staticmethod
    def _normalize_ordered_from_command(value: object | None) -> bool:
        return normalize_command_ordered(value)

    @staticmethod
    def _normalize_scale_from_command(scale: object | None) -> int:
        return normalize_command_scale(scale)

    @staticmethod
    def _normalize_namespace(value: object, field_name: str) -> tuple[str, str]:
        return normalize_namespace(value, field_name)

    @staticmethod
    def _normalize_insert_documents(spec: object) -> list[Document]:
        return normalize_insert_documents(spec)

    @staticmethod
    def _normalize_update_specs(spec: object) -> list[dict[str, object]]:
        return normalize_update_specs(spec)

    @staticmethod
    def _normalize_delete_specs(spec: object) -> list[dict[str, object]]:
        return normalize_delete_specs(spec)

    async def _collection_stats(
        self,
        collection_name: str,
        *,
        scale: int = 1,
        session: ClientSession | None = None,
    ) -> CollectionStatsSnapshot:
        collection = self._database.get_collection(collection_name)
        documents = await collection.find({}, session=session).to_list()
        indexes = await collection.list_indexes(session=session).to_list()
        data_size = sum(_bson_document_size(document) for document in documents)
        return CollectionStatsSnapshot(
            namespace=f"{self._db_name}.{collection_name}",
            count=len(documents),
            data_size=data_size,
            index_count=len(indexes),
            scale=scale,
        )

    async def _database_stats(
        self,
        *,
        scale: int = 1,
        session: ClientSession | None = None,
    ) -> DatabaseStatsSnapshot:
        collection_names = await self._engine.list_collections(self._db_name, context=session)
        objects = 0
        data_size = 0
        indexes = 0
        for collection_name in collection_names:
            stats = await self._collection_stats(collection_name, scale=1, session=session)
            objects += stats.count
            data_size += stats.data_size
            indexes += stats.index_count
        return DatabaseStatsSnapshot(
            db_name=self._db_name,
            collection_count=len(collection_names),
            object_count=objects,
            data_size=data_size,
            index_count=indexes,
            scale=scale,
        )

    async def _list_database_snapshots(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[DatabaseListingSnapshot]:
        database_names = await self._engine.list_databases(context=session)
        snapshots: list[DatabaseListingSnapshot] = []
        for database_name in database_names:
            database = type(self._database)(
                self._engine,
                database_name,
                mongodb_dialect=self._database._mongodb_dialect,
                mongodb_dialect_resolution=self._database._mongodb_dialect_resolution,
                pymongo_profile=self._database._pymongo_profile,
                pymongo_profile_resolution=self._database._pymongo_profile_resolution,
                write_concern=self._database._write_concern,
                read_concern=self._database._read_concern,
                read_preference=self._database._read_preference,
                codec_options=self._database._codec_options,
            )
            stats = await database._admin._database_stats(session=session)
            snapshots.append(
                DatabaseListingSnapshot(
                    name=database_name,
                    size_on_disk=stats.data_size,
                    empty=(
                        stats.collection_count == 0
                        and stats.object_count == 0
                    ),
                )
            )
        return snapshots

    async def _list_database_documents(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[DatabaseListingDocument]:
        return [
            snapshot.to_document()
            for snapshot in await self._list_database_snapshots(session=session)
        ]

    async def _build_collection_validation_snapshot(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> CollectionValidationSnapshot:
        normalize_validate_command_options(
            scandata=scandata,
            full=full,
            background=background,
            comment=comment,
        )

        collection_name = self._resolve_collection_reference(
            name_or_collection,
            "name_or_collection",
        )
        collection_names = await self._engine.list_collections(self._db_name, context=session)
        if collection_name not in collection_names:
            raise CollectionInvalid(f"collection '{collection_name}' does not exist")

        indexes = await self._database.get_collection(collection_name).list_indexes(
            session=session
        ).to_list()
        return CollectionValidationSnapshot(
            namespace=f"{self._db_name}.{collection_name}",
            record_count=await self._database.get_collection(collection_name).count_documents(
                {},
                session=session,
            ),
            index_count=len(indexes),
            keys_per_index={
                str(index["name"]): len(index.get("fields", []))
                for index in indexes
            },
        )

    async def validate_collection(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> CollectionValidationDocument:
        return (
            await self._build_collection_validation_snapshot(
                name_or_collection,
                scandata=scandata,
                full=full,
                background=background,
                session=session,
                comment=comment,
            )
        ).to_document()

    async def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        return await self._commands.command(command, session=session, **kwargs)

    async def _command_list_collections(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        options = normalize_list_collections_options(spec)
        if options.name_only:
            collection_names = await self._engine.list_collections(
                self._db_name,
                context=session,
            )
            first_batch = [
                {"name": name, "type": "collection"}
                for name in collection_names
            ]
            first_batch = [
                document
                for document in first_batch
                if QueryEngine.match(
                    document,
                    options.filter_spec,
                    dialect=self._mongodb_dialect,
                )
            ]
        else:
            snapshots = await self._list_collection_snapshots(session=session)
            first_batch = [snapshot.to_document() for snapshot in snapshots]
            first_batch = [
                document
                for document in first_batch
                if QueryEngine.match(
                    document,
                    options.filter_spec,
                    dialect=self._mongodb_dialect,
                )
            ]
        return CommandCursorResult(
            namespace=f"{self._db_name}.$cmd.listCollections",
            first_batch=first_batch,
        )

    async def _command_list_databases(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        options = normalize_list_databases_options(spec)
        databases = [
            snapshot.to_document()
            for snapshot in await self._list_database_snapshots(session=session)
        ]
        filtered = [
            document
            for document in databases
            if QueryEngine.match(
                document,
                options.filter_spec,
                dialect=self._mongodb_dialect,
            )
        ]
        total_size = sum(int(document.get("sizeOnDisk", 0)) for document in filtered)
        if options.name_only:
            filtered = [{"name": str(document["name"])} for document in filtered]
        return ListDatabasesCommandResult(
            databases=filtered,
            total_size=total_size,
        )

    async def _command_create(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._require_collection_name(spec.get("create"), "create")
        options = {
            key: value
            for key, value in spec.items()
            if key not in {"create", "$db"}
        }
        await self.create_collection(
            collection_name,
            session=session,
            **options,
        )
        return OkResult()

    async def _command_drop(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._require_collection_name(spec.get("drop"), "drop")
        await self.drop_collection(collection_name, session=session)
        return NamespaceOkResult(
            namespace=f"{self._db_name}.{collection_name}",
        )

    async def _command_rename_collection(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        source_db, source_name = self._normalize_namespace(
            spec.get("renameCollection"),
            "renameCollection",
        )
        target_db, target_name = self._normalize_namespace(spec.get("to"), "to")
        if source_db != self._db_name or target_db != self._db_name:
            raise OperationFailure(
                "renameCollection only supports namespaces in the current database"
            )
        drop_target = spec.get("dropTarget", False)
        if not isinstance(drop_target, bool):
            raise TypeError("dropTarget must be a bool")
        if drop_target:
            target_names = await self._engine.list_collections(
                self._db_name,
                context=session,
            )
            if target_name in target_names:
                await self.drop_collection(target_name, session=session)
        await self._database.get_collection(source_name).rename(target_name, session=session)
        return OkResult()

    async def _command_count(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name, operation = self._compile_command_count_operation(spec)
        return await self._execute_count_command(
            collection_name,
            operation,
            session=session,
        )

    async def _command_distinct(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name, key, operation = self._compile_command_distinct_operation(spec)
        return await self._execute_distinct_command(
            collection_name,
            key,
            operation,
            session=session,
        )

    async def _execute_distinct_command(
        self,
        collection_name: str,
        key: str,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> DistinctCommandResult:
        distinct_values: list[object] = []
        normalized_collation = normalize_collation(operation.collation)
        async for document in self._database.get_collection(collection_name)._build_cursor(
            operation,
            session=session,
        ):
            values = QueryEngine.extract_values(document, key)
            if not values:
                found, value = QueryEngine._get_field_value(document, key)
                if not found:
                    values = [None]
                elif isinstance(value, list):
                    continue
                else:
                    values = [value]
            candidates = values[1:] if isinstance(values[0], list) else values
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
        return DistinctCommandResult(values=distinct_values)

    async def _execute_count_command(
        self,
        collection_name: str,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> CountCommandResult:
        count = await self._database.get_collection(
            collection_name
        )._engine_count_with_operation(
            operation,
            session=session,
        )
        return CountCommandResult(count=count)

    async def _command_insert(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._require_collection_name(spec.get("insert"), "insert")
        documents = self._normalize_insert_documents(spec.get("documents"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        bypass_document_validation = spec.get("bypassDocumentValidation", False)
        if not isinstance(bypass_document_validation, bool):
            raise TypeError("bypassDocumentValidation must be a bool")
        collection = self._database.get_collection(collection_name)
        inserted = 0
        write_errors: list[WriteErrorEntry] = []
        for index, document in enumerate(documents):
            try:
                await collection.insert_one(
                    document,
                    bypass_document_validation=bypass_document_validation,
                    session=session,
                )
                inserted += 1
            except Exception as exc:
                write_errors.append(
                    WriteErrorEntry(
                        index=index,
                        errmsg=str(exc),
                    )
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "insert command failed",
                details=BulkWriteErrorDetails(
                    write_errors=write_errors,
                    inserted_count=inserted,
                ).to_document(),
            )
        return WriteCommandResult(count=inserted)

    async def _command_update(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._require_collection_name(spec.get("update"), "update")
        updates = self._normalize_update_specs(spec.get("updates"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        bypass_document_validation = spec.get("bypassDocumentValidation", False)
        if not isinstance(bypass_document_validation, bool):
            raise TypeError("bypassDocumentValidation must be a bool")
        collection = self._database.get_collection(collection_name)
        matched = 0
        modified = 0
        upserted: list[UpsertedWriteEntry] = []
        write_errors: list[WriteErrorEntry] = []
        for index, update_spec in enumerate(updates):
            try:
                query = self._normalize_filter(update_spec.get("q"))
                update_document = update_spec.get("u")
                if not isinstance(update_document, dict):
                    raise TypeError("u must be a document")
                multi = update_spec.get("multi", False)
                if not isinstance(multi, bool):
                    raise TypeError("multi must be a bool")
                upsert = update_spec.get("upsert", False)
                if not isinstance(upsert, bool):
                    raise TypeError("upsert must be a bool")
                array_filters = update_spec.get("arrayFilters")
                if array_filters is not None and (
                    not isinstance(array_filters, list)
                    or not all(is_filter(item) for item in array_filters)
                ):
                    raise TypeError("arrayFilters must be a list of dicts")
                hint = self._normalize_hint_from_command(update_spec.get("hint"))
                let = update_spec.get("let")
                if let is not None and not isinstance(let, dict):
                    raise TypeError("let must be a dict")

                is_operator_update = all(
                    isinstance(key, str) and key.startswith("$")
                    for key in update_document
                )
                if multi:
                    if not is_operator_update:
                        raise OperationFailure("replacement updates cannot be multi")
                    result = await collection.update_many(
                        query,
                        update_document,
                        upsert=upsert,
                        collation=update_spec.get("collation"),
                        array_filters=array_filters,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        bypass_document_validation=bypass_document_validation,
                        session=session,
                    )
                elif is_operator_update:
                    result = await collection.update_one(
                        query,
                        update_document,
                        upsert=upsert,
                        collation=update_spec.get("collation"),
                        array_filters=array_filters,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        bypass_document_validation=bypass_document_validation,
                        session=session,
                    )
                else:
                    result = await collection.replace_one(
                        query,
                        update_document,
                        upsert=upsert,
                        collation=update_spec.get("collation"),
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        bypass_document_validation=bypass_document_validation,
                        session=session,
                    )
                matched += result.matched_count
                modified += result.modified_count
                if result.upserted_id is not None:
                    upserted.append(
                        UpsertedWriteEntry(index=index, document_id=result.upserted_id)
                    )
            except Exception as exc:
                write_errors.append(
                    WriteErrorEntry(
                        index=index,
                        errmsg=str(exc),
                    )
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "update command failed",
                details=BulkWriteErrorDetails(
                    write_errors=write_errors,
                    matched_count=matched,
                    modified_count=modified,
                    upserted=upserted,
                ).to_document(),
            )
        return WriteCommandResult(
            count=matched,
            modified_count=modified,
            upserted=upserted or None,
        )

    async def _command_delete(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._require_collection_name(spec.get("delete"), "delete")
        deletes = self._normalize_delete_specs(spec.get("deletes"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        collection = self._database.get_collection(collection_name)
        deleted = 0
        write_errors: list[WriteErrorEntry] = []
        for index, delete_spec in enumerate(deletes):
            try:
                query = self._normalize_filter(delete_spec.get("q"))
                limit = delete_spec.get("limit", 0)
                if limit not in (0, 1):
                    raise TypeError("limit must be 0 or 1")
                hint = self._normalize_hint_from_command(delete_spec.get("hint"))
                let = delete_spec.get("let")
                if let is not None and not isinstance(let, dict):
                    raise TypeError("let must be a dict")
                if limit == 1:
                    result = await collection.delete_one(
                        query,
                        collation=delete_spec.get("collation"),
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                else:
                    result = await collection.delete_many(
                        query,
                        collation=delete_spec.get("collation"),
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                deleted += result.deleted_count
            except Exception as exc:
                write_errors.append(
                    WriteErrorEntry(
                        index=index,
                        errmsg=str(exc),
                    )
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "delete command failed",
                details=BulkWriteErrorDetails(
                    write_errors=write_errors,
                    removed_count=deleted,
                ).to_document(),
            )
        return WriteCommandResult(count=deleted)

    async def _command_find(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name, operation = self._compile_command_find_operation(
            spec,
            collection_field="find",
        )
        return await self._execute_find_command(
            collection_name,
            operation,
            session=session,
        )

    async def _execute_find_command(
        self,
        collection_name: str,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> CommandCursorResult:
        first_batch = await self._database.get_collection(collection_name)._build_cursor(
            operation,
            session=session,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._db_name}.{collection_name}",
            first_batch=first_batch,
        )

    async def _command_aggregate(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name, operation = self._compile_command_aggregate_operation(spec)
        return await self._execute_aggregate_command(
            collection_name,
            operation,
            session=session,
        )

    async def _execute_aggregate_command(
        self,
        collection_name: str,
        operation: AggregateOperation,
        *,
        session: ClientSession | None = None,
    ) -> CommandCursorResult:
        first_batch = await self._database.get_collection(
            collection_name
        )._build_aggregation_cursor(
            operation,
            session=session,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._db_name}.{collection_name}",
            first_batch=first_batch,
        )

    async def _command_explain(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        explain_spec = spec.get("explain")
        if not isinstance(explain_spec, dict) or not explain_spec:
            raise TypeError("explain must be a non-empty document")
        verbosity = spec.get("verbosity")
        if verbosity is not None and not isinstance(verbosity, str):
            raise TypeError("verbosity must be a string")

        explained_command_name = next(iter(explain_spec))
        if explained_command_name == "find":
            collection_name, operation = self._compile_command_find_operation(
                explain_spec,
                collection_field="find",
            )
            explanation = await self._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["batch_size"] = operation.batch_size
        elif explained_command_name == "aggregate":
            collection_name, operation = self._compile_command_aggregate_operation(
                explain_spec
            )
            explanation = await self._database.get_collection(
                collection_name
            )._build_aggregation_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
        elif explained_command_name == "update":
            collection_name = self._require_collection_name(
                explain_spec.get("update"),
                "update",
            )
            updates = self._normalize_update_specs(explain_spec.get("updates"))
            if len(updates) != 1:
                raise OperationFailure("explain update requires exactly one update specification")
            update_spec = updates[0]
            multi = update_spec.get("multi", False)
            if not isinstance(multi, bool):
                raise TypeError("multi must be a bool")
            operation = self._compile_command_update_selection_operation(
                update_spec,
                comment=explain_spec.get("comment"),
                max_time_ms=explain_spec.get("maxTimeMS"),
                limit=None if multi else 1,
            )
            explanation = await self._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "update"
            result["multi"] = multi
        elif explained_command_name == "delete":
            collection_name = self._require_collection_name(
                explain_spec.get("delete"),
                "delete",
            )
            deletes = self._normalize_delete_specs(explain_spec.get("deletes"))
            if len(deletes) != 1:
                raise OperationFailure("explain delete requires exactly one delete specification")
            delete_spec = deletes[0]
            limit = delete_spec.get("limit", 0)
            if limit not in (0, 1):
                raise TypeError("limit must be 0 or 1")
            operation = self._compile_command_delete_selection_operation(
                delete_spec,
                comment=explain_spec.get("comment"),
                max_time_ms=explain_spec.get("maxTimeMS"),
                limit=1 if limit == 1 else None,
            )
            explanation = await self._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "delete"
            result["limit"] = limit
        else:
            raise OperationFailure(
                f"Unsupported explain command: {explained_command_name}"
            )

        result.setdefault("ok", 1.0)
        if verbosity is not None:
            result["verbosity"] = verbosity
        return result

    async def _execute_find_and_modify(
        self,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        collection = self._database.get_collection(options.collection_name)
        if options.remove:
            return await self._execute_find_and_modify_remove(
                collection,
                options,
                session=session,
            )

        if options.update_spec is None:
            raise OperationFailure("findAndModify requires either remove or update")

        if self._is_operator_update(options.update_spec):
            return await self._execute_find_and_modify_operator_update(
                collection,
                options,
                session=session,
            )
        return await self._execute_find_and_modify_replacement(
            collection,
            options,
            session=session,
        )

    async def _execute_find_and_modify_remove(
        self,
        collection,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        if options.update_spec is not None:
            raise OperationFailure("findAndModify remove and update cannot be specified together")
        if options.upsert:
            raise OperationFailure("findAndModify remove does not support upsert")
        before = await collection.find_one_and_delete(
            options.query,
            projection=options.fields,
            collation=options.collation,
            sort=options.sort,
            hint=options.hint,
            comment=options.comment,
            max_time_ms=options.max_time_ms,
            let=options.let,
            session=session,
        )
        return FindAndModifyCommandResult(
            last_error_object=FindAndModifyLastErrorObject(
                count=0 if before is None else 1,
            ),
            value=before,
        )

    async def _find_and_modify_before_full(
        self,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._first_with_operation(
            options.collection_name,
            self._compile_find_and_modify_selection_operation(options),
            session=session,
        )

    async def _find_and_modify_fetch_upserted_value(
        self,
        collection_name: str,
        upserted_id: object,
        projection: Projection | None,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._first_with_operation(
            collection_name,
            self._compile_id_lookup_operation(
                upserted_id,
                projection=projection,
            ),
            session=session,
        )

    async def _execute_find_and_modify_operator_update(
        self,
        collection,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        before_full = await self._find_and_modify_before_full(options, session=session)
        return_document = ReturnDocument.AFTER if options.return_new else ReturnDocument.BEFORE

        if before_full is None and options.upsert:
            result = await collection.update_one(
                options.query,
                options.update_spec,
                upsert=True,
                collation=options.collation,
                sort=options.sort,
                array_filters=options.array_filters,
                hint=options.hint,
                comment=options.comment,
                let=options.let,
                bypass_document_validation=options.bypass_document_validation,
                session=session,
            )
            value = None
            if options.return_new:
                value = await self._find_and_modify_fetch_upserted_value(
                    options.collection_name,
                    result.upserted_id,
                    options.fields,
                    session=session,
                )
            return FindAndModifyCommandResult(
                last_error_object=FindAndModifyLastErrorObject(
                    count=1,
                    updated_existing=False,
                    upserted_id=result.upserted_id,
                ),
                value=value,
            )

        value = await collection.find_one_and_update(
            options.query,
            options.update_spec,
            projection=options.fields,
            collation=options.collation,
            sort=options.sort,
            upsert=options.upsert,
            return_document=return_document,
            array_filters=options.array_filters,
            hint=options.hint,
            comment=options.comment,
            max_time_ms=options.max_time_ms,
            let=options.let,
            bypass_document_validation=options.bypass_document_validation,
            session=session,
        )
        return FindAndModifyCommandResult(
            last_error_object=FindAndModifyLastErrorObject(
                count=0 if before_full is None and not options.upsert else 1,
                updated_existing=before_full is not None,
            ),
            value=value,
        )

    async def _execute_find_and_modify_replacement(
        self,
        collection,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        before_full = await self._find_and_modify_before_full(options, session=session)
        return_document = ReturnDocument.AFTER if options.return_new else ReturnDocument.BEFORE

        if before_full is None and options.upsert:
            result = await collection.replace_one(
                options.query,
                options.update_spec,
                upsert=True,
                collation=options.collation,
                sort=options.sort,
                hint=options.hint,
                comment=options.comment,
                let=options.let,
                bypass_document_validation=options.bypass_document_validation,
                session=session,
            )
            value = None
            if options.return_new:
                value = await self._find_and_modify_fetch_upserted_value(
                    options.collection_name,
                    result.upserted_id,
                    options.fields,
                    session=session,
                )
            return FindAndModifyCommandResult(
                last_error_object=FindAndModifyLastErrorObject(
                    count=1,
                    updated_existing=False,
                    upserted_id=result.upserted_id,
                ),
                value=value,
            )
        value = await collection.find_one_and_replace(
            options.query,
            options.update_spec,
            projection=options.fields,
            collation=options.collation,
            sort=options.sort,
            upsert=options.upsert,
            return_document=return_document,
            hint=options.hint,
            comment=options.comment,
            max_time_ms=options.max_time_ms,
            let=options.let,
            bypass_document_validation=options.bypass_document_validation,
            session=session,
        )
        return FindAndModifyCommandResult(
            last_error_object=FindAndModifyLastErrorObject(
                count=0 if before_full is None and not options.upsert else 1,
                updated_existing=before_full is not None,
            ),
            value=value,
        )

    @staticmethod
    def _is_operator_update(update_spec: dict[str, object]) -> bool:
        return all(
            isinstance(key, str) and key.startswith("$")
            for key in update_spec
        )

    async def _command_list_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._execute_list_indexes_command(
            self._require_collection_name(
                spec.get("listIndexes"),
                "listIndexes",
            ),
            comment=spec.get("comment"),
            session=session,
        )

    async def _execute_list_indexes_command(
        self,
        collection_name: str,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> CommandCursorResult:
        first_batch = await self._database.get_collection(collection_name).list_indexes(
            comment=comment,
            session=session,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._db_name}.{collection_name}",
            first_batch=first_batch,
        )

    async def _command_create_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._require_collection_name(
            spec.get("createIndexes"),
            "createIndexes",
        )
        indexes = self._normalize_index_models_from_command(spec.get("indexes"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        collection_names_before = set(
            await self._engine.list_collections(self._db_name, context=session)
        )
        collection = self._database.get_collection(collection_name)
        info_before = await collection.index_information(session=session)
        await collection.create_indexes(
            indexes,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            session=session,
        )
        info_after = await collection.index_information(session=session)
        return CreateIndexesCommandResult(
            num_indexes_before=len(info_before),
            num_indexes_after=len(info_after),
            created_collection_automatically=collection_name not in collection_names_before,
            note="all indexes already exist" if len(info_before) == len(info_after) else None,
        )

    async def _command_drop_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._require_collection_name(
            spec.get("dropIndexes"),
            "dropIndexes",
        )
        target = spec.get("index")
        collection = self._database.get_collection(collection_name)
        info_before = await collection.index_information(session=session)
        if target == "*":
            await collection.drop_indexes(comment=spec.get("comment"), session=session)
        elif isinstance(target, (str, list, tuple, dict)):
            await collection.drop_index(target, comment=spec.get("comment"), session=session)
        else:
            raise TypeError("index must be '*', a name, or a key specification")
        return DropIndexesCommandResult(
            previous_index_count=len(info_before),
            message=(
                f"non-_id indexes for collection {self._db_name}.{collection_name} dropped"
                if target == "*"
                else None
            ),
        )

    async def _command_drop_database(
        self,
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_names = await self._engine.list_collections(
            self._db_name,
            context=session,
        )
        for collection_name in collection_names:
            await self._engine.drop_collection(
                self._db_name,
                collection_name,
                context=session,
            )
        return DropDatabaseCommandResult(database_name=self._db_name)
