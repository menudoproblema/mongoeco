from typing import TYPE_CHECKING

from mongoeco.api.admin_parsing import (
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
from mongoeco.api.operations import FindOperation, compile_aggregate_operation, compile_find_operation
from mongoeco.api._async.database_commands import AsyncDatabaseCommandService
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.core.aggregation import _bson_document_size
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import BulkWriteError, CollectionInvalid, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
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
        await self._engine.create_collection(
            self._db_name,
            name,
            options=dict(options),
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
    ) -> CollectionStatsDocument:
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
        ).to_document()

    async def _database_stats(
        self,
        *,
        scale: int = 1,
        session: ClientSession | None = None,
    ) -> DatabaseStatsDocument:
        collection_names = await self._engine.list_collections(self._db_name, context=session)
        objects = 0
        data_size = 0
        indexes = 0
        for collection_name in collection_names:
            stats = await self._collection_stats(collection_name, scale=1, session=session)
            objects += int(stats["count"])
            data_size += int(stats["size"])
            indexes += int(stats["nindexes"])
        return DatabaseStatsSnapshot(
            db_name=self._db_name,
            collection_count=len(collection_names),
            object_count=objects,
            data_size=data_size,
            index_count=indexes,
            scale=scale,
        ).to_document()

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
                    size_on_disk=stats["storageSize"],
                    empty=(
                        int(stats["collections"]) == 0
                        and int(stats["objects"]) == 0
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
        ).to_document()

    async def _command_list_databases(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
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
        ).to_document()

    async def _command_create(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
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
        return OkResult().to_document()

    async def _command_drop(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("drop"), "drop")
        await self.drop_collection(collection_name, session=session)
        return NamespaceOkResult(
            namespace=f"{self._db_name}.{collection_name}",
        ).to_document()

    async def _command_rename_collection(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
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
        return OkResult().to_document()

    async def _command_count(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name, operation = self._compile_command_find_operation(
            {
                "count": spec.get("count"),
                "query": spec.get("query"),
                "projection": {"_id": 1},
                "hint": spec.get("hint"),
                "comment": spec.get("comment"),
                "maxTimeMS": spec.get("maxTimeMS"),
                "skip": spec.get("skip", 0),
                "limit": spec.get("limit"),
            },
            collection_field="count",
            filter_field="query",
            default_projection={"_id": 1},
        )
        count = await self._database.get_collection(
            collection_name
        )._engine_count_with_operation(
            operation,
            session=session,
        )
        return CountCommandResult(count=count).to_document()

    async def _command_distinct(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("distinct"),
            "distinct",
        )
        key = spec.get("key")
        if not isinstance(key, str) or not key:
            raise TypeError("key must be a non-empty string")
        query = self._normalize_filter(spec.get("query"))
        hint = self._normalize_hint_from_command(spec.get("hint"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        distinct_values: list[object] = []
        async for document in self._database.get_collection(collection_name).find(
            query,
            sort=None,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
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
                    self._mongodb_dialect.values_equal(existing, candidate)
                    for existing in distinct_values
                ):
                    distinct_values.append(candidate)
        return DistinctCommandResult(values=distinct_values).to_document()

    async def _command_insert(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("insert"), "insert")
        documents = self._normalize_insert_documents(spec.get("documents"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        collection = self._database.get_collection(collection_name)
        inserted = 0
        write_errors: list[dict[str, object]] = []
        for index, document in enumerate(documents):
            try:
                await collection.insert_one(document, session=session)
                inserted += 1
            except Exception as exc:
                write_errors.append(
                    {
                        "index": index,
                        "errmsg": str(exc),
                    }
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "insert command failed",
                details={
                    "writeErrors": write_errors,
                    "n": inserted,
                },
            )
        return WriteCommandResult(count=inserted).to_document()

    async def _command_update(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("update"), "update")
        updates = self._normalize_update_specs(spec.get("updates"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        collection = self._database.get_collection(collection_name)
        matched = 0
        modified = 0
        upserted: list[dict[str, object]] = []
        write_errors: list[dict[str, object]] = []
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
                        array_filters=array_filters,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                elif is_operator_update:
                    result = await collection.update_one(
                        query,
                        update_document,
                        upsert=upsert,
                        array_filters=array_filters,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                else:
                    result = await collection.replace_one(
                        query,
                        update_document,
                        upsert=upsert,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                matched += result.matched_count
                modified += result.modified_count
                if result.upserted_id is not None:
                    upserted.append({"index": index, "_id": result.upserted_id})
            except Exception as exc:
                write_errors.append(
                    {
                        "index": index,
                        "errmsg": str(exc),
                    }
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "update command failed",
                details={
                    "writeErrors": write_errors,
                    "n": matched,
                    "nModified": modified,
                    "upserted": upserted,
                },
            )
        return WriteCommandResult(
            count=matched,
            modified_count=modified,
            upserted=upserted or None,
        ).to_document()

    async def _command_delete(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("delete"), "delete")
        deletes = self._normalize_delete_specs(spec.get("deletes"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        collection = self._database.get_collection(collection_name)
        deleted = 0
        write_errors: list[dict[str, object]] = []
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
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                else:
                    result = await collection.delete_many(
                        query,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                deleted += result.deleted_count
            except Exception as exc:
                write_errors.append(
                    {
                        "index": index,
                        "errmsg": str(exc),
                    }
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "delete command failed",
                details={
                    "writeErrors": write_errors,
                    "n": deleted,
                },
            )
        return WriteCommandResult(count=deleted).to_document()

    async def _command_find(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name, operation = self._compile_command_find_operation(
            spec,
            collection_field="find",
        )
        first_batch = await self._database.get_collection(collection_name)._build_cursor(
            operation,
            session=session,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._db_name}.{collection_name}",
            first_batch=first_batch,
        ).to_document()

    async def _command_aggregate(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("aggregate"),
            "aggregate",
        )
        pipeline = spec.get("pipeline")
        if not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")
        cursor_spec = spec.get("cursor", {})
        if not isinstance(cursor_spec, dict):
            raise TypeError("cursor must be a document")
        batch_size = self._normalize_batch_size_from_command(
            cursor_spec.get("batchSize")
        )
        operation = compile_aggregate_operation(
            pipeline,
            hint=self._normalize_hint_from_command(spec.get("hint")),
            comment=spec.get("comment"),
            max_time_ms=self._normalize_max_time_ms_from_command(spec.get("maxTimeMS")),
            batch_size=batch_size,
            let=spec.get("let"),
        )
        first_batch = await self._database.get_collection(
            collection_name
        )._build_aggregation_cursor(
            operation,
            session=session,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._db_name}.{collection_name}",
            first_batch=first_batch,
        ).to_document()

    async def _command_explain(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
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
            collection_name = self._require_collection_name(
                explain_spec.get("aggregate"),
                "aggregate",
            )
            pipeline = explain_spec.get("pipeline")
            if not isinstance(pipeline, list):
                raise TypeError("pipeline must be a list")
            cursor_spec = explain_spec.get("cursor", {})
            if not isinstance(cursor_spec, dict):
                raise TypeError("cursor must be a document")
            operation = compile_aggregate_operation(
                pipeline,
                hint=self._normalize_hint_from_command(explain_spec.get("hint")),
                comment=explain_spec.get("comment"),
                max_time_ms=self._normalize_max_time_ms_from_command(
                    explain_spec.get("maxTimeMS")
                ),
                batch_size=self._normalize_batch_size_from_command(
                    cursor_spec.get("batchSize")
                ),
                let=explain_spec.get("let"),
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
            _, operation = self._compile_command_find_operation(
                {
                    "update": collection_name,
                    "q": update_spec.get("q"),
                    "projection": {"_id": 1},
                    "hint": update_spec.get("hint"),
                    "comment": explain_spec.get("comment"),
                    "maxTimeMS": explain_spec.get("maxTimeMS"),
                    "limit": None if multi else 1,
                },
                collection_field="update",
                filter_field="q",
                default_projection={"_id": 1},
                batch_size_field=None,
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
            _, operation = self._compile_command_find_operation(
                {
                    "delete": collection_name,
                    "q": delete_spec.get("q"),
                    "projection": {"_id": 1},
                    "hint": delete_spec.get("hint"),
                    "comment": explain_spec.get("comment"),
                    "maxTimeMS": explain_spec.get("maxTimeMS"),
                    "limit": 1 if limit == 1 else None,
                },
                collection_field="delete",
                filter_field="q",
                default_projection={"_id": 1},
                batch_size_field=None,
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

    async def _command_find_and_modify(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("findAndModify"),
            "findAndModify",
        )
        query = self._normalize_filter(spec.get("query"))
        sort = self._normalize_sort_document(spec.get("sort"))
        fields = spec.get("fields")
        remove = spec.get("remove", False)
        if not isinstance(remove, bool):
            raise TypeError("remove must be a bool")
        return_new = spec.get("new", False)
        if not isinstance(return_new, bool):
            raise TypeError("new must be a bool")
        upsert = spec.get("upsert", False)
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be a bool")
        array_filters = spec.get("arrayFilters")
        if array_filters is not None and (
            not isinstance(array_filters, list)
            or not all(is_filter(item) for item in array_filters)
        ):
            raise TypeError("arrayFilters must be a list of dicts")
        hint = self._normalize_hint_from_command(spec.get("hint"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        let = spec.get("let")
        if let is not None and not isinstance(let, dict):
            raise TypeError("let must be a dict")

        update_spec = spec.get("update")
        collection = self._database.get_collection(collection_name)
        if remove:
            if update_spec is not None:
                raise OperationFailure("findAndModify remove and update cannot be specified together")
            if upsert:
                raise OperationFailure("findAndModify remove does not support upsert")
            before = await collection.find_one_and_delete(
                query,
                projection=fields,
                sort=sort,
                hint=hint,
                comment=spec.get("comment"),
                max_time_ms=max_time_ms,
                let=let,
                session=session,
            )
            return FindAndModifyCommandResult(
                last_error_object=FindAndModifyLastErrorObject(
                    count=0 if before is None else 1,
                ),
                value=before,
            ).to_document()

        if update_spec is None:
            raise OperationFailure("findAndModify requires either remove or update")

        before_full = await collection.find(
            query,
            sort=sort,
            limit=1,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            session=session,
        ).first()
        return_document = ReturnDocument.AFTER if return_new else ReturnDocument.BEFORE

        if isinstance(update_spec, dict) and all(
            isinstance(key, str) and key.startswith("$")
            for key in update_spec
        ):
            if before_full is None and upsert:
                result = await collection.update_one(
                    query,
                    update_spec,
                    upsert=True,
                    sort=sort,
                    array_filters=array_filters,
                    hint=hint,
                    comment=spec.get("comment"),
                    let=let,
                    session=session,
                )
                value = None
                if return_new:
                    value = await collection.find(
                        {"_id": result.upserted_id},
                        fields,
                        limit=1,
                        session=session,
                    ).first()
                return FindAndModifyCommandResult(
                    last_error_object=FindAndModifyLastErrorObject(
                        count=1,
                        updated_existing=False,
                        upserted_id=result.upserted_id,
                    ),
                    value=value,
                ).to_document()
            value = await collection.find_one_and_update(
                query,
                update_spec,
                projection=fields,
                sort=sort,
                upsert=upsert,
                return_document=return_document,
                array_filters=array_filters,
                hint=hint,
                comment=spec.get("comment"),
                max_time_ms=max_time_ms,
                let=let,
                session=session,
            )
            return FindAndModifyCommandResult(
                last_error_object=FindAndModifyLastErrorObject(
                    count=0 if before_full is None and not upsert else 1,
                    updated_existing=before_full is not None,
                ),
                value=value,
            ).to_document()

        if not isinstance(update_spec, dict):
            raise TypeError("update must be a document")

        if before_full is None and upsert:
            result = await collection.replace_one(
                query,
                update_spec,
                upsert=True,
                sort=sort,
                hint=hint,
                comment=spec.get("comment"),
                let=let,
                session=session,
            )
            value = None
            if return_new:
                value = await collection.find(
                    {"_id": result.upserted_id},
                    fields,
                    limit=1,
                    session=session,
                ).first()
            return FindAndModifyCommandResult(
                last_error_object=FindAndModifyLastErrorObject(
                    count=1,
                    updated_existing=False,
                    upserted_id=result.upserted_id,
                ),
                value=value,
            ).to_document()

        value = await collection.find_one_and_replace(
            query,
            update_spec,
            projection=fields,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            let=let,
            session=session,
        )
        return FindAndModifyCommandResult(
            last_error_object=FindAndModifyLastErrorObject(
                count=0 if before_full is None and not upsert else 1,
                updated_existing=before_full is not None,
            ),
            value=value,
        ).to_document()

    async def _command_list_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("listIndexes"),
            "listIndexes",
        )
        first_batch = await self._database.get_collection(collection_name).list_indexes(
            comment=spec.get("comment"),
            session=session,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._db_name}.{collection_name}",
            first_batch=first_batch,
        ).to_document()

    async def _command_create_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
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
        ).to_document()

    async def _command_drop_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
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
        ).to_document()

    async def _command_drop_database(
        self,
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
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
        return DropDatabaseCommandResult(database_name=self._db_name).to_document()
