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
from mongoeco.api.public_api import (
    ARG_UNSET,
    DATABASE_LIST_COLLECTION_NAMES_SPEC,
    DATABASE_LIST_COLLECTIONS_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.api._async._database_admin_command_compiler import (
    DatabaseAdminCommandCompiler,
)
from mongoeco.api._async._database_admin_namespace import DatabaseNamespaceAdminService
from mongoeco.api._async._database_admin_read_commands import (
    DatabaseAdminReadCommandService,
)
from mongoeco.api._async._database_admin_routing import DatabaseAdminRoutingService
from mongoeco.api._async._database_admin_write_commands import (
    DatabaseAdminWriteCommandService,
)
from mongoeco.api._async.database_commands import AsyncDatabaseCommandService
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.core.collation import normalize_collation
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.semantic_core import compile_collection_validation_semantics
from mongoeco.errors import BulkWriteError, OperationFailure
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

_FILTER_UNSET = ARG_UNSET


class AsyncDatabaseAdminService:
    def __init__(self, database: "AsyncDatabase"):
        self._database = database
        self._commands = AsyncDatabaseCommandService(self)
        self._command_compiler = DatabaseAdminCommandCompiler(self)
        self._namespace_admin = DatabaseNamespaceAdminService(self)
        self._read_commands = DatabaseAdminReadCommandService(self)
        self._write_commands = DatabaseAdminWriteCommandService(self)
        self._routing = DatabaseAdminRoutingService(self)

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

    @staticmethod
    def _normalize_list_indexes_document(document: dict[str, object]) -> dict[str, object]:
        normalized = dict(document)
        normalized.pop("fields", None)
        return normalized

    def _normalize_list_indexes_document_for_collection(
        self,
        collection_name: str,
        document: dict[str, object],
    ) -> dict[str, object]:
        normalized = self._normalize_list_indexes_document(document)
        normalized.setdefault("ns", f"{self._db_name}.{collection_name}")
        return normalized

    @staticmethod
    def _validate_create_collection_options(options: dict[str, object]) -> None:
        capped = options.get("capped")
        if capped is not None and not isinstance(capped, bool):
            raise TypeError("capped must be a bool")

        for field_name in ("size", "max"):
            value = options.get(field_name)
            if value is None:
                continue
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{field_name} must be a positive integer")
            if value <= 0:
                raise ValueError(f"{field_name} must be > 0")

        if capped and "size" not in options:
            raise OperationFailure("capped collections require a positive size option")

    async def list_collection_names(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> list[str]:
        options = normalize_public_operation_arguments(
            DATABASE_LIST_COLLECTION_NAMES_SPEC,
            explicit={"filter_spec": filter_spec, "session": session},
            extra_kwargs={"filter": filter, **kwargs},
        )
        normalized_filter = (
            None
            if options.get("filter_spec") is None
            else self._normalize_filter(options["filter_spec"])
        )
        if normalized_filter is None:
            return await self._engine.list_collections(
                self._db_name,
                context=options.get("session"),
            )
        documents = await self.list_collections(
            normalized_filter,
            session=options.get("session"),
        ).to_list()
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
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> AsyncListingCursor:
        options = normalize_public_operation_arguments(
            DATABASE_LIST_COLLECTIONS_SPEC,
            explicit={"filter_spec": filter_spec, "session": session},
            extra_kwargs={"filter": filter, **kwargs},
        )
        normalized_filter = self._normalize_filter(options.get("filter_spec"))

        async def _load() -> list[CollectionListingDocument]:
            documents = [
                snapshot.to_document()
                for snapshot in await self._list_collection_snapshots(
                    session=options.get("session")
                )
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
        self._validate_create_collection_options(normalized_options)
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
        await self._database.get_collection(name).drop(session=session)

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
        return self._command_compiler.compile_find_operation(
            spec,
            collection_field=collection_field,
            filter_field=filter_field,
            projection_field=projection_field,
            sort_field=sort_field,
            hint_field=hint_field,
            comment_field=comment_field,
            max_time_ms_field=max_time_ms_field,
            skip_field=skip_field,
            limit_field=limit_field,
            batch_size_field=batch_size_field,
            default_projection=default_projection,
            default_limit=default_limit,
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
        return self._command_compiler.compile_aggregate_operation(
            spec,
            collection_field=collection_field,
            pipeline_field=pipeline_field,
            cursor_field=cursor_field,
            batch_size_field=batch_size_field,
            hint_field=hint_field,
            comment_field=comment_field,
            max_time_ms_field=max_time_ms_field,
            allow_disk_use_field=allow_disk_use_field,
            let_field=let_field,
        )

    def _compile_command_update_selection_operation(
        self,
        update_spec: dict[str, object],
        *,
        comment: object | None,
        max_time_ms: object | None,
        limit: int | None,
    ) -> FindOperation:
        return self._command_compiler.compile_update_selection_operation(
            update_spec,
            comment=comment,
            max_time_ms=max_time_ms,
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
        return self._command_compiler.compile_delete_selection_operation(
            delete_spec,
            comment=comment,
            max_time_ms=max_time_ms,
            limit=limit,
        )

    def _compile_command_count_operation(
        self,
        spec: dict[str, object],
    ) -> tuple[str, FindOperation]:
        return self._command_compiler.compile_count_operation(spec)

    def _compile_command_distinct_operation(
        self,
        spec: dict[str, object],
    ) -> tuple[str, str, FindOperation]:
        return self._command_compiler.compile_distinct_operation(spec)

    def _compile_find_and_modify_selection_operation(
        self,
        options: FindAndModifyCommandOptions,
        *,
        projection: Projection | None = None,
        limit: int | None = 1,
    ) -> FindOperation:
        return self._command_compiler.compile_find_and_modify_selection_operation(
            options,
            projection=projection,
            limit=limit,
        )

    def _compile_id_lookup_operation(
        self,
        document_id: object,
        *,
        projection: Projection | None = None,
    ) -> FindOperation:
        return self._command_compiler.compile_id_lookup_operation(
            document_id,
            projection=projection,
        )

    async def _first_with_operation(
        self,
        collection_name: str,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        cursor = self._database.get_collection(collection_name)._build_cursor(
            operation,
            session=session,
        )
        if hasattr(cursor, "first"):
            return await cursor.first()
        first_batch = await cursor.to_list()
        return first_batch[0] if first_batch else None

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
        return await self._namespace_admin.collection_stats(
            collection_name,
            scale=scale,
            session=session,
        )

    async def _database_stats(
        self,
        *,
        scale: int = 1,
        session: ClientSession | None = None,
    ) -> DatabaseStatsSnapshot:
        return await self._namespace_admin.database_stats(
            scale=scale,
            session=session,
        )

    async def _list_database_snapshots(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[DatabaseListingSnapshot]:
        return await self._namespace_admin.list_database_snapshots(session=session)

    async def _list_database_documents(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[DatabaseListingDocument]:
        return await self._namespace_admin.list_database_documents(session=session)

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
        return await self._namespace_admin.build_collection_validation_snapshot(
            name_or_collection,
            scandata=scandata,
            full=full,
            background=background,
            session=session,
            comment=comment,
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
        return await self._read_commands.command_list_collections(spec, session=session)

    async def _command_list_databases(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._read_commands.command_list_databases(spec, session=session)

    async def _command_create(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_create(spec, session=session)

    async def _command_drop(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_drop(spec, session=session)

    async def _command_rename_collection(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_rename_collection(spec, session=session)

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

    async def _command_db_hash(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._read_commands.command_db_hash(spec, session=session)

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
        return await self._read_commands.execute_distinct_command(
            collection_name,
            key,
            operation,
            session=session,
        )

    async def _execute_count_command(
        self,
        collection_name: str,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> CountCommandResult:
        return await self._read_commands.execute_count_command(
            collection_name,
            operation,
            session=session,
        )

    async def _execute_db_hash_command(
        self,
        collections: tuple[str, ...],
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ):
        return await self._read_commands.execute_db_hash_command(
            collections,
            comment=comment,
            session=session,
        )

    async def _command_insert(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_insert(spec, session=session)

    async def _command_update(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_update(spec, session=session)

    async def _command_delete(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_delete(spec, session=session)

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
        return await self._read_commands.execute_find_command(
            collection_name,
            operation,
            session=session,
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
        return await self._read_commands.execute_aggregate_command(
            collection_name,
            operation,
            session=session,
        )

    async def _command_explain(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._read_commands.command_explain(spec, session=session)

    async def _execute_find_and_modify(
        self,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        return await self._write_commands.execute_find_and_modify(options, session=session)

    async def _execute_find_and_modify_remove(
        self,
        collection,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        return await self._write_commands.execute_find_and_modify_remove(
            collection,
            options,
            session=session,
        )

    async def _find_and_modify_before_full(
        self,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._write_commands.find_and_modify_before_full(
            options,
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
        return await self._write_commands.find_and_modify_fetch_upserted_value(
            collection_name,
            upserted_id,
            projection,
            session=session,
        )

    async def _execute_find_and_modify_operator_update(
        self,
        collection,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        return await self._write_commands.execute_find_and_modify_operator_update(
            collection,
            options,
            session=session,
        )

    async def _execute_find_and_modify_replacement(
        self,
        collection,
        options: FindAndModifyCommandOptions,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        return await self._write_commands.execute_find_and_modify_replacement(
            collection,
            options,
            session=session,
        )

    @staticmethod
    def _is_operator_update(update_spec: dict[str, object]) -> bool:
        return DatabaseAdminWriteCommandService.is_operator_update(update_spec)

    async def _command_list_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._read_commands.command_list_indexes(spec, session=session)

    async def _execute_list_indexes_command(
        self,
        collection_name: str,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> CommandCursorResult:
        return await self._read_commands.execute_list_indexes_command(
            collection_name,
            comment=comment,
            session=session,
        )

    async def _command_create_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_create_indexes(spec, session=session)

    async def _command_drop_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_drop_indexes(spec, session=session)

    async def _command_drop_database(
        self,
        *,
        session: ClientSession | None = None,
    ) -> object:
        return await self._write_commands.command_drop_database(session=session)
