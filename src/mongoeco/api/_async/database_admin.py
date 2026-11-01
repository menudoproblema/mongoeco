from collections.abc import Mapping
from typing import TYPE_CHECKING

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
from mongoeco.api._command_bson import command_boolean
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
from mongoeco.api.bson_input import encode_bson_request
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.api.public_api import (
    ARG_UNSET,
    DATABASE_LIST_COLLECTION_NAMES_SPEC,
    DATABASE_LIST_COLLECTIONS_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.core._public_materialization import (
    materialize_command_response,
)
from mongoeco.core.bson_scalars import truncate_bson_int64
from mongoeco.core.bson_values import matches_bson_type
from mongoeco.core.change_events import validate_post_image_options
from mongoeco.core.collation import normalize_collation
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.server_errors import ErrorScope, server_error
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.semantic_core import compile_collection_validation_semantics
from mongoeco.errors import BulkWriteError, CollectionInvalid, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.session_guards import ensure_session_can_use_engine
from mongoeco.types import (
    BulkWriteErrorDetails,
    CodecOptions,
    CollectionListingDocument,
    CollectionListingSnapshot,
    CollectionStatsDocument,
    CollectionStatsSnapshot,
    CollectionValidationDocument,
    CollectionValidationSnapshot,
    CommandCursorResult,
    CountCommandResult,
    CreateIndexesCommandResult,
    DatabaseListingDocument,
    DatabaseListingSnapshot,
    DatabaseStatsDocument,
    DatabaseStatsSnapshot,
    DistinctCommandResult,
    Document,
    DropDatabaseCommandResult,
    DropIndexesCommandResult,
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
    UuidRepresentation,
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
    def _ensure_session_active(session: object | None) -> None:
        if session is not None:
            ensure_active = getattr(session, "ensure_active", None)
            if callable(ensure_active):
                ensure_active()

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
        return self._normalize_list_indexes_document(document)

    def _validate_create_collection_options(self, options: dict[str, object]) -> None:
        for field in (
            "validator",
            "validationLevel",
            "validationAction",
            "collation",
            "capped",
            "size",
            "max",
            "changeStreamPreAndPostImages",
        ):
            if field in options and options[field] is None:
                options.pop(field)
        for field, kind, description in (
            ("validator", "object", "document"),
            ("collation", "object", "document"),
            ("validationLevel", "string", "string"),
            ("validationAction", "string", "string"),
        ):
            if field in options and not matches_bson_type(options[field], kind):
                message = f"{field} must be a {description}"
                raise server_error(message, code=14, scope=ErrorScope.COMMAND)
        validate_post_image_options(options.get("changeStreamPreAndPostImages"))
        if options.get("changeStreamPreAndPostImages") is None:
            options.pop("changeStreamPreAndPostImages", None)
        capped = command_boolean(options.get("capped"), "capped", numeric=True)
        if "capped" in options:
            options["capped"] = capped
        for field_name in ("size", "max"):
            if field_name not in options:
                continue
            value = truncate_bson_int64(options[field_name])
            if value is None:
                message = f"{field_name} must be a number representable as int64"
                raise server_error(message, code=14, scope=ErrorScope.COMMAND)
            if field_name == "size" and value <= 0:
                message = "size must be > 0"
                code = 51024 if self._mongodb_dialect.server_version == "7.0" else 2
                raise server_error(message, code=code, scope=ErrorScope.COMMAND)
            options[field_name] = value
        if capped and "size" not in options:
            message = "capped collections require a positive size option"
            raise server_error(message, code=72, scope=ErrorScope.COMMAND)
        if not capped and any(field in options for field in ("size", "max")):
            message = "size and max require a capped collection"
            raise server_error(message, code=72, scope=ErrorScope.COMMAND)

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
        command = self._prepare_list_collections(options, names_only=True)
        result = await self._read_commands.execute_list_collections(
            command, session=options.get("session"),
        )
        return [document["name"] for document in result.first_batch]

    def _prepare_list_collections(self, options, *, names_only=False):
        filter_spec = options.get("filter_spec")
        if (
            names_only and filter_spec is not None
            and not isinstance(filter_spec, Mapping)
        ):
            message = "filter must be a mapping"
            raise TypeError(message)
        ensure_session_can_use_engine(self._engine, options.get("session"))
        command = {"listCollections": 1, "cursor": {}}
        if names_only and (
            filter_spec is None or not filter_spec
            or (len(filter_spec) == 1 and "name" in filter_spec)
        ):
            command["nameOnly"] = True
        if filter_spec is not None:
            command["filter"] = filter_spec
        if options.get("comment") is not None:
            command["comment"] = options["comment"]
        admitted = encode_bson_request(
            command, codec_options=CodecOptions(
                uuid_representation=UuidRepresentation.UNSPECIFIED,
            ),
        )
        return normalize_list_collections_options(admitted)

    async def _list_collection_snapshots(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[CollectionListingSnapshot]:
        self._ensure_session_active(session)
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
        command = self._prepare_list_collections(options)

        async def _load() -> list[CollectionListingDocument]:
            result = await self._read_commands.execute_list_collections(
                command, session=options.get("session"),
            )
            return [
                materialize_command_response(
                    document, codec_options=CodecOptions(
                        uuid_representation=UuidRepresentation.UNSPECIFIED,
                    ),
                )
                for document in result.first_batch
            ]

        return AsyncListingCursor(_load)

    async def create_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
        **options: object,
    ):
        ensure_session_can_use_engine(self._engine, session)
        check_exists = options.pop("check_exists", True)
        if check_exists and name in await self._engine.list_collections(
            self._db_name, context=session
        ):
            message = f"collection {name} already exists"
            raise CollectionInvalid(message)
        collection_options = {
            key: options.pop(key)
            for key in (
                "codec_options",
                "read_preference",
                "write_concern",
                "read_concern",
            )
            if key in options
        }
        collection = self._database.get_collection(name, **collection_options)
        collation = options.pop("collation", None)
        if collation is not None and not isinstance(collation, dict):
            message = "collation must be a dict or None"
            raise TypeError(message)
        if "size" in options:
            # PyMongo converts size before the create request is BSON encoded.
            options["size"] = float(options["size"])
        if collation is not None:
            options["collation"] = collation
        encoded = encode_bson_request(
            {"create": name, **options}, codec_options=collection.codec_options
        )
        await self._create_collection_prepared(
            name,
            session=session,
            **{key: value for key, value in encoded.items() if key != "create"},
        )
        # The SDK decodes the create reply even though it returns a collection.
        materialize_command_response(
            {"ok": 1.0}, codec_options=collection.codec_options
        )
        return collection

    async def _create_collection_prepared(self, name, *, session=None, **options):
        self._ensure_session_active(session)
        normalized_options = {
            key: value
            for key, value in options.items()
            if key not in {"comment", "maxTimeMS", "writeConcern", "readConcern"}
        }
        self._validate_create_collection_options(normalized_options)
        if "viewOn" in normalized_options or normalized_options.get("capped"):
            message = (
                "Views and capped collections are outside Mongoeco's supported subset"
            )
            raise server_error(message, code=115, reason="capability_unsupported")
        compile_collection_validation_semantics(
            normalized_options,
            dialect=self._mongodb_dialect,
        )
        try:
            await self._engine.create_collection(
                self._db_name,
                name,
                options=normalized_options,
                context=session,
            )
        except CollectionInvalid as error:
            existing = await self._engine.collection_options(
                self._db_name, name, context=session
            )
            if not self._namespace_admin.collection_options_match(
                existing or {}, normalized_options
            ):
                raise server_error(
                    str(error), code=48, scope=ErrorScope.COMMAND
                ) from error
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
        name = (
            name_or_collection if isinstance(name_or_collection, str)
            else resolve_collection_reference(name_or_collection, "name_or_collection")
        )
        command = {"validate": name, "scandata": scandata, "full": full}
        if comment is not None:
            command["comment"] = comment
        if background is not None:
            command["background"] = background
        result = await self._commands.command(command, session=session)
        if not result.get("valid", False):
            message = f"{name} invalid: {result!r}"
            raise CollectionInvalid(message)
        return result

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

    async def _command_coll_mod(self, spec, *, session=None):
        self._ensure_session_active(session)
        name = self._require_collection_name(spec.get("collMod"), "collMod")
        allowed = {
            "collMod",
            "$db",
            "changeStreamPreAndPostImages",
            "validator",
            "validationLevel",
            "validationAction",
            "comment",
            "writeConcern",
        }
        if spec.keys() - allowed:
            message = "Unsupported collMod option"
            raise server_error(message, code=40415)
        validate_post_image_options(spec.get("changeStreamPreAndPostImages"))
        changes = {
            key: value
            for key, value in spec.items()
            if key in {"validator", "validationLevel", "validationAction"}
            or (key == "changeStreamPreAndPostImages" and value is not None)
        }
        modifier = getattr(self._engine, "_modify_collection_options", None)
        if modifier is None:
            message = "collMod validation options require engine support"
            raise server_error(message, code=59)
        await modifier(
            self._db_name,
            name,
            options=changes,
            context=session,
            dialect=self._mongodb_dialect,
        )
        return {"ok": 1.0}

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
        execution_context=None,
    ) -> object:
        return await self._write_commands.command_update(
            spec,
            session=session,
            execution_context=execution_context,
        )

    async def _command_delete(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
        execution_context=None,
    ) -> object:
        return await self._write_commands.command_delete(
            spec,
            session=session,
            execution_context=execution_context,
        )

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
