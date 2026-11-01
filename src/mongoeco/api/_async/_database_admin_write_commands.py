from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.api._async import _collection_indexing, _collection_modify
from mongoeco.api._async._write_command_validation import (
    validate_delete_command_shapes as _validate_delete_command_shapes,
    validate_update_command_shapes as _validate_update_command_shapes,
    write_command_acknowledged,
)
from mongoeco.api._async._write_failures import record_write_failure
from mongoeco.api._change_delivery import change_delivery_scope
from mongoeco.api._command_bson import (
    command_boolean,
    command_type_error,
    rename_collection_options,
    validate_command_let,
    validate_write_fields,
)
from mongoeco.api.admin_parsing import normalize_drop_collection_name
from mongoeco.api.public_api import ARG_UNSET
from mongoeco.core._public_materialization import materialize_public_value
from mongoeco.core.server_errors import ErrorScope, server_error
from mongoeco.engines.adapter import EngineSpiAdapter
from mongoeco.engines.results import FindAndModifyOutcome
from mongoeco.errors import OperationFailure, WriteError
from mongoeco.types import (
    CreateIndexesCommandResult,
    Document,
    DropDatabaseCommandResult,
    FindAndModifyCommandResult,
    FindAndModifyLastErrorObject,
    NamespaceOkResult,
    OkResult,
    ReturnDocument,
    UpsertedWriteEntry,
    WriteCommandResult,
    WriteErrorEntry,
)


if TYPE_CHECKING:
    from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
    from mongoeco.core.expression_context import ExpressionExecutionContext
    from mongoeco.session import ClientSession


_FAILED_TO_PARSE = 9
_MISSING_REQUIRED_FIELD = 40414


def _write_command_result(
    result: WriteCommandResult, *, acknowledged: bool
) -> WriteCommandResult:
    if not acknowledged:
        # Execute the complete ordered/unordered write first. Only its reply
        # loses counters and per-write errors; command parsing still raises.
        return WriteCommandResult(
            count=0,
            modified_count=0 if result.modified_count is not None else None,
        )
    return result


class DatabaseAdminWriteCommandService:
    def __init__(self, admin: AsyncDatabaseAdminService) -> None:
        self._admin = admin

    async def command_create(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._admin._require_collection_name(
            spec.get("create"), "create"
        )
        options = {
            key: value for key, value in spec.items() if key not in {"create", "$db"}
        }
        await self._admin._create_collection_prepared(
            collection_name,
            session=session,
            **options,
        )
        return OkResult()

    async def command_drop(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = normalize_drop_collection_name(
            spec.get("drop"), dialect=self._admin._mongodb_dialect,
        )
        await self._admin._database.get_collection(collection_name)._drop_prepared(
            session=session,
        )
        return NamespaceOkResult(
            namespace=f"{self._admin._db_name}.{collection_name}",
        )

    async def command_rename_collection(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        (source_db, source_name), (target_db, target_name), drop_target = (
            rename_collection_options(spec, self._admin._db_name)
        )
        return await self.execute_rename_namespaces(
            (source_db, source_name), (target_db, target_name),
            drop_target=drop_target, session=session,
        )

    async def execute_rename_namespaces(
        self, source, target, *, drop_target=False, session=None,
    ):
        """Execute parsed namespaces with the existing DDL delivery scope."""
        source_db, source_name = source
        target_db, target_name = target
        if source_db != target_db:
            message = "renameCollection between databases is not supported locally"
            raise server_error(message, code=20)
        database = self._admin._database
        adapter = database._validated_engine_spi
        if adapter is None:
            adapter = database._validated_engine_spi = EngineSpiAdapter(
                self._admin._engine
            )
        options = {}
        if drop_target:
            rename = getattr(self._admin._engine, "_rename_collection", None)
            if not callable(rename):
                message = "atomic dropTarget requires engine support"
                raise server_error(message, code=20)
            options = {"drop_target": True}
        else:
            rename = self._admin._engine.rename_collection
        with change_delivery_scope(adapter, database._change_hub, session=session):
            await rename(
                source_db, source_name, target_name, context=session, **options
            )
        return OkResult()

    async def command_insert(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._admin._require_collection_name(
            spec.get("insert"), "insert"
        )
        documents = self._admin._normalize_insert_documents(spec.get("documents"))
        ordered = self._admin._normalize_ordered_from_command(spec.get("ordered", True))
        bypass_document_validation = spec.get("bypassDocumentValidation", False)
        bypass_document_validation = command_boolean(
            bypass_document_validation, "bypassDocumentValidation", numeric=True
        )
        acknowledged = write_command_acknowledged(
            spec, dialect=self._admin._mongodb_dialect.key
        )
        collection = self._admin._database.get_collection(collection_name)
        inserted = 0
        write_errors: list[WriteErrorEntry] = []
        command_failure = None
        for index, document in enumerate(documents):
            try:
                await collection._insert_one(
                    document,
                    bypass_document_validation=bypass_document_validation,
                    session=session,
                )
                inserted += 1
            except (WriteError, OperationFailure, TypeError, ValueError) as exc:
                command_failure = record_write_failure(
                    write_errors, exc, index=index, first_failure=command_failure
                )
                if ordered:
                    break
        if command_failure is not None:
            raise command_failure
        return _write_command_result(
            WriteCommandResult(count=inserted, write_errors=write_errors or None),
            acknowledged=acknowledged,
        )

    async def command_update(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
        execution_context: ExpressionExecutionContext | None = None,
    ) -> object:
        validate_write_fields(spec, "update")
        collection_name = self._admin._require_collection_name(
            spec.get("update"), "update"
        )
        updates = self._admin._normalize_update_specs(spec.get("updates"))
        _validate_update_command_shapes(updates, self._admin._mongodb_dialect.key)
        ordered = self._admin._normalize_ordered_from_command(spec.get("ordered", True))
        bypass_document_validation = spec.get("bypassDocumentValidation", False)
        bypass_document_validation = command_boolean(
            bypass_document_validation, "bypassDocumentValidation", numeric=True
        )
        collection = self._admin._database.get_collection(collection_name)
        command_context = (
            execution_context or self._admin._database._new_execution_context()
        )
        command_let = spec.get("let")
        validate_command_let(command_let)
        acknowledged = write_command_acknowledged(
            spec, dialect=self._admin._mongodb_dialect.key
        )
        matched = 0
        modified = 0
        upserted: list[UpsertedWriteEntry] = []
        write_errors: list[WriteErrorEntry] = []
        command_failure = None
        for index, update_spec in enumerate(updates):
            try:
                query = self._admin._normalize_filter(update_spec.get("q"))
                update_document = update_spec.get("u")
                multi = command_boolean(update_spec.get("multi"), "multi")
                upsert = command_boolean(update_spec.get("upsert"), "upsert")
                array_filters = update_spec.get("arrayFilters")
                hint = self._admin._normalize_hint_from_command(update_spec.get("hint"))
                let = command_let
                variables = command_context.with_command_let(
                    let, dialect=self._admin._mongodb_dialect, write=True
                )

                is_operator_update = self.is_operator_update(update_document)
                if multi and not is_operator_update:
                    raise OperationFailure("replacement updates cannot be multi")
                result = await _collection_modify.execute_server_update(
                    collection,
                    query,
                    update_document,
                    multi=multi,
                    replacement=not is_operator_update,
                    upsert=upsert,
                    sort=update_spec.get("sort"),
                    collation=update_spec.get("collation"),
                    hint=hint,
                    comment=spec.get("comment"),
                    let=variables,
                    bypass_document_validation=bypass_document_validation,
                    session=session,
                    **({"array_filters": array_filters} if is_operator_update else {}),
                )
                matched += 0 if result.did_upsert else result.matched_count
                modified += result.modified_count
                if result.did_upsert:
                    upserted.append(
                        UpsertedWriteEntry(
                            index=index,
                            document_id=materialize_public_value(result.upserted_id),
                        )
                    )
            except (WriteError, OperationFailure, TypeError, ValueError) as exc:
                command_failure = record_write_failure(
                    write_errors, exc, index=index, first_failure=command_failure
                )
                if ordered:
                    break
        if command_failure is not None:
            raise command_failure
        return _write_command_result(
            WriteCommandResult(
                count=matched + len(upserted),
                modified_count=modified,
                upserted=upserted or None,
                write_errors=write_errors or None,
            ),
            acknowledged=acknowledged,
        )

    async def command_delete(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
        execution_context: ExpressionExecutionContext | None = None,
    ) -> object:
        validate_write_fields(spec, "delete")
        command_boolean(
            spec.get("bypassDocumentValidation"),
            "bypassDocumentValidation",
            numeric=True,
        )
        collection_name = self._admin._require_collection_name(
            spec.get("delete"), "delete"
        )
        deletes = self._admin._normalize_delete_specs(spec.get("deletes"))
        _validate_delete_command_shapes(deletes)
        ordered = self._admin._normalize_ordered_from_command(spec.get("ordered", True))
        collection = self._admin._database.get_collection(collection_name)
        command_context = (
            execution_context or self._admin._database._new_execution_context()
        )
        command_let = spec.get("let")
        validate_command_let(command_let)
        acknowledged = write_command_acknowledged(
            spec, dialect=self._admin._mongodb_dialect.key
        )
        deleted = 0
        write_errors: list[WriteErrorEntry] = []
        command_failure = None
        for index, delete_spec in enumerate(deletes):
            try:
                query = self._admin._normalize_filter(delete_spec.get("q"))
                limit = delete_spec.get("limit", 0)
                hint = self._admin._normalize_hint_from_command(delete_spec.get("hint"))
                let = command_let
                variables = command_context.with_command_let(
                    let, dialect=self._admin._mongodb_dialect, write=True
                )
                result = await _collection_modify.execute_server_delete(
                    collection,
                    query,
                    multi=limit == 0,
                    collation=delete_spec.get("collation"),
                    hint=hint,
                    comment=spec.get("comment"),
                    let=variables,
                    session=session,
                )
                deleted += result.deleted_count
            except (WriteError, OperationFailure, TypeError, ValueError) as exc:
                command_failure = record_write_failure(
                    write_errors, exc, index=index, first_failure=command_failure
                )
                if ordered:
                    break
        if command_failure is not None:
            raise command_failure
        return _write_command_result(
            WriteCommandResult(count=deleted, write_errors=write_errors or None),
            acknowledged=acknowledged,
        )

    async def execute_find_and_modify(
        self,
        options,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        collection = self._admin._database.get_collection(options.collection_name)
        if options.remove:
            return await self.execute_find_and_modify_remove(
                collection,
                options,
                session=session,
            )

        if options.update_spec is None:
            message = "Either an update or remove=true must be specified"
            raise server_error(message, code=9, scope=ErrorScope.COMMAND)

        if self.is_operator_update(options.update_spec):
            return await self.execute_find_and_modify_operator_update(
                collection,
                options,
                session=session,
            )
        return await self.execute_find_and_modify_replacement(
            collection,
            options,
            session=session,
        )

    async def execute_find_and_modify_remove(
        self,
        collection,
        options,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        if options.update_spec is not None:
            raise OperationFailure(
                "findAndModify remove and update cannot be specified together"
            )
        if options.upsert:
            raise OperationFailure("findAndModify remove does not support upsert")
        before = await _collection_modify.find_one_and_delete(
            collection,
            options.query,
            filter=ARG_UNSET,
            extra_kwargs={},
            server_command=True,
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

    async def find_and_modify_before_full(
        self,
        options,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._admin._first_with_operation(
            options.collection_name,
            self._admin._command_compiler.compile_find_and_modify_selection_operation(
                options
            ),
            session=session,
        )

    async def find_and_modify_fetch_upserted_value(
        self,
        collection_name: str,
        upserted_id: object,
        projection,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._admin._first_with_operation(
            collection_name,
            self._admin._command_compiler.compile_id_lookup_operation(
                upserted_id,
                projection=projection,
            ),
            session=session,
        )

    async def execute_find_and_modify_operator_update(
        self,
        collection,
        options,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        return_document = (
            ReturnDocument.AFTER if options.return_new else ReturnDocument.BEFORE
        )
        arguments = {
            "projection": options.fields,
            "collation": options.collation,
            "sort": options.sort,
            "upsert": options.upsert,
            "return_document": return_document,
            "array_filters": options.array_filters,
            "hint": options.hint,
            "comment": options.comment,
            "max_time_ms": options.max_time_ms,
            "let": options.let,
            "bypass_document_validation": options.bypass_document_validation,
            "session": session,
        }
        outcome = await _collection_modify.find_one_and_update_outcome(
            collection,
            options.query,
            options.update_spec,
            filter=ARG_UNSET,
            update=ARG_UNSET,
            extra_kwargs={},
            server_command=True,
            **arguments,
        )
        if not isinstance(outcome, FindAndModifyOutcome):
            raise TypeError("find_one_and_update did not return an atomic outcome")
        return _collection_modify.find_and_modify_response(outcome)

    async def execute_find_and_modify_replacement(
        self,
        collection,
        options,
        *,
        session: ClientSession | None = None,
    ) -> FindAndModifyCommandResult:
        return_document = (
            ReturnDocument.AFTER if options.return_new else ReturnDocument.BEFORE
        )
        arguments = {
            "projection": options.fields,
            "collation": options.collation,
            "sort": options.sort,
            "upsert": options.upsert,
            "return_document": return_document,
            "hint": options.hint,
            "comment": options.comment,
            "max_time_ms": options.max_time_ms,
            "let": options.let,
            "bypass_document_validation": options.bypass_document_validation,
            "session": session,
        }
        outcome = await _collection_modify.find_one_and_replace_outcome(
            collection,
            options.query,
            options.update_spec,
            filter=ARG_UNSET,
            extra_kwargs={},
            server_command=True,
            **arguments,
        )
        if not isinstance(outcome, FindAndModifyOutcome):
            raise TypeError("find_one_and_replace did not return an atomic outcome")
        return _collection_modify.find_and_modify_response(outcome)

    @staticmethod
    def is_operator_update(update_spec: dict[str, object] | list[object]) -> bool:
        if isinstance(update_spec, list):
            return True
        return bool(update_spec) and next(iter(update_spec)).startswith("$")

    async def command_create_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._admin._require_collection_name(
            spec.get("createIndexes"),
            "createIndexes",
        )
        indexes = self._admin._normalize_index_models_from_command(spec.get("indexes"))
        max_time_ms = self._admin._normalize_max_time_ms_from_command(
            spec.get("maxTimeMS")
        )
        collection_names_before = set(
            await self._admin._engine.list_collections(
                self._admin._db_name, context=session
            )
        )
        collection = self._admin._database.get_collection(collection_name)
        info_before = await self._admin._engine.index_information(
            self._admin._db_name, collection_name, context=session
        )
        await _collection_indexing.execute_create_indexes(
            collection, indexes,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            session=session,
        )
        info_after = await self._admin._engine.index_information(
            self._admin._db_name, collection_name, context=session
        )
        return CreateIndexesCommandResult(
            num_indexes_before=max(1, len(info_before)),
            num_indexes_after=len(info_after),
            created_collection_automatically=collection_name
            not in collection_names_before,
            note="all indexes already exist"
            if len(info_before) == len(info_after)
            else None,
        )

    async def command_drop_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collection_name = self._admin._require_collection_name(
            spec.get("dropIndexes"),
            "dropIndexes",
        )
        target = spec.get("index")
        collection = self._admin._database.get_collection(collection_name)
        if not isinstance(target, (str, list, tuple, dict)):
            expected = (
                ("string",)
                if self._admin._mongodb_dialect.key == "8.0"
                else ("string", "object")
            )
            raise command_type_error(target, "dropIndexes.index", expected)
        names = await self._admin._engine.list_collections(
            self._admin._db_name, context=session
        )
        if collection_name not in names:
            message = f"ns not found {self._admin._db_name}.{collection_name}"
            raise server_error(message, code=26, scope=ErrorScope.COMMAND)
        return await _collection_indexing.execute_drop_index(
            collection, target, comment=spec.get("comment"), session=session,
        )

    async def command_drop_database(
        self,
        *,
        session: ClientSession | None = None,
    ) -> object:
        await self._admin._database._drop_database(session=session)
        return DropDatabaseCommandResult(database_name=self._admin._db_name)
