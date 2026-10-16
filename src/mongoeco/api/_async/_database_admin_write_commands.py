from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.api._async import _collection_modify
from mongoeco.api._async._write_command_validation import (
    command_shape_error as _command_shape_error,
    validate_selection_shapes,
    validate_update_entry_shape,
    write_command_acknowledged,
)
from mongoeco.api._async._write_errors import validate_classic_update_sort
from mongoeco.api._async._write_failures import record_write_failure
from mongoeco.api._command_bson import (
    command_boolean,
    validate_command_array_filters,
    validate_command_let,
    validate_write_fields,
)
from mongoeco.api.public_api import ARG_UNSET
from mongoeco.engines.results import FindAndModifyOutcome
from mongoeco.errors import CollectionInvalid, OperationFailure, WriteError
from mongoeco.types import (
    CreateIndexesCommandResult,
    Document,
    DropDatabaseCommandResult,
    DropIndexesCommandResult,
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


def _validate_write_query(spec) -> None:
    if spec.get("q") is None:
        _command_shape_error("missing required field q", code=_MISSING_REQUIRED_FIELD)
    if not isinstance(spec["q"], dict):
        _command_shape_error("q must be a document")


def _validate_update_command_shapes(updates, dialect: str) -> None:
    # Command parsing precedes execution of every item. An invalid final spec
    # must not publish an earlier valid update, even for unordered commands.
    for spec in updates:
        validate_write_fields(spec, "update", entry=True)
        validate_command_array_filters(spec, dialect=dialect)
        _validate_write_query(spec)
        validate_update_entry_shape(spec.get("u"), spec.get("arrayFilters"))
        if not isinstance(spec.get("u"), (dict, list)):
            _command_shape_error("u must be a document or pipeline", code=9)
        for field in ("multi", "upsert"):
            command_boolean(spec.get(field), field)
        validate_classic_update_sort(dialect, has_sort="sort" in spec)
        validate_selection_shapes(spec, update=True)


def _validate_delete_command_shapes(deletes) -> None:
    for spec in deletes:
        validate_write_fields(spec, "delete", entry=True)
        _validate_write_query(spec)
        limit = spec.get("limit")
        if isinstance(limit, bool) or limit not in (0, 1):
            _command_shape_error("limit must be 0 or 1", code=9)
        validate_selection_shapes(spec)


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
        await self._admin.create_collection(
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
        collection_name = self._admin._require_collection_name(spec.get("drop"), "drop")
        await self._admin.drop_collection(collection_name, session=session)
        return NamespaceOkResult(
            namespace=f"{self._admin._db_name}.{collection_name}",
        )

    async def command_rename_collection(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        source_db, source_name = self._admin._normalize_namespace(
            spec.get("renameCollection"),
            "renameCollection",
        )
        target_db, target_name = self._admin._normalize_namespace(spec.get("to"), "to")
        if source_db != self._admin._db_name or target_db != self._admin._db_name:
            raise OperationFailure(
                "renameCollection only supports namespaces in the current database"
            )
        drop_target = spec.get("dropTarget", False)
        if not isinstance(drop_target, bool):
            raise TypeError("dropTarget must be a bool")
        if drop_target:
            collection_names = set(
                await self._admin._engine.list_collections(
                    self._admin._db_name,
                    context=session,
                )
            )
            if source_name == target_name:
                raise CollectionInvalid("collection names must differ")
            if source_name not in collection_names:
                raise CollectionInvalid(f"collection '{source_name}' does not exist")
            if target_name in collection_names:
                await self._admin.drop_collection(target_name, session=session)
        await self._admin._database.get_collection(source_name).rename(
            target_name, session=session
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
        ordered = self._admin._normalize_ordered_from_command(spec.get("ordered"))
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
                await collection.insert_one(
                    document,
                    bypass_document_validation=bypass_document_validation,
                    session=session,
                )
                inserted += 1
            except (WriteError, OperationFailure, TypeError, ValueError) as exc:
                command_failure = (
                    record_write_failure(write_errors, exc, index=index, first_failure=command_failure)
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
        ordered = self._admin._normalize_ordered_from_command(spec.get("ordered"))
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
                variables = command_context.with_bindings(let or {})

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
                        UpsertedWriteEntry(index=index, document_id=result.upserted_id)
                    )
            except (WriteError, OperationFailure, TypeError, ValueError) as exc:
                command_failure = (
                    record_write_failure(write_errors, exc, index=index, first_failure=command_failure)
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
        command_boolean(spec.get("bypassDocumentValidation"), "bypassDocumentValidation", numeric=True)
        collection_name = self._admin._require_collection_name(
            spec.get("delete"), "delete"
        )
        deletes = self._admin._normalize_delete_specs(spec.get("deletes"))
        _validate_delete_command_shapes(deletes)
        ordered = self._admin._normalize_ordered_from_command(spec.get("ordered"))
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
                variables = command_context.with_bindings(let or {})
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
                command_failure = (
                    record_write_failure(write_errors, exc, index=index, first_failure=command_failure)
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
            raise OperationFailure("findAndModify requires either remove or update")

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
            value=collection._apply_codec_options_to_optional_document(before),
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
        result = outcome.captured.result
        applied = result.matched_count > 0 or result.did_upsert
        return FindAndModifyCommandResult(
            last_error_object=FindAndModifyLastErrorObject(
                count=1 if applied else 0,
                updated_existing=result.matched_count > 0,
                upserted_id=result.upserted_id,
            ),
            value=collection._apply_codec_options_to_optional_document(outcome.value),
        )

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
        result = outcome.captured.result
        applied = result.matched_count > 0 or result.did_upsert
        return FindAndModifyCommandResult(
            last_error_object=FindAndModifyLastErrorObject(
                count=1 if applied else 0,
                updated_existing=result.matched_count > 0,
                upserted_id=result.upserted_id,
            ),
            value=collection._apply_codec_options_to_optional_document(outcome.value),
        )

    @staticmethod
    def is_operator_update(update_spec: dict[str, object] | list[object]) -> bool:
        if isinstance(update_spec, list):
            return True
        return all(isinstance(key, str) and key.startswith("$") for key in update_spec)

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
        info_before = await collection.index_information(session=session)
        if target == "*":
            await collection.drop_indexes(comment=spec.get("comment"), session=session)
        elif isinstance(target, (str, list, tuple, dict)):
            await collection.drop_index(
                target, comment=spec.get("comment"), session=session
            )
        else:
            raise TypeError("index must be '*', a name, or a key specification")
        return DropIndexesCommandResult(
            previous_index_count=len(info_before),
            message=(
                f"non-_id indexes for collection {self._admin._db_name}.{collection_name} dropped"
                if target == "*"
                else None
            ),
        )

    async def command_drop_database(
        self,
        *,
        session: ClientSession | None = None,
    ) -> object:
        while True:
            collection_names = await self._admin._engine.list_collections(
                self._admin._db_name,
                context=session,
            )
            if not collection_names:
                break
            for collection_name in collection_names:
                await self._admin.drop_collection(collection_name, session=session)
        return DropDatabaseCommandResult(database_name=self._admin._db_name)
