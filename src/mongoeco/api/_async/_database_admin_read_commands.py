from __future__ import annotations

import socket
import time

from hashlib import md5
from typing import TYPE_CHECKING

from mongoeco.api._async import _collection_indexing
from mongoeco.api._async._collection_reads import collect_distinct_values
from mongoeco.api._command_bson import require_command_type
from mongoeco.api.admin_parsing import (
    normalize_command_max_time_ms,
    normalize_find_and_modify_options,
    normalize_list_collections_options,
    normalize_list_databases_options,
)
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import normalize_collation
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.json_compat import json_dumps_compact
from mongoeco.core.server_errors import ErrorScope, server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import (
    CommandCursorResult,
    CountCommandResult,
    DatabaseHashCommandResult,
    DistinctCommandResult,
    ListDatabasesCommandResult,
)


if TYPE_CHECKING:
    from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
    from mongoeco.api.operations import AggregateOperation, FindOperation
    from mongoeco.session import ClientSession


class DatabaseAdminReadCommandService:
    def __init__(self, admin: "AsyncDatabaseAdminService") -> None:
        self._admin = admin

    async def command_list_collections(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        options = normalize_list_collections_options(spec)
        return await self.execute_list_collections(options, session=session)

    async def execute_list_collections(self, options, *, session=None):
        """Execute prepared listing options without another public BSON boundary."""
        self._admin._database.get_collection("$cmd")._record_operation_metadata(
            operation="list_collections", comment=options.comment, session=session,
        )
        if options.name_only:
            collection_names = await self._admin._engine.list_collections(
                self._admin._db_name,
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
                    dialect=self._admin._mongodb_dialect,
                )
            ]
        else:
            snapshots = await self._admin._list_collection_snapshots(session=session)
            first_batch = [snapshot.to_document() for snapshot in snapshots]
            first_batch = [
                document
                for document in first_batch
                if QueryEngine.match(
                    document,
                    options.filter_spec,
                    dialect=self._admin._mongodb_dialect,
                )
            ]
        return CommandCursorResult(
            namespace=f"{self._admin._db_name}.$cmd.listCollections",
            first_batch=first_batch,
        )

    async def command_list_databases(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        if self._admin._db_name != "admin":
            message = "listDatabases may only be run against the admin database."
            raise server_error(message, code=13, scope=ErrorScope.COMMAND)
        options = normalize_list_databases_options(spec)
        databases = [
            snapshot.to_document()
            for snapshot in await self._admin._list_database_snapshots(session=session)
        ]
        filtered = [
            document
            for document in databases
            if QueryEngine.match(
                document,
                options.filter_spec,
                dialect=self._admin._mongodb_dialect,
            )
        ]
        total_size = sum(int(document.get("sizeOnDisk", 0)) for document in filtered)
        if options.name_only:
            filtered = [{"name": str(document["name"])} for document in filtered]
        return ListDatabasesCommandResult(
            databases=filtered,
            total_size=total_size,
        )

    async def command_count(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
        execution_context=None,
    ) -> object:
        collection_name, operation = self._admin._command_compiler.compile_count_operation(spec)
        if execution_context is not None:
            operation = operation.with_overrides(
                let=execution_context.with_command_let(
                    operation.let, dialect=self._admin._mongodb_dialect
                )
            )
        return await self.execute_count_command(
            collection_name,
            operation,
            session=session,
        )

    async def command_distinct(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
        execution_context=None,
    ) -> object:
        collection_name, key, operation = self._admin._command_compiler.compile_distinct_operation(spec)
        if execution_context is not None:
            operation = operation.with_overrides(
                let=execution_context.with_command_let(
                    operation.let, dialect=self._admin._mongodb_dialect
                )
            )
        return await self.execute_distinct_command(
            collection_name,
            key,
            operation,
            session=session,
        )

    async def command_db_hash(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        collections = spec.get("collections")
        normalized_collections: tuple[str, ...] = ()
        if collections is not None:
            if not isinstance(collections, list) or any(
                not isinstance(name, str) or not name for name in collections
            ):
                raise TypeError("collections must be a list of non-empty strings")
            normalized_collections = tuple(dict.fromkeys(collections))
        comment = spec.get("comment")
        if comment is not None and not isinstance(comment, str):
            raise TypeError("comment must be a string")
        return await self.execute_db_hash_command(
            normalized_collections,
            comment=comment,
            session=session,
        )

    async def execute_distinct_command(
        self,
        collection_name: str,
        key: str,
        operation: "FindOperation",
        *,
        session: ClientSession | None = None,
    ) -> DistinctCommandResult:
        normalized_collation = normalize_collation(operation.collation)
        collection = self._admin._database.get_collection(collection_name)
        cursor = collection._build_cursor(
            operation, session=session, apply_codec_options=False
        )
        cursor._expression_error_operation = "distinct"
        values = await collect_distinct_values(
            cursor, key, dialect=self._admin._mongodb_dialect,
            collation=normalized_collation,
        )
        return DistinctCommandResult(values=values)

    async def execute_count_command(
        self,
        collection_name: str,
        operation: "FindOperation",
        *,
        session: ClientSession | None = None,
    ) -> CountCommandResult:
        count = await self._admin._database.get_collection(
            collection_name
        )._engine_count_with_operation(
            operation,
            session=session,
        )
        return CountCommandResult(count=count)

    async def execute_db_hash_command(
        self,
        collections: tuple[str, ...],
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> DatabaseHashCommandResult:
        del comment
        started_at = time.perf_counter()
        available_collections = await self._admin._engine.list_collections(
            self._admin._db_name,
            context=session,
        )
        selected_collections = list(collections) if collections else available_collections
        missing_collections = [
            name
            for name in selected_collections
            if name not in available_collections
        ]
        if missing_collections:
            raise OperationFailure(
                f"dbHash unknown collections: {', '.join(missing_collections)}"
            )

        collection_hashes: dict[str, str] = {}
        for collection_name in selected_collections:
            collection = self._admin._database.get_collection(collection_name)
            documents = await self._admin._namespace_admin.collection_documents(
                collection_name, session=session,
            )
            indexes = await _collection_indexing.load_index_documents(
                collection, session=session,
            )
            options = await self._admin._engine.collection_options(
                self._admin._db_name,
                collection_name,
                context=session,
            )
            encoded_payload = {
                "documents": sorted(
                    json_dumps_compact(DocumentCodec.encode(document), sort_keys=True)
                    for document in documents
                ),
                "indexes": sorted(
                    json_dumps_compact(DocumentCodec.encode(index), sort_keys=True)
                    for index in indexes
                ),
                "options": DocumentCodec.encode(options),
            }
            collection_hashes[collection_name] = md5(
                json_dumps_compact(encoded_payload, sort_keys=True).encode("utf-8")
            ).hexdigest()

        global_hash = md5(
            json_dumps_compact(collection_hashes, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return DatabaseHashCommandResult(
            host=socket.gethostname(),
            collections=collection_hashes,
            md5=global_hash,
            time_millis=max(0, int((time.perf_counter() - started_at) * 1000)),
        )

    async def command_find(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
        execution_context=None,
    ) -> object:
        collection_name, operation = self._admin._command_compiler.compile_find_operation(
            spec,
            collection_field="find",
        )
        if execution_context is not None:
            operation = operation.with_overrides(
                let=execution_context.with_command_let(
                    operation.let, dialect=self._admin._mongodb_dialect
                )
            )
        return await self.execute_find_command(
            collection_name,
            operation,
            session=session,
        )

    async def execute_find_command(
        self,
        collection_name: str,
        operation: "FindOperation",
        *,
        session: ClientSession | None = None,
    ) -> CommandCursorResult:
        first_batch = await self._admin._database.get_collection(collection_name)._build_cursor(
            operation,
            session=session,
            apply_codec_options=False,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._admin._db_name}.{collection_name}",
            first_batch=first_batch,
        )

    async def command_aggregate(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
        execution_context=None,
    ) -> object:
        collection_name, operation = self._admin._command_compiler.compile_aggregate_operation(spec)
        if execution_context is not None:
            operation = operation.with_overrides(
                let=execution_context.with_command_let(
                    operation.let, dialect=self._admin._mongodb_dialect
                )
            )
        return await self.execute_aggregate_command(
            collection_name,
            operation,
            session=session,
        )

    async def execute_aggregate_command(
        self,
        collection_name: str,
        operation: "AggregateOperation",
        *,
        session: ClientSession | None = None,
    ) -> CommandCursorResult:
        first_batch = await self._admin._database.get_collection(
            collection_name
        )._build_aggregation_cursor(
            operation,
            session=session,
            apply_codec_options=False,
        ).to_list()
        return CommandCursorResult(
            namespace=f"{self._admin._db_name}.{collection_name}",
            first_batch=first_batch,
        )

    async def command_explain(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        explain_spec = spec.get("explain")
        require_command_type(explain_spec, "explain.explain", "object", required=True)
        verbosity = spec.get("verbosity")
        if verbosity is not None:
            require_command_type(verbosity, "explain.verbosity", "string")

        explained_command_name = next(iter(explain_spec), None)
        if explained_command_name is None:
            message = "Explain failed due to unknown command: "
            raise server_error(message, code=59, scope=ErrorScope.COMMAND)
        if "maxTimeMS" in spec:
            normalize_command_max_time_ms(spec["maxTimeMS"])
        explain_spec = dict(explain_spec)
        for option in ("comment", "maxTimeMS"):
            if option in spec:
                explain_spec.setdefault(option, spec[option])
        if explained_command_name == "find":
            collection_name, operation = self._admin._command_compiler.compile_find_operation(
                explain_spec,
                collection_field="find",
            )
            explanation = await self._admin._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "find"
            result["batch_size"] = operation.batch_size
        elif explained_command_name == "aggregate":
            if self._admin._mongodb_dialect.key == "9.0":
                explain_spec.setdefault("cursor", {})
            collection_name, operation = self._admin._command_compiler.compile_aggregate_operation(
                explain_spec
            )
            cursor = self._admin._database.get_collection(
                collection_name
            )._build_aggregation_cursor(operation, session=session)
            explanation = (
                await cursor.explain()
                if verbosity is None
                else await cursor.explain(verbosity)
            )
            result = dict(explanation)
            result["command"] = "aggregate"
        elif explained_command_name == "update":
            collection_name = self._admin._require_collection_name(
                explain_spec.get("update"),
                "update",
            )
            updates = self._admin._normalize_update_specs(explain_spec.get("updates"))
            if len(updates) != 1:
                raise OperationFailure("explain update requires exactly one update specification")
            update_spec = updates[0]
            multi = update_spec.get("multi", False)
            if not isinstance(multi, bool):
                raise TypeError("multi must be a bool")
            operation = self._admin._command_compiler.compile_update_selection_operation(
                update_spec,
                comment=explain_spec.get("comment"),
                max_time_ms=explain_spec.get("maxTimeMS"),
                limit=None if multi else 1,
                command_let=explain_spec.get("let"),
            )
            explanation = await self._admin._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "update"
            result["multi"] = multi
        elif explained_command_name == "delete":
            collection_name = self._admin._require_collection_name(
                explain_spec.get("delete"),
                "delete",
            )
            deletes = self._admin._normalize_delete_specs(explain_spec.get("deletes"))
            if len(deletes) != 1:
                raise OperationFailure("explain delete requires exactly one delete specification")
            delete_spec = deletes[0]
            limit = delete_spec.get("limit", 0)
            if limit not in (0, 1):
                raise TypeError("limit must be 0 or 1")
            operation = self._admin._command_compiler.compile_delete_selection_operation(
                delete_spec,
                comment=explain_spec.get("comment"),
                max_time_ms=explain_spec.get("maxTimeMS"),
                limit=1 if limit == 1 else None,
                command_let=explain_spec.get("let"),
            )
            explanation = await self._admin._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "delete"
            result["limit"] = limit
        elif explained_command_name == "count":
            collection_name, operation = self._admin._command_compiler.compile_count_operation(
                explain_spec
            )
            explanation = await self._admin._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "count"
        elif explained_command_name == "distinct":
            collection_name, key, operation = self._admin._command_compiler.compile_distinct_operation(
                explain_spec
            )
            explanation = await self._admin._database.get_collection(collection_name)._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "distinct"
            result["key"] = key
        elif explained_command_name == "findAndModify":
            options = normalize_find_and_modify_options(explain_spec)
            operation = self._admin._command_compiler.compile_find_and_modify_selection_operation(
                options,
                projection={"_id": 1},
            )
            explanation = await self._admin._database.get_collection(
                options.collection_name
            )._build_cursor(
                operation,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "findAndModify"
            result["remove"] = options.remove
            result["upsert"] = options.upsert
            result["new"] = options.return_new
            if options.fields is not None:
                result["fields"] = options.fields
        else:
            raise OperationFailure(
                f"Unsupported explain command: {explained_command_name}"
            )

        result.setdefault("ok", 1.0)
        result.setdefault("explained_command", explained_command_name)
        if explained_command_name == "findAndModify":
            collection_name = options.collection_name
        result.setdefault("collection", collection_name)
        result.setdefault("namespace", f"{self._admin._db_name}.{collection_name}")
        if verbosity is not None:
            result["verbosity"] = verbosity
        return result

    async def command_list_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> object:
        comment = spec.get("comment")
        return await self.execute_list_indexes_command(
            self._admin._require_collection_name(
                spec.get("listIndexes"),
                "listIndexes",
            ),
            comment=comment,
            session=session,
        )

    async def execute_list_indexes_command(
        self,
        collection_name: str,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> CommandCursorResult:
        names = await self._admin._engine.list_collections(
            self._admin._db_name, context=session
        )
        if collection_name not in names:
            message = f"ns does not exist: {self._admin._db_name}.{collection_name}"
            raise server_error(message, code=26, scope=ErrorScope.COMMAND)
        collection = self._admin._database.get_collection(collection_name)
        raw_first_batch = await _collection_indexing.load_index_documents(
            collection, session=session,
        )
        collection._record_operation_metadata(
            operation="list_indexes", comment=comment, session=session,
        )
        first_batch = [
            self._admin._normalize_list_indexes_document_for_collection(collection_name, document)
            for document in raw_first_batch
        ]
        return CommandCursorResult(
            namespace=f"{self._admin._db_name}.{collection_name}",
            first_batch=first_batch,
        )
