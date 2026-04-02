from __future__ import annotations

import time
from copy import deepcopy
from typing import TYPE_CHECKING, AsyncIterable

from mongoeco.api._async.cursor import AsyncCursor, _operation_issue_message
from mongoeco.api.operations import AggregateOperation, FindOperation, UpdateOperation, compile_find_operation
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import CollationDocument, Document, DocumentId, Filter, HintSpec, ObjectId, SortSpec

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.core.query_plan import QueryNode


class CollectionRuntimeCoordinator:
    def __init__(self, collection: "AsyncCollection"):
        self._collection = collection

    def record_operation_metadata(
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
        recorder = getattr(self._collection._engine, "_record_operation_metadata", None)
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

    async def profile_operation(
        self,
        *,
        op: str,
        command: dict[str, object],
        duration_ns: int,
        operation: FindOperation | None = None,
        errmsg: str | None = None,
    ) -> None:
        if self._collection._collection_name == "system.profile":
            return
        recorder = getattr(self._collection._engine, "_record_profile_event", None)
        if not callable(recorder):
            return
        execution_lineage: tuple[object, ...] = ()
        fallback_reason: str | None = None
        if operation is not None:
            planner = getattr(self._collection._engine, "plan_find_execution", None)
            if callable(planner):
                try:
                    execution_plan = await planner(
                        self._collection._db_name,
                        self._collection._collection_name,
                        operation,
                        dialect=self._collection._mongodb_dialect,
                        context=None,
                    )
                    execution_lineage = execution_plan.execution_lineage
                    fallback_reason = execution_plan.fallback_reason
                except Exception:
                    execution_lineage = ()
                    fallback_reason = None
        recorder(
            self._collection._db_name,
            op=op,
            command=command,
            duration_micros=max(1, duration_ns // 1000),
            execution_lineage=tuple(execution_lineage),
            fallback_reason=fallback_reason,
            ok=0.0 if errmsg is not None else 1.0,
            errmsg=errmsg,
        )

    async def document_by_id(
        self,
        document_id: DocumentId,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self._collection._engine.get_document(
            self._collection._db_name,
            self._collection._collection_name,
            document_id,
            dialect=self._collection._mongodb_dialect,
            context=session,
        )

    def publish_change_event(
        self,
        *,
        operation_type: str,
        document_key: Document,
        full_document: Document | None = None,
        update_description: dict[str, object] | None = None,
    ) -> None:
        if self._collection._change_hub is None:
            return
        self._collection._change_hub.publish(
            operation_type=operation_type,
            db_name=self._collection._db_name,
            coll_name=self._collection._collection_name,
            document_key=document_key,
            full_document=full_document,
            update_description=update_description,
        )

    def ensure_operation_executable(self, operation: FindOperation | UpdateOperation | AggregateOperation) -> None:
        if operation.planning_issues:
            raise OperationFailure(_operation_issue_message(operation))

    async def engine_update_with_operation(
        self,
        operation: UpdateOperation,
        *,
        upsert: bool = False,
        upsert_seed: Document | None = None,
        selector_filter: Filter | None = None,
        session: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ):
        self.ensure_operation_executable(operation)
        started_at = time.perf_counter_ns()
        method = getattr(self._collection._engine, "update_with_operation", None)
        try:
            if not callable(method):
                raise TypeError("engine must implement update_with_operation")
            result = await method(
                self._collection._db_name,
                self._collection._collection_name,
                operation,
                upsert=upsert,
                upsert_seed=upsert_seed,
                selector_filter=selector_filter,
                dialect=self._collection._mongodb_dialect,
                context=session,
                bypass_document_validation=bypass_document_validation,
            )
        except Exception as exc:
            await self._collection._profile_operation(
                op="update",
                command={
                    "update": self._collection._collection_name,
                    "q": operation.filter_spec,
                    "u": deepcopy(operation.update_spec or {}),
                    "upsert": upsert,
                    "bypassDocumentValidation": bypass_document_validation,
                },
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        await self._collection._profile_operation(
            op="update",
            command={
                "update": self._collection._collection_name,
                "q": operation.filter_spec,
                "u": deepcopy(operation.update_spec or {}),
                "upsert": upsert,
                "bypassDocumentValidation": bypass_document_validation,
            },
            duration_ns=time.perf_counter_ns() - started_at,
        )
        return result

    def engine_scan_with_operation(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        self.ensure_operation_executable(operation)
        from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

        semantics = compile_find_semantics_from_operation(
            operation,
            dialect=self._collection._mongodb_dialect,
        )
        return self._collection._engine.scan_find_semantics(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
            context=session,
        )

    async def engine_delete_with_operation(
        self,
        operation: UpdateOperation,
        *,
        session: ClientSession | None = None,
    ):
        self.ensure_operation_executable(operation)
        started_at = time.perf_counter_ns()
        try:
            result = await self._collection._engine.delete_with_operation(
                self._collection._db_name,
                self._collection._collection_name,
                operation,
                dialect=self._collection._mongodb_dialect,
                context=session,
            )
        except Exception as exc:
            await self._collection._profile_operation(
                op="remove",
                command={"delete": self._collection._collection_name, "q": operation.filter_spec},
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        await self._collection._profile_operation(
            op="remove",
            command={"delete": self._collection._collection_name, "q": operation.filter_spec},
            duration_ns=time.perf_counter_ns() - started_at,
        )
        return result

    async def engine_count_with_operation(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> int:
        self.ensure_operation_executable(operation)
        started_at = time.perf_counter_ns()
        from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

        semantics = compile_find_semantics_from_operation(
            operation,
            dialect=self._collection._mongodb_dialect,
        )
        count = await self._collection._engine.count_find_semantics(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
            context=session,
        )
        await self._collection._profile_operation(
            op="command",
            command={"count": self._collection._collection_name, "query": operation.filter_spec},
            duration_ns=time.perf_counter_ns() - started_at,
            operation=operation,
        )
        return count

    async def select_first_document(
        self,
        filter_spec: Filter,
        *,
        plan: "QueryNode" | None = None,
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
            dialect=self._collection._mongodb_dialect,
            plan=plan,
            planning_mode=self._collection._planning_mode,
        )
        return await self._collection._build_cursor(operation, session=session).first()

    def build_cursor(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> AsyncCursor:
        return AsyncCursor(
            self._collection,
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

    async def put_replacement_document(
        self,
        document: Document,
        *,
        overwrite: bool,
        session: ClientSession | None = None,
        bypass_document_validation: bool = False,
    ) -> None:
        success = await self._collection._engine.put_document(
            self._collection._db_name,
            self._collection._collection_name,
            document,
            overwrite=overwrite,
            context=session,
            bypass_document_validation=bypass_document_validation,
        )
        if not success:
            raise DuplicateKeyError(f"Duplicate key: _id={document['_id']}")

    def build_upsert_replacement_document(
        self,
        filter_spec: Filter,
        replacement: Document,
    ) -> Document:
        from mongoeco.core.upserts import seed_upsert_document

        seeded: Document = {}
        seed_upsert_document(seeded, filter_spec)
        if "_id" in seeded and "_id" in replacement:
            if not self._collection._mongodb_dialect.values_equal(seeded["_id"], replacement["_id"]):
                raise OperationFailure("The _id field cannot conflict with the replacement filter during upsert")
        document = deepcopy(seeded)
        document.update(deepcopy(replacement))
        if "_id" not in document:
            document["_id"] = ObjectId()
        return document

    @staticmethod
    def materialize_replacement_document(selected: Document, replacement: Document) -> Document:
        if "_id" in replacement:
            return deepcopy(replacement)

        replacement_items = [(key, deepcopy(value)) for key, value in replacement.items()]
        replacement_document: Document = {}
        inserted_id = False
        id_position = list(selected).index("_id") if "_id" in selected else 0

        for index in range(len(replacement_items) + 1):
            if index == id_position and not inserted_id:
                replacement_document["_id"] = deepcopy(selected.get("_id"))
                inserted_id = True
            if index < len(replacement_items):
                key, value = replacement_items[index]
                replacement_document[key] = value

        if not inserted_id:
            replacement_document["_id"] = deepcopy(selected.get("_id"))
        return replacement_document
