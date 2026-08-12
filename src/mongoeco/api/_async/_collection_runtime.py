from __future__ import annotations

import time
from copy import deepcopy
from typing import TYPE_CHECKING, AsyncIterable, Callable

from mongoeco.api._async.cursor import AsyncCursor, _operation_issue_message
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    UpdateOperation,
    compile_find_operation,
)
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.identity import materialize_replacement_document
from mongoeco.engines.adapter import adapt_engine
from mongoeco.engines.results import (
    EngineDeleteResult,
    EngineUpdateResult,
    InsertOutcome,
    MergeOutcome,
)
from mongoeco.engines.snapshots import ReadSnapshot
from mongoeco.errors import OperationFailure, WriteError
from mongoeco.session import ClientSession, EngineTransactionContext
from mongoeco.types import (
    CollationDocument,
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    HintSpec,
    ObjectId,
    SortSpec,
    UpdateResult,
)

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.core.query_plan import QueryNode


_CHANGE_STREAM_TRANSACTION_PREFIX = 'change_stream_hub:'
_PENDING_CHANGE_EVENTS_KEY = 'pending_change_events'


class CollectionRuntimeCoordinator:
    def __init__(self, collection: 'AsyncCollection'):
        self._collection = collection
        self._engine_spi = adapt_engine(getattr(collection, '_engine', object()))
        try:
            self._engine_spi.prepare_change_delivery(
                getattr(collection, '_change_hub', None)
            )
        except RuntimeError:
            # Lazy clients register immediately before their first write.
            pass

    def _prepare_engine_change_delivery(self, operation_context) -> None:
        change_hub = self._collection._change_hub
        self._engine_spi.prepare_change_delivery(change_hub)
        if (
            change_hub is None
            or operation_context is None
            or operation_context.session is None
            or not operation_context.session.in_transaction
            or self._engine_spi.capabilities.change_delivery not in {
                'commit-sequence',
                'transactional-outbox',
            }
        ):
            return
        session = operation_context.session
        hook_key = (
            f'change_outbox_dispatch:{id(self._collection._engine)}:'
            f'{id(change_hub)}'
        )
        session.register_transaction_hooks(
            hook_key,
            commit=lambda _session: self._dispatch_engine_changes(),
        )

    def _dispatch_engine_changes(self, operation_context=None) -> None:
        if (
            operation_context is not None
            and operation_context.session is not None
            and operation_context.session.in_transaction
        ):
            return
        change_hub = self._collection._change_hub
        try:
            self._engine_spi.dispatch_committed_changes(change_hub)
        except Exception as exc:
            if change_hub is None:
                return
            mark_failure = getattr(change_hub, 'mark_publish_failure', None)
            if callable(mark_failure):
                mark_failure(exc)

    @staticmethod
    def ensure_session_active(session: object | None) -> None:
        if session is not None:
            ensure_active = getattr(session, 'ensure_active', None)
            if callable(ensure_active):
                ensure_active()

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
        self.ensure_session_active(session)
        recorder = getattr(
            self._collection._engine, '_record_operation_metadata', None
        )
        if callable(recorder):
            try:
                recorder(
                    session,
                    operation=operation,
                    comment=comment,
                    max_time_ms=max_time_ms,
                    hint=hint,
                )
            except Exception:
                pass
        observe_operation = getattr(session, 'observe_operation', None)
        if callable(observe_operation):
            observe_operation()

    async def profile_operation(
        self,
        *,
        op: str,
        command: dict[str, object] | None = None,
        command_factory: Callable[[], dict[str, object]] | None = None,
        duration_ns: int,
        operation: FindOperation | None = None,
        errmsg: str | None = None,
    ) -> None:
        if self._collection._collection_name == 'system.profile':
            return
        recorder = getattr(
            self._collection._engine, '_record_profile_event', None
        )
        if not callable(recorder):
            return
        duration_micros = max(1, duration_ns // 1000)
        active = True
        is_active = getattr(
            self._collection._engine, '_profile_is_active', None
        )
        if callable(is_active):
            try:
                active = bool(
                    is_active(
                        self._collection._db_name,
                        duration_micros=duration_micros,
                    )
                )
            except Exception:
                active = True
        if not active:
            try:
                recorder(
                    self._collection._db_name,
                    op=op,
                    command={},
                    duration_micros=duration_micros,
                    execution_lineage=(),
                    fallback_reason=None,
                    ok=0.0 if errmsg is not None else 1.0,
                    errmsg=errmsg,
                )
            except Exception:
                pass
            return
        execution_lineage: tuple[object, ...] = ()
        fallback_reason: str | None = None
        if operation is not None:
            planner = getattr(
                self._collection._engine, 'plan_find_execution', None
            )
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
        resolved_command = command
        if resolved_command is None and command_factory is not None:
            resolved_command = command_factory()
        if resolved_command is None:
            resolved_command = {}
        try:
            recorder(
                self._collection._db_name,
                op=op,
                command=resolved_command,
                duration_micros=duration_micros,
                execution_lineage=tuple(execution_lineage),
                fallback_reason=fallback_reason,
                ok=0.0 if errmsg is not None else 1.0,
                errmsg=errmsg,
            )
        except Exception:
            pass

    async def document_by_id(
        self,
        document_id: DocumentId,
        *,
        session: ClientSession | None = None,
    ) -> Document | None:
        operation_context = self._collection._new_operation_context(
            session=session,
        )
        return await self._engine_spi.get_document(
            self._collection._db_name,
            self._collection._collection_name,
            document_id,
            dialect=self._collection._mongodb_dialect,
            operation_context=operation_context,
        )

    def publish_change_event(
        self,
        *,
        operation_type: str,
        document_key: Document,
        full_document: Document | None = None,
        update_description: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> None:
        change_hub = self._collection._change_hub
        if change_hub is None:
            return
        if (
            session is None
            or not bool(getattr(session, 'in_transaction', False))
        ) and not self._change_hub_should_publish(change_hub):
            self._mark_change_hub_gap(change_hub)
            return
        payload = {
            'operation_type': operation_type,
            'db_name': self._collection._db_name,
            'coll_name': self._collection._collection_name,
            'document_key': deepcopy(document_key),
            'full_document': deepcopy(full_document)
            if full_document is not None
            else None,
            'update_description': deepcopy(update_description)
            if update_description is not None
            else None,
        }
        if session is not None and bool(
            getattr(session, 'in_transaction', False)
        ):
            pending_events = self._pending_transaction_change_events(session)
            pending_events.append(payload)
            return
        self._publish_change_payload(payload)

    def should_publish_change_events(
        self, *, session: ClientSession | None = None
    ) -> bool:
        change_hub = self._collection._change_hub
        if change_hub is None:
            return False
        if session is not None and bool(
            getattr(session, 'in_transaction', False)
        ):
            return True
        return self._change_hub_should_publish(change_hub)

    def mark_change_event_gap(self) -> None:
        change_hub = self._collection._change_hub
        if change_hub is not None:
            self._mark_change_hub_gap(change_hub)

    @staticmethod
    def _change_hub_should_publish(change_hub: object) -> bool:
        should_publish = getattr(change_hub, 'should_publish_events', None)
        if callable(should_publish):
            return bool(should_publish())
        return True

    @staticmethod
    def _mark_change_hub_gap(change_hub: object) -> None:
        mark_gap = getattr(change_hub, 'mark_gap', None)
        if callable(mark_gap):
            mark_gap()

    def _pending_transaction_change_events(
        self, session: ClientSession
    ) -> list[dict[str, object]]:
        change_hub = self._collection._change_hub
        if change_hub is None:
            return []
        engine_key = f'{_CHANGE_STREAM_TRANSACTION_PREFIX}{id(change_hub)}'
        context = session.get_engine_context(engine_key)
        if context is None:
            context = EngineTransactionContext(
                engine_key=engine_key,
                connected=True,
                supports_transactions=True,
                transaction_active=session.in_transaction,
                metadata={_PENDING_CHANGE_EVENTS_KEY: []},
            )
            session.bind_engine_context(context)
        pending_events = context.metadata.get(_PENDING_CHANGE_EVENTS_KEY)
        if not isinstance(pending_events, list):
            pending_events = []
            context.metadata[_PENDING_CHANGE_EVENTS_KEY] = pending_events

        def _start(active_session: ClientSession) -> None:
            active_context = active_session.get_engine_context(engine_key)
            if active_context is None:
                return
            active_context.transaction_active = True
            active_context.metadata[_PENDING_CHANGE_EVENTS_KEY] = []

        def _commit(active_session: ClientSession) -> None:
            active_context = active_session.get_engine_context(engine_key)
            if active_context is None:
                return
            queued = active_context.metadata.get(_PENDING_CHANGE_EVENTS_KEY)
            active_context.metadata[_PENDING_CHANGE_EVENTS_KEY] = []
            active_context.transaction_active = False
            if not isinstance(queued, list):
                return
            for event in queued:
                if isinstance(event, dict):
                    self._publish_change_payload(event)

        def _abort(active_session: ClientSession) -> None:
            active_context = active_session.get_engine_context(engine_key)
            if active_context is None:
                return
            active_context.metadata[_PENDING_CHANGE_EVENTS_KEY] = []
            active_context.transaction_active = False

        session.register_transaction_hooks(
            engine_key,
            start=_start,
            commit=_commit,
            abort=_abort,
        )
        return pending_events

    def _publish_change_payload(self, payload: dict[str, object]) -> None:
        change_hub = self._collection._change_hub
        if change_hub is None:
            return
        try:
            change_hub.publish(**payload)
        except Exception as exc:  # A committed write cannot be rolled back here.
            mark_failure = getattr(change_hub, 'mark_publish_failure', None)
            if callable(mark_failure):
                mark_failure(exc)

    def _publish_captured_update_event(
        self,
        captured: EngineUpdateResult,
        *,
        matched_operation_type: str,
        session: ClientSession | None,
    ) -> None:
        document = captured.after_document
        result = captured.result
        if (
            document is None
            or '_id' not in document
            or (result.upserted_id is None and result.modified_count == 0)
        ):
            return
        self.publish_change_event(
            operation_type=(
                'insert'
                if result.upserted_id is not None
                else matched_operation_type
            ),
            document_key={'_id': deepcopy(document['_id'])},
            full_document=document,
            session=session,
        )

    def _publish_captured_delete_event(
        self,
        captured: EngineDeleteResult,
        *,
        session: ClientSession | None,
    ) -> None:
        document = captured.deleted_document
        if (
            captured.result.deleted_count == 0
            or document is None
            or '_id' not in document
        ):
            return
        self.publish_change_event(
            operation_type='delete',
            document_key={'_id': deepcopy(document['_id'])},
            session=session,
        )

    def ensure_operation_executable(
        self, operation: FindOperation | UpdateOperation | AggregateOperation
    ) -> None:
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
        replacement_document: Document | None = None,
        publish_operation_type: str | None = None,
    ) -> EngineUpdateResult:
        self.ensure_operation_executable(operation)
        self._prepare_engine_change_delivery(operation.context)
        started_at = time.perf_counter_ns()
        try:
            on_commit = (
                lambda captured: self._publish_captured_update_event(
                    captured,
                    matched_operation_type=publish_operation_type,
                    session=session,
                )
                if publish_operation_type is not None
                else None
            )
            result = await self._engine_spi.update_outcome(
                self._collection._db_name,
                self._collection._collection_name,
                operation,
                upsert=upsert,
                upsert_seed=upsert_seed,
                selector_filter=selector_filter,
                dialect=self._collection._mongodb_dialect,
                operation_context=operation.context,
                bypass_document_validation=bypass_document_validation,
                replacement_document=replacement_document,
                on_commit=(
                    on_commit if publish_operation_type is not None else None
                ),
            )
        except Exception as exc:
            await self._collection._profile_operation(
                op='update',
                command={
                    'update': self._collection._collection_name,
                    'q': operation.filter_spec,
                    'u': deepcopy(operation.update_spec or {}),
                    'upsert': upsert,
                    'bypassDocumentValidation': bypass_document_validation,
                },
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        self._dispatch_engine_changes(operation.context)
        await self._collection._profile_operation(
            op='update',
            command={
                'update': self._collection._collection_name,
                'q': operation.filter_spec,
                'u': deepcopy(operation.update_spec or {}),
                'upsert': upsert,
                'bypassDocumentValidation': bypass_document_validation,
            },
            duration_ns=time.perf_counter_ns() - started_at,
        )
        return result

    async def engine_insert_document(
        self,
        document: Document,
        *,
        overwrite: bool,
        session: ClientSession | None,
        bypass_document_validation: bool,
        on_commit: Callable[[InsertOutcome], None] | None,
        operation_context=None,
    ) -> InsertOutcome:
        self._prepare_engine_change_delivery(operation_context)
        outcome = await self._engine_spi.insert_outcome(
            self._collection._db_name,
            self._collection._collection_name,
            document,
            overwrite=overwrite,
            operation_context=operation_context,
            bypass_document_validation=bypass_document_validation,
            on_commit=on_commit,
        )
        self._dispatch_engine_changes(operation_context)
        return outcome

    async def engine_insert_documents(
        self,
        documents: list[Document],
        *,
        session: ClientSession | None,
        bypass_document_validation: bool,
        on_commit: Callable[[InsertOutcome], None] | None,
        operation_context=None,
    ) -> tuple[InsertOutcome, ...]:
        self._prepare_engine_change_delivery(operation_context)
        outcomes = await self._engine_spi.insert_many_outcomes(
            self._collection._db_name,
            self._collection._collection_name,
            documents,
            operation_context=operation_context,
            bypass_document_validation=bypass_document_validation,
            on_commit=on_commit,
        )
        self._dispatch_engine_changes(operation_context)
        return outcomes

    async def engine_merge_document(
        self,
        document: Document,
        *,
        when_matched: str,
        when_not_matched: str,
        operation_context,
    ) -> MergeOutcome:
        self._prepare_engine_change_delivery(operation_context)
        outcome = await self._engine_spi.merge_outcome(
            self._collection._db_name,
            self._collection._collection_name,
            document,
            when_matched=when_matched,
            when_not_matched=when_not_matched,
            operation_context=operation_context,
        )
        self._dispatch_engine_changes(operation_context)
        return outcome

    def engine_scan_with_operation(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
    ) -> ReadSnapshot:
        self.ensure_operation_executable(operation)
        from mongoeco.engines.semantic_core import (
            compile_find_semantics_from_operation,
        )

        semantics = compile_find_semantics_from_operation(
            operation,
            dialect=self._collection._mongodb_dialect,
        )
        if operation.context is None:
            raise TypeError('find operation is missing OperationContext')
        return self._engine_spi.open_read_snapshot(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
            operation_context=operation.context,
        )

    async def engine_get_document(
        self,
        document_id: DocumentId,
        *,
        projection,
        operation_context,
    ) -> Document | None:
        return await self._engine_spi.get_document(
            self._collection._db_name,
            self._collection._collection_name,
            document_id,
            projection=projection,
            dialect=self._collection._mongodb_dialect,
            operation_context=operation_context,
        )

    async def engine_delete_with_operation(
        self,
        operation: UpdateOperation,
        *,
        selector_filter: Filter | None = None,
        session: ClientSession | None = None,
        publish_change_event: bool = False,
    ) -> EngineDeleteResult:
        self.ensure_operation_executable(operation)
        self._prepare_engine_change_delivery(operation.context)
        started_at = time.perf_counter_ns()
        try:
            result = await self._engine_spi.delete_outcome(
                self._collection._db_name,
                self._collection._collection_name,
                operation,
                selector_filter=selector_filter,
                dialect=self._collection._mongodb_dialect,
                operation_context=operation.context,
                on_commit=(
                    lambda captured: self._publish_captured_delete_event(
                        captured,
                        session=session,
                    )
                    if publish_change_event
                    else None
                ),
            )
        except Exception as exc:
            await self._collection._profile_operation(
                op='remove',
                command={
                    'delete': self._collection._collection_name,
                    'q': operation.filter_spec,
                },
                duration_ns=time.perf_counter_ns() - started_at,
                errmsg=str(exc),
            )
            raise
        self._dispatch_engine_changes(operation.context)
        await self._collection._profile_operation(
            op='remove',
            command={
                'delete': self._collection._collection_name,
                'q': operation.filter_spec,
            },
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
        from mongoeco.engines.semantic_core import (
            compile_find_semantics_from_operation,
        )

        if operation.context is None:
            operation_context = self._collection._new_operation_context(
                session=session,
                collation=operation.collation,
                bindings=operation.let,
            )
            operation = operation.with_overrides(
                context=operation_context,
                let=operation_context.expressions,
            )
        semantics = compile_find_semantics_from_operation(
            operation,
            dialect=self._collection._mongodb_dialect,
        )
        count = await self._engine_spi.count_documents(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
            operation_context=operation.context,
        )
        await self._collection._profile_operation(
            op='command',
            command={
                'count': self._collection._collection_name,
                'query': operation.filter_spec,
            },
            duration_ns=time.perf_counter_ns() - started_at,
            operation=operation,
        )
        return count

    async def select_first_document(
        self,
        filter_spec: Filter,
        *,
        plan: 'QueryNode' | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        variables=None,
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
            variables=variables,
            dialect=self._collection._mongodb_dialect,
            plan=plan,
            planning_mode=self._collection._planning_mode,
        )
        return await self._collection._build_cursor(
            operation,
            session=session,
            apply_codec_options=False,
        ).first()

    def build_cursor(
        self,
        operation: FindOperation,
        *,
        session: ClientSession | None = None,
        apply_codec_options: bool = True,
        execution_variables=None,
    ) -> AsyncCursor:
        operation_context = operation.context
        if operation_context is None:
            operation_context = self._collection._new_operation_context(
                session=session,
                collation=operation.collation,
                bindings=operation.let,
            )
            operation = operation.with_overrides(
                context=operation_context,
                let=operation_context.expressions,
            )
        if execution_variables is None:
            execution_variables = operation_context.expressions
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
            let=operation.let,
            execution_variables=execution_variables,
            operation_context=operation_context,
            session=session,
            apply_codec_options=apply_codec_options,
        )

    def build_upsert_replacement_document(
        self,
        filter_spec: Filter,
        replacement: Document,
    ) -> Document:
        from mongoeco.core.upserts import seed_upsert_document

        seeded: Document = {}
        seed_upsert_document(seeded, filter_spec)
        if '_id' in seeded and '_id' in replacement:
            if not self._collection._mongodb_dialect.values_equal(
                seeded['_id'], replacement['_id']
            ):
                raise WriteError(
                    'The _id field cannot conflict with the replacement filter during upsert',
                    code=66,
                )
        document = deepcopy(seeded)
        document.update(deepcopy(replacement))
        if '_id' not in document:
            document['_id'] = ObjectId()
        return document

    @staticmethod
    def materialize_replacement_document(
        selected: Document, replacement: Document
    ) -> Document:
        return materialize_replacement_document(selected, replacement)
