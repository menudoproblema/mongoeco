from __future__ import annotations

import threading
import uuid

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mongoeco.api.operations import UpdateOperation
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_models import (
    SearchExecutionMode,
    SearchExecutionOutcome,
    SearchExecutionState,
    SearchExecutionTrace,
    SearchExplainVerbosity,
)
from mongoeco.engines.capabilities import (
    resolve_engine_capabilities,
    validate_engine_contract,
)
from mongoeco.engines.results import (
    CommittedChange,
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.semantic_core import EngineFindSemantics
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.errors import OperationFailure
from mongoeco.types import QueryPlanExplanation


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


_DOCUMENT_ARGUMENT_INDEX = 2
_MISSING_ARGUMENT = object()
_LOCAL_CHANGE_CONSUMER_INSTANCE = uuid.uuid4().hex


def _call_argument(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    *,
    name: str,
    index: int,
    default: object = _MISSING_ARGUMENT,
) -> object:
    if name in kwargs:
        return kwargs[name]
    if len(args) > index:
        return args[index]
    if default is not _MISSING_ARGUMENT:
        return default
    message = f"missing required engine argument: {name}"
    raise TypeError(message)


def _require_callable(
    owner: object,
    name: str,
    *,
    message: str,
) -> Callable[..., Any]:
    method = getattr(owner, name, None)
    if not callable(method):
        raise TypeError(message)
    return method


def _require_operation_context(value: object) -> OperationContext:
    if not isinstance(value, OperationContext):
        message = "SPI v2 operations require OperationContext"
        raise TypeError(message)
    return value


def _require_bound_update_context(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    operation_context: OperationContext,
) -> None:
    operation = _call_argument(
        args,
        kwargs,
        name="operation",
        index=_DOCUMENT_ARGUMENT_INDEX,
        default=None,
    )
    if (
        isinstance(operation, UpdateOperation)
        and operation.context is not operation_context
    ):
        message = "SPI v2 update operation has a divergent context"
        raise RuntimeError(message)


def _require_bound_read_context(
    args: tuple[object, ...],
    kwargs: dict[str, object],
    operation_context: OperationContext,
) -> None:
    semantics = _call_argument(
        args,
        kwargs,
        name="semantics",
        index=_DOCUMENT_ARGUMENT_INDEX,
        default=None,
    )
    if (
        isinstance(semantics, EngineFindSemantics)
        and semantics.operation_context is not operation_context
    ):
        message = "SPI v2 read semantics have a divergent context"
        raise RuntimeError(message)


class EngineSpiAdapter:
    """Canonical mutation boundary consumed by the API layer."""

    def __init__(self, engine: object) -> None:
        self.engine = engine
        self.capabilities = resolve_engine_capabilities(engine)
        validate_engine_contract(engine, self.capabilities)
        # Registration may allocate and trigger a cursor finalizer on the same
        # thread; keep this tiny state guard reentrant like the close path.
        self._change_delivery_lock = threading.RLock()
        self._prepared_change_sink: object | None = None

    async def execute_search(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
    ) -> SearchExecutionOutcome:
        """Execute the optional typed Search contract declared by SPI v2."""
        if not isinstance(request, SearchRequest):
            message = "Search SPI requires SearchRequest"
            raise TypeError(message)
        search_capabilities = self.capabilities.search
        if search_capabilities is None:
            message = f"{request.operator} is not supported by this engine"
            raise OperationFailure(message)
        effective_operator = request.effective_operator
        if effective_operator not in search_capabilities.operators:
            message = f"engine does not declare {effective_operator} support"
            raise OperationFailure(message)
        similarity = getattr(request.query, "similarity", None)
        if (
            effective_operator == "$vectorSearch"
            and similarity not in search_capabilities.vector_similarities
        ):
            message = f"engine does not declare vector similarity [{similarity}]"
            raise OperationFailure(message)
        if (
            request.mode is SearchExecutionMode.METADATA
            and not search_capabilities.metadata_collectors
        ):
            message = "engine does not declare Search metadata collector support"
            raise OperationFailure(message)
        stage_options = getattr(request.query, "stage_options", None)
        if (
            stage_options is not None
            and getattr(stage_options, "highlight", None) is not None
            and not search_capabilities.highlight
        ):
            message = "engine does not declare Search highlight support"
            raise OperationFailure(message)
        execute = _require_callable(
            self.engine,
            "execute_search",
            message=(
                "engine declaring Search capabilities must implement execute_search"
            ),
        )
        outcome = await execute(db_name, coll_name, request)
        if not isinstance(outcome, SearchExecutionOutcome):
            message = "Search SPI must return SearchExecutionOutcome"
            raise TypeError(message)
        if request.mode is SearchExecutionMode.METADATA and outcome.hits:
            message = "metadata Search outcome cannot contain hits"
            raise RuntimeError(message)
        if request.mode is SearchExecutionMode.HITS and (
            outcome.metadata.count is not None or outcome.metadata.facets
        ):
            message = "hits Search outcome cannot contain collector metadata"
            raise RuntimeError(message)
        return outcome

    async def explain_search(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
        verbosity: SearchExplainVerbosity,
    ) -> QueryPlanExplanation:
        """Explain Search through the optional typed SPI v2 contract."""
        if not isinstance(request, SearchRequest):
            message = "Search SPI requires SearchRequest"
            raise TypeError(message)
        if not isinstance(verbosity, SearchExplainVerbosity):
            message = "Search explain requires SearchExplainVerbosity"
            raise TypeError(message)
        search_capabilities = self.capabilities.search
        if search_capabilities is not None and search_capabilities.explain_verbosity:
            explain = _require_callable(
                self.engine,
                "explain_search",
                message=(
                    "engine declaring Search explain support must implement "
                    "explain_search"
                ),
            )
            explanation = await explain(
                db_name,
                coll_name,
                request,
                verbosity,
            )
            if not isinstance(explanation, QueryPlanExplanation):
                message = "Search explain SPI must return QueryPlanExplanation"
                raise TypeError(message)
            return self._attach_search_pipeline_plan(
                explanation,
                request,
                verbosity,
            )

        explanation = QueryPlanExplanation(
            engine=type(self.engine).__name__,
            strategy="search",
            plan="unsupported-search-engine",
            sort=None,
            skip=0,
            limit=None,
            hint=None,
            hinted_index=request.query.index_name,
            comment=None,
            max_time_ms=request.max_time_ms,
            details={
                "operator": request.operator,
                "verbosity": verbosity.value,
                "executionStats": None,
                "degradation": "engine-has-no-search-explain-spi",
            },
        )
        return self._attach_search_pipeline_plan(explanation, request, verbosity)

    @staticmethod
    def _attach_search_pipeline_plan(
        explanation: QueryPlanExplanation,
        request: SearchRequest,
        verbosity: SearchExplainVerbosity,
    ) -> QueryPlanExplanation:
        if request.pipeline_plan is None and not isinstance(
            explanation.details,
            dict,
        ):
            return explanation
        details = (
            dict(explanation.details)
            if isinstance(explanation.details, dict)
            else {"engineDetails": explanation.details}
        )
        if request.pipeline_plan is not None:
            details["pipelinePlan"] = request.pipeline_plan.to_document()
        if verbosity is SearchExplainVerbosity.QUERY_PLANNER:
            details["executionObservation"] = SearchExecutionTrace(
                backend=explanation.engine,
                operation_id=request.operation_context.operation_id,
                execution_state=SearchExecutionState.PLANNED,
            ).to_document()
        elif isinstance(details.get("executionStats"), dict):
            execution_stats = dict(details["executionStats"])
            execution_stats.setdefault(
                "operationId",
                request.operation_context.operation_id,
            )
            details["executionStats"] = execution_stats
        return replace(explanation, details=details)

    def prepare_change_delivery(self, sink: object | None) -> None:
        if (
            self.capabilities.change_delivery
            not in {
                "commit-sequence",
                "transactional-outbox",
            }
            or sink is None
        ):
            return
        with self._change_delivery_lock:
            if self._prepared_change_sink is sink:
                return
            register = _require_callable(
                self.engine,
                "register_change_consumer",
                message=(
                    "transactional-outbox engine must implement "
                    "register_change_consumer"
                ),
            )
            journal_path = getattr(sink, "journal_path", None)
            initial_checkpoint = None
            if journal_path is not None:
                state = getattr(sink, "state", None)
                next_token = getattr(state, "next_token", None)
                if isinstance(next_token, int):
                    initial_checkpoint = next_token - 1
            checkpoint = register(
                self._change_consumer_id(sink),
                initial_checkpoint=initial_checkpoint,
                durable=journal_path is not None,
            )
            align = getattr(sink, "align_commit_sequence", None)
            if callable(align):
                align(int(checkpoint) + 1)
            self._prepared_change_sink = sink

    def dispatch_committed_changes(self, sink: object | None) -> None:
        if (
            self.capabilities.change_delivery
            not in {
                "commit-sequence",
                "transactional-outbox",
            }
            or sink is None
        ):
            return
        dispatch = _require_callable(
            self.engine,
            "dispatch_committed_changes",
            message=(
                "transactional-outbox engine must implement dispatch_committed_changes"
            ),
        )
        dispatch(
            self._change_consumer_id(sink),
            lambda change: self._deliver_committed_change(sink, change),
        )

    def unregister_change_delivery(self, sink: object | None) -> None:
        if not self.capabilities.monotonic_commit_sequence or sink is None:
            return
        unregister = _require_callable(
            self.engine,
            "unregister_change_consumer",
            message=(
                "sequenced change-delivery engine must implement "
                "unregister_change_consumer"
            ),
        )
        unregister(self._change_consumer_id(sink))
        with self._change_delivery_lock:
            if self._prepared_change_sink is sink:
                self._prepared_change_sink = None

    @staticmethod
    def _change_consumer_id(sink: object) -> str:
        journal_path = getattr(sink, "journal_path", None)
        if isinstance(journal_path, str):
            canonical_path = Path(journal_path).expanduser().resolve()
            return f"journal-change-hub:{canonical_path}"
        return f"local-change-hub:{_LOCAL_CHANGE_CONSUMER_INSTANCE}:{id(sink)}"

    @staticmethod
    def _deliver_committed_change(
        sink: object,
        change: CommittedChange,
    ) -> None:
        state = getattr(sink, "state", None)
        next_token = getattr(state, "next_token", None)
        if isinstance(next_token, int) and change.sequence < next_token:
            return
        align = getattr(sink, "align_commit_sequence", None)
        if callable(align):
            align(change.sequence)
        if change.is_gap:
            mark_gap = _require_callable(
                sink,
                "mark_gap",
                message="change sink must implement mark_gap",
            )
            mark_gap()
            return
        publish = _require_callable(
            sink,
            "publish",
            message="change sink must implement publish",
        )
        publish(**change.payload)

    async def update_outcome(
        self,
        *args: object,
        on_commit: Callable[[MutationOutcome], None] | None = None,
        **kwargs: object,
    ) -> MutationOutcome:
        method = _require_callable(
            self.engine,
            "update_with_operation",
            message="engine must implement update_with_operation",
        )
        call_kwargs = dict(kwargs)
        operation_context = _require_operation_context(
            call_kwargs.get("operation_context"),
        )
        _require_bound_update_context(args, call_kwargs, operation_context)
        result = await method(*args, **call_kwargs)
        outcome = self._require_mutation_outcome(
            result,
            operation_context=operation_context,
        )
        if self._publishes_after_return and on_commit is not None:
            on_commit(outcome)
        return outcome

    async def delete_outcome(
        self,
        *args: object,
        on_commit: Callable[[DeleteOutcome], None] | None = None,
        **kwargs: object,
    ) -> DeleteOutcome:
        method = _require_callable(
            self.engine,
            "delete_with_operation",
            message="engine must implement delete_with_operation",
        )
        call_kwargs = dict(kwargs)
        operation_context = _require_operation_context(
            call_kwargs.get("operation_context"),
        )
        _require_bound_update_context(args, call_kwargs, operation_context)
        result = await method(*args, **call_kwargs)
        outcome = self._require_delete_outcome(
            result,
            operation_context=operation_context,
        )
        if self._publishes_after_return and on_commit is not None:
            on_commit(outcome)
        return outcome

    async def insert_outcome(
        self,
        *args: object,
        on_commit: Callable[[InsertOutcome], None] | None = None,
        **kwargs: object,
    ) -> InsertOutcome:
        method = _require_callable(
            self.engine,
            "insert_document",
            message="SPI v2 engine must implement insert_document",
        )
        call_kwargs = dict(kwargs)
        operation_context = _require_operation_context(
            call_kwargs.get("operation_context"),
        )
        outcome = self._require_insert_outcome(
            await method(*args, **call_kwargs),
            operation_context=operation_context,
        )
        if self._publishes_after_return and on_commit is not None and outcome:
            on_commit(outcome)
        return outcome

    async def insert_many_outcomes(
        self,
        db_name: object,
        coll_name: object,
        documents: Sequence[object],
        *,
        on_commit: Callable[[InsertOutcome], None] | None = None,
        **kwargs: object,
    ) -> tuple[InsertOutcome, ...]:
        if self.capabilities.batch_inserts:
            method = _require_callable(
                self.engine,
                "insert_documents",
                message="SPI v2 engine must implement insert_documents",
            )
            call_kwargs = dict(kwargs)
            _require_operation_context(call_kwargs.get("operation_context"))
            results = tuple(
                await method(
                    db_name,
                    coll_name,
                    documents,
                    **call_kwargs,
                ),
            )
            outcomes = tuple(
                self._require_insert_outcome(
                    item,
                    operation_context=call_kwargs["operation_context"],
                )
                for item in results
            )
            if len(outcomes) > len(documents) or (
                len(outcomes) != len(documents)
                and (not outcomes or outcomes[-1].applied)
            ):
                message = "batch insert outcome cardinality is inconsistent"
                raise RuntimeError(message)
        else:
            base_context = _require_operation_context(
                kwargs.get("operation_context"),
            )
            outcomes_list: list[InsertOutcome] = []
            for event_index, document in enumerate(documents):
                single_kwargs = dict(kwargs)
                single_kwargs["operation_context"] = base_context.derive(
                    change_event_index=event_index,
                )
                outcome = await self.insert_outcome(
                    db_name,
                    coll_name,
                    document,
                    on_commit=None,
                    **single_kwargs,
                )
                outcomes_list.append(outcome)
                if not outcome.applied:
                    break
            outcomes = tuple(outcomes_list)
        if self._publishes_after_return and on_commit is not None:
            for outcome in outcomes:
                if outcome:
                    on_commit(outcome)
        return outcomes

    async def merge_outcome(
        self,
        *args: object,
        on_commit: Callable[[MergeOutcome], None] | None = None,
        **kwargs: object,
    ) -> MergeOutcome:
        method = _require_callable(
            self.engine,
            "merge_document",
            message="engine must implement merge_document",
        )
        call_kwargs = dict(kwargs)
        operation_context = _require_operation_context(
            call_kwargs.get("operation_context"),
        )
        outcome = await method(*args, **call_kwargs)
        outcome = self._require_merge_outcome(
            outcome,
            operation_context=operation_context,
        )
        if self._publishes_after_return and on_commit is not None:
            on_commit(outcome)
        return outcome

    def open_read_snapshot(
        self,
        *args: object,
        operation_context=None,
        **kwargs: object,
    ) -> ReadSnapshot:
        operation_context = _require_operation_context(operation_context)
        _require_bound_read_context(args, kwargs, operation_context)
        if self.capabilities.explicit_read_snapshots:
            method = _require_callable(
                self.engine,
                "open_read_snapshot",
                message=(
                    "SPI v2 engine declaring explicit snapshots must "
                    "implement open_read_snapshot"
                ),
            )
            snapshot = method(
                *args,
                operation_context=operation_context,
                **kwargs,
            )
            if not isinstance(snapshot, ReadSnapshot):
                message = "SPI v2 engine did not return ReadSnapshot"
                raise TypeError(message)
            if snapshot.metadata.operation_id != operation_context.operation_id:
                snapshot.discard()
                message = "SPI v2 snapshot operation identity is inconsistent"
                raise RuntimeError(message)
            if snapshot.metadata.policy is not SnapshotPolicy.STABLE:
                snapshot.discard()
                message = "collection reads require a STABLE SPI v2 snapshot"
                raise RuntimeError(message)
            return snapshot
        method = _require_callable(
            self.engine,
            "scan_find_semantics",
            message=(
                "engine without explicit read snapshots must implement "
                "scan_find_semantics"
            ),
        )
        source = method(
            *args,
            context=operation_context.session,
            **kwargs,
        )
        return ReadSnapshot(
            source,
            policy=SnapshotPolicy.STABLE,
            operation_id=operation_context.operation_id,
        )

    async def get_document(
        self,
        *args: object,
        operation_context=None,
        **kwargs: object,
    ):
        method = _require_callable(
            self.engine,
            "get_document",
            message="engine must implement get_document",
        )
        operation_context = _require_operation_context(operation_context)
        return await method(
            *args,
            operation_context=operation_context,
            **kwargs,
        )

    async def count_documents(
        self,
        *args: object,
        operation_context=None,
        **kwargs: object,
    ) -> int:
        method = _require_callable(
            self.engine,
            "count_find_semantics",
            message="engine must implement count_find_semantics",
        )
        operation_context = _require_operation_context(operation_context)
        _require_bound_read_context(args, kwargs, operation_context)
        return int(
            await method(
                *args,
                operation_context=operation_context,
                **kwargs,
            ),
        )

    @property
    def _publishes_after_return(self) -> bool:
        return self.capabilities.change_delivery == "none"

    def _require_mutation_outcome(
        self,
        result: Any,
        *,
        operation_context: OperationContext | None = None,
    ) -> MutationOutcome:
        if isinstance(result, MutationOutcome):
            matched = result.matched_count > 0
            applied = result.modified_count > 0 or result.upserted_id is not None
            if (matched or applied) and result.after_document is None:
                message = "an applied SPI v2 mutation must expose its after image"
                raise RuntimeError(message)
            if matched and result.before_document is None:
                message = "a modified SPI v2 mutation must expose its before image"
                raise RuntimeError(message)
            self._validate_commit_sequence_contract(
                result.commit_sequence,
                applied=applied,
                operation_context=operation_context,
            )
            return result
        message = "SPI v2 engine did not return MutationOutcome"
        raise TypeError(message)

    def _require_delete_outcome(
        self,
        result: Any,
        *,
        operation_context: OperationContext | None = None,
    ) -> DeleteOutcome:
        if isinstance(result, DeleteOutcome):
            if result.deleted_count > 0 and result.deleted_document is None:
                message = "an applied SPI v2 delete must expose its deleted image"
                raise RuntimeError(message)
            self._validate_commit_sequence_contract(
                result.commit_sequence,
                applied=result.deleted_count > 0,
                operation_context=operation_context,
            )
            return result
        message = "SPI v2 engine did not return DeleteOutcome"
        raise TypeError(message)

    def _require_insert_outcome(
        self,
        result: Any,
        *,
        operation_context: OperationContext | None = None,
    ) -> InsertOutcome:
        if isinstance(result, InsertOutcome):
            if result.applied and result.document is None:
                message = "an applied SPI v2 insert must expose its document"
                raise RuntimeError(message)
            self._validate_commit_sequence_contract(
                result.commit_sequence,
                applied=result.applied,
                operation_context=operation_context,
            )
            return result
        message = "SPI v2 engine did not return InsertOutcome"
        raise TypeError(message)

    def _require_merge_outcome(
        self,
        result: Any,
        *,
        operation_context: OperationContext | None = None,
    ) -> MergeOutcome:
        if not isinstance(result, MergeOutcome):
            message = "engine did not return MergeOutcome"
            raise TypeError(message)
        self._validate_commit_sequence_contract(
            result.commit_sequence,
            applied=result.applied,
            operation_context=operation_context,
        )
        return result

    def _validate_commit_sequence_contract(
        self,
        sequence: int | None,
        *,
        applied: bool,
        operation_context: OperationContext | None,
    ) -> None:
        requires_sequence = (
            self.capabilities.monotonic_commit_sequence
            and operation_context is not None
            and operation_context.publication is not ChangePublicationPolicy.DISABLED
            and (
                operation_context.session is None
                or not operation_context.session.in_transaction
            )
        )
        if requires_sequence and applied and sequence is None:
            message = "an applied sequenced mutation requires commit_sequence"
            raise RuntimeError(message)
