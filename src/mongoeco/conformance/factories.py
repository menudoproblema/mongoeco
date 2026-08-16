from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator, Iterable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import (
    ChangeOperationType,
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
    RuntimeMetadataKey,
)
from mongoeco.core.search import compile_search_stage
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_models import SearchExecutionMode
from mongoeco.engines.results import (
    CommittedChange,
    DeleteOutcome,
    InsertOutcome,
    MutationOutcome,
)
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.types import DeleteResult, Document, UpdateResult


DEFAULT_CONFORMANCE_NOW = datetime(2026, 1, 2, 3, 4, 5, 123000, tzinfo=UTC)


def operation_context_factory(
    *,
    now: datetime = DEFAULT_CONFORMANCE_NOW,
    publication: ChangePublicationPolicy = ChangePublicationPolicy.DISABLED,
    change_operation_type: ChangeOperationType | None = None,
) -> OperationContext:
    """Build an isolated deterministic context for an external-engine check."""
    return OperationContext.create(
        dialect=MONGODB_DIALECT_70,
        expressions=ExpressionExecutionContext(now=now),
        publication=publication,
        change_operation_type=change_operation_type,
    )


@dataclass(slots=True)
class DeterministicClock:
    value: datetime = DEFAULT_CONFORMANCE_NOW
    captures: int = 0

    def capture_context(
        self,
        *,
        publication: ChangePublicationPolicy = ChangePublicationPolicy.DISABLED,
        change_operation_type: ChangeOperationType | None = None,
    ) -> OperationContext:
        self.captures += 1
        return operation_context_factory(
            now=self.value,
            publication=publication,
            change_operation_type=change_operation_type,
        )


@dataclass(slots=True)
class ConcurrentBarrier:
    parties: int
    _condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    _waiting: int = 0
    _generation: int = 0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.parties, int)
            or isinstance(self.parties, bool)
            or self.parties < 1
        ):
            message = "conformance barrier parties must be positive"
            raise ValueError(message)

    async def wait(self) -> int:
        async with self._condition:
            generation = self._generation
            position = self._waiting
            self._waiting += 1
            if self._waiting == self.parties:
                self._waiting = 0
                self._generation += 1
                self._condition.notify_all()
                return position
            await self._condition.wait_for(lambda: self._generation != generation)
            return position


class ControlledSnapshotSource(AsyncIterator[Document]):
    """Cancellation-aware source exposing deterministic lifecycle probes."""

    def __init__(self, documents: Iterable[Document]) -> None:
        self._documents = iter(deepcopy(tuple(documents)))
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False

    def __aiter__(self) -> ControlledSnapshotSource:
        return self

    async def __anext__(self) -> Document:
        self.started.set()
        await self.release.wait()
        try:
            return deepcopy(next(self._documents))
        except StopIteration as error:
            raise StopAsyncIteration from error

    async def aclose(self) -> None:
        self.closed = True


@dataclass(slots=True)
class CancellationScenario:
    snapshot: ReadSnapshot
    source: ControlledSnapshotSource
    cancelled: bool = False

    async def cancel_pending_read(self) -> None:
        """Cancel one blocked read and require the owned snapshot to close."""
        read = asyncio.create_task(self.snapshot.__anext__())
        await self.source.started.wait()
        read.cancel()
        try:
            await read
        except asyncio.CancelledError:
            self.cancelled = True
        else:
            message = "controlled cancellation did not cancel the pending read"
            raise AssertionError(message)
        if not self.source.closed or not self.snapshot.closed:
            message = "controlled cancellation leaked snapshot resources"
            raise AssertionError(message)


def cancellation_factory(
    documents: Iterable[Document] = ({"_id": "cancelled"},),
) -> CancellationScenario:
    """Build a deterministic blocked-read cancellation scenario."""
    snapshot, source = snapshot_factory(documents, controlled=True)
    if source is None:  # pragma: no cover - guarded by controlled=True
        message = "controlled snapshot source was not created"
        raise AssertionError(message)
    return CancellationScenario(snapshot, source)


def snapshot_factory(
    documents: Iterable[Document],
    *,
    controlled: bool = False,
    policy: SnapshotPolicy = SnapshotPolicy.STABLE,
) -> tuple[ReadSnapshot, ControlledSnapshotSource | None]:
    source = ControlledSnapshotSource(documents)
    if not controlled:
        source.release.set()
    return ReadSnapshot(source, policy=policy), source if controlled else None


@dataclass(frozen=True, slots=True)
class OutcomeFixtures:
    inserted: InsertOutcome
    matched: MutationOutcome
    unmatched: MutationOutcome
    deleted: DeleteOutcome


def outcome_factory(*, commit_sequence: int | None = None) -> OutcomeFixtures:
    before = {"_id": "value", "revision": 1}
    after = {"_id": "value", "revision": 2}
    return OutcomeFixtures(
        inserted=InsertOutcome(
            applied=True,
            document=before,
            commit_sequence=commit_sequence,
        ),
        matched=MutationOutcome(
            result=UpdateResult(1, 1),
            before_document=before,
            after_document=after,
            commit_sequence=commit_sequence,
        ),
        unmatched=MutationOutcome(result=UpdateResult(0, 0)),
        deleted=DeleteOutcome(
            result=DeleteResult(1),
            deleted_document=after,
            commit_sequence=commit_sequence,
        ),
    )


@dataclass(frozen=True, slots=True)
class PartialBatchScenario:
    documents: tuple[Document, ...]
    failure_index: int

    def __post_init__(self) -> None:
        if not self.documents:
            message = "partial batch scenario requires documents"
            raise ValueError(message)
        if not 0 <= self.failure_index < len(self.documents):
            message = "partial batch failure_index is out of range"
            raise ValueError(message)
        object.__setattr__(self, "documents", deepcopy(self.documents))

    @property
    def acknowledged_prefix(self) -> tuple[Document, ...]:
        return deepcopy(self.documents[: self.failure_index])

    @property
    def rejected_suffix(self) -> tuple[Document, ...]:
        return deepcopy(self.documents[self.failure_index :])


def partial_batch_factory(*, failure_index: int = 1) -> PartialBatchScenario:
    return PartialBatchScenario(
        documents=tuple({"_id": f"batch-{index}"} for index in range(3)),
        failure_index=failure_index,
    )


def change_delivery_factory(
    *,
    first_sequence: int = 1,
    count: int = 2,
    include_gap: bool = False,
) -> tuple[CommittedChange, ...]:
    if count < 1:
        message = "change delivery fixture count must be positive"
        raise ValueError(message)
    return tuple(
        CommittedChange(
            first_sequence + offset,
            None
            if include_gap and offset == count - 1
            else {"_id": f"event-{offset + 1}"},
        )
        for offset in range(count)
    )


def search_request_factory(
    *,
    context: OperationContext | None = None,
    metadata: bool = False,
    highlight: bool = False,
) -> SearchRequest:
    if metadata and highlight:
        message = "Search metadata fixture cannot include hit highlights"
        raise ValueError(message)
    specification: dict[str, object] = {
        "index": "by_text",
        "text": {"query": "ada", "path": "title"},
    }
    if metadata:
        specification["count"] = {"type": "total"}
        specification["facet"] = {"path": "kind", "type": "token"}
    if highlight:
        specification["highlight"] = {"path": "title"}
    operator = "$searchMeta" if metadata else "$search"
    return SearchRequest(
        operator=operator,
        specification=specification,
        query=compile_search_stage(operator, specification),
        mode=(SearchExecutionMode.METADATA if metadata else SearchExecutionMode.HITS),
        operation_context=context or operation_context_factory(),
        runtime_operator="$search" if metadata else None,
        runtime_specification=specification if metadata else None,
    )


def runtime_metadata_factory(
    document: Document | None = None,
) -> RuntimeDocumentState:
    highlights = [{"path": "title", "texts": [{"type": "hit", "value": "Ada"}]}]
    return (
        RuntimeDocumentState(document or {"_id": "search", "title": "Ada"})
        .with_metadata_value(RuntimeMetadataKey.TEXT_SCORE, 1.0)
        .with_metadata_value(RuntimeMetadataKey.SEARCH_HIGHLIGHTS, highlights)
        .with_virtual_field(
            "searchHighlights",
            highlights,
            source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
    )
