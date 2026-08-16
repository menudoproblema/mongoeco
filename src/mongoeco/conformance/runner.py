from __future__ import annotations

import asyncio
import time
import uuid

from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.conformance.models import (
    DEFAULT_SPI_V2_PROFILES,
    ConformanceCheckResult,
    ConformancePhase,
    ConformanceProfile,
    ConformanceReport,
    ConformanceStatus,
)
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import ChangePublicationPolicy, OperationContext
from mongoeco.core.runtime_metadata import RuntimeMetadataKey
from mongoeco.core.search import compile_search_stage
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_models import (
    SearchExecutionMode,
    SearchExplainVerbosity,
)
from mongoeco.engines.adapter import adapt_engine
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
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.types import SearchIndexDefinition


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from mongoeco.conformance.provider import EngineConformanceProvider


_SPI_V2 = 2
_EXPECTED_CHANGE_EVENTS = 2


@dataclass(frozen=True, slots=True)
class _CheckDefinition:
    profile: ConformanceProfile
    name: str
    capability: str
    check: Callable[[Any, str, str], Awaitable[None]]
    applicable: bool
    inapplicable_detail: str | None = None


def _context() -> OperationContext:
    return OperationContext.create(dialect=MONGODB_DIALECT_70)


async def run_engine_conformance(
    provider: EngineConformanceProvider,
    *,
    profiles: Iterable[ConformanceProfile] = DEFAULT_SPI_V2_PROFILES,
) -> ConformanceReport:
    """Run versioned public contracts and return every observed failure."""
    selected = frozenset(profiles)
    checks: list[ConformanceCheckResult] = []
    async with provider.open_engine() as engine:
        capabilities = resolve_engine_capabilities(engine)
        for definition in _checks(capabilities):
            if definition.profile not in selected:
                continue
            if not definition.applicable:
                checks.append(
                    ConformanceCheckResult(
                        profile=definition.profile,
                        name=definition.name,
                        status=ConformanceStatus.NOT_APPLICABLE,
                        capability=definition.capability,
                        detail=(
                            definition.inapplicable_detail
                            or "capability is not declared by the engine"
                        ),
                        evidence={"capabilityDeclared": False},
                    )
                )
                continue
            db_name, coll_name = provider.namespace(definition.name)
            started = time.perf_counter()
            status = ConformanceStatus.PASSED
            phase = ConformancePhase.CONTRACT
            detail: str | None = None
            cleanup_error: str | None = None
            try:
                await definition.check(engine, db_name, coll_name)
            except AssertionError as error:
                status = ConformanceStatus.FAILED
                detail = f"{type(error).__name__}: {error}"
            except Exception as error:
                status = ConformanceStatus.ERROR
                detail = f"{type(error).__name__}: {error}"
            try:
                await provider.cleanup_namespace(engine, db_name)
            except Exception as error:
                cleanup_error = f"{type(error).__name__}: {error}"
                if status is ConformanceStatus.PASSED:
                    status = ConformanceStatus.ERROR
                    phase = ConformancePhase.CLEANUP
                    detail = f"cleanup {cleanup_error}"
            checks.append(
                ConformanceCheckResult(
                    profile=definition.profile,
                    name=definition.name,
                    status=status,
                    capability=definition.capability,
                    phase=phase,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    detail=detail,
                    evidence={"capabilityDeclared": True},
                    cleanup_error=cleanup_error,
                )
            )
    return ConformanceReport(
        provider_name=provider.name,
        contract_version="spi-v2",
        checks=tuple(checks),
    )


def _checks(capabilities):
    return (
        _CheckDefinition(
            ConformanceProfile.SPI_V2_CORE,
            "capabilities",
            "spi-v2",
            _check_capabilities,
            applicable=True,
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_CORE,
            "crud-outcomes",
            "typed-outcomes",
            _check_crud_outcomes,
            applicable=True,
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_CORE,
            "batch-outcomes",
            "batch-inserts",
            _check_batch_outcomes,
            applicable=capabilities.batch_inserts,
            inapplicable_detail="engine does not declare batch insert support",
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_CORE,
            "operation-context",
            "operation-context",
            _check_operation_context,
            applicable=True,
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_ATOMICITY,
            "compare-and-set",
            "atomic-conditional-mutation",
            _check_atomic_mutation,
            applicable=True,
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_CLOCK,
            "injected-clock",
            "injected-clock",
            _check_injected_clock,
            applicable=capabilities.injected_clock,
            inapplicable_detail="engine does not declare injected-clock support",
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_SNAPSHOTS,
            "stable-snapshot",
            "stable-snapshot",
            _check_snapshot,
            applicable=capabilities.spi_version == _SPI_V2,
            inapplicable_detail="engine does not implement SPI v2 snapshots",
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_CHANGE_DELIVERY,
            "change-delivery-contract",
            "sequenced-change-delivery",
            _check_change_delivery,
            applicable=capabilities.monotonic_commit_sequence,
            inapplicable_detail="engine does not declare monotonic change delivery",
        ),
        _CheckDefinition(
            ConformanceProfile.SPI_V2_CHANGE_DELIVERY,
            "change-delivery-recovery",
            "sequenced-change-delivery",
            _check_change_delivery_recovery,
            applicable=capabilities.monotonic_commit_sequence,
            inapplicable_detail="engine does not declare monotonic change delivery",
        ),
        _CheckDefinition(
            ConformanceProfile.SEARCH_V1,
            "search-contract",
            "search-v1",
            _check_search,
            applicable=capabilities.search is not None,
            inapplicable_detail="engine does not declare Search support",
        ),
    )


async def _check_capabilities(engine: object, _db: str, _coll: str) -> None:
    capabilities = resolve_engine_capabilities(engine)
    if capabilities.spi_version != _SPI_V2:
        message = "conformance requires SPI v2"
        raise AssertionError(message)
    try:
        validate_engine_contract(engine, capabilities)
    except (TypeError, ValueError) as error:
        raise AssertionError(str(error)) from error


async def _check_crud_outcomes(engine: object, db: str, coll: str) -> None:
    context = _context()
    inserted = await engine.insert_document(
        db,
        coll,
        {"_id": "value", "revision": 1},
        overwrite=False,
        operation_context=context,
    )
    if not isinstance(inserted, InsertOutcome) or not inserted.applied:
        message = "insert_document must return an applied InsertOutcome"
        raise AssertionError(message)
    update_context = _context()
    operation = compile_update_operation(
        {"_id": "value"},
        update_spec={"$set": {"revision": 2}},
        dialect=MONGODB_DIALECT_70,
    ).with_overrides(
        context=update_context,
        let=update_context.expressions,
    )
    updated = await engine.update_with_operation(
        db,
        coll,
        operation,
        operation_context=update_context,
    )
    if not isinstance(updated, MutationOutcome) or updated.modified_count != 1:
        message = "update_with_operation must return MutationOutcome"
        raise AssertionError(message)
    merged = await engine.merge_document(
        db,
        coll,
        {"_id": "merged"},
        when_matched="replace",
        when_not_matched="insert",
        operation_context=_context(),
    )
    if not isinstance(merged, MergeOutcome) or not merged.applied:
        message = "merge_document must return MergeOutcome"
        raise AssertionError(message)
    delete_context = _context()
    delete_operation = compile_update_operation(
        {"_id": "value"},
        dialect=MONGODB_DIALECT_70,
    ).with_overrides(
        context=delete_context,
        let=delete_context.expressions,
    )
    deleted = await engine.delete_with_operation(
        db,
        coll,
        delete_operation,
        operation_context=delete_context,
    )
    if not isinstance(deleted, DeleteOutcome) or deleted.deleted_count != 1:
        message = "delete_with_operation must return DeleteOutcome"
        raise AssertionError(message)


async def _check_batch_outcomes(engine: object, db: str, coll: str) -> None:
    documents = [
        {"_id": "batch-1", "nested": {"value": 1}},
        {"_id": "batch-2", "nested": {"value": 2}},
    ]
    original = deepcopy(documents)
    try:
        outcomes = await adapt_engine(engine).insert_many_outcomes(
            db,
            coll,
            documents,
            operation_context=_context(),
        )
    except (TypeError, ValueError, RuntimeError) as error:
        raise AssertionError(str(error)) from error
    if len(outcomes) != len(documents) or not all(
        isinstance(outcome, InsertOutcome) and outcome.applied for outcome in outcomes
    ):
        message = "declared batch inserts must return one applied outcome per input"
        raise AssertionError(message)
    if documents != original:
        message = "batch insert must not mutate caller-owned documents"
        raise AssertionError(message)
    documents[0]["nested"]["value"] = 999
    stored = await engine.get_document(
        db,
        coll,
        "batch-1",
        operation_context=_context(),
    )
    if stored != original[0]:
        message = "batch outcomes must not alias engine-owned documents"
        raise AssertionError(message)


async def _check_operation_context(
    engine: object,
    db: str,
    coll: str,
) -> None:
    context = _context()
    await engine.insert_document(
        db,
        coll,
        {"_id": "context"},
        overwrite=False,
        operation_context=context,
    )
    stored = await engine.get_document(
        db,
        coll,
        "context",
        operation_context=context,
    )
    if stored != {"_id": "context"}:
        message = "OperationContext must cross the write/read boundary intact"
        raise AssertionError(message)


async def _check_atomic_mutation(engine: object, db: str, coll: str) -> None:
    context = _context()
    await engine.insert_document(
        db,
        coll,
        {"_id": "cas", "revision": 1, "fence": 0},
        overwrite=False,
        operation_context=context,
    )

    start = asyncio.Event()

    async def compete() -> MutationOutcome:
        await start.wait()
        competitor_context = _context()
        operation = compile_update_operation(
            {"_id": "cas", "revision": 1},
            update_spec={"$inc": {"revision": 1, "fence": 1}},
            dialect=MONGODB_DIALECT_70,
        ).with_overrides(
            context=competitor_context,
            let=competitor_context.expressions,
        )
        return await engine.update_with_operation(
            db,
            coll,
            operation,
            operation_context=competitor_context,
        )

    competitors = [asyncio.create_task(compete()) for _ in range(8)]
    await asyncio.sleep(0)
    start.set()
    outcomes = await asyncio.gather(*competitors)
    if not all(isinstance(outcome, MutationOutcome) for outcome in outcomes):
        message = "atomic mutations must return MutationOutcome"
        raise AssertionError(message)
    winners = [outcome for outcome in outcomes if outcome.matched_count == 1]
    if len(winners) != 1:
        message = "compare-and-set must produce exactly one winner"
        raise AssertionError(message)
    stored = await engine.get_document(
        db,
        coll,
        "cas",
        operation_context=_context(),
    )
    if stored != {"_id": "cas", "revision": 2, "fence": 1}:
        message = "compare-and-set must commit only the winning mutation"
        raise AssertionError(message)


async def _check_injected_clock(engine: object, db: str, coll: str) -> None:
    fixed_now = datetime(2026, 1, 2, 3, 4, 5, 123000, tzinfo=UTC)
    expressions = ExpressionExecutionContext(now=fixed_now)
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_70,
        expressions=expressions,
    )
    await engine.insert_document(
        db,
        coll,
        {"_id": "clock"},
        overwrite=False,
        operation_context=context,
    )
    operation = compile_update_operation(
        {"_id": "clock"},
        update_spec=[{"$set": {"capturedAt": "$$NOW"}}],
        dialect=MONGODB_DIALECT_70,
    ).with_overrides(context=context, let=context.expressions)
    outcome = await engine.update_with_operation(
        db,
        coll,
        operation,
        operation_context=context,
    )
    if not isinstance(outcome, MutationOutcome) or outcome.modified_count != 1:
        message = "injected clock update must return MutationOutcome"
        raise AssertionError(message)
    stored = await engine.get_document(
        db,
        coll,
        "clock",
        operation_context=context,
    )
    expected_now = DocumentCodec.to_internal(fixed_now)
    if stored != {"_id": "clock", "capturedAt": expected_now}:
        message = "$$NOW must use the OperationContext clock exactly once"
        raise AssertionError(message)


async def _check_snapshot(engine: object, db: str, coll: str) -> None:
    context = _context()
    await engine.insert_document(
        db,
        coll,
        {"_id": "snapshot", "nested": {"value": 1}},
        overwrite=False,
        operation_context=context,
    )
    await engine.insert_document(
        db,
        coll,
        {"_id": "snapshot-2"},
        overwrite=False,
        operation_context=context,
    )
    capabilities = resolve_engine_capabilities(engine)
    snapshot_owner = (
        adapt_engine(engine) if capabilities.spi_version == _SPI_V2 else engine
    )
    snapshot = snapshot_owner.open_read_snapshot(
        db,
        coll,
        compile_find_semantics(
            {},
            sort=[("_id", 1)],
            operation_context=context,
        ),
        operation_context=context,
    )
    if not isinstance(snapshot, ReadSnapshot):
        message = "open_read_snapshot must return ReadSnapshot"
        raise AssertionError(message)
    try:
        first = await snapshot.__anext__()
        await engine.insert_document(
            db,
            coll,
            {"_id": "after-snapshot"},
            overwrite=False,
            operation_context=_context(),
        )
        documents = [first, *[document async for document in snapshot]]
    finally:
        await snapshot.aclose()
    if snapshot.metadata.policy is not SnapshotPolicy.STABLE:
        message = "snapshot must declare STABLE policy"
        raise AssertionError(message)
    if (
        documents
        != [
            {"_id": "snapshot", "nested": {"value": 1}},
            {"_id": "snapshot-2"},
        ]
        or not snapshot.closed
    ):
        message = "snapshot must be owned, stable and close after exhaustion"
        raise AssertionError(message)
    documents[0]["nested"]["value"] = 999
    stored = await engine.get_document(
        db,
        coll,
        "snapshot",
        operation_context=_context(),
    )
    if stored != {"_id": "snapshot", "nested": {"value": 1}}:
        message = "snapshot items must not expose mutable engine-owned documents"
        raise AssertionError(message)


async def _check_change_delivery(
    engine: object,
    db: str,
    coll: str,
) -> None:
    capabilities = resolve_engine_capabilities(engine)
    validate_engine_contract(engine, capabilities)
    if capabilities.change_delivery not in {
        "commit-sequence",
        "transactional-outbox",
    }:
        message = "sequenced delivery profile requires a sequenced mode"
        raise AssertionError(message)
    consumer_id = f"conformance-{uuid.uuid4().hex}"
    checkpoint = engine.register_change_consumer(
        consumer_id,
        initial_checkpoint=None,
    )
    observed = []
    try:
        for identifier in ("event-1", "event-2"):
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type="insert",
            )
            outcome = await engine.insert_document(
                db,
                coll,
                {"_id": identifier},
                overwrite=False,
                operation_context=context,
            )
            if outcome.commit_sequence is None:
                msg = "sequenced writes require commit_sequence"
                raise AssertionError(msg)
        engine.dispatch_committed_changes(consumer_id, observed.append)
        sequences = [change.sequence for change in observed]
        if len(sequences) != _EXPECTED_CHANGE_EVENTS or sequences != sorted(
            set(sequences)
        ):
            msg = "change delivery must be monotonic and complete"
            raise AssertionError(msg)
        engine.dispatch_committed_changes(consumer_id, observed.append)
        if len(observed) != _EXPECTED_CHANGE_EVENTS:
            msg = "checkpointed delivery must not replay acknowledged events"
            raise AssertionError(msg)
        if checkpoint >= sequences[0]:
            msg = "consumer checkpoint must precede delivered events"
            raise AssertionError(msg)
    finally:
        engine.unregister_change_consumer(consumer_id)


async def _check_change_delivery_recovery(
    engine: object,
    db: str,
    coll: str,
) -> None:
    consumer_id = f"conformance-recovery-{uuid.uuid4().hex}"
    observer_id = f"conformance-isolation-{uuid.uuid4().hex}"
    engine.register_change_consumer(consumer_id, initial_checkpoint=None)
    engine.register_change_consumer(observer_id, initial_checkpoint=None)
    try:
        outcomes: list[InsertOutcome] = []
        for identifier in ("event-acknowledged", "event-retry"):
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type="insert",
            )
            outcome = await engine.insert_document(
                db,
                coll,
                {"_id": identifier, "nested": {"value": 1}},
                overwrite=False,
                operation_context=context,
            )
            if (
                not isinstance(outcome, InsertOutcome)
                or outcome.commit_sequence is None
            ):
                msg = "sequenced writes require commit_sequence"
                raise AssertionError(msg)
            outcomes.append(outcome)

        rejected_sequences: list[int] = []

        class _DeliveryRejectedError(RuntimeError):
            pass

        def reject(change: CommittedChange) -> None:
            rejected_sequences.append(change.sequence)
            if change.sequence == outcomes[0].commit_sequence:
                if change.payload is not None:
                    change.payload["mutatedByConsumer"] = True
                return
            raise _DeliveryRejectedError

        try:
            engine.dispatch_committed_changes(consumer_id, reject)
        except _DeliveryRejectedError:
            pass
        else:
            msg = "change delivery must propagate consumer failures"
            raise AssertionError(msg)

        isolated: list[CommittedChange] = []
        engine.dispatch_committed_changes(observer_id, isolated.append)
        if any(
            change.payload is not None and "mutatedByConsumer" in change.payload
            for change in isolated
        ):
            msg = "change consumers must receive isolated payload snapshots"
            raise AssertionError(msg)

        observed: list[CommittedChange] = []
        engine.dispatch_committed_changes(consumer_id, observed.append)
        expected = [item.commit_sequence for item in outcomes]
        if (
            rejected_sequences != expected
            or [change.sequence for change in observed] != expected[1:]
        ):
            msg = "partial delivery must retry only the unacknowledged suffix"
            raise AssertionError(msg)
    finally:
        engine.unregister_change_consumer(consumer_id)
        engine.unregister_change_consumer(observer_id)


async def _check_vector_search_capabilities(
    engine: object,
    db: str,
    coll: str,
    *,
    context: OperationContext,
    similarities: frozenset[str],
) -> None:
    adapter = adapt_engine(engine)
    for similarity in sorted(similarities):
        index_name = f"by_vector_{similarity}"
        await engine.create_search_index(
            db,
            coll,
            SearchIndexDefinition(
                {
                    "fields": [
                        {
                            "type": "vector",
                            "path": "embedding",
                            "numDimensions": 2,
                            "similarity": similarity,
                        },
                    ],
                },
                name=index_name,
                index_type="vectorSearch",
            ),
        )
        specification = {
            "index": index_name,
            "path": "embedding",
            "queryVector": [1.0, 0.0],
            "numCandidates": 1,
            "limit": 1,
        }
        request = SearchRequest(
            operator="$vectorSearch",
            specification=specification,
            query=compile_search_stage("$vectorSearch", specification),
            mode=SearchExecutionMode.HITS,
            operation_context=context,
        )
        outcome = await adapter.execute_search(db, coll, request)
        if [item["_id"] for item in outcome.documents] != ["search"]:
            msg = "declared vector Search capability must preserve hit semantics"
            raise AssertionError(msg)


async def _check_search(  # noqa: PLR0912, PLR0915 - capability matrix
    engine: object,
    db: str,
    coll: str,
) -> None:
    context = _context()
    await engine.insert_document(
        db,
        coll,
        {
            "_id": "search",
            "title": "Ada",
            "kind": "note",
            "embedding": [1.0, 0.0],
        },
        overwrite=False,
        operation_context=context,
    )
    await engine.create_search_index(
        db,
        coll,
        SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "kind": {"type": "token"},
                    },
                }
            },
            name="by_text",
        ),
    )
    specification = {
        "index": "by_text",
        "text": {"query": "ada", "path": "title"},
    }
    request = SearchRequest(
        operator="$search",
        specification=specification,
        query=compile_search_stage("$search", specification),
        mode=SearchExecutionMode.HITS,
        operation_context=context,
    )
    adapter = adapt_engine(engine)
    outcome = await adapter.execute_search(db, coll, request)
    if [document["_id"] for document in outcome.documents] != ["search"]:
        message = "Search outcome did not preserve the matching document"
        raise AssertionError(message)
    capabilities = resolve_engine_capabilities(engine)
    search_capabilities = capabilities.search
    if "$vectorSearch" in search_capabilities.operators:
        await _check_vector_search_capabilities(
            engine,
            db,
            coll,
            context=context,
            similarities=search_capabilities.vector_similarities,
        )
    if search_capabilities.metadata_collectors:
        metadata_specification = {
            **specification,
            "count": {"type": "total"},
            "facet": {"path": "kind", "type": "token"},
        }
        metadata_request = SearchRequest(
            operator="$searchMeta",
            specification=metadata_specification,
            query=compile_search_stage("$searchMeta", metadata_specification),
            mode=SearchExecutionMode.METADATA,
            operation_context=context,
            runtime_operator="$search",
            runtime_specification=metadata_specification,
        )
        metadata_outcome = await adapter.execute_search(db, coll, metadata_request)
        if metadata_outcome.hits or metadata_outcome.metadata.count is None:
            msg = "Search metadata collectors must not return hits"
            raise AssertionError(msg)
        if metadata_outcome.metadata.count.value != 1:
            msg = "Search metadata count must preserve hit semantics"
            raise AssertionError(msg)
        facets = metadata_outcome.metadata.facets
        if not facets or [
            (bucket.value, bucket.count) for bucket in facets[0].buckets
        ] != [
            ("note", 1),
        ]:
            msg = "Search facet collectors must preserve bucket semantics"
            raise AssertionError(msg)
    if search_capabilities.highlight:
        highlight_specification = {
            **specification,
            "highlight": {"path": "title"},
        }
        highlight_request = SearchRequest(
            operator="$search",
            specification=highlight_specification,
            query=compile_search_stage("$search", highlight_specification),
            mode=SearchExecutionMode.HITS,
            operation_context=context,
        )
        highlight_outcome = await adapter.execute_search(db, coll, highlight_request)
        has_highlights, highlights = highlight_outcome.runtime_states[0].metadata_value(
            RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
        if not has_highlights or not isinstance(highlights, list) or not highlights:
            msg = "Search highlight must publish typed runtime metadata"
            raise AssertionError(msg)
        persisted = highlight_outcome.runtime_states[0].persistence_document()
        if persisted != {
            "_id": "search",
            "title": "Ada",
            "kind": "note",
            "embedding": [1.0, 0.0],
        }:
            msg = "Search runtime metadata must not cross persistence boundaries"
            raise AssertionError(msg)
    if search_capabilities.explain_verbosity:
        planner = await adapter.explain_search(
            db,
            coll,
            request,
            SearchExplainVerbosity.QUERY_PLANNER,
        )
        execution = await adapter.explain_search(
            db,
            coll,
            request,
            SearchExplainVerbosity.EXECUTION_STATS,
        )
        if planner.details.get("executionStats") is not None:
            msg = "queryPlanner must not execute Search"
            raise AssertionError(msg)
        if execution.details.get("executionStats") is None:
            msg = "executionStats must expose Search runtime evidence"
            raise AssertionError(msg)
        execution_stats = execution.details["executionStats"]
        if (
            not isinstance(execution_stats, dict)
            or execution_stats.get("matchedCount") != 1
        ):
            msg = "executionStats must expose the observed Search match count"
            raise AssertionError(msg)
