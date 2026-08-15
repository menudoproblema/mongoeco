from __future__ import annotations

import asyncio
import uuid

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.conformance.models import (
    DEFAULT_SPI_V2_PROFILES,
    ConformanceCheckResult,
    ConformanceProfile,
    ConformanceReport,
)
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import ChangePublicationPolicy, OperationContext
from mongoeco.core.paths import get_document_value
from mongoeco.core.search import SEARCH_HIGHLIGHTS_METADATA_FIELD, compile_search_stage
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
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.types import SearchIndexDefinition


if TYPE_CHECKING:
    from collections.abc import Iterable

    from mongoeco.conformance.provider import EngineConformanceProvider


_SPI_V2 = 2
_EXPECTED_CHANGE_EVENTS = 2


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
        for profile, name, check, applicable in _checks(capabilities):
            if profile not in selected or not applicable:
                continue
            db_name, coll_name = provider.namespace(name)
            failure: Exception | None = None
            try:
                await check(engine, db_name, coll_name)
            except Exception as error:
                failure = error
                checks.append(
                    ConformanceCheckResult(
                        profile=profile,
                        name=name,
                        passed=False,
                        detail=f"{type(error).__name__}: {error}",
                    )
                )
            try:
                await provider.cleanup_namespace(engine, db_name)
            except Exception as error:
                if failure is None:
                    checks.append(
                        ConformanceCheckResult(
                            profile=profile,
                            name=name,
                            passed=False,
                            detail=f"cleanup {type(error).__name__}: {error}",
                        )
                    )
                else:
                    checks.append(
                        ConformanceCheckResult(
                            profile=profile,
                            name=f"{name}:cleanup",
                            passed=False,
                            detail=f"{type(error).__name__}: {error}",
                        )
                    )
                continue
            if failure is None:
                checks.append(
                    ConformanceCheckResult(
                        profile=profile,
                        name=name,
                        passed=True,
                    )
                )
    return ConformanceReport(
        provider_name=provider.name,
        contract_version="spi-v2",
        checks=tuple(checks),
    )


def _checks(capabilities):
    return (
        (
            ConformanceProfile.SPI_V2_CORE,
            "capabilities",
            _check_capabilities,
            True,
        ),
        (
            ConformanceProfile.SPI_V2_CORE,
            "crud-outcomes",
            _check_crud_outcomes,
            True,
        ),
        (
            ConformanceProfile.SPI_V2_CORE,
            "operation-context",
            _check_operation_context,
            True,
        ),
        (
            ConformanceProfile.SPI_V2_ATOMICITY,
            "compare-and-set",
            _check_atomic_mutation,
            True,
        ),
        (
            ConformanceProfile.SPI_V2_CLOCK,
            "injected-clock",
            _check_injected_clock,
            capabilities.injected_clock,
        ),
        (
            ConformanceProfile.SPI_V2_SNAPSHOTS,
            "stable-snapshot",
            _check_snapshot,
            capabilities.explicit_read_snapshots,
        ),
        (
            ConformanceProfile.SPI_V2_CHANGE_DELIVERY,
            "change-delivery-contract",
            _check_change_delivery,
            capabilities.monotonic_commit_sequence,
        ),
        (
            ConformanceProfile.SEARCH_V1,
            "search-contract",
            _check_search,
            capabilities.search is not None,
        ),
    )


async def _check_capabilities(engine: object, _db: str, _coll: str) -> None:
    capabilities = resolve_engine_capabilities(engine)
    if capabilities.spi_version != _SPI_V2:
        message = "conformance requires SPI v2"
        raise AssertionError(message)
    validate_engine_contract(engine, capabilities)


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
        {"_id": "snapshot"},
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
    snapshot = engine.open_read_snapshot(
        db,
        coll,
        compile_find_semantics({}, sort=[("_id", 1)]),
        operation_context=context,
    )
    if not isinstance(snapshot, ReadSnapshot):
        message = "open_read_snapshot must return ReadSnapshot"
        raise AssertionError(message)
    first = await snapshot.__anext__()
    await engine.insert_document(
        db,
        coll,
        {"_id": "after-snapshot"},
        overwrite=False,
        operation_context=_context(),
    )
    documents = [first, *[document async for document in snapshot]]
    if snapshot.metadata.policy is not SnapshotPolicy.STABLE:
        message = "snapshot must declare STABLE policy"
        raise AssertionError(message)
    if documents != [{"_id": "snapshot"}, {"_id": "snapshot-2"}] or not snapshot.closed:
        message = "snapshot must be owned, stable and close after exhaustion"
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


async def _check_search(engine: object, db: str, coll: str) -> None:
    context = _context()
    await engine.insert_document(
        db,
        coll,
        {"_id": "search", "title": "Ada", "kind": "note"},
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
        has_highlights, highlights = get_document_value(
            highlight_outcome.documents[0],
            SEARCH_HIGHLIGHTS_METADATA_FIELD,
        )
        if not has_highlights or not isinstance(highlights, list) or not highlights:
            msg = "Search highlight must publish sidecar metadata"
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
