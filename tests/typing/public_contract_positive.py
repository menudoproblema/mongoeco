from __future__ import annotations

from typing import assert_type

from mongoeco import AsyncMongoClient, MongoClient, ObjectId, ObjectIdLike
from mongoeco.compat import (
    DeprecationEntry,
    DeprecationStatus,
    compare_public_api_manifests,
    deprecation_catalog,
    deprecation_entries,
    public_api_manifest,
)
from mongoeco.conformance import (
    ConformanceCliExit,
    ConformanceReport,
    ControlledSnapshotSource,
    EngineConformanceProvider,
    operation_context_factory,
    outcome_factory,
    run_engine_conformance,
    search_request_factory,
    snapshot_factory,
)
from mongoeco.engines import (
    DeleteOutcome,
    EngineCapabilities,
    InsertOutcome,
    MutationOutcome,
    OperationContext,
    ReadSnapshot,
    SearchExecutionMetric,
    SearchExecutionOutcome,
    SearchExecutionTrace,
    SearchMetricDomain,
    SearchMetricName,
    SearchRequest,
    SnapshotMetadata,
)

from tests.consumer.engine_canary import ExternalCanaryEngine


sync_client = MongoClient()
async_client = AsyncMongoClient()
assert_type(sync_client, MongoClient)
assert_type(async_client, AsyncMongoClient)


class CompatibleObjectId:
    binary = bytes.fromhex("507f1f77bcf86cd799439011")


object_id_like: ObjectIdLike = CompatibleObjectId()
assert_type(ObjectId(object_id_like), ObjectId)

deprecations = deprecation_entries()
assert_type(deprecations, tuple[DeprecationEntry, ...])
assert_type(deprecations[0].status, DeprecationStatus)
assert_type(deprecation_catalog(), dict[str, object])
manifest = public_api_manifest()
assert_type(manifest, dict[str, object])
assert_type(
    compare_public_api_manifests(manifest, manifest), tuple[dict[str, object], ...]
)
cli_exit: ConformanceCliExit = ConformanceCliExit.SUCCESS

capabilities = EngineCapabilities(batch_inserts=False)
assert_type(capabilities, EngineCapabilities)
assert_type(ExternalCanaryEngine.capabilities, EngineCapabilities)

context = operation_context_factory()
assert_type(context, OperationContext)

outcomes = outcome_factory()
assert_type(outcomes.inserted, InsertOutcome)
assert_type(outcomes.matched, MutationOutcome)
assert_type(outcomes.deleted, DeleteOutcome)

snapshot, source = snapshot_factory(({"_id": "typed"},))
assert_type(snapshot, ReadSnapshot)
assert_type(source, ControlledSnapshotSource | None)
assert_type(snapshot.metadata, SnapshotMetadata)

request = search_request_factory()
assert_type(request, SearchRequest)

metric = SearchExecutionMetric(
    name=SearchMetricName.QUERY_MATCHED_COUNT,
    domain=SearchMetricDomain.QUERY,
    value=1,
)
assert_type(metric, SearchExecutionMetric)
trace = SearchExecutionTrace(
    backend="typed",
    operation_id="typed-operation",
    snapshot_captured=True,
    metrics=(metric,),
)
assert_type(trace, SearchExecutionTrace)
outcome = SearchExecutionOutcome(trace=trace)
assert_type(outcome, SearchExecutionOutcome)

provider = EngineConformanceProvider("typed", ExternalCanaryEngine)


async def validate_runner_return() -> None:
    report = await run_engine_conformance(provider)
    assert_type(report, ConformanceReport)
