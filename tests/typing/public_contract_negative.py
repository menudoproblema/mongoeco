# ruff: noqa
from __future__ import annotations

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat import (
    DeprecationStatus,
    compare_public_api_manifests,
    deprecation_catalog,
)
from mongoeco.conformance import (
    ConformanceCliExit,
    ControlledSnapshotSource,
    operation_context_factory,
    snapshot_factory,
)
from mongoeco.engines import (
    EngineCapabilities,
    InsertOutcome,
    OperationContext,
    SearchExecutionMetric,
    SearchExecutionOutcome,
    SearchMetricDomain,
    SearchMetricName,
)


sync_client: MongoClient = AsyncMongoClient()  # typing-error: assignment
async_client: AsyncMongoClient = MongoClient()  # typing-error: assignment
capabilities = EngineCapabilities(batch_inserts="yes")  # typing-error: arg-type
context: OperationContext = None  # typing-error: assignment
inserted = InsertOutcome(applied="yes")  # typing-error: arg-type
wrong_context: int = operation_context_factory()  # typing-error: assignment
snapshot, source = snapshot_factory(({"_id": "typed"},))
required_source: ControlledSnapshotSource = source  # typing-error: assignment
EngineCapabilities(unknown=True)  # typing-error: call-arg
SearchExecutionMetric(
    name=SearchMetricName.QUERY_MATCHED_COUNT,
    domain=SearchMetricDomain.QUERY,
    value="one",  # typing-error: arg-type
)
SearchExecutionOutcome(hits=[])  # typing-error: arg-type
status: DeprecationStatus = "deprecated"  # typing-error: assignment
catalog_items: list[object] = deprecation_catalog()  # typing-error: assignment
compare_public_api_manifests([], {})  # typing-error: arg-type
cli_exit: ConformanceCliExit = 0  # typing-error: assignment
