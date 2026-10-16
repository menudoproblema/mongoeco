from __future__ import annotations

from collections.abc import Callable, Iterable  # noqa: TC003 - mypy contract annotation
from typing import assert_type

from mongoeco import AsyncMongoClient, MongoClient, ObjectId, ObjectIdLike
from mongoeco.api import AsyncCollection, Collection  # noqa: TC001 - public method signature contract
from mongoeco.compat import (
    MONGODB_DIALECT_90,
    PYMONGO_PROFILE_418,
    PYMONGO_PROFILE_419,
    DeprecationEntry,
    DeprecationStatus,
    MongoDialect90,
    PyMongoProfile418,
    PyMongoProfile419,
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
assert_type(MONGODB_DIALECT_90, MongoDialect90)
assert_type(PYMONGO_PROFILE_418, PyMongoProfile418)
assert_type(PYMONGO_PROFILE_419, PyMongoProfile419)


def check_srv_validator_signature() -> None:
    def validator(host: str) -> bool:
        return host.endswith(".internal")

    callback: Callable[[str], bool] = validator
    MongoClient(pymongo_profile=PYMONGO_PROFILE_419, srv_host_validator=callback)
    AsyncMongoClient(pymongo_profile=PyMongoProfile419(), srv_host_validator=callback)


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


async def validate_insert_many_ordered_options(
    sync_collection: Collection,
    async_collection: AsyncCollection,
) -> None:
    sync_result = sync_collection.insert_many([{"_id": "sync"}], ordered=False)
    async_result = await async_collection.insert_many([{"_id": "async"}], ordered=False)
    bool(sync_result.acknowledged)
    bool(async_result.acknowledged)


async def validate_projection_comments_and_iterables(
    sync_collection: Collection,
    async_collection: AsyncCollection,
    documents: Iterable[dict[str, object]],
) -> None:
    sync_collection.insert_one({"name": "Ada"}, comment={"request": "sync"})
    await async_collection.insert_one({"name": "Ada"}, comment="async")
    sync_collection.insert_many(documents, comment="iterable")
    await async_collection.insert_many(documents, comment="iterable")
    sync_collection.insert_many(({"name": "Tuple"},), comment=1)
    await async_collection.insert_many(({"name": "Tuple"},), comment=1)
    sync_collection.insert_many({"name": str(n)} for n in range(2))
    await async_collection.insert_many({"name": str(n)} for n in range(2))
    sync_collection.find({}, ["name"])
    async_collection.find({}, ["name"])
    sync_collection.find_one({}, ("name",))
    await async_collection.find_one({}, ("name",))
    sync_collection.find_raw_batches({}, ["name"])
    await async_collection.find_raw_batches({}, ["name"])
    sync_collection.find_one_and_update({}, {"$set": {"n": 1}}, projection=["name"])
    await async_collection.find_one_and_update(
        {}, {"$set": {"n": 1}}, projection=["name"]
    )
    sync_collection.find_one_and_replace({}, {"name": "Ada"}, projection=["name"])
    await async_collection.find_one_and_replace(
        {}, {"name": "Ada"}, projection=["name"]
    )
    sync_collection.find_one_and_delete({}, projection={"name"})
    await async_collection.find_one_and_delete({}, projection={"name"})


async def validate_upsert_presence(
    sync_collection: Collection,
    async_collection: AsyncCollection,
) -> None:
    result = sync_collection.update_one({"_id": None}, {"$set": {"n": 1}}, upsert=True)
    assert_type(result.did_upsert, bool)
    result = await async_collection.update_many(
        {"_id": None}, {"$set": {"n": 1}}, upsert=True
    )
    assert_type(result.did_upsert, bool)
