# CXP Integration

`mongoeco` treats CXP as the canonical public capability model for the
`database/mongodb` interface, and reexports the related execution catalogs that
matter for consumers already working against the CXP catalog surface.

That means:

* the public compatibility story is described with CXP capability names;
* `compat` exports and `explain()` projections derive from that same model;
* `mongoeco` is ready to be wrapped by external systems that want to expose it
  as a live CXP provider.

It does **not** mean that `mongoeco` ships a built-in provider implementation
for negotiation or instance resolution. Those concerns belong to the system
that wants to use `mongoeco` as a resource or provider.

## What `mongoeco` publishes

The package publishes the canonical `database/mongodb` catalog, the aligned
execution catalogs, and its public metadata through:

* `mongoeco.cxp`
* `mongoeco.compat.export_cxp_catalog()`
* the top-level `cxp` block exposed by `find(...).explain()` and
  `aggregate(...).explain()`

Through `mongoeco.cxp`, the public catalog surface includes:

* `database/mongodb`
* the abstract family `execution/engine`
* the concrete execution catalog `execution/plan-run`

The canonical capabilities are:

* `read`
* `write`
* `aggregation`
* `transactions`
* `change_streams`
* `search`
* `vector_search`
* `collation`
* `persistence`
* `topology_discovery`

## Canonical first-level operations

The `database/mongodb` catalog also canonizes first-level public operations.
`mongoeco` aligns with that vocabulary.

Examples:

* `read` -> `find`, `find_one`, `count_documents`,
  `estimated_document_count`, `distinct`
* `write` -> `insert_one`, `insert_many`, `update_one`, `update_many`,
  `replace_one`, `delete_one`, `delete_many`, `bulk_write`
* `aggregation` -> `aggregate`
* `change_streams` -> `watch`
* `transactions` -> `start_session`, `with_transaction`

For `search` and `vector_search`, the bound public operation remains
`aggregate`, and the stage-level subset is described in metadata.

## Structured subset metadata

Some capabilities need more detail than a single operation name.

`mongoeco` therefore publishes structured metadata such as:

* `read.operationMetadata`
* `write.operationMetadata`
* `aggregation.supportedStages`
* `aggregation.supportedExpressionOperators`
* `aggregation.supportedGroupAccumulators`
* `aggregation.supportedWindowAccumulators`
* `aggregation.operationMetadata`
* `search.operators`
* `search.fieldMappings`
* `search.structuredParentPathOperators`
* `search.explainFeatures`
* `search.operationMetadata`
* `search.aggregateStage`
* `vector_search.similarities`
* `vector_search.operationMetadata`
* `vector_search.aggregateStage`

This keeps the CXP model honest:

* `aggregate` is the public operation
* stages and operators remain part of the supported subset metadata
* first-level operations keep their own option and subset metadata instead of
  pretending that every provider exposes the full MongoDB surface

In practice, the per-operation metadata is now rich enough to drive tooling
without guessing:

* `read.operationMetadata.find` carries scope, result type, session support and
  whether `filter` / `projection` are accepted;
* `write.operationMetadata.update_one` and `update_many` expose pipeline-update
  support, upsert support and the supported update operators;
* `aggregation.operationMetadata.aggregate` carries scope, result type,
  session support, accepted pipeline input and the supported stage/operator
  subset;
* `search.fieldMappings` now includes `embeddedDocuments`, making nested search
  paths part of the public subset instead of an undocumented local trick;
* `search.structuredParentPathOperators` and `search.explainFeatures` make it
  explicit that structured parent paths work across the visible textual subset
  and that `explain()` resolves them to mapped leaf paths.
* scalar search operators now expose consistent `pathSummary` metadata too,
  which makes `read`/`aggregation` tooling easier to build without operator-
  specific heuristics.
* `search.operatorSemantics` and `querySemantics` in `explain()` make the local
  textual tier explicit and machine-readable.
* `search.stageOptions` now declares the local advanced subset for
  `count`, `highlight` and `facet`, and `explain()` exposes
  `countPreview`, `highlightPreview` and `facetPreview`.

## Reusable profiles

The canonical `database/mongodb` catalog also exposes reusable profiles that
external systems can use for tests, resources or conformance checks:

* `mongodb-core`
* `mongodb-text-search`
* `mongodb-search`
* `mongodb-platform`
* `mongodb-aggregate-rich`

Each profile expresses:

* required capabilities
* required first-level operations
* required metadata keys

`mongodb-text-search` is the useful middle ground for tooling that needs
textual `$search` conformance but does not want to force `vector_search`.
`mongodb-search` remains the broader profile for providers that support both
textual and vector search through `aggregate`.

`mongoeco` now validates `mongodb-platform` as well. The platform tier is
backed by canonical metadata for:

* `collation.backend`
* `collation.capabilities`
* `persistence.persistent`
* `persistence.storageEngine`
* `topology_discovery.topologyType`
* `topology_discovery.serverCount`
* `topology_discovery.sdam`

`mongoeco.compat.export_cxp_catalog()` now also serializes each profile with:

* `requirements[].capabilityName`
* `requirements[].requiredOperations`
* `requirements[].requiredMetadataKeys`

That keeps the public export directly useful for profile-aware tooling without
forcing every consumer to import `cxp` just to inspect profile shape.

If a consumer only wants the reusable profile catalog, `mongoeco` also exposes:

* `mongoeco.export_cxp_profile_catalog()`
* `mongoeco.compat.export_cxp_profile_catalog()`

If it wants the same reusable profiles annotated with support status against
the current public runtime surface, `mongoeco` also exposes:

* `mongoeco.export_cxp_profile_support_catalog()`
* `mongoeco.compat.export_cxp_profile_support_catalog()`

If it wants a more tooling-friendly view centered on public operations instead
of capabilities, `mongoeco` also exposes:

* `mongoeco.export_cxp_operation_catalog()`
* `mongoeco.compat.export_cxp_operation_catalog()`

That makes it possible to gate tests or resources without negotiating through
`mongoeco` itself: a consumer can inspect `supported` and the validation
messages for each reusable profile and decide whether to run or skip.
The operation catalog is the complementary view for questions like "what does
`find` support?" or "under which profiles does `update_one` fit?" without
walking the full capability tree.

The same requirement shape is now reused in `explain()["cxp"]` whenever
`mongoeco` can infer a minimal reusable profile honestly, through:

* `minimalProfile`
* `minimalProfileRequirements`
* `compatibleProfiles`
* `compatibleProfileSupport`
* `operationName`
* `operationMetadata`

`mongoeco` reexports those profiles through `mongoeco.cxp`, but does not
perform profile negotiation itself.

The canonical MongoDB catalog also defines typed metadata schemas for the
capabilities that need a richer subset contract, such as:

* `MongoAggregationMetadata`
* `MongoSearchMetadata`
* `MongoVectorSearchMetadata`
* `MongoCollationMetadata`
* `MongoPersistenceMetadata`
* `MongoTopologyDiscoveryMetadata`

`mongoeco.cxp` reexports that MongoDB-facing vocabulary, together with the
aligned execution catalogs, so the public integration stays aligned with CXP
instead of carrying a parallel local model.

## Driver telemetry projection

`mongoeco` now also ships a reusable projection from driver monitor events to
the canonical `db.client.*` telemetry declared by the `database/mongodb`
catalog.

The source of truth remains the internal driver monitor:

* `ServerSelectedEvent`
* `ConnectionCheckedOutEvent`
* `ConnectionCheckedInEvent`
* `CommandStartedEvent`
* `CommandSucceededEvent`
* `CommandFailedEvent`

The public adapter lives in `mongoeco.cxp.telemetry`:

```python
from mongoeco import MongoClient
from mongoeco.cxp.telemetry import DriverTelemetryProjector


with MongoClient() as client:
    projector = DriverTelemetryProjector(provider_id="mongoeco-local")
    projector.attach(client.driver_monitor)

    client.execute_driver_command(
        "demo",
        "insert",
        {"insert": "items", "documents": [{"_id": 1}]},
        read_only=False,
    )

    snapshot = projector.snapshot()
    print(snapshot)
```

Current v1 coverage is intentionally conservative:

* `read`
* `write`
* `aggregation`
* `search`
* `vector_search`

`mongoeco` projects command attempts to the canonical signals declared by the
catalog:

* `db.client.operation*`
* `db.client.aggregate*`
* `db.client.search*`
* `db.client.vector_search*`

The driver events keep their richer internal details. The CXP side only emits
the canonical fields that can be derived reliably from the command attempt, for
example:

* `db.system.name`
* `db.operation.name`
* `db.namespace.name`
* `db.operation.outcome`
* `db.pipeline.stage.count`
* `db.pipeline.stage.name`
* `db.search.operator`
* `db.vector_search.similarity`

## Example

```python
from mongoeco import MongoClient, export_cxp_catalog
from mongoeco.engines.memory import MemoryEngine


print(export_cxp_catalog()["interface"])

with MongoClient(MemoryEngine()) as client:
    collection = client.get_database("demo").get_collection("items")
    collection.insert_one({"_id": 1, "score": 8})
    explain = collection.aggregate(
        [{"$match": {"score": {"$gte": 8}}}]
    ).explain()
    print(explain["cxp"])
```

When `mongoeco` can infer it honestly, the `cxp` block in `explain()` also
exposes the minimal reusable MongoDB profile for that capability path:

* `find(...)` -> `mongodb-core`
* `aggregate([{"$search": ...}])` -> `mongodb-text-search`
* `aggregate([{"$vectorSearch": ...}])` -> `mongodb-search`

That means a consumer can inspect one `explain()` result and recover both:

* the canonical capability path being exercised;
* the profile gate and exact profile requirements that would cover that path.

## Limits

Current limits of the `mongoeco` side of the integration:

* `mongoeco` does not ship a live CXP provider wrapper for its clients;
* it does not negotiate profiles, telemetry streams or capabilities on behalf of external
  frameworks;
* the built-in telemetry projection is limited to canonical command telemetry
  for `read`, `write`, `aggregation`, `search` and `vector_search`;
* it exposes canonical capabilities, canonical first-level operations,
  structured subset metadata, aligned catalog reexports, and a reusable driver
  telemetry projector, but keeps implementation-specific details out of the
  catalog.
