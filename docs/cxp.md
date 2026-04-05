# CXP Integration

`mongoeco` treats CXP as the canonical public capability model for the
`database/mongodb` interface.

That means:

* the public compatibility story is described with CXP capability names;
* `compat` exports and `explain()` projections derive from that same model;
* `mongoeco` is ready to be wrapped by external systems that want to expose it
  as a live CXP provider.

It does **not** mean that `mongoeco` ships a built-in provider implementation
for negotiation, instance resolution or telemetry collection. Those concerns
belong to the system that wants to use `mongoeco` as a resource or provider.

## What `mongoeco` publishes

The package publishes the canonical `database/mongodb` catalog and its public
metadata through:

* `mongoeco.cxp`
* `mongoeco.compat.export_cxp_catalog()`
* the top-level `cxp` block exposed by `find(...).explain()` and
  `aggregate(...).explain()`

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

* `aggregation.supportedStages`
* `aggregation.supportedExpressionOperators`
* `aggregation.supportedGroupAccumulators`
* `aggregation.supportedWindowAccumulators`
* `search.operators`
* `search.aggregateStage`
* `vector_search.similarities`
* `vector_search.aggregateStage`

This keeps the CXP model honest:

* `aggregate` is the public operation
* stages and operators remain part of the supported subset metadata

## Reusable profiles

The canonical `database/mongodb` catalog also exposes reusable profiles that
external systems can use for tests, resources or conformance checks:

* `mongodb-core`
* `mongodb-search`
* `mongodb-platform`
* `mongodb-aggregate-rich`

Each profile expresses:

* required capabilities
* required first-level operations
* required metadata keys

`mongoeco` reexports those profiles through `mongoeco.cxp`, but does not
perform profile negotiation itself.

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

## Limits

Current limits of the `mongoeco` side of the integration:

* `mongoeco` does not ship a live CXP provider wrapper for its clients;
* it does not negotiate profiles or capabilities on behalf of external
  frameworks;
* it exposes canonical capabilities, canonical first-level operations and
  structured subset metadata, but keeps implementation-specific details out of
  the catalog.
