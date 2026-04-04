# Test Runtime

## When To Use It

Use `mongoeco` as a test runtime when you want:

* more semantic fidelity than a lightweight fake;
* the same PyMongo-shaped code paths in local tests and contracts;
* a fast in-memory baseline with `MemoryEngine`;
* a second embedded backend with `SQLiteEngine` when persistence or pushdown
  matters.

## When Not To Use It

Avoid this package when your tests specifically require:

* a real MongoDB server process;
* server-managed replication or sharding behavior;
* Atlas-only capabilities;
* driver integration against an external deployment.

## Minimal Recipe

```python
from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine


with MongoClient(MemoryEngine()) as client:
    collection = client.demo.runtime
    collection.insert_many(
        [
            {"_id": 1, "kind": "note", "body": "Ada wrote the first algorithm"},
            {"_id": 2, "kind": "note", "body": "Ada wrote the practical first algorithm"},
        ]
    )
    print(collection.find({"kind": "note"}).sort([("_id", 1)]).to_list())
```

## Example

* [examples/test_runtime_local.py](/Users/uve/Proyectos/mongoeco2/examples/test_runtime_local.py)

That example shows:

* the same collection contract against `MemoryEngine` and `SQLiteEngine`;
* a small `$search.phrase` assertion with `slop`;
* consistent local `explain()` output across both engines.

## Limits To Keep In Mind

* This is a local semantic runtime, not a hidden integration test harness for a
  real cluster.
* Differences between `MemoryEngine` and `SQLiteEngine` are documented and
  intentional; choose the engine that matches the test you actually need.
