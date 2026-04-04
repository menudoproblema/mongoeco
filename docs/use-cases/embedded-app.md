# Embedded App

## When To Use It

Use `mongoeco` as an embedded app runtime when you want:

* local persistence on disk without a separate database process;
* a PyMongo-shaped collection API inside a desktop app, CLI or single-node
  service;
* explicit local `explain()` diagnostics instead of opaque hidden behavior;
* search or aggregation features that stay inside the same process.

`SQLiteEngine` is the default fit for this package of use cases.

## When Not To Use It

Do not use this shape when you need:

* a distributed MongoDB deployment;
* Atlas Search parity or remote operational guarantees;
* multi-node failover, scaling or topology management;
* server-grade durability promises outside the documented local subset.

## Minimal Recipe

```python
from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine


with MongoClient(SQLiteEngine("app.db")) as client:
    collection = client.embedded.todos
    collection.insert_one({"_id": 1, "kind": "task", "title": "Ship docs", "done": False})
    print(collection.find({"done": False}).sort([("title", 1)]).to_list())
```

## Example

* [examples/sqlite_embedded_app.py](/Users/uve/Proyectos/mongoeco2/examples/sqlite_embedded_app.py)

That example shows:

* collection bootstrap;
* local persistence with SQLite;
* ordinary CRUD plus `collStats`.

## Limits To Keep In Mind

* `mongoeco` stays local-first and embedded by design.
* Search and aggregation are strong local features, not a promise of Atlas or
  cluster parity.
* SQLite pushdown is deliberate and visible in `explain()`, but not every query
  shape can be pushed down exactly.
