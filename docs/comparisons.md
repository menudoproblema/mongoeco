# Comparisons

## `mongoeco` vs MongoDB server

`mongoeco` is intentionally MongoDB-like, not a hidden server replacement.

What `mongoeco` gives you:

* local async/sync collection APIs with PyMongo-shaped ergonomics;
* embedded `MemoryEngine` and `SQLiteEngine`;
* explicit compatibility modeling through `mongodb_dialect` and
  `pymongo_profile`;
* local `$text`, `$search`, `$vectorSearch`, geospatial, aggregation and
  admin/runtime introspection within the documented subset.

What it does not try to give you:

* distributed topology behavior equal to a real cluster;
* remote service semantics like Atlas Search;
* cluster-grade durability, scaling or operational guarantees;
* geodesic spatial behavior.

Use MongoDB server when you need server-grade deployment and operational
behavior. Use `mongoeco` when local semantic fidelity and embedded workflows
matter more than distributed infrastructure.

## `mongoeco` vs `mongomock`

The project is no longer positioned as "another mock".

`mongoeco` is stronger when you need:

* explicit compatibility policy instead of best-effort imitation;
* local explainability and planning visibility;
* richer embedded behavior for aggregation, search, vector search and runtime
  admin surfaces;
* both in-memory and SQLite-backed execution with cross-engine parity tests.

`mongomock` can still be enough when you only want a very lightweight fake for
simple CRUD tests and you do not need the heavier local runtime surface.

## Positioning in one sentence

`mongoeco` is best understood as an embedded MongoDB-like runtime for local
development, testing, demos and single-node persistence, with explicit
contracts about what is and is not supported.
