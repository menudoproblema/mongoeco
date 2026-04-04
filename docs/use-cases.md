# Use Cases

## When `mongoeco` fits well

`mongoeco` is strongest when you need MongoDB-like semantics locally without
running a real server for every workflow.

Good fits:

* local development where the code already speaks PyMongo-style collections;
* test suites that need more semantic fidelity than a lightweight mock;
* embedded applications that want persistence without a separate database
  process;
* compatibility tooling, demos and contract tests that must exercise query,
  aggregation, `$text`, `$search` or `$vectorSearch` locally;
* offline or single-node workflows where local explainability matters more
  than distributed scale.

## Typical usage patterns

### 1. In-memory development and tests

Use `MemoryEngine` when you want:

* fast local setup;
* semantic baseline behavior;
* deterministic tests without filesystem state.

Reference:

* [examples/memory_quickstart.py](/Users/uve/Proyectos/mongoeco2/examples/memory_quickstart.py)

### 2. Embedded persistence with SQLite

Use `SQLiteEngine` when you want:

* local persistence on disk;
* explainable pushdown and local indexing;
* stronger search and vector execution through FTS5 and `usearch`.

Reference:

* [examples/sqlite_embedded_app.py](/Users/uve/Proyectos/mongoeco2/examples/sqlite_embedded_app.py)

### 3. Local search and vector workflows

Use the local search surface when you want:

* Atlas-like query shapes in local development;
* explicit, honest fallbacks instead of pretending full Atlas Search parity;
* embedded `$search` and `$vectorSearch` with visible explain metadata.

Current local `$search` subset includes:

* `text`
* `phrase`
* `autocomplete`
* `wildcard`
* `exists`
* `in`
* `equals`
* `range`
* `near`
* `compound`

Reference:

* [examples/search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
* [examples/vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)

## When `mongoeco` is not the right tool

Avoid `mongoeco` when you need:

* a production MongoDB cluster replacement;
* Atlas Search parity or service-level scaling guarantees;
* distributed transactions, distributed change streams or distributed
  topology behavior;
* geodesic spatial behavior instead of the documented local planar subset.

For the exact compatibility envelope, see:

* [COMPATIBILITY.md](/Users/uve/Proyectos/mongoeco2/COMPATIBILITY.md)
