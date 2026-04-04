# Use Cases

`mongoeco` is easiest to adopt when you start from a concrete local-first
workflow rather than from the full compatibility matrix.

Use one of these three entry points:

## 1. Embedded App

Choose this when you want persistence on disk, a PyMongo-shaped API and a
single-process local runtime.

* Guide: [docs/use-cases/embedded-app.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases/embedded-app.md)
* Example: [examples/sqlite_embedded_app.py](/Users/uve/Proyectos/mongoeco2/examples/sqlite_embedded_app.py)

## 2. Test Runtime

Choose this when you want a local contract runtime that is more faithful than a
lightweight mock and can run against both memory and SQLite.

* Guide: [docs/use-cases/test-runtime.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases/test-runtime.md)
* Examples:
  * [examples/memory_quickstart.py](/Users/uve/Proyectos/mongoeco2/examples/memory_quickstart.py)
  * [examples/test_runtime_local.py](/Users/uve/Proyectos/mongoeco2/examples/test_runtime_local.py)

## 3. Local Search And Retrieval

Choose this when you want local `$search` and `$vectorSearch` with explicit
diagnostics, not a hidden Atlas-like promise.

* Guide: [docs/use-cases/local-search-and-retrieval.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases/local-search-and-retrieval.md)
* Examples:
  * [examples/search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
  * [examples/vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)

## What `mongoeco` is not

`mongoeco` is not the right fit when you need:

* a production MongoDB cluster replacement;
* Atlas Search parity or remote service semantics;
* distributed topology behavior;
* server-grade operational guarantees outside the documented subset.

For the exact compatibility envelope, see [COMPATIBILITY.md](/Users/uve/Proyectos/mongoeco2/COMPATIBILITY.md).
