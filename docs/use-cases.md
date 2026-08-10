# Use Cases

`mongoeco` is easiest to adopt when you start from a concrete local-first
workflow rather than from the full compatibility matrix.

Use one of these three entry points:

## 1. Embedded App

Choose this when you want persistence on disk, a PyMongo-shaped API and a
single-process local runtime.

* Guide: [embedded-app.md](use-cases/embedded-app.md)
* Example: [sqlite_embedded_app.py](../examples/sqlite_embedded_app.py)

## 2. Test Runtime

Choose this when you want a local contract runtime that is more faithful than a
lightweight mock and can run against both memory and SQLite.

* Guide: [test-runtime.md](use-cases/test-runtime.md)
* Examples:
  * [memory_quickstart.py](../examples/memory_quickstart.py)
  * [test_runtime_local.py](../examples/test_runtime_local.py)

## 3. Local Search And Retrieval

Choose this when you want local `$search` and `$vectorSearch` with explicit
diagnostics, not a hidden Atlas-like promise.

* Guide: [local-search-and-retrieval.md](use-cases/local-search-and-retrieval.md)
* Examples:
  * [search_and_vector_local.py](../examples/search_and_vector_local.py)
  * [vector_search_diagnostics.py](../examples/vector_search_diagnostics.py)

## What `mongoeco` is not

`mongoeco` is not the right fit when you need:

* a production MongoDB cluster replacement;
* Atlas Search parity or remote service semantics;
* distributed topology behavior;
* server-grade operational guarantees outside the documented subset.

For the exact compatibility envelope, see [COMPATIBILITY.md](../COMPATIBILITY.md).
