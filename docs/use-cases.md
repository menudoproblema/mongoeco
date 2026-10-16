# Choose a Mongoeco workflow

## 1. Test a Python application that uses MongoDB

Start here to replace database preparation in a test fixture while preserving
application operations and assertions. The pytest kit offers real PyMongo
sync/async clients and an isolated local URI, with Memory/SQLite selection.

- [Application testing](use-cases/test-runtime.md).
- [Coming from mongomock](migrating-from-mongomock.md).
- [Pytest configuration](pytest.md).
- [Native write compatibility evidence](write-parity-validation.md).

The kit is available from Mongoeco 4.10.0 with the optional `pytest` extra.
Direct-client examples are [memory_quickstart.py](../examples/memory_quickstart.py)
and [test_runtime_local.py](../examples/test_runtime_local.py).

## 2. Persist data in an embedded application

Choose SQLite when a desktop app, CLI or local service needs data on disk
without a separate database service, using the documented PyMongo-shaped API.

- [Embedded application guide](use-cases/embedded-app.md).
- [sqlite_embedded_app.py](../examples/sqlite_embedded_app.py).

## 3. Search and retrieve locally

Choose the local `$search` and `$vectorSearch` subsets for embedded retrieval,
with explicit index mappings and planning diagnostics.

- [Search and retrieval guide](use-cases/local-search-and-retrieval.md).
- [search_and_vector_local.py](../examples/search_and_vector_local.py).
- [vector_search_diagnostics.py](../examples/vector_search_diagnostics.py).

Keep MongoDB tests for deployment-dependent behavior and operations outside
the verified subset. The [compatibility guide](../COMPATIBILITY.md) and
[comparisons](comparisons.md) describe that boundary. The
[documentation index](README.md) links advanced runtime, architecture and CXP.
