# Examples

These examples are small, executable entry points for the public API.

Use them when you want to answer one of these questions quickly:

* "How do I start with the in-memory engine?"
* "How do I use SQLite as embedded persistence?"
* "How do I treat `mongoeco` as a local test runtime?"
* "How do I run local `$search` and `$vectorSearch` without a server?"
* "How do I inspect `mongoeco` through the canonical CXP contract?"

## Embedded app

* [memory_quickstart.py](/Users/uve/Proyectos/mongoeco2/examples/memory_quickstart.py)
  Async-first CRUD with `MemoryEngine`.
* [sqlite_embedded_app.py](/Users/uve/Proyectos/mongoeco2/examples/sqlite_embedded_app.py)
  Small embedded application with `SQLiteEngine`.

## Test runtime

* [test_runtime_local.py](/Users/uve/Proyectos/mongoeco2/examples/test_runtime_local.py)
  Run the same local contract against `MemoryEngine` and `SQLiteEngine`,
  including `$search.phrase` with `slop`.

## Local search and retrieval

* [search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
  Local `$search` and `$vectorSearch`, including exact `phrase` versus
  `phrase.slop`, numeric `near`, richer field mappings including
  `embeddedDocuments`, plus title/body and embedded-path `compound` with
  `equals` + `in` + `range` + `near` + `exists` + `regex`, and projected
  `vectorSearchScore`.
* [vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)
  Public-facing `vectorSearch` diagnostics: compare `similarity`,
  `numCandidates`, `minScore`, projected `vectorSearchScore`, residual filters
  and exact fallback.
* [cxp_adapter.py](/Users/uve/Proyectos/mongoeco2/examples/cxp_adapter.py)
  Inspect the canonical `database/mongodb` catalog and the `cxp` projection
  exposed by `aggregate(...).explain()`.

Run them from the repository root after installing the package:

```bash
python -m pip install -e .[dev]
python examples/memory_quickstart.py
python examples/sqlite_embedded_app.py
python examples/test_runtime_local.py
python examples/search_and_vector_local.py
python examples/vector_search_diagnostics.py
python examples/cxp_adapter.py
```
