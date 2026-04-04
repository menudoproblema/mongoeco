# Examples

These examples are small, executable entry points for the public API.

Use them when you want to answer one of these questions quickly:

* "How do I start with the in-memory engine?"
* "How do I use SQLite as embedded persistence?"
* "How do I run local `$search` and `$vectorSearch` without a server?"

Available examples:

* [memory_quickstart.py](/Users/uve/Proyectos/mongoeco2/examples/memory_quickstart.py)
  Async-first CRUD with `MemoryEngine`.
* [sqlite_embedded_app.py](/Users/uve/Proyectos/mongoeco2/examples/sqlite_embedded_app.py)
  Small embedded application with `SQLiteEngine`.
* [search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
  Local `$search` and `$vectorSearch`, including `phrase` + `in` + `range` +
  `regex` inside `compound` and a boolean `vectorSearch.filter` with residual
  explain output.
* [vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)
  Public-facing `vectorSearch` diagnostics: compare `similarity`,
  `numCandidates`, residual filters and exact fallback.

Run them from the repository root after installing the package:

```bash
python -m pip install -e .[dev]
python examples/memory_quickstart.py
python examples/sqlite_embedded_app.py
python examples/search_and_vector_local.py
python examples/vector_search_diagnostics.py
```
