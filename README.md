# mongoeco

`mongoeco` is an async-first MongoDB-like persistence library with pluggable
storage engines.

It is built for local development, test environments, embedded persistence and
compatibility work where a PyMongo-shaped API is useful without requiring a
real MongoDB server for every workflow.

## Current Scope

What is already in place:

* async and sync client APIs
* memory and SQLite engines
* transactional local sessions and local admin/runtime introspection
* aggregation runtime with pushdown and spill guardrails
* compatibility modeling across MongoDB dialects and PyMongo profiles
* local wire/driver runtime
* local geospatial, classic `$text`, `$search` and ANN-backed `$vectorSearch`

What this is not:

* a drop-in replacement for a production MongoDB cluster
* a full Atlas Search implementation
* a geodesic geospatial engine
* a full-text/vector engine with server-grade scaling guarantees

## When To Use `mongoeco`

`mongoeco` fits well when you want:

* a local PyMongo-like runtime for development or CI;
* more semantic fidelity than a lightweight mock;
* embedded persistence with either memory or SQLite;
* local `$text`, `$search` and `$vectorSearch` without standing up a server;
* explicit compatibility and `explain()` diagnostics instead of opaque best
  effort behavior.

Reference:

* [docs/use-cases.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases.md)
* [docs/comparisons.md](/Users/uve/Proyectos/mongoeco2/docs/comparisons.md)

## Installation

Editable local install:

```bash
python -m pip install -e .
```

Development install:

```bash
python -m pip install -e .[dev]
```

The base package now includes `pyuca` as a runtime dependency so Unicode
collations keep a deterministic UCA-backed behavior even when `PyICU` is not
installed.

Advanced ICU collation backend (optional):

```bash
python -m pip install PyICU
```

Collation backend policy:

* `PyICU` stays optional by contract: it is never required for the supported
  baseline subset
* `PyICU` if available: preferred backend, including advanced collation knobs
  such as `backwards`, `alternate`, `maxVariable` and `normalization`
* `pyuca` fallback: Unicode collation for the supported basic subset
  (`locale=en`, `strength`, `caseLevel`, `numericOrdering`)
* if advanced knobs are requested without `PyICU`, `mongoeco` raises an error
  instead of silently ignoring them

Optional fast JSON backend:

```bash
python -m pip install -e .[json-fast]
```

`mongoeco` uses the standard library `json` module by default, even if
`orjson` is installed. You can choose the backend at process start with
`MONGOECO_JSON_BACKEND`:

* `stdlib`: always use the standard library JSON backend
* `orjson`: require `orjson` and use it
* `auto`: use `orjson` when available, otherwise fall back to `stdlib`

Example:

```bash
MONGOECO_JSON_BACKEND=orjson python your_app.py
```

Unicode collation backend:

* `mongoeco` prefers `PyICU` when it is available
* otherwise it uses the bundled `pyuca` dependency
* the `simple` collation keeps using the BSON/Python baseline comparator and
  rejects Unicode tailoring knobs such as `caseLevel` or `numericOrdering`
* the currently supported locale surface is `simple` and `en`
* the currently supported strengths are `1`, `2` and `3`
* `numericOrdering` and `caseLevel` are supported for `locale=en`
* `PyICU` and `pyuca` are intentionally close, but may still differ on
  advanced tailoring details outside the currently supported locale surface
* local change streams retain a bounded in-memory history; the retention size
  can be tuned with `change_stream_history_size` on async/sync clients and on
  direct async database/collection constructors
* local change streams can also persist that retained history to a journal file
  with `change_stream_journal_path`, allowing `resume_after` / `start_after`
  to survive client recreation inside the same local environment
* the journal can also be hardened with `change_stream_journal_fsync=True`
  and bounded by size with `change_stream_journal_max_bytes`
* when journaling is enabled, `mongoeco` keeps an incremental event log and
  compacts it back into a retained snapshot as the local history rolls forward;
  each log entry carries an integrity checksum and truncated tail writes are
  ignored on reload
* clients, databases and direct collections expose `change_stream_state()` so
  local retained history, journal files and compaction progress can be
  inspected at runtime
* clients, databases and direct collections also expose
  `change_stream_backend_info()`, which makes the contract explicit:
  change streams are local, optionally persistent via journal, resumable inside
  that local environment, and not distributed across nodes
* the local driver now starts non-direct single-seed topologies as
  provisional `UNKNOWN` and relies on `hello` discovery to converge towards
  `standalone`, `replicaSet` or `sharded` topology shapes
* retryable reads and writes now apply to real wire connection failures too:
  connect/read/write socket errors are normalized to `ConnectionFailure`
* replica-set discovery also tracks per-server health states (`healthy`,
  `recovering`, `degraded`, `unreachable`) and uses them to prefer healthier
  candidates when ordering eligible servers
* clients expose `sdam_capabilities()` so the supported SDAM subset is
  inspectable at runtime instead of being implicit in the implementation
* `mongoeco.collation_backend_info()` reports the active Unicode backend,
  while `mongoeco.collation_capabilities_info()` reports the supported locale
  surface and which advanced knobs require `PyICU`

## Quick Start

Async with the in-memory engine:

```python
import asyncio

from mongoeco import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine


async def main() -> None:
    async with AsyncMongoClient(MemoryEngine()) as client:
        collection = client.demo.users
        await collection.insert_one({"_id": "1", "name": "Ada"})
        document = await collection.find_one({"name": "Ada"})
        print(document)


asyncio.run(main())
```

Sync with SQLite:

```python
from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine


with MongoClient(SQLiteEngine("mongoeco.db")) as client:
    collection = client.demo.users
    collection.insert_one({"_id": "1", "name": "Ada"})
    print(collection.find_one({"_id": "1"}))
```

## Examples

Executable examples live under [examples/README.md](/Users/uve/Proyectos/mongoeco2/examples/README.md):

* [memory_quickstart.py](/Users/uve/Proyectos/mongoeco2/examples/memory_quickstart.py)
* [sqlite_embedded_app.py](/Users/uve/Proyectos/mongoeco2/examples/sqlite_embedded_app.py)
* [search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
* [vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)

The local `$search` subset now includes:

* `text`
* `phrase`
* `autocomplete`
* `wildcard`
* `regex`
* `exists`
* `in`
* `equals`
* `range`
* `near`
* `compound`

Examples worth showing first:

* [search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
  demonstrates a local `compound` query with title/body `phrase` + `in` +
  `range` + `exists` + `regex`.
* [vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)
  demonstrates how to read `similarity`, `numCandidates`, `minScore`,
  projected `vectorSearchScore`, residual filtering and exact fallback in local
  `$vectorSearch`.

## Compatibility

`mongoeco` models two separate axes:

* MongoDB server semantics through `mongodb_dialect`
* PyMongo surface compatibility through `pymongo_profile`

Planning mode is a third, separate concern:

* `STRICT` fails fast when a query, update or aggregation shape is not
  executable under the current runtime
* `RELAXED` preserves the request metadata and reports `planning_issues`
  instead of compiling an executable plan for unsupported shapes

See:

* [COMPATIBILITY.md](COMPATIBILITY.md)
* [DIALECTS.md](DIALECTS.md)
* [docs/architecture/index.md](docs/architecture/index.md)
* [docs/comparisons.md](docs/comparisons.md)

## Testing

The repository currently uses the standard library test runner:

```bash
python -m pip install -e .[dev,wire]
python -m unittest discover -s tests -p 'test*.py'
```

Contract-testing rule for new features:

* every new public feature should land with async/sync parity coverage when
  both surfaces expose it
* engine-visible behavior should also add cross-engine parity coverage for
  `MemoryEngine` and `SQLiteEngine` whenever the contract is meant to be shared
* regressions caused by facade reconstruction (`with_options()`, `database`,
  `get_collection()`, `rename()`) should be fixed with explicit tests for the
  inherited runtime options involved, not only with the implementation change
* feature work that changes public errors or degraded planning behavior should
  pin the relevant user-facing message or error shape in tests

Architecture reference:

* [docs/architecture/index.md](docs/architecture/index.md)

## Benchmarks

There is a benchmark harness under
[benchmarks/README.md](benchmarks/README.md) intended for reproducible local
profiling, regression tracking and community-facing performance analysis.

Quick smoke run:

```bash
python -m benchmarks.run \
  --engine all \
  --size 1000 \
  --warmup 0 \
  --repetitions 1
```

The harness currently covers:

* reads and point lookups;
* sort/limit and cursor materialization;
* mostly-streamable vs materializing aggregation;
* targeted local `search` and `vectorSearch` diagnostics.

Current rule of thumb from local diagnostics:

* `MemoryEngine` remains strongest on many Python-baseline filter paths;
* `SQLiteEngine` is strongest when it can push work to SQL, FTS5 or `usearch`;
* `wildcard`, `exists`, `in`, `equals`, `range` and some `compound` search shapes in
  SQLite now use a mix of materialized candidate prefilters and exact Python
  matching, depending on the operator/backend path;
* `vectorSearch` on SQLite is already materially faster than the exact
  baseline when the ANN backend is materialized.
* the public vector diagnostics also expose `similarity`, effective
  `numCandidates`, candidate evaluation counts and exact fallback reasons in
  benchmark metadata, so benchmark discussions can stay concrete instead of
  anecdotal.

For anything you plan to cite publicly, use the reproducible commands in
[benchmarks/README.md](benchmarks/README.md) instead of copying ad hoc local
numbers into docs.

## Project Status

The repository is in active development and the public package surface is still
best treated as pre-release.

Release-readiness checklist:

* [docs/release-checklist.md](docs/release-checklist.md)

## License

This project is licensed under the Apache License 2.0. See
[LICENSE](LICENSE).
