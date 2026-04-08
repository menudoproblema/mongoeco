# mongoeco

`mongoeco` is an async-first embedded runtime aligned with the canonical CXP
`database/mongodb` interface, with a MongoDB-like surface and pluggable
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
* a canonical public capability model for `database/mongodb` through CXP
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

* a local `database/mongodb` runtime for development or CI;
* a local PyMongo-like runtime without a server process;
* more semantic fidelity than a lightweight mock;
* embedded persistence with either memory or SQLite;
* local `$text`, `$search` and `$vectorSearch` without standing up a server;
* explicit compatibility and `explain()` diagnostics instead of opaque best
  effort behavior.

Reference:

* [docs/use-cases.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases.md)
* [docs/use-cases/embedded-app.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases/embedded-app.md)
* [docs/use-cases/test-runtime.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases/test-runtime.md)
* [docs/use-cases/local-search-and-retrieval.md](/Users/uve/Proyectos/mongoeco2/docs/use-cases/local-search-and-retrieval.md)
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

The base install now also includes `cxp>=3.0.0`, so `mongoeco` can expose the
canonical `database/mongodb` contract directly.
Reference:

* [docs/cxp.md](/Users/uve/Proyectos/mongoeco2/docs/cxp.md)

The public compatibility export and the top-level `cxp` blocks surfaced by
`find(...).explain()` / `aggregate(...).explain()` are now projected from that
canonical CXP capability model.

The public CXP story now includes canonical first-level operation bindings as
well:

* `read` -> `find`, `find_one`, `count_documents`, `estimated_document_count`,
  `distinct`
* `write` -> `insert_one`, `insert_many`, `update_one`, `update_many`,
  `replace_one`, `delete_one`, `delete_many`, `bulk_write`
* `aggregation` -> `aggregate`
* `change_streams` -> `watch`
* `transactions` -> `start_session`, `with_transaction`

For `search` and `vector_search`, `mongoeco` still binds the public operation
`aggregate` and uses metadata to describe the supported stage-level subset.
That metadata also carries operation-level hints such as result shape, scope,
session support and core accepted inputs for `read`, `write` and
`aggregation`.

For reusable profile gates, the practical split is now:

* `mongodb-text-search` when you need textual `$search` without requiring
  `vector_search`;
* `mongodb-search` when you need both textual and vector search;
* `mongodb-platform` when you need the broader platform surface, including
  canonical metadata for collation, persistence and topology discovery.

The public compat export serializes those profiles with structured
requirements, and the `cxp` block in `explain()` now carries the minimal
reusable profile, its structured requirements and the broader compatible
profiles for the current capability path when that can be inferred honestly.

If a consumer only needs the reusable profile catalog, it can use
`mongoeco.compat.export_cxp_profile_catalog()`. If it wants the same profiles
annotated with current runtime support, it can use
`mongoeco.compat.export_cxp_profile_support_catalog()`.
That is enough to build simple test gates without coupling `mongoeco` to any
particular runner or resource system.
If it wants to consume support from the operation point of view, it can also
use `mongoeco.compat.export_cxp_operation_catalog()`.
That operation view now includes canonical telemetry requirements per
capability binding, so tooling can validate profile and observability contracts
without inferring signal names.

The root `mongoeco` package is intentionally narrower than the full runtime
internals. It centers on:

* clients and session entry points
* common BSON-facing types and operation models
* URI/configuration helpers needed by the main runtime surface

Compatibility tooling and the canonical MongoDB CXP contract surface live in
their own packages:

* `mongoeco.compat`
* `mongoeco.cxp`

Lower-level driver/runtime details remain available from subpackages such as
`mongoeco.driver`.

`mongoeco.compat` follows the same idea: the top-level package keeps the
catalog exports, profile resolution helpers and option-support surface, while
more tactical constants and raw catalog data stay in explicit submodules.

Import guidance by layer:

* use `mongoeco` for clients, sessions, BSON-facing types and URI/config helpers
* use `mongoeco.compat` for dialect/profile resolution and compat/tooling exports
* use `mongoeco.cxp` for the curated MongoDB CXP contract and reusable profiles
* use `mongoeco.cxp.catalogs.interfaces.database.mongodb` for the full
  MongoDB capability/operation vocabulary
* use `mongoeco.driver`, `mongoeco.engines` and `mongoeco.wire` only when you
  intentionally need lower-level runtime surfaces

## Public Surface Stability (3.x)

The 3.x contract is intentionally explicit:

* stable import roots are `mongoeco`, `mongoeco.compat` and `mongoeco.cxp`
* each root surface is curated through its `__all__`
* lower-level runtime symbols stay in explicit subpackages

The root package no longer exposes transport aliases. Import transport classes
from `mongoeco.driver` directly.

`mongoeco` does not ship a live CXP provider wrapper for its clients. Instead,
it exposes the canonical catalog and projects the active capability path
through `compat` and `explain()`. External systems can wrap `mongoeco` if they
want to negotiate profiles or instantiate resources through CXP.

`mongoeco.cxp` is the canonical contract source; `mongoeco.compat` keeps
projected reporting views over that same source instead of maintaining a
parallel CXP catalog model.

The direct path for CXP-facing tooling is:

```python
from mongoeco import MongoClient
from mongoeco.compat import export_cxp_catalog
from mongoeco.cxp import MONGODB_CATALOG
from mongoeco.engines.memory import MemoryEngine


print(export_cxp_catalog()["interface"])

with MongoClient(MemoryEngine()) as client:
    collection = client.get_database("demo").get_collection("items")
    collection.insert_one({"_id": 1, "score": 8})
    explain = collection.aggregate([{"$match": {"score": {"$gte": 8}}}]).explain()
    print(MONGODB_CATALOG.interface)
    print(explain["cxp"])
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

Runtime behavior highlights:

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
* [test_runtime_local.py](/Users/uve/Proyectos/mongoeco2/examples/test_runtime_local.py)
* [search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
* [vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)
* [cxp_adapter.py](/Users/uve/Proyectos/mongoeco2/examples/cxp_adapter.py)

The local `$search` subset now includes:

* `text`
* `phrase` with optional `slop`
* `autocomplete`
* `wildcard`
* `regex`
* `exists`
* `in`
* `equals`
* `range`
* `near`
* `compound`
* this local textual tier is now considered closed in its documented subset;
  what remains is advanced Atlas-like behavior, not the daily embedded
  perimeter.
* the advanced local subset now also exposes:
  * `autocomplete.tokenOrder` with `any` / `sequential`
  * `regex.flags` with `i` / `m` / `s` / `x`
  * `wildcard.allowAnalyzedField`
  * `$search.count`, `$search.highlight` and `$search.facet` as local
    stage options, with `highlight` visible in result documents through
    `searchHighlights` and `count` / `facet` / `highlight` previews in
    `explain()`

Examples worth showing first:

* [test_runtime_local.py](/Users/uve/Proyectos/mongoeco2/examples/test_runtime_local.py)
  demonstrates `MemoryEngine` and `SQLiteEngine` as local contract runtimes
  with the same `$search.phrase` behavior.
* [search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
  demonstrates exact `phrase` versus `phrase.slop`, numeric `near`, richer
  local search field mappings including explicit `document` and
  `embeddedDocuments` paths, parent-path resolution for `metadata` and
  `contributors`, numeric/date `near`, parent-path `exists` over structured
  mappings, parent-path `autocomplete` / `wildcard` / `regex` over structured
  mappings with resolved descendant leaf paths in `explain()`, explicit
  operator `querySemantics`, consistent
  scalar `pathSummary` metadata for `equals` / `range` / `near`, scalar filters
  over embedded-document paths, `searchHighlights` in local result documents,
  `count` / `facet` previews in `explain()`, and a local
  `compound` query with visible embedded path/ranking explain metadata plus
  `equals` + `in` + `range` + `near` + `exists` + `regex`.
* [vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)
  compares `MemoryEngine` and `SQLiteEngine` for local hybrid retrieval,
  including `scoreBreakdown`, `candidatePlan`, `hybridRetrieval`,
  `pruningSummary`, `prefilterSources`, projected `vectorSearchScore`,
  residual filtering and exact fallback in local `$vectorSearch`.
* [cxp_adapter.py](/Users/uve/Proyectos/mongoeco2/examples/cxp_adapter.py)
  demonstrates the canonical CXP `database/mongodb` catalog and the `cxp`
  projection exposed by `aggregate(...).explain()`.

## Compatibility

`mongoeco` now exposes three public contract layers:

* the canonical CXP capability model for `database/mongodb`
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

The repository uses `pytest` as the primary test runner:

```bash
python -m pip install -e .[dev,wire]
python -m pytest -q
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
* [docs/release-3.3.0-draft.md](/Users/uve/Proyectos/mongoeco2/docs/release-3.3.0-draft.md)
* [TODO.md](/Users/uve/Proyectos/mongoeco2/TODO.md)
* [MISSING_FEATURES.md](/Users/uve/Proyectos/mongoeco2/MISSING_FEATURES.md)

## License

This project is licensed under the Apache License 2.0. See
[LICENSE](LICENSE).
