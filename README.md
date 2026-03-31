# mongoeco

`mongoeco` is an async-first MongoDB-like persistence library with pluggable
storage engines.

It is designed for local development, test environments, embedded persistence
and compatibility work where a PyMongo-shaped API is useful without requiring a
real MongoDB server for every workflow.

## Current Scope

What is already in place:

* async and sync client APIs
* memory and SQLite engines
* transactional local sessions
* aggregation runtime with pushdown and spill guardrails
* compatibility modeling across MongoDB dialects and PyMongo profiles
* local wire/driver runtime
* search index lifecycle plus local `$search` / experimental `$vectorSearch`

What this is not:

* a drop-in replacement for a production MongoDB cluster
* a full Atlas Search implementation
* a full-text/vector engine with server-grade scaling guarantees

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
* the `simple` collation keeps using the BSON/Python baseline comparator
* the currently supported locale surface is `simple` and `en`
* the currently supported strengths are `1`, `2` and `3`
* `numericOrdering` and `caseLevel` are supported inside that subset
* `PyICU` and `pyuca` are intentionally close, but may still differ on
  advanced tailoring details outside the currently supported locale surface

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

## Testing

The repository currently uses the standard library test runner:

```bash
python -m pip install -e .[dev]
python -m unittest discover -s tests -p 'test*.py'
```

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

Latest local smoke snapshot from March 30, 2026:

- command:

```bash
python -m benchmarks.run \
  --engine all \
  --size 1000 \
  --warmup 0 \
  --repetitions 1 \
  --format table
```

- high-level reading:
  - `memory` is currently strongest on point lookups and most filter-heavy workloads
  - `sqlite` is currently strongest on full-sort workloads and remains competitive on filter-heavy workloads
  - `mongomock` is still a useful baseline, but `mongoeco` now beats it in many lookup, filter and sort cases on this harness
- notable smoke numbers:
  - `secondary_lookup_indexed_1k`
    - `memory-sync 0.1647s`
    - `sqlite-sync 0.3760s`
    - `memory-async 0.1542s`
    - `sqlite-async 0.3089s`
    - `mongomock 0.6840s`
  - `filter_selectivity_high_100`
    - `memory-sync 0.3720s`
    - `sqlite-sync 0.4404s`
    - `memory-async 0.3201s`
    - `sqlite-async 0.3534s`
    - `mongomock 0.2676s`
  - `sort_limit_indexed_200`
    - `memory-sync 0.1971s`
    - `sqlite-sync 0.3427s`
    - `memory-async 0.1783s`
    - `sqlite-async 0.4458s`
    - `mongomock 1.4541s`
  - `sort_shape_top_level_full_50`
    - `memory-sync 0.6109s`
    - `sqlite-sync 0.3267s`
    - `memory-async 0.5790s`
    - `sqlite-async 0.3182s`
    - `mongomock 0.4602s`

Treat this as a smoke snapshot, not a publication-quality claim. For anything
you plan to cite publicly, use the reproducible report commands below with
warmup and repeated runs.

Recommended community matrix:

- `size=100`
  - small-dataset overhead and API-path visibility
- `size=1000`
  - primary reference point for balanced comparisons
- `size=5000`
  - larger-scale behavior without turning the default community workflow into a
    multi-hour run

The `size=50` case is mainly useful for local smoke checks, and `size=500`
rarely changes the story enough to justify making it part of the default
published matrix.

Reproducible local report:

```bash
python -m benchmarks.report \
  --engine all \
  --size 10000 \
  --warmup 1 \
  --repetitions 5 \
  --output-json benchmarks/reports/latest.json \
  --output-markdown benchmarks/reports/latest.md
```

Suggested community runs:

```bash
python -m benchmarks.report \
  --engine all \
  --size 100 \
  --warmup 1 \
  --repetitions 5 \
  --output-json benchmarks/reports/matrix-100.json \
  --output-markdown benchmarks/reports/matrix-100.md

python -m benchmarks.report \
  --engine all \
  --size 1000 \
  --warmup 1 \
  --repetitions 5 \
  --output-json benchmarks/reports/matrix-1000.json \
  --output-markdown benchmarks/reports/matrix-1000.md

python -m benchmarks.report \
  --engine all \
  --size 5000 \
  --warmup 1 \
  --repetitions 5 \
  --output-json benchmarks/reports/matrix-5000.json \
  --output-markdown benchmarks/reports/matrix-5000.md
```

Latest serious reference snapshot used for local analysis:

- date: March 30, 2026
- command:

```bash
python -m benchmarks.report \
  --engine all \
  --size 1000 \
  --warmup 1 \
  --repetitions 5 \
  --output-json benchmarks/reports/latest-1000-serious.json \
  --output-markdown benchmarks/reports/latest-1000-serious.md
```

- environment:
  - Python `3.14.0`
  - macOS `15.6`
  - `arm64`
  - JSON backend `stdlib`
  - git revision `7277a30904192aa8c6cbb7547ec3035840781bb4`
  - worktree was dirty, so treat the numbers as a strong local reference, not a
    final published baseline
- representative means:
  - `secondary_lookup_indexed_1k`
    - `memory-sync 0.1430s`
    - `sqlite-sync 0.3142s`
    - `memory-async 0.1414s`
    - `sqlite-async 0.3048s`
    - `mongomock 0.6643s`
  - `filter_selectivity_high_100`
    - `memory-sync 0.3275s`
    - `sqlite-sync 0.3898s`
    - `memory-async 0.3069s`
    - `sqlite-async 0.3556s`
    - `mongomock 0.2529s`
  - `sort_limit_indexed_200`
    - `memory-sync 0.1769s`
    - `sqlite-sync 0.2927s`
    - `memory-async 0.1705s`
    - `sqlite-async 0.4060s`
    - `mongomock 1.3748s`
  - `sort_shape_top_level_full_50`
    - `memory-sync 0.5765s`
    - `sqlite-sync 0.2887s`
    - `memory-async 0.5709s`
    - `sqlite-async 0.2946s`
    - `mongomock 0.4256s`

Current high-level reading from that snapshot:

- `memory` is strongest on point lookups and most filter-heavy workloads
- `sqlite` is strongest on full-sort workloads and remains competitive on many
  filter-heavy workloads
- `mongomock` remains a useful baseline, but it is no longer the fastest option
  in many lookup, filter and sort scenarios covered by this harness

`orjson` note:

- `mongoeco` defaults to `stdlib` JSON for reproducibility
- SQLite-specific A/B runs show that `orjson` can help materially on
  JSON-heavy filter and materialization workloads at `size=1000` and above
- the same A/B runs do not show a universal improvement for lookup-heavy
  workloads
- because of that, benchmark discussions should always state the JSON backend
  explicitly when SQLite is involved

Benchmark outputs are treated as local artifacts and should stay out of git.
The harness itself is versioned so anyone can reproduce the same workload mix,
dataset seeds and report structure from a given revision.

## Project Status

The repository is in active development and the public package surface is still
best treated as pre-release.

## License

This project is licensed under the Apache License 2.0. See
[LICENSE](LICENSE).
