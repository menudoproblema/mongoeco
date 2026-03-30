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

Benchmark outputs are treated as local artifacts and should stay out of git.
The harness itself is versioned so anyone can reproduce the same workload mix,
dataset seeds and report structure from a given revision.

## Project Status

The repository is in active development and the public package surface is still
best treated as pre-release.

## License

This project is licensed under the Apache License 2.0. See
[LICENSE](LICENSE).
