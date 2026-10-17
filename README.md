# Mongoeco

Local MongoDB testing and embedded persistence for Python, with PyMongo-style
sync and async APIs.

Test application queries and writes locally, using a fresh in-memory database
or SQLite. Start with the direct Python API, or keep real PyMongo clients and
point them at an isolated local server with the optional pytest kit.

**4.10.1** corrects command and write error details, validation timing and BSON
preservation. See the [release notes](https://github.com/menudoproblema/mongoeco/blob/main/docs/release-4.10.1.md)
for changes, compatibility limits and validation. The pytest kit is available
since 4.10.0.

## Install and try it

```bash
python -m pip install mongoeco
```

```python
from mongoeco import MongoClient

with MongoClient() as client:
    users = client.demo.users
    users.insert_one({"_id": "ada", "name": "Ada"})
    assert users.find_one({"_id": "ada"})["name"] == "Ada"
```

The default engine is in memory. Mongoeco 4.10 supports CPython **3.13,
3.14 and 3.15**, with Python 3.13 support through **October 2028**.
See the [support policy](https://github.com/menudoproblema/mongoeco/blob/main/docs/python-support.md).

## Test an application that uses MongoDB

If your application already accepts a database client or connection URI, start
by changing that test fixture. Keep its queries, writes and assertions, then
run the operations your application actually uses against both local engines.

### Keep real PyMongo clients with pytest

Install Mongoeco 4.10 or later with the `pytest` extra:

```bash
python -m pip install "mongoeco[pytest]>=4.10"
```

Load the plugin explicitly in your root `conftest.py`:

```python
pytest_plugins = ("mongoeco.pytest_plugin",)
```

Then use a managed, real `pymongo.MongoClient` in a test:

```python
def test_create_user(mongoeco_client):
    users = mongoeco_client.app.users
    users.insert_one({"_id": "ada", "name": "Ada"})
    assert users.count_documents({}) == 1
```

```bash
python -m pytest --mongoeco-backend=both
```

Each test gets its own server, dynamic port and engine. The kit closes its
clients and server and removes temporary SQLite files, including after failures.
`both` runs only tests that depend on the kit against Memory and SQLite.
To preserve your own client constructor and options, consume `mongoeco_uri`
in your existing fixture instead.

For native async clients, install your runner and load its adapter:

```bash
python -m pip install "mongoeco[pytest]>=4.10" "pytest-asyncio>=1.4,<2"
```

```python
# conftest.py: use this adapter instead of mongoeco.pytest_plugin
pytest_plugins = ("mongoeco.pytest_asyncio",)
```

```python
import pytest

@pytest.mark.asyncio
async def test_create_user(mongoeco_async_client):
    users = mongoeco_async_client.app.users
    await users.insert_one({"_id": "ada", "name": "Ada"})
    assert await users.count_documents({}) == 1
```

An AnyIO adapter is also available for the asyncio backend. The
[pytest guide](https://github.com/menudoproblema/mongoeco/blob/main/docs/pytest.md)
covers runners, options, ownership and parallel execution with pytest-xdist.

### Use the direct async API

The base package also provides its own async client without a wire server:

```python
import asyncio
from mongoeco import AsyncMongoClient

async def main():
    async with AsyncMongoClient() as client:
        await client.demo.users.insert_one({"_id": "ada", "name": "Ada"})
        assert await client.demo.users.count_documents({}) == 1

asyncio.run(main())
```

Coming from mongomock? Follow the
[migration guide](https://github.com/menudoproblema/mongoeco/blob/main/docs/migrating-from-mongomock.md)
for client injection, preserving PyMongo imports and testing one application
path first. The [application testing guide](https://github.com/menudoproblema/mongoeco/blob/main/docs/use-cases/test-runtime.md)
shows a complete fixture and application function.

## Verified compatibility and limits

Mongoeco's regression corpus compares observable results with captured
**MongoDB 7.0.43, 8.0.32 and 9.0.2** results. It checks BSON types, error codes,
write counters and partial effects through sync/async APIs and Memory/SQLite.
Examples covered in 4.10 include:

| Application operation | Verified behavior | Evidence and coverage |
| --- | --- | --- |
| `insert_many(..., ordered=True/False)` with an invalid document or duplicate | Preserve successful writes, original error indices, duplicate code `11000` and `nInserted`; continue later independent writes only when unordered | [Write corpus](https://github.com/menudoproblema/mongoeco/blob/main/docs/write-parity-validation.md) |
| Positional update on a field that contains a number instead of an array | Reject the update without applying its other changes to that document | [Positional regression cases](https://github.com/menudoproblema/mongoeco/blob/main/tests/differential/positional_cases.py) |
| Failed inserts and bulk models | Preserve `writeErrors[].op`, BSON values and partial counts | [Error payload regressions](https://github.com/menudoproblema/mongoeco/blob/main/docs/write-error-operations.md) |

The [compatibility guide](https://github.com/menudoproblema/mongoeco/blob/main/COMPATIBILITY.md)
and [dialect guide](https://github.com/menudoproblema/mongoeco/blob/main/DIALECTS.md)
define the supported subset. MongoDB dialect and PyMongo API profile are separate
choices; defaults remain MongoDB `7.0` and PyMongo `4.9`.
The [pytest acceptance report](https://github.com/menudoproblema/mongoeco/blob/main/docs/pytest-review.md)
records the installed-wheel matrix, failure cleanup and anonymous consumer canary.

Use MongoDB integration tests for replication, sharding, server failover,
network retry behavior, distributed transactions and Atlas-specific behavior.
Local sessions, change streams and search have their own documented limits.
The finite regression corpus does not establish universal MongoDB or PyMongo
compatibility. See the [comparison guide](https://github.com/menudoproblema/mongoeco/blob/main/docs/comparisons.md)
when choosing which tests to run locally and which to keep on a real server.

## SQLite persistence and local search

Keep data on disk without running a database service:

```python
from mongoeco import MongoClient
from mongoeco.engines import SQLiteEngine

with MongoClient(SQLiteEngine("app.db")) as client:
    client.app.todos.update_one(
        {"_id": "ship"}, {"$set": {"done": False}}, upsert=True
    )
    assert client.app.todos.find_one({"_id": "ship"})["done"] is False
```

The local runtime supports aggregation, classic `$text`, a documented `$search`
subset and ANN-backed `$vectorSearch`. `explain()` reports candidate selection,
residual filtering and fallback behavior. This is local search, with its own
ranking and supported operators; it does not establish Atlas Search parity.

- [Embedded persistence guide](https://github.com/menudoproblema/mongoeco/blob/main/docs/use-cases/embedded-app.md)
- [Search and retrieval guide](https://github.com/menudoproblema/mongoeco/blob/main/docs/use-cases/local-search-and-retrieval.md)
- [Executable examples](https://github.com/menudoproblema/mongoeco/blob/main/examples/README.md)

## Documentation and development

Start at the [documentation index](https://github.com/menudoproblema/mongoeco/blob/main/docs/README.md).
Advanced topics have their own guides:

- [Runtime options, collation and change streams](https://github.com/menudoproblema/mongoeco/blob/main/docs/runtime-reference.md)
- [Architecture and public contracts](https://github.com/menudoproblema/mongoeco/blob/main/docs/architecture/index.md)
- [CXP capability exports](https://github.com/menudoproblema/mongoeco/blob/main/docs/cxp-exchange.md)
- [Custom engines and conformance](https://github.com/menudoproblema/mongoeco/blob/main/docs/engine-conformance.md)
- [Development and checks](https://github.com/menudoproblema/mongoeco/blob/main/docs/development.md)
- [Reproducible benchmarks](https://github.com/menudoproblema/mongoeco/blob/main/benchmarks/README.md)
- [Changelog](https://github.com/menudoproblema/mongoeco/blob/main/CHANGELOG.md) and [release checklist](https://github.com/menudoproblema/mongoeco/blob/main/docs/release-checklist.md)

Mongoeco is a PEP 561 typed package. The documented public API and engine SPI v2
are supported contracts; see [deprecations and migrations](https://github.com/menudoproblema/mongoeco/blob/main/docs/deprecations.md)
for version-specific changes.
The [engine SPI decision](https://github.com/menudoproblema/mongoeco/blob/main/docs/architecture/engine-spi-v3-proposal.md)
keeps v2 and shelves v3; no engine migration is planned for the next release.

## License

[Apache License 2.0](https://github.com/menudoproblema/mongoeco/blob/main/LICENSE).
