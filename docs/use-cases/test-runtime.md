# Test a Python application that uses MongoDB

Start by replacing the database preparation for one application test. Keep its
queries and assertions, then run that path against Memory and SQLite. The
[pytest kit](../pytest.md) uses real PyMongo clients over a local wire server;
no external MongoDB deployment is needed for the supported local subset.

The kit is implemented for the upcoming release. Until publication, install
its candidate wheel with the `pytest` extra. The commands below describe the
released-package workflow once it is available.

## Keep an existing application function

For example, this application code accepts a database supplied by its caller.
It can keep using the same PyMongo imports and type annotations as before.

```python
# users.py
from pymongo.database import Database


def create_user(database: Database, name: str):
    return database.users.insert_one({"name": name}).inserted_id
```

Install the test dependency:

```bash
python -m pip install "mongoeco[pytest]"
```

Change only test preparation:

```python
# conftest.py
import pytest
from pymongo import MongoClient

pytest_plugins = ("mongoeco.pytest_plugin",)


@pytest.fixture
def database(mongoeco_uri):
    with MongoClient(mongoeco_uri, tz_aware=True) as client:
        yield client.application
```

The test calls the application function with that database:

```python
# test_users.py
from users import create_user


def test_create_user(database):
    user_id = create_user(database, "Ada")
    assert database.users.find_one({"_id": user_id})["name"] == "Ada"
```

```bash
python -m pytest --mongoeco-backend=both
```

If you do not need your own constructor, a `database` fixture can simply return
`mongoeco_client.application`. The managed fixture yields a real
`pymongo.MongoClient`. Clients constructed from `mongoeco_uri` remain yours to
close; the kit closes the clients it creates.

## Async applications

Install `pytest-asyncio>=1.4,<2` alongside `mongoeco[pytest]` and load
`mongoeco.pytest_asyncio` instead of the common plugin. The managed client is a
real `pymongo.AsyncMongoClient`:

```python
# conftest.py
pytest_plugins = ("mongoeco.pytest_asyncio",)
```

```python
# test_async_users.py
import pytest


async def create_user(database, name):
    return (await database.users.insert_one({"name": name})).inserted_id


@pytest.mark.asyncio
async def test_create_user(mongoeco_async_client):
    database = mongoeco_async_client.application
    user_id = await create_user(database, "Ada")
    assert (await database.users.find_one({"_id": user_id}))["name"] == "Ada"
```

For your own async constructor, use `async with AsyncMongoClient(mongoeco_uri, ...)`
in a fixture owned by your runner. See the [fixture guide](../pytest.md) for
AnyIO on asyncio, function loop scopes and unsupported runner combinations.

## Isolation, failures and parallel workers

Each test receives a new engine, catalog and dynamic port. URI and managed
clients within that test share their data. SQLite uses a private temporary
directory and executor. Cleanup covers setup failures, failed assertions and
async cancellation; it does not depend on a successful test body.

Use `--mongoeco-backend=memory`, `sqlite`, or `both`. Only tests depending on the
kit are parametrized. Install pytest-xdist separately to run the same suite
with `python -m pytest --mongoeco-backend=both -n 4`.
The [acceptance report](../pytest-review.md) records tested interpreter,
pytest and SDK combinations, parallel workers and cleanup regressions.

## Choose the appropriate boundary

| Application preparation | Starting point |
| --- | --- |
| Accepts a PyMongo client or database | Inject `mongoeco_client`, `mongoeco_async_client`, or a database from one |
| Creates its client from a configurable URI | Supply `mongoeco_uri` and preserve the constructor and options |
| Uses mongomock or patches constructors globally | Follow the [migration guide](../migrating-from-mongomock.md) |
| Can use a PyMongo-shaped client without retaining native client types | Inject Mongoeco's direct `MongoClient` or `AsyncMongoClient`; no wire server needed |

Direct API examples are in [memory_quickstart.py](../../examples/memory_quickstart.py)
and [test_runtime_local.py](../../examples/test_runtime_local.py). The latter
executes the same collection and local search contract with both engines.

## Compatibility to check before expanding adoption

Start with the actual queries, write options, BSON types and exceptions used by
that application path. In the next release, `insert_many(ordered=...)` preserves
native partial effects and bulk error details; do not expect an entire invalid
batch to be rolled back. See the [native write corpus](../write-parity-validation.md)
and [error payload regressions](../write-error-operations.md).

Keep MongoDB integration tests for replication, failover, sharding, remote
network retries and distributed behavior. Local change streams, sessions and
search have bounded contracts in the [compatibility guide](../../COMPATIBILITY.md).
A passing local suite or the anonymous canary does not certify every operation
in an application. Mongoeco requires Python >=3.13; consumers supporting older
interpreters need a conditioned test dependency and alternative preparation.
