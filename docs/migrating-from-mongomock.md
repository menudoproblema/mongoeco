# Coming from mongomock

Try Mongoeco on one test module before changing a whole suite. The first step is
to change how the application receives its database, while keeping its data,
operations and assertions. This guide targets Mongoeco 4.10.0 or later with the
[official pytest kit](pytest.md); use its candidate wheel until published.

## 1. Choose the client boundary

If application code already imports or annotates PyMongo clients, use the kit's
local URI or managed native clients. This keeps native client, collection and
result types. For applications that only need a PyMongo-shaped API and accept an
injected client, the direct Mongoeco API is another option.

| Current preparation | Smallest useful change to try |
| --- | --- |
| Fixture returns `mongomock.MongoClient()` | Return the managed `mongoeco_client`, or its database |
| Application accepts a connection URI | Pass `mongoeco_uri` to its existing PyMongo constructor |
| Async PyMongo application | Use `mongoeco_async_client` with the matching runner adapter |
| Global patch of `pymongo.MongoClient` | Move the test seam to an injected client, database, URI or client factory |

The size of the change depends on that seam. A fixture-based application can
keep its business code untouched; a hard-coded client constructor may require
making configuration or dependency injection explicit.

## 2. Replace a client fixture

Install the test extra from the target release:

```bash
python -m pip install "mongoeco[pytest]"
```

For an existing synchronous fixture, this is the whole preparation change:

```python
# conftest.py
import pytest

pytest_plugins = ("mongoeco.pytest_plugin",)


@pytest.fixture
def database_client(mongoeco_client):
    return mongoeco_client
```

Keep the existing tests using `database_client`. Each test gets a fresh server
and engine, even when database and collection names repeat. The kit closes the
client, active connections and server, including when a test fails.

To keep constructor options such as `tz_aware` or a custom document class,
consume `mongoeco_uri` with your own PyMongo context manager instead. The
[application testing guide](use-cases/test-runtime.md) includes a complete
application function, URI fixture and test. The [fixture guide](pytest.md)
owns async runner setup, options and ownership.

## 3. Run both engines and inspect failures

```bash
python -m pytest tests/test_users.py --mongoeco-backend=both
```

Keep the original assertions. Check each failure against the native behavior
that matters to the application before changing an expectation. Some fixtures
may rely on behavior that differs from MongoDB; Mongoeco can also have gaps in
operations outside its verified subset. Running both engines checks that the
application path works with each local storage implementation.

Concrete guarantees for the next release include:

- `insert_many(..., ordered=True)` stops on the first server write error and
  retains prior successes. `ordered=False` can retain later independent
  successes. Collection helpers expose `BulkWriteError` with error indices,
  codes, `writeErrors[].op` and partial counters.
- A positional update over a non-array field fails without applying other
  changes to that same document. A valid empty array or an array with no
  matching elements retains its valid behavior.
- Duplicate keys, invalid IDs and findAndModify errors have bounded regression
  coverage for error families, codes, BSON types and resulting documents.

See the [write evidence](write-parity-validation.md),
[identity extension](write-identity-validation.md) and
[error payload checks](write-error-operations.md). These are claims about
Mongoeco's tested corpus, not claims that every mongomock version behaves
otherwise. The [comparison guide](comparisons.md) maps the choices without a
universal replacement or performance claim.

## 4. Retain real-server tests

Use MongoDB for tests whose result depends on replication, sharding, failover,
remote retries, distributed transactions or Atlas-specific semantics.
The [compatibility guide](../COMPATIBILITY.md) describes dialect/profile
selection and local limits; selecting a profile does not turn a local engine
into that server version. The [kit acceptance report](pytest-review.md) records
actual SDK/interpreter combinations and the installed-wheel consumer canary.

The package requires Python >=3.13. If your suite also supports older Python,
condition the dependency and preserve another database preparation for those
cells. Do not raise an application's minimum by adding the extra unconditionally.
