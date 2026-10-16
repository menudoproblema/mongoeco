# Choosing a local test runtime

Use the operations and deployment properties your application tests need to
choose a boundary. Mongoeco provides a finite, tested local subset; a shared
query shape does not establish full MongoDB, PyMongo or Atlas compatibility.

## Mongoeco and MongoDB server

| Need | Mongoeco provides | Keep a MongoDB test for |
| --- | --- | --- |
| Application CRUD, queries and aggregation | Direct sync/async APIs and native PyMongo clients through a local wire endpoint, within the documented subset | Operations or options outside the verified corpus |
| Write errors and partial effects | Native captures checked against error codes, BSON, counters and resulting documents | Deployment-dependent failure behavior |
| Test isolation | New Memory/SQLite engine, catalog and port per pytest test | Server configuration, authentication and topology requirements of the deployment |
| Persistence | SQLite storage inside a local process | Cluster durability, replication and failover |
| Search | Local `$text`, `$search` and `$vectorSearch` subsets with `explain()` diagnostics | Atlas Search ranking, collectors and service behavior |

Mongoeco's local sessions and change streams have explicit local contracts.
They do not establish distributed transaction or multi-node change stream
behavior. See the [compatibility guide](../COMPATIBILITY.md),
[architecture](architecture/index.md) and [native write evidence](write-parity-validation.md).

## Moving a suite from mongomock

The useful comparison is whether Mongoeco satisfies the application's actual
operations and preparation requirements. Start with the
[migration guide](migrating-from-mongomock.md) rather than assuming interchangeable
constructors or universal drop-in behavior.

These Mongoeco properties have concrete verification:

| Property to evaluate | Mongoeco's evidence |
| --- | --- |
| Keep native PyMongo imports and client types | Opt-in [pytest fixtures](pytest.md), real sync/async clients and an installed-wheel [acceptance matrix](pytest-review.md) |
| Preserve ordered/unordered batch effects | `insert_many` [native corpus](write-parity-validation.md), including partial counters and error indices |
| Inspect the failed bulk operation | [`writeErrors[].op` regressions](write-error-operations.md), including BSON and raw-command boundaries |
| Reject invalid positional updates | [Native positional corpus](../tests/differential/positional_cases.py) and [write corpus acceptance](plan-write-parity.md) |
| Run one suite against two local engines | Memory/SQLite contract tests and fixture parametrization |
| Inspect supported operations and query planning | [Compatibility guide](../COMPATIBILITY.md), `explain()` and [CXP exports](cxp-exchange.md) |

This table describes Mongoeco's evidence. It is not an audit of every mongomock
release or a comparative benchmark. Keep an existing mock where it satisfies
the suite's needs; evaluate migration on unchanged application assertions and
retain the necessary real-server tests.

## Memory or SQLite

| Choose | When it fits | Practical boundary |
| --- | --- | --- |
| `MemoryEngine` | Fresh application data with no database file, in-process tests | State belongs to that engine instance |
| `SQLiteEngine` | Embedded persistence, file reopen tests, local indexing and search | SQL/FTS5/ANN pushdown and fallback paths differ from Memory |

Both expose the same intended collection contract for the covered subset, with
cross-engine parity tests. Their execution plans and persistence properties
remain different; inspect `explain()` where the plan matters.

For measured performance comparisons, use the
[benchmark harness](../benchmarks/README.md) with recorded inputs and environment.
For adoption, begin with the [application test guide](use-cases/test-runtime.md),
then consider [embedded persistence](use-cases/embedded-app.md) or
[local search](use-cases/local-search-and-retrieval.md).
