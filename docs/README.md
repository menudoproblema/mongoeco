# Mongoeco documentation

Start with application tests, then choose local persistence or search as needed.
The [4.10.0 release notes](release-4.10.0.md) describe the pytest kit,
compatibility improvements, migration and delivery evidence.

## Test a Python application

- [Application testing](use-cases/test-runtime.md): inject a database into existing
  application code and run the same tests against Memory and SQLite.
- [Coming from mongomock](migrating-from-mongomock.md): change the preparation,
  retain application assertions, and identify compatibility checks to keep.
- [Pytest fixtures](pytest.md): native sync/async PyMongo clients, a local URI,
  runner setup, backend selection and cleanup.
- [Comparisons](comparisons.md): choose between a local runtime and MongoDB tests.

## Check compatibility

- [Compatibility guide](../COMPATIBILITY.md) and [dialects](../DIALECTS.md):
  supported operations, API profiles and explicit limits.
- [Native write corpus](write-parity-validation.md): results, BSON, error codes,
  counters and partial effects compared with real MongoDB captures.
- [Write identity](write-identity-validation.md),
  [error payloads](write-error-operations.md),
  [API options](pymongo-api-conveniences.md) and
  [BSON ID conversion](id-bson-conversion.md): bounded regression evidence.
- [Pytest acceptance](pytest-review.md): installed-wheel matrix, parallel tests
  and anonymous consumer canary.
- [Python support policy](python-support.md) and [PyMongo 4.19](pymongo419.md).

## Store and search locally

- [Embedded applications](use-cases/embedded-app.md).
- [Local search and retrieval](use-cases/local-search-and-retrieval.md).
- [Executable examples](../examples/README.md).
- [Runtime reference](runtime-reference.md): public API boundaries, collation,
  JSON backends, local change streams and driver diagnostics.

## Advanced tooling and development

- [Architecture index](architecture/index.md) and
  [testing policy](architecture/testing-and-compatibility.md).
- [CXP exchange](cxp-exchange.md): inspect the declared compatibility subset.
- [Engine SPI v2](architecture/engine-spi-v2.md) and
  [engine conformance](engine-conformance.md).
- [Development](development.md) and [benchmarks](../benchmarks/README.md).
- [Changelog](../CHANGELOG.md), [deprecations](deprecations.md),
  [release checklist](release-checklist.md), [backlog](../TODO.md) and
  [missing features](../MISSING_FEATURES.md).
