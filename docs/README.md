# Mongoeco documentation

Start with application tests, then choose local persistence or search as needed.
The [4.10.1 release notes](release-4.10.1.md) describe the error and BSON fixes,
migration and delivery evidence. The [4.10.0 release notes](release-4.10.0.md)
cover the pytest kit and supported Python/PyMongo versions.

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
- [findAndModify errors](find-modify-errors.md): complete command errors and
  recovery after a unique-key collision.
- [Shared error documents](shared-error-documents.md) and
  [mirrored error paths](shared-error-mirrors.md): error envelopes, BSON metadata
  and existing SQLite files.
- [Error preparation and transport](error-contract-refactor.md) and
  [expression/BSON regressions](error-composition-regressions.md): native
  MongoDB 7/8/9 corpus and bounded guarantees.
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
