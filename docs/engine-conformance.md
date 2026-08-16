# Public Engine Conformance

MongoEco exposes a framework-neutral conformance kit under
`mongoeco.conformance`. External engines provide only a factory and lifecycle;
the runner owns isolated namespaces and executes versioned profiles.

```python
from mongoeco.conformance import (
    EngineConformanceProvider,
    run_engine_conformance,
)

provider = EngineConformanceProvider(
    name='my-engine',
    factory=MyEngine,
)
report = await run_engine_conformance(provider)
report.require_success()
```

The default profiles cover SPI v2 capabilities, CRUD outcomes,
`OperationContext`, compare-and-set atomicity, stable snapshots, sequenced
change delivery and the optional `search-v1` profile when declared by the
engine. Engines that declare `injected_clock` are also checked with a fixed
`$$NOW` captured by the operation context. Optional checks without a declared
capability remain visible with status `not-applicable`; they never count as a
pass. A report with no executed checks fails explicitly, so an empty or wholly
inapplicable selection cannot pass vacuously.

The Search profile exercises every declared optional capability: hit
execution, every declared operator and vector similarity, metadata collectors,
typed highlight runtime metadata, persistence isolation and both explain
verbosities. Declaring support is
therefore a testable contract, not a feature flag accepted on trust.

The core runner has no pytest dependency and returns all failures in one
`ConformanceReport`. Projects that use pytest can install
`mongoeco[engine-testing]` and call
`mongoeco.conformance.pytest.assert_conformance(report)`.

Providers may be synchronous or asynchronous factories. If the returned engine
defines `connect()` and `disconnect()`, the runner invokes and awaits them. A
fresh database namespace is generated for every check. The runner calls the
provider `cleanup` callback after every pass or failure; when it is omitted, it
uses `engine.drop_database()` when available. Check and cleanup failures are
reported independently so a broken assertion cannot hide leaked state.

Reports use `schemaVersion="mongoeco-conformance-report/v1"`, independently
from `contractVersion="spi-v2"`. Every check exposes a stable ID, capability,
phase, duration, evidence, status and cleanup error. JSON serialization is
deterministic and `human_summary()` provides the same status counts for logs.
The public 4.5 constructor `ConformanceCheckResult(..., passed=...)` remains a
compatibility boundary and is normalized into the typed status model.

`mongoeco.conformance.conformance_report_schema()` returns an owned copy of the
public Draft 2020-12 JSON Schema embedded in wheel and sdist. Additive optional
fields may evolve within v1; changing required fields, meanings or accepted
states requires a new `schemaVersion`. `contractVersion` evolves separately
when the engine contract changes. Consumers should select the schema by
`schemaVersion`, never derive it from the current Python dataclasses.

The package exports `operation_context_factory()`, `snapshot_factory()`,
`outcome_factory()`, `partial_batch_factory()`, `change_delivery_factory()`,
`search_request_factory()`, `runtime_metadata_factory()` and
`cancellation_factory()`. The cancellation scenario blocks a snapshot read,
cancels it deterministically and verifies resource closure. These fixtures
contain no pytest dependency and can be reused by external engines.

The snapshot profile always crosses the public adapter boundary. It validates
both native `open_read_snapshot()` implementations and the compatible
`scan_find_semantics` fallback, including operation identity, `STABLE` policy,
ownership, item mutation isolation and isolation from a concurrent insert.
Sequenced change delivery injects a partial batch failure, requires only the
unacknowledged suffix to remain pending and verifies that consumers cannot
mutate each other's payloads.

The kit tests itself against deliberately defective engines. The negative
matrix includes false capability declarations, invalid outcomes, non-atomic
CAS, mutable and live snapshots, reread clocks, incomplete batch cardinality,
mutable or duplicate events, cleanup failure, persisted Search metadata and
execution/explain divergence. A declared capability that violates its contract
is `failed`; `error` is reserved for runner or infrastructure failures.

SPI v1 remains available only through the compatibility adapter during 4.x; it
is intentionally outside this kit and cannot pass the SPI v2 core profile.

## CLI

The module entry point is a thin layer over `run_engine_conformance()`:

```bash
python -m mongoeco.conformance package.engine:factory \
  --profile spi-v2-core \
  --format json \
  --output conformance.json \
  --require-success
```

Factories and cleanup callbacks may be synchronous or asynchronous. Repeat
`--profile` or pass comma-separated values. Exit codes are stable: `0` success,
`1` contractual failure under `--require-success`, `2` load error, `3` internal
error, `4` timeout and `5` schema error. Reports go to stdout or `--output`;
diagnostics go to stderr.

The repository's `tests.consumer.engine_canary.ExternalCanaryEngine` is the
black-box reference for third-party implementation. It owns its storage, does
not derive from built-in engines and imports only `mongoeco.engines`.
`python scripts/smoke_external_engine.py` runs its applicable profile and
validates the resulting report against the public schema. Release CI executes
that smoke after installing the built wheel from `site-packages`.
