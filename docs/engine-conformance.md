# Public Engine Conformance

MongoEco 4.5 exposes a framework-neutral conformance kit under
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
`$$NOW` captured by the operation context. Inapplicable optional profiles are
omitted rather than reported as passes.

The Search profile exercises every declared optional capability: hit
execution, metadata collectors, highlight sidecar output and both explain
verbosities. Declaring support is therefore a testable contract, not a feature
flag accepted on trust.

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

SPI v1 remains available only through the compatibility adapter during 4.x; it
is intentionally outside this kit and cannot pass the SPI v2 core profile.
