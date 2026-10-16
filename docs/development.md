# Development

For using Mongoeco in an application, start with the
[package quickstart](../README.md#install-and-try-it) or
[pytest guide](pytest.md). The commands below work on a repository checkout.

```bash
git clone https://github.com/menudoproblema/mongoeco.git
cd mongoeco
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,wire]"
python -m pytest -q
```

The [testing and compatibility policy](architecture/testing-and-compatibility.md)
owns the required gates, native references and coverage expectations.
New public behavior needs sync/async coverage when both APIs expose it, and
Memory/SQLite coverage where the engine contract is shared. Pin observable
errors and partial effects, including inherited options after facade
reconstruction, rather than testing only internal implementation details.

CI tests the installed wheel. The [release checklist](release-checklist.md)
covers reproducible wheel/sdist builds, typing, compatibility snapshots, clean
installs, the pytest matrix and anonymous consumer evidence. A source-only
pytest run is useful feedback and does not replace those artifact gates.

Storage engine authors can run the independent public conformance CLI:

```bash
python -m mongoeco.conformance package.engine:factory \
  --format json --output conformance.json --require-success
```

See [engine conformance](engine-conformance.md) for profiles and the optional
`engine-testing` extra. See the [benchmark guide](../benchmarks/README.md) for
reproducible performance comparisons; do not infer general performance claims
from a single local run.

Mongoeco 4.x uses Semantic Versioning with documented exceptions for the SPI v1
removal in [4.7](migrating-to-4.7.md) and the legacy CXP removal in
[4.8](cxp-c4-migration.md). The public surface and future removals are tracked
in the [deprecation catalog](deprecations.md).
