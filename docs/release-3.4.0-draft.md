# Release Draft 3.4.0

Status: prepared locally for the `3.4.0` cut. Publication to PyPI is optional.

## Headline

`mongoeco 3.4.0` strengthens mock/runtime testing as a product surface:
high-level driver failpoints, strict mock-safe compat profiling, differential
golden replay, and deterministic vector explain parity checks.

## Main user-facing changes

### Driver failpoints for higher-level failure scenarios

* New runtime failpoint controller for driver execution lifecycle:
  * retryable command failure injection;
  * transient transaction failure injection;
  * write concern timeout injection;
  * forced server selection timeout.
* Failpoint-injected flows remain visible through canonical driver monitor
  events, preserving telemetry and diagnostics semantics.

### Strict compat profile for mock/test tooling

* New `mongoeco.compat.export_mock_safe_profile_catalog()` projection.
* `mongoeco-mock-safe` gives a strict local gating contract over canonical
  CXP capability metadata without replacing canonical reusable CXP profiles.
* Compat snapshots were regenerated to keep the published catalog contract
  deterministic.

### Differential replay baseline

* Added a local replay golden fixture derived from real Mongo differential
  cases.
* Added replay tests that enforce memory/sqlite parity against that baseline.
* Added script to refresh the golden baseline from real Mongo when needed.

### Determinism hardening for vector explain parity

* Added integration coverage that asserts repeatability of vector results and
  explain shape per engine.
* Added cross-engine semantic parity checks for stable explain dimensions
  (`queryOperator`, `pathSummary`, hybrid mode, prefilter sources).

## Validation summary

* `python -m pytest -q`
* `python -m pytest --cov=src/mongoeco --cov-report=term -q`
* Global coverage stays at `100%`.

## Limits that remain explicit

* `mongoeco` remains embedded/local runtime infrastructure, not distributed
  MongoDB server replacement.
* CXP negotiation/lifecycle remains outside `mongoeco` core and belongs to
  runner/orchestrator layers.

