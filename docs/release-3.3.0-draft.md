# Release Draft 3.3.0

Status: prepared locally for the `3.3.0` cut. Publication to PyPI is optional.

## Headline

`mongoeco 3.3.0` consolidates the CXP-first public contract, upgrades to
`cxp>=3.0.0`, and closes telemetry and tooling-facing metadata so compatibility
consumers no longer need ad hoc heuristics.

## Main user-facing changes

### CXP contract consolidation

* `mongoeco.cxp` remains the canonical contract source.
* `mongoeco.compat` now projects `profiles`, `profileSupport` and `operations`
  directly from canonical CXP exports, preventing catalog drift.
* Public operation exports now include canonical telemetry requirements per
  capability binding.

### Telemetry projection maturity

* Driver telemetry projection emits canonical resource attributes:
  * `cxp.resource.name`
  * `cxp.resource.kind`
* Snapshot-level telemetry contracts are now consumable for tooling/tests from
  both capability and operation views.

### Platform/profile completeness

* `mongodb-platform` operation metadata is now closed for:
  * `collation`
  * `persistence`
  * `topology_discovery`
* Public profile/operation metadata can be consumed as a stable contract for
  tests/tooling without manual patching.

### Runtime quality and coverage

* Additional validation and diagnostics in local text/index paths for
  `memory` and `sqlite`.
* Test suite and `src/mongoeco` coverage are both closed at `100%`.

## Limits that remain explicit

* `mongoeco` is still an embedded/local runtime and does not claim full server
  parity with distributed MongoDB deployments.
* `mongoeco` still does not become a CXP live provider by default; telemetry
  is projected from internal runtime events.

## Suggested short announcement

> `mongoeco 3.3.0` upgrades to `cxp>=3.0.0` and finalizes a cleaner
> CXP-first contract: canonical `mongoeco.cxp`, projected `compat`, richer
> telemetry metadata, and a tooling-grade profile/operation surface with
> full test coverage.
