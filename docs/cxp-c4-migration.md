# Mongoeco migration to exchange-only CXP

This guide describes the exchange-only CXP surface published in Mongoeco
4.8.0 and preserved in 4.8.1. The removal of the legacy CXP public surface is an
explicit minor-version exception; the preceding published 4.7.0 still imports
the old protocol and must not be combined with CXP 5.

| Retired surface | Replacement or owner |
| --- | --- |
| `mongoeco.cxp.MONGODB_CATALOG`, profiles, handshake, descriptors and `mongoeco.cxp.catalogs` | Exact `mongoeco.cxp.exchange` catalog, snapshot, tier and profile documents; use CXP `CatalogStore` and `evaluate_requirements_detailed` with context v2 |
| `mongoeco.compat.export_cxp_*` views | `export_exchange_catalog()` embeds the exact catalog and named requirements; `export_full_compat_catalog()` uses `exchange` instead of `cxp` |
| Legacy profile-support and operation compatibility calculations | One CXP exchange evaluation. The mock-safe gate and cursor `explain()["cxp"]["profileVerdicts"]` use the same pinned requirements |
| `mongoeco.cxp.telemetry` and `mongoeco.cxp.driver_telemetry` | `mongoeco.telemetry_contract`, `mongoeco.driver.telemetry_projector` and `mongoeco.driver.telemetry_validation` |
| Runtime subset metadata previously derived through CXP catalog objects | Mongoeco-owned `compat/resources/runtime-subsets-v1.json`; the historical output remains byte-equivalent in reporting |

Metadata values, operation payload validation, runtime lifecycle and telemetry
stay under Mongoeco ownership. Exchange asserts only the reported metadata key
set, operation name and result type. No ignored extension or favorable default
carries a compatibility guarantee. An unreported required property remains
indeterminate; a reported set missing a required key is incompatible.

The old API fixture `tests/fixtures/public_api_manifest_v1.json` and compat
catalog snapshots remain untouched. The exchange public API manifest is
`tests/fixtures/public_api_manifest_exchange_source.json`; comparison records
five removals and two additions. The exact legacy Python source and tests are
archived in `evidence/mongoeco-legacy-cxp-python.zip`, and the old CXP guide is
`evidence/mongoeco-legacy-cxp.md`.

When upgrading consumers, rebuild Mongoeco and its consumers against
the exact CXP 5.0.0 artifact, regenerate any lockfiles after publication, run
the full installed artifact matrix and compare the final public API manifest.
Mongoeco 4.8+ requires `cxp[exchange]>=5.0.0,<6`.

## Aggregation compatibility in 4.8.1

Mongoeco 4.8.1 corrects joins after grouping, nested resource scopes and BSON
comparison semantics. `$collStats.count` now returns the MongoDB-compatible
scalar instead of a nested count document: replace `$count.count` projections
with `$count`. Import roots, SPI v2 and persistent formats remain unchanged.
