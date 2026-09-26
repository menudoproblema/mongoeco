# Mongoeco migration to exchange-only CXP

This guide describes the isolated removal-major source. It is not a published
release or a version assignment. The preceding migration release keeps the old
imports with deprecation warnings while consumers change.

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
catalog snapshots remain untouched. The source candidate manifest is
`tests/fixtures/public_api_manifest_exchange_source.json`; comparison records
five removals and two additions. The exact legacy Python source and tests are
archived in `evidence/mongoeco-legacy-cxp-python.zip`, and the old CXP guide is
`evidence/mongoeco-legacy-cxp.md`.

Before a public removal release, publish and verify the required CXP
migration minors, rebuild Mongoeco and its consumers against the selected CXP
major artifact, update dependency bounds and lockfiles to that exact release,
run the full installed artifact matrix and compare the final public API
manifest. The current source keeps the pre-major package version and `<5` CXP
bound so a resolver cannot install an unpublished incompatible major.
