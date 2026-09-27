# Release 4.8.0 — candidate preparation

Status: local source candidate. No tag or package publication has been made.

## Scope

This version replaces Mongoeco's public legacy CXP protocol with exact
`mongoeco.cxp.exchange` documents evaluated by CXP 5.0.0. The removal of
legacy imports and reporting fields is a documented minor-version exception.
The concrete replacements are in [the migration guide](cxp-c4-migration.md).
The project requires `cxp[exchange]>=5.0.0,<6`; Mongoeco 4.7.0 is not
compatible with CXP 5 despite its published dependency metadata.

The owner catalog is `org.mongoeco:mongodb@1.2.0`, `cxp.catalog` spec_version
2, with exact source references and explicit string domains. Snapshots and
requirements keep their own spec_version 1 and pin the catalog's exact hash.
Operational validation, wire commands, runtime state and telemetry remain
Mongoeco contracts. The [exchange guide](cxp-exchange.md) and its linked
conservation matrices state what is represented, tested or still pending;
catalog option acceptance alone does not establish a deployment guarantee.

## Current local evidence

- The source suite passes: 4,389 passed, 26 skipped, 2,497 subtests passed.
- The public API manifest, public typing contract, changed-file Ruff ratchet
  and `git diff --check` pass.
- The local 4.8.0 candidate passed installed wheel and sdist smokes with CXP 5
  and Cosecha. The exact commit and artifact hashes are retained in the
  preparation receipt outside the distributable source.
- The command option inventory has 71 top-level options across 22 database
  commands. Four accepted no-op options have bounded positive and wrong-type
  cases; twenty-four effective options have scoped positive and negative cases.
  Seventeen additional `comment` options have verified `system.profile`
  behavior under enabled and disabled profiling, while their other effects
  remain pending. The other 26 effective options lack individual oracles.

The full [release checklist](release-checklist.md), including remote CI and
the final installed artifact matrix, remains a gate for any later publication.
This preparation does not create a tag or authorize an upload.
