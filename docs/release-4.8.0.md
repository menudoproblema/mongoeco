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

- The source suite passes: 4,634 passed, 26 skipped, 2,502 subtests passed;
  measured coverage is 99.01% against the 99.00% minimum. `unittest` passes
  3,551 cases and the deep property profile passes four.
- The public API manifest, public typing contract, changed-file Ruff ratchet
  and `git diff --check` pass.
- Wheel and sdist builds of the local candidate pass installed smoke tests
  with CXP 5 and Cosecha. The exact source revision, hashes and environment
  are retained in the local release evidence receipt.
- The 25 real differential cases pass separately against MongoDB 7.0.39 and
  8.0.24, with no failure or skip. The four-version PyMongo profile matrix
  matches its checked-in summary. The current exchange export is written
  outside the historical legacy snapshot fixtures.
- The command option inventory has 71 top-level options across 22 database
  commands. Four accepted no-op options have bounded positive and wrong-type
  cases; all 67 effective options have scoped positive and negative cases.
  The 17 `comment` options retain explicit session metadata and profile only
  when profiling is enabled, through API and wire.
- All 46 advertised wire command names are classified as Mongoeco operational
  contracts for the inspected consumers. Cursor IDs now require the creating
  namespace for `getMore` and `killCursors`; `getMore` also requires the
  creating session identity when present. `killCursors` may omit `lsid`, but
  an included identity must match the creating session.
  Authenticated cursors stay bound to their creating user identity across
  connections.
  Bounded positive and negative
  cases cover the store and wire executor. The 26 request, response and
  effective context fields of this cursor path have a separate conservation
  matrix. The remaining wire
  command fields are still pending.
- Wire `hello` no longer announces client-requested compressors: the proxy
  does not handle `OP_COMPRESSED`. Valid preferences remain connection metadata
  and the response's `compression` field is absent. The existing
  `WireSurface.compression` configuration is accepted without transport effect.
  The three command aliases reject invalid command values and databases before
  recording connection state. A separate matrix pins the bounded `hello`
  request and response fields and explicitly marks absent topology fields.

The full [release checklist](release-checklist.md), including remote CI and
the final installed artifact matrix, remains a gate for any later publication.
This preparation does not create a tag or authorize an upload.
