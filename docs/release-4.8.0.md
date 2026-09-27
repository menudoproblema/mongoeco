# Release 4.8.0 — candidate preparation

Status: local source candidate. No tag or package publication has been made.

## Scope

This version replaces Mongoeco's public legacy CXP protocol with exact
`mongoeco.cxp.exchange` documents evaluated by CXP 5.0.0. The removal of
legacy imports and reporting fields is a documented minor-version exception.
The concrete replacements are in [the migration guide](cxp-c4-migration.md).
The project requires `cxp[exchange]>=5.0.0,<6`; CXP 5.0.0 is published.
Mongoeco 4.7.0 is not compatible with CXP 5 despite its published dependency
metadata. The CI constraint now pins CXP 5.0.0, matching this requirement.

The owner catalog is `org.mongoeco:mongodb@1.2.0`, `cxp.catalog` spec_version
2, with exact source references and explicit string domains. Snapshots and
requirements keep their own spec_version 1 and pin the catalog's exact hash.
Operational validation, wire commands, runtime state and telemetry remain
Mongoeco contracts. The [exchange guide](cxp-exchange.md) and its linked
conservation matrices state what is represented, tested or still pending;
catalog option acceptance alone does not establish a deployment guarantee.

## Current local evidence

- The source suite passes: 4,644 passed, 26 skipped, 2,550 subtests passed;
  measured coverage is 99.01% against the 99.00% minimum. `unittest` passes
  3,559 cases and the deep property profile passes four.
- The public API manifest, public typing contract, changed-file Ruff ratchet
  and `git diff --check` pass.
- Fresh constrained pip installations of the candidate wheel on Python 3.13
  and 3.14 resolve published CXP 5.0.0, install the CI test and benchmark
  dependencies, pass `pip check` and import from `site-packages`.
- The latest released artifact smoke checks historical Mongoeco 4.7.0 with
  CXP 4.3.0, the last compatible published CXP major. It does not claim that
  Mongoeco 4.7.0 resolves safely without a CXP upper bound.
- Reproducible wheel and sdist builds of the local candidate pass installed
  smoke tests with CXP 5 and Cosecha. Four isolated Python 3.13/3.14 cells
  each pass 13 consumer smokes, including complete wire `listCommands`.
  The exact source revision, hashes and environment are retained in the local
  release evidence receipt.
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
  contracts for the inspected consumers. Wire `listCommands` now reports all 46
  implemented names, including nine wire-only handlers; the embedded API still
  reports its 37 database commands. Cursor IDs now require the creating
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
- A separate row-level matrix pins the local wire fields of `ping`,
  `buildInfo`, `hostInfo`, `getCmdLineOpts` and `whatsmyuri`. Wire
  `whatsmyuri.you` now uses the current connection peer; the embedded API
  keeps its no-connection placeholder. The other wire command fields remain
  open in the conservation inventory.

The full [release checklist](release-checklist.md), including remote CI and
the final installed artifact matrix, remains a gate for any later publication.
Remote CI and the MongoDB 7.0/8.0 differential on the final source revision
have not yet been accredited. The locally passing gates do not authorize a tag.
This preparation does not create a tag or authorize an upload.
