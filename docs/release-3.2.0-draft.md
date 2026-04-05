# Release Draft 3.2.0

Status: prepared locally for the `3.2.0` cut. Publication to PyPI remains
deferred.

## Headline

`mongoeco 3.2.0` makes the embedded/local runtime easier to understand and
adopt, and now exposes its public capability story through the canonical CXP
`database/mongodb` model.

## Narrative

`mongoeco` is not trying to become a distributed MongoDB replacement.

This release instead makes the product more legible in the places where it is
already strongest:

* embedded `read`, `write` and `aggregation`;
* local `search` and `vector_search`;
* platform capabilities such as `persistence`, `collation`,
  `change_streams` and `topology_discovery`;
* explicit public diagnostics through `compat`, `explain()` and CXP.

## Public capability story

The public contract is now easier to describe in canonical terms:

### Core

* `read`
* `write`
* `aggregation`

### Search

* `search`
* `vector_search`

### Platform

* `transactions`
* `change_streams`
* `collation`
* `persistence`
* `topology_discovery`

## Main user-facing changes

### CXP-backed public compatibility model

* `mongoeco` now aligns its public contract with the canonical CXP
  `database/mongodb` interface.
* the public compatibility export now includes a top-level `cxp` section.
* the local `database/mongodb` catalog now also canonizes first-level public
  operations such as `find`, `insert_one`, `update_one`, `aggregate`, `watch`
  and `with_transaction`, and capability snapshots publish those bindings
  directly.
* legacy runtime subsets are now a projection from the canonical CXP
  capability model instead of a separate source of truth.
* `find(...).explain()` and `aggregate(...).explain()` now expose a top-level
  `cxp` block describing the public capability path being exercised.
* `aggregation` now exposes structured subset metadata for the real local
  support surface, including supported stages, expression operators and
  accumulators.

### More teachable local search

The local `$search` subset is now much easier to demonstrate in real examples:

* `phrase`
* `phrase.slop`
* `in`
* `equals`
* `range`
* `regex`
* `exists`
* `compound`

### More visible local vector search

The local `$vectorSearch` surface now makes its knobs and diagnostics easier
to consume publicly:

* `similarity`
* `numCandidates`
* `minScore`
* projected `vectorSearchScore`
* residual filter diagnostics
* exact fallback diagnostics

### Stronger examples and adoption docs

`mongoeco` now ships with clearer executable examples and case-oriented docs:

* embedded app
* test runtime
* local search and retrieval
* CXP-first contract alignment

## Limits that remain explicit

This release does not change the product stance:

* no claim of full MongoDB server compatibility;
* no claim of full Atlas Search compatibility;
* no distributed topology/runtime semantics beyond the documented local SDAM
  subset;
* engine-specific local behavior stays visible as metadata, not promoted to
  fake canonical capabilities.

## Suggested announcement shape

Short version:

> `mongoeco 3.2.0` turns the embedded/local Mongo-like runtime into a clearer
> product story: canonical CXP capability model for `database/mongodb`,
> stronger public `compat`/`explain()` projections, and more teachable local
> `search` / `vector_search` for apps, tests and demos.
