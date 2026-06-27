# Roadmap 4.0

## Intent

`mongoeco 4.0` should only happen if the project makes its public promise
clearer and materially easier to trust.

The bar should be higher than:

* more subset coverage
* more local operators
* more examples
* more internal cleanup
* exposing more metadata without simplifying the product story

`4.0.0` only makes sense if it makes `mongoeco` clearly read as the most
reliable, auditable and interchangeable local MongoDB mock/runtime for tests,
with CXP used to expose the capabilities it offers in a structured way.

## Product Position

The intended public reading for `4.0` is:

* `mongoeco` is a serious local replacement for lightweight MongoDB mocks;
* it provides a PyMongo-shaped test/runtime surface backed by explicit MongoDB
  and PyMongo compatibility policy;
* it is auditable through deterministic diagnostics, explain output, parity
  tests and capability metadata;
* it is interchangeable for tooling because it can describe its supported
  surface through CXP.

CXP is not the reason `mongoeco` exists. It is the interoperability layer that
lets external tooling understand what this runtime can and cannot do without
reverse-engineering behavior.

`compat` remains part of the direct user language of `mongoeco`: MongoDB
dialects, PyMongo profiles, operation options and local support status should
stay easy to reason about without requiring users to think in CXP terms.

## Core Principles

### 1. Mock/Runtime-First Product

The canonical product promise should be understandable without mentioning CXP:

* local MongoDB-like execution for tests and embedded workflows;
* better semantic fidelity than best-effort mocks;
* explicit compatibility controls through `mongodb_dialect` and
  `pymongo_profile`;
* Memory and SQLite engines with documented parity boundaries;
* failures, diagnostics and explain output that make local behavior auditable.

If a user only wants to know whether `mongoeco` can replace a lightweight mock
in a test suite, the answer should be visible directly from docs, API behavior
and compatibility policy.

### 2. CXP As Capability Exposure

CXP should describe `mongoeco`'s capabilities for tooling and provider-style
interchangeability.

That means CXP should expose:

* interface identity;
* capabilities;
* operations;
* profiles;
* telemetry vocabulary;
* structured capability metadata.

It should not displace the direct MongoDB/PyMongo vocabulary that users already
need when writing tests. CXP is the structured export layer; `compat` is still
the direct local compatibility model.

### 3. Intentional Public Surface

The public API should feel deliberate.

That means:

* fewer ambiguous exports;
* clearer separation between user-facing runtime APIs, compatibility metadata,
  CXP export metadata and internal diagnostics;
* fewer "legacy-looking" names that survive only by inertia;
* no duplicate public stories where CXP, `compat` and docs imply different
  support levels.

If a symbol stays public in `4.0`, it should have a reason tied to one of the
public promises: mock/runtime use, compatibility control, diagnostics or
capability exposure.

### 4. Tooling-Grade Metadata

By `4.0`, external tooling should be able to answer useful questions directly
from public metadata without reverse-engineering behavior:

* which profile is enough for this test?
* which operations are available?
* which read/write options are accepted?
* which aggregation subset is real?
* which search/vector subset is real?
* which telemetry signals are available?

This is one of the main reasons to justify a major version at all. It makes the
runtime interchangeable without making interchangeability the whole product.

### 5. Clean Runtime Boundaries

The runtime should not absorb responsibilities that belong to a separate
platform layer unless there is a strong product reason.

In particular, `4.0` should avoid:

* accidental coupling to orchestration/resource systems;
* platform-owned negotiation or lifecycle concerns in the core;
* duplicating CXP catalog logic when a structured export is enough;
* making tests depend on provider machinery when a direct local client is the
  clearer API.

If a live provider layer becomes useful, it should be bounded and optional.
The core runtime should remain usable directly.

### 6. Stronger Product Definition For Search And Vector Search

`$search` and `$vectorSearch` should read as intentional local test/runtime
products, not as whatever subset happened to accumulate.

That implies:

* a clearly closed local text-search tier;
* a clearly named advanced local subset;
* honest documentation of Atlas-like gaps;
* explain output that teaches the contract instead of just dumping details.

The goal is not full Atlas parity by default.
The goal is a coherent local product surface that is useful in tests and
embedded workflows.

### 7. Major Version Must Reduce Ambiguity

`4.0` should remove ambiguity, not just add more features.

If the release does not make the public story clearer, it probably does not
deserve a major version.

## What Would Justify 4.0

These changes would justify a major release if taken together:

* `mongoeco` is clearly positioned as a reliable, auditable local MongoDB
  mock/runtime for tests;
* `compat` remains the direct MongoDB/PyMongo compatibility language and is
  aligned with, not replaced by, CXP exports;
* CXP capability metadata is strong enough to support real tooling and profile
  gating without guesswork;
* public package structure is simplified around runtime use, compatibility
  control, diagnostics and capability exposure;
* the distinction between internal diagnostics and interoperable telemetry is
  explicit and stable;
* the product narrative for `search` / `vector_search` is materially sharper
  than in `3.x`;
* any breaking changes remove real ambiguity or accidental public surface.

## What Does Not Justify 4.0 By Itself

These are useful, but not enough on their own:

* a few more `$search` operators;
* another local subset feature;
* more coverage;
* more examples;
* internal refactors without contract impact;
* a larger compatibility matrix that still leaves the public story fuzzy;
* making CXP more prominent without improving the direct mock/runtime story.

## Candidate Scope

The likely shape of a `4.0` release would be:

### Public Product Story

* state plainly that `mongoeco` is a local MongoDB mock/runtime for tests and
  embedded workflows;
* document when it is a better fit than a lightweight mock and when a real
  MongoDB server is still required;
* make auditability, deterministic diagnostics and compatibility policy part
  of the first-page story.

### Public Contract

* keep `mongodb_dialect` and `pymongo_profile` as first-class user-facing
  controls;
* tighten `mongoeco.cxp` so it is obviously an interoperability/capability
  export layer;
* document which parts of the surface are runtime API, compatibility metadata,
  CXP export and internal diagnostics.

### Public Metadata

* finish operation-centric metadata where it materially helps tooling;
* keep profiles and profile support first-class;
* make telemetry projection part of the stable public story;
* ensure CXP exports and compat catalogs describe the same support boundaries.

### Search / Vector Product

* declare the local textual tier closed and stable;
* keep advanced local search as a named tier or contract, not as a loose pile
  of features;
* decide how far Atlas-like behavior is actually in scope for the major cycle.

### Legacy Cleanup

* identify public compatibility narratives that should be de-emphasized,
  renamed or removed;
* remove exports or shapes that survive only for transitional reasons;
* provide migration notes for any user-visible cleanup.

## Non-Goals

The following should not become implicit promises of `4.0`:

* full Atlas Search parity;
* full server-grade distributed behavior;
* turning `mongoeco` into a platform orchestrator;
* replacing direct MongoDB/PyMongo compatibility language with CXP-only
  concepts;
* making provider-style integration mandatory for normal tests.

## Release Gates

Before declaring `4.0` real, these conditions should be true:

1. The public product can be explained in a few stable paragraphs without
   caveats or backtracking.
2. A user can understand when to use `mongoeco` instead of a lightweight mock or
   a real MongoDB server.
3. CXP profiles, operations and telemetry are good enough for real external
   tooling.
4. The runtime and the external capability contract have clean boundaries.
5. The remaining legacy surface is small enough that keeping it would create
   more confusion than value.
6. The major release removes ambiguity in at least one visible way, not just by
   adding features.

## Open Decisions

These questions still need explicit answers before `4.0` planning becomes
execution planning:

* What public symbols are runtime API, compatibility metadata, CXP export or
  internal diagnostics?
* Which legacy exports or names create real confusion?
* How far should `compat` remain first-class, and where should it explicitly
  align with CXP?
* What metadata is the minimum viable contract for serious tooling?
* Which search and vector behaviors are product commitments, and which remain
  optional or experimental?
* Are any breaking changes worth the migration cost, or can the same clarity be
  achieved in `3.x`?

## Current Reading

As of now, `3.x` already has much of the groundwork:

* explicit MongoDB dialect and PyMongo profile modeling;
* CXP capability and profile exports;
* operation-level metadata;
* explain projections;
* telemetry projection;
* closed MongoDB catalog tiers;
* strong local runtime and cross-engine parity tests.

That means `4.0` does not need to invent a new direction.
It needs to make the existing direction explicit, simpler and harder to
misread: `mongoeco` as a reliable, auditable and interchangeable local
MongoDB mock/runtime, with CXP exposing what it can do.
