# MongoDB compatibility documents in CXP exchange

Mongoeco owns the `org.mongoeco` / `mongodb` catalog and its profile and tier
requirements. They are packaged as JSON data under
`mongoeco.cxp.exchange.data`; a consumer can read and evaluate them without
importing a CXP catalog, descriptor, handshake or registry module.
Install `cxp[exchange]>=5.0.0,<6` to use the document API and context v2.
Catalog `1.2.0` uses `cxp.catalog` spec_version 2. Snapshot and requirements
remain spec_version 1; context remains spec_version 2. The earlier catalog
`1.1.0` keeps its own identity and hash, and consumers must adopt the new pin
explicitly.

```python
from cxp.exchange import Document, evaluate_requirements
from mongoeco.cxp.exchange import (
    load_mongodb_declared_snapshot,
    load_mongodb_profile,
    mongodb_catalog_store,
)

context = Document(
    {
        "document_type": "cxp.context",
        "spec_version": 2,
        "payload": {
            "subject_id": "mongoeco-public-catalog",
            "configuration_revision": "mongoeco-public-catalog-1.2.0",
            "accepted_sources": ["declared"],
        },
    },
    expected_type="cxp.context",
)
result = evaluate_requirements(
    load_mongodb_declared_snapshot(),
    load_mongodb_profile("mongodb-core"),
    context,
    catalogs=mongodb_catalog_store(),
)
assert result.payload["verdict"] == "compatible"
```

The catalog declares ten MongoDB capabilities and the operation names and
result types from the published generic MongoDB interface. The three tiers are
named `all` requirements over their exact capability sets. The six profiles
also require their operation names and metadata key presence. They include
`mongodb-mock-safe`, which preserves the mock and tooling gate through
operation bindings and required owner-validated metadata keys. Omitting a
required reported key makes that profile incompatible.
`compat.export_mock_safe_profile_catalog()` evaluates this same pinned
document through exchange and has no separate compatibility evaluator. The
property
`metadata_keys` is a `string_set` with a closed domain sourced from Mongoeco's
metadata validators: `contains_all` preserves the old
`required_metadata_keys` condition. A missing capability or unreported key set
is indeterminate; an explicitly reported set without a required key is
incompatible. No profile is inferred from a tier, and there is no implicit
ordering among tiers.

Every catalog, capability, property and operation has an owner source with
reference, revision and locator. The source and any SHA-256 constrain the
catalog's meaning; CXP does not fetch or authenticate that material. String
domains are closed only where the owner has an exact vocabulary; other string
properties declare an open domain. A snapshot or requirement using a token
outside a closed domain is invalid before evaluation, including in an
unselected `any` branch. A valid but unsatisfied requirement is incompatible,
and a missing observation remains indeterminate.

Catalog version `1.2.0` also defines thirteen typed value properties for
`transactions` and `change_streams`. The declared snapshot reports their
exact boolean and string values, including `false` for distributed or
persistent behavior where applicable. A consumer can require an exact value;
an absent property remains indeterminate. The catalog reference in every
profile, tier and snapshot was changed together, so the earlier catalog hash
cannot silently acquire these meanings.

The authored top-level `read`, `write` and `aggregation` values are also
reported as typed properties. This includes query and update operator sets,
aggregation stages and accumulators, and explicit boolean support flags.
Metadata validation happens before projection; omitted fields stay unknown
instead of taking favorable defaults from the owner schema.

The `read` capability additionally defines properties scoped by name to each
of its five published operations (`find.*`, `find_one.*`, and so on). Option
sets, acceptance flags, scope and session or explain support stay tied to the
operation that declares them. Each `resultType` remains represented by that
operation's exact `result_type` binding. A requirement on
`find.supportedOptions` makes no claim about `find_one.supportedOptions`.
Mongoeco validates the known nested read metadata before projecting it; an
unknown operation, field or wrong type rejects. A nested read claim also
requires the same operation's exact result binding; a mismatched or absent
binding rejects before a snapshot is returned.

The eight `write` operations have the same scoped treatment. For example,
`update_one.supportedOptions` includes `sort`, while
`update_many.supportedOptions` does not, and its `acceptsSort` value is
explicitly `false`. An empty option set is a reported negative claim; an
omitted set remains unknown. The owner validator checks each nested write
field's type and operation name, and the snapshot builder requires an exact
result binding before reporting its values.

`aggregation.aggregate` and `vector_search.aggregate` have separate property
namespaces despite sharing an operation name. Their supported stage, operator,
option and scope values follow the owner declaration. In particular, vector
search reports `aggregate.supportsDatabaseScope=false`; omission remains
unknown. The owner validator checks the nested types and the result binding.

Top-level search and vector search scalars and string sets, including the
text-search tier, filter modes, mappings, and backend, are typed catalog
properties. The generic declared snapshot omits vector backend, mode and
filter mode because the selected engine changes them. A snapshot of a
concrete configuration may report their validated values. Narrative notes
and structured nested operator semantics remain owner-operated until their
correlations have an exact exchange mapping.

Persistence and the seven SDAM feature flags have exact boolean properties.
The generic declared snapshot reports the driver-wide SDAM flags, but omits
`persistent`: MemoryEngine and SQLiteEngine do not make the same persistence
claim. `false` in a concrete snapshot is a negative claim; omission stays
unknown. Server count and topology also belong to a selected resource's
observation.

Collation backend and supported capability values are scoped as `backend.*`
and `capabilities.*` properties. The owner validator rejects unknown nested
fields and wrong types. The integer list `supportedStrengths` remains an
operational input-validation contract because exchange v1 has no integer-set
value kind; a string conversion would change its meaning. The generic declared
snapshot omits the four backend availability/selection values and
`capabilities.fallbackBackend`: optional ICU and pyuca installations change
them. A snapshot for an installed environment can report those values.

`search.aggregate` reports its own operation options, accepted no-ops,
leading-stage and scope flags with the exact cursor result binding. Its
structured `stageOptions` remain in Mongoeco's input and execution contract;
the exchange projection does not assert that an opaque value satisfies a
specific nested stage requirement.

The packaged snapshot describes Mongoeco's **declared public library
surface**. Its source is `declared`; it does not prove what a running
deployment has observed or tested. Runtime providers must emit their own
validated snapshot with only the capabilities, bindings and properties they
can substantiate. In particular, copying this declaration into an `observed`
snapshot would misstate provenance. A consumer demanding `observed` or
`tested` receives an indeterminate result from this declared snapshot.

`build_mongodb_snapshot` accepts a `MongoSnapshotIdentity` and explicit
`MongoCapabilityClaim`/`MongoOperationClaim` values. It checks Mongoeco-owned
typed metadata before projecting the keys and supported values actually present,
then asks
`CatalogStore` to validate the complete snapshot. Passing `metadata=None`
omits the key set and leaves metadata requirements indeterminate; it is not
converted to an empty observed set. The caller supplies the source kind and
reference for every homogeneous snapshot. The owner validator rejects unknown
top-level metadata keys and wrong top-level value types for all ten
capabilities. A metadata field whose value is `None` prevents asserting a
complete `metadata_keys` set; the other validated values can still be reported.
For the capabilities above, `None` is omitted as an unknown value and
`false` remains an explicit negative value. Nested metadata for the remaining
capabilities and operations stays governed by Mongoeco's operational contract;
its values are not yet represented as exchange claims.

Structured MongoDB metadata values, telemetry, runtime input validation and
`explain()` projections remain operational contracts of Mongoeco. Cursor
`explain()["cxp"]` now carries the exact exchange catalog reference and
`profileVerdicts` evaluated from Mongoeco's installed declared snapshot with
context v2. The verdicts describe the provider declaration, not a guessed
minimal profile for one query. `operationMetadata` is a separate Mongoeco-owned
description of the exercised operation. Unknown capability names reject before
projection. Telemetry
primitives now live in `mongoeco.telemetry_contract`; the projector and its
operational shape validator live in `mongoeco.driver`. The retired
generic catalog has no operation input or result schemas; its result-type
identities are retained in the exchange operation bindings. Any future
compatibility decision about structured subsets requires explicit portable
properties and an exact catalog version. An opaque extension or a metadata
key alone cannot assert the value of such a subset.

The operational command catalog distinguishes an accepted input from an
effective option. `listCollections.authorizedCollections` is type-checked but
does not filter the local namespace list by authorization. The `validate`
options `scandata`, `full` and `background` are also type-checked and reported
as warnings without changing the validation pass or making it asynchronous.
`listCommands.supportedOptions` lists accepted names; its
`acceptedNoopOptions` identifies these four cases explicitly. A consumer must
not treat their presence in `supportedOptions` as evidence of an effect.
Through the wire proxy, `listCommands.commands` reports all 46 configured,
implemented command names, including nine commands handled only by wire
authentication, cursor, session or transaction handlers. Their entries state
the routing family but do not claim a complete option inventory. A restricted
`WireSurface` yields only its implemented configured names. The embedded
`database.command("listCommands")` reports the 37 database commands because it
does not expose the nine wire-only handlers.
The 71 advertised top-level options across 22 database commands are inventoried
in [`cxp-database-command-option-conservation.csv`](cxp-database-command-option-conservation.csv).
The four accepted no-op options have bounded behavior and wrong-type tests.
Fifty effective options have scoped positive and negative cases: `filter` and
`nameOnly` on both listing commands, plus `batchSize` on `find` and
`aggregate`, `comment` and `maxTimeMS` on `explain`, and the eight remaining
`find` options, eight `count`/`distinct` options, seven admin options, four
remaining `aggregate` options and fifteen write command options.
For `batchSize`, wire limits `firstBatch` and exposes `getMore`;
direct `database.command()` materializes the full result. Direct `find` uses
the option for local prefetch, while direct `aggregate` ignores top-level
`batchSize` and uses `cursor.batchSize` for that purpose. Top-level `explain`
options propagate to supported explained commands unless an inner value is
explicit; invalid outer `maxTimeMS` is rejected. The other 17 `comment` options
retain the command comment in an explicit API session and emit a
`system.profile` event only when profiling is enabled, through API and wire.
All 67 effective options have scoped positive and negative oracles. The inventory
does not turn these operational options into CXP compatibility guarantees.
For `createIndexes`, `supportedOptions` lists only command-level options.
`indexSpecFields` lists the exact accepted fields of each element of
`indexes`, including `key`; `indexSpecAliases` records the accepted Python
spellings. `acceptedNoopIndexSpecFields` identifies `background` and both
spellings of wildcard projection. None of these lists asserts that every
other index field has a proven physical effect. Unknown fields and two
non-null spellings of the same supplied field are rejected before index
creation. The 22 fields have individual conditions and positive/negative
parser cases in
[`cxp-create-indexes-spec-conservation.csv`](cxp-create-indexes-spec-conservation.csv).
This operational command inventory remains separate from the 16 authored
`IndexModel` fields and from deployment compatibility claims.
For the public `create_index` API, `background` and `wildcard_projection` are
also accepted and type-checked without being passed to the engine; their owner
catalog entries now state `accepted-noop`. The same applies when these options
arrive inside an `IndexModel` passed to `create_indexes`. `IndexModel.document`
reflects the authored model, including both options, and is not an observation
of an installed index. The 16 model fields and the exact scope of their current
evidence are recorded in
[`cxp-index-model-conservation.csv`](cxp-index-model-conservation.csv). Index
creation itself remains a local operational contract, separate from a
deployment guarantee.
The accessible Cosecha MongoDB provider reconstructs PyMongo `IndexModel`
instances while restoring index dumps; Mochuelo Q2 also builds PyMongo models
for index reconciliation. Cosecha's installed provider now rejects a snapshot
restore when Mongoeco discards `wildcardProjection`. That check preserves the
snapshot's meaning without claiming the option is effective. Mochuelo's Q2
path still requires its own consumer gate after Q2 admission.

The catalog's identity, version and SHA-256 in every requirement and snapshot
are exact pins. Changing the capability set or semantics requires a new
catalog version and explicitly adopted requirement documents.

The unpublished 4.8.0 source removes the old `mongoeco.cxp` reexports and modules.
`mongoeco.compat.export_exchange_catalog()` embeds the exact owner documents
in its reporting view. Historical source and tests remain in
`evidence/mongoeco-legacy-cxp-python.zip`, outside installed packages. Public
removal requires the coordinated release sequence and installed consumer gates.

The row-level audit of the owner metadata is in
[`cxp-operational-metadata-conservation.csv`](cxp-operational-metadata-conservation.csv).
It pins the source SHA-256 and assigns all 410 logical facts to an exchange
claim or a producer-operated contract. Of those, 289 have exact exchange
claims; 104 owner operational rows have individual positive and negative
oracles; five owner narratives have documented limits; nine catalog definitions
and three owner runtime facts still require concrete deployment observations.
The operational oracles cover collation, persistence and topology inspection,
Search stage options and Search operator semantics, including installed-wheel
replay. This file covers the packaged operational metadata only. It does not
close the separate inventory of public APIs, wire commands, BSON behavior or
deployed resources.

The local wire command inventory is recorded separately in
[`cxp-wire-command-conservation.csv`](cxp-wire-command-conservation.csv).
It lists all 46 advertised command names, their routing kind and family, the
exact source hashes and current evidence. All 46 commands have bounded positive
and negative behavior tests, including client-facing PyMongo paths for the
database commands. It does not yet inventory every argument and result field
of those commands; the `createIndexes.indexes[]` field inventory above closes
one distinct part of that work, and the top-level database-command option
inventory closes another census without proving every claimed effect. For
`hello`, malformed optional
client/compression metadata
is accepted without replacing previously valid connection metadata; it does
not assert support for those malformed values. Client compression preferences
remain connection metadata. The proxy does not handle `OP_COMPRESSED` and
therefore omits `compression` from its `hello` response even when the client
requests a compressor or `WireSurface.compression` is configured. That surface
field is accepted without transport effect in this version. The three `hello`
command spellings now require a command value equal to `1` (including `True`
and `1.0`); a missing or
invalid `$db` is rejected before registering the handshake. The bounded
request and response field contract, including the absent compression and
topology markers, is itemized in
[`cxp-wire-hello-field-conservation.csv`](cxp-wire-hello-field-conservation.csv).
The response reports local proxy limits and a selected MongoDB dialect
compatibility version. Its `gitVersion` value is the literal marker
`mongoeco`, not a source revision, and `isWritablePrimary` does not describe
the state of a physical replica. The operational
routing remains owned by Mongoeco. The inspected PyMongo paths send commands
and consume their responses; the inspected Cosecha, GDT and Mochuelo checkouts
do not compare individual Mongoeco wire command names through exchange. All 46
names therefore remain Mongoeco operational contracts, with no per-command
exchange claim for those consumers. This classification does not establish
that every argument or result field has been conserved. The accessible
repository census does not establish the absence of published external consumers.
Five local wire inspection commands (`ping`, `buildInfo`, `hostInfo`,
`getCmdLineOpts` and `whatsmyuri`) have a separate bounded request/response
inventory in
[`cxp-wire-static-admin-field-conservation.csv`](cxp-wire-static-admin-field-conservation.csv).
`buildInfo.version` describes the selected MongoDB dialect and `gitVersion`
is a literal producer marker. `hostInfo` describes the local process;
`memSizeMB=0` is a placeholder, not measured capacity. `getCmdLineOpts` reports
local arguments and placeholders, not effective deployment configuration.
Over wire, `whatsmyuri.you` now reports the current connection peer instead
of the embedded API's `127.0.0.1:0` placeholder, matching the command's
[current-client meaning](https://www.mongodb.com/docs/manual/reference/command/whatsmyuri/).
These fields remain Mongoeco operational facts, not deployment claims in CXP.
Cursor IDs are scoped to the namespace that produced them. Wire `getMore`
rejects a known ID presented with another database or collection;
`killCursors` reports that ID as unknown in the other namespace and leaves
the original cursor available. Unknown IDs still produce an empty `nextBatch`
for `getMore`; a paginated result without a namespace is rejected before a
cursor ID is issued. The positive and negative owner oracles cover both the cursor
store and the command executor.
When the creating command carries `lsid`, `getMore` must carry the same
session identity. A missing or different `lsid` is rejected without consuming
the cursor; BSON document key order does not change identity. A cursor created
without `lsid` likewise does not acquire a session later. `killCursors` can
omit `lsid`; when it includes one, the identity must match the creating
session or the cursor is reported unknown and retained. Omission is permitted by the
[MongoDB driver sessions specification](https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.md#sessions-and-cursors).
This local correlation does not make a deployment-level exchange claim.
With wire authentication enabled, a cursor also remains bound to its creating
authenticated user identity. A different user cannot read it with `getMore`
or remove it with `killCursors`, while the same user can continue from another
connection. This matches the user coauthorization condition described by the
[MongoDB server authentication design](https://github.com/mongodb/mongo/blob/master/src/mongo/db/auth/README.md).
The 26 request, result and effective context fields in this bounded cursor
path are itemized in
[`cxp-wire-cursor-field-conservation.csv`](cxp-wire-cursor-field-conservation.csv),
including effective database scope, first and subsequent batches, and the
four `killCursors` outcome lists. This does not cover argument or result
fields of the other wire commands.

The static `serverCount: 1` and `topologyType: unknown` values describe the
initial local topology seed, not a Mongo deployment. A `hello` observation can
change both. `storageEngine` is likewise selected at runtime. Admission must
use observations for its actual binding and generation; these three source
values do not authorize a favorable default.

In the owner metadata, `search.stageOptions.facet.previewOnly` describes only
the deprecated 4.x `facetPreview` explain alias. Typed `$searchMeta` facet
collector output remains the canonical operational result. The
`atlasParity: subset` markers on autocomplete, regex and wildcard denote a
local `search-v1` syntax subset and never assert compatibility with an Atlas
deployment. The owner contract and deprecation record are
[`search-contract-v1.md`](architecture/search-contract-v1.md) and
[`deprecations-v1.json`](../src/mongoeco/compat/resources/deprecations-v1.json).
