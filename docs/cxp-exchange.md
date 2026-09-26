# MongoDB compatibility documents in CXP exchange

Mongoeco owns the `org.mongoeco` / `mongodb` catalog and its profile and tier
requirements. They are packaged as JSON data under
`mongoeco.cxp.exchange.data`; a consumer can read and evaluate them without
importing a CXP catalog, descriptor, handshake or registry module.
Install `cxp[exchange]>=5.0.0,<6` to use the document API and context v2.

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
            "configuration_revision": "mongoeco-public-catalog-1.1.0",
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
`metadata_keys` is a `string_set`: `contains_all` preserves the old
`required_metadata_keys` condition. A missing capability or unreported key set
is indeterminate; an explicitly reported set without a required key is
incompatible. No profile is inferred from a tier, and there is no implicit
ordering among tiers.

Catalog version `1.1.0` also defines thirteen typed value properties for
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
claim or a producer-operated contract. Its current status is open: 289 rows
have an exact claim in the generic declaration or an operation alias; nine
have catalog definitions but require a concrete runtime observation; 18
collation behavior and scope rows have Memory/SQLite sync and async oracles,
including an installed-wheel replay; 94 still need owner review or individual
operational oracles. This file covers the packaged
operational metadata only. It does not close the separate inventory of public
APIs, wire commands, BSON behavior, or deployed resources.
