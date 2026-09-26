# MongoDB compatibility documents in CXP exchange

Mongoeco owns the `org.mongoeco` / `mongodb` catalog and its profile and tier
requirements. They are packaged as JSON data under
`mongoeco.cxp.exchange.data`; a consumer can read and evaluate them without
importing a CXP catalog, descriptor, handshake or registry module.
Install `cxp[exchange]>=4.3.0,<5` to use the document API and context v2.

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

The removal-major source removes the old `mongoeco.cxp` reexports and modules.
`mongoeco.compat.export_exchange_catalog()` embeds the exact owner documents
in its reporting view. Historical source and tests remain in
`evidence/mongoeco-legacy-cxp-python.zip`, outside installed packages. Public
removal requires the coordinated release sequence and installed consumer gates.
