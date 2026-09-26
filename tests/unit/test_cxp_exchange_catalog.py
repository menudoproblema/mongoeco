"""Independent checks for Mongoeco's portable compatibility declarations."""

from copy import deepcopy

import pytest

from cxp.exchange import (
    Document,
    InvalidDocumentError,
    catalog_reference,
    evaluate_requirements_detailed,
)

import mongoeco.cxp as facade

from mongoeco.cxp.exchange import (
    PROFILE_NAMES,
    TIER_NAMES,
    MongoCapabilityClaim,
    MongoOperationClaim,
    MongoSnapshotIdentity,
    build_mongodb_snapshot,
    load_mongodb_catalog,
    load_mongodb_declared_snapshot,
    load_mongodb_profile,
    load_mongodb_tier,
    mongodb_catalog_store,
)
from mongoeco.cxp.exchange.metadata import validate_mongodb_metadata
from mongoeco.cxp.exchange.projection import (
    build_mongodb_exchange_explain_projection,
)


def _context() -> Document:
    return Document(
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


def test_explain_projection_uses_pinned_catalog_and_one_evaluator() -> None:
    projection = build_mongodb_exchange_explain_projection(
        capability="aggregation",
        additional_capabilities=("vector_search",),
    )
    assert projection["catalog"] == catalog_reference(load_mongodb_catalog())
    assert projection["profileVerdicts"] == dict.fromkeys(PROFILE_NAMES, "compatible")
    assert projection["operationName"] == "aggregate"
    assert projection["operationMetadata"]["aggregateStage"] == "$vectorSearch"


def test_explain_projection_rejects_unknown_capability() -> None:
    with pytest.raises(ValueError, match="Unknown MongoDB capability"):
        build_mongodb_exchange_explain_projection(capability="unreported")


def test_legacy_root_exports_are_absent() -> None:
    assert facade.__all__ == ()
    assert not hasattr(facade, "MONGODB_CATALOG")
    assert not hasattr(facade, "export_cxp_capability_catalog")


def _snapshot_with(
    *, missing_capability: str | None = None, missing_metadata: str | None = None
) -> Document:
    content = deepcopy(load_mongodb_declared_snapshot().as_dict())
    if missing_capability:
        content["payload"]["capabilities"] = [
            item
            for item in content["payload"]["capabilities"]
            if item["name"] != missing_capability
        ]
    if missing_metadata:
        for item in content["payload"]["capabilities"]:
            if item["name"] == "aggregation":
                item["properties"]["metadata_keys"].remove(missing_metadata)
    return Document(content, expected_type="cxp.snapshot")


@pytest.mark.parametrize("name", PROFILE_NAMES)
def test_declared_surface_satisfies_every_profile(name: str) -> None:
    result = evaluate_requirements_detailed(
        load_mongodb_declared_snapshot(),
        load_mongodb_profile(name),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "compatible"


def test_mock_safe_profile_requires_its_correlated_claims() -> None:
    missing = deepcopy(load_mongodb_declared_snapshot().as_dict())
    for claim in missing["payload"]["capabilities"]:
        if claim["name"] == "vector_search":
            claim["properties"]["metadata_keys"].remove("explainFeatures")
    snapshot = Document(missing, expected_type="cxp.snapshot")
    result = evaluate_requirements_detailed(
        snapshot,
        load_mongodb_profile("mongodb-mock-safe"),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "incompatible"


@pytest.mark.parametrize("name", TIER_NAMES)
def test_declared_surface_satisfies_every_tier(name: str) -> None:
    result = evaluate_requirements_detailed(
        load_mongodb_declared_snapshot(),
        load_mongodb_tier(name),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "compatible"


def test_missing_capability_is_insufficient_information() -> None:
    result = evaluate_requirements_detailed(
        _snapshot_with(missing_capability="aggregation"),
        load_mongodb_profile("mongodb-core"),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"


def test_declared_missing_metadata_key_fails_profile() -> None:
    result = evaluate_requirements_detailed(
        _snapshot_with(missing_metadata="supportedStages"),
        load_mongodb_profile("mongodb-core"),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "incompatible"


def test_unreported_metadata_keys_are_insufficient_information() -> None:
    content = load_mongodb_declared_snapshot().as_dict()
    for item in content["payload"]["capabilities"]:
        if item["name"] == "aggregation":
            del item["properties"]["metadata_keys"]
    snapshot = Document(content, expected_type="cxp.snapshot")
    result = evaluate_requirements_detailed(
        snapshot,
        load_mongodb_profile("mongodb-core"),
        _context(),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"


def test_catalog_identity_and_hash_are_exact() -> None:
    catalog = load_mongodb_catalog()
    reference = catalog_reference(catalog)
    assert catalog.sha256 == (
        "203873ae07652b6bdf2c8d46f86b3e4cd4d3407a58e1b3c6aa1a46830de39776"
    )
    assert reference == load_mongodb_declared_snapshot().payload["catalog"]
    assert load_mongodb_declared_snapshot().payload["source"]["reference"] == (
        "org.mongoeco:public-catalog:1.1.0"
    )
    assert all(
        reference == load_mongodb_profile(name).payload["catalog"]
        for name in PROFILE_NAMES
    )
    assert all(
        reference == load_mongodb_tier(name).payload["catalog"] for name in TIER_NAMES
    )
    changed = load_mongodb_declared_snapshot().as_dict()
    changed["payload"]["catalog"]["sha256"] = "0" * 64
    with pytest.raises(InvalidDocumentError):
        evaluate_requirements_detailed(
            Document(changed, expected_type="cxp.snapshot"),
            load_mongodb_profile("mongodb-core"),
            _context(),
            catalogs=mongodb_catalog_store(),
        )


def test_runtime_projection_rejects_same_version_with_different_content() -> None:
    changed = deepcopy(load_mongodb_catalog().as_dict())
    changed["payload"]["description"] = "Different owner semantics"
    with pytest.raises(ValueError, match="exact Mongoeco-owned MongoDB catalog"):
        build_mongodb_snapshot(
            catalog=Document(changed, expected_type="cxp.catalog"),
            identity=MongoSnapshotIdentity(
                provider_id="provider-A",
                subject_id="subject-A",
                configuration_revision="revision-A",
                observed_at="2026-09-26T00:00:00Z",
                source_kind="observed",
                source_reference="owner-report-sha256:example",
            ),
            capabilities=(),
        )


def test_declared_source_does_not_satisfy_observed_only_policy() -> None:
    content = _context().as_dict()
    content["payload"]["accepted_sources"] = ["observed"]
    result = evaluate_requirements_detailed(
        load_mongodb_declared_snapshot(),
        load_mongodb_profile("mongodb-core"),
        Document(content, expected_type="cxp.context"),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"
    assert {finding.code for finding in result.findings} == {"source_not_accepted"}


def _provider_snapshot(metadata: dict[str, object] | None) -> Document:
    return build_mongodb_snapshot(
        catalog=load_mongodb_catalog(),
        identity=MongoSnapshotIdentity(
            provider_id="provider-A",
            subject_id="subject-A",
            configuration_revision="revision-A",
            observed_at="2026-09-26T00:00:00Z",
            source_kind="observed",
            source_reference="owner-report-sha256:example",
        ),
        capabilities=(
            MongoCapabilityClaim(
                name="aggregation",
                support="supported",
                metadata=metadata,
                operations=(
                    MongoOperationClaim(
                        name="aggregate",
                        result_type="org.mongoeco:result.cursor:1",
                    ),
                ),
            ),
        ),
    )


def test_runtime_projection_validates_metadata_before_reporting_keys() -> None:
    snapshot = _provider_snapshot(
        {
            "supportedStages": ["$match"],
            "supportedExpressionOperators": ["$add"],
            "supportedGroupAccumulators": ["$sum"],
            "supportedWindowAccumulators": [],
        }
    )
    reported = snapshot.payload["capabilities"][0]["properties"]["metadata_keys"]
    assert reported == [
        "supportedExpressionOperators",
        "supportedGroupAccumulators",
        "supportedStages",
        "supportedWindowAccumulators",
    ]
    with pytest.raises(ValueError, match="Invalid MongoDB aggregation metadata"):
        _provider_snapshot({"supportedStages": [1]})
    with pytest.raises(ValueError, match="Invalid MongoDB aggregation metadata"):
        _provider_snapshot({"supportedExpressionOperators": ["$add"]})


@pytest.mark.parametrize(
    ["capability", "metadata"],
    [
        ("read", {"queryFieldOperators": ["$eq"]}),
        ("write", {"updateOperators": ["$set"]}),
        ("transactions", {"distributed": False}),
        ("change_streams", {"persistent": False}),
        ("aggregation", {"supportedStages": ["$match"]}),
        ("search", {"operators": ["text"]}),
        ("vector_search", {"similarities": ["cosine"]}),
        ("collation", {"backend": {"selectedBackend": "pyuca"}}),
        ("persistence", {"persistent": True, "storageEngine": "sqlite"}),
        ("topology_discovery", {"topologyType": "Single", "serverCount": 1}),
    ],
)
def test_owner_metadata_rejects_unvalidated_keys(
    capability: str, metadata: dict[str, object]
) -> None:
    validate_mongodb_metadata(capability, metadata)
    with pytest.raises(ValueError, match=f"Invalid MongoDB {capability} metadata"):
        validate_mongodb_metadata(capability, {**metadata, "unvalidatedClaim": True})


def test_runtime_projection_omits_unobserved_metadata() -> None:
    snapshot = _provider_snapshot(None)
    assert snapshot.payload["capabilities"][0]["properties"] == {}
    context = Document(
        {
            "document_type": "cxp.context",
            "spec_version": 2,
            "payload": {
                "subject_id": "subject-A",
                "configuration_revision": "revision-A",
                "accepted_sources": ["observed"],
            },
        },
        expected_type="cxp.context",
    )
    result = evaluate_requirements_detailed(
        snapshot,
        load_mongodb_profile("mongodb-aggregate-rich"),
        context,
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"


def test_declared_transaction_and_change_stream_values_are_exact() -> None:
    claims = {
        item["name"]: item["properties"]
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
    }
    assert {
        key: value
        for key, value in claims["transactions"].items()
        if key != "metadata_keys"
    } == {
        "async": True,
        "distributed": False,
        "embedded": True,
        "mode": "local",
        "sync": True,
    }
    assert {
        key: value
        for key, value in claims["change_streams"].items()
        if key != "metadata_keys"
    } == {
        "boundedHistory": True,
        "distributed": False,
        "implementation": "local",
        "persistent": False,
        "resumable": True,
        "resumableAcrossClientRestarts": False,
        "resumableAcrossNodes": False,
        "resumableAcrossProcesses": False,
    }


def test_find_options_are_scoped_to_the_find_operation() -> None:
    read = next(
        item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
        if item["name"] == "read"
    )
    assert read["properties"]["find.supportedOptions"] == [
        "batch_size",
        "comment",
        "hint",
        "let",
        "max_time_ms",
    ]
    assert read["properties"]["find.acceptsHint"] is True
    assert read["properties"]["find.unsupportedOptions"] == []
    assert read["operations"][0] == {
        "name": "find",
        "result_type": "org.mongoeco:result.cursor:1",
    }
    assert "find_one.supportedOptions" not in read["properties"]

    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "find-hint-support",
        "operator": "contains_all",
        "capability": "read",
        "operations": ["find"],
        "path": "/properties/find.supportedOptions",
        "values": ["hint"],
        "require_effective": True,
        "extensions": {},
        "critical_extensions": [],
    }
    requirement = Document(requirement_content, expected_type="cxp.requirements")
    snapshot_content = load_mongodb_declared_snapshot().as_dict()

    def verdict() -> str:
        return evaluate_requirements_detailed(
            Document(snapshot_content, expected_type="cxp.snapshot"),
            requirement,
            _context(),
            catalogs=mongodb_catalog_store(),
        ).verdict

    assert verdict() == "compatible"
    for item in snapshot_content["payload"]["capabilities"]:
        if item["name"] == "read":
            item["properties"]["find.supportedOptions"].remove("hint")
    assert verdict() == "incompatible"
    for item in snapshot_content["payload"]["capabilities"]:
        if item["name"] == "read":
            del item["properties"]["find.supportedOptions"]
    assert verdict() == "indeterminate"


def test_find_runtime_projection_validates_nested_values() -> None:
    identity = MongoSnapshotIdentity(
        provider_id="provider-A",
        subject_id="subject-A",
        configuration_revision="revision-A",
        observed_at="2026-09-26T00:00:00Z",
        source_kind="observed",
        source_reference="owner-report-sha256:example",
    )

    def snapshot_for(find: dict[str, object]) -> Document:
        return build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=identity,
            capabilities=(
                MongoCapabilityClaim(
                    name="read",
                    support="supported",
                    metadata={"operationMetadata": {"find": find}},
                    operations=(
                        MongoOperationClaim(
                            name="find",
                            result_type="org.mongoeco:result.cursor:1",
                        ),
                    ),
                ),
            ),
        )

    properties = snapshot_for(
        {"acceptsHint": False, "supportedOptions": [], "supportsSession": None}
    ).payload["capabilities"][0]["properties"]
    assert properties["find.acceptsHint"] is False
    assert properties["find.supportedOptions"] == []
    assert "find.supportsSession" not in properties
    with pytest.raises(ValueError, match="Invalid MongoDB read metadata"):
        snapshot_for({"acceptsHint": "false"})
    with pytest.raises(ValueError, match="Invalid MongoDB read metadata"):
        snapshot_for({"unknownOption": True})


def test_value_requirement_distinguishes_false_from_missing() -> None:
    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "change-stream-persistence",
        "operator": "equals",
        "capability": "change_streams",
        "path": "/properties/persistent",
        "value": False,
        "require_effective": True,
        "extensions": {},
        "critical_extensions": [],
    }
    requirement = Document(requirement_content, expected_type="cxp.requirements")
    declared = load_mongodb_declared_snapshot().as_dict()

    def verdict(content: dict[str, object]) -> str:
        snapshot = Document(content, expected_type="cxp.snapshot")
        return evaluate_requirements_detailed(
            snapshot,
            requirement,
            _context(),
            catalogs=mongodb_catalog_store(),
        ).verdict

    assert verdict(declared) == "compatible"
    for claim in declared["payload"]["capabilities"]:
        if claim["name"] == "change_streams":
            claim["properties"]["persistent"] = True
    assert verdict(declared) == "incompatible"
    for claim in declared["payload"]["capabilities"]:
        if claim["name"] == "change_streams":
            del claim["properties"]["persistent"]
    assert verdict(declared) == "indeterminate"


def test_runtime_projection_keeps_false_and_omits_unreported_value() -> None:
    snapshot = build_mongodb_snapshot(
        catalog=load_mongodb_catalog(),
        identity=MongoSnapshotIdentity(
            provider_id="provider-A",
            subject_id="subject-A",
            configuration_revision="revision-A",
            observed_at="2026-09-26T00:00:00Z",
            source_kind="observed",
            source_reference="owner-report-sha256:example",
        ),
        capabilities=(
            MongoCapabilityClaim(
                name="transactions",
                support="supported",
                metadata={"distributed": False, "mode": "local", "sync": None},
                operations=(),
            ),
        ),
    )
    properties = snapshot.payload["capabilities"][0]["properties"]
    assert properties == {
        "distributed": False,
        "mode": "local",
    }
    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "transaction-sync-metadata",
        "operator": "contains_all",
        "capability": "transactions",
        "path": "/properties/metadata_keys",
        "values": ["sync"],
        "require_effective": True,
        "extensions": {},
        "critical_extensions": [],
    }
    context_content = _context().as_dict()
    context_content["payload"] = {
        "subject_id": "subject-A",
        "configuration_revision": "revision-A",
        "accepted_sources": ["observed"],
    }
    result = evaluate_requirements_detailed(
        snapshot,
        Document(requirement_content, expected_type="cxp.requirements"),
        Document(context_content, expected_type="cxp.context"),
        catalogs=mongodb_catalog_store(),
    )
    assert result.verdict == "indeterminate"
    with pytest.raises(ValueError, match="Invalid MongoDB transactions metadata"):
        build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=MongoSnapshotIdentity(
                provider_id="provider-A",
                subject_id="subject-A",
                configuration_revision="revision-A",
                observed_at="2026-09-26T00:00:00Z",
                source_kind="observed",
                source_reference="owner-report-sha256:example",
            ),
            capabilities=(
                MongoCapabilityClaim(
                    name="transactions",
                    support="supported",
                    metadata={"distributed": "false"},
                    operations=(),
                ),
            ),
        )


def test_runtime_projection_rejects_unsupported_operation_result() -> None:
    with pytest.raises(InvalidDocumentError):
        build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=MongoSnapshotIdentity(
                provider_id="provider-A",
                subject_id="subject-A",
                configuration_revision="revision-A",
                observed_at="2026-09-26T00:00:00Z",
                source_kind="observed",
                source_reference="owner-report-sha256:example",
            ),
            capabilities=(
                MongoCapabilityClaim(
                    name="aggregation",
                    support="supported",
                    metadata={"supportedStages": ["$match"]},
                    operations=(
                        MongoOperationClaim(
                            name="aggregate",
                            result_type="org.mongoeco:result.wrong:1",
                        ),
                    ),
                ),
            ),
        )
