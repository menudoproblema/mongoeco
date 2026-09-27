"""Independent checks for Mongoeco's portable compatibility declarations."""

import hashlib
import json

from copy import deepcopy
from importlib.resources import files

import msgspec
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
from mongoeco.cxp.exchange.metadata import METADATA_SCHEMAS, validate_mongodb_metadata
from mongoeco.cxp.exchange.projection import (
    build_mongodb_exchange_explain_projection,
)


CATALOG_SPEC_VERSION = 2


def test_unknown_profile_and_tier_names_never_fall_back() -> None:
    with pytest.raises(ValueError, match="Unknown MongoDB profile"):
        load_mongodb_profile("mongodb-other")
    with pytest.raises(ValueError, match="Unknown MongoDB tier"):
        load_mongodb_tier("premium")


def _context() -> Document:
    return Document(
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
        "84c69da54a8d09f6dce9a3cbc394cf8c2f251402c72e18b3f7488af86ef08aeb"
    )
    assert reference == load_mongodb_declared_snapshot().payload["catalog"]
    assert load_mongodb_declared_snapshot().payload["source"]["reference"] == (
        "org.mongoeco:public-catalog:1.2.0"
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


@pytest.mark.parametrize("mutation", ["identity", "content"])
def test_snapshot_builder_rejects_foreign_or_changed_catalog(mutation: str) -> None:
    content = load_mongodb_catalog().as_dict()
    if mutation == "identity":
        content["payload"]["identity"]["name"] = "different"
        expected = "Mongoeco-owned MongoDB catalog"
    else:
        content["payload"]["description"] = "different catalog bytes"
        expected = "exact Mongoeco-owned MongoDB catalog"
    catalog = Document(content, expected_type="cxp.catalog")
    identity = MongoSnapshotIdentity(
        provider_id="provider-A",
        subject_id="subject-A",
        configuration_revision="revision-A",
        observed_at="2026-09-26T00:00:00Z",
        source_kind="observed",
        source_reference="owner-report-sha256:example",
    )
    with pytest.raises(ValueError, match=expected):
        build_mongodb_snapshot(catalog=catalog, identity=identity, capabilities=())


def test_catalog_v2_provenance_and_metadata_vocabularies_match_owner() -> None:
    catalog = load_mongodb_catalog()
    assert catalog.spec_version == CATALOG_SPEC_VERSION
    owner_path = files("mongoeco.cxp.exchange").joinpath(
        "data/operational-metadata.json"
    )
    metadata_path = files("mongoeco.cxp.exchange").joinpath("metadata.py")
    owner = json.loads(owner_path.read_text(encoding="utf-8"))
    expected_hashes = {
        "src/mongoeco/cxp/exchange/data/operational-metadata.json": hashlib.sha256(
            owner_path.read_bytes()
        ).hexdigest(),
        "src/mongoeco/cxp/exchange/metadata.py": hashlib.sha256(
            metadata_path.read_bytes()
        ).hexdigest(),
    }

    def check_source(source: dict[str, object]) -> None:
        reference = source["reference"]
        if reference in expected_hashes:
            assert source["sha256"] == expected_hashes[reference]
            if reference.endswith("operational-metadata.json"):
                value: object = owner
                for token in str(source["locator"]).strip("/").split("/"):
                    if token:
                        assert isinstance(value, dict)
                        value = value[token]
        else:
            assert reference == "src/mongoeco/cxp/exchange/data/catalog.json"
            assert source["scope"] == (
                "Existing owner operation and result contract in catalog v1.1.0"
            )

    check_source(catalog.payload["source"])
    for capability in catalog.payload["capabilities"]:
        name = capability["name"]
        check_source(capability["source"])
        for definition in capability["properties"].values():
            check_source(definition["source"])
        for operation in capability["operations"]:
            check_source(operation["source"])
        keys = capability["properties"]["metadata_keys"]["domain"]
        assert keys == {
            "mode": "closed",
            "values": sorted(
                field.encode_name
                for field in msgspec.structs.fields(METADATA_SCHEMAS[name])
            ),
        }


def test_v2_rejects_unknown_key_even_outside_required_capabilities() -> None:
    content = load_mongodb_declared_snapshot().as_dict()
    collation = next(
        item for item in content["payload"]["capabilities"]
        if item["name"] == "collation"
    )
    collation["properties"]["metadata_keys"].append("inventedFlag")
    with pytest.raises(InvalidDocumentError, match="outside the closed domain"):
        evaluate_requirements_detailed(
            Document(content, expected_type="cxp.snapshot"),
            load_mongodb_profile("mongodb-core"),
            _context(),
            catalogs=mongodb_catalog_store(),
        )


def test_v2_rejects_invalid_requirement_in_otherwise_satisfied_any() -> None:
    content = load_mongodb_profile("mongodb-core").as_dict()
    content["payload"]["requirement"]["operator"] = "any"
    content["payload"]["requirement"]["conditions"] = [
        content["payload"]["requirement"]["conditions"][0],
        {
            "id": "unknown-metadata-key",
            "operator": "contains_all",
            "capability": "collation",
            "path": "/properties/metadata_keys",
            "values": ["inventedFlag"],
            "require_effective": True,
            "extensions": {},
            "critical_extensions": [],
        },
    ]
    with pytest.raises(InvalidDocumentError, match="outside the closed domain"):
        evaluate_requirements_detailed(
            load_mongodb_declared_snapshot(),
            Document(content, expected_type="cxp.requirements"),
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


def test_owner_metadata_rejects_non_string_keys_before_projection() -> None:
    with pytest.raises(ValueError, match="string-keyed object"):
        validate_mongodb_metadata("read", {1: "untrusted"})  # type: ignore[dict-item]


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
    assert read["properties"]["find_one.supportedOptions"] == []
    assert read["properties"]["find_one.acceptsHint"] is False

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


def test_write_options_are_scoped_to_their_operation() -> None:
    write = next(
        item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
        if item["name"] == "write"
    )
    assert "sort" in write["properties"]["update_one.supportedOptions"]
    assert "sort" not in write["properties"]["update_many.supportedOptions"]
    assert write["properties"]["update_many.acceptsSort"] is False
    assert write["properties"]["insert_one.supportedOptions"] == []

    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "update-sort-support",
        "operator": "contains_all",
        "capability": "write",
        "operations": ["update_one"],
        "path": "/properties/update_one.supportedOptions",
        "values": ["sort"],
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
        if item["name"] == "write":
            item["properties"]["update_one.supportedOptions"].remove("sort")
    assert verdict() == "incompatible"
    for item in snapshot_content["payload"]["capabilities"]:
        if item["name"] == "write":
            del item["properties"]["update_one.supportedOptions"]
    assert verdict() == "indeterminate"


@pytest.mark.parametrize(
    "capability", ["read", "write", "aggregation", "vector_search"]
)
def test_all_projected_operation_facts_match_owner_declaration(
    capability: str,
) -> None:
    owner = json.loads(
        files("mongoeco.cxp.exchange")
        .joinpath("data/operational-metadata.json")
        .read_text(encoding="utf-8")
    )
    claim = next(
        item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
        if item["name"] == capability
    )
    bindings = {item["name"]: item["result_type"] for item in claim["operations"]}
    for operation, facts in owner[capability]["operationMetadata"].items():
        assert bindings[operation] == f"org.mongoeco:result.{facts['resultType']}:1"
        for field, value in facts.items():
            if field != "resultType":
                reported = claim["properties"][f"{operation}.{field}"]
                if isinstance(value, list):
                    assert len(value) == len(set(value))
                    assert reported == sorted(value)
                else:
                    assert reported == value


@pytest.mark.parametrize(
    ["capability", "fields"],
    [
        (
            "read",
            (
                "async",
                "embedded",
                "queryFieldOperators",
                "queryTopLevelOperators",
                "sync",
            ),
        ),
        (
            "write",
            ("async", "embedded", "supportsPipelineUpdate", "sync", "updateOperators"),
        ),
        (
            "aggregation",
            (
                "async",
                "embedded",
                "explainable",
                "supportedExpressionOperators",
                "supportedGroupAccumulators",
                "supportedStages",
                "supportedWindowAccumulators",
                "sync",
            ),
        ),
        (
            "search",
            (
                "advancedAtlasLikeGaps",
                "aggregateStage",
                "exactFilterFieldMappings",
                "explainFeatures",
                "fieldMappings",
                "operators",
                "sqliteBackends",
                "structuredFieldMappings",
                "structuredParentPathOperators",
                "textSearchTier",
                "textualFieldMappings",
            ),
        ),
        (
            "vector_search",
            (
                "aggregateStage",
                "explainFeatures",
                "fallback",
                "hybridFilterModes",
                "similarities",
            ),
        ),
    ],
)
def test_io_and_aggregation_values_match_owner_declaration(
    capability: str, fields: tuple[str, ...]
) -> None:
    owner = json.loads(
        files("mongoeco.cxp.exchange")
        .joinpath("data/operational-metadata.json")
        .read_text(encoding="utf-8")
    )
    claim = next(
        item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
        if item["name"] == capability
    )
    definitions = next(
        item
        for item in load_mongodb_catalog().payload["capabilities"]
        if item["name"] == capability
    )["properties"]
    for field in fields:
        value = owner[capability][field]
        if isinstance(value, list):
            assert len(value) == len(set(value))
            assert claim["properties"][field] == sorted(value)
        else:
            assert claim["properties"][field] == value
        assert field in definitions


def test_search_tier_value_distinguishes_missing_and_out_of_domain() -> None:
    requirement_content = load_mongodb_profile("mongodb-search").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "closed-local-search-tier",
        "operator": "equals",
        "capability": "search",
        "path": "/properties/textSearchTier",
        "value": "closed-local-tier",
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
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "search":
            claim["properties"]["textSearchTier"] = "other-tier"
    with pytest.raises(InvalidDocumentError, match="outside the closed domain"):
        verdict()
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "search":
            del claim["properties"]["textSearchTier"]
    assert verdict() == "indeterminate"


def test_sdam_and_persistence_flags_match_owner_and_omit_unknown() -> None:
    owner = json.loads(
        files("mongoeco.cxp.exchange")
        .joinpath("data/operational-metadata.json")
        .read_text(encoding="utf-8")
    )
    claims = {
        item["name"]: item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
    }
    assert "persistent" not in claims["persistence"]["properties"]
    assert "persistent" in next(
        item
        for item in load_mongodb_catalog().payload["capabilities"]
        if item["name"] == "persistence"
    )["properties"]
    for field, value in owner["topology_discovery"]["sdam"].items():
        assert claims["topology_discovery"]["properties"][f"sdam.{field}"] is value
    assert "topologyType" not in claims["topology_discovery"]["properties"]
    assert "serverCount" not in claims["topology_discovery"]["properties"]
    for field in ("backend", "mode", "filterMode"):
        assert field not in claims["vector_search"]["properties"]

    identity = MongoSnapshotIdentity(
        provider_id="provider-A",
        subject_id="subject-A",
        configuration_revision="revision-A",
        observed_at="2026-09-26T00:00:00Z",
        source_kind="observed",
        source_reference="owner-report-sha256:example",
    )

    def snapshot_for(sdam: dict[str, object]) -> Document:
        return build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=identity,
            capabilities=(
                MongoCapabilityClaim(
                    name="topology_discovery",
                    support="supported",
                    metadata={
                        "topologyType": "single",
                        "serverCount": 1,
                        "sdam": sdam,
                    },
                    operations=(),
                ),
            ),
        )

    properties = snapshot_for({"fullSdam": False, "topologyVersionAware": None})
    reported = properties.payload["capabilities"][0]["properties"]
    assert reported["sdam.fullSdam"] is False
    assert "sdam.topologyVersionAware" not in reported
    with pytest.raises(ValueError, match="Invalid MongoDB topology_discovery metadata"):
        snapshot_for({"fullSdam": "false"})
    with pytest.raises(ValueError, match="Invalid MongoDB topology_discovery metadata"):
        snapshot_for({"unknownFlag": True})

    persistence = build_mongodb_snapshot(
        catalog=load_mongodb_catalog(),
        identity=identity,
        capabilities=(
            MongoCapabilityClaim(
                name="persistence",
                support="supported",
                metadata={"persistent": False, "storageEngine": "memory"},
                operations=(),
            ),
        ),
    )
    assert persistence.payload["capabilities"][0]["properties"]["persistent"] is False

    vector = build_mongodb_snapshot(
        catalog=load_mongodb_catalog(),
        identity=identity,
        capabilities=(
            MongoCapabilityClaim(
                name="vector_search",
                support="supported",
                metadata={
                    "similarities": ["cosine"],
                    "backend": "python",
                    "mode": "exact",
                    "filterMode": "post-candidate",
                },
                operations=(),
            ),
        ),
    )
    properties = vector.payload["capabilities"][0]["properties"]
    assert properties["backend"] == "python"
    assert properties["mode"] == "exact"
    assert properties["filterMode"] == "post-candidate"


def test_sdam_false_requirement_has_three_distinct_outcomes() -> None:
    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "no-full-sdam",
        "operator": "equals",
        "capability": "topology_discovery",
        "path": "/properties/sdam.fullSdam",
        "value": False,
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
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "topology_discovery":
            claim["properties"]["sdam.fullSdam"] = True
    assert verdict() == "incompatible"
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "topology_discovery":
            del claim["properties"]["sdam.fullSdam"]
    assert verdict() == "indeterminate"


def test_persistence_requires_a_concrete_configuration() -> None:
    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "persistent-engine",
        "operator": "equals",
        "capability": "persistence",
        "path": "/properties/persistent",
        "value": True,
        "require_effective": True,
        "extensions": {},
        "critical_extensions": [],
    }
    requirement = Document(requirement_content, expected_type="cxp.requirements")
    assert evaluate_requirements_detailed(
        load_mongodb_declared_snapshot(),
        requirement,
        _context(),
        catalogs=mongodb_catalog_store(),
    ).verdict == "indeterminate"

    identity = MongoSnapshotIdentity(
        provider_id="provider-A",
        subject_id="subject-A",
        configuration_revision="revision-A",
        observed_at="2026-09-26T00:00:00Z",
        source_kind="observed",
        source_reference="owner-report-sha256:example",
    )
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

    def verdict(*, persistent: bool) -> str:
        snapshot = build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=identity,
            capabilities=(
                MongoCapabilityClaim(
                    name="persistence",
                    support="supported",
                    metadata={
                        "persistent": persistent,
                        "storageEngine": "configured-engine",
                    },
                    operations=(),
                ),
            ),
        )
        return evaluate_requirements_detailed(
            snapshot, requirement, context, catalogs=mongodb_catalog_store()
        ).verdict

    assert verdict(persistent=False) == "incompatible"
    assert verdict(persistent=True) == "compatible"


def test_collation_facts_keep_backend_and_capability_scope() -> None:
    owner = json.loads(
        files("mongoeco.cxp.exchange")
        .joinpath("data/operational-metadata.json")
        .read_text(encoding="utf-8")
    )
    claim = next(
        item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
        if item["name"] == "collation"
    )
    for scope, fields in {
        "backend": (
            "advancedOptionsAvailable",
            "availableBackends",
            "selectedBackend",
            "unicodeAvailable",
        ),
        "capabilities": (
            "advancedOptionsRequireIcu",
            "fallbackBackend",
            "optionalIcuBackend",
            "supportedLocales",
            "supportsCaseLevel",
            "supportsNumericOrdering",
        ),
    }.items():
        for field in fields:
            value = owner["collation"][scope][field]
            property_name = f"{scope}.{field}"
            if scope == "backend" or field == "fallbackBackend":
                assert property_name not in claim["properties"]
                continue
            reported = claim["properties"][property_name]
            if isinstance(value, list):
                assert reported == sorted(value)
            else:
                assert reported == value
    assert "capabilities.supportedStrengths" not in claim["properties"]

    identity = MongoSnapshotIdentity(
        provider_id="provider-A",
        subject_id="subject-A",
        configuration_revision="revision-A",
        observed_at="2026-09-26T00:00:00Z",
        source_kind="observed",
        source_reference="owner-report-sha256:example",
    )

    def snapshot_for(metadata: dict[str, object]) -> Document:
        return build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=identity,
            capabilities=(
                MongoCapabilityClaim(
                    name="collation",
                    support="supported",
                    metadata=metadata,
                    operations=(),
                ),
            ),
        )

    projected = snapshot_for(
        {"backend": {"advancedOptionsAvailable": False}, "capabilities": {}}
    ).payload["capabilities"][0]["properties"]
    assert projected["backend.advancedOptionsAvailable"] is False
    assert "capabilities.supportedLocales" not in projected
    with pytest.raises(ValueError, match="Invalid MongoDB collation metadata"):
        snapshot_for({"backend": {"selectedBackend": 42}})
    with pytest.raises(ValueError, match="Invalid MongoDB collation metadata"):
        snapshot_for({"capabilities": {"unknownFlag": True}})


def test_collation_support_aliases_match_exact_io_operations() -> None:
    owner = json.loads(
        files("mongoeco.cxp.exchange")
        .joinpath("data/operational-metadata.json")
        .read_text(encoding="utf-8")
    )
    capabilities = {
        item["name"]: item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
    }
    for operation, facts in owner["collation"]["operationMetadata"].items():
        if operation == "serverStatus":
            continue
        capability = (
            "read" if operation in owner["read"]["operationMetadata"] else "write"
        )
        expected = facts["supportsCollation"]
        assert (
            owner[capability]["operationMetadata"][operation]["acceptsCollation"]
            is expected
        )
        assert (
            capabilities[capability]["properties"][f"{operation}.acceptsCollation"]
            is expected
        )


def test_collation_backend_availability_has_three_outcomes() -> None:
    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "advanced-collation",
        "operator": "equals",
        "capability": "collation",
        "path": "/properties/backend.advancedOptionsAvailable",
        "value": True,
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

    assert verdict() == "indeterminate"
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "collation":
            claim["properties"]["backend.advancedOptionsAvailable"] = False
    assert verdict() == "incompatible"
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "collation":
            claim["properties"]["backend.advancedOptionsAvailable"] = True
    assert verdict() == "compatible"
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "collation":
            del claim["properties"]["backend.advancedOptionsAvailable"]
    assert verdict() == "indeterminate"


def test_search_aggregate_claims_keep_result_and_scope() -> None:
    owner = json.loads(
        files("mongoeco.cxp.exchange")
        .joinpath("data/operational-metadata.json")
        .read_text(encoding="utf-8")
    )["search"]["operationMetadata"]["aggregate"]
    claim = next(
        item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
        if item["name"] == "search"
    )
    assert claim["operations"] == [
        {"name": "aggregate", "result_type": "org.mongoeco:result.cursor:1"}
    ]
    for field, value in owner.items():
        if field in {"resultType", "stageOptions"}:
            continue
        reported = claim["properties"][f"aggregate.{field}"]
        if isinstance(value, list):
            assert reported == sorted(value)
        else:
            assert reported == value
    assert "aggregate.stageOptions" not in claim["properties"]

    requirement_content = load_mongodb_profile("mongodb-search").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "search-database-scope",
        "operator": "equals",
        "capability": "search",
        "operations": ["aggregate"],
        "path": "/properties/aggregate.supportsDatabaseScope",
        "value": True,
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

    assert verdict() == "incompatible"
    for item in snapshot_content["payload"]["capabilities"]:
        if item["name"] == "search":
            item["properties"]["aggregate.supportsDatabaseScope"] = True
    assert verdict() == "compatible"
    for item in snapshot_content["payload"]["capabilities"]:
        if item["name"] == "search":
            del item["properties"]["aggregate.supportsDatabaseScope"]
    assert verdict() == "indeterminate"

    with pytest.raises(ValueError, match="Invalid MongoDB search metadata"):
        validate_mongodb_metadata(
            "search",
            {
                "operators": [],
                "operationMetadata": {"aggregate": {"supportsDatabaseScope": "false"}},
            },
        )


def test_read_operator_set_distinguishes_missing_and_excluded() -> None:
    requirement_content = load_mongodb_profile("mongodb-core").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "read-jsonschema-query",
        "operator": "contains_all",
        "capability": "read",
        "path": "/properties/queryTopLevelOperators",
        "values": ["$jsonSchema"],
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
            item["properties"]["queryTopLevelOperators"].remove("$jsonSchema")
    assert verdict() == "incompatible"
    for item in snapshot_content["payload"]["capabilities"]:
        if item["name"] == "read":
            del item["properties"]["queryTopLevelOperators"]
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

    def snapshot_for(
        find: dict[str, object], *, binding: str | None = "cursor"
    ) -> Document:
        return build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=identity,
            capabilities=(
                MongoCapabilityClaim(
                    name="read",
                    support="supported",
                    metadata={"operationMetadata": {"find": find}},
                    operations=(
                        (
                            MongoOperationClaim(
                                name="find",
                                result_type=f"org.mongoeco:result.{binding}:1",
                            ),
                        )
                        if binding is not None
                        else ()
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
    with pytest.raises(ValueError, match="lacks its exact binding"):
        snapshot_for({"acceptsHint": True}, binding=None)
    with pytest.raises(ValueError, match="result differs from its binding"):
        snapshot_for({"resultType": "document"})


def test_write_runtime_projection_validates_nested_values() -> None:
    identity = MongoSnapshotIdentity(
        provider_id="provider-A",
        subject_id="subject-A",
        configuration_revision="revision-A",
        observed_at="2026-09-26T00:00:00Z",
        source_kind="observed",
        source_reference="owner-report-sha256:example",
    )

    def snapshot_for(
        update_one: dict[str, object], *, binding: str | None = "update_result"
    ) -> Document:
        return build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=identity,
            capabilities=(
                MongoCapabilityClaim(
                    name="write",
                    support="supported",
                    metadata={"operationMetadata": {"update_one": update_one}},
                    operations=(
                        (
                            MongoOperationClaim(
                                name="update_one",
                                result_type=f"org.mongoeco:result.{binding}:1",
                            ),
                        )
                        if binding is not None
                        else ()
                    ),
                ),
            ),
        )

    properties = snapshot_for(
        {"acceptsSort": False, "supportedOptions": [], "supportsUpsert": None}
    ).payload["capabilities"][0]["properties"]
    assert properties["update_one.acceptsSort"] is False
    assert properties["update_one.supportedOptions"] == []
    assert "update_one.supportsUpsert" not in properties
    with pytest.raises(ValueError, match="Invalid MongoDB write metadata"):
        snapshot_for({"acceptsSort": "false"})
    with pytest.raises(ValueError, match="Invalid MongoDB write metadata"):
        snapshot_for({"unknownOption": True})
    with pytest.raises(ValueError, match="lacks its exact binding"):
        snapshot_for({"acceptsSort": True}, binding=None)
    with pytest.raises(ValueError, match="result differs from its binding"):
        snapshot_for({"resultType": "delete_result"})


def test_unknown_write_operation_metadata_rejects_before_projection() -> None:
    with pytest.raises(ValueError, match="Invalid MongoDB write metadata"):
        validate_mongodb_metadata(
            "write", {"operationMetadata": {"not_in_catalog": {"acceptsSort": True}}}
        )


def test_vector_aggregate_scope_is_explicitly_negative() -> None:
    vector = next(
        item
        for item in load_mongodb_declared_snapshot().payload["capabilities"]
        if item["name"] == "vector_search"
    )
    assert vector["properties"]["aggregate.supportsDatabaseScope"] is False
    assert vector["properties"]["aggregate.supportsCollectionScope"] is True
    assert vector["operations"] == [
        {"name": "aggregate", "result_type": "org.mongoeco:result.cursor:1"}
    ]

    requirement_content = load_mongodb_profile("mongodb-search").as_dict()
    requirement_content["payload"]["requirement"] = {
        "id": "vector-database-scope",
        "operator": "equals",
        "capability": "vector_search",
        "operations": ["aggregate"],
        "path": "/properties/aggregate.supportsDatabaseScope",
        "value": True,
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

    assert verdict() == "incompatible"
    for claim in snapshot_content["payload"]["capabilities"]:
        if claim["name"] == "vector_search":
            del claim["properties"]["aggregate.supportsDatabaseScope"]
    assert verdict() == "indeterminate"


def test_aggregate_operation_projection_rejects_unproven_bindings() -> None:
    identity = MongoSnapshotIdentity(
        provider_id="provider-A",
        subject_id="subject-A",
        configuration_revision="revision-A",
        observed_at="2026-09-26T00:00:00Z",
        source_kind="observed",
        source_reference="owner-report-sha256:example",
    )

    def snapshot_for(
        capability: str, metadata: dict[str, object], *, result: str = "cursor"
    ) -> Document:
        return build_mongodb_snapshot(
            catalog=load_mongodb_catalog(),
            identity=identity,
            capabilities=(
                MongoCapabilityClaim(
                    name=capability,
                    support="supported",
                    metadata={
                        **(
                            {"supportedStages": []}
                            if capability == "aggregation"
                            else {"similarities": []}
                        ),
                        "operationMetadata": {"aggregate": metadata},
                    },
                    operations=(
                        MongoOperationClaim(
                            name="aggregate",
                            result_type=f"org.mongoeco:result.{result}:1",
                        ),
                    ),
                ),
            ),
        )

    properties = snapshot_for(
        "vector_search", {"supportsDatabaseScope": False, "supportedOptions": []}
    ).payload["capabilities"][0]["properties"]
    assert properties["aggregate.supportsDatabaseScope"] is False
    assert properties["aggregate.supportedOptions"] == []
    for capability in ("aggregation", "vector_search"):
        with pytest.raises(ValueError, match=f"Invalid MongoDB {capability} metadata"):
            snapshot_for(capability, {"supportsDatabaseScope": "false"})
        with pytest.raises(ValueError, match="result differs from its binding"):
            snapshot_for(capability, {"resultType": "cursor"}, result="document")


@pytest.mark.parametrize(
    ["operation", "field", "value", "result_type"],
    [
        ("find_one", "acceptsHint", False, "document"),
        ("count_documents", "acceptsFilter", True, "count"),
        ("estimated_document_count", "acceptsFilter", False, "count"),
        ("distinct", "acceptsFieldPath", True, "array"),
    ],
)
def test_other_read_operation_values_keep_their_scope(
    operation: str, field: str, value: object, result_type: str
) -> None:
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
                name="read",
                support="supported",
                metadata={"operationMetadata": {operation: {field: value}}},
                operations=(
                    MongoOperationClaim(
                        name=operation,
                        result_type=f"org.mongoeco:result.{result_type}:1",
                    ),
                ),
            ),
        ),
    )
    properties = snapshot.payload["capabilities"][0]["properties"]
    assert properties[f"{operation}.{field}"] is value
    other_operations = {
        "find",
        "find_one",
        "count_documents",
        "estimated_document_count",
        "distinct",
    } - {operation}
    assert all(
        not key.startswith(f"{other}.")
        for other in other_operations
        for key in properties
    )


def test_unknown_read_operation_metadata_rejects_before_projection() -> None:
    with pytest.raises(ValueError, match="Invalid MongoDB read metadata"):
        validate_mongodb_metadata(
            "read", {"operationMetadata": {"not_in_catalog": {"acceptsHint": True}}}
        )


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
