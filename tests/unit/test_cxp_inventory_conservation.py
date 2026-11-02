"""Public declarations stay bound to integrated inventories, not just each other."""

import json

from copy import deepcopy
from importlib.resources import files

import pytest

from cxp.exchange import Document, evaluate_requirements_detailed
from cxp.exchange.errors import InvalidDocumentError

from mongoeco.compat import _catalog_operators as operators, resolve_mongodb_dialect
from mongoeco.core.aggregation.granularity import _SERIES
from mongoeco.core.aggregation.runtime import evaluate_expression
from mongoeco.cxp.exchange import (
    load_mongodb_catalog,
    load_mongodb_declared_snapshot,
    load_mongodb_profile,
    mongodb_catalog_store,
)

from tests.unit.test_cxp_exchange_catalog import _provider_snapshot


def _owner():
    return json.loads(
        files("mongoeco.cxp.exchange")
        .joinpath("data/operational-metadata.json")
        .read_text()
    )


def _check(owner):
    for capability, field, constant in (
        ("read", "queryFieldOperators", "SUPPORTED_QUERY_FIELD_OPERATORS"),
        ("read", "queryTopLevelOperators", "SUPPORTED_QUERY_TOP_LEVEL_OPERATORS"),
        ("write", "updateOperators", "SUPPORTED_UPDATE_OPERATORS"),
        (
            "aggregation",
            "supportedExpressionOperators",
            "SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS",
        ),
        ("aggregation", "supportedStages", "SUPPORTED_AGGREGATION_STAGES"),
        ("aggregation", "supportedGroupAccumulators", "SUPPORTED_GROUP_ACCUMULATORS"),
        ("aggregation", "supportedWindowAccumulators", "SUPPORTED_WINDOW_ACCUMULATORS"),
    ):
        expected = set(getattr(operators, constant))
        assert set(owner[capability][field]) == expected
        for operation in owner[capability].get("operationMetadata", {}).values():
            if field in operation:
                assert set(operation[field]) == expected


def test_integrated_inventory_conservation():
    _check(_owner())
    expected = operators.SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS
    assert all(
        resolve_mongodb_dialect(v).aggregation_expression_operators == expected
        for v in ("7.0", "8.0", "9.0")
    )
    assert {"$not", "$last"} <= expected
    assert not {"$exists", "$elemMatch", "$all", "$nin", "$const", "$meta"} & expected
    value = 42
    assert evaluate_expression({}, {"$const": value}) == value


def test_coordinated_document_changes_cannot_override_runtime_oracle():
    owner = deepcopy(_owner())
    owner["aggregation"]["supportedExpressionOperators"].append("$exists")
    owner["aggregation"]["operationMetadata"]["aggregate"][
        "supportedExpressionOperators"
    ].append("$exists")
    with pytest.raises(AssertionError):
        _check(owner)


def _evaluate(prop, value, snapshot=None):
    snapshot = snapshot or load_mongodb_declared_snapshot()
    requirement = load_mongodb_profile("mongodb-core").as_dict()
    requirement["payload"]["requirement"] = {
        "id": "inventory-oracle",
        "operator": "contains_all",
        "capability": "aggregation",
        "path": "/properties/" + prop,
        "values": [value],
        "require_effective": True,
        "extensions": {},
        "critical_extensions": [],
    }
    context = Document(
        {
            "document_type": "cxp.context",
            "spec_version": 2,
            "payload": {
                "subject_id": snapshot.payload["subject_id"],
                "configuration_revision": snapshot.payload["configuration_revision"],
                "accepted_sources": ["declared"],
            },
        },
        expected_type="cxp.context",
    )
    return evaluate_requirements_detailed(
        snapshot,
        Document(requirement, expected_type="cxp.requirements"),
        context,
        catalogs=mongodb_catalog_store(),
    ).verdict


@pytest.mark.parametrize(
    "operator", ["$exists", "$elemMatch", "$all", "$nin", "$not", "$last", "$sin"]
)
@pytest.mark.parametrize("prefix", ["", "aggregate."])
def test_expression_verdicts(operator, prefix):
    if prefix == "" and operator not in ("$not", "$last"):
        with pytest.raises(InvalidDocumentError):
            _evaluate(prefix + "supportedExpressionOperators", operator)
    else:
        assert _evaluate(prefix + "supportedExpressionOperators", operator) == (
            "compatible" if operator in ("$not", "$last") else "incompatible"
        )


def test_granularity_domain_and_claim():
    prop = "aggregate.bucketAutoGranularitySeries"
    expected = set(_SERIES) | {"POWERSOF2"}
    definition = next(
        x
        for x in load_mongodb_catalog().payload["capabilities"]
        if x["name"] == "aggregation"
    )["properties"][prop]
    assert set(definition["domain"]["values"]) == expected
    for series in expected:
        assert _evaluate(prop, series) == "compatible"
    with pytest.raises(InvalidDocumentError):
        _evaluate(prop, "invented")
    for missing, verdict in ((True, "indeterminate"), (False, "incompatible")):
        content = load_mongodb_declared_snapshot().as_dict()
        properties = next(
            x for x in content["payload"]["capabilities"] if x["name"] == "aggregation"
        )["properties"]
        if missing:
            del properties[prop]
        else:
            properties[prop] = []
        assert (
            _evaluate(prop, "R5", Document(content, expected_type="cxp.snapshot"))
            == verdict
        )


def test_all_published_inventory_representations_match_product():
    owner = _owner()
    snapshot = load_mongodb_declared_snapshot()
    catalog = load_mongodb_catalog()
    for name in ("read", "write", "aggregation"):
        claim = next(x for x in snapshot.payload["capabilities"] if x["name"] == name)
        definitions = next(
            x for x in catalog.payload["capabilities"] if x["name"] == name
        )["properties"]
        fields = (
            ("queryFieldOperators", "queryTopLevelOperators")
            if name == "read"
            else ("updateOperators",)
            if name == "write"
            else (
                "supportedExpressionOperators",
                "supportedStages",
                "supportedGroupAccumulators",
                "supportedWindowAccumulators",
            )
        )
        for field in fields:
            assert set(claim["properties"][field]) == set(owner[name][field])
            assert set(definitions[field]["domain"]["values"]) == set(
                owner[name][field]
            )
            for operation, metadata in owner[name]["operationMetadata"].items():
                if field in metadata:
                    assert set(claim["properties"][operation + "." + field]) == set(
                        metadata[field]
                    )


def test_granularity_uses_existing_validated_builder():

    metadata = {
        "supportedStages": ["$bucketAuto"],
        "operationMetadata": {
            "aggregate": {"resultType": "cursor", "bucketAutoGranularitySeries": ["R5"]}
        },
    }
    snapshot = _provider_snapshot(metadata)
    assert snapshot.payload["capabilities"][0]["properties"][
        "aggregate.bucketAutoGranularitySeries"
    ] == ["R5"]
    metadata["operationMetadata"]["aggregate"]["bucketAutoGranularitySeries"] = [1]
    with pytest.raises(ValueError, match="Invalid MongoDB aggregation metadata"):
        _provider_snapshot(metadata)
