"""Native diagnostics retain their meaning across BSON type-list ordering."""

import copy
import json

from datetime import UTC, datetime
from itertools import permutations
from pathlib import Path

import pytest

from bson import Binary, Decimal128, Int64
from bson.errors import InvalidDocument
from pymongo.errors import OperationFailure
from scripts.capture_differential_replay_golden import _capture_expectation

from mongoeco.core import bson_identity, bson_scalars, identity, paths

from tests.differential._real_parity_base import (
    _assert_native_observation,
    _decode_capture,
    _normalize,
)
from tests.differential.semantic_expression_grammar_cases import _observe


NAIVE_DATE = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)
IN_TYPE_ERROR_CODE = 40081


def _densify_type_error(
    received="bool", types="long, double, int, decimal", closing="']"
):
    return {
        "ok": False,
        "error_type": "OperationFailure",
        "code": {"$numberInt": "14"},
        "code_name": "TypeMismatch",
        "error_labels": [],
        "message": (
            "BSON field '$densify.range.step' is the wrong type "
            f"'{received}', expected types '[{types}{closing}"
        ),
    }


@pytest.mark.parametrize("dialect", ["7_0", "8_0", "9_0"])
def test_type_error_comparison_consumes_actual_native_fixture_messages(dialect):
    fixture = Path(__file__).parents[1] / "fixtures"
    native = json.loads(
        (fixture / f"mongodb_semantic_guarantees_{dialect}.json").read_text()
    )
    for case in ("densify_bool", "densify_str"):
        error = native["cases"][case]
        normalized = _capture_expectation(error)
        assert normalized["message"]["field"] == "$densify.range.step"
        received = "bool" if case == "densify_bool" else "string"
        closing = "]'" if dialect == "9_0" else "']"
        assert normalized == _capture_expectation(
            _densify_type_error(received, closing=closing)
        )


@pytest.mark.parametrize("received", ["bool", "string"])
@pytest.mark.parametrize("closing", ["']", "]'"])
def test_numeric_type_enumeration_permutations_keep_the_native_guarantee(
    received, closing
):
    original = _densify_type_error(received, closing=closing)
    untouched = copy.deepcopy(original)
    for order in permutations(("long", "double", "int", "decimal")):
        observed = _densify_type_error(received, ", ".join(order), closing)
        assert _capture_expectation(observed) == _capture_expectation(original)
    assert original == untouched


@pytest.mark.parametrize(
    "change",
    [
        {"code": {"$numberInt": "9"}},
        {"code_name": "FailedToParse"},
        {"error_type": "ConfigurationError"},
        {"error_labels": ["TransientTransactionError"]},
        {"message": _densify_type_error("string")["message"]},
        {"message": _densify_type_error(types="int, long, double")["message"]},
        {"message": _densify_type_error(types="int, long, double, bool")["message"]},
        {
            "message": _densify_type_error(types="int, long, double, decimal, int")[
                "message"
            ]
        },
        {"message": "context: " + _densify_type_error()["message"]},
        {"message": _densify_type_error()["message"] + "; phase changed"},
        {"message": _densify_type_error()["message"].replace(".step", ".bounds")},
        {"message": _densify_type_error(closing="]'")["message"]},
    ],
)
def test_type_list_comparison_still_detects_semantic_error_changes(change):
    original = _densify_type_error()
    assert _capture_expectation(original | change) != _capture_expectation(original)


def test_type_list_order_is_not_ignored_in_other_fields_or_successful_data():
    original = _densify_type_error()
    reordered = _densify_type_error(types="decimal, int, double, long")
    for value in (original, {"ok": True, "result": [original, original]}):
        successful = {"ok": True, "result": value}
        assert _capture_expectation(successful) == successful
    other_field = original["message"].replace("$densify.range.step", "$other.field")
    changed_field = reordered["message"].replace("$densify.range.step", "$other.field")
    assert _capture_expectation(original | {"message": other_field}) != (
        _capture_expectation(original | {"message": changed_field})
    )


def test_canonical_write_observations_decode_once_without_changing_seed_types():
    payload = {
        "corpus": {"observationEncoding": "canonical-json"},
        "cases": {"write": json.dumps({"value": {"$numberLong": "1"}, "counter": 2})},
        "case_manifests": [{"seed_documents": [{"x": {"$numberLong": "1"}}]}],
    }
    decoded = _decode_capture(json.dumps(payload))
    assert decoded["cases"]["write"] == {"value": {"$numberLong": "1"}, "counter": 2}
    assert isinstance(decoded["case_manifests"][0]["seed_documents"][0]["x"], Int64)
    assert _normalize(decoded["cases"]["write"]["value"]) != _normalize(
        {"$numberInt": "1"}
    )
    payload["corpus"]["observationEncoding"] = "bson"
    payload["cases"]["write"] = {"value": {"$numberLong": "1"}}
    assert isinstance(
        _decode_capture(json.dumps(payload))["cases"]["write"]["value"], Int64
    )
    payload["corpus"]["observationEncoding"] = "unsupported"
    with pytest.raises(ValueError, match="unknown capture observation encoding"):
        _decode_capture(json.dumps(payload))


@pytest.mark.parametrize(
    ["left", "right"],
    [
        (True, 1),
        (False, 0),
        (1, Int64(1)),
        (Int64(1), 1.0),
        (1.0, Decimal128("1")),
        (0.0, -0.0),
        (Decimal128("1"), Decimal128("1.00")),
        (Decimal128("0"), Decimal128("-0")),
        (float("nan"), "NaN"),
        (float("nan"), Decimal128("NaN")),
        ({}, {"value": None}),
        ({"a": 1, "b": 2}, {"b": 2, "a": 1}),
        ([1, 2], [2, 1]),
        ({"code": 2}, {}),
        ({"modified": 0}, {"modified": 1}),
        ({"value": 1}, {"value": Int64(1)}),
        (2**40, Int64(2**40)),
        (b"value", Binary(b"value", 0)),
        (NAIVE_DATE, datetime(2026, 1, 1, tzinfo=UTC)),
        (NAIVE_DATE, NAIVE_DATE.replace(microsecond=1)),
    ],
)
def test_result_comparison_preserves_observable_bson_distinctions(left, right):
    assert _normalize(left) != _normalize(right)
    assert _normalize({"nested": [left]}) != _normalize({"nested": [right]})


def test_result_comparison_keeps_only_declared_transport_equivalences():
    assert _normalize((1, {"value": Int64(1)})) == _normalize([1, {"value": Int64(1)}])
    assert _normalize(float("nan")) == _normalize(-float("nan"))
    assert _normalize(Decimal128("1.00")) == _normalize(Decimal128("1.00"))


def test_result_comparison_is_independent_of_production_value_rules(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("a production equality/identity rule reached the oracle")

    monkeypatch.setattr(identity, "canonical_document_id", forbidden)
    monkeypatch.setattr(bson_scalars, "compare_bson_numeric", forbidden)
    monkeypatch.setattr(paths, "same_bson_update_value", forbidden)
    monkeypatch.setattr(bson_identity, "same_bson_update_value", forbidden)
    assert _normalize({"a": [Int64(1), -0.0]}) != _normalize({"a": [1, 0.0]})


def test_result_comparison_rejects_unencodable_objects_instead_of_using_repr():
    with pytest.raises(InvalidDocument):
        _normalize({"nested": [object()]})


@pytest.mark.parametrize("namespace", ["actual.records", "wrong.records"])
def test_grammar_observation_normalizes_only_its_actual_namespace(namespace):
    prefix = "Executor error during aggregate command on namespace: "
    suffix = " :: caused by :: invalid input"
    message = prefix + namespace + suffix

    def reject():
        raise OperationFailure(
            message, code=IN_TYPE_ERROR_CODE, details={"errmsg": message}
        )

    observed = _observe(reject, "actual.records")
    expected = (
        prefix + "<namespace>" + suffix if namespace == "actual.records" else message
    )
    assert observed["message"] == expected
    assert observed["code"] == IN_TYPE_ERROR_CODE
    assert _observe(lambda: {"message": message}, "actual.records")["result"] == {
        "message": message
    }


@pytest.mark.parametrize("change", ["code", "phase", "state", "supported-type"])
def test_capped_contract_exception_does_not_hide_other_differences(change):
    native = {
        key: {"ok": True, "aux_exists": True, "state": {"count": 0}}
        for key in ("capped-size-string", "capped-max-zero")
    }
    actual = {
        key: {
            "ok": False,
            "phase": "invoke",
            "exception": "OperationFailure",
            "code": 115,
            "state": {"count": 0},
            "aux_exists": False,
            "indexes": [],
        }
        for key in native
    }
    native["supported"] = {"value": 1}
    actual["supported"] = {"value": 1}
    before = copy.deepcopy((actual, native))
    _assert_native_observation(
        actual, native, corpus="bson_catalog", name="semantic_catalog_create_options"
    )
    assert (actual, native) == before
    if change == "code":
        actual["capped-size-string"]["code"] = 9
    elif change == "phase":
        actual["capped-size-string"]["phase"] = "iterate"
    elif change == "state":
        actual["capped-size-string"]["state"] = {"count": 1}
    else:
        actual["supported"]["value"] = True
    with pytest.raises(AssertionError):
        _assert_native_observation(
            actual,
            native,
            corpus="bson_catalog",
            name="semantic_catalog_create_options",
        )
