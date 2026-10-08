"""Native diagnostics retain their meaning across BSON type-list ordering."""

import copy
import json

from itertools import permutations
from pathlib import Path

import pytest

from scripts.capture_differential_replay_golden import _capture_expectation


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
