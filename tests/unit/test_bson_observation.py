"""Independent replay failures retain their BSON-sensitive discrimination."""

import pytest

from bson import Binary, Int64

from tests.differential._real_parity_base import _assert_native_observation


@pytest.mark.parametrize(["actual", "expected", "description"], [
    ({"a": 1}, {"a": 1, "b": None}, "field count"),
    ({"b": 2, "a": 1}, {"a": 1, "b": 2}, "field order or presence"),
    ({"v": Int64(1)}, {"v": 1}, r"\['v'\].*Int64"),
    ({"v": Binary(b"x", 3)}, {"v": Binary(b"x", 4)}, r"\['v'\].*Binary"),
    ({"v": [1, 3]}, {"v": [1, 2]}, r"\['v'\]\[1\].*int 3.*int 2"),
])
def test_compare_preserves_bson_differences(actual, expected, description):
    with pytest.raises(AssertionError, match=description):
        _assert_native_observation(actual, expected)


def test_mutation_failure_identifies_sdk_exception():
    actual = {"return/int": {"exception": "TypeError", "state": [1]}}
    expected = {"return/int": {"exception": "ValueError", "state": [1]}}
    with pytest.raises(
        AssertionError,
        match=r"mutation_precedence.*\['return/int'\]\['exception'\].*TypeError.*ValueError",
    ):
        _assert_native_observation(actual, expected, name="mutation_precedence")
