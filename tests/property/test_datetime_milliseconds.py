"""Date coordinates agree with the driver's independent BSON encoding."""

from datetime import datetime

from bson import BSON
from hypothesis import given, settings, strategies as st

from mongoeco.core.bson_scalars import (
    bson_datetime_from_milliseconds,
    bson_datetime_milliseconds,
)


@settings(max_examples=200, deadline=None)
@given(value=st.datetimes())
def test_datetime_coordinates_match_encoded_bson_and_roundtrip(value):
    raw = BSON.encode({"d": value})
    expected = int.from_bytes(raw[7:15], "little", signed=True)
    assert bson_datetime_milliseconds(value) == expected
    assert bson_datetime_from_milliseconds(expected) == BSON(raw).decode()["d"]


@settings(max_examples=100, deadline=None)
@given(
    value=st.datetimes(
        min_value=datetime(1900, 1, 1),  # noqa: DTZ001 - Hypothesis bounds must be naive
        max_value=datetime(2100, 1, 1),  # noqa: DTZ001 - Hypothesis bounds must be naive
        timezones=st.timezones(),
    )
)
def test_aware_coordinates_match_encoded_utc(value):
    raw = BSON.encode({"d": value})
    expected = int.from_bytes(raw[7:15], "little", signed=True)
    assert bson_datetime_milliseconds(value) == expected
    assert bson_datetime_from_milliseconds(expected) == BSON(raw).decode()["d"]
