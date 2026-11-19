"""Independent BSON result comparison for native differential and replay tests."""

import math

from collections.abc import Mapping
from datetime import datetime
from reprlib import repr as short_repr

from bson import BSON
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions


_COMPARISON_CODEC_OPTIONS = CodecOptions(
    uuid_representation=UuidRepresentation.STANDARD
)


def bson_comparison_value(value):
    """Observe results independently of production identity/equality helpers.

    Container order, public scalar type and BSON identity are significant.
    Lists and tuples represent the same observed array, as in replay's existing
    JSON transport.
    Only double NaN payload/sign are incidental here; write cases that observe
    a binary no-op retain their explicit effects and canonical observations.
    Unknown Python objects fail BSON encoding instead of comparing their repr.
    """
    if isinstance(value, Mapping):
        return (
            "document",
            tuple(
                (type(key), key, bson_comparison_value(item))
                for key, item in value.items()
            ),
        )
    if isinstance(value, (list, tuple)):
        return ("array", tuple(bson_comparison_value(item) for item in value))
    if isinstance(value, float) and math.isnan(value):
        return (type(value), "double", "NaN")
    if isinstance(value, datetime):
        # Re-encoding would erase extra microseconds and aware/naive differences
        # in a public result; the real driver has already decoded its BSON date.
        return (type(value), value.isoformat(timespec="microseconds"))
    return (
        type(value),
        BSON.encode({"value": value}, codec_options=_COMPARISON_CODEC_OPTIONS),
    )


def describe_bson_difference(actual, expected, observed, reference, path="$"):
    """Explain a mismatch already detected by the independent comparison."""
    if observed[0] == reference[0] == "document":
        for got, wanted in zip(observed[1], reference[1], strict=False):
            got_type, got_key, got_value = got
            want_type, want_key, want_value = wanted
            if (got_type, got_key) != (want_type, want_key):
                return (
                    f"{path}: field order or presence differs at "
                    f"{short_repr(got_key)} vs {short_repr(want_key)}"
                )
            if got_value != want_value:
                return describe_bson_difference(
                    actual[got_key], expected[want_key], got_value, want_value,
                    f"{path}[{short_repr(got_key)}]",
                )
        return f"{path}: field count {len(actual)} != {len(expected)}"
    if observed[0] == reference[0] == "array":
        for index, (got, wanted) in enumerate(
            zip(observed[1], reference[1], strict=False)
        ):
            if got != wanted:
                return describe_bson_difference(
                    actual[index], expected[index], got, wanted, f"{path}[{index}]"
                )
        return f"{path}: array length {len(actual)} != {len(expected)}"
    return (
        f"{path}: observed {type(actual).__name__} {short_repr(actual)}; "
        f"expected {type(expected).__name__} {short_repr(expected)}"
    )
