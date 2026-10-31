"""Independent BSON result comparison for native differential and replay tests."""

import math

from collections.abc import Mapping
from datetime import datetime

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

