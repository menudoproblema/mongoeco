from __future__ import annotations

import datetime
from decimal import Decimal
import uuid
from typing import Any

from mongoeco.types import Binary, Regex, Timestamp, is_object_id_like, normalize_object_id


SQLITE_SORT_BUCKET_WEIGHTS: dict[str, int] = {
    "null": 1,
    "number": 2,
    "string": 3,
    "plain-object": 4,
    "array": 5,
    "binary": 6,
    "uuid": 6,
    "objectid": 7,
    "bool": 8,
    "datetime": 9,
    "timestamp": 10,
    "regex": 11,
    "fallback": 100,
}


def bson_engine_key(value: Any) -> Any:
    """Clave hashable y estable para igualdad/orden interno entre engines.

    No sustituye la comparación BSON oficial, pero deriva de la misma familia
    de brackets y evita que cada engine mantenga su propia tabla de dispatch.
    """

    if value is None:
        return ("none", None)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("int", value)
    if isinstance(value, float):
        return ("float", value)
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, Binary):
        return ("binary", value.subtype, bytes(value))
    if isinstance(value, bytes):
        return ("bytes", bytes(value))
    if isinstance(value, uuid.UUID):
        return ("uuid", value)
    if is_object_id_like(value):
        return ("objectid", normalize_object_id(value))
    if isinstance(value, datetime.datetime):
        return ("datetime", value)
    if isinstance(value, Timestamp):
        return ("timestamp", value.time, value.inc)
    if isinstance(value, Regex):
        return ("regex", value.pattern, value.flags)
    if isinstance(value, dict):
        return ("dict", tuple((key, bson_engine_key(item)) for key, item in value.items()))
    if isinstance(value, list):
        return ("list", tuple(bson_engine_key(item) for item in value))
    try:
        hash(value)
        return (value.__class__, value)
    except TypeError:
        return ("repr", repr(value))


def bson_numeric_index_key(value: int | float | Decimal) -> str:
    """Clave textual con orden lexicografico compatible con orden numerico.

    Se usa en indices auxiliares SQLite donde la comparacion debe mantenerse en
    texto. No pretende serializar BSON completo; solo cubrir numericos finitos
    con un orden estable y compartido.
    """

    decimal = Decimal(str(value))
    if not decimal.is_finite():
        raise NotImplementedError("NaN and infinity are not supported in SQLite multikey indexes")
    normalized = decimal.normalize()
    if normalized == 0:
        return "1|0|!"

    digits = "".join(str(digit) for digit in normalized.as_tuple().digits)
    adjusted = normalized.adjusted()
    bias = 500_000
    if normalized > 0:
        return f"2|{adjusted + bias:06d}|{digits}!"
    complemented = "".join(str(9 - int(digit)) for digit in digits)
    return f"0|{bias - adjusted:06d}|{complemented}~"
