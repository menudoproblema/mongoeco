from __future__ import annotations

import datetime
import uuid
from typing import Any

from mongoeco.types import ObjectId


SQLITE_SORT_BUCKET_WEIGHTS: dict[str, int] = {
    "null": 1,
    "number": 2,
    "string": 3,
    "plain-object": 4,
    "array": 5,
    "uuid": 6,
    "objectid": 7,
    "bool": 8,
    "datetime": 9,
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
    if isinstance(value, bytes):
        return ("bytes", value)
    if isinstance(value, uuid.UUID):
        return ("uuid", value)
    if isinstance(value, ObjectId):
        return ("objectid", value)
    if isinstance(value, datetime.datetime):
        return ("datetime", value)
    if isinstance(value, dict):
        return ("dict", tuple((key, bson_engine_key(item)) for key, item in value.items()))
    if isinstance(value, list):
        return ("list", tuple(bson_engine_key(item) for item in value))
    try:
        hash(value)
        return (value.__class__, value)
    except TypeError:
        return ("repr", repr(value))
