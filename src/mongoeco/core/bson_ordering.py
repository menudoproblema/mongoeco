from __future__ import annotations

import datetime
import math
import uuid

from decimal import Decimal, localcontext
from typing import TYPE_CHECKING, Any

from mongoeco.core.bson_scalars import (
    is_bson_numeric,
    normalize_utc_bson_datetime,
    numeric_comparison_decimal,
    unwrap_bson_numeric,
    wrap_bson_numeric,
)


try:
    from bson.code import Code
except ImportError:  # pragma: no cover - optional BSON
    Code = ()

from mongoeco.types import (
    Binary,
    ObjectId,
    Regex,
    Timestamp,
    is_object_id_like,
    normalize_object_id,
)


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


def bson_type_rank(value: Any, type_order: Mapping[type, int]) -> int:
    """Use explicit dialect ranks before recognizing numeric wrapper types."""
    if is_object_id_like(value):
        return type_order.get(ObjectId, 100)
    rank = type_order.get(type(value))
    if rank is not None:
        return rank
    comparable = unwrap_bson_numeric(value)
    rank = type_order.get(type(comparable))
    if rank is not None:
        return rank
    # Internal ownership markers are transparent BSON containers. An explicit
    # dialect rank for a subclass still takes precedence above.
    if isinstance(comparable, dict):
        return type_order.get(dict, 100)
    if isinstance(comparable, list):
        return type_order.get(list, 100)
    numeric = wrap_bson_numeric(value)
    return type_order.get(type(numeric), 100) if numeric is not None else 100


def compare_bson_documents(
    left: dict,
    right: dict,
    *,
    type_order: Mapping[type, int],
    compare_values: Callable[[Any, Any], int],
) -> int:
    """Compare each value's type before its field name and recursive value.

    Field names always retain binary order; collation affects only values.
    The value callback keeps dialect overrides and nested collation in context.
    """
    for (left_key, left_value), (right_key, right_value) in zip(
        left.items(), right.items(), strict=False
    ):
        left_rank = bson_type_rank(left_value, type_order)
        right_rank = bson_type_rank(right_value, type_order)
        if left_rank != right_rank:
            return -1 if left_rank < right_rank else 1
        if left_key != right_key:
            return -1 if left_key < right_key else 1
        comparison = compare_values(left_value, right_value)
        if comparison:
            return comparison
    return (len(left) > len(right)) - (len(left) < len(right))


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
    if isinstance(value, Code):
        return (
            "code",
            str(value),
            None if value.scope is None else bson_equality_key(value.scope),
        )
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
        return ("datetime", normalize_utc_bson_datetime(value))
    if isinstance(value, Timestamp):
        return ("timestamp", value.time, value.inc)
    if isinstance(value, Regex):
        return ("regex", value.pattern, value.flags)
    if isinstance(value, dict):
        return (
            "dict",
            tuple((key, bson_engine_key(item)) for key, item in value.items()),
        )
    if isinstance(value, list):
        return ("list", tuple(bson_engine_key(item) for item in value))
    try:
        hash(value)
        return (value.__class__, value)
    except TypeError:
        return ("repr", repr(value))


def _numeric_equality_decimal(numeric):
    number = numeric_comparison_decimal(numeric)
    if number.is_zero():
        number = Decimal(0)
    else:
        sign, digits, exponent = number.as_tuple()
        end = len(digits)
        while end > 1 and digits[end - 1] == 0:
            end -= 1
        number = Decimal((sign, digits[:end], exponent + len(digits) - end))
    # Equal keys must also encode identically when spill partitions hash
    # their codec representation. Decimal hashing alone ignores cohorts.
    return number


def bson_equality_key(value: Any) -> Any:
    """Clave hashable que conserva la igualdad semantica BSON.

    Esta clave es deliberadamente distinta de ``bson_engine_key``: la segunda
    conserva la identidad fisica de tipos numericos para almacenamiento, mientras
    que los indices deben considerar equivalentes las representaciones numericas
    que MongoDB compara como iguales.
    """

    if isinstance(value, bool):
        return ("bool", value)
    if is_bson_numeric(value):
        wrapped = wrap_bson_numeric(value)
        assert wrapped is not None
        numeric = wrapped.value
        if isinstance(numeric, float):
            if math.isnan(numeric):
                return ("number", "nan")
            if math.isinf(numeric):
                return ("number", "-inf" if numeric < 0 else "+inf")
        elif isinstance(numeric, Decimal):
            if numeric.is_nan():
                return ("number", "nan")
            if numeric.is_infinite():
                return ("number", "-inf" if numeric < 0 else "+inf")
        # Decimal equality and hashing already ignore scale and signed zero;
        # normalize() would unnecessarily round under the ambient context.
        return ("number", _numeric_equality_decimal(numeric))
    if isinstance(value, (Binary, bytes)):
        return ("binary", getattr(value, "subtype", 0), bytes(value))
    if isinstance(value, dict):
        return (
            "dict",
            tuple((key, bson_equality_key(item)) for key, item in value.items()),
        )
    if isinstance(value, list):
        return ("list", tuple(bson_equality_key(item) for item in value))
    return bson_engine_key(value)


def bson_numeric_index_key(value: int | float | Decimal) -> str:
    """Clave textual con orden lexicografico compatible con orden numerico.

    Se usa en indices auxiliares SQLite donde la comparacion debe mantenerse en
    texto. No pretende serializar BSON completo; solo cubrir numericos finitos
    con un orden estable y compartido.
    """

    decimal = Decimal(str(value))
    if not decimal.is_finite():
        raise NotImplementedError(
            "NaN and infinity are not supported in SQLite multikey indexes"
        )
    # Preserve the legacy text format without ambient rounding. This remains
    # an auxiliary storage representation, not a BSON equality/order oracle.
    with localcontext() as context:
        context.prec = max(34, len(decimal.as_tuple().digits))
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
