import decimal
import math
from dataclasses import dataclass
from typing import Any

from mongoeco.types import Decimal128


INT32_MIN = -(1 << 31)
INT32_MAX = (1 << 31) - 1
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
DECIMAL128_CONTEXT = decimal.Context(prec=34, Emin=-6143, Emax=6144)


class BsonScalarOverflowError(OverflowError):
    """Error interno de fidelidad BSON para escalares fuera de rango."""


@dataclass(frozen=True, slots=True)
class BsonInt32:
    value: int


@dataclass(frozen=True, slots=True)
class BsonInt64:
    value: int


@dataclass(frozen=True, slots=True)
class BsonDouble:
    value: float


@dataclass(frozen=True, slots=True)
class BsonDecimal128:
    value: decimal.Decimal


type BsonNumeric = BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128


def _normalize_decimal128(value: decimal.Decimal) -> decimal.Decimal:
    return DECIMAL128_CONTEXT.create_decimal(value)


def wrap_bson_numeric(value: object) -> BsonNumeric | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128):
        return value
    if isinstance(value, int):
        if value < INT64_MIN or value > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")
        if INT32_MIN <= value <= INT32_MAX:
            return BsonInt32(value)
        return BsonInt64(value)
    if isinstance(value, float):
        return BsonDouble(value)
    if isinstance(value, Decimal128):
        return BsonDecimal128(_normalize_decimal128(value.to_decimal()))
    if isinstance(value, decimal.Decimal):
        return BsonDecimal128(_normalize_decimal128(value))
    return None


def unwrap_bson_numeric(value: object) -> object:
    if isinstance(value, BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128):
        return value.value
    return value


def bson_numeric_alias(value: object) -> str | None:
    wrapped = wrap_bson_numeric(value)
    if isinstance(wrapped, BsonInt32):
        return "int"
    if isinstance(wrapped, BsonInt64):
        return "long"
    if isinstance(wrapped, BsonDouble):
        return "double"
    if isinstance(wrapped, BsonDecimal128):
        return "decimal"
    return None


def is_bson_numeric(value: object) -> bool:
    return wrap_bson_numeric(value) is not None


def compare_bson_numeric(left: object, right: object) -> int:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("compare_bson_numeric requires numeric BSON values")

    left_value = wrapped_left.value
    right_value = wrapped_right.value

    if isinstance(left_value, float) and math.isnan(left_value):
        return 0 if isinstance(right_value, float) and math.isnan(right_value) else -1
    if isinstance(right_value, float) and math.isnan(right_value):
        return 1

    if isinstance(left_value, float) and math.isinf(left_value):
        if isinstance(right_value, float) and math.isinf(right_value):
            if left_value == right_value:
                return 0
            return -1 if left_value < right_value else 1
        return -1 if left_value < 0 else 1

    if isinstance(right_value, float) and math.isinf(right_value):
        return 1 if right_value < 0 else -1

    left_decimal = _numeric_to_decimal(left_value)
    right_decimal = _numeric_to_decimal(right_value)
    if left_decimal == right_decimal:
        return 0
    return -1 if left_decimal < right_decimal else 1


def _numeric_to_decimal(value: int | float | decimal.Decimal) -> decimal.Decimal:
    if isinstance(value, Decimal128):
        return _normalize_decimal128(value.to_decimal())
    if isinstance(value, decimal.Decimal):
        return _normalize_decimal128(value)
    if isinstance(value, float):
        return decimal.Decimal(str(value))
    return decimal.Decimal(value)


def validate_bson_value(value: object) -> None:
    if isinstance(value, dict):
        for item in value.values():
            validate_bson_value(item)
        return
    if isinstance(value, list):
        for item in value:
            validate_bson_value(item)
        return
    wrapped = wrap_bson_numeric(value)
    if isinstance(wrapped, BsonDecimal128):
        _normalize_decimal128(wrapped.value)


def bson_add(left: object, right: object) -> object:
    return _wrap_numeric_result(left, right, _numeric_to_decimal(unwrap_bson_numeric(left)) + _numeric_to_decimal(unwrap_bson_numeric(right)))


def bson_multiply(left: object, right: object) -> object:
    return _wrap_numeric_result(left, right, _numeric_to_decimal(unwrap_bson_numeric(left)) * _numeric_to_decimal(unwrap_bson_numeric(right)))


def bson_subtract(left: object, right: object) -> object:
    return _wrap_numeric_result(left, right, _numeric_to_decimal(unwrap_bson_numeric(left)) - _numeric_to_decimal(unwrap_bson_numeric(right)))


def bson_divide(left: object, right: object) -> object:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("BSON arithmetic requires numeric values")
    if isinstance(wrapped_left, BsonDecimal128) or isinstance(wrapped_right, BsonDecimal128):
        result = _normalize_decimal128(_numeric_to_decimal(wrapped_left.value) / _numeric_to_decimal(wrapped_right.value))
        return _wrap_from_templates(result, left, right)
    result = float(unwrap_bson_numeric(left)) / float(unwrap_bson_numeric(right))
    return _wrap_from_templates(result, left, right)


def bson_mod(left: object, right: object) -> object:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("BSON arithmetic requires numeric values")
    if isinstance(wrapped_left, BsonDecimal128) or isinstance(wrapped_right, BsonDecimal128):
        left_decimal = _numeric_to_decimal(wrapped_left.value)
        right_decimal = _numeric_to_decimal(wrapped_right.value)
        quotient = decimal.Decimal(int(left_decimal / right_decimal))
        result = _normalize_decimal128(left_decimal - (right_decimal * quotient))
        return _wrap_from_templates(result, left, right)
    left_value = unwrap_bson_numeric(left)
    right_value = unwrap_bson_numeric(right)
    if isinstance(left_value, float) and not math.isfinite(left_value):
        return math.nan
    if isinstance(right_value, float) and not math.isfinite(right_value):
        return math.nan
    quotient = int(left_value / right_value)
    result = left_value - right_value * quotient
    return _wrap_from_templates(result, left, right)


def bson_rewrap_numeric(result: object, *templates: object) -> object:
    return _wrap_from_templates(result, *templates)


def bson_bitwise(operator: str, left: object, right: object) -> object:
    left_value = _coerce_integral(left)
    right_value = _coerce_integral(right)
    if operator == "and":
        result = left_value & right_value
    elif operator == "or":
        result = left_value | right_value
    elif operator == "xor":
        result = left_value ^ right_value
    else:
        raise ValueError(f"unsupported BSON bitwise operator: {operator}")
    if not isinstance(left, BsonInt32 | BsonInt64) and not isinstance(right, BsonInt32 | BsonInt64):
        return result
    wrapped = wrap_bson_numeric(result)
    assert isinstance(wrapped, BsonInt32 | BsonInt64)
    return wrapped


def _coerce_integral(value: object) -> int:
    unwrapped = unwrap_bson_numeric(value)
    if not isinstance(unwrapped, int) or isinstance(unwrapped, bool):
        raise TypeError("BSON bitwise operations require integral values")
    if unwrapped < INT64_MIN or unwrapped > INT64_MAX:
        raise BsonScalarOverflowError("integer exceeds BSON int64 range")
    return unwrapped


def _wrap_numeric_result(left: object, right: object, result: decimal.Decimal) -> object:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("BSON arithmetic requires numeric values")

    preserve_wrappers = isinstance(left, BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128) or isinstance(
        right,
        BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128,
    )

    if isinstance(wrapped_left, BsonDecimal128) or isinstance(wrapped_right, BsonDecimal128):
        normalized = _normalize_decimal128(result)
        return BsonDecimal128(normalized) if preserve_wrappers else normalized

    if isinstance(wrapped_left, BsonDouble) or isinstance(wrapped_right, BsonDouble):
        normalized = float(result)
        return BsonDouble(normalized) if preserve_wrappers else normalized

    integer_result = int(result)
    if result != decimal.Decimal(integer_result):
        normalized = float(result)
        return BsonDouble(normalized) if preserve_wrappers else normalized

    if integer_result < INT64_MIN or integer_result > INT64_MAX:
        raise BsonScalarOverflowError("integer exceeds BSON int64 range")
    if not preserve_wrappers:
        return integer_result
    if INT32_MIN <= integer_result <= INT32_MAX:
        return BsonInt32(integer_result)
    return BsonInt64(integer_result)


def _wrap_from_templates(result: object, *templates: object) -> object:
    preserve_wrappers = any(isinstance(template, BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128) for template in templates)
    if not preserve_wrappers:
        return result
    unwrapped = unwrap_bson_numeric(result)
    if any(isinstance(template, BsonDecimal128) for template in templates):
        return BsonDecimal128(_normalize_decimal128(_numeric_to_decimal(unwrapped)))
    if any(isinstance(template, BsonDouble) for template in templates):
        return BsonDouble(float(unwrapped))
    if isinstance(unwrapped, decimal.Decimal):
        integer_result = int(unwrapped)
        if unwrapped != decimal.Decimal(integer_result):
            return BsonDouble(float(unwrapped))
    elif isinstance(unwrapped, float):
        if not unwrapped.is_integer():
            return BsonDouble(float(unwrapped))
        integer_result = int(unwrapped)
    else:
        integer_result = int(unwrapped)
    if integer_result < INT64_MIN or integer_result > INT64_MAX:
        raise BsonScalarOverflowError("integer exceeds BSON int64 range")
    if any(isinstance(template, BsonInt64) for template in templates) or integer_result < INT32_MIN or integer_result > INT32_MAX:
        return BsonInt64(integer_result)
    return BsonInt32(integer_result)
