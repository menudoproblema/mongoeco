from __future__ import annotations

import decimal
import math
from dataclasses import dataclass
from typing import Any

from mongoeco.errors import OperationFailure
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

    def __post_init__(self) -> None:
        if not isinstance(self.value, int) or isinstance(self.value, bool):
            raise TypeError("BsonInt32 requires an integer value")
        if self.value < INT32_MIN or self.value > INT32_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int32 range")


@dataclass(frozen=True, slots=True)
class BsonInt64:
    value: int

    def __post_init__(self) -> None:
        if not isinstance(self.value, int) or isinstance(self.value, bool):
            raise TypeError("BsonInt64 requires an integer value")
        if self.value < INT64_MIN or self.value > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")


@dataclass(frozen=True, slots=True)
class BsonDouble:
    value: float


@dataclass(frozen=True, slots=True)
class BsonDecimal128:
    value: decimal.Decimal


type BsonNumeric = BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128


def _normalize_decimal128(value: decimal.Decimal) -> decimal.Decimal:
    return DECIMAL128_CONTEXT.create_decimal(value)


def _is_nan_numeric_value(value: object) -> bool:
    return (
        isinstance(value, float) and math.isnan(value)
    ) or (
        isinstance(value, decimal.Decimal) and value.is_nan()
    )


def _is_infinite_numeric_value(value: object) -> bool:
    return (
        isinstance(value, float) and math.isinf(value)
    ) or (
        isinstance(value, decimal.Decimal) and value.is_infinite()
    )


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
    if isinstance(value, BsonInt32):
        return "int"
    if isinstance(value, BsonInt64):
        return "long"
    if isinstance(value, BsonDouble):
        return "double"
    if isinstance(value, BsonDecimal128):
        return "decimal"
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        if INT32_MIN <= value <= INT32_MAX:
            return "int"
        if value < INT64_MIN or value > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, Decimal128 | decimal.Decimal):
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

    if _is_nan_numeric_value(left_value):
        return 0 if _is_nan_numeric_value(right_value) else -1
    if _is_nan_numeric_value(right_value):
        return 1

    if _is_infinite_numeric_value(left_value):
        if _is_infinite_numeric_value(right_value):
            if left_value == right_value:
                return 0
            return -1 if left_value < right_value else 1
        return -1 if left_value < 0 else 1

    if _is_infinite_numeric_value(right_value):
        return 1 if right_value < 0 else -1

    if (
        isinstance(left_value, int)
        and not isinstance(left_value, bool)
        and isinstance(right_value, int)
        and not isinstance(right_value, bool)
    ):
        if left_value == right_value:
            return 0
        return -1 if left_value < right_value else 1

    if isinstance(left_value, float) and isinstance(right_value, float):
        if left_value == right_value:
            return 0
        return -1 if left_value < right_value else 1

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
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, dict):
            for key in current:
                if not isinstance(key, str):
                    raise TypeError("BSON document keys must be strings")
            pending.extend(current.values())
            continue
        if isinstance(current, (list, tuple)):
            pending.extend(current)
            continue
        if isinstance(current, (set, frozenset)):
            raise TypeError("set values are not BSON-serializable")
        wrapped = wrap_bson_numeric(current)
        if isinstance(wrapped, BsonDecimal128):
            _normalize_decimal128(wrapped.value)


def bson_add(left: object, right: object) -> object:
    if (
        isinstance(left, int)
        and not isinstance(left, bool)
        and isinstance(right, int)
        and not isinstance(right, bool)
    ):
        result = left + right
        if result < INT64_MIN or result > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")
        return result
    float_result = _try_fast_float_arithmetic(left, right, "add")
    if float_result is not None:
        return float_result
    return _wrap_numeric_result(left, right, _numeric_to_decimal(unwrap_bson_numeric(left)) + _numeric_to_decimal(unwrap_bson_numeric(right)))


def bson_multiply(left: object, right: object) -> object:
    if (
        isinstance(left, int)
        and not isinstance(left, bool)
        and isinstance(right, int)
        and not isinstance(right, bool)
    ):
        result = left * right
        if result < INT64_MIN or result > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")
        return result
    float_result = _try_fast_float_arithmetic(left, right, "multiply")
    if float_result is not None:
        return float_result
    return _wrap_numeric_result(left, right, _numeric_to_decimal(unwrap_bson_numeric(left)) * _numeric_to_decimal(unwrap_bson_numeric(right)))


def bson_subtract(left: object, right: object) -> object:
    if (
        isinstance(left, int)
        and not isinstance(left, bool)
        and isinstance(right, int)
        and not isinstance(right, bool)
    ):
        result = left - right
        if result < INT64_MIN or result > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")
        return result
    float_result = _try_fast_float_arithmetic(left, right, "subtract")
    if float_result is not None:
        return float_result
    return _wrap_numeric_result(left, right, _numeric_to_decimal(unwrap_bson_numeric(left)) - _numeric_to_decimal(unwrap_bson_numeric(right)))


def bson_divide(left: object, right: object) -> object:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("BSON arithmetic requires numeric values")
    if unwrap_bson_numeric(right) == 0:
        raise OperationFailure("$divide cannot divide by zero")
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
    if unwrap_bson_numeric(right) == 0:
        raise OperationFailure("$mod cannot divide by zero")
    if isinstance(wrapped_left, BsonDecimal128) or isinstance(wrapped_right, BsonDecimal128):
        left_decimal = _numeric_to_decimal(wrapped_left.value)
        right_decimal = _numeric_to_decimal(wrapped_right.value)
        quotient = decimal.Decimal(int(left_decimal / right_decimal))
        result = _normalize_decimal128(left_decimal - (right_decimal * quotient))
        return _wrap_from_templates(result, left, right)
    left_value = unwrap_bson_numeric(left)
    right_value = unwrap_bson_numeric(right)
    if (
        isinstance(left_value, int)
        and not isinstance(left_value, bool)
        and isinstance(right_value, int)
        and not isinstance(right_value, bool)
    ):
        quotient = abs(left_value) // abs(right_value)
        if (left_value < 0) != (right_value < 0):
            quotient = -quotient
        result = left_value - right_value * quotient
        return _wrap_from_templates(result, left, right)
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
    if not isinstance(wrapped, BsonInt32 | BsonInt64):
        raise TypeError("BSON bitwise operations require signed 64-bit integer results")
    return wrapped


def _coerce_integral(value: object) -> int:
    unwrapped = unwrap_bson_numeric(value)
    if not isinstance(unwrapped, int) or isinstance(unwrapped, bool):
        raise TypeError("BSON bitwise operations require integral values")
    if unwrapped < INT64_MIN or unwrapped > INT64_MAX:
        raise BsonScalarOverflowError("integer exceeds BSON int64 range")
    return unwrapped


def _try_fast_float_arithmetic(left: object, right: object, operator: str) -> object | None:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("BSON arithmetic requires numeric values")
    if isinstance(wrapped_left, BsonDecimal128) or isinstance(wrapped_right, BsonDecimal128):
        return None
    if not isinstance(wrapped_left, BsonDouble) and not isinstance(wrapped_right, BsonDouble):
        return None

    left_value = float(unwrap_bson_numeric(left))
    right_value = float(unwrap_bson_numeric(right))
    if operator == "add":
        result = left_value + right_value
    elif operator == "subtract":
        result = left_value - right_value
    elif operator == "multiply":
        result = left_value * right_value
    else:
        raise ValueError(f"unsupported BSON float arithmetic operator: {operator}")

    preserve_wrappers = isinstance(left, BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128) or isinstance(
        right,
        BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128,
    )
    return BsonDouble(result) if preserve_wrappers else result


def _numeric_template_metadata(value: object) -> tuple[str | None, bool]:
    if isinstance(value, BsonDecimal128):
        return "decimal", True
    if isinstance(value, BsonDouble):
        return "double", True
    if isinstance(value, BsonInt64):
        return "long", True
    if isinstance(value, BsonInt32):
        return "int", True
    if isinstance(value, bool):
        return None, False
    if isinstance(value, Decimal128 | decimal.Decimal):
        return "decimal", False
    if isinstance(value, float):
        return "double", False
    if isinstance(value, int):
        if value < INT64_MIN or value > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")
        return ("int" if INT32_MIN <= value <= INT32_MAX else "long"), False
    return None, False


def _wrap_numeric_result(left: object, right: object, result: decimal.Decimal) -> object:
    left_kind, preserve_left = _numeric_template_metadata(left)
    right_kind, preserve_right = _numeric_template_metadata(right)
    if left_kind is None or right_kind is None:
        raise TypeError("BSON arithmetic requires numeric values")

    preserve_wrappers = preserve_left or preserve_right

    if left_kind == "decimal" or right_kind == "decimal":
        normalized = _normalize_decimal128(result)
        return BsonDecimal128(normalized) if preserve_wrappers else normalized

    if left_kind == "double" or right_kind == "double":
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
    preserve_wrappers = False
    has_decimal = False
    has_double = False
    has_int64 = False
    for template in templates:
        if isinstance(template, BsonDecimal128):
            preserve_wrappers = True
            has_decimal = True
        elif isinstance(template, BsonDouble):
            preserve_wrappers = True
            has_double = True
        elif isinstance(template, BsonInt64):
            preserve_wrappers = True
            has_int64 = True
        elif isinstance(template, BsonInt32):
            preserve_wrappers = True
    if not preserve_wrappers:
        return result
    unwrapped = unwrap_bson_numeric(result)
    if has_decimal:
        return BsonDecimal128(_normalize_decimal128(_numeric_to_decimal(unwrapped)))
    if has_double:
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
    if has_int64 or integer_result < INT32_MIN or integer_result > INT32_MAX:
        return BsonInt64(integer_result)
    return BsonInt32(integer_result)
