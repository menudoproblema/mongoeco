from __future__ import annotations

import decimal
import math

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from operator import add, mul, sub
from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.types import Decimal128


try:
    from bson.decimal128 import Decimal128 as _PublicDecimal128
    from bson.int64 import Int64 as _PublicInt64
except ImportError:  # pragma: no cover - optional BSON
    _PublicInt64 = None
    _PublicDecimal128 = None

INT32_MIN = -(1 << 31)
INT32_MAX = (1 << 31) - 1
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
_BSON_EPOCH = datetime(1970, 1, 1, tzinfo=UTC).replace(tzinfo=None)
DECIMAL128_CONTEXT = decimal.Context(prec=34, Emin=-6143, Emax=6144)
_DECIMAL128_ARITHMETIC_CONTEXT = decimal.Context(
    prec=34, Emin=-6143, Emax=6144, rounding=decimal.ROUND_HALF_EVEN, clamp=1,
    traps=[],
)
_DOUBLE_TO_DECIMAL_CONTEXT = decimal.Context(
    prec=15, rounding=decimal.ROUND_HALF_EVEN, traps=[],
)


def utc_bson_now() -> datetime:
    """Devuelve la hora UTC compatible con la precisión BSON."""
    return normalize_utc_bson_datetime(datetime.now(UTC))


def normalize_utc_bson_datetime(value: datetime) -> datetime:
    """Normaliza una fecha al formato UTC naïve y milisegundos de BSON."""
    if not isinstance(value, datetime):
        raise TypeError("now_factory must return a datetime")
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    microsecond = (value.microsecond // 1_000) * 1_000
    return value if microsecond == value.microsecond else value.replace(microsecond=microsecond)


def bson_datetime_milliseconds(value: datetime) -> int:
    """Exact epoch coordinate after UTC/BSON millisecond normalization."""
    delta = normalize_utc_bson_datetime(value) - _BSON_EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def bson_datetime_from_milliseconds(value: int) -> datetime:
    """Inverse coordinate within Python datetime's supported range."""
    return _BSON_EPOCH + timedelta(milliseconds=value)


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
    if _PublicInt64 is not None and isinstance(value, _PublicInt64):
        return BsonInt64(int(value))
    if isinstance(value, int):
        if value < INT64_MIN or value > INT64_MAX:
            raise BsonScalarOverflowError("integer exceeds BSON int64 range")
        if INT32_MIN <= value <= INT32_MAX:
            return BsonInt32(value)
        return BsonInt64(value)
    if isinstance(value, float):
        return BsonDouble(value)
    if _PublicDecimal128 is not None and isinstance(value, _PublicDecimal128):
        return BsonDecimal128(value.to_decimal())
    if isinstance(value, Decimal128):
        return BsonDecimal128(_normalize_decimal128(value.to_decimal()))
    if isinstance(value, decimal.Decimal):
        return BsonDecimal128(_normalize_decimal128(value))
    return None


def truncate_bson_int64(value: object) -> int | None:
    """Truncate a BSON number to int64, returning None outside that domain.

    Callers own argument errors versus ineligible candidates and, where needed,
    reject fractional values by comparing the original number to this integer.
    """
    numeric = wrap_bson_numeric(value)
    if numeric is None:
        return None
    try:
        integer = int(numeric.value)
    except (ValueError, OverflowError):
        return None
    return integer if INT64_MIN <= integer <= INT64_MAX else None


def unwrap_bson_numeric(value: object) -> object:
    if isinstance(value, BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128):
        return value.value
    return value


def exact_bson_int64(value: object) -> int | None:
    """An exact signed BSON integer across numeric representations; never bool."""
    integer = truncate_bson_int64(value)
    return (
        integer
        if integer is not None and compare_bson_numeric(value, integer) == 0
        else None
    )


def bson_numeric_alias(value: object) -> str | None:
    if _PublicDecimal128 is not None and isinstance(value, _PublicDecimal128):
        return "decimal"
    if _PublicInt64 is not None and isinstance(value, _PublicInt64):
        return "long"
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

    left_decimal = numeric_comparison_decimal(left_value)
    right_decimal = numeric_comparison_decimal(right_value)
    if left_decimal == right_decimal:
        return 0
    return -1 if left_decimal < right_decimal else 1


def numeric_comparison_decimal(value: float | decimal.Decimal) -> decimal.Decimal:
    """Preserve the exact finite numeric value, independently of decimal context.

    Arithmetic promotion has a separate conversion: rounding a double for an
    operation must not change which stored decimal it compares equal to.
    """
    if isinstance(value, float):
        # Decimal(float) obeys the ambient FloatOperation trap and sets its flag.
        return decimal.Decimal.from_float(value)  # noqa: FURB164
    return decimal.Decimal(value)


def _numeric_to_decimal(value: int | float | decimal.Decimal) -> decimal.Decimal:
    if isinstance(value, Decimal128):
        return _normalize_decimal128(value.to_decimal())
    if isinstance(value, decimal.Decimal):
        return _normalize_decimal128(value)
    if isinstance(value, float):
        with decimal.localcontext(_DOUBLE_TO_DECIMAL_CONTEXT) as context:
            converted = context.create_decimal_from_float(value)
            if not value or not math.isfinite(value):
                return converted
            # MongoDB arithmetic conversion retains exactly 15 significant digits,
            # including trailing zeros. Accumulator conversion stays separate.
            quantum = decimal.Decimal(1).scaleb(converted.adjusted() - 14)
            return context.quantize(converted, quantum)
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


def bson_add(left: object, right: object, *, promote_overflow: bool = False) -> object:
    return _bson_arithmetic(left, right, add, promote_overflow=promote_overflow)


def bson_multiply(left: object, right: object, *, promote_overflow: bool = False) -> object:
    return _bson_arithmetic(left, right, mul, promote_overflow=promote_overflow)


def bson_subtract(left: object, right: object, *, promote_overflow: bool = False) -> object:
    return _bson_arithmetic(left, right, sub, promote_overflow=promote_overflow)


def _bson_arithmetic(left, right, operation, *, promote_overflow):
    numeric_left, numeric_right = wrap_bson_numeric(left), wrap_bson_numeric(right)
    if numeric_left is None or numeric_right is None:
        message = "BSON arithmetic requires numeric values"
        raise TypeError(message)
    if isinstance(numeric_left, (BsonInt32, BsonInt64)) and isinstance(
        numeric_right, (BsonInt32, BsonInt64)
    ):
        result = operation(numeric_left.value, numeric_right.value)
        if not INT64_MIN <= result <= INT64_MAX:
            if promote_overflow:
                # Promotion converts operands before performing the operation.
                # Rounding the exact overflowing integer gives different tie results.
                return _wrap_float_result(
                    operation(float(numeric_left.value), float(numeric_right.value)),
                    left, right,
                )
            message = "integer exceeds BSON int64 range"
            raise BsonScalarOverflowError(message)
        if not INT32_MIN <= result <= INT32_MAX:
            return BsonInt64(result)
        return bson_rewrap_numeric(result, left, right)
    if isinstance(numeric_left, BsonDecimal128) or isinstance(
        numeric_right, BsonDecimal128
    ):
        with decimal.localcontext(_DECIMAL128_ARITHMETIC_CONTEXT):
            result = operation(
                _numeric_to_decimal(numeric_left.value),
                _numeric_to_decimal(numeric_right.value),
            )
            return BsonDecimal128(_normalize_decimal128(result))
    return _wrap_float_result(
        operation(float(numeric_left.value), float(numeric_right.value)), left, right
    )


def _wrap_float_result(result, *templates):
    return (
        BsonDouble(result)
        if any(
            isinstance(value, (BsonInt32, BsonInt64, BsonDouble, BsonDecimal128))
            or (_PublicInt64 is not None and isinstance(value, _PublicInt64))
            for value in templates
        )
        else result
    )


def bson_divide(left: object, right: object) -> object:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("BSON arithmetic requires numeric values")
    if unwrap_bson_numeric(right) == 0:
        raise OperationFailure("can't $divide by zero", code=2)
    if isinstance(wrapped_left, BsonDecimal128) or isinstance(
        wrapped_right, BsonDecimal128
    ):
        with decimal.localcontext(_DECIMAL128_ARITHMETIC_CONTEXT):
            result = (
                _numeric_to_decimal(wrapped_left.value)
                / _numeric_to_decimal(wrapped_right.value)
            )
            return BsonDecimal128(_normalize_decimal128(result))
    result = float(wrapped_left.value) / float(wrapped_right.value)
    return _wrap_float_result(result, left, right)


def bson_mod(
    left: object, right: object, *, integral_double_divisor: bool = False,
) -> object:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is None or wrapped_right is None:
        raise TypeError("BSON arithmetic requires numeric values")
    if unwrap_bson_numeric(right) == 0:
        raise OperationFailure("can't $mod by zero", code=16610)
    if isinstance(wrapped_left, BsonDecimal128) or isinstance(
        wrapped_right, BsonDecimal128
    ):
        with decimal.localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
            left_decimal = _numeric_to_decimal(wrapped_left.value)
            right_decimal = _numeric_to_decimal(wrapped_right.value)
            if left_decimal.is_finite() and right_decimal.is_finite():
                # MongoDB remainder is valid even when the integer quotient
                # needs more than 34 digits. Retain that quotient internally;
                # only the final remainder is a Decimal128 result.
                context.prec = max(
                    context.prec, left_decimal.adjusted() - right_decimal.adjusted() + 1
                )
            result = left_decimal % right_decimal
            return BsonDecimal128(_normalize_decimal128(result))
    left_value = wrapped_left.value
    right_value = wrapped_right.value
    result_templates = (left, right)
    if (
        integral_double_divisor
        and isinstance(wrapped_left, (BsonInt32, BsonInt64))
        and isinstance(wrapped_right, BsonDouble)
        and math.isfinite(right_value)
        and right_value.is_integer()
        and INT64_MIN <= right_value < (1 << 63)
    ):
        right_value = int(right_value)
        result_templates = (left,)  # Retain the dividend's BSON numeric type.
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
        return _wrap_from_templates(result, *result_templates)
    left_double, right_double = float(left_value), float(right_value)
    result = (
        math.fmod(left_double, right_double)
        if math.isfinite(left_double) and not math.isnan(right_double)
        else math.nan
    )
    return _wrap_float_result(result, *result_templates)


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
    wrapped = _wrap_from_templates(result, left, right)
    if not isinstance(wrap_bson_numeric(wrapped), BsonInt32 | BsonInt64):
        raise TypeError("BSON bitwise operations require signed 64-bit integer results")
    return wrapped


def _coerce_integral(value: object) -> int:
    unwrapped = unwrap_bson_numeric(value)
    if not isinstance(unwrapped, int) or isinstance(unwrapped, bool):
        raise TypeError("BSON bitwise operations require integral values")
    if unwrapped < INT64_MIN or unwrapped > INT64_MAX:
        raise BsonScalarOverflowError("integer exceeds BSON int64 range")
    return unwrapped


def _wrap_from_templates(result: object, *templates: object) -> object:
    preserve_wrappers = False
    has_decimal = False
    has_double = False
    has_int64 = False
    for template in templates:
        if _PublicInt64 is not None and isinstance(template, _PublicInt64):
            preserve_wrappers = True
            has_int64 = True
        elif isinstance(template, BsonDecimal128):
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
        elif isinstance(template, (Decimal128, decimal.Decimal)):
            has_decimal = True
        elif isinstance(template, float):
            has_double = True
    unwrapped = unwrap_bson_numeric(result)
    if has_decimal:
        return BsonDecimal128(_normalize_decimal128(_numeric_to_decimal(unwrapped)))
    if has_double:
        return BsonDouble(float(unwrapped)) if preserve_wrappers else float(unwrapped)
    if not preserve_wrappers:
        return result
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
