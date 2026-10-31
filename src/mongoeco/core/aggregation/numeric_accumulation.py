"""Fixed-size numeric state shared by sum and average consumers.

The two floating parts use the standard error-free TwoSum/FastTwoSum
transformations. Decimal inputs accumulate separately until finalization;
operand traversal and selection belong to the caller's expression context.
"""

import math

from dataclasses import dataclass, field
from decimal import Decimal, localcontext

from mongoeco.core.bson_scalars import (
    _DECIMAL128_ARITHMETIC_CONTEXT,
    INT32_MAX,
    INT32_MIN,
    INT64_MAX,
    INT64_MIN,
    BsonDecimal128,
    BsonDouble,
    BsonInt64,
    wrap_bson_numeric,
)


@dataclass(slots=True)
class NumericSum:
    high: float = 0.0
    low: float = 0.0
    simple: float = 0.0
    kind: str = "int"
    decimal_total: Decimal | None = None

    def _add_double(self, operand: float) -> None:
        self.simple += operand
        # FastTwoSum combines the next operand with the previous remainder.
        combined = operand + self.low
        remainder = self.low - (combined - operand)
        # TwoSum retains the rounding error even after opposite-sign cancellation.
        rounded = self.high + combined
        recovered = rounded - combined
        residual = (self.high - recovered) + (combined - (rounded - recovered))
        self.high = rounded
        self.low = remainder + residual

    def add(self, value: object) -> bool:
        numeric = wrap_bson_numeric(value)
        if numeric is None:
            return False
        if isinstance(numeric, BsonDecimal128):
            with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
                self.decimal_total = context.add(
                    self.decimal_total
                    if self.decimal_total is not None
                    else Decimal(0),
                    numeric.value,
                )
        elif isinstance(numeric, BsonDouble):
            self.kind = "double"
            self._add_double(numeric.value)
        elif isinstance(numeric, BsonInt64):
            if self.kind != "double":
                self.kind = "long"
            # Each part is exactly representable as double, including negative int64.
            integer = numeric.value
            high = (abs(integer) // (1 << 32)) * (1 << 32)
            if integer < 0:
                high = -high
            self._add_double(float(integer - high))
            self._add_double(float(high))
        else:
            self._add_double(float(numeric.value))
        return True

    def _double(self) -> float:
        return self.simple if math.isnan(self.high) else self.high

    def _decimal(self) -> Decimal:
        with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
            non_decimal = context.create_decimal_from_float(
                self.high if math.isfinite(self.high) else self.simple
            )
            if math.isfinite(self.high):
                non_decimal = context.add(
                    non_decimal, context.create_decimal_from_float(self.low)
                )
            return context.add(
                self.decimal_total if self.decimal_total is not None else Decimal(0),
                non_decimal,
            )

    def sum(self) -> object:
        if self.decimal_total is not None:
            return BsonDecimal128(self._decimal())
        if self.kind == "double" or not math.isfinite(self.high):
            return self._double()
        integer = int(self.high) + int(self.low)
        if not INT64_MIN <= integer <= INT64_MAX:
            return self._double()
        if self.kind == "long" or not INT32_MIN <= integer <= INT32_MAX:
            return BsonInt64(integer)
        return integer

    def average(self, count: int, *, finalize_sum: bool = False) -> object:
        if not count:
            return None
        if self.decimal_total is not None:
            with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
                return BsonDecimal128(context.divide(self._decimal(), Decimal(count)))
        if finalize_sum and self.kind != "double":
            total = self.sum()
            return (total.value if isinstance(total, BsonInt64) else total) / count
        return self._double() / count


@dataclass(slots=True)
class RemovableNumericSum:
    """Window adapter retaining arithmetic history and current-member counts."""

    total: NumericSum = field(default_factory=NumericSum)
    count: int = 0
    doubles: int = 0
    decimals: int = 0
    nan: int = 0
    positive_infinity: int = 0
    negative_infinity: int = 0

    def update(self, value: object, quantity: int) -> None:
        numeric = wrap_bson_numeric(value)
        if numeric is None:
            return
        self.count += quantity
        decimal_input = isinstance(numeric, BsonDecimal128)
        double_input = isinstance(numeric, BsonDouble)
        self.decimals += quantity if decimal_input else 0
        self.doubles += quantity if double_input else 0
        operand = numeric.value
        if decimal_input or double_input:
            is_nan = operand.is_nan() if decimal_input else math.isnan(operand)
            finite = operand.is_finite() if decimal_input else math.isfinite(operand)
            if is_nan:
                self.nan += quantity
                return
            if not finite:
                if operand > 0:
                    self.positive_infinity += quantity
                else:
                    self.negative_infinity += quantity
                return
        if quantity == 1:
            self.total.add(numeric)
        elif decimal_input:
            self.total.add(BsonDecimal128(operand.copy_negate()))
        elif double_input:
            self.total.add(-operand)
        elif operand in (INT32_MIN, INT64_MIN):
            maximum = INT64_MAX if isinstance(numeric, BsonInt64) else INT32_MAX
            self.total.add(
                BsonInt64(maximum) if isinstance(numeric, BsonInt64) else maximum
            )
            self.total.add(1)
        else:
            inverse = -operand
            self.total.add(
                BsonInt64(inverse) if isinstance(numeric, BsonInt64) else inverse
            )

    def sum(self) -> object:
        special = (
            math.nan
            if self.nan or (self.positive_infinity and self.negative_infinity)
            else math.inf
            if self.positive_infinity
            else -math.inf
            if self.negative_infinity
            else None
        )
        if special is not None:
            return BsonDecimal128(Decimal(str(special))) if self.decimals else special
        result = self.total.sum()
        if isinstance(result, BsonDecimal128) and not self.decimals:
            if self.doubles:
                return float(result.value)
            if result.value.is_finite():
                integer = int(result.value)
                if INT64_MIN <= integer <= INT64_MAX:
                    return _narrow_integer(integer)
            return float(result.value)
        if (
            isinstance(result, float)
            and not self.doubles
            and INT64_MIN <= result < float(INT64_MAX)
        ):
            return _narrow_integer(round(result))
        if isinstance(result, BsonInt64):
            result = _narrow_integer(result.value)
        return result

    def average(self) -> object:
        if not self.count:
            return None
        total = self.sum()
        if isinstance(total, BsonDecimal128):
            with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
                return BsonDecimal128(context.divide(total.value, Decimal(self.count)))
        return (total.value if isinstance(total, BsonInt64) else total) / self.count


def _narrow_integer(value: int) -> object:
    return value if INT32_MIN <= value <= INT32_MAX else BsonInt64(value)
