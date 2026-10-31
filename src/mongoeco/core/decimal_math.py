"""Portable Decimal128 transcendental steps used by MongoDB's Intel BID library.

The binary128 intermediates matter: a correctly rounded Decimal.ln/log10/exp
alone does not reproduce their composition. Fractions retain those finite
intermediates, with no native dependency or application decimal-context state.
See resources/Intel-Decimal-Math-LICENSE.txt for the adapted BID algorithms.
"""

from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Context, Decimal, localcontext
from fractions import Fraction

from mongoeco.core.bson_scalars import _DECIMAL128_ARITHMETIC_CONTEXT


_WORK_CONTEXT = Context(
    prec=100, Emin=-999999, Emax=999999, rounding=ROUND_HALF_EVEN, traps=[]
)
_BINARY_RADIX = 2
_DECIMAL_RADIX = 10
_EXP_SPLIT = 11000
_EXP10_BOUND = 6400
_ZERO = Decimal("0E-6176")
_EXP_11000 = Decimal("1.735005819277738509638425077349642E+4777")
_EXP_MINUS_11000 = Decimal("5.763669429168183696272822960961269E-4778")


def _binary128(value: Fraction, *, nearest: bool = True) -> Fraction:
    numerator, denominator = value.numerator, value.denominator
    if not numerator:
        return Fraction(0)
    sign = -1 if numerator < 0 else 1
    numerator = abs(numerator)
    exponent = numerator.bit_length() - denominator.bit_length()
    below = (
        numerator < denominator << exponent
        if exponent >= 0
        else numerator << -exponent < denominator
    )
    if below:
        exponent -= 1
    shift = 112 - max(exponent, -16382)
    if shift >= 0:
        numerator <<= shift
    else:
        denominator <<= -shift
    coefficient, remainder = divmod(numerator, denominator)
    if nearest and (
        remainder * 2 > denominator
        or (remainder * 2 == denominator and coefficient % 2)
    ):
        coefficient += 1
    return (
        Fraction(sign * coefficient, 1 << shift)
        if shift >= 0
        else Fraction(sign * (coefficient << -shift))
    )


def _constant(bits: int) -> Fraction:
    exponent = (bits >> 112) - 16383 - 112
    coefficient = (bits & ((1 << 112) - 1)) + (1 << 112)
    return (
        Fraction(coefficient << exponent)
        if exponent >= 0
        else Fraction(coefficient, 1 << -exponent)
    )


_INV_LN10 = _constant(0x3FFDBCB7B1526E50E32A6AB7555F5A68)
_INV_LN2 = _constant(0x3FFF71547652B82FE1777D0FFDA0D23A)
_SCALE_LN10 = _constant(0x400C4135EB3929FBA719F2C946D2D728)
_SCALE_LOG2_10 = _constant(0x400CCF68B235391208F6437DD4607B55)
_LN2 = _constant(0x3FFE62E42FEFA39EF35793C7673007E6)


def _decimal(value: Fraction) -> Decimal:
    return _WORK_CONTEXT.divide(Decimal(value.numerator), Decimal(value.denominator))


def _binary_log(value: Fraction, base: int) -> Fraction:
    result = _WORK_CONTEXT.ln(_decimal(value))
    if base == _BINARY_RADIX:
        result = _WORK_CONTEXT.divide(result, _WORK_CONTEXT.ln(Decimal(2)))
    return _binary128(Fraction(result))


def decimal_log(value: Decimal, *, base: int = 0) -> Decimal:
    """BID natural/log2/log10 steps, including near-one correction and scaling."""
    with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
        if not value.is_finite() or value <= 0:
            return context.ln(value)
        scale = Fraction(0)
        if value > Decimal("1E4464"):
            value = context.multiply(value, Decimal("1E-4464"))
            scale = (
                Fraction(4464)
                if base == _DECIMAL_RADIX
                else _SCALE_LOG2_10
                if base == _BINARY_RADIX
                else _SCALE_LN10
            )
        elif value < Decimal("1E-4464"):
            value = context.multiply(value, Decimal("1E4464"))
            scale = -(
                Fraction(4464)
                if base == _DECIMAL_RADIX
                else _SCALE_LOG2_10
                if base == _BINARY_RADIX
                else _SCALE_LN10
            )
        binary = _binary128(Fraction(value))
        result = _binary_log(binary, base)
        difference = _binary128(binary - 1)
        if not scale and abs(difference) < Fraction(1, 2):
            decimal_difference = context.subtract(value, Decimal(1))
            correction = _binary128(
                difference - _binary128(Fraction(decimal_difference))
            )
            if base == _BINARY_RADIX:
                correction = _binary128(correction * _INV_LN2)
            correction = _binary128(correction / binary)
            result = _binary128(result - correction)
        if base == _DECIMAL_RADIX:
            result = _binary128(result * _INV_LN10)
        return context.create_decimal(_decimal(_binary128(result + scale)))


def decimal_exp(value: Decimal, *, base: int = 0) -> Decimal:
    """BID exponential with a two-part binary128 input and decimal scaling."""
    with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
        if not value.is_finite() or value.is_zero():
            return (
                context.power(Decimal(2), value)
                if base == _BINARY_RADIX
                else context.exp(value)
            )
        limit = 25000 if base == _BINARY_RADIX else 15000
        if value > limit:
            return Decimal("Infinity")
        if value < -limit:
            return _ZERO
        # BID's two-part conversion truncates both parts, rather than rounding
        # the high part as its ordinary one-part conversion does.
        high = _binary128(Fraction(value), nearest=False)
        low = _binary128(Fraction(value) - high, nearest=False)
        scale = None
        if high > _EXP_SPLIT:
            high = _binary128(high - 11000)
            scale = (
                context.power(Decimal(2), 11000)
                if base == _BINARY_RADIX
                else _EXP_11000
            )
        elif high < -_EXP_SPLIT:
            high = _binary128(high + 11000)
            scale = (
                context.power(Decimal(2), -11000)
                if base == _BINARY_RADIX
                else _EXP_MINUS_11000
            )
        precise = (
            _WORK_CONTEXT.power(Decimal(2), _decimal(high))
            if base == _BINARY_RADIX
            else _WORK_CONTEXT.exp(_decimal(high))
        )
        result = _binary128(Fraction(precise))
        correction = _binary128(result * _LN2) if base == _BINARY_RADIX else result
        result = _binary128(result + _binary128(correction * low))
        converted = context.create_decimal(_decimal(result))
        return context.multiply(converted, scale) if scale is not None else converted


def decimal_pow(left: Decimal, right: Decimal) -> Decimal:
    """Keep the BID dominant exponential, residual correction and result scale."""
    with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
        if left == _DECIMAL_RADIX:
            result = _exp10(right)
        elif left == _BINARY_RADIX:
            result = decimal_exp(right, base=2)
        elif right.is_zero() or left == 1:
            result = Decimal(1)
        elif not left.is_finite() or not right.is_finite() or left.is_zero():
            result = context.power(left, right)
        elif left < 0 and right != right.to_integral_value():
            result = Decimal("NaN")
        else:
            with localcontext(_WORK_CONTEXT):
                logarithm = left.copy_abs().ln()
                high = logarithm.quantize(Decimal("1E-28"), rounding=ROUND_DOWN)
                low = (logarithm - high).quantize(Decimal("1E-47"), rounding=ROUND_DOWN)
            if logarithm.copy_abs() < Decimal("3.6893488147419103232E-9"):
                delta = context.subtract(left.copy_abs(), Decimal(1))
                square = context.multiply(delta, delta)
                series = context.multiply(Decimal("-0.5"), square)
                cube = context.multiply(delta, square)
                series = context.add(
                    series,
                    context.multiply(context.divide(Decimal(1), Decimal(3)), cube),
                )
                fourth = context.multiply(delta, cube)
                series = context.add(series, context.multiply(Decimal("-0.25"), fourth))
                high = context.add(delta, series)
                low = context.subtract(context.subtract(high, delta), series)
            dominant = context.multiply(right, high)
            residual = context.fma(right, high, dominant.copy_negate())
            low = context.fma(right, low, residual)
            result = decimal_exp(dominant)
            if result.is_finite() and not result.is_zero():
                result = context.fma(result, low, result)
                if left < 0 and int(right) % 2:
                    result = result.copy_negate()
        return context.add(result, _ZERO)


def _exp10(value: Decimal) -> Decimal:
    with localcontext(_DECIMAL128_ARITHMETIC_CONTEXT) as context:
        if not value.is_finite():
            return context.power(Decimal(10), value)
        # Intel BID's exp10 zero branch returns zero (despite its comment).
        # MongoDB routes Decimal128 base-10 power through that branch.
        if value.is_zero():
            return Decimal(0)
        if value.copy_abs() > _EXP10_BOUND:
            return _ZERO if value < 0 else Decimal("Infinity")
        integer = int(value.to_integral_value())
        fraction = context.subtract(value, Decimal(integer))
        binary = _binary128(Fraction(fraction))
        precise = _WORK_CONTEXT.power(Decimal(10), _decimal(binary))
        converted = context.create_decimal(_decimal(_binary128(Fraction(precise))))
        return context.multiply(converted, Decimal((0, (1,), integer)))
