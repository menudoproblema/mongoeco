"""Preferred-number boundaries for the optional bucketAuto granularity operand."""

from __future__ import annotations

import decimal
import math

from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.aggregation.scalar_expressions import _aggregation_type_name
from mongoeco.core.bson_scalars import (
    DECIMAL128_CONTEXT,
    BsonDecimal128,
    bson_divide,
    bson_multiply,
    is_bson_numeric,
    unwrap_bson_numeric,
    wrap_bson_numeric,
)
from mongoeco.core.server_errors import server_error


# Standard Renard/E preferred-number data, in integer mantissas per decade.
_SERIES = {
    "R5": (10, 16, 25, 40, 63),
    "R10": (100, 125, 160, 200, 250, 315, 400, 500, 630, 800),
    "R20": (
        100,
        112,
        125,
        140,
        160,
        180,
        200,
        224,
        250,
        280,
        315,
        355,
        400,
        450,
        500,
        560,
        630,
        710,
        800,
        900,
    ),
    "R40": (
        100,
        106,
        112,
        118,
        125,
        132,
        140,
        150,
        160,
        170,
        180,
        190,
        200,
        212,
        224,
        236,
        250,
        265,
        280,
        300,
        315,
        355,
        375,
        400,
        425,
        450,
        475,
        500,
        530,
        560,
        600,
        630,
        670,
        710,
        750,
        800,
        850,
        900,
        950,
    ),
    "R80": (
        103,
        109,
        115,
        122,
        128,
        136,
        145,
        155,
        165,
        175,
        185,
        195,
        206,
        218,
        230,
        243,
        258,
        272,
        290,
        307,
        325,
        345,
        365,
        387,
        412,
        437,
        462,
        487,
        515,
        545,
        575,
        615,
        650,
        690,
        730,
        775,
        825,
        875,
        925,
        975,
    ),
    "1-2-5": (10, 20, 50),
    "E6": (10, 15, 22, 33, 47, 68),
    "E12": (10, 12, 15, 18, 22, 27, 33, 39, 47, 56, 68, 82),
    "E24": (
        10,
        11,
        12,
        13,
        15,
        16,
        18,
        20,
        22,
        24,
        27,
        30,
        33,
        36,
        39,
        43,
        47,
        51,
        56,
        62,
        68,
        75,
        82,
        91,
    ),
    "E48": (
        100,
        105,
        110,
        115,
        121,
        127,
        133,
        140,
        147,
        154,
        162,
        169,
        178,
        187,
        196,
        205,
        215,
        226,
        237,
        249,
        261,
        274,
        287,
        301,
        316,
        332,
        348,
        365,
        383,
        402,
        422,
        442,
        464,
        487,
        511,
        536,
        562,
        590,
        619,
        649,
        681,
        715,
        750,
        787,
        825,
        866,
        909,
        953,
    ),
    "E96": (
        100,
        102,
        105,
        107,
        110,
        113,
        115,
        118,
        121,
        124,
        127,
        130,
        133,
        137,
        140,
        143,
        147,
        150,
        154,
        158,
        162,
        165,
        169,
        174,
        178,
        182,
        187,
        191,
        196,
        200,
        205,
        210,
        215,
        221,
        226,
        232,
        237,
        243,
        249,
        255,
        261,
        267,
        274,
        280,
        287,
        294,
        301,
        309,
        316,
        324,
        332,
        340,
        348,
        357,
        365,
        374,
        383,
        392,
        402,
        412,
        422,
        432,
        442,
        453,
        464,
        475,
        487,
        499,
        511,
        523,
        536,
        549,
        562,
        576,
        590,
        604,
        619,
        634,
        649,
        665,
        681,
        698,
        715,
        732,
        750,
        768,
        787,
        806,
        825,
        845,
        866,
        887,
        909,
        931,
        953,
        976,
    ),
    "E192": (
        100,
        101,
        102,
        104,
        105,
        106,
        107,
        109,
        110,
        111,
        113,
        114,
        115,
        117,
        118,
        120,
        121,
        123,
        124,
        126,
        127,
        129,
        130,
        132,
        133,
        135,
        137,
        138,
        140,
        142,
        143,
        145,
        147,
        149,
        150,
        152,
        154,
        156,
        158,
        160,
        162,
        164,
        165,
        167,
        169,
        172,
        174,
        176,
        178,
        180,
        182,
        184,
        187,
        189,
        191,
        193,
        196,
        198,
        200,
        203,
        205,
        208,
        210,
        213,
        215,
        218,
        221,
        223,
        226,
        229,
        232,
        234,
        237,
        240,
        243,
        246,
        249,
        252,
        255,
        258,
        261,
        264,
        267,
        271,
        274,
        277,
        280,
        284,
        287,
        291,
        294,
        298,
        301,
        305,
        309,
        312,
        316,
        320,
        324,
        328,
        332,
        336,
        340,
        344,
        348,
        352,
        357,
        361,
        365,
        370,
        374,
        379,
        383,
        388,
        392,
        397,
        402,
        407,
        412,
        417,
        422,
        427,
        432,
        437,
        442,
        448,
        453,
        459,
        464,
        470,
        475,
        481,
        487,
        493,
        499,
        505,
        511,
        517,
        523,
        530,
        536,
        542,
        549,
        556,
        562,
        569,
        576,
        583,
        590,
        597,
        604,
        612,
        619,
        626,
        634,
        642,
        649,
        657,
        665,
        673,
        681,
        690,
        698,
        706,
        715,
        723,
        732,
        741,
        750,
        759,
        768,
        777,
        787,
        796,
        806,
        816,
        825,
        835,
        845,
        856,
        866,
        876,
        887,
        898,
        909,
        920,
        931,
        942,
        953,
        965,
        976,
        988,
    ),
}


def validate_granularity(name):
    if not isinstance(name, str):
        kind = _aggregation_type_name(name, missing_sentinel=MISSING)
        msg = (
            "The $bucketAuto 'granularity' field must be a string, "
            f"but found type: {kind}"
        )
        raise server_error(
            msg,
            code=40261,
        )
    if name not in _SERIES and name != "POWERSOF2":
        msg = f"Unknown rounding granularity '{name}'"
        raise server_error(msg, code=40257)
    return name


def validate_boundary(value):
    if not is_bson_numeric(value):
        kind = _aggregation_type_name(value, missing_sentinel=MISSING)
        msg = (
            "$bucketAuto can specify a 'granularity' with numeric boundaries only, "
            f"but found a value with type: {kind}"
        )
        raise server_error(
            msg,
            code=40258,
        )
    number = unwrap_bson_numeric(wrap_bson_numeric(value))
    if (isinstance(number, decimal.Decimal) and number.is_nan()) or (
        isinstance(number, float) and math.isnan(number)
    ):
        msg = (
            "$bucketAuto can specify a 'granularity' with numeric boundaries only, "
            "but found a NaN"
        )
        raise server_error(
            msg,
            code=40259,
        )
    if number < 0:
        msg = (
            "$bucketAuto can specify a 'granularity' with non-negative numbers only, "
            "but found a negative number"
        )
        raise server_error(
            msg,
            code=40260,
        )


def round_boundary(value, name, *, upward, checkpoint=None):
    """Choose a strict neighbour, preserving BSON domains and decimal context."""
    if checkpoint is not None:
        checkpoint()
    numeric = wrap_bson_numeric(value)
    number = unwrap_bson_numeric(numeric)
    if float(number) == 0:
        return value
    if name == "POWERSOF2":
        return _power_boundary(numeric, upward=upward)
    if math.isinf(float(number)) or float(number) == 0:
        return value
    return _preferred_boundary(
        numeric, number, name, upward=upward, checkpoint=checkpoint
    )


def _preferred_boundary(numeric, number, name, *, upward, checkpoint):
    scale = (
        BsonDecimal128(decimal.Decimal(1))
        if isinstance(numeric, BsonDecimal128)
        else 1.0
    )
    mantissas = _SERIES[name]

    def scaled(mantissa):
        return (
            bson_multiply(float(mantissa), scale)
            if isinstance(numeric, BsonDecimal128)
            else mantissa * scale
        )

    def raw(v):
        return unwrap_bson_numeric(v)

    def shift(factor):
        nonlocal scale
        if checkpoint is not None:
            checkpoint()
        if isinstance(numeric, BsonDecimal128):
            scale = bson_multiply(scale, 10) if factor > 1 else bson_divide(scale, 10)
        else:
            scale = scale * 10 if factor > 1 else scale / 10

    # Locate the decade by multiplication, retaining binary rounding of the scale.
    if upward:
        while number >= raw(scaled(mantissas[-1])):
            shift(10)
        while number < raw(scaled(mantissas[0])):
            candidate = scaled(mantissas[0])
            shift(0.1)
            if number >= raw(scaled(mantissas[-1])):
                return candidate
        return next(scaled(n) for n in mantissas if raw(scaled(n)) > number)
    while number <= raw(scaled(mantissas[0])):
        shift(0.1)
    if raw(scale) == 0:
        return 0.0
    while number > raw(scaled(mantissas[-1])):
        candidate = scaled(mantissas[-1])
        shift(10)
        if number <= raw(scaled(mantissas[0])):
            return candidate
    return next(scaled(n) for n in reversed(mantissas) if raw(scaled(n)) < number)


def _power_boundary(value, *, upward):
    number = unwrap_bson_numeric(value)
    if not math.isfinite(float(number)):
        msg = "A granularity rounder can only round finite numbers"
        raise server_error(msg, code=11785400)
    if isinstance(number, int):
        exponent = number.bit_length()
        if not upward:
            exponent -= 2 if number & (number - 1) == 0 else 1
    elif isinstance(number, decimal.Decimal):
        with decimal.localcontext(DECIMAL128_CONTEXT):
            logarithm = float(number.ln() / decimal.Decimal(2).ln())
        exponent = math.floor(logarithm) + 1 if upward else math.ceil(logarithm) - 1
    else:
        logarithm = math.log2(number)
        exponent = math.floor(logarithm) + 1 if upward else math.ceil(logarithm) - 1
    # Reuse the existing numeric expression's width, overflow and Decimal128 rules.
    from mongoeco.core.aggregation.runtime import (  # noqa: PLC0415 - runtime imports grouping handlers
        evaluate_expression,
    )

    power = (
        BsonDecimal128(decimal.Decimal(exponent))
        if isinstance(value, BsonDecimal128)
        else exponent
    )
    return wrap_bson_numeric(evaluate_expression({}, {"$pow": [2, power]}))
