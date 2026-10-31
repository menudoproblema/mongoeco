"""Native arithmetic promotion and strict write overflow observations."""

from bson import Decimal128, Int64


PAIRS = (
    ("int_max", 2**31 - 1, 1),
    ("int_min", -(2**31), -1),
    ("long_small", Int64(4), 2),
    ("long_max", Int64(2**63 - 1), 1),
    ("long_min", Int64(-(2**63)), -1),
    ("sum_rounding", Int64(2**63 - 1), Int64(1025)),
    ("subtract_overflow", Int64(2**63 - 1), Int64(-(2**63))),
    ("subtract_min", Int64(-(2**63)), Int64(2**63 - 1)),
    ("multiply", Int64(2**62), 4),
    ("multiply_negative", Int64(-(2**62)), 4),
    ("long_exact", Int64(2**53 + 1), 1),
    ("mixed_double", Int64(2**63 - 1), 2.0),
    ("mixed_decimal", Int64(2**63 - 1), Decimal128("2")),
    ("double_overflow", 1e308, 2),
    ("mod_integral_double", Int64(7), 2.0),
    ("mod_int_double", 7, 2.0),
    ("mod_fractional_double", Int64(7), 2.5),
    ("mod_negative_double", Int64(-(2**63) + 1), 2.0),
    ("mod_boundary_double", Int64(2**63 - 1), float(2**63)),
    ("mod_outside_double", Int64(2**63 - 1), float(2**63 + 2048)),
)
SEED = [{"_id": name, "a": a, "b": b} for name, a, b in PAIRS]
FIELDS = {
    op: {"$" + op: ["$a", "$b"]}
    for op in ("add", "subtract", "multiply", "divide", "mod")
}
FIELDS.update(
    {
        "addDecimalLate": {"$add": ["$a", "$b", Decimal128("0")]},
        "addDecimalEarly": {"$add": [Decimal128("0"), "$a", "$b"]},
        "multiplyDecimalLate": {"$multiply": ["$a", "$b", Decimal128("1")]},
        "multiplyDecimalEarly": {"$multiply": [Decimal128("1"), "$a", "$b"]},
    }
)
TYPES = {name + "Type": {"$type": "$" + name} for name in FIELDS}
SEQUENCES = (
    ("int_cancel", [2**31 - 1, 1, -(2**31 - 1)]),
    ("long_cancel", [Int64(2**63 - 1), 1, Int64(-(2**63 - 1))]),
    ("overflow_decimal_last", [Int64(2**63 - 1), 1, Decimal128("0")]),
    ("overflow_decimal_first", [Decimal128("0"), Int64(2**63 - 1), 1]),
    ("double_cancel", [1e16, 1.0, -1e16]),
    ("late_double", [Int64(2**63 - 1), 1, 1.0]),
)


def arithmetic(collection):
    return list(
        collection.aggregate(
            [
                {"$project": FIELDS},
                {"$set": TYPES},
                {"$sort": {"_id": 1}},
            ]
        )
    )


def variadic(collection):
    expressions = {
        name: {"$add": [{"$literal": value} for value in values]}
        for name, values in SEQUENCES
    }
    expressions.update(
        {
            "multiply_zero": {"$multiply": [Int64(2**63 - 1), 2, 0]},
            "multiply_int_zero": {"$multiply": [2**30, 4, 0]},
            "multiply_decimal_last": {
                "$multiply": [Int64(2**63 - 1), 2, Decimal128("1")]
            },
            "multiply_decimal_first": {
                "$multiply": [Decimal128("1"), Int64(2**63 - 1), 2]
            },
        }
    )
    return list(
        collection.aggregate(
            [
                {"$sort": {"_id": 1}},
                {"$limit": 1},
                {"$project": expressions},
                {
                    "$set": {
                        name + "Type": {"$type": "$" + name} for name in expressions
                    }
                },
            ]
        )
    )


def pipeline_write(collection):
    result = collection.update_many({}, [{"$set": FIELDS}])
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "documents": list(
            collection.aggregate(
                [
                    {"$set": TYPES},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
    }


SEMANTIC_OVERFLOW_CASES = (
    ("semantic_overflow_arithmetic", arithmetic),
    ("semantic_overflow_variadic", variadic),
    ("semantic_overflow_pipeline_write", pipeline_write),
)

NUMERIC_CONTROL_SEED = [
    {
        "_id": f"{kind}_{value}",
        "x": constructor(str(value) if kind == "decimal" else value),
    }
    for kind, constructor in (
        ("int", int),
        ("long", Int64),
        ("double", float),
        ("decimal", Decimal128),
    )
    for value in (0, 1, 4, 9)
]


def unary_numeric_controls(collection):
    expressions = {
        "sqrt": {"$sqrt": "$x"},
        "exp": {"$exp": "$x"},
        "ln": {"$ln": {"$add": ["$x", 1]}},
        "log10": {"$log10": {"$add": ["$x", 1]}},
    }
    return list(
        collection.aggregate(
            [
                {"$project": expressions},
                {
                    "$set": {
                        name + "Type": {"$type": "$" + name} for name in expressions
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )


def remainder_edges(collection):
    expressions = {
        name: {"$mod": [left, right]}
        for name, left, right in (
            ("negative_integral", -7.0, 2.0),
            ("negative_fractional", -7.0, 2.5),
            ("negative_zero", -0.0, 2.0),
            ("positive_zero", 0.0, -2.0),
            ("negative_divisor", 7.0, -2.0),
            ("infinite_divisor", 1.0, float("inf")),
            ("negative_infinite_divisor", 1.0, -float("inf")),
            ("infinite_dividend", float("inf"), 2.0),
            ("negative_infinite_dividend", -float("inf"), 2.0),
            ("nan_dividend", float("nan"), 2.0),
            ("nan_divisor", 1.0, float("nan")),
            ("long_zero", Int64(-(2**63) + 1), 2.0),
            ("long_fractional", Int64(-(2**63) + 1), 2.5),
            ("long_infinite_divisor", Int64(7), float("inf")),
            ("large_quotient", 1e308, 3.0),
            ("tiny_divisor", 1.0, 1e-300),
            ("negative_tiny_divisor", -1.0, 1e-300),
        )
    }
    return list(
        collection.aggregate(
            [
                {"$limit": 1},
                {"$project": {"_id": 0, **expressions}},
                {
                    "$set": {
                        name + "Type": {"$type": "$" + name}
                        for name in expressions
                    }
                },
            ]
        )
    )


SEMANTIC_NUMERIC_CONTROL_CASES = (
    ("semantic_overflow_numeric_controls", unary_numeric_controls),
    ("semantic_overflow_remainder_edges", remainder_edges),
)
