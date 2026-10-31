"""Native witnesses for Decimal128 functions beyond the basic binary operators."""

from bson import Decimal128, Int64


SEED = [
    {"_id": i, "x": Decimal128(value)}
    for i, value in enumerate(
        (
            "1.234567890123456789012345678901234",
            "2",
            "3",
            "10",
            "0.1",
            "0.9999999999999999999999999999999999",
            "1.000000000000000000000000000000001",
            "100",
            "0.000001",
            "10000",
        )
    )
]
FIELDS = {
    "abs": {"$abs": {"$multiply": ["$x", -1]}},
    "exp": {"$exp": "$x"},
    "ln": {"$ln": "$x"},
    "log10": {"$log10": "$x"},
    "sqrt": {"$sqrt": "$x"},
    "log": {"$log": ["$x", Decimal128("3")]},
    "log2": {"$log": ["$x", 2]},
    "logBase10": {"$log": ["$x", 10]},
    "pow": {"$pow": ["$x", 2]},
    "powOdd": {"$pow": ["$x", 3]},
    "powZero": {"$pow": ["$x", 0]},
    "powFraction": {"$pow": ["$x", Decimal128("0.5")]},
    "round": {"$round": ["$x", 25]},
    "trunc": {"$trunc": ["$x", 25]},
    "roundInteger": {"$round": ["$x", 0]},
    "floor": {"$floor": "$x"},
    "ceil": {"$ceil": "$x"},
}


def advanced(collection):
    return list(collection.aggregate([{"$project": FIELDS}, {"$sort": {"_id": 1}}]))


def pipeline_write(collection):
    result = collection.update_many({}, [{"$set": FIELDS}])
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "documents": list(collection.aggregate([{"$sort": {"_id": 1}}])),
    }


def domains(collection):
    results = []
    for expression in (
        {"$sqrt": Decimal128("-1")},
        {"$ln": Decimal128("-1")},
        {"$log10": Decimal128("0")},
        {"$log": [Decimal128("2"), Decimal128("1")]},
        {"$pow": [Decimal128("0"), Decimal128("-1")]},
    ):
        try:
            list(collection.aggregate([{"$project": {"value": expression}}]))
        except Exception as exc:
            results.append(
                {"error": type(exc).__name__, "code": getattr(exc, "code", None)}
            )
        else:
            results.append({"ok": True})
    results.append(list(collection.aggregate([{"$sort": {"_id": 1}}])))
    return results


EDGE_SEED = [
    {"_id": i, "x": Decimal128(value)}
    for i, value in enumerate(
        (
            "NaN",
            "Infinity",
            "-Infinity",
            "-0",
            "1E+6144",
            "1E-6176",
            "-1.234567890123456789012345678901234",
            "12000",
            "-12000",
            "-1E6144",
        )
    )
]


def edges(collection):
    fields = {
        name: value
        for name, value in FIELDS.items()
        if name
        in {
            "exp",
            "pow",
            "powOdd",
            "powZero",
            "round",
            "trunc",
            "roundInteger",
            "floor",
            "ceil",
        }
    }
    fields["abs"] = {"$abs": "$x"}
    fields["sqrtAbs"] = {"$sqrt": {"$abs": "$x"}}
    return list(collection.aggregate([{"$project": fields}, {"$sort": {"_id": 1}}]))


def edge_logs(collection):
    return list(
        collection.aggregate(
            [
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {"$gt": ["$x", 0]},
                                {"$eq": ["$x", Decimal128("NaN")]},
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        name: FIELDS[name]
                        for name in ("ln", "log10", "log", "log2", "logBase10")
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )


def power_limits(collection):
    pairs = (
        ("NaN", "0"),
        ("1", "NaN"),
        ("0", "0"),
        ("-1", "0.5"),
        ("-2", "3"),
        ("-2", "-3"),
        ("2", "Infinity"),
        ("0.5", "-Infinity"),
        ("10", "0"),
        ("2", "15000"),
        ("2", "-21000"),
        ("10", "6144"),
        ("10", "-6176"),
        ("10", "0.5"),
    )
    return list(
        collection.aggregate(
            [
                {"$limit": 1},
                {
                    "$project": {
                        "values": [
                            {"$pow": [Decimal128(x), Decimal128(y)]} for x, y in pairs
                        ]
                    }
                },
            ]
        )
    )


SEMANTIC_DECIMAL_ADVANCED_CASES = (
    ("semantic_decimal_advanced_functions", advanced),
    ("semantic_decimal_advanced_write", pipeline_write),
    ("semantic_decimal_advanced_domains", domains),
    ("semantic_decimal_advanced_power_limits", power_limits),
)
SEMANTIC_DECIMAL_ADVANCED_EDGE_CASES = (
    ("semantic_decimal_advanced_edges", edges),
    ("semantic_decimal_advanced_edge_logs", edge_logs),
)


def _generated_seed():
    state = 104729
    documents = []
    for i in range(32):
        state = (state * 48271) % 2147483647
        coefficient = state * 10**24 + state**3 % 10**24
        documents.append({"_id": i, "x": Decimal128(f"{coefficient}E-32")})
    return documents


GENERATED_SEED = _generated_seed()
SEMANTIC_DECIMAL_GENERATED_CASES = (("semantic_decimal_advanced_generated", advanced),)

CONTROL_SEED = [
    {"_id": i, "x": value} for i, value in enumerate((2, 2.0, Int64(2), 2.675))
]
SEMANTIC_DECIMAL_CONTROL_CASES = (
    ("semantic_decimal_advanced_numeric_controls", advanced),
)
