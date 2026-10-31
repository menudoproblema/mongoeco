"""Native Decimal128 arithmetic, accumulator and truth-value witnesses."""

from bson import Decimal128


SEED = [
    {
        "_id": 1,
        "x": Decimal128("1.234567890123456789012345678901234"),
        "y": Decimal128("3"),
    },
    {"_id": 2, "x": Decimal128("2"), "y": Decimal128("7")},
]
TRUTH_VALUES = tuple(Decimal128(v) for v in ("0", "-0", "1", "NaN", "Infinity"))
FIELDS = {
    "add": {"$add": ["$x", "$y"]},
    "subtract": {"$subtract": ["$x", "$y"]},
    "multiply": {"$multiply": ["$x", "$y"]},
    "divide": {"$divide": ["$x", "$y"]},
    "mod": {"$mod": ["$x", "$y"]},
    "sum": {"$sum": ["$x", "$y"]},
    "avg": {"$avg": ["$x", "$y"]},
    "doublePromotion": {"$add": [Decimal128("0"), 1.2345678901234567]},
    "doubleMultiply": {"$multiply": [Decimal128("1"), 1.2345678901234567]},
    "doubleDivide": {"$divide": [Decimal128("1"), 1.2345678901234567]},
    "doubleSum": {"$sum": [1.2345678901234567, Decimal128("0")]},
    "doubleAvg": {"$avg": [1.2345678901234567, Decimal128("0")]},
}


def arithmetic(collection):
    return list(collection.aggregate([{"$project": FIELDS}, {"$sort": {"_id": 1}}]))


def groups(collection):
    mixed = {"$cond": [{"$eq": ["$_id", 1]}, 1.2345678901234567, Decimal128("0")]}
    return list(
        collection.aggregate(
            [
                {
                    "$group": {
                        "_id": None,
                        "sum": {"$sum": "$x"},
                        "avg": {"$avg": "$x"},
                        "mixedAvg": {"$avg": mixed},
                        "mixedSum": {"$sum": mixed},
                    }
                },
                {"$set": {"avgType": {"$type": "$avg"}, "sumType": {"$type": "$sum"}}},
            ]
        )
    )


def windows(collection):
    return list(
        collection.aggregate(
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"_id": 1},
                        "output": {
                            "avg": {
                                "$avg": "$x",
                                "window": {"documents": ["unbounded", "current"]},
                            },
                            "sum": {
                                "$sum": "$x",
                                "window": {"documents": ["unbounded", "current"]},
                            },
                        },
                    }
                },
                {"$project": {"avg": 1, "sum": 1, "avgType": {"$type": "$avg"}}},
            ]
        )
    )


def pipeline_write(collection):
    result = collection.update_many({}, [{"$set": FIELDS}])
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "documents": list(collection.aggregate([{"$sort": {"_id": 1}}])),
    }


def truth_values(collection):
    return list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "truth": [
                            {"$cond": [{"$literal": v}, True, False]}
                            for v in TRUTH_VALUES
                        ],
                        "and": [
                            {"$and": [{"$literal": v}, True]} for v in TRUTH_VALUES
                        ],
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )


def bool_conversion(collection):
    return list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "cast": [{"$toBool": {"$literal": v}} for v in TRUTH_VALUES]
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )


SEMANTIC_DECIMAL_CASES = (
    ("semantic_decimal_arithmetic", arithmetic),
    ("semantic_decimal_group", groups),
    ("semantic_decimal_window", windows),
    ("semantic_decimal_pipeline_write", pipeline_write),
    ("semantic_decimal_truth", truth_values),
    ("semantic_decimal_bool_conversion", bool_conversion),
)

EDGE_PAIRS = (
    ("9.999999999999999999999999999999999E+6144", "2"),
    ("1E-6176", "2"),
    ("-0", "2"),
    ("Infinity", "2"),
    ("0", "Infinity"),
    ("NaN", "1"),
)
EDGE_SEED = [
    {"_id": i, "x": Decimal128(x), "y": Decimal128(y)}
    for i, (x, y) in enumerate(EDGE_PAIRS)
]
SEMANTIC_DECIMAL_EDGE_CASES = (
    ("semantic_decimal_edge_arithmetic", arithmetic),
    ("semantic_decimal_edge_write", pipeline_write),
)
