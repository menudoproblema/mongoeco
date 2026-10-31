"""Native compensated sum/average, promotion and operand-context witnesses."""

from bson import Decimal128, Int64


SEQUENCES = (
    ("cancel", [1e16, 1.0, -1e16]),
    ("cancel_reversed", [-1e16, 1.0, 1e16]),
    ("tenths", [0.1] * 10),
    ("low_parts", [1e16, 0.1, 0.2, -1e16]),
    ("long_exact", [Int64(2**63 - 1), Int64(-(2**63 - 1)), Int64(1)]),
    ("long_overflow", [Int64(2**63 - 1), 1]),
    ("long_overflow_cancel", [Int64(2**63 - 1), 1, Int64(-(2**63 - 1))]),
    ("long_underflow", [Int64(-(2**63)), -1]),
    ("long_small", [Int64(1), -1]),
    ("int_promote", [2**31 - 1, 1]),
    ("int_cancel", [2**31 - 1, 1, -(2**31 - 1)]),
    ("mixed_cancel", [1e16, Decimal128("0.1"), Decimal128("0.11"), -1e16]),
    ("decimal_cancel", [Decimal128("1E34"), 1.0, Decimal128("-1E34")]),
    ("decimal_round", [Decimal128("1E34"), Decimal128("1"), Decimal128("-1E34")]),
    ("decimal_last", [1e16, 1.0, -1e16, Decimal128("0")]),
    ("decimal_first", [Decimal128("0"), 1e16, 1.0, -1e16]),
    ("decimal_double", [0.1, 0.2, Decimal128("0")]),
    ("decimal_long", [Int64(2**63 - 1), Decimal128("0")]),
    ("nan", [float("nan"), 1.0]),
    ("infinity", [float("inf"), 1.0]),
    ("infinities", [float("inf"), float("-inf")]),
    ("double_overflow", [1e308, 1e308, -1e308]),
    ("decimal_infinity", [float("inf"), Decimal128("0")]),
    ("decimal_nan", [Decimal128("NaN"), 1]),
    ("negative_zero", [-0.0, -0.0]),
    ("operands", [1, [2, 3], None, False, {}, 1]),
    ("ignored", [None, False, [], {}, "x"]),
    ("empty", []),
    ("remove_nan", [float("nan"), 1.0, 2.0, 3.0, 4.0]),
    ("remove_infinities", [float("inf"), float("-inf"), 1.0, 2.0, 3.0, 4.0]),
    ("remove_decimal_nan", [Decimal128("NaN"), 1, 2, 3, 4]),
    ("remove_decimal", [Decimal128("0.1"), 1.0, 2.0, 3.0, 4]),
    ("remove_long_min", [Int64(-(2**63)), 1, 2, 3, 4]),
    ("remove_int_min", [-(2**31), 1, 2, 3, 4]),
    ("remove_low_parts", [1e16, 1.0, -1e16, 0.0, 1e-16, 2.0, -2.0, 0.0]),
)


def _generated_sequences():
    operands = (
        1e16,
        -1e16,
        0.1,
        1.0,
        -1.0,
        1e-300,
        -1e-300,
        Int64(2**63 - 1),
        Int64(-(2**63)),
        Decimal128("0.1"),
        Decimal128("1E34"),
        Decimal128("-1E34"),
        Decimal128("0"),
    )
    state = 571
    for i in range(8):
        values = []
        for _ in range(9):
            state = (1664525 * state + 1013904223) % (2**32)
            values.append(operands[state % len(operands)])
        yield f"generated_{i}", values


SEED = [
    {"_id": name, "v": values} for name, values in (*SEQUENCES, *_generated_sequences())
]
RESULT_TYPES = {"sType": {"$type": "$s"}, "aType": {"$type": "$a"}}
GROUP_PIPELINE = [
    {"$unwind": {"path": "$v", "includeArrayIndex": "i"}},
    {"$sort": {"_id": 1, "i": 1}},
    {"$group": {"_id": "$_id", "s": {"$sum": "$v"}, "a": {"$avg": "$v"}}},
    {"$set": RESULT_TYPES},
    {"$sort": {"_id": 1}},
]


def expressions(collection):
    return list(
        collection.aggregate(
            [
                {"$project": {"s": {"$sum": "$v"}, "a": {"$avg": "$v"}}},
                {"$set": RESULT_TYPES},
                {"$sort": {"_id": 1}},
            ]
        )
    )


def groups(collection):
    return list(collection.aggregate(GROUP_PIPELINE))


def subpipeline(collection):
    return list(collection.aggregate([{"$facet": {"groups": GROUP_PIPELINE}}]))


def _windows(collection, bounds_key):
    return list(
        collection.aggregate(
            [
                {"$unwind": {"path": "$v", "includeArrayIndex": "i"}},
                {
                    "$setWindowFields": {
                        "partitionBy": "$_id",
                        "sortBy": {"i": 1},
                        "output": {
                            "s": {
                                "$sum": "$v",
                                "window": {bounds_key: ["unbounded", "current"]},
                            },
                            "a": {
                                "$avg": "$v",
                                "window": {bounds_key: ["unbounded", "current"]},
                            },
                            "slidingS": {
                                "$sum": "$v",
                                "window": {bounds_key: [-2, "current"]},
                            },
                            "slidingA": {
                                "$avg": "$v",
                                "window": {bounds_key: [-2, "current"]},
                            },
                        },
                    }
                },
                {"$set": RESULT_TYPES},
                {"$project": {"v": 0}},
                {"$sort": {"_id": 1, "i": 1}},
            ]
        )
    )


def windows(collection):
    return _windows(collection, "documents")


def range_windows(collection):
    return _windows(collection, "range")


def pipeline_write(collection):
    result = collection.update_many(
        {},
        [
            {
                "$set": {
                    "s": {"$sum": "$v"},
                    "a": {"$avg": "$v"},
                }
            }
        ],
    )
    return {
        "matched": result.matched_count,
        "modified": result.modified_count,
        "documents": list(
            collection.aggregate(
                [
                    {"$set": RESULT_TYPES},
                    {"$project": {"v": 0}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
    }


SEMANTIC_SUM_CASES = (
    ("semantic_sum_expressions", expressions),
    ("semantic_sum_group", groups),
    ("semantic_sum_subpipeline", subpipeline),
    ("semantic_sum_windows", windows),
    ("semantic_sum_range_windows", range_windows),
    ("semantic_sum_pipeline_write", pipeline_write),
)


STREAM_SEED = [
    {"_id": position * len(SEED) + index, "g": document["_id"], "v": values[position]}
    for position in range(max(len(document["v"]) for document in SEED))
    for index, document in enumerate(SEED)
    if position < len(values := document["v"])
]
STREAM_PIPELINE = [
    {"$sort": {"_id": 1}},
    {"$group": {"_id": "$g", "s": {"$sum": "$v"}, "a": {"$avg": "$v"}}},
    {"$set": RESULT_TYPES},
    {"$sort": {"_id": 1}},
]


def stream_groups(collection):
    return list(collection.aggregate(STREAM_PIPELINE))


SEMANTIC_SUM_STREAM_CASES = (("semantic_sum_stream_groups", stream_groups),)
