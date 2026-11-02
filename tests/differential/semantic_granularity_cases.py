"""Preferred-number bucket witnesses through public collection APIs."""

from functools import partial

from bson import Decimal128, Int64

from tests.support_write_parity import SEMANTIC_ERRORS


SEED = [{"_id": 1, "v": 1}]
SERIES = (
    "R5",
    "R10",
    "R20",
    "R40",
    "R80",
    "1-2-5",
    "E6",
    "E12",
    "E24",
    "E48",
    "E96",
    "E192",
    "POWERSOF2",
)
_FRACTIONAL_DATASET = 5
DATASETS = (
    [0, 1, 2, 5, 6, 10, 23, 100, 101, 200],
    list(range(20)),
    [0, 0, 0],
    [1, 1, 1],
    [1, 2, 3, 4, 5, 8, 16, 32],
    [0.01, 0.1, 0.2, 0.5, 1.0, 1.03, 1.06, 1.1],
)


def buckets(collection, series):
    observed = {}
    for mode, convert in (
        ("plain", lambda x: x),
        ("double", float),
        ("decimal", lambda x: Decimal128(str(x))),
        ("long", Int64),
    ):
        for index, values in enumerate(DATASETS):
            if mode == "long" and index == _FRACTIONAL_DATASET:
                continue
            collection.delete_many({})
            collection.insert_many(
                [{"_id": i, "v": convert(v)} for i, v in enumerate(values)]
            )
            for count in (1, 3, 7):
                observed[f"{mode}-{index}-{count}"] = list(
                    collection.aggregate(
                        [
                            {
                                "$bucketAuto": {
                                    "groupBy": "$v",
                                    "buckets": count,
                                    "granularity": series,
                                    "output": {
                                        "count": {"$sum": 1},
                                        "sum": {"$sum": "$v"},
                                    },
                                }
                            }
                        ]
                    )
                )
    return observed


def _observe_bucket(collection, granularity):
    phase = "invoke"
    try:
        cursor = collection.aggregate(
            [
                {
                    "$bucketAuto": {
                        "groupBy": "$v",
                        "buckets": 3,
                        "granularity": granularity,
                    }
                }
            ]
        )
        phase = "consume"
        return list(cursor)
    except SEMANTIC_ERRORS as error:
        if getattr(error, "code", None) is None:
            raise
        return {"code": error.code, "phase": phase}


def invalid(collection):
    observed = {}
    cases = (
        ("negative", "R5", -1),
        ("null", "R5", None),
        ("string", "R5", "a"),
        ("bool", "R5", True),
        ("array", "R5", []),
        ("nan", "R5", float("nan")),
        ("unknown", "BAD", 1),
        ("type", 5, 1),
    )
    for name, granularity, value in cases:
        collection.delete_many({})
        collection.insert_one({"v": value})
        observed[name] = _observe_bucket(collection, granularity)
    collection.delete_many({})
    observed["unknown_empty"] = _observe_bucket(collection, "BAD")
    observed["type_empty"] = _observe_bucket(collection, 5)
    return observed


def edges(collection, series):
    observed = {}
    values = {
        "int64": [Int64(2**53), Int64(2**53 + 1), Int64(2**63 - 1)],
        "subnormal": [5e-324, 1e-308, 1e-307],
        "large_double": [1e307, 1e308, 1.79e308],
        "decimal_small": [Decimal128("1e-6000"), Decimal128("2e-6000")],
        "decimal_large": [Decimal128("1e6000"), Decimal128("2e6000")],
        "positive_infinity": [1, float("inf")],
        "negative_zero": [-0.0, 0.0, 1],
        "decimal_nan": [Decimal128("NaN")],
    }
    for name, data in values.items():
        collection.delete_many({})
        collection.insert_many([{"_id": i, "v": v} for i, v in enumerate(data)])
        observed[name] = _observe_bucket(collection, series)
    collection.delete_many({})
    observed["empty"] = list(
        collection.aggregate(
            [{"$bucketAuto": {"groupBy": "$v", "buckets": 3, "granularity": series}}]
        )
    )
    return observed


SEMANTIC_GRANULARITY_CASES = (
    *(
        ("semantic_granularity_" + name, partial(buckets, series=name))
        for name in SERIES
    ),
    *(
        ("semantic_granularity_edges_" + name, partial(edges, series=name))
        for name in SERIES
    ),
    ("semantic_granularity_invalid", invalid),
)
