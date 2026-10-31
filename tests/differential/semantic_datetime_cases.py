"""Native date coordinates, inverse conversion and window consumers (H49)."""

# ruff: noqa: DTZ001 - default BSON dates are naive UTC

from datetime import datetime, timedelta, timezone

from bson import Decimal128, Int64


EPOCH = datetime(1970, 1, 1)
DATES = [
    EPOCH + timedelta(milliseconds=1001),
    EPOCH - timedelta(milliseconds=1001),
    datetime(2038, 4, 22, 20, 55, 58, 687000),
    datetime(1900, 1, 1, 0, 0, 1, 1000),
    datetime(9999, 12, 31, 23, 59, 59, 999000),
    datetime(1, 1, 1),
    datetime(
        2020, 1, 1, 0, 0, 0, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30))
    ),
]
SEED = [{"_id": i, "d": value} for i, value in enumerate(DATES)]


def coordinates(collection):
    return list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "_id": 1,
                        "millis": {"$toLong": "$d"},
                        "subtract": {"$subtract": ["$d", EPOCH]},
                        "diff": {
                            "$dateDiff": {
                                "startDate": EPOCH,
                                "endDate": "$d",
                                "unit": "millisecond",
                            }
                        },
                        "roundtrip": {"$toDate": {"$toLong": "$d"}},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )


def inverse(collection):
    # Native conversion truncates fractional milliseconds toward zero.
    return list(
        collection.aggregate(
            [
                {"$limit": 1},
                {
                    "$project": {
                        "_id": 0,
                        "int32": {
                            "$convert": {
                                "input": 1001,
                                "to": "date",
                                "onError": "invalid-int32",
                            }
                        },
                        **{
                            f"n{i}": {"$toDate": value}
                            for i, value in enumerate(
                                [
                                    Int64(1001),
                                    Int64(-1001),
                                    1001.9,
                                    -1001.9,
                                    0.9,
                                    -0.9,
                                    2155582558687,
                                ]
                            )
                        },
                    }
                },
            ]
        )
    )


def windows(collection):
    collection.delete_many({})
    collection.insert_many(
        [
            {"_id": 1, "d": EPOCH, "y": 0},
            {"_id": 2, "d": EPOCH + timedelta(milliseconds=1001), "y": 1001},
        ]
    )
    return list(
        collection.aggregate(
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"d": 1},
                        "output": {
                            "rate": {
                                "$derivative": {
                                    "input": {"$toLong": "$y"},
                                    "unit": "second",
                                },
                                "window": {"documents": ["unbounded", "current"]},
                            },
                            "area": {
                                "$integral": {
                                    "input": {"$toLong": 1},
                                    "unit": "second",
                                },
                                "window": {"documents": ["unbounded", "current"]},
                            },
                        },
                    }
                },
                {"$project": {"_id": 1, "rate": 1, "area": 1}},
            ]
        )
    )


def arithmetic(collection):
    operands = [
        Int64(1001),
        0.5,
        1.5,
        -0.5,
        -1.5,
        2.5,
        Decimal128("0.5"),
        Decimal128("1.5"),
        Decimal128("2.5"),
        Decimal128("-0.5"),
        Decimal128("-2.5"),
        {"$toLong": 1001},
    ]
    fields = {
        f"{op[1:]}{i}": {op: [EPOCH, value]}
        for op in ("$add", "$subtract")
        for i, value in enumerate(operands)
    }
    fields["successive"] = {"$add": [EPOCH, 0.5, 0.5]}
    fields["dateAddLong"] = {
        "$dateAdd": {
            "startDate": EPOCH,
            "unit": "millisecond",
            "amount": {"$toLong": 1001},
        }
    }
    fields["dateAddDiff"] = {
        "$dateAdd": {
            "startDate": EPOCH,
            "unit": "millisecond",
            "amount": {
                "$dateDiff": {
                    "startDate": EPOCH,
                    "endDate": DATES[0],
                    "unit": "millisecond",
                }
            },
        }
    }
    fields["dateTruncLong"] = {
        "$dateTrunc": {
            "date": DATES[0],
            "unit": "millisecond",
            "binSize": {"$toLong": 2},
        }
    }
    fields["datePartsLong"] = {"$dateFromParts": {"year": {"$toLong": 1970}}}
    zero, one, two = ({"$toLong": n} for n in (0, 1, 2))
    fields["substrLong"] = {"$substrCP": ["abc", zero, two]}
    fields["sliceLong"] = {"$slice": [[1, 2, 3], one, two]}
    fields["sliceCountLong"] = {"$slice": [[1, 2, 3], two]}
    fields["indexArrayLong"] = {"$indexOfArray": [[1, 2, 3], 2, zero, two]}
    fields["indexStringLong"] = {"$indexOfCP": ["abc", "b", zero, two]}
    fields["rangeLong"] = {"$range": [zero, two, one]}
    fields["roundPlaceLong"] = {"$round": [1.234, one]}
    fields["percentileLong"] = {
        "$percentile": {"input": [1, 2, 3], "p": [zero, one], "method": "approximate"}
    }
    for i, date in enumerate(DATES[:2]):
        fields[f"dateTruncAnchor{i}"] = {
            "$dateTrunc": {"date": date, "unit": "millisecond", "binSize": 7}
        }
    return list(
        collection.aggregate([{"$limit": 1}, {"$project": {"_id": 0, **fields}}])
    )


def typed_stage_consumers(collection):
    collection.delete_many({})
    collection.insert_many(
        [
            {"_id": 1, "x": 1, "y": 0},
            {"_id": 2, "x": 2, "y": None},
            {"_id": 3, "x": 3, "y": 4},
        ]
    )
    typed = {"$set": {"x": {"$toLong": "$x"}, "y": {"$toLong": "$y"}}}
    windows = {
        "$setWindowFields": {
            "sortBy": {"x": 1},
            "output": {
                "avg": {"$expMovingAvg": {"input": "$y", "N": 2}},
                "filled": {"$linearFill": "$y"},
            },
        }
    }
    return {
        "windows": list(collection.aggregate([typed, windows, {"$sort": {"_id": 1}}])),
        "fill": list(
            collection.aggregate(
                [
                    typed,
                    {
                        "$fill": {
                            "sortBy": {"x": 1},
                            "output": {"y": {"method": "linear"}},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
        "densify": list(
            collection.aggregate(
                [
                    typed,
                    {
                        "$densify": {
                            "field": "x",
                            "range": {"step": 1, "bounds": "full"},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
    }


def window_errors(collection):
    output = {}
    for operator in ("$derivative", "$integral"):
        for label, value in (("null", None), ("string", "x")):
            collection.delete_many({})
            collection.insert_one({"_id": 1, "x": 1, "y": value})
            try:
                output[f"{operator}_{label}"] = list(
                    collection.aggregate(
                        [
                            {
                                "$setWindowFields": {
                                    "sortBy": {"x": 1},
                                    "output": {
                                        "v": {
                                            operator: {"input": "$y"},
                                            "window": {"documents": [-1, 0]},
                                        }
                                    },
                                }
                            }
                        ]
                    )
                )
            except Exception as error:
                output[f"{operator}_{label}"] = {
                    "exception": type(error).__name__,
                    "code": getattr(error, "code", None),
                }
    return output


SEMANTIC_DATETIME_CASES = (
    ("semantic_datetime_coordinates", coordinates),
    ("semantic_datetime_inverse", inverse),
    ("semantic_datetime_windows", windows),
    ("semantic_datetime_arithmetic", arithmetic),
    ("semantic_datetime_window_errors", window_errors),
    ("semantic_datetime_typed_stage_consumers", typed_stage_consumers),
)
