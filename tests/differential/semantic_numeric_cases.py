"""Native witnesses for numeric equality and its hash/index consumers."""

from bson import Decimal128, Int64


VALUES = (
    0.1,
    Decimal128("0.1"),
    0.5,
    Decimal128("0.5"),
    Int64(2**53 + 1),
    float(2**53 + 1),
    Decimal128(str(2**53 + 1)),
    1,
    1.0,
    Decimal128("1"),
    True,
    -0.0,
    Decimal128("-0"),
)
SEED = [{"_id": i, "v": value} for i, value in enumerate(VALUES)]


def comparisons(collection):
    return list(
        collection.aggregate(
            [
                {"$sort": {"_id": 1}},
                {
                    "$project": {
                        "v": 1,
                        "comparisons": [
                            {
                                "eq": {"$eq": ["$v", {"$literal": value}]},
                                "cmp": {"$cmp": ["$v", {"$literal": value}]},
                            }
                            for value in VALUES
                        ],
                    }
                },
            ]
        )
    )


def groups(collection):
    return list(
        collection.aggregate(
            [
                {"$sort": {"_id": 1}},
                {"$group": {"_id": "$v", "ids": {"$push": "$_id"}}},
                {"$sort": {"ids.0": 1}},
            ]
        )
    )


def lookups(collection):
    return list(
        collection.aggregate(
            [
                {
                    "$lookup": {
                        "from": collection.name,
                        "localField": "v",
                        "foreignField": "v",
                        "as": "matches",
                    }
                },
                {"$project": {"ids": "$matches._id"}},
                {"$sort": {"_id": 1}},
            ]
        )
    )


def unique_index(collection):
    collection.delete_many({})
    collection.create_index("v", unique=True)
    results = []
    for left, right in (
        (0.1, Decimal128("0.1")),
        (0.5, Decimal128("0.5")),
        (Int64(2**53 + 1), float(2**53 + 1)),
        (Int64(2**53 + 1), Decimal128(str(2**53 + 1))),
        (1, 1.0),
        (True, 1),
        (-0.0, Decimal128("-0")),
    ):
        collection.delete_many({})
        collection.insert_one({"_id": 1, "v": left})
        try:
            collection.insert_one({"_id": 2, "v": right})
        except Exception as exc:
            outcome = {"error": type(exc).__name__, "code": getattr(exc, "code", None)}
        else:
            outcome = {"ok": True}
        results.append(
            {
                "outcome": outcome,
                "documents": list(collection.aggregate([{"$sort": {"_id": 1}}])),
            }
        )
    return results


SEMANTIC_NUMERIC_CASES = (
    ("semantic_numeric_comparisons", comparisons),
    ("semantic_numeric_groups", groups),
    ("semantic_numeric_lookups", lookups),
    ("semantic_numeric_unique_index", unique_index),
)
