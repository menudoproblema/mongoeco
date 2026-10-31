"""Physical sizes from MongoDB, observed without production sizing helpers."""

import re

from copy import deepcopy
from datetime import UTC, datetime

from bson import (
    Binary,
    Code,
    DBRef,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    Timestamp,
)


SEED = [{"_id": "initial"}]
VALUES = (
    None,
    False,
    True,
    1,
    Int64(1),
    -(2**63),
    1.0,
    -0.0,
    float("nan"),
    Decimal128("1.00"),
    Decimal128("NaN"),
    Decimal128("sNaN"),
    "",
    "á雪\x00",
    b"ab",
    Binary(b"ab", 0),
    Binary(b"ab", 2),
    Binary(bytes(16), 3),
    Binary(bytes(16), 4),
    Binary(bytes(16), 5),
    ObjectId("0123456789abcdef01234567"),
    datetime(2038, 4, 22, tzinfo=UTC),
    Timestamp(1, 2),
    MinKey(),
    MaxKey(),
    Regex("á雪", "imsux"),
    Code("á雪"),
    Code("x", {}),
    Code("x", {"n": Int64(1), "b": Binary(b"ab", 2)}),
    DBRef("c", 1),
    DBRef("c", Int64(1), "db", tenant="t"),
    [],
    list(range(15)),
    {"x": [1, Int64(1), Decimal128("1.00")]},
    {"nested": {"v": Code("x", {"r": DBRef("c", 1)})}},
)


def literal_documents(collection):
    project = {"_id": 0}
    for i, value in enumerate((*VALUES, re.compile("á雪", re.IGNORECASE))):
        project[f"v{i}"] = {"$bsonSize": {"$literal": {"v": deepcopy(value)}}}
    return list(collection.aggregate([{"$project": project}]))


def stored_documents(collection):
    collection.delete_many({})
    collection.insert_many(
        [{"_id": i, "v": deepcopy(value)} for i, value in enumerate(VALUES)]
    )
    return list(
        collection.aggregate(
            [
                {"$project": {"_id": 1, "size": {"$bsonSize": "$$ROOT"}}},
                {"$sort": {"_id": 1}},
            ]
        )
    )


def expression_results(collection):
    return list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "_id": 0,
                        "number_types": {
                            "$bsonSize": {
                                "i": {"$toInt": 1},
                                "l": {"$toLong": 1},
                                "d": {"$toDouble": 1},
                                "n": {"$toDecimal": "1.00"},
                            }
                        },
                        "wrapped_array": {
                            "$bsonSize": {"a": [{"$toLong": 1}, {"$toDecimal": "NaN"}]}
                        },
                        "dbref": {
                            "$bsonSize": {
                                "$literal": DBRef("c", Int64(1), "db", tenant="t")
                            }
                        },
                        "empty": {"$bsonSize": {"$literal": {}}},
                        "null": {"$bsonSize": None},
                        "missing": {"$bsonSize": "$missing"},
                    }
                }
            ]
        )
    )


def argument_validation(collection):
    output = {}
    for i, value in enumerate(
        (
            1,
            True,
            "x",
            [],
            b"a",
            Code("x"),
            Regex("x"),
            datetime(1970, 1, 1, tzinfo=UTC),
            None,
            DBRef("c", 1),
        )
    ):
        try:
            output[str(i)] = {
                "value": list(
                    collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "size": {
                                        "$bsonSize": {"$literal": deepcopy(value)}
                                    },
                                }
                            }
                        ]
                    )
                )
            }
        except Exception as exc:
            output[str(i)] = {
                "error": type(exc).__name__,
                "code": getattr(exc, "code", None),
            }
    return output


SEMANTIC_BSON_SIZE_CASES = (
    ("semantic_bson_size_literal", literal_documents),
    ("semantic_bson_size_stored", stored_documents),
    ("semantic_bson_size_expressions", expression_results),
    ("semantic_bson_size_validation", argument_validation),
)
