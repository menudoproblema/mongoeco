"""Additional request-lifetime and encoded scalar guards for the B07 boundary."""

# ruff: noqa: B023 - each operation is consumed in its loop iteration

import re

from decimal import Decimal

from bson import Binary, DBRef, Decimal128, Int64, Regex

from tests.differential.semantic_bson_admission_cases import _observe


SEED = [{"_id": 0, "v": 1}, {"_id": 1, "v": 2}]


def _reset(collection):
    collection.drop()
    collection.insert_many(SEED)


def request_reference_lifetime(collection):
    result = {}
    for route in (
        "projection",
        "let",
        "comment",
        "clone",
        "rewind",
        "explain_reference",
    ):
        _reset(collection)

        def invoke():
            query = {"v": 1}
            if route == "projection":
                projection = {"v": 0}
                cursor = collection.find(query, projection)
                projection["v"] = 1
            elif route == "let":
                variables = {"x": 1}
                cursor = collection.find(query, let=variables)
                variables["x"] = Decimal("1.25")
            elif route == "comment":
                comment = []
                cursor = collection.find(query, comment=comment)
                comment.append(object())
            elif route == "clone":
                original = collection.find(query)
                cursor = original.clone()
                query["v"] = 2
                original.close()
            elif route == "explain_reference":
                cursor = collection.find(query)
                cursor.explain()
                query["v"] = 2
            else:
                cursor = collection.find(query)
                assert [row["_id"] for row in cursor] == [0]
                query["v"] = 2
                cursor.rewind()
            return cursor

        result[route] = _observe(collection, invoke, cursor=True)
    return result


def request_scalar_edges(collection):
    result = {}
    values = (
        ("regex", Regex("a")),
        ("compiled-regex", re.compile("a")),
        ("byte-regex", re.compile(b"a")),
        ("invalid-utf8-regex", re.compile(b"\xff")),
        ("nul-regex", Regex("a\x00b")),
        ("short-legacy-uuid", Binary(b"ab", 3)),
        ("short-standard-uuid", Binary(b"ab", 4)),
        ("short-md5", Binary(b"ab", 5)),
        ("int64", Int64(1)),
        ("snan", Decimal128("-sNaN")),
    )
    for label, value in values:
        for route in ("find", "aggregate"):
            _reset(collection)
            call = (
                (lambda: collection.find({"v": value}))
                if route == "find"
                else (lambda: collection.aggregate([{"$match": {"v": value}}]))
            )
            result[f"{label}/{route}"] = _observe(collection, call, cursor=True)
    return result


def request_find_one_id(collection):
    result = {}
    values = (
        ("none", None),
        ("int", 1),
        ("long", Int64(1)),
        ("double", 1.0),
        ("decimal", Decimal128("1.0")),
        ("bool", True),
        ("string", "1"),
        ("array", [1]),
        ("dbref", DBRef("c", 1)),
        ("raw-decimal", Decimal(1)),
        ("object", object()),
        ("overflow", 1 << 70),
        ("mapping", {"_id": 1}),
    )
    for label, value in values:
        _reset(collection)
        result[label] = _observe(collection, lambda: collection.find_one(value))
    return result


SEMANTIC_BSON_REQUEST_CASES = (
    ("semantic_request_reference_lifetime", request_reference_lifetime),
    ("semantic_request_scalar_edges", request_scalar_edges),
    ("semantic_request_find_one_id", request_find_one_id),
)
