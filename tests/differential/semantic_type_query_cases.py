"""Query type witnesses distinguish SQL eligibility from BSON matching."""

import datetime

from bson import Binary, Code, Decimal128, Int64, ObjectId


SEED = [{"_id": "initial"}]
VALUES = [
    b"ab",
    Binary(b"ab", 0),
    Binary(bytes(16), 5),
    Binary(bytes(16), 4),
    ObjectId("0123456789abcdef01234567"),
    datetime.datetime(2038, 4, 22, 20, 55, 58, 687000),  # noqa: DTZ001 - BSON naive UTC fixture
    True,
    1,
    1.5,
    "text",
    None,
    {"x": 1},
    [],
    [b"ab"],
    [[b"ab"]],
]
SPECS = [
    "binData",
    5,
    "objectId",
    7,
    "date",
    9,
    [5, 7, 9],
    ["binData", "date"],
    "object",
    3,
    "array",
    4,
    "number",
    "int",
    16,
    "double",
    1,
    "string",
    2,
    "bool",
    8,
    "null",
    10,
]


def _reset(collection, values=VALUES):
    collection.delete_many({})
    collection.insert_many(
        [{"_id": i, "v": v, "a": [v], "n": [{"v": v}]} for i, v in enumerate(values, 1)]
    )
    collection.insert_one({"_id": len(values) + 1})


def selection(collection):
    _reset(collection)
    output = {}
    for index, spec in enumerate(SPECS):
        for path in ("v", "a", "n.v", "n.0.v"):
            query = {path: {"$type": spec}}
            output[f"{index}-{path}"] = [
                row["_id"] for row in collection.find(query, sort=[("_id", 1)])
            ]
        output[f"{index}-not"] = [
            row["_id"]
            for row in collection.find(
                {"v": {"$not": {"$type": spec}}}, sort=[("_id", 1)]
            )
        ]
        query = {"v": {"$type": spec}}
        output[f"{index}-count"] = collection.count_documents(query)
        output[f"{index}-page"] = [
            row["_id"]
            for row in collection.find(query, sort=[("_id", -1)], skip=1, limit=2)
        ]
    return output


def writes(collection):
    output = {}
    for index, spec in enumerate(("binData", 5, "objectId", 7, "date", 9)):
        _reset(collection)
        query = {"a": {"$type": spec}}
        result = collection.update_many(query, {"$set": {"matched": True}})
        output[str(index)] = {
            "counts": [result.matched_count, result.modified_count],
            "ids": [
                row["_id"]
                for row in collection.find({"matched": True}, sort=[("_id", 1)])
            ],
        }
    return output


def argument_grammar(collection):
    output = {}
    arguments = [
        "binData",
        5,
        Int64(5),
        5.0,
        Decimal128("5"),
        "bindata",
        "BINDATA",
        " binData ",
        " number ",
        5.5,
        Decimal128("5.5"),
        True,
        None,
        0,
        [],
        float("nan"),
        float("inf"),
        Code("binData"),
        ["binData", 5],
        ["date", "objectId"],
    ]
    # Argument validity includes deprecated type names; this does not claim
    # storage/decoding support for every legacy BSON value.
    arguments.extend(
        [
            *range(-1, 20),
            127,
            "minKey",
            "double",
            "string",
            "object",
            "array",
            "binData",
            "undefined",
            "objectId",
            "bool",
            "date",
            "null",
            "regex",
            "dbPointer",
            "javascript",
            "symbol",
            "javascriptWithScope",
            "int",
            "timestamp",
            "long",
            "decimal",
            "maxKey",
            "number",
            "INT",
            "STRING",
            "OBJECTID",
            "Double",
            "Decimal",
        ]
    )
    for populated in (False, True):
        collection.delete_many({})
        if populated:
            collection.insert_one({"_id": 1, "v": b"ab"})
        for i, arg in enumerate(arguments):
            phase = "open"
            try:
                cursor = collection.aggregate(
                    [{"$match": {"v": {"$type": arg}}}, {"$project": {"_id": 1}}]
                )
                phase = "iterate"
                output[f"{populated}-{i}"] = {
                    "ok": True,
                    "ids": [r["_id"] for r in cursor],
                }
            except Exception as exc:
                output[f"{populated}-{i}"] = {
                    "ok": False,
                    "exception": type(exc).__name__,
                    "code": getattr(exc, "code", None),
                    "phase": phase,
                }
    return output


def numeric_storage(collection):
    _reset(
        collection,
        [1, Int64(1), Int64(2**40), 1.0, float("nan"), float("inf"), Decimal128("1.0")],
    )
    return {
        f"{path}-{i}": [
            r["_id"]
            for r in collection.find({path: {"$type": spec}}, sort=[("_id", 1)])
        ]
        for i, spec in enumerate(
            ("number", "int", 16, "long", 18, "double", 1, "decimal", 19)
        )
        for path in ("v", "a", "n.v")
    }


SEMANTIC_TYPE_QUERY_CASES = (
    ("semantic_type_selection", selection),
    ("semantic_type_writes", writes),
    ("semantic_type_argument_grammar", argument_grammar),
    ("semantic_type_numeric_storage", numeric_storage),
)
