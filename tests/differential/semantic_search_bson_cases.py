"""Native MQL selections for Search BSON domains and cache-lifecycle checks."""

from copy import deepcopy
from datetime import datetime, timedelta

from bson import Binary, Code, DBRef, Decimal128


EPOCH = datetime(1970, 1, 1)  # noqa: DTZ001 - naive UTC default BSON client


# The action owns its BSON-rich seed; EJSON metadata must not canonicalize it.
INITIAL_SEED = [{"_id": 0}]
REF = {"$ref": "items", "$id": 1}
_FIELDS = [
    {"v": b"x"},
    {"v": Binary(b"x", 0)},
    {"v": Binary(b"x", 5)},
    {"v": Code("keep")},
    {"v": Code("keep", {})},
    {"v": Code("keep", {"n": 1})},
    {"v": "keep"},
    {"v": "KEEP"},
    {"v": DBRef("items", 1)},
    {"v": REF},
    {"v": {"$id": 1, "$ref": "items"}},
    {"v": {**REF, "$db": None}},
    {"v": {"nested": DBRef("items", 1)}},
    {"v": [DBRef("items", 1)]},
    {"v": [Code("keep")]},
    {"v": None},
    {},
    {"v": []},
    {"v": True},
    {"v": 1},
    {"v": 1.0},
    {"v": Decimal128("1.0")},
    {"v": 2**53},
    {"v": 2**53 + 1},
    {"v": float(2**53)},
    {"v": Decimal128("0.1")},
    {"v": 0.1},
    {"v": EPOCH + timedelta(milliseconds=1001)},
    {"v": EPOCH - timedelta(milliseconds=1001)},
    {"v": Binary(b"x" * 16, 4)},
    {"v": [1, 20]},
    {"v": {"n": 1}},
    {"v": "keep", "embedding": None},
]
SEED = [
    {
        "_id": f"{index:02}",
        "title": "Ada",
        "kind": "keep",
        "embedding": [1.0, 0.0],
        **fields,
    }
    for index, fields in enumerate(_FIELDS)
]
FILTERS = {
    "all": {},
    "binary0": {"v": b"x"},
    "binary5": {"v": Binary(b"x", 5)},
    "binary_in": {"v": {"$in": [b"x", Binary(b"x", 5)]}},
    "code": {"v": Code("keep")},
    "code_empty_scope": {"v": Code("keep", {})},
    "code_numeric_scope": {"v": Code("keep", {"n": 1.0})},
    "string": {"v": "keep"},
    "code_string_in": {"v": {"$in": [Code("keep"), "keep"]}},
    "dbref": {"v": DBRef("items", 1)},
    "dbref_explicit": {"v": {"$eq": REF}},
    "dbref_reversed": {"v": {"$eq": {"$id": 1, "$ref": "items"}}},
    "nested_dbref": {"v.nested": DBRef("items", 1)},
    "null": {"v": None},
    "not_null": {"v": {"$ne": None}},
    "missing": {"v": {"$exists": False}},
    "boolean": {"v": True},
    "numeric": {"v": 1},
    "large": {"v": {"$gt": 2**53}},
    "double_large": {"v": float(2**53)},
    "decimal_range": {"v": {"$gt": Decimal128("0.1"), "$lt": 1}},
    "date": {"v": {"$lt": EPOCH}},
    "partial_and": {"kind": "keep", "v": Code("keep")},
    "partial_or": {"$or": [{"v": Code("keep")}, {"v": 1}]},
    "nin_binary": {"v": {"$nin": [b"x", Binary(b"x", 5)]}},
    "not_numeric": {"v": {"$not": {"$gt": 2**53}}},
}
PHASES = ("seed", "scalarized", "restored", "removed")


def reset(collection):
    collection.delete_many({})
    collection.insert_many(deepcopy(SEED))


def advance(collection, phase):
    if phase == "scalarized":
        collection.update_many(
            {"_id": {"$in": ["03", "08", "14"]}}, {"$set": {"v": "keep"}}
        )
        collection.update_one({"_id": "23"}, {"$set": {"v": Binary(b"x", 5)}})
        collection.update_one({"_id": "00"}, {"$unset": {"embedding": ""}})
        collection.insert_one(
            {
                "_id": "33",
                "title": "Ada",
                "kind": "keep",
                "v": Code("keep"),
                "embedding": [1.0, 0.0],
            }
        )
    elif phase == "restored":
        for row in SEED:
            if row["_id"] in {"00", "03", "08", "14", "23"}:
                collection.replace_one({"_id": row["_id"]}, deepcopy(row))
    elif phase == "removed":
        collection.delete_many({"_id": {"$in": ["08", "33"]}})
        collection.update_one({"_id": "32"}, {"$set": {"v": Binary(b"x", 5)}})


def select_mql(collection, query, *, domain, **_context):
    if domain == "vector":
        query = {"$and": [query, {"embedding": [1.0, 0.0]}]}
    return [row["_id"] for row in collection.find(query, sort=[("_id", 1)])]


def selections_after_writes(collection, *, domain, select=select_mql):
    reset(collection)
    result = {}
    for phase in PHASES:
        advance(collection, phase)
        result[phase] = {
            name: select(collection, query, domain=domain, phase=phase, name=name)
            for name, query in FILTERS.items()
        }
    return result


def document_selections(collection):
    return selections_after_writes(collection, domain="document")


def vector_selections(collection):
    return selections_after_writes(collection, domain="vector")


SEMANTIC_SEARCH_BSON_CASES = (
    ("semantic_search_bson_document", document_selections),
    ("semantic_search_bson_vector", vector_selections),
)
