"""Native query witnesses for type bracketing and leaf matching (H37-H39)."""

from copy import deepcopy
from datetime import datetime
from functools import partial

from bson import SON, Code, DBRef, Decimal128, Int64, MaxKey, MinKey, Regex

from tests.differential.semantic_expression_grammar_cases import _reset


VALUES = [
    None,
    False,
    True,
    -1,
    0,
    5,
    6,
    Int64(2**53 + 1),
    0.1,
    Decimal128("0.1"),
    Decimal128("0.5"),
    float("nan"),
    float("inf"),
    "a",
    "z",
    {},
    {"k": 5},
    [],
    [6],
    [[6]],
    [1, 20],
    [None],
    b"a",
    datetime(2020, 1, 1),  # noqa: DTZ001 - default BSON decoding uses naive UTC
    MinKey(),
    MaxKey(),
    Code("a"),
    Code("z", {"x": 1}),
]
SEED = (
    [{"_id": 0}]
    + [
        {"_id": index, "a": value, "o": {"a": deepcopy(value)}}
        for index, value in enumerate(VALUES, start=1)
    ]
    + [{"_id": 100, "o": [{"a": 6}, {"a": []}]}]
)
TARGETS = {
    "null": None,
    "false": False,
    "number": 5,
    "large": Int64(2**53),
    "decimal": Decimal128("0.1"),
    "nan": float("nan"),
    "string": "b",
    "object": {"k": 4},
    "array": [5],
    "empty_array": [],
    "binary": b"b",
    "date": datetime(2021, 1, 1),  # noqa: DTZ001 - same native BSON date as the seed
    "min": MinKey(),
    "max": MaxKey(),
    "code": Code("b"),
    "code_scope": Code("b", {"x": 1}),
}


def matches(collection, *, path, surface):
    output = {}
    for label, target in TARGETS.items():
        for operator in ("$eq", "$gt", "$gte", "$lt", "$lte"):
            query = {path: {operator: target}}
            if surface == "count":
                value = collection.count_documents(query)
            elif surface == "aggregate":
                value = [
                    row["_id"]
                    for row in collection.aggregate(
                        [{"$match": query}, {"$sort": {"_id": 1}}]
                    )
                ]
            else:
                value = [
                    row["_id"] for row in collection.find(query, sort=[("_id", 1)])
                ]
            output[f"{label}_{operator}"] = value
    return output


def expression_control(collection):
    return [
        row["_id"]
        for row in collection.find({"$expr": {"$gt": ["$a", 5]}}, sort=[("_id", 1)])
    ]


def selection_writes(collection):
    _reset(
        collection,
        [
            {"_id": 1, "a": "z", "items": ["z", 6], "records": [{"a": "z"}, {"a": 6}]},
            {"_id": 2, "a": 6, "items": [2, 8]},
        ],
    )
    changed = collection.update_many({"a": {"$gt": 5}}, {"$set": {"selected": True}})
    pulled = collection.update_many({}, {"$pull": {"items": {"$gt": 5}}})
    collection.update_many(
        {"_id": 1},
        {"$set": {"records.$[item].selected": True}},
        array_filters=[{"item.a": {"$gt": 5}}],
    )
    return {
        "matched": changed.matched_count,
        "pulled": pulled.modified_count,
        "state": list(collection.find({}, sort=[("_id", 1)])),
        "recovery": collection.count_documents({}),
    }


SQL_SEED = [
    {"_id": 0},
    {"_id": 1, "a": None},
    {"_id": 2, "a": "z"},
    {"_id": 3, "a": True},
    {"_id": 4, "a": 6},
    {"_id": 5, "a": ["z", 7]},
    {"_id": 6, "a": []},
    {"_id": 7, "a": [1, 20]},
]


def sql_routes(collection):
    # Avoid tagged-value/global-fallback guards masking the SQL leaf itself.
    _reset(collection, SQL_SEED)
    return matches(collection, path="a", surface="find")


def logical_nulls(collection):
    output = {}
    for state, documents in (("rich", SEED), ("sql", SQL_SEED)):
        _reset(collection, documents)
        for path in ("a", "o.a"):
            for label, clause in {
                "ne_null": {"$ne": None},
                "nin_null": {"$nin": [None]},
                "not_null": {"$not": {"$eq": None}},
                "in_null": {"$in": [None, 5]},
                "ne_number": {"$ne": 5},
                "not_number": {"$not": {"$gt": 5}},
                "missing": {"$exists": False},
            }.items():
                output[f"{state}_{path}_{label}"] = [
                    row["_id"]
                    for row in collection.find({path: clause}, sort=[("_id", 1)])
                ]
    return output


def partial_index_routes(collection):
    _reset(
        collection,
        [
            {"_id": 1, "a": "z", "b": 1},
            {"_id": 2, "a": 5, "b": 2},
            {"_id": 3, "a": 6, "b": 3},
        ],
    )
    collection.database.command(
        {
            "createIndexes": collection.name,
            "indexes": [
                {
                    "key": {"a": 1},
                    "name": "partial_a",
                    "partialFilterExpression": {"a": {"$gt": 5}},
                },
                {
                    "key": {"b": 1},
                    "name": "partial_b",
                    "unique": True,
                    "partialFilterExpression": {"a": {"$gt": 5}},
                },
            ],
        }
    )
    result = {
        name: [row["_id"] for row in collection.find(query, sort=[("_id", 1)])]
        for name, query in {
            "other_family": {"a": {"$gt": "b"}},
            "inclusive": {"a": {"$gte": 5}},
            "equality": {"a": "z"},
        }.items()
    }
    collection.insert_one({"_id": 4, "a": 7, "b": 1})
    changed = collection.update_many({"a": {"$gt": 5}}, {"$set": {"selected": True}})
    result["matched"] = changed.matched_count
    result["selected"] = [
        row["_id"] for row in collection.find({"selected": True}, sort=[("_id", 1)])
    ]
    return result


BSON_REPRESENTATION_SEED = [
    {"_id": 0, "a": Code("abc")},
    {"_id": 1, "a": Code("abc", {"x": 1})},
    {"_id": 2, "a": DBRef("items", 1)},
    {"_id": 3, "a": "abc"},
    {"_id": 4, "a": {"$ref": "items", "$id": 1}},
    {"_id": 5, "a": [Code("abc"), DBRef("items", 1)]},
    {"_id": 6, "a": SON([("x", 1)])},
    {"_id": 7, "a": {"$mongoeco": {"type": "literal", "value": 1}}},
    {"_id": 8, "a": [SON([("x", 1)])]},
    {"_id": 9, "a": Code("abc", {})},
]
BSON_REPRESENTATION_CONDITIONS = {
    "string": {"$type": "string"},
    "object": {"$type": "object"},
    "object_code": {"$type": 3},
    "code": {"$type": "javascript"},
    "scoped": {"$type": "javascriptWithScope"},
    "regex": {"$regex": "abc"},
    "membership": {"$in": [Regex("abc")]},
    "negated_membership": {"$nin": [Regex("abc")]},
}


def bson_representations(collection):
    _reset(collection, BSON_REPRESENTATION_SEED)
    output = {}
    for label, condition in BSON_REPRESENTATION_CONDITIONS.items():
        query = {"a": condition}
        output[label] = {
            "find": [r["_id"] for r in collection.find(query, sort=[("_id", 1)])],
            "count": collection.count_documents(query),
            "aggregate": [
                r["_id"]
                for r in collection.aggregate(
                    [{"$match": query}, {"$sort": {"_id": 1}}]
                )
            ],
        }
    return output


SEMANTIC_QUERY_MATCHING_CASES = (
    ("semantic_query_matching_bson_representations", bson_representations),
    *(
        (
            f"semantic_query_matching_{path.replace('.', '_')}_{surface}",
            partial(matches, path=path, surface=surface),
        )
        for path in ("a", "o.a")
        for surface in ("find", "count", "aggregate")
    ),
    ("semantic_query_matching_expression_control", expression_control),
    ("semantic_query_matching_partial_indexes", partial_index_routes),
    ("semantic_query_matching_selection_writes", selection_writes),
    ("semantic_query_matching_sql_routes", sql_routes),
    ("semantic_query_matching_logical_nulls", logical_nulls),
)
