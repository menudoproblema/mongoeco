"""H45/H46: native MQL eligibility, independent of local Search and ranking."""

from bson import Decimal128
from pymongo.errors import DuplicateKeyError

from tests.differential.semantic_expression_grammar_cases import _reset


_FIELDS = [
    {"a": 2**53},
    {"a": 2**53 + 1},
    {"a": [1, 20]},
    {"a": 7},
    {},
    {"a": None},
    {"a": [7], "items": [{"x": 7}]},
    {"a": {"x": 7}},
    {"a": []},
    {"a": True},
    {"a": "z"},
    {"a": Decimal128("0.1")},
    {"a": 0.1},
    {"a": {}},
    {"a": [[7]]},
    {"items": {"x": 7}},
]
SEED = [
    {
        "_id": chr(97 + i),
        "title": "Ada",
        "embedding": [1.0, 0.0],
        "kind": "keep",
        **fields,
    }
    for i, fields in enumerate(_FIELDS)
]
for _document in SEED:
    _document["scalar"] = 0
SEED[0]["scalar"] = 2**53
SEED[1]["scalar"] = 2**53 + 1
SEED[4].pop("kind")
SEED[5]["kind"] = None
SEED[7]["kind"] = {}
SEED[8]["kind"] = []
FILTERS = {
    "scalar_large": {"scalar": {"$gt": 2**53}},
    "scalar_equal": {"scalar": 2**53},
    "scalar_in": {"scalar": {"$in": [float(2**53)]}},
    "large": {"a": {"$gt": 2**53}},
    "array_range": {"a": {"$gt": 5, "$lt": 10}},
    "array_and": {"$and": [{"a": {"$gt": 5}}, {"a": {"$lt": 10}}]},
    "null": {"a": None},
    "in_null": {"a": {"$in": [None, 7]}},
    "not_null": {"a": {"$ne": None}},
    "broadcast": {"items.x": {"$gt": 5}},
    "position": {"a.0": 7},
    "array_literal": {"a": [7]},
    "decimal_bound": {"a": {"$gt": Decimal128("0.1"), "$lt": 1}},
    "boolean": {"a": {"$gt": False}},
    "string": {"a": {"$gt": "b"}},
    "exists": {"kind": {"$exists": True}},
    "absent": {"kind": {"$exists": False}},
    "or": {"$or": [{"a": None}, {"a": {"$gt": 2**53}}]},
    "nor": {"$nor": [{"a": None}, {"a": {"$gt": 2**53}}]},
    "partial_and": {"$and": [{"title": "Ada"}, {"a": {"$ne": None}}]},
    "or_partial": {"$or": [{"title": "Ada"}, {"a": {"$ne": None}}]},
}


def eligibility(collection):
    return {
        name: [row["_id"] for row in collection.find(query, sort=[("_id", 1)])]
        for name, query in FILTERS.items()
    }


TRAVERSAL_SEED = [
    {"_id": "a", "a": [7]},
    {"_id": "b", "a": [[7]]},
    {"_id": "c", "a": [[[7]]]},
    {"_id": "d", "a": [{"0": 7}]},
    {"_id": "e", "a": [{}, {"0": 7}]},
    {"_id": "f", "a": {"0": 7}},
    {"_id": "g"},
    {"_id": "h", "a": []},
    {"_id": "i", "a": 7},
    {"_id": "j", "a": {}},
    {"_id": "k", "a": [1, {"x": 7}]},
    {"_id": "l", "a": [[], {"x": 7}]},
    {"_id": "m", "a": [None, {"x": 7}]},
    {"_id": "n", "a": [{}, {"x": 7}]},
    {"_id": "o", "a": [{"0": [7]}]},
]
TRAVERSAL_CLAUSES = {
    "eq": 7,
    "array": [7],
    "null": None,
    "ne_null": {"$ne": None},
    "in": {"$in": [7]},
    "in_null": {"$in": [None]},
    "nin_null": {"$nin": [None]},
    "gt": {"$gt": 5},
    "gte_null": {"$gte": None},
    "type_int": {"$type": "int"},
    "type_array": {"$type": "array"},
    "exists": {"$exists": True},
    "absent": {"$exists": False},
    "all": {"$all": [7]},
    "all_null": {"$all": [None]},
    "all_array": {"$all": [[7]]},
    "all_elem": {"$all": [{"$elemMatch": {"$eq": 7}}]},
    "size": {"$size": 1},
    "elem_match": {"$elemMatch": {"$eq": 7}},
    "bits": {"$bitsAnySet": 2},
}


def traversal_controls(collection):
    _reset(collection, TRAVERSAL_SEED)
    output = {}
    for path in ("a", "a.x", "a.0", "a.0.0", "a.00"):
        for label, clause in TRAVERSAL_CLAUSES.items():
            query = {path: clause}
            output[path + "_" + label] = {
                "ids": [
                    row["_id"] for row in collection.find(query, sort=[("_id", 1)])
                ],
                "count": collection.count_documents(query),
                "aggregate": [
                    row["_id"]
                    for row in collection.aggregate(
                        [{"$match": query}, {"$sort": {"_id": 1}}]
                    )
                ],
            }
    result = collection.update_many({"a.0": 7}, {"$set": {"selected": True}})
    output["selection_write"] = {
        "matched": result.matched_count,
        "ids": [
            doc["_id"] for doc in collection.find({"selected": True}, sort=[("_id", 1)])
        ],
        "recovery": collection.count_documents({}),
    }
    return output


def traversal_partial_indexes(collection):
    _reset(collection, [{"_id": 1, "a": [{}, {"x": 7}], "b": 1}])
    collection.database.command(
        {
            "createIndexes": collection.name,
            "indexes": [
                {
                    "key": {"b": 1},
                    "name": "partial_b",
                    "unique": True,
                    "partialFilterExpression": {"a.x": {"$gte": None}},
                }
            ],
        }
    )
    query = {"b": 1, "a.x": {"$gte": None}}
    before = [doc["_id"] for doc in collection.find(query, sort=[("_id", 1)])]
    hinted = [
        doc["_id"]
        for doc in collection.find(query, hint="partial_b", sort=[("_id", 1)])
    ]
    try:
        collection.insert_one({"_id": 2, "a": {"x": None}, "b": 1})
        error = None
    except DuplicateKeyError as rejected:
        error = rejected.code
    collection.update_one({"_id": 1}, {"$set": {"a": []}})
    collection.insert_one({"_id": 2, "a": {"x": None}, "b": 1})
    return {
        "before": before,
        "hint": hinted,
        "duplicate": error,
        "after": [doc["_id"] for doc in collection.find(query, sort=[("_id", 1)])],
        "recovery": collection.count_documents({}),
    }


def mutations(collection, *, select=None):
    _reset(collection, SEED)
    if select is None:

        def select(query):
            return [row["_id"] for row in collection.find(query, sort=[("_id", 1)])]

    output = {"before": select({"scalar": {"$gt": 2**53}})}
    collection.update_one({"_id": "a"}, {"$set": {"scalar": [1, 20]}})
    output["array"] = select({"scalar": {"$gt": 5, "$lt": 10}})
    collection.delete_many({"_id": "a"})
    collection.insert_one(
        {"_id": "q", "title": "Ada", "embedding": [1.0, 0.0], "scalar": 2**53 + 2}
    )
    output["insert"] = select({"scalar": {"$gt": 2**53}})
    collection.delete_many({"_id": "b"})
    output["delete"] = select({"scalar": {"$gt": 2**53}})
    output["recovery"] = collection.count_documents({})
    return output


def post_lookup(collection):
    _reset(collection, [{"_id": 1}, {"_id": 2}, {"_id": 3}])
    foreign = collection.database[collection.name + "_foreign"]
    foreign.insert_many(
        [
            {"_id": "one", "owner": 1, "items": [{}, {"x": 7}]},
            {"_id": "two", "owner": 2, "items": [{"x": 7}]},
        ]
    )
    lookup = {
        "$lookup": {
            "from": foreign.name,
            "localField": "_id",
            "foreignField": "owner",
            "as": "enrollments",
        }
    }
    filters = {
        "exists_ne": {"enrollments.items.x": {"$exists": True, "$ne": None}},
        "ne": {"enrollments.items.x": {"$ne": None}},
        "null": {"enrollments.items.x": None},
        "elem": {
            "enrollments": {
                "$elemMatch": {"items": {"$elemMatch": {"x": {"$ne": None}}}}
            }
        },
    }
    return {
        name: [
            row["_id"]
            for row in collection.aggregate(
                [lookup, {"$match": query}, {"$sort": {"_id": 1}}]
            )
        ]
        for name, query in filters.items()
    }


SEMANTIC_SEARCH_ELIGIBILITY_CASES = (
    ("semantic_search_post_lookup", post_lookup),
    ("semantic_search_candidate_mutations", mutations),
    ("semantic_search_traversal_partial_indexes", traversal_partial_indexes),
    ("semantic_search_traversal_controls", traversal_controls),
    ("semantic_search_eligibility", eligibility),
)
