"""Independent expression grammar, argument forms and validation witnesses."""

from copy import deepcopy
from functools import partial

from bson import Decimal128, Int64
from pymongo.errors import OperationFailure


SEED = [
    {"_id": 1, "a": None, "items": [1, 2]},
    {"_id": 2, "items": []},
    {"_id": 3, "a": False, "items": None},
    {"_id": 4, "a": 0},
    {"_id": 5, "a": 5, "items": [None, 4]},
    {"_id": 6, "a": {"k": 5}, "items": [[1], [2]]},
]
QUERY_ONLY = {
    operator: {operator: ["$items", operand]}
    for operator, operand in (
        ("$exists", True),
        ("$elemMatch", {"$eq": 1}),
        ("$all", [1]),
        ("$nin", [None]),
        ("$regex", "a"),
        ("$nor", []),
    )
}
ARITIES = {
    "cmp_empty": {"$cmp": []},
    "cmp_one": {"$cmp": [1]},
    "cmp_three": {"$cmp": [1, 2, 3]},
    "ifnull_empty": {"$ifNull": []},
    "ifnull_one": {"$ifNull": [1]},
    "not_empty": {"$not": []},
    "not_two": {"$not": [True, False]},
    "first_two": {"$first": [1, 2]},
    "last_two": {"$last": [1, 2]},
    "in_one": {"$in": [1]},
    "field_bound_and": {"$and": ["$a", [{"$gt": 1}, {"$lt": 5}]]},
    "field_bound_or": {"$or": ["$a", [{"$gt": 1}]]},
}
TYPE_ERRORS = {
    "in_null": {"$in": [1, None]},
    "in_missing": {"$in": [1, "$absent"]},
    "in_number": {"$in": [1, 4]},
    "in_string": {"$in": [1, "a"]},
    "in_object": {"$in": [1, {"k": 5}]},
    "first_scalar": {"$first": 5},
    "last_scalar": {"$last": 5},
    "first_field_scalar": {"$first": "$a"},
    "last_field_scalar": {"$last": "$a"},
    "index_fractional": {"$arrayElemAt": ["$items", 1.5]},
    "index_huge": {"$arrayElemAt": ["$items", Int64(2**31)]},
    "index_bool": {"$arrayElemAt": ["$items", True]},
}
INACTIVE = {
    "cond_unknown": {"$cond": [True, 1, {"$exists": ["$a", True]}]},
    "cond_arity": {"$cond": [True, 1, {"$cmp": [1]}]},
    "cond_in_null": {"$cond": [True, 1, {"$in": [1, None]}]},
    "cond_first_scalar": {"$cond": [False, {"$first": 5}, 1]},
    "and_in_null": {"$and": [False, {"$in": [1, None]}]},
    "or_in_null": {"$or": [True, {"$in": [1, None]}]},
    "switch_unknown": {
        "$switch": {
            "branches": [{"case": True, "then": 1}],
            "default": {"$exists": ["$a", True]},
        }
    },
    "switch_in_null": {
        "$switch": {
            "branches": [{"case": True, "then": 1}],
            "default": {"$in": [1, None]},
        }
    },
}


def _reset(collection, documents):
    collection.delete_many({})
    for document in documents:
        collection.insert_one(deepcopy(document))


def _invoke(collection, context, expression):
    if context == "project":
        return list(
            collection.aggregate(
                [{"$project": {"v": expression}}, {"$sort": {"_id": 1}}]
            )
        )
    if context == "set":
        return list(
            collection.aggregate([{"$set": {"v": expression}}, {"$sort": {"_id": 1}}])
        )
    if context == "expr":
        return list(
            collection.aggregate(
                [{"$match": {"$expr": expression}}, {"$sort": {"_id": 1}}]
            )
        )
    if context == "find":
        return list(collection.find({"$expr": expression}, sort=[("_id", 1)]))
    if context == "count":
        return collection.count_documents({"$expr": expression})
    result = collection.update_many({}, [{"$set": {"v": expression}}])
    return {"matched": result.matched_count, "modified": result.modified_count}


def _observe(invoke, namespace):
    try:
        return {"ok": True, "result": invoke()}
    except (OperationFailure, TypeError, ValueError) as error:
        details = getattr(error, "details", None) or {}
        message = details.get("errmsg", str(error))
        prefix = f"Executor error during aggregate command on namespace: {namespace} ::"
        if message.startswith(prefix):
            message = message.replace(
                prefix,
                "Executor error during aggregate command on namespace: <namespace> ::",
                1,
            )
        return {
            "ok": False,
            "family": type(error).__name__,
            "code": getattr(error, "code", None),
            "codeName": details.get("codeName"),
            "message": message,
        }


def invalid_forms(collection, *, context, expressions):
    result = {}
    for state, documents in (("empty", []), ("rows", SEED)):
        for name, expression in expressions.items():
            _reset(collection, documents)
            observation = _observe(
                partial(_invoke, collection, context, expression),
                f"{collection.database.name}.{collection.name}",
            )
            observation["state"] = list(collection.find({}, sort=[("_id", 1)]))
            observation["recovery"] = len(list(collection.find({})))
            result[f"{state}_{name}"] = observation
    return result


def valid_forms(collection):
    fields = {
        "not_array": {"$not": [{"$eq": ["$a", 5]}]},
        "not_scalar": {"$not": {"$eq": ["$a", 5]}},
        "and_scalar": {"$and": "$a"},
        "or_scalar": {"$or": "$a"},
        "and_literal": {"$and": [{"k": 5}]},
        "or_literal": {"$or": [{"k": 5}]},
        "and_nested_array": {"$and": ["$a", [{"k": 5}]]},
        "or_nested_array": {"$or": ["$a", [{"k": 5}]]},
        "and_empty": {"$and": []},
        "or_empty": {"$or": []},
        "first": {"$first": "$items"},
        "last": {"$last": "$items"},
        "double_index": {"$arrayElemAt": ["$items", 1.0]},
        "decimal_index": {"$arrayElemAt": ["$items", Decimal128("-1")]},
        "long_index": {"$arrayElemAt": ["$items", Int64(0)]},
        "in_array": {"$in": [None, [None, 1]]},
    }
    return list(
        collection.aggregate(
            [
                {"$project": fields},
                {"$set": {name + "Type": {"$type": "$" + name} for name in fields}},
                {"$sort": {"_id": 1}},
            ]
        )
    )


def nested_forms(collection, *, context):
    output = {}
    expressions = {
        "query_only": QUERY_ONLY["$exists"],
        "old_logical": ARITIES["field_bound_and"],
        "not": {"$not": "$a"},
        "literal": {"$or": [{"k": 5}]},
    }
    for state, documents in (("empty", []), ("rows", SEED)):
        for name, expression in expressions.items():
            _reset(collection, documents)
            nested = [{"$project": {"v": expression}}, {"$sort": {"_id": 1}}]
            if context == "facet":
                pipeline = [{"$facet": {"result": nested}}]
            elif context == "lookup":
                pipeline = [
                    {
                        "$lookup": {
                            "from": collection.name,
                            "let": {"outer": "$a"},
                            "pipeline": nested,
                            "as": "result",
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            else:
                pipeline = [
                    {"$unionWith": {"coll": collection.name, "pipeline": nested}},
                    {"$sort": {"_id": 1, "v": 1}},
                ]
            output[f"{state}_{name}"] = _observe(
                lambda pipeline=pipeline: list(collection.aggregate(pipeline)),
                f"{collection.database.name}.{collection.name}",
            )
    return output


def average_contexts(collection):
    _reset(
        collection,
        [
            {"_id": i, "x": value, "items": [1]}
            for i, value in enumerate((Int64(2**63 - 1), 1, Int64(-(2**63 - 1))))
        ],
    )
    return {
        operator: list(
            collection.aggregate(
                [
                    {"$sort": {"_id": 1}},
                    {"$group": {"_id": {operator: operand}, "average": {"$avg": "$x"}}},
                ]
            )
        )
        for operator, operand in (
            ("$not", False),
            ("$first", "$items"),
            ("$last", "$items"),
        )
    }


SEMANTIC_EXPRESSION_GRAMMAR_CASES = (
    ("semantic_expression_valid_forms", valid_forms),
    ("semantic_expression_average_contexts", average_contexts),
    *(
        (
            f"semantic_expression_nested_{context}",
            partial(nested_forms, context=context),
        )
        for context in ("facet", "lookup", "union")
    ),
    *(
        (
            f"semantic_expression_{family}_{context}",
            partial(invalid_forms, context=context, expressions=expressions),
        )
        for family, expressions, contexts in (
            (
                "query_only",
                QUERY_ONLY,
                ("project", "set", "expr", "find", "count", "update"),
            ),
            ("arity", ARITIES, ("project", "expr", "update")),
            ("types", TYPE_ERRORS, ("project", "expr", "update")),
            ("inactive", INACTIVE, ("project", "expr", "update")),
        )
        for context in contexts
    ),
)
