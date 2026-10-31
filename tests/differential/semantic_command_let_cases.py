"""Command let programs versus evaluated bindings, using only public clients."""

from copy import deepcopy
from datetime import datetime
from functools import partial

from pymongo import DeleteMany, InsertOne, ReturnDocument, UpdateOne

from tests.support_write_parity import (
    SEMANTIC_ERRORS,
    BSONObservation,
    public_error,
    public_result,
)


SEED = [
    {"_id": 1, "a": 5, "v": "existing"},
    {"_id": 2, "a": 9, "v": "existing"},
    {"_id": 3, "v": "existing"},
]
VALID = {
    "arithmetic": {"x": {"$add": [2, 3]}},
    "literal_string": {"x": {"$literal": "$a"}},
    "literal_operator": {"x": {"$literal": {"$add": [2, 3]}}},
    "literal_ejson": {"x": {"$literal": {"$numberLong": "1"}}},
    "sequential": {"base": 3, "x": {"$add": ["$$base", 2]}},
    "unicode": {"é": 5, "x": "$$é"},
    "current": {"CURRENT": {"$literal": {"a": 5}}, "x": "$a"},
    "current_null": {"CURRENT": None, "x": "$a"},
    "current_scalar": {"CURRENT": 5, "x": "$$CURRENT"},
    "lexical_current": {
        "x": {"$let": {"vars": {"CURRENT": {"$literal": {"a": 5}}}, "in": "$a"}}
    },
    "map": {
        "x": {"$map": {"input": [1, 2], "as": "item", "in": {"$add": ["$$item", 1]}}}
    },
    "remove": {"x": "$$REMOVE"},
    "inactive_error": {"x": {"$cond": [True, 5, {"$divide": [1, 0]}]}},
}
INVALID = {
    "field": {"x": "$a"},
    "dotted": {"x": "$a.b"},
    "root": {"x": "$$ROOT"},
    "current": {"x": "$$CURRENT.a"},
    "inactive_field": {"x": {"$cond": [True, 1, "$a"]}},
    "nested_field": {"x": {"nested": "$a"}},
    "undefined": {"x": "$$unknown"},
    "reversed": {"x": {"$add": ["$$base", 2]}, "base": 3},
    "self": {"x": "$$x"},
    "unknown_inactive": {"x": {"$cond": [True, 1, {"$unknown": 1}]}},
    "upper_name": {"X": 5},
    "empty_name": {"": 5},
    "dotted_name": {"a.b": 5},
    "underscore_name": {"_x": 5},
    "reserved": {"NOW": 5, "x": "$$NOW"},
}


def _reset(collection, documents=SEED):
    collection.delete_many({})
    for document in documents:
        collection.insert_one(deepcopy(document))


def _invoke(collection, route, bindings, *, unused=False):  # noqa: PLR0911, PLR0912 - explicit public request forms
    query = {"_id": 1} if unused else {"$expr": {"$eq": ["$a", "$$x"]}}
    update = [{"$set": {"v": 1 if unused else "$$x"}}]
    if route == "aggregate":
        projection = (
            {"_id": 1}
            if unused
            else {"v": "$$x", "a": "$a", "root": "$$ROOT.a", "type": {"$type": "$$x"}}
        )
        return list(
            collection.aggregate(
                [{"$project": projection}, {"$sort": {"_id": 1}}], let=bindings
            )
        )
    if route == "find":
        return list(collection.find(query, let=bindings, sort=[("_id", 1)]))
    if route == "find_one":
        return collection.find_one(query, let=bindings)
    if route == "count":
        return collection.count_documents(query, let=bindings)
    if route == "update":
        return collection.update_many({}, update, let=bindings)
    if route == "replace":
        return collection.replace_one({"_id": 1}, {"v": "$$x"}, let=bindings)
    if route == "find_modify":
        return collection.find_one_and_update(
            {"_id": 1}, update, let=bindings, return_document=ReturnDocument.AFTER
        )
    if route == "delete":
        return collection.delete_many(query, let=bindings)
    if route == "raw_update":
        return collection.database.command(
            {
                "update": collection.name,
                "let": bindings,
                "ordered": False,
                "updates": [{"q": {}, "u": update, "multi": True}],
            }
        )
    if route == "raw_delete":
        return collection.database.command(
            {
                "delete": collection.name,
                "let": bindings,
                "deletes": [{"q": query, "limit": 0}],
            }
        )
    if route == "raw_find_modify":
        return collection.database.command(
            {
                "findAndModify": collection.name,
                "let": bindings,
                "query": {"_id": 1},
                "update": update,
                "new": True,
            }
        )
    if route in {"raw_count", "raw_distinct"}:
        command = (
            {"count": collection.name, "query": query}
            if route == "raw_count"
            else {"distinct": collection.name, "key": "a", "query": query}
        )
        return collection.database.command({**command, "let": bindings})
    if route == "raw_find":
        command = {
            "find": collection.name,
            "filter": query,
            "sort": {"_id": 1},
            "let": bindings,
        }
    else:
        command = {
            "aggregate": collection.name,
            "pipeline": [{"$project": {"v": "$$x"}}, {"$sort": {"_id": 1}}],
            "cursor": {},
            "let": bindings,
        }
    result = collection.database.command(command)
    cursor = result["cursor"]
    return {
        "namespace_matches": cursor["ns"]
        == f"{collection.database.name}.{collection.name}",
        "closed": cursor["id"] == 0,
        "documents": cursor["firstBatch"],
    }


def _observe(collection, invoke):
    bson = BSONObservation(SEED, error_contract=True)
    try:
        result = {"result": public_result(invoke(), bson)}
    except SEMANTIC_ERRORS as error:
        details = getattr(error, "details", None) or {}
        result = {"error": public_error(error, bson)}
        result["diagnostics"] = (
            [entry.get("errmsg") for entry in details["writeErrors"]]
            if "writeErrors" in details
            else details.get("errmsg")
        )
    result["state"] = bson.documents(list(collection.find({}, sort=[("_id", 1)])))
    result["recovery"] = len(list(collection.find({})))
    return result


def binding_matrix(collection, *, route, bindings):
    output = {}
    for state, documents in (("empty", []), ("rows", SEED)):
        for name, values in bindings.items():
            _reset(collection, documents)
            output[f"{state}_{name}"] = _observe(
                collection, partial(_invoke, collection, route, values)
            )
    return output


def unused_bindings(collection):
    output = {}
    for route in ("aggregate", "find_one", "update", "replace", "delete"):
        for state, documents in (("empty", []), ("rows", SEED)):
            _reset(collection, documents)
            output[f"{route}_{state}"] = _observe(
                collection,
                partial(_invoke, collection, route, INVALID["field"], unused=True),
            )
    return output


def bulk_boundaries(collection):
    output = {}
    for ordered in (True, False):
        for name, bindings in (
            ("valid", VALID["arithmetic"]),
            ("field", INVALID["field"]),
            ("name", INVALID["upper_name"]),
        ):
            for order in (
                "insert",
                "insert_update",
                "update_insert",
                "two_updates",
                "delete_insert",
            ):
                _reset(collection, [])
                insert = InsertOne({"_id": 1, "a": 5})
                update = UpdateOne({"_id": 1}, [{"$set": {"v": "$$x"}}])
                models = {
                    "insert": [insert],
                    "insert_update": [insert, update],
                    "update_insert": [update, insert],
                    "two_updates": [
                        update,
                        UpdateOne({"_id": 2}, [{"$set": {"v": "$$x"}}]),
                    ],
                    "delete_insert": [
                        DeleteMany({"$expr": {"$eq": ["$a", "$$x"]}}),
                        insert,
                    ],
                }[order]
                output[f"{ordered}_{name}_{order}"] = _observe(
                    collection,
                    partial(
                        collection.bulk_write, models, ordered=ordered, let=bindings
                    ),
                )
    return output


def random_binding(collection):
    bindings = {"x": {"$rand": {}}}
    values = list(collection.aggregate([{"$project": {"v": "$$x"}}], let=bindings))
    collection.update_many({}, [{"$set": {"v": "$$x"}}], let=bindings)
    written = list(collection.find({}, sort=[("_id", 1)]))
    return {
        "read_one_value": len({doc["v"] for doc in values}) == 1,
        "read_in_range": all(
            isinstance(doc["v"], float) and 0 <= doc["v"] < 1 for doc in values
        ),
        "write_one_value": len({doc["v"] for doc in written}) == 1,
        "write_in_range": all(
            isinstance(doc["v"], float) and 0 <= doc["v"] < 1 for doc in written
        ),
        "commands_independent": values[0]["v"] != written[0]["v"],
    }


def clock_binding(collection):
    output = {}
    bson = BSONObservation(SEED, error_contract=True)
    for route in ("read", "write"):
        _reset(collection)
        try:
            if route == "read":
                rows = list(
                    collection.aggregate(
                        [{"$project": {"v": "$$x", "now": "$$NOW"}}], let={"x": "$$NOW"}
                    )
                )
            else:
                collection.update_many(
                    {}, [{"$set": {"v": "$$x", "now": "$$NOW"}}], let={"x": "$$NOW"}
                )
                rows = list(collection.find({}))
            output[route] = {
                "shared": len({row["v"] for row in rows}) == 1,
                "alias": all(row["v"] == row["now"] for row in rows),
                "bson_dates": all(
                    isinstance(row["v"], datetime)
                    and row["v"].tzinfo is None
                    and row["v"].microsecond % 1000 == 0
                    for row in rows
                ),
                "ids": sorted(row["_id"] for row in rows),
            }
        except SEMANTIC_ERRORS as error:
            details = getattr(error, "details", None) or {}
            output[route] = {
                "error": public_error(error, bson),
                "diagnostics": details.get("errmsg"),
                "state": bson.documents(list(collection.find({}, sort=[("_id", 1)]))),
            }
    return output


def cursor_bindings(collection):
    output = {}
    for name in ("literal_string", "literal_operator", "literal_ejson"):
        expected_type = "string" if name == "literal_string" else "object"
        cursor = collection.find(
            {"$expr": {"$eq": [{"$type": "$$x"}, expected_type]}},
            let=VALID[name],
            sort=[("_id", 1)],
        )
        bson = BSONObservation(SEED)
        output[name] = {
            "original": bson.documents(list(cursor)),
        }
        cursor.rewind()
        output[name]["rewind"] = bson.documents(list(cursor))
        output[name]["fresh"] = bson.documents(
            list(
                collection.find(
                    {"$expr": {"$eq": [{"$type": "$$x"}, expected_type]}},
                    let=VALID[name],
                    sort=[("_id", 1)],
                )
            )
        )
    return output


def error_precedence(collection):
    output = {}
    for route in ("aggregate", "update"):
        for state, documents in (("empty", []), ("rows", SEED)):
            _reset(collection, documents)
            invoke = (
                partial(
                    collection.aggregate,
                    [{"$project": {"v": {"$unknown": 1}}}],
                    let=INVALID["field"],
                )
                if route == "aggregate"
                else partial(
                    collection.update_many,
                    {},
                    [{"$set": {"v": {"$unknown": 1}}}],
                    let=INVALID["field"],
                )
            )
            output[f"{route}_{state}"] = _observe(collection, invoke)
    return output


SEMANTIC_COMMAND_LET_CASES = (
    ("semantic_command_let_unused", unused_bindings),
    ("semantic_command_let_bulk", bulk_boundaries),
    ("semantic_command_let_random", random_binding),
    ("semantic_command_let_clock", clock_binding),
    ("semantic_command_let_cursors", cursor_bindings),
    ("semantic_command_let_precedence", error_precedence),
    *(
        (
            f"semantic_command_let_{family}_{route}",
            partial(binding_matrix, route=route, bindings=bindings),
        )
        for family, bindings, routes in (
            (
                "values",
                VALID,
                (
                    "aggregate",
                    "find",
                    "count",
                    "update",
                    "replace",
                    "find_modify",
                    "delete",
                    "raw_find",
                    "raw_aggregate",
                    "raw_update",
                    "raw_delete",
                    "raw_find_modify",
                ),
            ),
            (
                "errors",
                INVALID,
                (
                    "aggregate",
                    "find",
                    "update",
                    "find_modify",
                    "raw_update",
                    "raw_delete",
                ),
            ),
            (
                "unsupported",
                {"literal": {"x": 5}, "field": INVALID["field"]},
                ("raw_count", "raw_distinct"),
            ),
        )
        for route in routes
    ),
)
