"""BSON strings, JavaScript constants and DBRef contexts through public callers."""

from copy import deepcopy
from functools import partial

from bson import BSON, Code, DBRef


SEED = [{"_id": 1, "a": 42, "items": [1, 2]}, {"_id": 2, "a": 7, "items": []}]
VALUES = {
    "field_code": Code("$a"),
    "variable_code": Code("$$unknown"),
    "empty_code": Code(""),
    "scoped_code": Code("$a", {"z": 1, "a": 2}),
    "empty_scope": Code("$$unknown", {}),
    "bare_dollar": Code("$"),
    "bare_variable": Code("$$"),
    "string_field": "$a",
    "literal_code": {"$literal": Code("$$unknown")},
}


def _reset(collection, *, rows):
    collection.delete_many({})
    if rows:
        collection.insert_many(deepcopy(SEED))


def _aggregate(collection, pipeline, **options):
    phase = "open"
    try:
        cursor = collection.aggregate(pipeline, **options)
        phase = "iterate"
        return {"ok": True, "rows": list(cursor)}
    except Exception as error:
        return {
            "ok": False,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
            "codeName": (getattr(error, "details", None) or {}).get("codeName"),
            "phase": phase,
        }


def literals(collection):
    result = {}
    for rows in (False, True):
        _reset(collection, rows=rows)
        for label, expression in VALUES.items():
            result[f"{rows}-{label}"] = _aggregate(
                collection,
                [
                    {"$project": {"v": expression}},
                    {"$set": {"kind": {"$type": "$v"}}},
                    {"$sort": {"_id": 1}},
                ],
            )
    return result


def lexical_scopes(collection):
    _reset(collection, rows=True)
    expressions = {
        "object": {"nested": [Code("$$unknown"), {"v": Code("$a")}]},
        "let": {"$let": {"vars": {"v": Code("$a")}, "in": "$$v"}},
        "map": {"$map": {"input": "$items", "as": "v", "in": Code("$$unknown")}},
        "reduce": {
            "$reduce": {"input": "$items", "initialValue": 0, "in": Code("$$unknown")}
        },
        "filter": {"$filter": {"input": "$items", "cond": Code("$$unknown")}},
        "inactive": {"$cond": [True, 1, Code("$$unknown")]},
    }
    return {
        name: _aggregate(
            collection, [{"$project": {"v": expression}}, {"$sort": {"_id": 1}}]
        )
        for name, expression in expressions.items()
    }


def command_constants(collection):
    result = {}
    for rows in (False, True):
        _reset(collection, rows=rows)
        for label, expression in VALUES.items():
            result[f"{rows}-{label}"] = _aggregate(
                collection,
                [{"$project": {"v": "$$v"}}, {"$sort": {"_id": 1}}],
                let={"v": expression},
            )
    return result


def selectors_and_updates(collection):
    result = {}
    for label, expression in VALUES.items():
        _reset(collection, rows=True)
        query = {"$expr": {"$eq": [expression, {"$literal": Code("$a")}]}}
        try:
            selected = [row["_id"] for row in collection.find(query, sort=[("_id", 1)])]
            counted = collection.count_documents(query)
            distinct = sorted(collection.distinct("_id", query))
            changed = collection.update_many({}, [{"$set": {"v": expression}}])
            result[label] = {
                "ids": selected,
                "count": counted,
                "distinct": distinct,
                "counts": [changed.matched_count, changed.modified_count],
                "state": list(collection.find({}, sort=[("_id", 1)])),
            }
        except Exception as error:
            result[label] = {
                "exception": type(error).__name__,
                "code": getattr(error, "code", None),
            }
    return result


def field_arguments(collection):
    result = {}
    for rows in (False, True):
        _reset(collection, rows=rows)
        for operator in ("$getField", "$setField", "$unsetField"):
            for label, field in {
                "plain_code": Code("a"),
                "path_code": Code("$a"),
                "variable_code": Code("$$unknown"),
                "scoped_code": Code("a", {}),
                "literal_code": {"$literal": Code("a")},
                "string": "a",
            }.items():
                spec = {"field": field, "input": {"$literal": {"a": 1}}}
                if operator == "$setField":
                    spec["value"] = 2
                result[f"{rows}-{operator}-{label}"] = _aggregate(
                    collection, [{"$project": {"v": {operator: spec}}}]
                )
    return result


def nested_contexts(collection, *, dbref=False):
    result = {}
    for rows in (False, True):
        _reset(collection, rows=rows)
        if dbref and rows:
            collection.update_many({}, {"$set": {"v": {"$ref": "c", "$id": 1}}})
        for context in ("facet", "lookup", "union"):
            nested = (
                [{"$match": {"v": {"$ref": "c", "$id": 1}}}, {"$project": {"_id": 1}}]
                if dbref
                else [{"$project": {"v": Code("$$unknown")}}]
            )
            nested.append({"$sort": {"_id": 1}})
            if context == "facet":
                pipeline = [{"$facet": {"result": nested}}]
            elif context == "lookup":
                pipeline = [
                    {
                        "$lookup": {
                            "from": collection.name,
                            "pipeline": nested,
                            "as": "result",
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            else:
                pipeline = [
                    {"$unionWith": {"coll": collection.name, "pipeline": nested}},
                    {"$sort": {"_id": 1}},
                ]
            result[f"{rows}-{context}"] = _aggregate(collection, pipeline)
    return result


def raw_nested_values(collection):
    result = {}
    for label, value in {
        "ordered": {"$ref": "c", "$id": 1},
        "reversed": {"$id": 1, "$ref": "c"},
        "extra": {"$ref": "c", "$id": 1, "z": {"$id": 2, "$ref": "d"}},
        "sdk": DBRef("c", 1),
    }.items():
        collection.delete_many({})
        scope = {"nested": value, "tail": 1}
        collection.insert_one(
            {"_id": 1, "v": [value], "code": Code("$$unknown", scope)}
        )
        query = {"v": {"$in": [value]}}
        result[label] = {
            "ids": [row["_id"] for row in collection.find(query)],
            "read": collection.find_one({"_id": 1}),
            "literal": _aggregate(
                collection,
                [{"$project": {"v": {"$literal": [value, Code("$a", scope)]}}}],
            ),
        }
    return result


def write_pipeline_zero_batch(collection):
    _reset(collection, rows=True)
    target = collection.database[collection.name + "_merge_target"]
    result = {}
    for size in (None, 0, 1):
        target.delete_many({})
        options = {} if size is None else {"batchSize": size}
        cursor = collection.aggregate([{"$merge": {"into": target.name}}], **options)
        try:
            result[str(size)] = {
                "count_after_open": target.count_documents({}),
                "state_after_open": list(target.find({}, sort=[("_id", 1)])),
            }
        finally:
            cursor.close()
    return result


SEMANTIC_BSON_GRAMMAR_CONTEXT_CASES = (
    ("semantic_bson_grammar_literals", literals),
    ("semantic_bson_grammar_scopes", lexical_scopes),
    ("semantic_bson_grammar_command_constants", command_constants),
    ("semantic_bson_grammar_selectors_updates", selectors_and_updates),
    ("semantic_bson_grammar_field_arguments", field_arguments),
    ("semantic_bson_grammar_nested_code", nested_contexts),
    ("semantic_bson_grammar_nested_dbref", partial(nested_contexts, dbref=True)),
    ("semantic_bson_grammar_raw_nested", raw_nested_values),
    ("semantic_bson_grammar_merge_zero_batch", write_pipeline_zero_batch),
)


def response_boundaries(collection):
    result = {}
    for batch in (None, 0, 1, 2):
        for error_at in (1, 3, 103):
            collection.delete_many({})
            collection.insert_many(
                [
                    {"_id": n, "a": 42, "f": Code("a") if n == error_at else "a"}
                    for n in range(1, 114)
                ]
            )
            options = {} if batch is None else {"batchSize": batch}
            phase = "open"
            received = []
            cursor = None
            try:
                cursor = collection.aggregate(
                    [
                        {"$sort": {"_id": 1}},
                        {
                            "$project": {
                                "v": {"$getField": {"field": "$f", "input": "$$ROOT"}}
                            }
                        },
                    ],
                    **options,
                )
                phase = "iterate"
                received.extend(document["_id"] for document in cursor)
                value = {"ok": True}
            except Exception as error:
                value = {
                    "ok": False,
                    "exception": type(error).__name__,
                    "code": getattr(error, "code", None),
                    "phase": phase,
                }
            finally:
                if cursor is not None:
                    cursor.close()
            value["received"] = received
            result[f"{batch}-{error_at}"] = value
    return result


SEMANTIC_BSON_GRAMMAR_CONTEXT_CASES += (
    ("semantic_bson_grammar_response_boundaries", response_boundaries),
)


def distinct_before_decoding(collection):
    result = {}
    variants = {
        "order": [{"$ref": "c", "$id": 1}, {"$id": 1, "$ref": "c"}],
        "db_null": [{"$ref": "c", "$id": 1}, {"$ref": "c", "$id": 1, "$db": None}],
        "code_ref": [{"$ref": Code("c"), "$id": 1}, {"$ref": "c", "$id": 1}],
    }
    for label, values in variants.items():
        collection.delete_many({})
        collection.insert_many(
            [{"_id": n, "v": value} for n, value in enumerate([*values, values[0]])]
        )
        for surface, actual in (
            ("api", collection.distinct("v")),
            (
                "command",
                collection.database.command({"distinct": collection.name, "key": "v"})[
                    "values"
                ],
            ),
        ):
            result[f"{label}-{surface}"] = {
                "count": len(actual),
                "encoded": sorted(BSON.encode({"v": value}).hex() for value in actual),
            }
    return result


SEMANTIC_BSON_GRAMMAR_CONTEXT_CASES += (
    ("semantic_bson_grammar_distinct_before_decoding", distinct_before_decoding),
)
