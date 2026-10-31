"""Public mutation state and counters; SDK BSON is the physical observer."""

from copy import deepcopy
from functools import partial
from struct import unpack

from bson import BSON, Binary, Code, Decimal128, Int64
from pymongo import ReplaceOne, UpdateMany, UpdateOne

from tests.differential.semantic_update_identity_cases import SCENARIOS


SEED = [{"_id": "initial"}]
ROOTS = (
    ("double", 1, 1.0),
    ("long", 1, Int64(1)),
    ("decimal", 1, Decimal128("1")),
    ("scope_number", Code("x", {"v": 1}), Code("x", {"v": 1.0})),
    ("scope_width", Code("x", {"v": 1}), Code("x", {"v": Int64(1)})),
    ("binary_alias", b"ab", Binary(b"ab", 0)),
    ("signed_zero", 0.0, -0.0),
    ("nan_payload", float("nan"), unpack("<d", bytes.fromhex("010000000000f87f"))[0]),
    ("decimal_cohort", Decimal128("1.0"), Decimal128("1.00")),
    ("scope_order", Code("x", {"a": 1, "b": 2}), Code("x", {"b": 2, "a": 1})),
    ("scope_none_empty", Code("x"), Code("x", {})),
    ("nested_number", {"k": [{"n": 1}]}, {"k": [{"n": 1.0}]}),
)


def _state(collection):
    return [BSON.encode(row).hex() for row in collection.find({}, sort=[("_id", 1)])]


def root_identity(base_collection):
    output = {}
    for name, before, after in ROOTS:
        for mode in ("classic", "pipeline", "replacement"):
            # Separate cases also on the failing runtime: a NaN _id must not
            # make the next case's cleanup fail before its observation.
            collection = base_collection.database[f"root_{name}_{mode}"]
            collection.delete_many({})
            collection.insert_one({"_id": deepcopy(before), "v": 0})
            try:
                if mode == "replacement":
                    result = collection.replace_one(
                        {}, {"_id": deepcopy(after), "v": 0}
                    )
                else:
                    update = (
                        {"$set": {"_id": deepcopy(after)}}
                        if mode == "classic"
                        else [{"$set": {"_id": {"$literal": deepcopy(after)}}}]
                    )
                    result = collection.update_one({}, update)
                observation = {
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                }
            except Exception as exc:
                observation = {
                    "error": type(exc).__name__,
                    "code": getattr(exc, "code", None),
                }
            observation["state"] = _state(collection)
            output[name + "-" + mode] = observation
    return output


def many_assignments(collection, *, mode):
    collection.create_index([("v", 1)], name="v_effects")
    output = {}
    for name, before, after in SCENARIOS:
        collection.delete_many({})
        collection.insert_many([{"_id": i, "v": deepcopy(before)} for i in (1, 2)])
        update = (
            {"$set": {"v": deepcopy(after)}}
            if mode == "classic"
            else [{"$set": {"v": {"$literal": deepcopy(after)}}}]
        )
        first = collection.update_many({}, update)
        second = collection.update_many({}, update)
        output[name] = {
            "first": [first.matched_count, first.modified_count],
            "second": [second.matched_count, second.modified_count],
            "state": _state(collection),
            "indexed_matches": [
                row["_id"]
                for row in collection.find(
                    {"v": {"$eq": deepcopy(after)}}, hint="v_effects", sort=[("_id", 1)]
                )
            ],
        }
    return output


def bulk_assignments(collection, *, ordered):
    output = {}
    for name, before, after in SCENARIOS:
        collection.delete_many({})
        collection.insert_many(
            [{"_id": i, "v": deepcopy(before)} for i in (1, 2, 3, 4)]
        )
        requests = [
            UpdateOne({"_id": 1}, {"$set": {"v": deepcopy(after)}}),
            UpdateMany(
                {"_id": {"$in": [2, 3]}},
                [{"$set": {"v": {"$literal": deepcopy(after)}}}],
            ),
            ReplaceOne({"_id": 4}, {"_id": 4, "v": deepcopy(after)}),
        ]
        result = collection.bulk_write(requests, ordered=ordered)
        repeated = collection.bulk_write(requests, ordered=ordered)
        output[name] = {
            "counts": [result.matched_count, result.modified_count],
            "repeated": [repeated.matched_count, repeated.modified_count],
            "state": _state(collection),
        }
    return output


def command_channels(collection):
    output = {}
    for name, before, after in SCENARIOS:
        collection.delete_many({})
        collection.insert_many([{"_id": i, "v": deepcopy(before)} for i in (1, 2)])
        result = collection.database.command(
            {
                "update": collection.name,
                "updates": [
                    {"q": {"_id": 1}, "u": {"$set": {"v": deepcopy(after)}}},
                    {
                        "q": {"_id": 2},
                        "u": [{"$set": {"v": {"$literal": deepcopy(after)}}}],
                    },
                ],
            }
        )
        returned = collection.find_one_and_update(
            {"_id": 1}, {"$set": {"v": deepcopy(after)}}, return_document=True
        )
        command_return = collection.database.command(
            {
                "findAndModify": collection.name,
                "query": {"_id": 2},
                "update": [{"$set": {"v": {"$literal": deepcopy(after)}}}],
                "new": True,
            }
        )
        output[name] = {
            "counts": [result["n"], result["nModified"]],
            "returned": BSON.encode(returned).hex(),
            "command_returned": BSON.encode(command_return["value"]).hex(),
            "lastErrorObject": command_return["lastErrorObject"],
            "state": _state(collection),
        }
    return output


def partial_unique_membership(collection):
    collection.database.command(
        {
            "createIndexes": collection.name,
            "indexes": [
                {
                    "key": {"u": 1},
                    "name": "u_double",
                    "unique": True,
                    "partialFilterExpression": {"v": {"$type": "double"}},
                }
            ],
        }
    )
    output = {}
    for mode in ("classic", "pipeline", "replacement"):
        collection.delete_many({})
        collection.insert_many(
            [{"_id": 1, "u": "same", "v": 1}, {"_id": 2, "u": "same", "v": 2.0}]
        )
        update = {"$set": {"v": 1.0}} if mode == "classic" else [{"$set": {"v": 1.0}}]
        try:
            result = (
                collection.replace_one({"_id": 1}, {"_id": 1, "u": "same", "v": 1.0})
                if mode == "replacement"
                else collection.update_one({"_id": 1}, update)
            )
            rejection = {"modified": result.modified_count}
        except Exception as exc:
            rejection = {
                "error": type(exc).__name__,
                "code": getattr(exc, "code", None),
            }
        output[mode + "-rejected"] = {"result": rejection, "state": _state(collection)}
        collection.update_one({"_id": 2}, {"$set": {"u": "other"}})
        result = (
            collection.replace_one({"_id": 1}, {"_id": 1, "u": "same", "v": 1.0})
            if mode == "replacement"
            else collection.update_one({"_id": 1}, update)
        )
        output[mode + "-accepted"] = {
            "modified": result.modified_count,
            "state": _state(collection),
        }
    return output


def nan_root_maintenance(base_collection):
    output = {}
    nan = unpack("<d", bytes.fromhex("010000000000f87f"))[0]
    other = unpack("<d", bytes.fromhex("020000000000f87f"))[0]
    for name, value, alias in (
        ("scalar", nan, other),
        ("document", {"n": nan}, {"n": other}),
        ("nested", {"a": [{"n": nan}]}, {"a": [{"n": other}]}),
        ("scope", Code("x", {"n": nan}), Code("x", {"n": other})),
    ):
        collection = base_collection.database[f"nan_{name}"]
        collection.insert_one({"_id": deepcopy(value), "v": 0})
        result = {"count": collection.count_documents({"_id": deepcopy(alias)})}
        try:
            collection.insert_one({"_id": deepcopy(alias), "v": 99})
            result["duplicate"] = {"accepted": True}
        except Exception as exc:
            result["duplicate"] = {
                "error": type(exc).__name__,
                "code": getattr(exc, "code", None),
            }
        for mode in ("classic", "pipeline", "replacement"):
            try:
                update = (
                    {"$set": {"v": 1}} if mode == "classic" else [{"$set": {"v": 1}}]
                )
                changed = (
                    collection.replace_one(
                        {"_id": deepcopy(alias)}, {"_id": deepcopy(value), "v": 1}
                    )
                    if mode == "replacement"
                    else collection.update_one({"_id": deepcopy(alias)}, update)
                )
                result[mode] = [changed.matched_count, changed.modified_count]
            except Exception as exc:
                result[mode] = {
                    "error": type(exc).__name__,
                    "code": getattr(exc, "code", None),
                }
        result["state"] = _state(collection)
        try:
            result["deleted"] = collection.delete_one(
                {"_id": deepcopy(alias)}
            ).deleted_count
        except Exception as exc:
            result["deleted"] = {
                "error": type(exc).__name__,
                "code": getattr(exc, "code", None),
            }
        result["remaining"] = collection.count_documents({})
        output[name] = result
    return output


def nonfinite_index_candidates(collection):
    collection.delete_many({})
    values = [
        float("nan"),
        float("-inf"),
        float("inf"),
        Decimal128("NaN"),
        Decimal128("-Infinity"),
        Decimal128("Infinity"),
        1.0,
        1,
    ]
    collection.insert_many(
        [{"_id": i, "v": deepcopy(value)} for i, value in enumerate(values, 1)]
        + [{"_id": i + 10, "v": [deepcopy(value)]} for i, value in enumerate(values, 1)]
    )
    collection.create_index([("v", 1)], name="v_nonfinite")
    output = {}
    for name, operand in (
        ("nan", float("nan")),
        ("negative_infinity", float("-inf")),
        ("infinity", float("inf")),
        ("decimal_nan", Decimal128("NaN")),
        ("decimal_infinity", Decimal128("Infinity")),
        ("finite", 1),
    ):
        for operator in ("$eq", "$ne", "$in", "$nin", "$gt", "$gte", "$lt", "$lte"):
            argument = [operand] if operator in ("$in", "$nin") else operand
            query = {"v": {operator: deepcopy(argument)}}
            output[name + operator] = {
                "ids": [
                    row["_id"]
                    for row in collection.find(
                        query, hint="v_nonfinite", sort=[("_id", 1)]
                    )
                ],
                "count": collection.count_documents(query),
            }
    output["sort"] = [
        row["_id"]
        for row in collection.find({}, sort=[("v", 1), ("_id", 1)], hint="v_nonfinite")
    ]
    return output


SEMANTIC_WRITE_EFFECT_CASES = (
    ("semantic_write_effect_root_identity", root_identity),
    ("semantic_write_effect_nan_maintenance", nan_root_maintenance),
    ("semantic_write_effect_nonfinite_candidates", nonfinite_index_candidates),
    ("semantic_write_effect_many_classic", partial(many_assignments, mode="classic")),
    ("semantic_write_effect_many_pipeline", partial(many_assignments, mode="pipeline")),
    ("semantic_write_effect_bulk_ordered", partial(bulk_assignments, ordered=True)),
    ("semantic_write_effect_bulk_unordered", partial(bulk_assignments, ordered=False)),
    ("semantic_write_effect_command_channels", command_channels),
    ("semantic_write_effect_partial_unique", partial_unique_membership),
)
