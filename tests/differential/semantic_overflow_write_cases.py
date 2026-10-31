"""Native strict arithmetic writes, error payloads and partial effects."""

from copy import deepcopy
from functools import partial

from bson import Decimal128, Int64, ObjectId
from pymongo import ReturnDocument, UpdateOne

from tests.support_write_parity import (
    SEMANTIC_ERRORS,
    BSONObservation,
    public_error,
    public_result,
)


WRITE_SEED = [
    {"_id": 0, "a": 1, "b": 0, "zone": "many"},
    {"_id": 1, "a": Int64(2**63 - 1), "b": 0, "zone": "many"},
    {"_id": 2, "a": 1, "b": 0, "zone": "many"},
    {"_id": 3, "a": Int64(-(2**63)), "b": 0},
    {"_id": 4, "a": 4, "b": 0},
    {"_id": 5, "items": [{"n": 1}, {"n": Int64(2**63 - 1)}], "b": 0},
    *[
        {"_id": identifier, "a": Int64(2**63 - 1), "b": 0}
        for identifier in (
            'a"b',
            {"k": 1},
            Int64(99),
            Decimal128("1.5"),
            ObjectId("000000000000000000000001"),
        )
    ],
]


def _state(collection):
    return list(collection.aggregate([{"$sort": {"_id": 1}}]))


def _reset(collection):
    collection.delete_many({})
    for document in WRITE_SEED:
        collection.insert_one(deepcopy(document))


def _observe(collection, invoke):
    bson = BSONObservation(WRITE_SEED, error_contract=True)
    try:
        result = {"result": public_result(invoke(), bson)}
    except SEMANTIC_ERRORS as error:
        result = {"error": public_error(error, bson)}
        details = getattr(error, "details", None) or {}
        result["diagnostics"] = (
            [item.get("errmsg") for item in details["writeErrors"]]
            if "writeErrors" in details
            else details.get("errmsg")
        )
    result["state"] = bson.documents(_state(collection))
    return result


def individual_writes(collection):
    output = {}
    for method in ("update_one", "find_one_and_update"):
        for operator in ("$inc", "$mul"):
            for identifier in (1, 3):
                _reset(collection)
                operand = (
                    (1 if identifier == 1 else -1)
                    if operator == "$inc"
                    else (2 if identifier == 1 else -1)
                )
                update = {"$set": {"b": 1}, operator: {"a": operand}}
                kwargs = (
                    {"return_document": ReturnDocument.AFTER}
                    if method == "find_one_and_update"
                    else {}
                )
                output[f"{method}_{operator}_{identifier}"] = _observe(
                    collection,
                    partial(
                        getattr(collection, method),
                        {"_id": identifier},
                        update,
                        **kwargs,
                    ),
                )
                output[f"recovery_{method}_{operator}_{identifier}"] = _observe(
                    collection,
                    partial(
                        collection.update_one, {"_id": identifier}, {"$inc": {"b": 1}}
                    ),
                )
    return output


def many_write(collection):
    _reset(collection)
    return _observe(
        collection,
        lambda: collection.update_many(
            {"zone": "many"},
            {"$set": {"b": 1}, "$inc": {"a": 1}},
            hint="_id_",
        ),
    )


def bulk_write(collection):
    output = {}
    for ordered in (True, False):
        _reset(collection)
        output[str(ordered)] = _observe(
            collection,
            partial(
                collection.bulk_write,
                [
                    UpdateOne({"_id": i}, {"$set": {"b": 1}, "$inc": {"a": 1}})
                    for i in (0, 1, 2)
                ],
                ordered=ordered,
            ),
        )
    return output


def array_write(collection):
    _reset(collection)
    return _observe(
        collection,
        lambda: collection.update_one(
            {"_id": 5},
            {"$set": {"b": 1}, "$inc": {"items.$[].n": 1}},
        ),
    )


def raw_commands(collection):
    output = {}
    for command in ("update", "findAndModify"):
        _reset(collection)
        update = {"$set": {"b": 1}, "$inc": {"a": 1}}
        request = (
            {"update": collection.name, "updates": [{"q": {"_id": 1}, "u": update}]}
            if command == "update"
            else {
                "findAndModify": collection.name,
                "query": {"_id": 1},
                "update": update,
                "new": True,
            }
        )
        output[command] = _observe(
            collection, partial(collection.database.command, request)
        )
    return output


def identifiers(collection):
    output = []
    for document in WRITE_SEED[6:]:
        _reset(collection)
        output.append(
            _observe(
                collection,
                partial(
                    collection.update_one,
                    {"_id": document["_id"]},
                    {"$inc": {"a": 1}},
                ),
            )
        )
    return output


SEMANTIC_OVERFLOW_WRITE_CASES = (
    ("semantic_overflow_individual_writes", individual_writes),
    ("semantic_overflow_many_write", many_write),
    ("semantic_overflow_bulk_write", bulk_write),
    ("semantic_overflow_array_write", array_write),
    ("semantic_overflow_raw_commands", raw_commands),
    ("semantic_overflow_identifiers", identifiers),
)


def write_controls(collection):
    output = {}
    for operator in ("$inc", "$mul"):
        for name, operand in (
            ("double", 2.0),
            ("decimal", Decimal128("2")),
            ("long", Int64(-1)),
        ):
            _reset(collection)
            output[f"{operator}_{name}"] = _observe(
                collection,
                partial(
                    collection.update_one,
                    {"_id": 1},
                    {operator: {"a": operand}},
                ),
            )
            output[f"{operator}_{name}_missing"] = _observe(
                collection,
                partial(
                    collection.update_one,
                    {"_id": 4},
                    {operator: {"missing": operand}},
                ),
            )
    return output


SEMANTIC_OVERFLOW_WRITE_CASES += (("semantic_overflow_write_controls", write_controls),)


def numeric_noops(collection):
    scenarios = (
        ("set_decimal_cohort", Decimal128("1.50"), "$set", Decimal128("1.5")),
        ("set_zero_sign", -0.0, "$set", 0.0),
        ("set_nan", float("nan"), "$set", float("nan")),
        ("inc_zero", 2.0, "$inc", 0.0),
        ("mul_one", 2.0, "$mul", 1.0),
        ("inc_decimal_zero", Decimal128("1.50"), "$inc", 0.0),
        ("mul_decimal_zero", Decimal128("0"), "$mul", 1.2345678901234567),
        ("inc_negative_zero", -0.0, "$inc", 0.0),
        ("inc_nan", float("nan"), "$inc", 1.0),
        ("mul_nan", float("nan"), "$mul", 2.0),
        ("set_width", 1, "$set", Int64(1)),
        ("inc_width", 1, "$inc", Int64(0)),
        ("set_boolean", True, "$set", 1),
        ("set_object", {"x": 2.0}, "$set", {"x": 2.0}),
        ("set_array", [2.0], "$set", [2.0]),
        ("set_key_order", {"a": 1, "b": 2}, "$set", {"b": 2, "a": 1}),
        ("inc_mixed_decimal", Decimal128("1.5"), "$inc", 2.0),
        ("mul_mixed_decimal", Decimal128("1.5"), "$mul", 2.0),
    )
    output = {}
    for name, start, operator, operand in scenarios:
        _reset(collection)
        collection.update_one({"_id": 4}, {"$set": {"a": start}})
        output[name] = _observe(
            collection,
            partial(
                collection.update_one,
                {"_id": 4},
                {operator: {"a": operand}},
            ),
        )
    for name, operand in (
        ("nan", float("nan")),
        ("infinity", float("inf")),
        ("decimal_nan", Decimal128("NaN")),
        ("decimal_scaled", Decimal128("1.50")),
    ):
        _reset(collection)
        output[f"mul_missing_{name}"] = _observe(
            collection,
            partial(
                collection.update_one,
                {"_id": 4},
                {"$mul": {"missing": operand}},
            ),
        )
    return output


SEMANTIC_OVERFLOW_WRITE_CASES += (("semantic_overflow_numeric_noops", numeric_noops),)


def write_identity_contexts(collection):
    scenarios = (
        ("pipeline_width", 1, "update_one", [{"$set": {"a": Int64(1)}}]),
        (
            "pipeline_cohort",
            Decimal128("1.50"),
            "update_one",
            [{"$set": {"a": Decimal128("1.5")}}],
        ),
        ("pipeline_same", 2.0, "update_one", [{"$set": {"a": 2.0}}]),
        ("replace_width", 1, "replace_one", {"_id": 4, "a": Int64(1), "b": 0}),
        ("replace_missing_id", 1, "replace_one", {"a": 1, "b": 0}),
        ("replace_same", 2.0, "replace_one", {"_id": 4, "a": 2.0, "b": 0}),
    )
    output = {}
    for name, start, method, payload in scenarios:
        _reset(collection)
        collection.update_one({"_id": 4}, {"$set": {"a": start}})
        output[name] = _observe(
            collection,
            partial(
                getattr(collection, method),
                {"_id": 4},
                payload,
            ),
        )
    return output


SEMANTIC_OVERFLOW_WRITE_CASES += (
    ("semantic_overflow_write_identity", write_identity_contexts),
)
