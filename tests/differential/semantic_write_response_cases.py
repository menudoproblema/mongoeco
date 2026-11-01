"""Positive write DTOs and command replies with observable codec policies."""

# ruff: noqa: B023 - each closure runs before its loop advances

from datetime import datetime

from bson import Binary, Int64
from pymongo import DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne

from tests.differential.semantic_bson_admission_cases import _view
from tests.differential.semantic_bson_dispatch_cases import _describe, _options


SEED = [{"_id": 1, "v": 1}, {"_id": 2, "v": 2}]
DATE_ID = datetime(1970, 1, 1, 0, 0, 1, 1000)  # noqa: DTZ001 - naive UTC input is intentional
POLICIES = ("plain", "active", "factory", "factory-active", "failing-decoder")


def _selected(collection, policy, calls):
    class Reply(dict):
        def __init__(self, *args, **kwargs):
            calls.append({"factory": "new"})
            super().__init__(*args, **kwargs)

        def __setitem__(self, key, value):
            calls.append({"factory": "set", "key": key, "value": _describe(value)})
            super().__setitem__(key, value)

    def decode(value):
        calls.append({"decoder": _describe(value)})
        if policy == "failing-decoder":
            message = "write response decoder called"
            raise ValueError(message)
        return {"decoded": _view(value)}

    return _options(
        collection,
        [(kind, decode) for kind in (int, Int64, datetime, bytes)]
        if policy in {"active", "factory-active", "failing-decoder"}
        else (),
        document_class=Reply if policy in {"factory", "factory-active"} else dict,
        aware=True,
    )


def _reset(collection):
    collection.delete_many({})
    collection.insert_many(SEED)


def _result(result):
    fields = (
        "acknowledged",
        "inserted_id",
        "inserted_ids",
        "matched_count",
        "modified_count",
        "deleted_count",
        "upserted_id",
        "did_upsert",
        "inserted_count",
        "upserted_count",
        "upserted_ids",
        "bulk_api_result",
    )
    return {
        name: _view(getattr(result, name))
        for name in fields
        if hasattr(type(result), name)
    }


def _observe(collection, selected, calls, operation):
    try:
        result = operation(selected)
        observation = {"ok": True, "result": _result(result)}
    except Exception as error:
        observation = {
            "ok": False,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
        }
    observation["calls"] = list(calls)
    observation["state"] = _view(list(collection.find().sort("_id")))
    return observation


def write_dto_options(collection):
    output = {}
    for policy in POLICIES:
        for route in (
            "insert",
            "inserts",
            "update",
            "upsert",
            "null-upsert",
            "replace",
            "delete",
        ):
            _reset(collection)
            calls = []
            selected = _selected(collection, policy, calls)
            operations = {
                "insert": lambda c: c.insert_one({"_id": Binary(b"ab", 0)}),
                "inserts": lambda c: c.insert_many([{"_id": 3}, {"_id": 4}]),
                "update": lambda c: c.update_many({}, {"$set": {"v": 3}}),
                "upsert": lambda c: c.update_one(
                    {"_id": DATE_ID}, {"$set": {"v": 3}}, upsert=True
                ),
                "null-upsert": lambda c: c.update_one(
                    {"_id": None}, {"$set": {"v": 3}}, upsert=True
                ),
                "replace": lambda c: c.replace_one({"_id": 1}, {"v": 3}),
                "delete": lambda c: c.delete_many({}),
            }
            output[f"{policy}/{route}"] = _observe(
                collection, selected, calls, operations[route]
            )
    return output


def bulk_dto_options(collection):
    output = {}
    for policy in POLICIES:
        for ordered in (True, False):
            _reset(collection)
            calls = []
            selected = _selected(collection, policy, calls)
            models = [
                UpdateOne({"_id": DATE_ID}, {"$set": {"v": 4}}, upsert=True),
                InsertOne({"_id": 3, "v": 3}),
                UpdateOne({"_id": Binary(b"ab", 5)}, {"$set": {"v": 5}}, upsert=True),
                UpdateMany({"_id": 1}, {"$set": {"v": 6}}),
                DeleteOne({"_id": 2}),
                ReplaceOne({"_id": 4}, {"v": 7}, upsert=True),
            ]
            output[f"{policy}/{ordered}"] = _observe(
                collection,
                selected,
                calls,
                lambda c: c.bulk_write(models, ordered=ordered),
            )
    return output


def command_write_options(collection):
    output = {}
    for policy in POLICIES:
        for route in ("insert", "update", "upsert", "delete", "write-error", "mixed"):
            _reset(collection)
            calls = []
            selected = _selected(collection, policy, calls)
            database = collection.database.with_options(
                codec_options=selected.codec_options
            )
            commands = {
                "insert": {"insert": collection.name, "documents": [{"_id": 3}]},
                "update": {
                    "update": collection.name,
                    "updates": [{"q": {}, "u": {"$set": {"v": 3}}, "multi": True}],
                },
                "upsert": {
                    "update": collection.name,
                    "updates": [
                        {"q": {"_id": DATE_ID}, "u": {"$set": {"v": 3}}, "upsert": True}
                    ],
                },
                "delete": {
                    "delete": collection.name,
                    "deletes": [{"q": {}, "limit": 0}],
                },
            }
            error = {"q": {"_id": 1}, "u": {"$set": {"_id": 9}}}
            commands["write-error"] = {"update": collection.name, "updates": [error]}
            commands["mixed"] = {
                "update": collection.name,
                "updates": [*commands["upsert"]["updates"], error],
            }
            try:
                reply = database.command(commands[route])
                observed = {"ok": True, "reply": _view(reply)}
            except Exception as error:
                observed = {
                    "ok": False,
                    "exception": type(error).__name__,
                    "code": getattr(error, "code", None),
                }
            observed["calls"] = list(calls)
            observed["state"] = _view(list(collection.find().sort("_id")))
            output[f"{policy}/{route}"] = observed
    return output


def write_immutable_error_options(collection):
    output = {}
    for policy in POLICIES:
        for route in ("update", "update-many", "find-update", "bulk"):
            _reset(collection)
            calls = []
            selected = _selected(collection, policy, calls)
            query, change = {"_id": 1}, {"$set": {"_id": 9}}
            operations = {
                "update": lambda c: c.update_one(query, change),
                "update-many": lambda c: c.update_many(query, change),
                "find-update": lambda c: c.find_one_and_update(query, change),
                "bulk": lambda c: c.bulk_write([UpdateOne(query, change)]),
            }
            try:
                operations[route](selected)
                observed = {"ok": True}
            except Exception as error:
                details = getattr(error, "details", None) or {}
                item = (
                    details["writeErrors"][0]
                    if route == "bulk" and "writeErrors" in details
                    else details
                )
                observed = {
                    "ok": False,
                    "exception": type(error).__name__,
                    "code": item.get("code", getattr(error, "code", None)),
                    "errmsg": item.get("errmsg", str(error)),
                }
            observed["calls"] = list(calls)
            observed["state"] = _view(list(collection.find().sort("_id")))
            output[f"{policy}/{route}"] = observed
    return output


SEMANTIC_WRITE_RESPONSE_CASES = (
    ("semantic_write_dto_options", write_dto_options),
    ("semantic_bulk_dto_options", bulk_dto_options),
    ("semantic_command_write_options", command_write_options),
    ("semantic_write_immutable_error_options", write_immutable_error_options),
)
