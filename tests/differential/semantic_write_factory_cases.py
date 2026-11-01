"""Write decoding failure precedences, factory effects and committed state."""

from pymongo import InsertOne, UpdateOne

from tests.differential.semantic_bson_admission_cases import _view
from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_write_response_cases import _result


SEED = [{"_id": 1, "v": 1, "payload": {"n": 2}}, {"_id": 2, "v": 2}]
ROUTES = (
    "insert",
    "inserts",
    "update",
    "upsert",
    "replace",
    "delete",
    "bulk",
    "command-insert",
    "command-update",
    "command-upsert",
    "command-delete",
    "command-write-error",
    "command-mixed",
    "command-find-error",
    "command-default",
    "command-find",
    "command-find-default",
    "find-update",
    "find-replace",
    "find-delete",
    "find-no-match",
    "find-upsert",
    "find-upsert-before",
    "find-error",
)


def _execute(collection, route):  # noqa: PLR0911 - enumerate supported public surfaces
    query, change = {"_id": 1}, {"$set": {"v": 3}}
    bad = {"$set": {"_id": 9}}
    if route.startswith("command-"):
        commands = {
            "command-insert": {"insert": collection.name, "documents": [{"_id": 3}]},
            "command-update": {
                "update": collection.name,
                "updates": [{"q": query, "u": change}],
            },
            "command-upsert": {
                "update": collection.name,
                "updates": [{"q": {"_id": 3}, "u": change, "upsert": True}],
            },
            "command-delete": {
                "delete": collection.name,
                "deletes": [{"q": query, "limit": 1}],
            },
            "command-write-error": {
                "update": collection.name,
                "updates": [{"q": query, "u": bad}],
            },
            "command-mixed": {
                "update": collection.name,
                "updates": [
                    {"q": {"_id": 3}, "u": change, "upsert": True},
                    {"q": query, "u": bad},
                ],
            },
            "command-find-error": {
                "findAndModify": collection.name,
                "query": query,
                "update": bad,
            },
        }
        commands["command-default"] = commands["command-insert"]
        commands["command-find"] = {
            "findAndModify": collection.name,
            "query": query,
            "update": change,
        }
        commands["command-find-default"] = commands["command-find"]
        database = collection.database.with_options(
            codec_options=collection.codec_options
        )
        if route.endswith("default"):
            return database.command(commands[route])
        return database.command(commands[route], codec_options=collection.codec_options)
    if route.startswith("find-"):
        if route == "find-delete":
            return collection.find_one_and_delete(query)
        if route == "find-replace":
            return collection.find_one_and_replace(
                query, {"v": 3, "payload": {"n": 4}}, return_document=True
            )
        if route in {"find-upsert", "find-upsert-before"}:
            return collection.find_one_and_update(
                {"_id": 3},
                {"$set": {"v": 3, "payload": {"n": 4}}},
                upsert=True,
                return_document=route == "find-upsert",
            )
        return collection.find_one_and_update(
            {"_id": 9} if route == "find-no-match" else query,
            bad if route == "find-error" else change,
        )
    if route == "insert":
        return collection.insert_one({"_id": 3})
    if route == "inserts":
        return collection.insert_many([{"_id": 3}, {"_id": 4}])
    if route == "update":
        return collection.update_one(query, change)
    if route == "upsert":
        return collection.update_one({"_id": 3}, change, upsert=True)
    if route == "replace":
        return collection.replace_one(query, {"v": 3})
    if route == "delete":
        return collection.delete_one(query)
    return collection.bulk_write([InsertOne({"_id": 3}), UpdateOne(query, change)])


def _trace(collection, route, *, registry, fail_at=None, fail_key=None):
    events, objects = [], []

    def describe(value):
        if isinstance(value, ObservedDocument):
            return {"document": value.trace_id}
        if isinstance(value, list):
            return {"list": [describe(item) for item in value]}
        return _view(value)

    class ObservedDocument(dict):
        def __init__(self, *args, **kwargs):
            self.trace_id = len(objects)
            objects.append(self)
            events.append(["init", self.trace_id, len(args), list(kwargs)])
            if fail_at is not None and self.trace_id == fail_at:
                message = "write factory rejected construction"
                raise ValueError(message)
            super().__init__(*args, **kwargs)

        def __setitem__(self, key, value):
            events.append(["set", self.trace_id, key, describe(value)])
            if key == fail_key:
                message = "write factory rejected field"
                raise ValueError(message)
            super().__setitem__(key, value)

    def number(value):
        events.append(["decode", value])
        return value

    selected = _options(
        collection, [(int, number)] if registry else (), document_class=ObservedDocument
    )
    try:
        result = _execute(selected, route)
        observed = {
            "ok": True,
            "type": type(result).__name__,
            "result": _view(result)
            if isinstance(result, dict) or result is None
            else _result(result),
        }
    except Exception as error:
        details = getattr(error, "details", None) or {}
        observed = {
            "ok": False,
            "phase": "invoke",
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
            "message": details.get("errmsg", str(error)),
        }
    return {
        **observed,
        "events": events,
        "constructed_keys": [list(row) for row in objects],
        "state": _view(list(collection.find().sort("_id"))),
    }


def _reset(collection):
    collection.delete_many({})
    collection.insert_many(SEED)


def write_factory_construction(collection):
    output = {}
    for route in ROUTES:
        for registry in (False, True):
            for fail_at in (None, 0, 1, 2, 3, 4):
                _reset(collection)
                output[f"{route}/{registry}/{fail_at}"] = _trace(
                    collection, route, registry=registry, fail_at=fail_at
                )
    return output


def write_factory_assignment(collection):
    output = {}
    for route in ROUTES:
        for registry in (False, True):
            for key in (
                "n",
                "ok",
                "lastErrorObject",
                "value",
                "_id",
                "payload",
                "code",
                "errmsg",
            ):
                _reset(collection)
                output[f"{route}/{registry}/{key}"] = _trace(
                    collection, route, registry=registry, fail_key=key
                )
    return output


SEMANTIC_WRITE_FACTORY_CASES = (
    ("semantic_write_factory_construction", write_factory_construction),
    ("semantic_write_factory_assignment", write_factory_assignment),
)
