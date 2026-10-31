"""Public command/findAndModify replies and committed BSON effects."""

import time

from copy import deepcopy

from bson import BSON

from tests.differential.semantic_update_event_cases import (
    _BARRIER,
    _drain,
    _observe_event,
)


SEED = [{"_id": "initial"}]
ENTRIES = ("update-command", "find-command", "find-api")


def _reply(collection, entry, update, *, replacement, upsert, image_options=None):  # noqa: PLR0913 - independent public request options
    image_options = image_options or {}
    selector = {"_id": 1}
    if entry == "update-command":
        reply = collection.database.command(
            {
                "update": collection.name,
                "updates": [{"q": selector, "u": update, "upsert": upsert}],
            }
        )
        output = {"n": reply["n"], "nModified": reply["nModified"]}
        if "upserted" in reply:
            output["upserted"] = BSON.encode({"values": reply["upserted"]}).hex()
        if "writeErrors" in reply:
            output["writeErrors"] = [
                {"index": error["index"], "code": error["code"]}
                for error in reply["writeErrors"]
            ]
        return output
    if entry == "find-command":
        command = {
            "findAndModify": collection.name,
            "query": selector,
            "update": update,
            "new": image_options.get("return_new", True),
            "upsert": upsert,
        }
        if "projection" in image_options:
            command["fields"] = image_options["projection"]
        reply = collection.database.command(command)
        return {
            "value": None
            if reply["value"] is None
            else BSON.encode(reply["value"]).hex(),
            "lastErrorObject": BSON.encode(reply["lastErrorObject"]).hex(),
        }
    method = (
        collection.find_one_and_replace
        if replacement
        else collection.find_one_and_update
    )
    options = {
        "return_document": image_options.get("return_new", True),
        "upsert": upsert,
    }
    if "projection" in image_options:
        options["projection"] = image_options["projection"]
    value = method(selector, update, **options)
    # This API exposes an image, not modified_count. Do not infer a counter.
    return {"value": None if value is None else BSON.encode(value).hex()}


def _run(collection, entry, seed, update, *, replacement=False, image_options=None):  # noqa: PLR0913 - independent public request options
    collection.delete_many({})
    if seed is not None:
        collection.insert_one(deepcopy(seed))
    stream = collection.watch(max_await_time_ms=50)
    try:
        try:
            reply = _reply(
                collection,
                entry,
                deepcopy(update),
                replacement=replacement,
                upsert=seed is None,
                image_options=image_options,
            )
        except Exception as error:
            reply = {
                "error": type(error).__name__,
                "code": getattr(error, "code", None),
            }
        collection.insert_one({"_id": _BARRIER})
        return {
            "reply": reply,
            "events": _drain(stream, collection),
            "state": [
                BSON.encode(row).hex()
                for row in collection.find({"_id": {"$ne": _BARRIER}})
            ],
        }
    finally:
        stream.close()


def entrypoint_events(collection):
    output = {}
    for entry in ENTRIES:
        for kind in ("classic", "pipeline", "replacement"):
            for padding in (0, 1000):
                for noop in (False, True):
                    seed = {"_id": 1, "v": 1.0 if noop else 1, "pad": "x" * padding}
                    update = (
                        {"_id": 1, "v": 1.0, "pad": "x" * padding}
                        if kind == "replacement"
                        else [{"$set": {"v": 1.0}}]
                        if kind == "pipeline"
                        else {"$set": {"v": 1.0}}
                    )
                    output[f"{entry}-{kind}-{padding}-{noop}"] = _run(
                        collection,
                        entry,
                        seed,
                        update,
                        replacement=kind == "replacement",
                    )
            update = (
                {"_id": 1, "v": 1.0}
                if kind == "replacement"
                else [{"$set": {"v": 1.0}}]
                if kind == "pipeline"
                else {"$set": {"v": 1.0}}
            )
            output[f"{entry}-{kind}-upsert"] = _run(
                collection,
                entry,
                None,
                update,
                replacement=kind == "replacement",
            )
    return output


def entrypoint_ids(collection):
    output = {}
    for entry in ENTRIES:
        for kind in ("classic", "pipeline", "replacement"):
            for value in (1.0, 2):
                seed = {"_id": 1, "pad": "x" * 1000}
                update = (
                    {"_id": value, "pad": seed["pad"]}
                    if kind == "replacement"
                    else [{"$set": {"_id": value}}]
                    if kind == "pipeline"
                    else {"$set": {"_id": value}}
                )
                output[f"{entry}-{kind}-{type(value).__name__}"] = _run(
                    collection,
                    entry,
                    seed,
                    update,
                    replacement=kind == "replacement",
                )
    return output


def entrypoint_images(collection):
    output = {}
    for entry in ("find-command", "find-api"):
        for kind in ("classic", "pipeline", "replacement"):
            seed = {"_id": 1, "v": 1, "pad": "x" * 1000}
            update = (
                {**seed, "v": 1.0}
                if kind == "replacement"
                else [{"$set": {"v": 1.0}}]
                if kind == "pipeline"
                else {"$set": {"v": 1.0}}
            )
            for return_new in (False, True):
                for project in ("all", "id", "empty"):
                    options = {"return_new": return_new}
                    if project == "id":
                        options["projection"] = {"_id": 1}
                    elif project == "empty":
                        options["projection"] = {"_id": 0, "v": 0, "pad": 0}
                    output[f"{entry}-{kind}-{return_new}-{project}"] = _run(
                        collection,
                        entry,
                        seed,
                        update,
                        replacement=kind == "replacement",
                        image_options=options,
                    )
    return output


def committed_prefix(collection):
    output = {}
    for entry in ("update-command", "update-many"):
        collection.delete_many({})
        collection.insert_many([{"_id": 1, "v": 1}, {"_id": 2, "v": "invalid"}])
        stream = collection.watch(max_await_time_ms=50)
        try:
            try:
                if entry == "update-command":
                    reply = collection.database.command(
                        {
                            "update": collection.name,
                            "updates": [
                                {"q": {"_id": 1}, "u": {"$set": {"v": 1.0}}},
                                {"q": {"_id": 2}, "u": {"$inc": {"v": 1}}},
                            ],
                        }
                    )
                    observed = {
                        "n": reply["n"],
                        "nModified": reply["nModified"],
                        "errors": [
                            {"index": error["index"], "code": error["code"]}
                            for error in reply.get("writeErrors", [])
                        ],
                    }
                else:
                    result = collection.update_many({}, {"$inc": {"v": 1}})
                    observed = {
                        "matched": result.matched_count,
                        "modified": result.modified_count,
                    }
            except Exception as error:
                observed = {
                    "error": type(error).__name__,
                    "code": getattr(error, "code", None),
                }
            # A later request must not be needed to dispatch the already
            # committed prefix after the write API raises an error.
            first = None
            deadline = time.monotonic() + 5
            while first is None and time.monotonic() < deadline:
                first = stream.try_next()
            if first is None:
                message = "committed prefix event was not delivered after write failure"
                raise AssertionError(message)
            collection.insert_one({"_id": _BARRIER})
            output[entry] = {
                "reply": observed,
                "events": [
                    _observe_event(first, collection),
                    *_drain(stream, collection),
                ],
                "state": [
                    BSON.encode(row).hex()
                    for row in collection.find({"_id": {"$ne": _BARRIER}})
                ],
            }
        finally:
            stream.close()
    return output


SEMANTIC_UPDATE_ENTRYPOINT_CASES = (
    ("semantic_update_entrypoint_events", entrypoint_events),
    ("semantic_update_entrypoint_ids", entrypoint_ids),
    ("semantic_update_entrypoint_images", entrypoint_images),
    ("semantic_update_entrypoint_committed_prefix", committed_prefix),
)
