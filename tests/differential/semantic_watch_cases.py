"""Local watch preparation and stream-owned expression context vs native RS."""

import time

from datetime import datetime

from bson import BSON
from bson.raw_bson import RawBSONDocument


SEED = [{"_id": "initial"}]


def _error(error, *, phase, stream=None):
    result = {
        "phase": phase,
        "error": type(error).__name__,
        "code": getattr(error, "code", None),
    }
    if stream is not None:
        result["alive"] = stream.alive
    return result


def _next(stream):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        event = stream.try_next()
        if event is not None:
            return event
    message = "watch event was not delivered"
    raise AssertionError(message)


def preparation(collection):
    output = {}
    for name, pipeline in (
        ("not_list", {"$match": {}}),
        ("tuple", ({"$match": {}},)),
        ("empty_stage", [{}]),
        ("scalar_stage", [3]),
        ("multiple_keys", [{"$match": {}, "$project": {"x": 1}}]),
        ("unknown_stage", [{"$notAStage": {}}]),
        ("stage_without_dollar", [{"notAStage": {}}]),
        ("empty_stage_name", [{"": {}}]),
        ("group", [{"$group": {"_id": 1}}]),
        ("invalid_group", [{"$group": {}}]),
        ("group_array", [{"$group": []}]),
        ("limit", [{"$limit": 1}]),
        ("match_array", [{"$match": []}]),
        ("match_unknown", [{"$match": {"v": {"$notAnOperator": 1}}}]),
        ("expr_exists", [{"$match": {"$expr": {"$exists": "$v"}}}]),
        ("project_array", [{"$project": []}]),
        ("project_empty", [{"$project": {}}]),
        ("unknown_expression", [{"$project": {"x": {"$notAnOperator": 1}}}]),
        ("collision", [{"$project": {"a": 1, "a.b": 1}}]),
        ("mixed_collision", [{"$project": {"a": 1, "a.b": 0}}]),
        ("unknown_variable", [{"$set": {"x": "$$notDefined"}}]),
        ("constant_division", [{"$set": {"x": {"$divide": [1, 0]}}}]),
        ("replace_path", [{"$replaceWith": "$fullDocument"}]),
        ("replace_constant", [{"$replaceWith": 0}]),
        ("invalid_replace_root", [{"$replaceRoot": {"wrong": 1}}]),
        ("missing_replace_root", [{"$replaceRoot": {}}]),
        ("replace_root_array", [{"$replaceRoot": []}]),
        ("unset_empty", [{"$unset": []}]),
    ):
        stream = None
        try:
            stream = collection.watch(pipeline, max_await_time_ms=20)
            output[name] = {"phase": "open", "ok": True}
        except Exception as error:
            output[name] = _error(error, phase="open")
        finally:
            if stream is not None:
                stream.close()
    return output


def evaluation(collection):
    output = {}
    for name, pipeline in (
        ("replace_identity", [{"$replaceWith": "$$ROOT"}]),
        (
            "replace_subject",
            [
                {
                    "$replaceWith": {
                        "$mergeObjects": ["$$ROOT", {"subject": "$fullDocument.v"}]
                    }
                }
            ],
        ),
        ("replace_path", [{"$replaceWith": "$fullDocument"}]),
        ("replace_constant", [{"$replaceWith": 0}]),
        ("remove_token", [{"$project": {"_id": 0}}]),
        ("alter_token", [{"$set": {"_id": "$fullDocument._id"}}]),
        (
            "runtime_divide",
            [{"$set": {"subject": {"$divide": [1, "$fullDocument.v"]}}}],
        ),
    ):
        collection.delete_many({})
        stream = None
        phase = "open"
        try:
            stream = collection.watch(pipeline, max_await_time_ms=20)
            phase = "event"
            collection.insert_one({"_id": 1, "v": 0})
            event = _next(stream)
            fields = {
                key: event[key]
                for key in (
                    "operationType",
                    "documentKey",
                    "fullDocument",
                    "subject",
                    "v",
                )
                if key in event
            }
            output[name] = {
                "phase": "event",
                "value": BSON.encode(fields).hex(),
                "alive": stream.alive,
            }
        except Exception as error:
            output[name] = _error(error, phase=phase, stream=stream)
        finally:
            if stream is not None:
                stream.close()
    return output


def typed_match(collection):
    collection.delete_many({})
    pipeline = [
        {"$match": {"fullDocument.v": None}},
        {"$project": {"documentKey": 1, "kind": {"$type": "$fullDocument.v"}}},
    ]
    stream = collection.watch(pipeline, max_await_time_ms=20)
    try:
        collection.insert_one({"_id": 1, "v": None})
        raw = BSON.encode({"_id": 2, "v": None}).replace(b"\x0av\x00", b"\x06v\x00")
        collection.database.command(
            {"insert": collection.name, "documents": [RawBSONDocument(raw)]}
        )
        collection.insert_one({"_id": 3})
        collection.insert_one({"_id": "barrier"})
        result = []
        while True:
            event = _next(stream)
            if event["documentKey"]["_id"] == "barrier":
                break
            result.append({"id": event["documentKey"]["_id"], "kind": event["kind"]})
        return result
    finally:
        stream.close()


def clocks(collection):
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    output = {}
    pipeline = [
        {"$match": {"ns.db": collection.database.name, "ns.coll": collection.name}},
        {"$set": {"probeNow": "$$NOW"}},
        {
            "$set": {
                "sameNow": {"$eq": ["$probeNow", "$$NOW"]},
                "kind": {"$type": "$$NOW"},
            }
        },
    ]
    for scope, owner in (
        ("collection", collection),
        ("database", collection.database),
        ("client", client),
    ):
        collection.delete_many({})
        stream = owner.watch(pipeline, max_await_time_ms=20)
        resumed = None
        try:
            collection.insert_one({"_id": 1})
            first = _next(stream)
            empty_batch = stream.try_next() is None
            time.sleep(0.02)
            collection.insert_one({"_id": 2})
            second = _next(stream)
            time.sleep(0.02)
            resumed = owner.watch(
                pipeline, resume_after=first["_id"], max_await_time_ms=20
            )
            replay = _next(resumed)
            output[scope] = {
                "date": isinstance(first["probeNow"], datetime),
                "kind": first["kind"],
                "same_within": first["sameNow"] and second["sameNow"],
                "same_between_batches": first["probeNow"] == second["probeNow"],
                "empty_batch": empty_batch,
                "reopened_later": replay["probeNow"] > first["probeNow"],
                "replayed_id": replay["documentKey"]["_id"],
            }
        finally:
            if resumed is not None:
                resumed.close()
            stream.close()
    return output


def shared_validation(collection):
    output = {}
    for populated in (False, True):
        for name, stage in (
            ("project_empty", {"$project": {}}),
            ("project_array", {"$project": []}),
            ("collision", {"$project": {"a": 1, "a.b": 1}}),
            ("mixed_collision", {"$project": {"a": 1, "a.b": 0}}),
            ("replace_unknown", {"$replaceRoot": {"wrong": 1}}),
            ("replace_missing", {"$replaceRoot": {}}),
            ("replace_array", {"$replaceRoot": []}),
            ("replace_constant", {"$replaceWith": 0}),
            ("group_empty", {"$group": {}}),
            ("group_array", {"$group": []}),
            ("match_array", {"$match": []}),
            ("unknown_stage", {"$notAStage": {}}),
        ):
            collection.delete_many({})
            if populated:
                collection.insert_one({"_id": 1, "a": {"b": 2}})
            try:
                rows = list(collection.aggregate([stage]))
                result = {"rows": [BSON.encode(row).hex() for row in rows]}
            except Exception as error:
                result = _error(error, phase="aggregate")
            output[f"aggregate-{name}-{populated}"] = result
            if next(iter(stage)) in {"$project", "$replaceRoot", "$replaceWith"}:
                try:
                    counts = collection.update_one({}, [stage])
                    result = {
                        "matched": counts.matched_count,
                        "modified": counts.modified_count,
                    }
                except Exception as error:
                    result = _error(error, phase="update")
                output[f"update-{name}-{populated}"] = result
    return output


SEMANTIC_WATCH_CASES = (
    ("semantic_watch_preparation", preparation),
    ("semantic_watch_evaluation", evaluation),
    ("semantic_watch_typed_match", typed_match),
    ("semantic_watch_clocks", clocks),
    ("semantic_watch_shared_validation", shared_validation),
)
