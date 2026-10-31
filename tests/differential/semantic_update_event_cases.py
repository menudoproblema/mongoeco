"""Native committed event contents; tokens/time are opaque, scope is relational."""

import time

from copy import deepcopy
from functools import partial

from bson import BSON, Code, Decimal128, Int64
from pymongo import ReplaceOne, UpdateOne

from tests.differential.semantic_update_identity_cases import SCENARIOS


SEED = [{"_id": "initial"}]
_BARRIER = "__event_barrier"


def _observe_event(event, collection):
    # No event field below is normalized by a production comparator. BSON
    # preserves types, scope order and binary payload/subtype inside contents.
    output = {
        "operationType": event["operationType"],
        "documentKey": BSON.encode(event["documentKey"]).hex(),
        "scope_matches": event["ns"]
        == {"db": collection.database.name, "coll": collection.name},
    }
    for field in ("fullDocument", "updateDescription"):
        if field in event:
            output[field] = (
                BSON.encode(event[field]).hex() if event[field] is not None else None
            )
    return output


def _drain(stream, collection):
    observed = []
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        event = stream.try_next()
        if event is None:
            continue
        if event.get("documentKey", {}).get("_id") == _BARRIER:
            return observed
        observed.append(_observe_event(event, collection))
    message = "committed event barrier was not observed"
    raise AssertionError(message)


def _single(collection, seed, update, *, mode="classic", options=None):
    collection.delete_many({})
    if seed is not None:
        collection.insert_one(deepcopy(seed))
    stream = collection.watch(max_await_time_ms=50)
    try:
        try:
            result = (
                collection.replace_one({}, deepcopy(update), **(options or {}))
                if mode == "replacement"
                else collection.update_one({}, deepcopy(update), **(options or {}))
            )
            counts = {
                "matched": result.matched_count,
                "modified": result.modified_count,
                "upserted": result.upserted_id is not None,
            }
        except Exception as exc:
            counts = {"error": type(exc).__name__, "code": getattr(exc, "code", None)}
        collection.insert_one({"_id": _BARRIER})
        events = _drain(stream, collection)
        state = [
            BSON.encode(row).hex()
            for row in collection.find({"_id": {"$ne": _BARRIER}})
        ]
        return {"result": counts, "events": events, "state": state}
    finally:
        stream.close()


def assignment_events(collection):
    output = {}
    for name, before, after in SCENARIOS:
        for padding in (0, 1000):
            seed = {"_id": 1, "v": deepcopy(before), "pad": "x" * padding}
            for mode in ("classic", "pipeline", "replacement"):
                update = (
                    {"_id": 1, "v": deepcopy(after), "pad": "x" * padding}
                    if mode == "replacement"
                    else {"$set": {"v": deepcopy(after)}}
                    if mode == "classic"
                    else [{"$set": {"v": {"$literal": deepcopy(after)}}}]
                )
                output[f"{name}-{padding}-{mode}"] = _single(
                    collection, seed, update, mode=mode
                )
    return output


def structural_events(collection):
    long = "x" * 200
    cases = (
        ("whole", {"o": {"a": 1, "b": 1}}, {"$set": {"o": {"a": 2, "b": 1}}}, {}),
        ("dotted", {"o": {"a": 1, "b": 1}}, {"$set": {"o.a": 2}}, {}),
        ("created_parent", {}, {"$set": {"a.b": 1, "a.c": 2}}, {}),
        ("created_nested", {"a": {}}, {"$set": {"a.b.c": 1, "a.b.d": 2}}, {}),
        ("array_grow", {"a": [1, 2]}, {"$set": {"a.4": 7}}, {}),
        ("unset_array", {"a": [1, 2]}, {"$unset": {"a.1": 1}}, {}),
        ("unset_null_array", {"a": [None]}, {"$unset": {"a.0": 1}}, {}),
        ("unset_all_null_array", {"a": [None, None]}, {"$unset": {"a.$[]": 1}}, {}),
        ("inc_nan", {"a": float("nan")}, {"$inc": {"a": 1}}, {}),
        ("mul_nan", {"a": Decimal128("NaN")}, {"$mul": {"a": 1}}, {}),
        (
            "push_slice_unchanged",
            {"a": [1, 2]},
            {"$push": {"a": {"$each": [], "$slice": 2}}},
            {},
        ),
        (
            "push_sort_unchanged",
            {"a": [1, 2]},
            {"$push": {"a": {"$each": [], "$sort": 1}}},
            {},
        ),
        (
            "push_append_truncate",
            {"a": [1, 2]},
            {"$push": {"a": {"$each": [3], "$slice": 2}}},
            {},
        ),
        ("push_empty_created", {}, {"$push": {"a": {"$each": []}}}, {}),
        ("add_to_set_empty_created", {}, {"$addToSet": {"a": {"$each": []}}}, {}),
        (
            "positional_one_changed",
            {"a": [{"v": 3}, {"v": 2}]},
            {"$set": {"a.$[].v": 3}},
            {},
        ),
        ("unset_nested", {"o": {"a": 1, "b": 2}}, {"$unset": {"o.a": 1}}, {}),
        ("push", {"a": [1, 2]}, {"$push": {"a": 3}}, {}),
        ("push_each", {"a": [1, 2]}, {"$push": {"a": {"$each": [3, 4]}}}, {}),
        (
            "push_slice",
            {"a": [1, 2, 3, 4]},
            {"$push": {"a": {"$each": [5], "$slice": 3}}},
            {},
        ),
        (
            "push_position",
            {"a": [1, 2, 3]},
            {"$push": {"a": {"$each": [5], "$position": 1}}},
            {},
        ),
        ("push_sort", {"a": [3, 1]}, {"$push": {"a": {"$each": [2], "$sort": 1}}}, {}),
        ("add_to_set", {"a": [1, 2]}, {"$addToSet": {"a": {"$each": [2, 3, 4]}}}, {}),
        ("pull", {"a": [1, 2, 1, 3]}, {"$pull": {"a": 1}}, {}),
        ("pull_all", {"a": [1, 2, 1, 3]}, {"$pullAll": {"a": [1]}}, {}),
        ("pop", {"a": [1, 2, 3]}, {"$pop": {"a": 1}}, {}),
        ("rename", {"a": 1}, {"$rename": {"a": "o.b"}}, {}),
        ("rename_equal", {"a": 1, "b": 1}, {"$rename": {"a": "b"}}, {}),
        ("inc_mul", {"a": 1, "b": 2}, {"$inc": {"a": 1}, "$mul": {"b": 2}}, {}),
        ("all_positional", {"a": [{"v": 1}, {"v": 2}]}, {"$set": {"a.$[].v": 3}}, {}),
        (
            "filtered_positional",
            {"a": [{"v": 1}, {"v": 2}]},
            {"$set": {"a.$[x].v": 3}},
            {"array_filters": [{"x.v": {"$gte": 2}}]},
        ),
        ("numeric_field", {"o": {"0": 1}}, {"$set": {"o.0": 2}}, {}),
        (
            "section_order",
            {"x": 1, "o": {"a": 1, "b": 2}},
            {"$set": {"z": 1, "o.b": 3}, "$unset": {"x": 1}},
            {},
        ),
        (
            "pipeline_reorder",
            {"o": {"a": 1, "b": 2}},
            [{"$set": {"o": {"$literal": {"b": 2, "a": 1}}}}],
            {},
        ),
        (
            "pipeline_shrink",
            {"a": list(range(15))},
            [{"$set": {"a": {"$slice": ["$a", 3]}}}],
            {},
        ),
        (
            "pipeline_delete",
            {"a": 1, "o": {"x": 2, "b": 3}},
            [{"$unset": ["a", "o.x"]}],
            {},
        ),
        (
            "pipeline_numeric_field",
            {"o": {"0": {"a": 1, "pad": long}, "pad": long}},
            [{"$set": {"o": {"$literal": {"0": {"a": 2, "pad": long}, "pad": long}}}}],
            {},
        ),
        (
            "pipeline_dot_field",
            {"o": {"a.b": 1, "pad": long}},
            [{"$set": {"o": {"$literal": {"a.b": 2, "pad": long}}}}],
            {},
        ),
    )
    output = {}
    for name, body, update, options in cases:
        output[name] = _single(
            collection, {"_id": 1, **body, "pad": "x" * 1000}, update, options=options
        )
    output["upsert"] = _single(
        collection, None, {"$set": {"_id": 1, "v": 1}}, options={"upsert": True}
    )
    return output


def root_id_events(collection):
    output = {}
    for name, before, after in (
        ("double", 1, 1.0),
        ("long", 1, Int64(1)),
        ("decimal", Decimal128("1.0"), Decimal128("1.00")),
        ("scope", Code("x", {"v": 1}), Code("x", {"v": 1.0})),
    ):
        for padding in (0, 1000):
            for mode in ("classic", "pipeline", "replacement"):
                seed = {"_id": before, "pad": "x" * padding}
                update = (
                    {"_id": after, "pad": "x" * padding}
                    if mode == "replacement"
                    else {"$set": {"_id": after}}
                    if mode == "classic"
                    else [{"$set": {"_id": {"$literal": after}}}]
                )
                output[f"{name}-{padding}-{mode}"] = _single(
                    collection, seed, update, mode=mode
                )
    return output


def multiple_events(collection, *, ordered):
    collection.delete_many({})
    collection.insert_many([{"_id": i, "v": 1} for i in (1, 2, 3)])
    stream = collection.watch(max_await_time_ms=50)
    try:
        result = collection.bulk_write(
            [
                UpdateOne({"_id": 1}, {"$set": {"v": 1.0}}),
                UpdateOne({"_id": 2}, [{"$set": {"v": 1.0}}]),
                ReplaceOne({"_id": 3}, {"_id": 3, "v": 1.0}),
            ],
            ordered=ordered,
        )
        repeated = collection.update_many({}, {"$set": {"v": 1.0}})
        collection.insert_one({"_id": _BARRIER})
        events = _drain(stream, collection)
        if not ordered:
            events.sort(key=lambda event: event["documentKey"])
        return {
            "counts": [result.matched_count, result.modified_count],
            "repeated": [repeated.matched_count, repeated.modified_count],
            "events": events,
        }
    finally:
        stream.close()


def threshold_events(collection):
    output = {}
    for padding in (0, 1, 4, 5, 6, 12, 13, 14, 15, 16, 20, 40):
        for name, update in (
            ("update", [{"$set": {"v": 1.0}}]),
            ("insert_update", [{"$set": {"v": 1.0, "new": 1}}]),
            ("delete", [{"$unset": "v"}]),
            ("omit_id", [{"$unset": "_id"}]),
        ):
            output[f"{name}-{padding}"] = _single(
                collection, {"_id": 1, "v": 1, "pad": "x" * padding}, update
            )
    output["empty_array"] = _single(
        collection,
        {"_id": 1, "v": list(range(15)), "pad": "x" * 1000},
        [{"$set": {"v": []}}],
    )
    return output


def _merge_with_events(source, target, mode):
    stream = target.watch(max_await_time_ms=50)
    try:
        try:
            for _ in range(2):
                list(
                    source.aggregate(
                        [
                            {
                                "$merge": {
                                    "into": target.name,
                                    "whenMatched": mode,
                                    "whenNotMatched": "insert",
                                }
                            }
                        ]
                    )
                )
            result = {"ok": True}
        except Exception as exc:
            result = {"error": type(exc).__name__, "code": getattr(exc, "code", None)}
        target.insert_one({"_id": _BARRIER})
        return {
            "result": result,
            "events": _drain(stream, target),
            "state": [
                BSON.encode(row).hex()
                for row in target.find({"_id": {"$ne": _BARRIER}})
            ],
        }
    finally:
        stream.close()


def merge_events(collection):
    output = {}
    source = collection.database["event_source"]
    for mode in ("merge", "replace"):
        for change_id in (False, True):
            collection.delete_many({})
            source.delete_many({})
            collection.insert_one({"_id": 1, "v": 1, "pad": "x" * 1000})
            source.insert_one({"_id": 1.0 if change_id else 1, "v": 1.0})
            output[f"{mode}-{change_id}"] = _merge_with_events(source, collection, mode)
    target = collection.database["event_conflict"]
    target.delete_many({})
    target.create_index("email", unique=True)
    target.insert_one({"_id": 2, "email": "same"})
    source.delete_many({})
    source.insert_one({"_id": 1, "email": "same"})
    output["secondary_unique_conflict"] = _merge_with_events(source, target, "fail")
    return output


def transaction_events(collection, *, commit):
    collection.delete_many({})
    collection.insert_many([{"_id": i, "v": 1, "pad": "x" * 1000} for i in (1, 2, 3)])
    stream = collection.watch(max_await_time_ms=50)
    # The local sync API does not expose Database.client. This is only a
    # fixture/session handle adapter; writes and observations use public APIs.
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    session = client.start_session()
    try:
        session.start_transaction()
        results = [
            collection.update_one({"_id": 1}, {"$set": {"v": 1.0}}, session=session),
            collection.update_one({"_id": 2}, [{"$set": {"v": 1.0}}], session=session),
            collection.replace_one({"_id": 3}, {"_id": 3, "v": 1.0}, session=session),
        ]
        # This precommit observation is accompanied by a post-commit/abort
        # barrier below. A lone timed empty batch is not absence acceptance.
        precommit = stream.try_next()
        if commit:
            session.commit_transaction()
        else:
            session.abort_transaction()
        collection.insert_one({"_id": _BARRIER})
        return {
            "counts": [[r.matched_count, r.modified_count] for r in results],
            "precommit": None
            if precommit is None
            else _observe_event(precommit, collection),
            "events": _drain(stream, collection),
            "state": [
                BSON.encode(row).hex()
                for row in collection.find(
                    {"_id": {"$ne": _BARRIER}}, sort=[("_id", 1)]
                )
            ],
        }
    finally:
        if session.in_transaction:
            session.abort_transaction()
        end = getattr(session, "end_session", None) or session.close
        end()
        stream.close()


SEMANTIC_UPDATE_EVENT_CASES = (
    ("semantic_update_event_threshold", threshold_events),
    ("semantic_update_event_merge", merge_events),
    ("semantic_update_event_assignments", assignment_events),
    ("semantic_update_event_structural", structural_events),
    ("semantic_update_event_root_id", root_id_events),
    ("semantic_update_event_bulk_ordered", partial(multiple_events, ordered=True)),
    ("semantic_update_event_bulk_unordered", partial(multiple_events, ordered=False)),
    (
        "semantic_update_event_transaction_commit",
        partial(transaction_events, commit=True),
    ),
    (
        "semantic_update_event_transaction_abort",
        partial(transaction_events, commit=False),
    ),
)
