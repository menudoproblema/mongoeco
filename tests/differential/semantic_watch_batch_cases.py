"""Opening responses, buffered rows and getMore option phases against PyMongo."""

from bson import BSON, Code

from tests.differential.semantic_watch_cases import _next
from tests.differential.semantic_watch_protocol_cases import _owners


SEED = [{"_id": "initial"}]
BAD_PIPE = [{"$project": {"x": {"$bad": 1}}}]
DIVIDE = [{"$set": {"ratio": {"$divide": [1, "$fullDocument.v"]}}}]


def _error(error, phase, stream):
    return {
        "phase": phase,
        "error": type(error).__name__,
        "code": getattr(error, "code", None),
        "code_name": (getattr(error, "details", None) or {}).get("codeName"),
        "alive": None if stream is None else stream.alive,
    }


def options(collection):
    output = {}
    for scope, owner in _owners(collection):
        for name, pipeline, kwargs in (
            ("none", [], {"full_document": None, "max_await_time_ms": 0}),
            ("empty_mode", [], {"full_document": ""}),
            ("bad_mode_pipe", BAD_PIPE, {"full_document": "bad"}),
            ("bad_mode_token", [], {"full_document": "bad", "resume_after": {}}),
            ("integer_mode", [], {"full_document": 1, "resume_after": {}}),
            ("code_mode", [], {"full_document": Code("default")}),
            ("pipeline_shape", {}, {"full_document": 1}),
            ("last_scalar", [1], {"full_document": "bad"}),
            ("earlier_scalar", [1, {"$match": {}}], {"full_document": "bad"}),
            ("string_stage", ["x"], {"full_document": "bad"}),
            ("float_pipe", BAD_PIPE, {"max_await_time_ms": 1.5}),
            ("float", [], {"max_await_time_ms": 1.5}),
            ("token_pipe", BAD_PIPE, {"resume_after": {}}),
            ("negative", [], {"max_await_time_ms": -1}),
            ("bool", [], {"max_await_time_ms": True}),
            ("zero", [], {"max_await_time_ms": 0}),
        ):
            stream = None
            phase = "open"
            try:
                stream = owner.watch(pipeline, **kwargs)
                phase = "read"
                output[f"{scope}/{name}"] = {
                    "phase": phase,
                    "empty": stream.try_next() is None,
                    "alive": stream.alive,
                }
            except Exception as error:
                output[f"{scope}/{name}"] = _error(error, phase, stream)
            finally:
                if stream is not None:
                    stream.close()
    return output


def backlog(collection):
    output = {}
    for scope, owner in _owners(collection):
        for name, pipeline, kwargs in (
            ("divide", DIVIDE, {}),
            ("float_divide", DIVIDE, {"max_await_time_ms": 1.5}),
            ("token_removed", [{"$project": {"_id": 0}}], {}),
            ("negative", [], {"max_await_time_ms": -1}),
            ("bool", [], {"max_await_time_ms": True}),
        ):
            collection.delete_many({})
            keeper = owner.watch(max_await_time_ms=20)
            stream = None
            phase = "open"
            try:
                collection.insert_one({"_id": "anchor"})
                token = _next(keeper)["_id"]
                collection.insert_one({"_id": 1, "v": 0})
                stream = owner.watch(pipeline, resume_after=token, **kwargs)
                phase = "read"
                output[f"{scope}/{name}"] = {
                    "phase": phase,
                    "documentKey": stream.try_next()["documentKey"],
                }
            except Exception as error:
                output[f"{scope}/{name}"] = _error(error, phase, stream)
            finally:
                if stream is not None:
                    stream.close()
                keeper.close()
    return output


def batch_boundaries(collection):
    output = {}
    for label, count, bad, padding in (
        ("count_inside", 103, 101, 0),
        ("count_outside", 103, 102, 0),
        ("count_live", 103, 102, 0),
        ("byte_limit", 3, None, 6 * 1024 * 1024),
        ("byte_evaluation", 3, 3, 6 * 1024 * 1024),
    ):
        collection.delete_many({})
        keeper = collection.watch(max_await_time_ms=20)
        stream = None
        phase = "open"
        read = 0
        try:
            collection.insert_one({"_id": "anchor"})
            token = _next(keeper)["_id"]
            if label == "count_live":
                stream = collection.watch(DIVIDE, max_await_time_ms=0)
            collection.insert_many(
                [
                    {
                        "_id": index,
                        "v": 0 if index == bad else 1,
                        "padding": "x" * padding,
                    }
                    for index in range(1, count + 1)
                ]
            )
            kwargs = {"max_await_time_ms": -1} if label == "byte_limit" else {}
            if label != "count_live":
                stream = collection.watch(DIVIDE, resume_after=token, **kwargs)
            phase = "read"
            while read < count:
                stream.next()
                read += 1
            output[label] = {"read": read}
        except Exception as error:
            output[label] = {**_error(error, phase, stream), "read": read}
        finally:
            if stream is not None:
                stream.close()
            keeper.close()
    return output


def prefetched_lookup(collection):
    collection.delete_many({})
    keeper = collection.watch(max_await_time_ms=20)
    stream = None
    try:
        collection.insert_one({"_id": 1, "v": 1})
        token = _next(keeper)["_id"]
        collection.update_one({"_id": 1}, {"$set": {"v": 2}})
        stream = collection.watch(resume_after=token, full_document="updateLookup")
        collection.update_one({"_id": 1}, {"$set": {"v": 3}})
        event = stream.next()
        return {
            "image": event["fullDocument"]["v"],
            "change": event["updateDescription"]["updatedFields"]["v"],
        }
    finally:
        if stream is not None:
            stream.close()
        keeper.close()


def exact_batch_bytes(collection):
    output = {}
    half = 8 * 1024 * 1024
    padding = "x" * half
    for initial in (True, False):
        # Initial and getMore responses have distinct BSON array field names.
        boundary = -56 if initial else -55
        variants = [(delta, False) for delta in (boundary - 1, boundary, boundary + 1)]
        if initial:
            variants.append((boundary, True))
        for delta, filtered_tail in variants:
            collection.delete_many({})
            keeper = collection.watch([{"$project": {"_id": 1}}], max_await_time_ms=20)
            stream = None
            phase = "open"
            read_sizes = []
            try:
                collection.insert_one({"_id": 0, "v": 1, "padding": padding})
                anchor = _next(keeper)["_id"]
                # Size the actual opaque token within the expression, then
                # observe returned framing with the independent SDK encoder.
                # Token lengths/bytes never have to agree between backends.
                second_size = (
                    half + delta
                    if initial
                    else {"$subtract": [half + delta, {"$bsonSize": "$_id"}]}
                )
                target = {
                    "$cond": [{"$eq": ["$documentKey._id", 1]}, half, second_size]
                }
                overhead = {
                    "$bsonSize": {"_id": "$_id", "p": {"$literal": ""}, "q": 1.0}
                }
                pipeline = [
                    {
                        "$project": {
                            "_id": 1,
                            # ASCII gives equal code-point and byte lengths,
                            # without building a per-byte UTF-8 boundary set.
                            "p": {
                                "$substrCP": [
                                    "$fullDocument.padding",
                                    0,
                                    {"$subtract": [target, overhead]},
                                ]
                            },
                            "q": {"$divide": [1, "$fullDocument.v"]},
                        }
                    }
                ]
                if filtered_tail:
                    pipeline.insert(0, {"$match": {"fullDocument.v": {"$ne": 2}}})
                if not initial:
                    stream = collection.watch(pipeline, max_await_time_ms=0)
                for index in (1, 2):
                    collection.insert_one({"_id": index, "v": 1, "padding": padding})
                if filtered_tail:
                    collection.insert_one({"_id": 3, "v": 2, "padding": padding})
                    collection.insert_one({"_id": 4, "v": 0, "padding": padding})
                if initial:
                    stream = collection.watch(
                        pipeline, resume_after=anchor, max_await_time_ms=-1
                    )
                else:
                    # If both valid rows fit, this error discards their entire
                    # response. If row two overflows, row one remains visible.
                    collection.insert_one({"_id": 3, "v": 0, "padding": padding})
                phase = "read"
                read_sizes.extend(len(BSON.encode(stream.next())) for _ in range(3))
            except Exception as error:
                name = f"{initial}/{delta}" + (
                    "/filtered_tail" if filtered_tail else ""
                )
                output[name] = {
                    **_error(error, phase, stream),
                    "read_sizes": read_sizes,
                }
            finally:
                if stream is not None:
                    stream.close()
                keeper.close()
    return output


def initial_invalidation(collection):
    output = {}
    for name, pipeline in (
        ("visible", []),
        ("hidden", [{"$match": {"operationType": "insert"}}]),
        ("drop_only", [{"$match": {"operationType": "drop"}}]),
    ):
        collection.insert_one({"_id": name})
        keeper = collection.watch(max_await_time_ms=20)
        stream = None
        phase = "open"
        try:
            collection.insert_one({"_id": "anchor"})
            token = _next(keeper)["_id"]
            collection.drop()
            stream = collection.watch(
                pipeline, resume_after=token, max_await_time_ms=20
            )
            row = {"alive_open": stream.alive, "events": []}
            phase = "read"
            for _ in range(2):
                event = stream.try_next()
                row["events"].append(None if event is None else event["operationType"])
            row["alive_after"] = stream.alive
            output[name] = row
        except Exception as error:
            output[name] = _error(error, phase, stream)
        finally:
            if stream is not None:
                stream.close()
            keeper.close()
    return output


def resume_anchor(collection):
    output = {}
    for scope, owner in _owners(collection):
        for name, pipeline, future in (
            ("excluded_type", [{"$match": {"operationType": "update"}}], True),
            ("excluded_field", [{"$match": {"fullDocument.v": 1}}], True),
            ("anchor_error", DIVIDE, False),
            ("not_yet_surpassed", [{"$match": {"operationType": "update"}}], False),
        ):
            collection.delete_many({})
            keeper = owner.watch(max_await_time_ms=20)
            stream = None
            phase = "open"
            try:
                collection.insert_one({"_id": "anchor", "v": 0})
                token = _next(keeper)["_id"]
                if future:
                    if name == "excluded_type":
                        collection.update_one({"_id": "anchor"}, {"$set": {"v": 1}})
                    else:
                        collection.insert_one({"_id": 1, "v": 1})
                stream = owner.watch(pipeline, resume_after=token, max_await_time_ms=0)
                phase = "read"
                output[f"{scope}/{name}"] = {
                    "empty": stream.try_next() is None,
                    "alive": stream.alive,
                }
            except Exception as error:
                output[f"{scope}/{name}"] = _error(error, phase, stream)
            finally:
                if stream is not None:
                    stream.close()
                keeper.close()
    return output


SEMANTIC_WATCH_BATCH_CASES = (
    ("semantic_watch_options", options),
    ("semantic_watch_backlog", backlog),
    ("semantic_watch_batch_boundaries", batch_boundaries),
    ("semantic_watch_exact_batch_bytes", exact_batch_bytes),
    ("semantic_watch_prefetched_lookup", prefetched_lookup),
    ("semantic_watch_initial_invalidation", initial_invalidation),
    ("semantic_watch_resume_anchor", resume_anchor),
)
