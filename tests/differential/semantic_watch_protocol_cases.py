"""Iteration, context managers and terminal reads through actual watch APIs."""

import time

from tests.differential.semantic_watch_cases import _next


SEED = [{"_id": "initial"}]


def _owners(collection):
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    return (
        ("collection", collection),
        ("database", collection.database),
        ("client", client),
    )


def _terminal(stream):
    result = {"alive": stream.alive, "try_next": stream.try_next()}
    for name, read in (("next", stream.next), ("iterator", lambda: next(iter(stream)))):
        try:
            result[name] = {"value": read()}
        except Exception as error:
            result[name] = {
                "error": type(error).__name__, "code": getattr(error, "code", None),
            }
    return result


def protocols(collection):
    output = {}
    for scope, owner in _owners(collection):
        collection.delete_many({})
        stream = owner.watch(max_await_time_ms=20)
        try:
            with stream as entered:
                identity = entered is stream and iter(stream) is stream
                collection.insert_one({"_id": 1})
                first = next(stream)
                collection.insert_one({"_id": 2})
                second = stream.next()
                output[scope] = {
                    "identity": identity,
                    "first": first["documentKey"], "second": second["documentKey"],
                    "alive_inside": stream.alive,
                }
            stream.close()
            output[scope]["after"] = _terminal(stream)
        finally:
            stream.close()
        stream = owner.watch(max_await_time_ms=20)
        try:
            try:
                with stream:
                    message = "context body failed"
                    raise ValueError(message)
            except ValueError:
                output[scope]["body_error_propagated"] = True
            output[scope]["after_body_error"] = _terminal(stream)
        finally:
            stream.close()
    return output


def failed_read(collection):
    output = {}
    for scope, owner in _owners(collection):
        for name, pipeline in (
            ("token_removed", [{"$project": {"_id": 0}}]),
            ("division", [{"$set": {"ratio": {"$divide": [1, "$fullDocument.v"]}}}]),
        ):
            collection.delete_many({})
            stream = owner.watch(pipeline, max_await_time_ms=20)
            try:
                collection.insert_one({"_id": 1, "v": 0})
                try:
                    _next(stream)
                    result = {"unexpected_event": True}
                except Exception as error:
                    result = {
                        "error": type(error).__name__,
                        "code": getattr(error, "code", None),
                    }
                result["after"] = _terminal(stream)
                output[f"{scope}/{name}"] = result
            finally:
                stream.close()
    return output


def invalidation(collection):
    output = {}
    for scope in ("collection", "database"):
        collection.insert_one({"_id": scope})
        owners = dict(_owners(collection))
        visible = owners[scope].watch(max_await_time_ms=20)
        hidden = owners[scope].watch(
            [{"$match": {"operationType": "insert"}}], max_await_time_ms=20,
        )
        try:
            if scope == "collection":
                collection.drop()
            else:
                owners["client"].drop_database(collection.database.name)
            kinds = []
            while visible.alive:
                kinds.append(_next(visible)["operationType"])
            deadline = time.monotonic() + 3
            unexpected = []
            while hidden.alive and time.monotonic() < deadline:
                event = hidden.try_next()
                if event is not None:
                    unexpected.append(event["operationType"])
            if hidden.alive:
                message = "filtered invalidate did not close the stream"
                raise AssertionError(message)
            output[scope] = {
                "visible_kinds": kinds, "unexpected_hidden": unexpected,
                "visible_after": _terminal(visible), "hidden_after": _terminal(hidden),
            }
        finally:
            visible.close()
            hidden.close()
    return output


SEMANTIC_WATCH_PROTOCOL_CASES = (
    ("semantic_watch_protocols", protocols),
    ("semantic_watch_failed_read", failed_read),
    ("semantic_watch_invalidation", invalidation),
)
