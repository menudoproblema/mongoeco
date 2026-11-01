"""Committed write events survive client-side response decoding failures."""

# ruff: noqa: B023 - every write and its decoding finish before the route advances

from bson import BSON

from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_update_event_cases import _BARRIER, _drain


SEED = [{"_id": 1, "v": 1}]


def write_factory_events(collection):
    output = {}
    for route in ("update", "replace", "delete", "rejected"):
        for registry in (False, True):
            collection.delete_many({})
            collection.insert_many(SEED)

            class RejectValue(dict):
                def __setitem__(self, key, value):
                    if key == ("errmsg" if route == "rejected" else "v"):
                        message = "write event response rejected"
                        raise ValueError(message)
                    super().__setitem__(key, value)

            selected = _options(
                collection,
                [(int, lambda value: value)] if registry else (),
                document_class=RejectValue,
            )
            stream = collection.watch(max_await_time_ms=50)
            try:
                try:
                    if route == "update":
                        selected.find_one_and_update({"_id": 1}, {"$set": {"v": 3}})
                    elif route == "replace":
                        selected.find_one_and_replace({"_id": 1}, {"v": 3})
                    elif route == "delete":
                        selected.find_one_and_delete({"_id": 1})
                    else:
                        selected.find_one_and_update({"_id": 1}, {"$set": {"_id": 9}})
                    result = {"ok": True}
                except Exception as error:
                    result = {
                        "ok": False,
                        "exception": type(error).__name__,
                        "code": getattr(error, "code", None),
                    }
                collection.insert_one({"_id": _BARRIER})
                events = _drain(stream, collection)
                state = [
                    BSON.encode(row).hex()
                    for row in collection.find({"_id": {"$ne": _BARRIER}})
                ]
                output[f"{route}/{registry}"] = {
                    "result": result,
                    "events": events,
                    "state": state,
                }
            finally:
                stream.close()
    return output


SEMANTIC_WRITE_FACTORY_EVENT_CASES = (
    ("semantic_write_factory_events", write_factory_events),
)
