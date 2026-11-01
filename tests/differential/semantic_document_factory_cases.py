"""Observe document construction separately from user type decoding."""

from bson import Code, ObjectId

from tests.differential.semantic_bson_dispatch_cases import _options
from tests.support_write_parity import BSONObservation


OID = ObjectId("0123456789abcdef01234567")
SEED = [{"_id": 0}]


def _trace(  # noqa: PLR0913 - independent observable decoder controls
    collection, *, kind, registry, fail_at=None, fail_key=None, batch_size=None
):
    events = []
    objects = []

    def describe(value):
        if isinstance(value, ObservedDocument):
            return {"document": value.trace_id}
        if isinstance(value, list):
            return {"list": [describe(item) for item in value]}
        if isinstance(value, str):
            return {"str": "<namespace>" if value == collection.full_name else value}
        if type(value) in (int, float, bool, type(None)):
            return {"type": type(value).__name__, "value": value}
        return {"type": type(value).__name__}

    class ObservedDocument(dict):
        def __init__(self, *args, **kwargs):
            self.trace_id = len(objects)
            objects.append(self)
            events.append(["init", self.trace_id, len(args), list(kwargs)])
            if fail_at is not None and self.trace_id == fail_at:
                message = "document factory rejected construction"
                raise ValueError(message)
            super().__init__(*args, **kwargs)

        def __setitem__(self, key, value):
            events.append(["set", self.trace_id, key, describe(value)])
            if key == fail_key:
                message = "document factory rejected field"
                raise ValueError(message)
            super().__setitem__(key, value)

    def number(value):
        events.append(["decode", value])
        return value

    decoded = _options(
        collection,
        [(int, number)] if registry else (),
        document_class=ObservedDocument,
    )
    cursor = None
    phase = "open"
    try:
        if kind == "find_one":
            rows = [decoded.find_one({})]
        else:
            options = (
                {}
                if batch_size is None
                else {"batchSize" if kind == "aggregate" else "batch_size": batch_size}
            )
            cursor = (
                decoded.aggregate([], **options)
                if kind == "aggregate"
                else decoded.find({}, **options)
            )
            phase = "iterate"
            rows = list(cursor)
        result = {"ok": True, "rows": rows}
    except Exception as error:
        result = {
            "ok": False,
            "phase": phase,
            "exception": type(error).__name__,
            "message": str(error),
        }
    finally:
        if cursor is not None:
            cursor.close()
    return {
        **result,
        "events": events,
        "constructed_keys": [list(row) for row in objects],
    }


def document_factory_trace(collection):
    collection.delete_many({})
    collection.insert_one(
        {"_id": OID, "n": 1, "nested": {"n": 2}, "scope": Code("x", {"n": 3})}
    )
    result = {}
    for kind in ("find", "find_one", "aggregate"):
        for registry in (False, True):
            result[f"{kind}-{registry}"] = _trace(
                collection, kind=kind, registry=registry
            )
    return BSONObservation([OID]).value(result)


def document_factory_failures(collection):
    collection.delete_many({})
    collection.insert_one({"_id": OID, "n": 1})
    result = {}
    for kind in ("find", "find_one", "aggregate"):
        for registry in (False, True):
            for fail_at in range(5):
                result[f"{kind}-{registry}-{fail_at}"] = _trace(
                    collection, kind=kind, registry=registry, fail_at=fail_at
                )
    return BSONObservation([OID]).value(result)


def document_factory_responses(collection):
    result = {}
    for empty in (False, True):
        collection.delete_many({})
        if not empty:
            collection.insert_many([{"_id": OID, "n": 1}, {"_id": "second", "n": 2}])
        for kind in ("find", "aggregate"):
            for registry in (False, True):
                for size in (0, 1, 2):
                    result[f"{empty}-{kind}-{registry}-{size}"] = _trace(
                        collection, kind=kind, registry=registry, batch_size=size
                    )
    return BSONObservation([OID]).value(result)


def document_factory_setter_failures(collection):
    collection.delete_many({})
    collection.insert_one({"_id": OID, "n": 1})
    result = {}
    for kind in ("find", "find_one", "aggregate"):
        for registry in (False, True):
            for key in ("n", "firstBatch", "cursor", "ok"):
                result[f"{kind}-{registry}-{key}"] = _trace(
                    collection, kind=kind, registry=registry, fail_key=key
                )
    return BSONObservation([OID]).value(result)


SEMANTIC_DOCUMENT_FACTORY_CASES = (
    ("semantic_document_factory_trace", document_factory_trace),
    ("semantic_document_factory_failures", document_factory_failures),
    ("semantic_document_factory_responses", document_factory_responses),
    ("semantic_document_factory_setter_failures", document_factory_setter_failures),
)
