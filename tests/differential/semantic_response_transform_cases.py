"""Nonraising document factories at the public response boundary."""

from copy import deepcopy

from tests.differential.semantic_bson_admission_cases import _view
from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_write_factory_cases import (
    ROUTES,
    SEED as WRITE_FACTORY_SEED,
    _execute,
    _reset,
)
from tests.differential.semantic_write_response_cases import _result


SEED = WRITE_FACTORY_SEED
TRANSFORMS = {
    "v": 41,
    "ok": 0.0,
    "n": 42,
    "value": {"rewritten": True},
    "lastErrorObject": {"n": 42, "updatedExisting": True},
    "writeErrors": [],
}


def _document_factory(events, field, replacement):
    class TransformedDocument(dict):
        def __setitem__(self, key, value):
            events.append(key)
            super().__setitem__(key, deepcopy(replacement) if key == field else value)

    return TransformedDocument


def response_transformations(collection):
    output = {}
    for route in (*ROUTES, "find", "aggregate"):
        for registry in (False, True):
            for field, replacement in TRANSFORMS.items():
                _reset(collection)
                events = []

                document_class = _document_factory(events, field, replacement)

                selected = _options(
                    collection,
                    [(int, lambda value: value)] if registry else (),
                    document_class=document_class,
                )
                try:
                    result = (
                        list(selected.find().sort("_id"))
                        if route == "find"
                        else list(selected.aggregate([{"$sort": {"_id": 1}}]))
                        if route == "aggregate"
                        else _execute(selected, route)
                    )
                    observed = {
                        "ok": True,
                        "type": type(result).__name__,
                        "result": _view(result)
                        if isinstance(result, (dict, list)) or result is None
                        else _result(result),
                    }
                except Exception as error:
                    details = getattr(error, "details", None) or {}
                    observed = {
                        "ok": False,
                        "exception": type(error).__name__,
                        "code": getattr(error, "code", None),
                        "message": details.get("errmsg", str(error)),
                    }
                output[f"{route}/{registry}/{field}"] = {
                    **observed,
                    "events": events,
                    "state": _view(list(collection.find().sort("_id"))),
                }
    return output


SEMANTIC_RESPONSE_TRANSFORM_CASES = (
    ("semantic_response_transformations", response_transformations),
)
