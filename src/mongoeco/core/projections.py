from copy import deepcopy
from typing import Any

from mongoeco.types import Document, Projection


def apply_projection(doc: Document, projection: Projection | None) -> Document:
    if not projection:
        return doc

    include_id = projection.get("_id", True)
    fields = {key: value for key, value in projection.items() if key != "_id"}

    if not fields:
        result = deepcopy(doc) if not include_id else {"_id": doc.get("_id")}
        if not include_id and "_id" in result:
            del result["_id"]
        return result

    is_inclusion = any(value for value in fields.values())

    if is_inclusion:
        result: Document = {}
        for path, value in fields.items():
            if value:
                _set_projection_value(result, doc, path)
    else:
        result = deepcopy(doc)
        for path, value in fields.items():
            if not value:
                _delete_projection_value(result, path)

    if include_id and "_id" in doc:
        result["_id"] = doc["_id"]
    elif not include_id and "_id" in result:
        del result["_id"]

    return result


def _set_projection_value(target: Document, source: Document, path: str) -> None:
    if "." not in path:
        if path in source:
            target[path] = source[path]
        return

    first, rest = path.split(".", 1)
    if first in source and isinstance(source[first], dict):
        if first not in target:
            target[first] = {}
        _set_projection_value(target[first], source[first], rest)


def _delete_projection_value(target: Document, path: str) -> None:
    if "." not in path:
        if path in target:
            del target[path]
        return

    first, rest = path.split(".", 1)
    if first in target and isinstance(target[first], dict):
        _delete_projection_value(target[first], rest)
