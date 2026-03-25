from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Projection


def _projection_flag(value: object, *, dialect: MongoDialect) -> int | None:
    return dialect.projection_flag(value)


def validate_projection_spec(
    projection: Projection,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Projection:
    if not isinstance(projection, dict):
        raise OperationFailure("projection must be a document specification")
    non_id_flags: list[int] = []
    for key, value in projection.items():
        if not isinstance(key, str):
            raise OperationFailure("projection field names must be strings")
        if any(segment == "$" for segment in key.split(".")):
            raise OperationFailure("positional projection is not supported")
        flag = _projection_flag(value, dialect=dialect)
        if flag is None:
            raise OperationFailure("projection values must be 0, 1, True or False")
        if key != "_id":
            non_id_flags.append(flag)
    if any(flag == 1 for flag in non_id_flags) and any(flag == 0 for flag in non_id_flags):
        raise OperationFailure("cannot mix inclusion and exclusion in projection")
    return projection


def apply_projection(
    doc: Document,
    projection: Projection | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Document:
    if not projection:
        return doc
    projection = validate_projection_spec(projection, dialect=dialect)

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
    if isinstance(source, list):
        projected_items: list[Any] = []
        for item in source:
            if isinstance(item, dict):
                projected_item: Document = {}
                _set_projection_value(projected_item, item, path)
                projected_items.append(projected_item)
            elif isinstance(item, list):
                projected_item = []
                _set_projection_value(projected_item, item, path)
                projected_items.append(projected_item)
            else:
                projected_items.append({})
        if isinstance(target, list):
            target.extend(projected_items)
        return

    if "." not in path:
        if path in source:
            target[path] = source[path]
        return

    first, rest = path.split(".", 1)
    if first in source and isinstance(source[first], dict):
        if first not in target:
            target[first] = {}
        _set_projection_value(target[first], source[first], rest)
    elif first in source and isinstance(source[first], list):
        if first not in target:
            target[first] = []
        _set_projection_value(target[first], source[first], rest)


def _delete_projection_value(target: Document, path: str) -> None:
    if isinstance(target, list):
        for item in target:
            if isinstance(item, (dict, list)):
                _delete_projection_value(item, path)
        return

    if "." not in path:
        if path in target:
            del target[path]
        return

    first, rest = path.split(".", 1)
    if first in target and isinstance(target[first], dict):
        _delete_projection_value(target[first], rest)
    elif first in target and isinstance(target[first], list):
        _delete_projection_value(target[first], rest)
