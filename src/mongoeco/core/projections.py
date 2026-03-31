from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.paths import get_document_value, set_document_value
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Projection


@dataclass(frozen=True, slots=True)
class _ProjectionOperatorSpec:
    operator: str
    value: object


@dataclass(frozen=True, slots=True)
class _ParsedProjection:
    projection: Projection
    regular_fields: dict[str, int]
    operator_fields: dict[str, _ProjectionOperatorSpec]
    include_id: bool
    is_inclusion: bool


def _projection_flag(value: object, *, dialect: MongoDialect) -> int | None:
    return dialect.policy.projection_flag(value)


def validate_projection_spec(
    projection: Projection,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Projection:
    return _parse_projection_spec(projection, dialect=dialect).projection


def apply_projection(
    doc: Document,
    projection: Projection | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Document:
    if not projection:
        return doc
    parsed = _parse_projection_spec(projection, dialect=dialect)

    if not parsed.regular_fields and not parsed.operator_fields:
        result = deepcopy(doc) if not parsed.include_id else {"_id": doc.get("_id")}
        if not parsed.include_id and "_id" in result:
            del result["_id"]
        return result

    if parsed.is_inclusion:
        result: Document = {}
        for path, value in parsed.regular_fields.items():
            if value:
                _set_projection_value(result, doc, path)
        for path, spec in parsed.operator_fields.items():
            if spec.operator == "$slice":
                _apply_slice_projection(result, doc, path, spec.value)
        # MongoDB returns $elemMatch projected fields after the other inclusions.
        for path, spec in parsed.operator_fields.items():
            if spec.operator == "$elemMatch":
                _apply_elem_match_projection(result, doc, path, spec.value, dialect=dialect)
    else:
        result = deepcopy(doc)
        for path, value in parsed.regular_fields.items():
            if not value:
                _delete_projection_value(result, path)
        for path, spec in parsed.operator_fields.items():
            if spec.operator == "$slice":
                _apply_slice_projection(result, result, path, spec.value)

    if parsed.include_id and "_id" in doc:
        result["_id"] = deepcopy(doc["_id"])
    elif not parsed.include_id and "_id" in result:
        del result["_id"]

    return result


def _parse_projection_spec(
    projection: Projection,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> _ParsedProjection:
    if not isinstance(projection, dict):
        raise OperationFailure("projection must be a document specification")
    regular_fields: dict[str, int] = {}
    operator_fields: dict[str, _ProjectionOperatorSpec] = {}
    has_regular_inclusion = False
    has_regular_exclusion = False
    has_elem_match = False
    has_slice = False
    for key, value in projection.items():
        if not isinstance(key, str):
            raise OperationFailure("projection field names must be strings")
        if any(segment == "$" for segment in key.split(".")):
            raise OperationFailure("positional projection is not supported")
        operator_spec = _parse_projection_operator_value(key, value)
        if operator_spec is not None:
            operator_fields[key] = operator_spec
            if key == "_id":
                raise OperationFailure("projection operators are not supported on _id")
            if operator_spec.operator == "$elemMatch":
                has_elem_match = True
            if operator_spec.operator == "$slice":
                has_slice = True
            continue
        flag = _projection_flag(value, dialect=dialect)
        if flag is None:
            raise OperationFailure("projection values must be 0, 1, True or False")
        if key == "_id":
            continue
        regular_fields[key] = flag
        if flag == 1:
            has_regular_inclusion = True
        else:
            has_regular_exclusion = True
    if has_regular_inclusion and has_regular_exclusion:
        raise OperationFailure("cannot mix inclusion and exclusion in projection")
    if has_elem_match and has_regular_exclusion:
        raise OperationFailure("cannot mix exclusion with $elemMatch projection")
    _validate_projection_operator_path_collisions({*regular_fields, *operator_fields})
    is_inclusion = has_regular_inclusion or has_elem_match
    if not is_inclusion and has_slice:
        is_inclusion = False
    return _ParsedProjection(
        projection=projection,
        regular_fields=regular_fields,
        operator_fields=operator_fields,
        include_id=bool(projection.get("_id", True)),
        is_inclusion=is_inclusion,
    )


def _parse_projection_operator_value(
    path: str,
    value: object,
) -> _ProjectionOperatorSpec | None:
    if not isinstance(value, dict):
        return None
    if len(value) != 1:
        raise OperationFailure("projection operator specifications must be single-key documents")
    operator, operand = next(iter(value.items()))
    if operator == "$slice":
        _validate_projection_slice_operand(operand)
        return _ProjectionOperatorSpec(operator=operator, value=operand)
    if operator == "$elemMatch":
        if not isinstance(operand, dict):
            raise OperationFailure("$elemMatch projection requires a document specification")
        return _ProjectionOperatorSpec(operator=operator, value=operand)
    raise OperationFailure(f"Unsupported projection operator: {operator} for field {path}")


def _validate_projection_slice_operand(value: object) -> None:
    if isinstance(value, int) and not isinstance(value, bool):
        return
    if isinstance(value, list) and len(value) == 2:
        skip, limit = value
        if (
            isinstance(skip, int)
            and not isinstance(skip, bool)
            and isinstance(limit, int)
            and not isinstance(limit, bool)
            and limit > 0
        ):
            return
    raise OperationFailure("$slice projection requires an integer or a [skip, limit] pair")


def _validate_projection_operator_path_collisions(paths: set[str]) -> None:
    ordered_paths = sorted(paths)
    for index, left in enumerate(ordered_paths):
        for right in ordered_paths[index + 1 :]:
            if right.startswith(left + ".") or left.startswith(right + "."):
                raise OperationFailure(f"Path collision in projection between {left!r} and {right!r}")


def _apply_slice_projection(
    target: Document,
    source: Document,
    path: str,
    operand: object,
) -> None:
    found, value = get_document_value(source, path)
    if not found:
        return
    if not isinstance(value, list):
        set_document_value(target, path, deepcopy(value))
        return
    set_document_value(target, path, _slice_projection_array(value, operand))


def _slice_projection_array(value: list[Any], operand: object) -> list[Any]:
    if isinstance(operand, int):
        if operand >= 0:
            return deepcopy(value[:operand])
        return deepcopy(value[operand:])
    skip, limit = operand
    return deepcopy(value[skip : skip + limit])


def _apply_elem_match_projection(
    target: Document,
    source: Document,
    path: str,
    operand: object,
    *,
    dialect: MongoDialect,
) -> None:
    found, value = get_document_value(source, path)
    if not found or not isinstance(value, list):
        return
    for candidate in value:
        if QueryEngine._match_elem_match_candidate(candidate, operand, dialect=dialect):
            set_document_value(target, path, [deepcopy(candidate)])
            return


def _set_projection_value(target: Document, source: Document, path: str) -> None:
    if isinstance(source, list):
        projected_items: list[Any] = []
        for item in source:
            if isinstance(item, dict):
                projected_item: Document = {}
                _set_projection_value(projected_item, item, path)
                if projected_item:
                    projected_items.append(projected_item)
            elif isinstance(item, list):
                projected_item = []
                _set_projection_value(projected_item, item, path)
                projected_items.append(projected_item)
        if isinstance(target, list):
            target.extend(projected_items)
        return

    if "." not in path:
        if path in source:
            target[path] = deepcopy(source[path])
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
