from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.paths import get_document_value, set_document_value
from mongoeco.core.search import (
    SEARCH_HIGHLIGHTS_METADATA_FIELD,
    TEXT_SCORE_FIELD,
    VECTOR_SEARCH_SCORE_FIELD,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter, Projection


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


@dataclass(slots=True)
class _ProjectionPathNode:
    include_value: bool = False
    children: dict[str, "_ProjectionPathNode"] | None = None

    def child(self, segment: str) -> "_ProjectionPathNode":
        if self.children is None:
            self.children = {}
        return self.children.setdefault(segment, _ProjectionPathNode())


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
    selector_filter: Filter | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Document:
    if projection is None:
        return doc
    parsed = _parse_projection_spec(projection, dialect=dialect)

    if not parsed.regular_fields and not parsed.operator_fields:
        if projection:
            result = (
                {"_id": deepcopy(doc["_id"])}
                if parsed.include_id and "_id" in doc
                else {}
            )
            if not parsed.include_id:
                result = deepcopy(doc)
                if "_id" in result:
                    del result["_id"]
            return result
        result = deepcopy(doc)
        return result

    if parsed.is_inclusion:
        result = _apply_regular_inclusion_projection(
            doc,
            tuple(path for path, value in parsed.regular_fields.items() if value),
        )
        for path, spec in parsed.operator_fields.items():
            if spec.operator == "$slice":
                _apply_slice_projection(result, doc, path, spec.value)
            elif spec.operator == "$positional":
                _apply_positional_projection(
                    result,
                    doc,
                    path,
                    selector_filter=selector_filter,
                    dialect=dialect,
                )
            elif spec.operator == "$meta":
                _apply_meta_projection(result, doc, path, spec.value)
        # MongoDB returns $elemMatch projected fields after the other inclusions.
        for path, spec in parsed.operator_fields.items():
            if spec.operator == "$elemMatch":
                _apply_elem_match_projection(
                    result,
                    doc,
                    path,
                    spec.value,
                    dialect=dialect,
                )
    else:
        result = deepcopy(doc)
        for path, value in parsed.regular_fields.items():
            if not value:
                _delete_projection_value(result, path)
        for path, spec in parsed.operator_fields.items():
            if spec.operator == "$slice":
                _apply_slice_projection(result, result, path, spec.value)

    if parsed.include_id and "_id" in doc:
        document_id = deepcopy(doc["_id"])
        if parsed.is_inclusion:
            result = {"_id": document_id, **result}
        else:
            result["_id"] = document_id
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
    has_meta = False
    has_positional = False
    has_slice = False
    for key, value in projection.items():
        if not isinstance(key, str):
            raise OperationFailure("projection field names must be strings")
        positional_path = _parse_positional_projection_path(key)
        if positional_path is not None:
            flag = _projection_flag(value, dialect=dialect)
            if flag != 1:
                raise OperationFailure(
                    "positional projection requires an inclusion flag",
                )
            operator_fields[positional_path] = _ProjectionOperatorSpec(
                operator="$positional",
                value=True,
            )
            has_positional = True
            continue
        operator_spec = _parse_projection_operator_value(key, value)
        if operator_spec is not None:
            operator_fields[key] = operator_spec
            if key == "_id":
                raise OperationFailure(
                    "projection operators are not supported on _id",
                )
            if operator_spec.operator == "$elemMatch":
                has_elem_match = True
            if operator_spec.operator == "$slice":
                has_slice = True
            if operator_spec.operator == "$meta":
                has_meta = True
            continue
        flag = _projection_flag(value, dialect=dialect)
        if flag is None:
            raise OperationFailure(
                "projection values must be 0, 1, True or False",
            )
        if key == "_id":
            continue
        regular_fields[key] = flag
        if flag == 1:
            has_regular_inclusion = True
        else:
            has_regular_exclusion = True
    if has_regular_inclusion and has_regular_exclusion:
        raise OperationFailure(
            "cannot mix inclusion and exclusion in projection",
        )
    if has_elem_match and has_regular_exclusion:
        raise OperationFailure(
            "cannot mix exclusion with $elemMatch projection",
        )
    if has_meta and has_regular_exclusion:
        raise OperationFailure("cannot mix exclusion with $meta projection")
    if has_positional and has_regular_exclusion:
        raise OperationFailure(
            "cannot mix exclusion with positional projection",
        )
    if (
        has_positional
        and sum(
            1 for spec in operator_fields.values() if spec.operator == "$positional"
        )
        > 1
    ):
        raise OperationFailure("only one positional projection is supported")
    _validate_projection_operator_path_collisions(
        {*regular_fields, *operator_fields},
    )
    is_inclusion = has_regular_inclusion or has_elem_match or has_positional or has_meta
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
        raise OperationFailure(
            "projection operator specifications must be single-key documents",
        )
    operator, operand = next(iter(value.items()))
    if operator == "$slice":
        _validate_projection_slice_operand(operand)
        return _ProjectionOperatorSpec(operator=operator, value=operand)
    if operator == "$elemMatch":
        if not isinstance(operand, dict):
            raise OperationFailure(
                "$elemMatch projection requires a document specification",
            )
        return _ProjectionOperatorSpec(operator=operator, value=operand)
    if operator == "$meta":
        if operand not in {
            "searchHighlights",
            "textScore",
            "vectorSearchScore",
        }:
            raise OperationFailure(
                "$meta projection only supports searchHighlights, textScore "
                "or vectorSearchScore",
            )
        return _ProjectionOperatorSpec(operator=operator, value=operand)
    raise OperationFailure(
        f"Unsupported projection operator: {operator} for field {path}",
    )


def _parse_positional_projection_path(path: str) -> str | None:
    segments = path.split(".")
    positional_indexes = [
        index for index, segment in enumerate(segments) if segment == "$"
    ]
    if not positional_indexes:
        return None
    if len(positional_indexes) != 1 or positional_indexes[0] != len(segments) - 1:
        raise OperationFailure(
            "positional projection requires a single terminal '$' segment",
        )
    if len(segments) < 2:
        raise OperationFailure(
            "positional projection requires an array field path",
        )
    return ".".join(segments[:-1])


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
    raise OperationFailure(
        "$slice projection requires an integer or a [skip, limit] pair",
    )


def _validate_projection_operator_path_collisions(paths: set[str]) -> None:
    ordered_paths = sorted(paths)
    for index, left in enumerate(ordered_paths):
        for right in ordered_paths[index + 1 :]:
            if right.startswith(left + ".") or left.startswith(right + "."):
                raise OperationFailure(
                    f"Path collision in projection between {left!r} and {right!r}",
                )


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
        if QueryEngine._match_elem_match_candidate(
            candidate,
            operand,
            dialect=dialect,
        ):
            set_document_value(target, path, [deepcopy(candidate)])
            return


def _apply_meta_projection(
    target: Document,
    source: Document,
    path: str,
    operand: object,
) -> None:
    if operand == "textScore":
        found, value = get_document_value(source, TEXT_SCORE_FIELD)
    elif operand == "vectorSearchScore":
        found, value = get_document_value(source, VECTOR_SEARCH_SCORE_FIELD)
    elif operand == "searchHighlights":
        found, value = get_document_value(
            source,
            SEARCH_HIGHLIGHTS_METADATA_FIELD,
        )
    else:
        raise OperationFailure(
            "$meta projection only supports searchHighlights, textScore "
            "or vectorSearchScore",
        )
    set_document_value(target, path, deepcopy(value) if found else None)


def _apply_positional_projection(
    target: Document,
    source: Document,
    path: str,
    *,
    selector_filter: Filter | None,
    dialect: MongoDialect,
) -> None:
    from mongoeco.core.operators import UpdateEngine

    if selector_filter is None:
        raise OperationFailure(
            "positional projection requires a selector filter",
        )
    found, value = get_document_value(source, path)
    if not found or not isinstance(value, list):
        return
    matcher = UpdateEngine._build_legacy_positional_matcher(
        path,
        source,
        selector_filter,
        dialect=dialect,
    )
    for candidate in value:
        if matcher(candidate):
            set_document_value(target, path, [deepcopy(candidate)])
            return


def _apply_regular_inclusion_projection(
    source: Document,
    paths: tuple[str, ...],
) -> Document:
    root = _ProjectionPathNode()
    for path in paths:
        node = root
        for segment in path.split("."):
            node = node.child(segment)
        node.include_value = True

    found, projected = _project_inclusion_value(source, root)
    if found and isinstance(projected, dict):
        return projected
    return {}


def _project_inclusion_value(
    source: Any,
    node: _ProjectionPathNode,
) -> tuple[bool, Any]:
    if node.include_value:
        return True, deepcopy(source)

    if isinstance(source, list):
        projected_items: list[Any] = []
        for item in source:
            found, projected = _project_inclusion_value(item, node)
            if found:
                projected_items.append(projected)
        # An existing array remains observable even when none of its elements
        # expose one of the selected descendant paths.
        return True, projected_items

    if not isinstance(source, dict) or node.children is None:
        return False, None

    projected_document: Document = {}
    for segment, source_value in source.items():
        child = node.children.get(segment)
        if child is None:
            continue
        found, projected = _project_inclusion_value(source_value, child)
        if found:
            projected_document[segment] = projected
    return bool(projected_document), projected_document


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
    if (first in target and isinstance(target[first], dict)) or (
        first in target and isinstance(target[first], list)
    ):
        _delete_projection_value(target[first], rest)
