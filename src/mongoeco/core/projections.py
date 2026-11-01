from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.compat.catalog import SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS
from mongoeco.core.aggregation.extensions import (
    get_registered_aggregation_expression_operator,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.paths import (
    get_document_value,
    set_document_value,
    validate_projection_paths,
)
from mongoeco.core.runtime_metadata import RuntimeDocumentState
from mongoeco.core.search import (
    SEARCH_HIGHLIGHTS_METADATA_FIELD,
    TEXT_SCORE_FIELD,
    VECTOR_SEARCH_SCORE_FIELD,
)
from mongoeco.core.server_errors import server_error
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
    computed_fields: dict[str, object]
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


_SLICE_PAIR_LENGTH = 2


def _projection_flag(value: object, *, dialect: MongoDialect) -> int | None:
    return dialect.policy.projection_flag(value)


def validate_projection_spec(
    projection: Projection,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Projection:
    return _parse_projection_spec(projection, dialect=dialect).projection


def apply_projection(  # noqa: PLR0913 - explicit projection execution context
    doc: Document,
    projection: Projection | None,
    *,
    selector_filter: Filter | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables=None,
    collation=None,
) -> Document:
    if projection is None:
        return doc
    # Single-document callers do not retain operands beyond this call.
    parsed = _parse_projection_spec(projection, dialect=dialect, variables=variables)
    return _apply_parsed_projection(
        doc,
        parsed,
        _projection_inclusion_root(parsed),
        selector_filter=selector_filter,
        dialect=dialect,
        variables=variables,
        collation=collation,
    )


class _ProjectionExecutor:
    """One prepared projection, validated independently of matching rows."""

    __slots__ = (
        "_collation",
        "_dialect",
        "_parsed",
        "_root",
        "_selector",
        "_variables",
    )

    def __init__(
        self,
        projection,
        *,
        selector_filter=None,
        dialect=MONGODB_DIALECT_70,
        variables=None,
        collation=None,
    ):
        self._dialect = dialect
        self._variables = variables
        self._collation = collation
        self._parsed = (
            None
            if projection is None
            else _parse_projection_spec(
                deepcopy(projection), dialect=dialect, variables=variables
            )
        )
        self._root = (
            None if self._parsed is None else _projection_inclusion_root(self._parsed)
        )
        self._selector = (
            deepcopy(selector_filter)
            if self._parsed is not None
            and any(
                spec.operator == "$positional"
                for spec in self._parsed.operator_fields.values()
            )
            else None
        )

    def __call__(self, doc, *, variables=None, collation=None):
        if self._parsed is None:
            return doc
        return _apply_parsed_projection(
            doc,
            self._parsed,
            self._root,
            selector_filter=self._selector,
            dialect=self._dialect,
            variables=self._variables if variables is None else variables,
            collation=self._collation if collation is None else collation,
        )


def _projection_inclusion_root(
    parsed: _ParsedProjection,
) -> _ProjectionPathNode | None:
    if not parsed.is_inclusion:
        return None
    return _compile_inclusion_paths(
        tuple(path for path, value in parsed.regular_fields.items() if value)
    )


def _apply_parsed_projection(  # noqa: PLR0913 - explicit projection execution context
    doc: Document,
    parsed: _ParsedProjection,
    inclusion_root: _ProjectionPathNode | None,
    *,
    selector_filter: Filter | None,
    dialect: MongoDialect,
    variables=None,
    collation=None,
) -> Document:
    if (
        not parsed.regular_fields
        and not parsed.operator_fields
        and not parsed.computed_fields
    ):
        return _apply_id_only_projection(doc, parsed)

    if parsed.is_inclusion:
        result = _apply_inclusion_projection(
            doc,
            parsed,
            inclusion_root,
            selector_filter=selector_filter,
            dialect=dialect,
        )
        apply_computed_projection_fields(
            result,
            doc,
            parsed.computed_fields,
            variables,
            dialect=dialect,
            collation=collation,
        )
    else:
        result = deepcopy(doc)
        for path, value in parsed.regular_fields.items():
            if not value:
                _delete_projection_value(result, path)
        for path, spec in parsed.operator_fields.items():
            if spec.operator == "$slice":
                _apply_slice_projection(result, result, path, spec.value)

    if "_id" in parsed.computed_fields:
        return result
    if parsed.include_id and "_id" in doc:
        document_id = deepcopy(doc["_id"])
        if parsed.is_inclusion:
            result = {"_id": document_id, **result}
        else:
            result["_id"] = document_id
    elif not parsed.include_id and "_id" in result:
        del result["_id"]

    return result


def _apply_id_only_projection(doc: Document, parsed: _ParsedProjection) -> Document:
    if not parsed.projection:
        return deepcopy(doc)
    if parsed.include_id:
        return {"_id": deepcopy(doc["_id"])} if "_id" in doc else {}
    result = deepcopy(doc)
    result.pop("_id", None)
    return result


def _apply_inclusion_projection(
    doc: Document,
    parsed: _ParsedProjection,
    inclusion_root: _ProjectionPathNode | None,
    *,
    selector_filter: Filter | None,
    dialect: MongoDialect,
) -> Document:
    result = _apply_inclusion_tree(doc, inclusion_root)
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
    return result


def expand_projection_spec(projection):
    result = {}

    def visit(path, value):
        if isinstance(value, dict) and (
            not value or not next(iter(value)).startswith("$")
        ):
            if not value:
                message = f"Invalid empty sub-projection: {path}"
                raise server_error(message, code=51270)
            for child, operand in value.items():
                visit(path + "." + child, operand)
        else:
            if path in result:
                message = f"Path collision at {path}"
                raise server_error(message, code=31250)
            result[path] = value

    for path, value in projection.items():
        visit(path, value)
    return result


def _set_computed_projection_field(target, source, path, value, *, remove=False):
    head, separator, tail = path.partition(".")
    if not separator:
        if remove:
            target.pop(head, None)
        else:
            target[head] = deepcopy(value)
        return
    original = source.get(head) if isinstance(source, dict) else None
    if isinstance(original, list):
        current = target.get(head)
        if not isinstance(current, list) or len(current) != len(original):
            current = [{} for _ in original]

        def apply_member(target_member, source_member):
            if isinstance(source_member, list):
                previous = target_member if isinstance(target_member, list) else []
                return [
                    apply_member(
                        previous[index] if index < len(previous) else {}, child
                    )
                    for index, child in enumerate(source_member)
                ]
            if not isinstance(target_member, dict):
                target_member = {}
            _set_computed_projection_field(
                target_member, source_member, tail, value, remove=remove
            )
            return target_member

        for index, member in enumerate(original):
            current[index] = apply_member(current[index], member)
        target[head] = current
    else:
        nested = target.get(head)
        if not isinstance(nested, dict):
            nested = {}
            target[head] = nested
        _set_computed_projection_field(nested, original, tail, value, remove=remove)


def apply_computed_projection_fields(  # noqa: PLR0913 - explicit projection execution context
    target, source, expressions, variables, *, dialect, collation=None
):
    from mongoeco.core.aggregation.expression_values import (  # noqa: PLC0415 - aggregation package initialization
        MISSING,
    )
    from mongoeco.core.aggregation.runtime import (  # noqa: PLC0415 - projection/runtime dependency cycle
        evaluate_expression,
    )

    document = (
        source.public_document() if isinstance(source, RuntimeDocumentState) else source
    )
    for path, expression in expressions.items():
        value = evaluate_expression(
            source, expression, variables, dialect=dialect, collation=collation
        )
        _set_computed_projection_field(
            target, document, path, value, remove=value is MISSING
        )


def _parse_projection_spec(
    projection: Projection,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables=None,
) -> _ParsedProjection:
    if not isinstance(projection, dict):
        raise OperationFailure("projection must be a document specification")
    projection = expand_projection_spec(projection)
    computed_fields = {}
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
            from mongoeco.core.aggregation.preparation import (  # noqa: PLC0415 - preparation uses projection grammar
                prepare_expression,
            )

            if has_regular_exclusion:
                code = 31252 if isinstance(value, dict) else 31310
                raise server_error(
                    "Cannot use an expression in exclusion projection", code=code
                )
            prepare_expression(
                value,
                dialect=dialect,
                variables=() if variables is None else variables,
                fold_constants=True,
            )
            computed_fields[key] = value
            continue
        if key == "_id":
            continue
        regular_fields[key] = flag
        if flag == 1:
            has_regular_inclusion = True
        else:
            if computed_fields:
                raise server_error(
                    "Cannot exclude fields in inclusion projection", code=31254
                )
            has_regular_exclusion = True
    if has_regular_inclusion and has_regular_exclusion:
        message = "cannot mix inclusion and exclusion in projection"
        raise server_error(
            message,
            code=31254,
        )
    if has_elem_match and has_regular_exclusion:
        raise OperationFailure(
            "cannot mix exclusion with $elemMatch projection",
        )
    if has_meta and has_regular_exclusion:
        raise OperationFailure("cannot mix exclusion with $meta projection")
    if has_positional and has_regular_exclusion:
        message = "cannot mix exclusion with positional projection"
        raise OperationFailure(message)
    if (
        has_positional
        and sum(
            1 for spec in operator_fields.values() if spec.operator == "$positional"
        )
        > 1
    ):
        message = "only one positional projection is supported"
        raise server_error(message, code=31276)
    validate_projection_paths(
        {*regular_fields, *operator_fields, *computed_fields},
    )
    is_inclusion = (
        has_regular_inclusion
        or has_elem_match
        or has_positional
        or has_meta
        or bool(computed_fields)
    )
    if not is_inclusion and has_slice:
        is_inclusion = False
    return _ParsedProjection(
        projection=projection,
        regular_fields=regular_fields,
        operator_fields=operator_fields,
        computed_fields=computed_fields,
        include_id=_projection_flag(projection.get("_id", True), dialect=dialect) != 0,
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
            message = "$elemMatch projection requires a document specification"
            raise server_error(
                message,
                code=31274,
            )
        return _ProjectionOperatorSpec(operator=operator, value=operand)
    if operator == "$meta":
        if operand not in {
            "searchHighlights",
            "textScore",
            "vectorSearchScore",
        }:
            message = (
                "$meta projection only supports searchHighlights, textScore "
                "or vectorSearchScore"
            )
            raise server_error(
                message,
                code=17308,
            )
        return _ProjectionOperatorSpec(operator=operator, value=operand)

    if (
        operator in SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS
        or get_registered_aggregation_expression_operator(operator) is not None
    ):
        return None
    message = f"Unsupported projection operator: {operator} for field {path}"
    raise server_error(
        message,
        code=31325,
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
    if isinstance(value, list) and len(value) == _SLICE_PAIR_LENGTH:
        skip, limit = value
        if (
            isinstance(skip, int)
            and not isinstance(skip, bool)
            and isinstance(limit, int)
            and not isinstance(limit, bool)
            and limit > 0
        ):
            return
    message = "$slice projection requires an integer or a [skip, limit] pair"
    raise server_error(
        message,
        code=28724
        if isinstance(value, list) and len(value) == _SLICE_PAIR_LENGTH
        else 28667,
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
        message = (
            "$meta projection only supports searchHighlights, textScore "
            "or vectorSearchScore"
        )
        raise server_error(
            message,
            code=17308,
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


def _compile_inclusion_paths(
    paths: tuple[str, ...],
) -> _ProjectionPathNode:
    root = _ProjectionPathNode()
    for path in paths:
        node = root
        for segment in path.split("."):
            node = node.child(segment)
        node.include_value = True
    return root


def _apply_inclusion_tree(
    source: Document,
    root: _ProjectionPathNode | None,
) -> Document:
    if root is None:
        return {}
    if isinstance(source, RuntimeDocumentState):
        source = source.public_document()
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
    return True, projected_document


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
