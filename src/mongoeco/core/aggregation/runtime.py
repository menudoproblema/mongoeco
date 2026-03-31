import base64
import datetime
import decimal
import math
import re
import uuid
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
from mongoeco.core.aggregation.array_string_expressions import (
    ARRAY_STRING_EXPRESSION_OPERATORS,
    evaluate_array_string_expression,
)
from mongoeco.core.aggregation.extensions import (
    AggregationExpressionExtensionContext,
    get_registered_aggregation_expression_operator,
)
from mongoeco.core.aggregation.control_object_expressions import (
    CONTROL_OBJECT_EXPRESSION_OPERATORS,
    evaluate_control_object_expression,
)
from mongoeco.core.aggregation.date_expressions import (
    DATE_EXPRESSION_OPERATORS,
    evaluate_date_expression,
)
from mongoeco.core.aggregation.numeric_expressions import (
    NUMERIC_EXPRESSION_OPERATORS,
    _require_numeric,
    evaluate_numeric_expression,
)
from mongoeco.core.aggregation.scalar_expressions import (
    SCALAR_EXPRESSION_OPERATORS,
    evaluate_scalar_expression,
)
from mongoeco.core.bson_scalars import (
    bson_numeric_alias,
)
from mongoeco.core.filtering import BSONComparator
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.paths import delete_document_value, get_document_value, set_document_value
from mongoeco.core.projections import apply_projection, validate_projection_spec
from mongoeco.core.query_plan import compile_filter
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, DBRef, Decimal128, Document, ObjectId, Regex, Timestamp, UndefinedType
from mongoeco.core.aggregation.accumulators import _evaluate_pick_n_input
from mongoeco.core.aggregation.planning import Pipeline, _require_sort
from mongoeco.core.aggregation.spill import AggregationSpillPolicy


type AggregationStageHandler = Callable[[list[Document], object, "AggregationStageContext"], list[Document]]

_CURRENT_COLLECTION_RESOLVER_KEY = "__mongoeco_current_collection__"


def _aggregation_key(value: Any) -> Any:
    if value is None:
        return ("none", None)
    if isinstance(value, bool):
        return ("bool", value)
    numeric_alias = bson_numeric_alias(value)
    if numeric_alias == "int":
        return ("int", value)
    if numeric_alias == "long":
        return ("long", value)
    if numeric_alias == "double":
        return ("float", value)
    if numeric_alias == "decimal":
        return ("decimal", value)
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, (bytes, Binary)):
        return ("bytes", value)
    if isinstance(value, uuid.UUID):
        return ("uuid", value)
    if isinstance(value, ObjectId):
        return ("objectid", value)
    if isinstance(value, datetime.datetime):
        return ("datetime", value)
    if isinstance(value, Timestamp):
        return ("timestamp", value)
    if isinstance(value, dict):
        return ("dict", tuple((key, _aggregation_key(item)) for key, item in value.items()))
    if isinstance(value, list):
        return ("list", tuple(_aggregation_key(item) for item in value))
    try:
        hash(value)
        return (value.__class__, value)
    except TypeError:
        return ("repr", repr(value))


@dataclass(frozen=True, slots=True)
class AggregationStageContext:
    stage_index: int
    collection_resolver: Callable[[str], list[Document]] | None = None
    variables: dict[str, Any] | None = None
    dialect: MongoDialect = MONGODB_DIALECT_70
    collation: CollationSpec | None = None
    spill_policy: AggregationSpillPolicy | None = None


def _require_unwind_spec(spec: object) -> tuple[str, bool, str | None]:
    if isinstance(spec, str):
        path = spec
        preserve = False
        include_array_index = None
    elif isinstance(spec, dict):
        path = spec.get("path")
        preserve = bool(spec.get("preserveNullAndEmptyArrays", False))
        include_array_index = spec.get("includeArrayIndex")
        if include_array_index is not None and not isinstance(include_array_index, str):
            raise OperationFailure("$unwind includeArrayIndex must be a string")
    else:
        raise OperationFailure("$unwind requires a path string or document")

    if not isinstance(path, str) or not path.startswith("$") or path == "$":
        raise OperationFailure("$unwind path must be a string starting with '$'")

    if include_array_index is not None and include_array_index.startswith("$"):
        raise OperationFailure("$unwind includeArrayIndex must be a field name, not a path expression")

    return path[1:], preserve, include_array_index


def _require_lookup_spec(spec: object) -> dict[str, Any]:
    if not isinstance(spec, dict):
        raise OperationFailure("$lookup requires a document specification")
    if "from" not in spec or "as" not in spec:
        raise OperationFailure("$lookup requires from and as")
    from_collection = spec["from"]
    output_field = spec["as"]
    if not all(isinstance(value, str) and value for value in (from_collection, output_field)):
        raise OperationFailure("$lookup from and as must be non-empty strings")
    if from_collection.startswith("$"):
        raise OperationFailure("$lookup from must be a collection name, not a path expression")
    if output_field.startswith("$"):
        raise OperationFailure("$lookup as must be a field name, not a path expression")

    if "pipeline" in spec:
        let_spec = spec.get("let", {})
        if not isinstance(let_spec, dict):
            raise OperationFailure("$lookup let must be a document")
        lookup = {
            "from": from_collection,
            "as": output_field,
            "pipeline": _require_pipeline_spec("$lookup", spec["pipeline"]),
            "let": let_spec,
        }
        has_local = "localField" in spec
        has_foreign = "foreignField" in spec
        if has_local != has_foreign:
            raise OperationFailure("$lookup localField and foreignField must be provided together")
        if has_local:
            local_field = spec["localField"]
            foreign_field = spec["foreignField"]
            if not all(isinstance(value, str) and value for value in (local_field, foreign_field)):
                raise OperationFailure("$lookup fields must be non-empty strings")
            if local_field.startswith("$") or foreign_field.startswith("$"):
                raise OperationFailure("$lookup localField and foreignField must be field paths, not path expressions")
            lookup["localField"] = local_field
            lookup["foreignField"] = foreign_field
        return lookup

    required = {"from", "localField", "foreignField", "as"}
    if not required <= set(spec):
        raise OperationFailure("$lookup requires from, localField, foreignField and as")
    if "let" in spec:
        raise OperationFailure("$lookup let requires pipeline form")
    local_field = spec["localField"]
    foreign_field = spec["foreignField"]
    if not all(isinstance(value, str) and value for value in (local_field, foreign_field)):
        raise OperationFailure("$lookup fields must be non-empty strings")
    if local_field.startswith("$") or foreign_field.startswith("$"):
        raise OperationFailure("$lookup localField and foreignField must be field paths, not path expressions")
    return {
        "from": from_collection,
        "as": output_field,
        "localField": local_field,
        "foreignField": foreign_field,
    }


def _require_union_with_spec(spec: object) -> dict[str, Any]:
    if isinstance(spec, str):
        if not spec:
            raise OperationFailure("$unionWith collection name must be a non-empty string")
        return {"coll": spec, "pipeline": []}
    if not isinstance(spec, dict):
        raise OperationFailure("$unionWith requires a collection name string or a document specification")
    pipeline = spec.get("pipeline", [])
    if "pipeline" in spec:
        pipeline = _require_pipeline_spec("$unionWith", pipeline)
    coll = spec.get("coll")
    if coll is not None:
        if not isinstance(coll, str) or not coll:
            raise OperationFailure("$unionWith coll must be a non-empty string")
        if coll.startswith("$"):
            raise OperationFailure("$unionWith coll must be a collection name, not a path expression")
    elif "pipeline" not in spec:
        raise OperationFailure("$unionWith requires coll or pipeline")
    if set(spec) - {"coll", "pipeline"}:
        raise OperationFailure("$unionWith only supports coll and pipeline")
    return {"coll": coll, "pipeline": pipeline}


def _require_pipeline_spec(operator: str, spec: object) -> Pipeline:
    if not isinstance(spec, list):
        raise OperationFailure(f"{operator} requires a pipeline list")
    return spec


def _apply_unwind(documents: list[Document], spec: object) -> list[Document]:
    path, preserve, include_array_index = _require_unwind_spec(spec)
    result: list[Document] = []
    for document in documents:
        found, value = get_document_value(document, path)
        if not found or value is None:
            if preserve:
                preserved = deepcopy(document)
                if include_array_index is not None:
                    preserved[include_array_index] = None
                result.append(preserved)
            continue

        if isinstance(value, list):
            if not value:
                if preserve:
                    preserved = deepcopy(document)
                    if include_array_index is not None:
                        preserved[include_array_index] = None
                    result.append(preserved)
                continue
            for index, item in enumerate(value):
                unwound = deepcopy(document)
                set_document_value(unwound, path, item)
                if include_array_index is not None:
                    unwound[include_array_index] = index
                result.append(unwound)
            continue

        unwound = deepcopy(document)
        if include_array_index is not None:
            unwound[include_array_index] = None
        result.append(unwound)
    return result


def _lookup_matches(
    local_values: list[Any],
    foreign_values: list[Any],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    left = local_values or [None]
    right = foreign_values or [None]
    return any(
        QueryEngine._query_equality_matches(
            local_value,
            foreign_value,
            null_matches_undefined=dialect.policy.null_query_matches_undefined(),
            dialect=dialect,
            collation=collation,
        )
        for local_value in left
        for foreign_value in right
    )


def _require_expression_args(operator: str, spec: object, *, min_args: int, max_args: int | None = None) -> list[object]:
    if not isinstance(spec, list):
        raise OperationFailure(f"{operator} requires a list expression")
    if len(spec) < min_args:
        raise OperationFailure(f"{operator} requires at least {min_args} arguments")
    if max_args is not None and len(spec) > max_args:
        raise OperationFailure(f"{operator} accepts at most {max_args} arguments")
    return spec


def _compare_values(
    left: Any,
    right: Any,
    operator: str,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> bool:
    if operator == "$eq":
        return QueryEngine._values_equal(left, right, dialect=dialect)
    if operator == "$ne":
        return not QueryEngine._values_equal(left, right, dialect=dialect)
    comparison = dialect.policy.compare_values(left, right)
    return {
        "$gt": comparison > 0,
        "$gte": comparison >= 0,
        "$lt": comparison < 0,
        "$lte": comparison <= 0,
    }[operator]


def _require_array(operator: str, value: object) -> list[Any]:
    if not isinstance(value, list):
        raise OperationFailure(f"{operator} requires array arguments")
    return value

def _expression_truthy(value: Any, *, dialect: MongoDialect) -> bool:
    return dialect.policy.expression_truthy(value)


def _append_unique_values(
    target: list[Any],
    values: list[Any],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> None:
    for value in values:
        if any(
            QueryEngine._values_equal(value, existing, dialect=dialect)
            for existing in target
        ):
            continue
        target.append(deepcopy(value))


def _resolve_variable_expression(expression: str, variables: dict[str, Any]) -> Any:
    name_and_path = expression[2:]
    name, _, path = name_and_path.partition(".")
    if name == "REMOVE":
        return _REMOVE if not path else None
    value = variables.get(name)
    if not path:
        return value
    if value is None:
        return None
    resolved = _resolve_aggregation_field_path(value, path)
    return None if resolved is _MISSING else resolved


_MISSING = object()
_REMOVE = object()


def _resolve_aggregation_field_path(value: Any, path: str) -> Any:
    if not path:
        return value

    if isinstance(value, DBRef):
        return _resolve_aggregation_field_path(value.as_document(), path)

    if isinstance(value, list):
        head, _, tail = path.partition(".")
        if head.startswith("-") and head[1:].isdigit():
            return _MISSING
        if head.isdigit():
            index = int(head)
            if index >= len(value):
                return _MISSING
            next_value = value[index]
            return _resolve_aggregation_field_path(next_value, tail)

        resolved_items: list[Any] = []
        for item in value:
            resolved = _resolve_aggregation_field_path(item, path)
            if resolved is _MISSING:
                continue
            resolved_items.append(resolved)
        return resolved_items if resolved_items else _MISSING

    if not isinstance(value, dict):
        return _MISSING

    head, _, tail = path.partition(".")
    if head not in value:
        return _MISSING
    return _resolve_aggregation_field_path(value[head], tail)


def _mongo_mod(left: int | float, right: int | float) -> int | float:
    if not math.isfinite(left) or not math.isfinite(right):
        return math.nan
    quotient = int(left / right)
    return left - right * quotient


def _stringify_aggregation_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal128):
        return str(value)
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, datetime.datetime):
        normalized = value.astimezone(datetime.UTC) if value.tzinfo is not None else value
        return normalized.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(value, (bytes, bytearray, Binary)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, Regex):
        return f"/{value.pattern}/{value.flags}"
    if isinstance(value, Timestamp):
        return f"Timestamp({value.time}, {value.inc})"
    return str(value)


def _normalize_numeric_place(operator: str, place: Any) -> int:
    if not isinstance(place, int) or isinstance(place, bool):
        raise OperationFailure(f"{operator} place must be an integer")
    if place < -20 or place > 100:
        raise OperationFailure(f"{operator} place must be between -20 and 100")
    return place


def _round_numeric(value: int | float, place: int) -> int | float:
    if isinstance(value, int) and place >= 0:
        return value
    quantizer = decimal.Decimal(f"1e{-place}")
    rounded = decimal.Decimal(str(value)).quantize(quantizer, rounding=decimal.ROUND_HALF_EVEN)
    return int(rounded) if place <= 0 else float(rounded)


def _trunc_numeric(value: int | float, place: int) -> int | float:
    if isinstance(value, int) and place >= 0:
        return value
    factor = 10 ** place if place >= 0 else 10 ** (-place)
    if place >= 0:
        truncated = math.trunc(value * factor) / factor
        return int(truncated) if place == 0 else truncated
    truncated = math.trunc(value / factor) * factor
    return int(truncated)

def _bson_cstring_size(value: str) -> int:
    encoded = value.encode("utf-8")
    if b"\x00" in encoded:
        raise OperationFailure("$bsonSize cannot encode strings containing NUL bytes")
    return len(encoded) + 1


def _bson_string_size(value: str) -> int:
    encoded = value.encode("utf-8")
    return 4 + len(encoded) + 1


def _bson_value_size(value: Any) -> int:
    if isinstance(value, float):
        return 8
    if isinstance(value, str):
        return _bson_string_size(value)
    if isinstance(value, dict):
        return _bson_document_size(value)
    if isinstance(value, list):
        return _bson_document_size({str(index): item for index, item in enumerate(value)})
    if isinstance(value, (bytes, bytearray, Binary)):
        return 4 + 1 + len(value)
    if isinstance(value, uuid.UUID):
        return 4 + 1 + 16
    if isinstance(value, ObjectId):
        return 12
    if isinstance(value, bool):
        return 1
    if isinstance(value, datetime.datetime):
        return 8
    if value is None or isinstance(value, UndefinedType):
        return 0
    if isinstance(value, re.Pattern):
        return _bson_cstring_size(value.pattern) + _bson_cstring_size("")
    if isinstance(value, Regex):
        return _bson_cstring_size(value.pattern) + _bson_cstring_size(value.flags)
    if isinstance(value, Timestamp):
        return 8
    if isinstance(value, int):
        return 4 if -(1 << 31) <= value <= (1 << 31) - 1 else 8
    raise OperationFailure(f"$bsonSize cannot encode value of type {type(value).__name__}")


def _bson_element_size(key: str, value: Any) -> int:
    return 1 + _bson_cstring_size(key) + _bson_value_size(value)


def _bson_document_size(document: dict[str, Any]) -> int:
    total = 4 + 1
    for key, value in document.items():
        total += _bson_element_size(key, value)
    return total


def _add_milliseconds(value: datetime.datetime, milliseconds: int | float) -> datetime.datetime:
    return value + datetime.timedelta(milliseconds=milliseconds)


def _subtract_values(left: Any, right: Any) -> Any:
    if isinstance(left, datetime.datetime):
        if isinstance(right, datetime.datetime):
            delta = left - right
            return int(delta.total_seconds() * 1000)
        if isinstance(right, (int, float)) and not isinstance(right, bool):
            return _add_milliseconds(left, -right)
    if isinstance(right, datetime.datetime):
        raise OperationFailure("$subtract only supports number-number, date-date, or date-number")
    return _require_numeric("$subtract", left) - _require_numeric("$subtract", right)


def _evaluate_expression_with_missing(
    document: Document,
    expression: object,
    variables: dict[str, Any],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Any:
    if isinstance(expression, str):
        if expression.startswith("$$"):
            return _resolve_variable_expression(expression, variables)
        if expression.startswith("$"):
            return _resolve_aggregation_field_path(document, expression[1:])
    return evaluate_expression(document, expression, variables, dialect=dialect)


def evaluate_expression(
    document: Document,
    expression: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Any:
    if variables is None:
        variables = {"ROOT": document, "CURRENT": document}
    else:
        variables = {**variables, "ROOT": document, "CURRENT": document}

    if isinstance(expression, str):
        if expression.startswith("$$"):
            return _resolve_variable_expression(expression, variables)
        if expression.startswith("$"):
            value = _resolve_aggregation_field_path(document, expression[1:])
            return None if value is _MISSING else value
        return expression

    if isinstance(expression, list):
        return [
            evaluate_expression(document, item, variables, dialect=dialect)
            for item in expression
        ]

    if not isinstance(expression, dict):
        return expression

    if len(expression) == 1:
        operator, spec = next(iter(expression.items()))
        if isinstance(operator, str) and operator.startswith("$"):
            extension_handler = get_registered_aggregation_expression_operator(operator)
            if extension_handler is not None:
                return extension_handler(
                    document,
                    spec,
                    variables,
                    dialect,
                    AggregationExpressionExtensionContext(
                        evaluate_expression=lambda current_document, current_expression, current_variables=None: evaluate_expression(
                            current_document,
                            current_expression,
                            current_variables,
                            dialect=dialect,
                        ),
                        evaluate_expression_with_missing=lambda current_document, current_expression, current_variables=None: _evaluate_expression_with_missing(
                            current_document,
                            current_expression,
                            current_variables,
                            dialect=dialect,
                        ),
                        require_expression_args=lambda current_operator, current_spec, current_min_args, current_max_args=None: _require_expression_args(
                            current_operator,
                            current_spec,
                            min_args=current_min_args,
                            max_args=current_max_args,
                        ),
                        missing_sentinel=_MISSING,
                    ),
                )
            if not dialect.supports_aggregation_expression_operator(operator):
                raise OperationFailure(f"Unsupported aggregation expression: {operator}")
            if operator == "$literal":
                return deepcopy(spec)
            if operator in SCALAR_EXPRESSION_OPERATORS:
                return evaluate_scalar_expression(
                    operator,
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                    evaluate_expression=lambda current_document, current_expression, current_variables=None: evaluate_expression(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    evaluate_expression_with_missing=lambda current_document, current_expression, current_variables=None: _evaluate_expression_with_missing(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    require_expression_args=lambda current_operator, current_spec, current_min_args, current_max_args=None: _require_expression_args(
                        current_operator,
                        current_spec,
                        min_args=current_min_args,
                        max_args=current_max_args,
                    ),
                    stringify_value=_stringify_aggregation_value,
                    bson_document_size=_bson_document_size,
                    missing_sentinel=_MISSING,
                )
            if operator in ARRAY_STRING_EXPRESSION_OPERATORS:
                return evaluate_array_string_expression(
                    operator,
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                    evaluate_expression=lambda current_document, current_expression, current_variables=None: evaluate_expression(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    evaluate_expression_with_missing=lambda current_document, current_expression, current_variables=None: _evaluate_expression_with_missing(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    require_expression_args=lambda current_operator, current_spec, current_min_args, current_max_args=None: _require_expression_args(
                        current_operator,
                        current_spec,
                        min_args=current_min_args,
                        max_args=current_max_args,
                    ),
                    missing_sentinel=_MISSING,
                )
            if operator in CONTROL_OBJECT_EXPRESSION_OPERATORS:
                return evaluate_control_object_expression(
                    operator,
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                    evaluate_expression=lambda current_document, current_expression, current_variables=None: evaluate_expression(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    evaluate_expression_with_missing=lambda current_document, current_expression, current_variables=None: _evaluate_expression_with_missing(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    require_expression_args=lambda current_operator, current_spec, current_min_args, current_max_args=None: _require_expression_args(
                        current_operator,
                        current_spec,
                        min_args=current_min_args,
                        max_args=current_max_args,
                    ),
                    compare_values=lambda left, right, comparison_operator: _compare_values(
                        left,
                        right,
                        comparison_operator,
                        dialect=dialect,
                    ),
                    expression_truthy=lambda value: _expression_truthy(value, dialect=dialect),
                    require_array=_require_array,
                    evaluate_pick_n_input=lambda current_operator, current_document, current_spec, current_variables=None: _evaluate_pick_n_input(
                        current_operator,
                        current_document,
                        current_spec,
                        current_variables,
                        dialect=dialect,
                        evaluate_expression=lambda nested_document, nested_expression, nested_variables=None: evaluate_expression(
                            nested_document,
                            nested_expression,
                            nested_variables,
                            dialect=dialect,
                        ),
                        evaluate_expression_with_missing=lambda nested_document, nested_expression, nested_variables=None: _evaluate_expression_with_missing(
                            nested_document,
                            nested_expression,
                            nested_variables,
                            dialect=dialect,
                        ),
                        missing_sentinel=_MISSING,
                    ),
                    missing_sentinel=_MISSING,
                )
            if operator in NUMERIC_EXPRESSION_OPERATORS:
                return evaluate_numeric_expression(
                    operator,
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                    evaluate_expression=lambda current_document, current_expression, current_variables=None: evaluate_expression(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    evaluate_expression_with_missing=lambda current_document, current_expression, current_variables=None: _evaluate_expression_with_missing(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    require_expression_args=lambda current_operator, current_spec, current_min_args, current_max_args=None: _require_expression_args(
                        current_operator,
                        current_spec,
                        min_args=current_min_args,
                        max_args=current_max_args,
                    ),
                    missing_sentinel=_MISSING,
                )
            if operator in DATE_EXPRESSION_OPERATORS:
                return evaluate_date_expression(
                    operator,
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                    evaluate_expression=lambda current_document, current_expression, current_variables=None: evaluate_expression(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    evaluate_expression_with_missing=lambda current_document, current_expression, current_variables=None: _evaluate_expression_with_missing(
                        current_document,
                        current_expression,
                        current_variables,
                        dialect=dialect,
                    ),
                    missing_sentinel=_MISSING,
                )
            raise OperationFailure(f"Unsupported aggregation expression: {operator}")

    return {
        key: evaluate_expression(document, value, variables, dialect=dialect)
        for key, value in expression.items()
    }


from mongoeco.core.aggregation.grouping_stages import (  # noqa: E402
    _apply_bucket,
    _apply_bucket_auto,
    _apply_count,
    _apply_group,
    _apply_set_window_fields,
    _apply_sort_by_count,
)
from mongoeco.core.aggregation.join_stages import (  # noqa: E402
    _apply_facet,
    _apply_lookup,
    _apply_union_with,
)
from mongoeco.core.aggregation.transform_stages import (  # noqa: E402
    _apply_add_fields,
    _apply_match,
    _apply_project,
    _apply_replace_root,
    _apply_sample,
    _apply_unset,
)


from mongoeco.core.aggregation.stages import AGGREGATION_STAGE_HANDLERS, AggregationStageHandler, apply_pipeline
