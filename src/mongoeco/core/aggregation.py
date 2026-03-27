import base64
import calendar
import datetime
import decimal
import math
import random
import re
import uuid
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any
from zoneinfo import ZoneInfo

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import BSONComparator
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.paths import delete_document_value, get_document_value, set_document_value
from mongoeco.core.projections import apply_projection, validate_projection_spec
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter, ObjectId, Projection, SortSpec, UndefinedType


type PipelineStage = dict[str, Any]
type Pipeline = list[PipelineStage]

_ACCUMULATOR_FLAGS_KEY = ("__mongoeco_internal__", "accumulator_flags")
_CURRENT_COLLECTION_RESOLVER_KEY = "__mongoeco_current_collection__"


def _aggregation_key(value: Any) -> Any:
    if value is None:
        return ("none", None)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, int):
        return ("int", value)
    if isinstance(value, float):
        return ("float", value)
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, bytes):
        return ("bytes", value)
    if isinstance(value, uuid.UUID):
        return ("uuid", value)
    if isinstance(value, ObjectId):
        return ("objectid", value)
    if isinstance(value, datetime.datetime):
        return ("datetime", value)
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
class AggregationPushdown:
    filter_spec: Filter
    projection: Projection | None
    sort: SortSpec | None
    skip: int
    limit: int | None
    remaining_pipeline: Pipeline


@dataclass(slots=True)
class _AverageAccumulator:
    total: int | float = 0
    count: int = 0


@dataclass(slots=True)
class _StdDevAccumulator:
    population: bool
    total: float = 0.0
    sum_of_squares: float = 0.0
    count: int = 0
    invalid: bool = False


def _accumulator_flags(bucket: dict[object, Any]) -> dict[str, bool]:
    flags = bucket.get(_ACCUMULATOR_FLAGS_KEY)
    if isinstance(flags, dict):
        return flags
    new_flags: dict[str, bool] = {}
    bucket[_ACCUMULATOR_FLAGS_KEY] = new_flags
    return new_flags


def _require_stage(stage: object) -> tuple[str, object]:
    if not isinstance(stage, dict) or len(stage) != 1:
        raise OperationFailure("Each pipeline stage must be a single-key document")
    operator, spec = next(iter(stage.items()))
    if not isinstance(operator, str) or not operator.startswith("$"):
        raise OperationFailure("Pipeline stage operator must start with '$'")
    return operator, spec


def _require_projection(spec: object) -> Projection:
    return _require_projection_for_dialect(spec, dialect=MONGODB_DIALECT_70)


def _require_projection_for_dialect(
    spec: object,
    *,
    dialect: MongoDialect,
) -> Projection:
    if not isinstance(spec, dict):
        raise OperationFailure("$project requires a document specification")
    include_flags: list[int] = []
    exclude_flags: list[int] = []
    computed_fields_present = False
    for key, value in spec.items():
        if not isinstance(key, str):
            raise OperationFailure("$project field names must be strings")
        flag = _projection_flag(value, dialect=dialect)
        if flag is None:
            if key != "_id":
                computed_fields_present = True
            continue
        if key == "_id":
            if flag == 1:
                include_flags.append(flag)
            continue
        if flag == 1:
            include_flags.append(flag)
        else:
            exclude_flags.append(flag)
    if include_flags and exclude_flags:
        raise OperationFailure("cannot mix inclusion and exclusion in $project")
    if exclude_flags and computed_fields_present:
        raise OperationFailure("cannot mix exclusion and computed fields in $project")
    return spec


def _require_sort(spec: object) -> SortSpec:
    if not isinstance(spec, dict):
        raise OperationFailure("$sort requires a document specification")
    sort: SortSpec = []
    for field, direction in spec.items():
        if not isinstance(field, str):
            raise OperationFailure("$sort fields must be strings")
        if isinstance(direction, bool):
            raise OperationFailure("$sort directions must be 1 or -1")
        if direction not in (1, -1):
            raise OperationFailure("$sort directions must be 1 or -1")
        sort.append((field, direction))
    return sort


def _require_non_negative_int(name: str, value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise OperationFailure(f"{name} requires a non-negative integer")
    return value


def _is_simple_projection(spec: object) -> bool:
    return _is_simple_projection_for_dialect(spec, dialect=MONGODB_DIALECT_70)


def _is_simple_projection_for_dialect(
    spec: object,
    *,
    dialect: MongoDialect,
) -> bool:
    if not isinstance(spec, dict):
        return False
    return all(
        _projection_flag(value, dialect=dialect) is not None
        for value in spec.values()
    )


def _projection_flag(value: object, *, dialect: MongoDialect) -> int | None:
    return dialect.projection_flag(value)


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


def _require_unset_spec(spec: object) -> list[str]:
    if isinstance(spec, str):
        fields = [spec]
    elif isinstance(spec, list):
        fields = spec
    else:
        raise OperationFailure("$unset requires a field path string or a list of field path strings")

    if not fields or not all(isinstance(field, str) and field for field in fields):
        raise OperationFailure("$unset requires non-empty field path strings")
    return fields


def _require_sample_spec(spec: object) -> int:
    if not isinstance(spec, dict):
        raise OperationFailure("$sample requires a document specification")
    if set(spec) != {"size"}:
        raise OperationFailure("$sample requires only a size field")
    return _require_non_negative_int("$sample size", spec["size"])


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
) -> bool:
    left = local_values or [None]
    right = foreign_values or [None]
    return any(
        QueryEngine._query_equality_matches(
            local_value,
            foreign_value,
            null_matches_undefined=dialect.null_query_matches_undefined(),
            dialect=dialect,
        )
        for local_value in left
        for foreign_value in right
    )


def _match_spec_contains_expr(match_spec: object) -> bool:
    if not isinstance(match_spec, dict):
        return False
    for key, value in match_spec.items():
        if key == "$expr":
            return True
        if key in {"$and", "$or", "$nor"} and isinstance(value, list):
            if any(_match_spec_contains_expr(item) for item in value):
                return True
    return False


def _merge_match_filters(left: Filter, right: Filter) -> Filter:
    if not left:
        return right
    return {"$and": [left, right]}


def _require_expression_args(operator: str, spec: object, *, min_args: int, max_args: int | None = None) -> list[object]:
    if not isinstance(spec, list):
        raise OperationFailure(f"{operator} requires a list expression")
    if len(spec) < min_args:
        raise OperationFailure(f"{operator} requires at least {min_args} arguments")
    if max_args is not None and len(spec) > max_args:
        raise OperationFailure(f"{operator} accepts at most {max_args} arguments")
    return spec


def _validate_accumulator_expression(operator: str, expression: object) -> None:
    if operator == "$count":
        if not isinstance(expression, dict) or expression:
            raise OperationFailure("$count accumulator requires an empty document")


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
    comparison = dialect.compare_values(left, right)
    return {
        "$gt": comparison > 0,
        "$gte": comparison >= 0,
        "$lt": comparison < 0,
        "$lte": comparison <= 0,
    }[operator]


def _require_numeric(operator: str, value: object) -> int | float:
    if not isinstance(value, (int, float, decimal.Decimal)) or isinstance(value, bool):
        raise OperationFailure(f"{operator} requires numeric arguments")
    return value


def _is_numeric(value: object) -> bool:
    return isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool)


def _sum_accumulator_operand(value: Any) -> int | float | None:
    if _is_numeric(value):
        return value
    if isinstance(value, list):
        return sum(item for item in value if _is_numeric(item))
    return None


def _stddev_accumulator_operand(value: Any) -> float | None:
    if isinstance(value, bool) or value is None or isinstance(value, list):
        return None
    if not _is_numeric(value):
        return None
    numeric_value = float(value)
    if not math.isfinite(numeric_value):
        return math.nan
    return numeric_value


def _stddev_expression_values(
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[float]:
    raw_values: list[Any]
    if isinstance(spec, list):
        raw_values = [
            evaluate_expression(document, item, variables, dialect=dialect)
            for item in spec
        ]
        traverse_arrays = False
    else:
        raw_values = [
            evaluate_expression(document, spec, variables, dialect=dialect)
        ]
        traverse_arrays = True

    values: list[float] = []
    for raw_value in raw_values:
        if raw_value is None or isinstance(raw_value, bool):
            continue
        if isinstance(raw_value, list):
            if not traverse_arrays:
                continue
            candidates = raw_value
        else:
            candidates = [raw_value]
        for candidate in candidates:
            if isinstance(candidate, bool) or candidate is None:
                continue
            if not _is_numeric(candidate):
                continue
            numeric_value = float(candidate)
            if not math.isfinite(numeric_value):
                return []
            values.append(numeric_value)
    return values


def _compute_stddev(values: list[float], *, population: bool) -> float | None:
    if not values:
        return None
    if population and len(values) == 1:
        return 0.0
    if not population and len(values) < 2:
        return None
    total = sum(values)
    sum_of_squares = sum(value * value for value in values)
    if population:
        mean = total / len(values)
        variance = max((sum_of_squares / len(values)) - (mean * mean), 0.0)
    else:
        variance = max(
            (sum_of_squares - ((total * total) / len(values))) / (len(values) - 1),
            0.0,
        )
    return math.sqrt(variance)


def _require_array(operator: str, value: object) -> list[Any]:
    if not isinstance(value, list):
        raise OperationFailure(f"{operator} requires array arguments")
    return value


def _slice_array(value: list[Any], spec: list[object], document: Document, variables: dict[str, Any] | None, *, dialect: MongoDialect) -> list[Any]:
    if len(spec) == 2:
        count = evaluate_expression(document, spec[1], variables, dialect=dialect)
        if not isinstance(count, int) or isinstance(count, bool):
            raise OperationFailure("$slice count must be an integer")
        if count == 0:
            return []
        if count > 0:
            return deepcopy(value[:count])
        return deepcopy(value[count:])

    position = evaluate_expression(document, spec[1], variables, dialect=dialect)
    count = evaluate_expression(document, spec[2], variables, dialect=dialect)
    if not isinstance(position, int) or isinstance(position, bool):
        raise OperationFailure("$slice position must be an integer")
    if not isinstance(count, int) or isinstance(count, bool):
        raise OperationFailure("$slice count must be an integer")
    if count < 0:
        raise OperationFailure("$slice count must be a non-negative integer")
    start = position if position >= 0 else len(value) + position
    start = min(max(start, 0), len(value))
    return deepcopy(value[start : start + count])


def _expression_truthy(value: Any, *, dialect: MongoDialect) -> bool:
    return dialect.expression_truthy(value)


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


def _normalize_sort_array_spec(spec: object) -> SortSpec | int:
    if spec in (1, -1) and not isinstance(spec, bool):
        return spec
    if not isinstance(spec, dict):
        raise OperationFailure("$sortArray sortBy must be 1, -1 or a document")
    sort_spec: SortSpec = []
    for field, direction in spec.items():
        if not isinstance(field, str):
            raise OperationFailure("$sortArray sort fields must be strings")
        if isinstance(direction, bool):
            raise OperationFailure("$sortArray sort directions must be 1 or -1")
        if direction not in (1, -1):
            raise OperationFailure("$sortArray sort directions must be 1 or -1")
        sort_spec.append((field, direction))
    return sort_spec


def _truncate_datetime(value: datetime.datetime, unit: str, bin_size: int, start_of_week: str) -> datetime.datetime:
    if bin_size <= 0:
        raise OperationFailure("$dateTrunc binSize must be a positive integer")

    if unit == "year":
        year = ((value.year - 1) // bin_size) * bin_size + 1
        return value.replace(year=year, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    if unit == "quarter":
        quarter_index = (value.month - 1) // 3
        truncated_quarter = (quarter_index // bin_size) * bin_size
        month = truncated_quarter * 3 + 1
        return value.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

    if unit == "month":
        month_index = value.month - 1
        month = (month_index // bin_size) * bin_size + 1
        return value.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

    if unit == "week":
        weekdays = {
            "sunday": 6,
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
        }
        normalized = start_of_week.lower()
        if normalized not in weekdays:
            raise OperationFailure("$dateTrunc startOfWeek is invalid")
        weekday_index = weekdays[normalized]
        day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
        delta = (day_start.weekday() - weekday_index) % 7
        week_start = day_start - datetime.timedelta(days=delta)
        anchor = datetime.datetime(1970, 1, 4, tzinfo=value.tzinfo)
        if normalized != "sunday":
            anchor_delta = (anchor.weekday() - weekday_index) % 7
            anchor -= datetime.timedelta(days=anchor_delta)
        weeks = (week_start - anchor).days // 7
        return anchor + datetime.timedelta(weeks=(weeks // bin_size) * bin_size)

    if unit == "day":
        day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        days = (day_start - anchor).days
        return anchor + datetime.timedelta(days=(days // bin_size) * bin_size)

    if unit == "hour":
        hour_start = value.replace(minute=0, second=0, microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        hours = int((hour_start - anchor).total_seconds() // 3600)
        return anchor + datetime.timedelta(hours=(hours // bin_size) * bin_size)

    if unit == "minute":
        minute_start = value.replace(second=0, microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        minutes = int((minute_start - anchor).total_seconds() // 60)
        return anchor + datetime.timedelta(minutes=(minutes // bin_size) * bin_size)

    if unit == "second":
        second_start = value.replace(microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        seconds = int((second_start - anchor).total_seconds())
        return anchor + datetime.timedelta(seconds=(seconds // bin_size) * bin_size)

    raise OperationFailure(f"Unsupported $dateTrunc unit: {unit}")


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

    if isinstance(value, list):
        head, _, tail = path.partition(".")
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
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


def _compare_strings_case_insensitive(left: str, right: str) -> int:
    normalized_left = left.lower()
    normalized_right = right.lower()
    if normalized_left < normalized_right:
        return -1
    if normalized_left > normalized_right:
        return 1
    return 0


def _substr_string(value: str, start: int, length: int) -> str:
    if start < 0:
        return ""
    raw = value.encode("utf-8")
    if start > len(raw):
        return ""
    end = len(raw) if length < 0 else min(len(raw), start + length)

    boundaries = {0, len(raw)}
    offset = 0
    for char in value:
        boundaries.add(offset)
        offset += len(char.encode("utf-8"))
        boundaries.add(offset)

    if start not in boundaries or end not in boundaries:
        raise OperationFailure("$substr byte offsets must align with UTF-8 code point boundaries")
    return raw[start:end].decode("utf-8")


def _substr_code_points(value: str, start: int, length: int) -> str:
    if start < 0:
        return ""
    if start > len(value):
        return ""
    end = len(value) if length < 0 else min(len(value), start + length)
    return value[start:end]


def _normalize_index_bounds(operator: str, start: Any, end: Any, upper_bound: int) -> tuple[int, int]:
    if start is None:
        start_index = 0
    else:
        if not isinstance(start, int) or isinstance(start, bool):
            raise OperationFailure(f"{operator} start must be an integer")
        if start < 0:
            raise OperationFailure(f"{operator} start must be a non-negative integer")
        start_index = start
    if end is None:
        end_index = upper_bound
    else:
        if not isinstance(end, int) or isinstance(end, bool):
            raise OperationFailure(f"{operator} end must be an integer")
        if end < 0:
            raise OperationFailure(f"{operator} end must be a non-negative integer")
        end_index = end
    if start_index > upper_bound:
        return upper_bound, upper_bound
    return start_index, min(end_index, upper_bound)


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


def _format_timezone_offset(value: datetime.datetime) -> str:
    offset = value.utcoffset() or datetime.timedelta()
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    return f"{sign}{hours:02d}{minutes:02d}"


def _mongo_format_datetime(value: datetime.datetime, fmt: str) -> str:
    result = fmt
    replacements = {
        "%Y": f"{value.year:04d}",
        "%m": f"{value.month:02d}",
        "%d": f"{value.day:02d}",
        "%H": f"{value.hour:02d}",
        "%M": f"{value.minute:02d}",
        "%S": f"{value.second:02d}",
        "%L": f"{value.microsecond // 1000:03d}",
        "%z": _format_timezone_offset(value),
        "%G": f"{value.isocalendar().year:04d}",
        "%V": f"{value.isocalendar().week:02d}",
        "%u": str(value.isocalendar().weekday),
    }
    for token, replacement in replacements.items():
        result = result.replace(token, replacement)
    return result


def _python_strptime_format(operator: str, fmt: str) -> str:
    supported_tokens = {
        "%Y": "%Y",
        "%m": "%m",
        "%d": "%d",
        "%H": "%H",
        "%M": "%M",
        "%S": "%S",
        "%L": "%f",
        "%z": "%z",
    }
    result = ""
    index = 0
    while index < len(fmt):
        if fmt[index] == "%" and index + 1 < len(fmt):
            token = fmt[index : index + 2]
            if token not in supported_tokens:
                raise OperationFailure(f"{operator} format token is not supported")
            result += supported_tokens[token]
            index += 2
            continue
        result += fmt[index]
        index += 1
    return result


def _default_date_to_string_format(timezone: datetime.tzinfo) -> str:
    return "%Y-%m-%dT%H:%M:%S.%LZ" if timezone == datetime.UTC else "%Y-%m-%dT%H:%M:%S.%L"


def _to_utc_naive(value: datetime.datetime) -> datetime.datetime:
    aware = value.astimezone(datetime.UTC) if value.tzinfo is not None else value.replace(tzinfo=datetime.UTC)
    return aware.replace(tzinfo=None)


def _require_int_part(operator: str, name: str, value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise OperationFailure(f"{operator} {name} must evaluate to an integer")
    return value


def _require_integral_numeric(operator: str, value: Any) -> int:
    if isinstance(value, bool):
        raise OperationFailure(f"{operator} requires integral numeric arguments")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    raise OperationFailure(f"{operator} requires integral numeric arguments")


def _build_date_from_parts(
    operator: str,
    spec: dict[str, Any],
    document: Document,
    variables: dict[str, Any],
    *,
    dialect: MongoDialect,
) -> datetime.datetime:
    timezone = _resolve_timezone(
        operator,
        evaluate_expression(document, spec["timezone"], variables, dialect=dialect)
        if "timezone" in spec
        else None,
    )

    standard_keys = {"year", "month", "day"}
    iso_keys = {"isoWeekYear", "isoWeek", "isoDayOfWeek"}
    has_standard = any(key in spec for key in standard_keys)
    has_iso = any(key in spec for key in iso_keys)
    if has_standard and has_iso:
        raise OperationFailure(f"{operator} cannot mix calendar and iso week date parts")
    if not has_standard and not has_iso:
        raise OperationFailure(f"{operator} requires either year or isoWeekYear")
    if has_standard and "year" not in spec:
        raise OperationFailure(f"{operator} year must be specified for calendar date parts")
    if has_iso and "isoWeekYear" not in spec:
        raise OperationFailure(f"{operator} isoWeekYear must be specified for iso week date parts")

    hour = _require_int_part(operator, "hour", evaluate_expression(document, spec.get("hour", 0), variables, dialect=dialect))
    minute = _require_int_part(operator, "minute", evaluate_expression(document, spec.get("minute", 0), variables, dialect=dialect))
    second = _require_int_part(operator, "second", evaluate_expression(document, spec.get("second", 0), variables, dialect=dialect))
    millisecond = _require_int_part(operator, "millisecond", evaluate_expression(document, spec.get("millisecond", 0), variables, dialect=dialect))

    try:
        if has_standard:
            year = _require_int_part(operator, "year", evaluate_expression(document, spec["year"], variables, dialect=dialect))
            month = _require_int_part(operator, "month", evaluate_expression(document, spec.get("month", 1), variables, dialect=dialect))
            day = _require_int_part(operator, "day", evaluate_expression(document, spec.get("day", 1), variables, dialect=dialect))
            localized = datetime.datetime(year, month, day, hour, minute, second, millisecond * 1000, tzinfo=timezone)
        else:
            iso_year = _require_int_part(operator, "isoWeekYear", evaluate_expression(document, spec["isoWeekYear"], variables, dialect=dialect))
            iso_week = _require_int_part(operator, "isoWeek", evaluate_expression(document, spec.get("isoWeek", 1), variables, dialect=dialect))
            iso_day = _require_int_part(operator, "isoDayOfWeek", evaluate_expression(document, spec.get("isoDayOfWeek", 1), variables, dialect=dialect))
            base = datetime.date.fromisocalendar(iso_year, iso_week, iso_day)
            localized = datetime.datetime(base.year, base.month, base.day, hour, minute, second, millisecond * 1000, tzinfo=timezone)
    except ValueError as exc:
        raise OperationFailure(f"{operator} produced an invalid date") from exc
    return _to_utc_naive(localized)


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
    if isinstance(value, (bytes, bytearray)):
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


def _trim_string(value: str, chars: str | None, *, mode: str) -> str:
    if chars is None:
        start = 0
        end = len(value)
        if mode in {"left", "both"}:
            while start < end and (value[start].isspace() or value[start] == "\x00"):
                start += 1
        if mode in {"right", "both"}:
            while end > start and (value[end - 1].isspace() or value[end - 1] == "\x00"):
                end -= 1
        return value[start:end]
    if mode == "left":
        return value.lstrip(chars)
    if mode == "right":
        return value.rstrip(chars)
    return value.strip(chars)


def _compile_aggregation_regex(
    regex_value: Any,
    options_value: Any,
    *,
    operator: str,
) -> re.Pattern[str]:
    supported = {"i": re.IGNORECASE, "m": re.MULTILINE, "s": re.DOTALL, "x": re.VERBOSE}
    flags = 0
    pattern: str

    if options_value is not None and not isinstance(options_value, str):
        raise OperationFailure(f"{operator} options must be a string")

    if isinstance(regex_value, re.Pattern):
        if options_value not in {None, ""}:
            raise OperationFailure(f"{operator} cannot specify options in both regex and options")
        disallowed_flags = regex_value.flags & (re.DOTALL | re.VERBOSE)
        if disallowed_flags:
            raise OperationFailure(f"{operator} regex patterns only support embedded i and m options")
        flags = regex_value.flags & (re.IGNORECASE | re.MULTILINE)
        pattern = regex_value.pattern
    elif isinstance(regex_value, str):
        pattern = regex_value
    else:
        raise OperationFailure(f"{operator} regex must resolve to a string or regex pattern")

    if options_value:
        for option in options_value:
            if option not in supported:
                raise OperationFailure(f"Unsupported regex option: {option}")
            flags |= supported[option]

    return re.compile(pattern, flags)


def _build_regex_match_result(match: re.Match[str]) -> dict[str, Any]:
    return {
        "match": match.group(0),
        "idx": match.start(),
        "captures": list(match.groups(default=None)),
    }


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


def _aggregation_type_name(value: Any) -> str:
    if value is _MISSING:
        return "missing"
    if isinstance(value, UndefinedType):
        return "undefined"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "long" if value < -(1 << 31) or value > (1 << 31) - 1 else "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, decimal.Decimal):
        return "decimal"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, (bytes, bytearray, uuid.UUID)):
        return "binData"
    if isinstance(value, ObjectId):
        return "objectId"
    if isinstance(value, datetime.datetime):
        return "date"
    if isinstance(value, re.Pattern):
        return "regex"
    return type(value).__name__


def _datetime_to_epoch_millis(value: datetime.datetime) -> int:
    normalized = value.astimezone(datetime.UTC) if value.tzinfo is not None else value.replace(tzinfo=datetime.UTC)
    return int(normalized.timestamp() * 1000)


def _parse_base10_int_string(operator: str, value: str) -> int:
    text = value.strip()
    if not text or not re.fullmatch(r"[+-]?\d+", text):
        raise OperationFailure(f"{operator} cannot convert the string value")
    return int(text, 10)


def _convert_aggregation_scalar(operator: str, value: Any, target: str) -> Any:
    if value is _MISSING or value is None:
        return None
    if isinstance(value, UndefinedType):
        return None

    if target == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value != 0
        if isinstance(value, (str, list, dict, bytes, bytearray, uuid.UUID, ObjectId, datetime.datetime)):
            return True
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "int":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            if value < -(1 << 31) or value > (1 << 31) - 1:
                raise OperationFailure(f"{operator} overflow")
            return value
        if isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                raise OperationFailure(f"{operator} cannot convert the value")
            integer = int(value)
            if integer < -(1 << 31) or integer > (1 << 31) - 1:
                raise OperationFailure(f"{operator} overflow")
            return integer
        if isinstance(value, str):
            integer = _parse_base10_int_string(operator, value)
            if integer < -(1 << 31) or integer > (1 << 31) - 1:
                raise OperationFailure(f"{operator} overflow")
            return integer
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "long":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            if value < -(1 << 63) or value > (1 << 63) - 1:
                raise OperationFailure(f"{operator} overflow")
            return value
        if isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                raise OperationFailure(f"{operator} cannot convert the value")
            integer = int(value)
            if integer < -(1 << 63) or integer > (1 << 63) - 1:
                raise OperationFailure(f"{operator} overflow")
            return integer
        if isinstance(value, str):
            integer = _parse_base10_int_string(operator, value)
            if integer < -(1 << 63) or integer > (1 << 63) - 1:
                raise OperationFailure(f"{operator} overflow")
            return integer
        if isinstance(value, datetime.datetime):
            return _datetime_to_epoch_millis(value)
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "double":
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                raise OperationFailure(f"{operator} cannot convert the string value")
            try:
                return float(text)
            except ValueError as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        if isinstance(value, datetime.datetime):
            return float(_datetime_to_epoch_millis(value))
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "date":
        if isinstance(value, datetime.datetime):
            return _to_utc_naive(value)
        if isinstance(value, ObjectId):
            return datetime.datetime.fromtimestamp(value.generation_time, tz=datetime.UTC).replace(tzinfo=None)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if not math.isfinite(value):
                raise OperationFailure(f"{operator} cannot convert the value")
            return datetime.datetime.fromtimestamp(value / 1000, tz=datetime.UTC).replace(tzinfo=None)
        if isinstance(value, str):
            try:
                return _to_utc_naive(datetime.datetime.fromisoformat(value.replace("Z", "+00:00")))
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "objectId":
        if isinstance(value, ObjectId):
            return value
        if isinstance(value, str):
            try:
                return ObjectId(value)
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "decimal":
        if isinstance(value, bool):
            return decimal.Decimal(int(value))
        if isinstance(value, int):
            return decimal.Decimal(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                raise OperationFailure(f"{operator} cannot convert the value")
            return decimal.Decimal(str(value))
        if isinstance(value, decimal.Decimal):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                raise OperationFailure(f"{operator} cannot convert the string value")
            try:
                return decimal.Decimal(text)
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "string":
        return _stringify_aggregation_value(value)

    raise OperationFailure(f"Unsupported conversion target for {operator}")


def _resolve_timezone(operator: str, timezone_value: Any) -> datetime.tzinfo:
    if timezone_value is None:
        return datetime.UTC
    if not isinstance(timezone_value, str):
        raise OperationFailure(f"{operator} timezone must evaluate to a string")
    normalized = timezone_value.strip()
    if normalized in {"UTC", "GMT", "Z"}:
        return datetime.UTC
    offset_match = re.fullmatch(r"([+-])(\d{2})(?::?(\d{2}))?", normalized)
    if offset_match:
        sign, hours_text, minutes_text = offset_match.groups()
        hours = int(hours_text)
        minutes = int(minutes_text or "0")
        delta = datetime.timedelta(hours=hours, minutes=minutes)
        if sign == "-":
            delta = -delta
        return datetime.timezone(delta)
    try:
        return ZoneInfo(normalized)
    except Exception as exc:
        raise OperationFailure(f"{operator} timezone is invalid") from exc


def _localize_datetime(value: datetime.datetime, timezone: datetime.tzinfo) -> datetime.datetime:
    aware_utc = value.astimezone(datetime.UTC) if value.tzinfo is not None else value.replace(tzinfo=datetime.UTC)
    return aware_utc.astimezone(timezone)


def _restore_datetime_timezone(
    value: datetime.datetime,
    original: datetime.datetime,
) -> datetime.datetime:
    utc_value = value.astimezone(datetime.UTC)
    if original.tzinfo is None:
        return utc_value.replace(tzinfo=None)
    return utc_value.astimezone(original.tzinfo)


def _evaluate_localized_date_operand(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> datetime.datetime | None:
    timezone_value = None
    date_expression: Any = spec
    if isinstance(spec, dict):
        if 'date' not in spec:
            raise OperationFailure(f'{operator} requires a date expression')
        date_expression = spec['date']
        timezone_value = (
            evaluate_expression(document, spec['timezone'], variables, dialect=dialect)
            if 'timezone' in spec
            else None
        )
    value = _evaluate_expression_with_missing(
        document,
        date_expression,
        variables,
        dialect=dialect,
    )
    if value is _MISSING or value is None:
        return None
    if not isinstance(value, datetime.datetime):
        raise OperationFailure(f'{operator} requires a date input')
    timezone = _resolve_timezone(operator, timezone_value)
    return _localize_datetime(value, timezone)


def _add_calendar_months(value: datetime.datetime, months: int) -> datetime.datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _add_date_unit(value: datetime.datetime, unit: str, amount: int) -> datetime.datetime:
    if unit == "millisecond":
        return value + datetime.timedelta(milliseconds=amount)
    if unit == "second":
        return value + datetime.timedelta(seconds=amount)
    if unit == "minute":
        return value + datetime.timedelta(minutes=amount)
    if unit == "hour":
        return value + datetime.timedelta(hours=amount)
    if unit == "day":
        return value + datetime.timedelta(days=amount)
    if unit == "week":
        return value + datetime.timedelta(weeks=amount)
    if unit == "month":
        return _add_calendar_months(value, amount)
    if unit == "quarter":
        return _add_calendar_months(value, amount * 3)
    if unit == "year":
        return _add_calendar_months(value, amount * 12)
    raise OperationFailure(f"Unsupported date unit: {unit}")


def _require_date_unit(operator: str, unit: Any) -> str:
    if not isinstance(unit, str):
        raise OperationFailure(f"{operator} unit must evaluate to a string")
    if unit not in {"year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond"}:
        raise OperationFailure(f"{operator} unit is invalid")
    return unit


def _month_difference(start: datetime.datetime, end: datetime.datetime) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def _date_diff_units(
    start: datetime.datetime,
    end: datetime.datetime,
    unit: str,
    *,
    start_of_week: str,
) -> int:
    if unit == "millisecond":
        return int((end - start).total_seconds() * 1000)
    if unit == "second":
        return int(( _truncate_datetime(end, "second", 1, start_of_week) - _truncate_datetime(start, "second", 1, start_of_week)).total_seconds())
    if unit == "minute":
        return int(( _truncate_datetime(end, "minute", 1, start_of_week) - _truncate_datetime(start, "minute", 1, start_of_week)).total_seconds() // 60)
    if unit == "hour":
        return int(( _truncate_datetime(end, "hour", 1, start_of_week) - _truncate_datetime(start, "hour", 1, start_of_week)).total_seconds() // 3600)
    if unit == "day":
        return (_truncate_datetime(end, "day", 1, start_of_week) - _truncate_datetime(start, "day", 1, start_of_week)).days
    if unit == "week":
        return (_truncate_datetime(end, "week", 1, start_of_week) - _truncate_datetime(start, "week", 1, start_of_week)).days // 7
    if unit == "month":
        return _month_difference(_truncate_datetime(start, "month", 1, start_of_week), _truncate_datetime(end, "month", 1, start_of_week))
    if unit == "quarter":
        return _month_difference(_truncate_datetime(start, "quarter", 1, start_of_week), _truncate_datetime(end, "quarter", 1, start_of_week)) // 3
    if unit == "year":
        return _truncate_datetime(end, "year", 1, start_of_week).year - _truncate_datetime(start, "year", 1, start_of_week).year
    raise OperationFailure("$dateDiff unit is invalid")


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
            if not dialect.supports_aggregation_expression_operator(operator):
                raise OperationFailure(f"Unsupported aggregation expression: {operator}")
            if operator == "$literal":
                return deepcopy(spec)
            if operator == "$rand":
                if spec != {}:
                    raise OperationFailure("$rand does not accept arguments")
                return random.random()
            if operator == "$convert":
                if not isinstance(spec, dict) or "input" not in spec or "to" not in spec:
                    raise OperationFailure("$convert requires input and to")
                value = _evaluate_expression_with_missing(document, spec["input"], variables, dialect=dialect)
                if value is _MISSING or value is None:
                    return evaluate_expression(document, spec["onNull"], variables, dialect=dialect) if "onNull" in spec else None
                target = evaluate_expression(document, spec["to"], variables, dialect=dialect)
                if isinstance(target, dict):
                    target = target.get("type")
                if not isinstance(target, str):
                    raise OperationFailure("$convert to must resolve to a string")
                aliases = {
                    "bool": "bool",
                    "int": "int",
                    "long": "long",
                    "double": "double",
                    "date": "date",
                    "objectId": "objectId",
                    "string": "string",
                }
                try:
                    if target not in aliases:
                        raise OperationFailure("$convert target type is not supported")
                    return _convert_aggregation_scalar(operator, value, aliases[target])
                except OperationFailure:
                    if "onError" in spec:
                        return evaluate_expression(document, spec["onError"], variables, dialect=dialect)
                    raise
            if operator == "$bsonSize":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                if value is _MISSING or value is None:
                    return None
                if not isinstance(value, dict):
                    raise OperationFailure("$bsonSize requires an object input")
                return _bson_document_size(value)
            if operator in {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}:
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                left = evaluate_expression(document, args[0], variables, dialect=dialect)
                right = evaluate_expression(document, args[1], variables, dialect=dialect)
                return _compare_values(left, right, operator, dialect=dialect)
            if operator == "$and":
                args = _require_expression_args(operator, spec, min_args=0)
                return all(
                    _expression_truthy(
                        evaluate_expression(document, item, variables, dialect=dialect),
                        dialect=dialect,
                    )
                    for item in args
                )
            if operator == "$or":
                args = _require_expression_args(operator, spec, min_args=0)
                return any(
                    _expression_truthy(
                        evaluate_expression(document, item, variables, dialect=dialect),
                        dialect=dialect,
                    )
                    for item in args
                )
            if operator == "$in":
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                needle = evaluate_expression(document, args[0], variables, dialect=dialect)
                haystack = evaluate_expression(document, args[1], variables, dialect=dialect)
                if haystack is None:
                    return None
                if not isinstance(haystack, list):
                    raise OperationFailure("$in requires the second argument to evaluate to a list")
                return any(
                    QueryEngine._values_equal(needle, item, dialect=dialect)
                    for item in haystack
                )
            if operator == "$ifNull":
                args = _require_expression_args(operator, spec, min_args=2)
                for item in args:
                    value = evaluate_expression(document, item, variables, dialect=dialect)
                    if value is not None and not isinstance(value, UndefinedType):
                        return value
                return None
            if operator == "$cond":
                if isinstance(spec, list):
                    args = _require_expression_args(operator, spec, min_args=3, max_args=3)
                    condition, when_true, when_false = args
                elif isinstance(spec, dict):
                    if not {"if", "then", "else"} <= set(spec):
                        raise OperationFailure("$cond object form requires if, then and else")
                    condition = spec["if"]
                    when_true = spec["then"]
                    when_false = spec["else"]
                else:
                    raise OperationFailure("$cond requires a list or document specification")
                condition_value = evaluate_expression(
                    document,
                    condition,
                    variables,
                    dialect=dialect,
                )
                branch = when_true if _expression_truthy(condition_value, dialect=dialect) else when_false
                return evaluate_expression(document, branch, variables, dialect=dialect)
            if operator == "$setField":
                if not isinstance(spec, dict) or not {"field", "input", "value"} <= set(spec):
                    raise OperationFailure("$setField requires field, input, and value")
                field_name = evaluate_expression(document, spec["field"], variables, dialect=dialect)
                if not isinstance(field_name, str):
                    raise OperationFailure("$setField field must resolve to a string")
                input_value = _evaluate_expression_with_missing(document, spec["input"], variables, dialect=dialect)
                if input_value is _MISSING or input_value is None:
                    return None
                if not isinstance(input_value, dict):
                    raise OperationFailure("$setField input must resolve to an object")
                result = deepcopy(input_value)
                result[field_name] = evaluate_expression(document, spec["value"], variables, dialect=dialect)
                return result
            if operator == "$unsetField":
                if not isinstance(spec, dict) or not {"field", "input"} <= set(spec):
                    raise OperationFailure("$unsetField requires field and input")
                field_name = evaluate_expression(document, spec["field"], variables, dialect=dialect)
                if not isinstance(field_name, str):
                    raise OperationFailure("$unsetField field must resolve to a string")
                input_value = _evaluate_expression_with_missing(document, spec["input"], variables, dialect=dialect)
                if input_value is _MISSING or input_value is None or isinstance(input_value, UndefinedType):
                    return None
                if not isinstance(input_value, dict):
                    raise OperationFailure("$unsetField input must resolve to an object")
                result = deepcopy(input_value)
                result.pop(field_name, None)
                return result
            if operator == "$switch":
                if not isinstance(spec, dict) or "branches" not in spec:
                    raise OperationFailure("$switch requires branches")
                branches = spec["branches"]
                if not isinstance(branches, list) or not branches:
                    raise OperationFailure("$switch branches must be a non-empty array")
                for branch in branches:
                    if not isinstance(branch, dict) or set(branch) != {"case", "then"}:
                        raise OperationFailure("$switch branches must contain case and then")
                    condition_value = evaluate_expression(document, branch["case"], variables, dialect=dialect)
                    if _expression_truthy(condition_value, dialect=dialect):
                        return evaluate_expression(document, branch["then"], variables, dialect=dialect)
                if "default" in spec:
                    return evaluate_expression(document, spec["default"], variables, dialect=dialect)
                raise OperationFailure("$switch could not find a matching branch for an input, and no default was specified")
            if operator in {"$add", "$multiply"}:
                args = _require_expression_args(operator, spec, min_args=2)
                raw_values = [
                    evaluate_expression(document, item, variables, dialect=dialect)
                    for item in args
                ]
                if any(value is None for value in raw_values):
                    return None
                if operator == "$add":
                    date_values = [
                        value for value in raw_values
                        if isinstance(value, datetime.datetime)
                    ]
                    if date_values:
                        if len(date_values) != 1:
                            raise OperationFailure("$add only supports a single date argument")
                        result = date_values[0]
                        for value in raw_values:
                            if isinstance(value, datetime.datetime):
                                continue
                            result = _add_milliseconds(result, _require_numeric(operator, value))
                        return result
                values = [_require_numeric(operator, value) for value in raw_values]
                result = values[0]
                for value in values[1:]:
                    result = result + value if operator == "$add" else result * value
                return result
            if operator in {"$subtract", "$divide", "$mod"}:
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                left_raw = evaluate_expression(document, args[0], variables, dialect=dialect)
                right_raw = evaluate_expression(document, args[1], variables, dialect=dialect)
                if left_raw is None or right_raw is None:
                    return None
                if operator == "$subtract":
                    return _subtract_values(left_raw, right_raw)
                left = _require_numeric(operator, left_raw)
                right = _require_numeric(operator, right_raw)
                if operator == "$divide":
                    if right == 0:
                        raise OperationFailure("$divide cannot divide by zero")
                    return left / right
                if right == 0:
                    raise OperationFailure("$mod cannot divide by zero")
                return _mongo_mod(left, right)
            if operator in {"$bitAnd", "$bitOr", "$bitXor"}:
                args = _require_expression_args(operator, spec, min_args=2)
                values = [
                    _require_integral_numeric(
                        operator,
                        evaluate_expression(document, item, variables, dialect=dialect),
                    )
                    for item in args
                ]
                result = values[0]
                for value in values[1:]:
                    if operator == "$bitAnd":
                        result &= value
                    elif operator == "$bitOr":
                        result |= value
                    else:
                        result ^= value
                return result
            if operator == "$bitNot":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = _require_integral_numeric(
                    operator,
                    evaluate_expression(document, args[0], variables, dialect=dialect),
                )
                return ~value
            if operator in {"$abs", "$exp", "$ln", "$log10", "$sqrt"}:
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                raw_value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                if raw_value is _MISSING or raw_value is None:
                    return None
                value = _require_numeric(operator, raw_value)
                if operator == "$abs":
                    return abs(value)
                if math.isnan(value):
                    return value
                if operator == "$exp":
                    return math.exp(value)
                if operator == "$sqrt":
                    if value < 0:
                        raise OperationFailure("$sqrt cannot operate on negative numbers")
                    return math.sqrt(value)
                if value <= 0:
                    raise OperationFailure(f"{operator} requires a positive numeric argument")
                return math.log(value) if operator == "$ln" else math.log10(value)
            if operator in {"$stdDevPop", "$stdDevSamp"}:
                values = _stddev_expression_values(
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                )
                return _compute_stddev(values, population=operator == "$stdDevPop")
            if operator in {"$log", "$pow"}:
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                left_raw = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                right_raw = _evaluate_expression_with_missing(document, args[1], variables, dialect=dialect)
                if left_raw is _MISSING or right_raw is _MISSING or left_raw is None or right_raw is None:
                    return None
                left = _require_numeric(operator, left_raw)
                right = _require_numeric(operator, right_raw)
                if operator == "$pow":
                    return math.pow(left, right)
                if math.isnan(left) or math.isnan(right):
                    return math.nan
                if left <= 0 or right <= 0 or right == 1:
                    raise OperationFailure("$log requires a positive argument and base where base != 1")
                return math.log(left, right)
            if operator in {"$round", "$trunc"}:
                args = _require_expression_args(operator, spec, min_args=1, max_args=2)
                raw_value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                if raw_value is _MISSING or raw_value is None:
                    return None
                value = _require_numeric(operator, raw_value)
                if math.isnan(value) or math.isinf(value):
                    return value
                place_raw = evaluate_expression(document, args[1], variables, dialect=dialect) if len(args) == 2 else 0
                place = _normalize_numeric_place(operator, place_raw)
                if operator == "$round":
                    return _round_numeric(value, place)
                return _trunc_numeric(value, place)
            if operator in {"$floor", "$ceil"}:
                args = _require_expression_args(operator, spec, min_args=1, max_args=1)
                raw_value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if raw_value is None:
                    return None
                value = _require_numeric(operator, raw_value)
                if math.isnan(value) or math.isinf(value):
                    return value
                integer = int(value)
                if operator == "$floor":
                    return integer if integer <= value else integer - 1
                return integer if integer >= value else integer + 1
            if operator == "$range":
                args = _require_expression_args(operator, spec, min_args=2, max_args=3)
                start = evaluate_expression(document, args[0], variables, dialect=dialect)
                end = evaluate_expression(document, args[1], variables, dialect=dialect)
                step = evaluate_expression(document, args[2], variables, dialect=dialect) if len(args) == 3 else 1
                if not all(isinstance(value, int) and not isinstance(value, bool) for value in (start, end, step)):
                    raise OperationFailure("$range requires integer arguments")
                if step == 0:
                    raise OperationFailure("$range step cannot be zero")
                return list(range(start, end, step))
            if operator == "$size":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if value is None:
                    return None
                if not isinstance(value, list):
                    raise OperationFailure("$size requires an array value")
                return len(value)
            if operator == "$arrayElemAt":
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                values = evaluate_expression(document, args[0], variables, dialect=dialect)
                index = evaluate_expression(document, args[1], variables, dialect=dialect)
                if values is None:
                    return None
                if index is None:
                    return None
                if not isinstance(values, list):
                    raise OperationFailure("$arrayElemAt requires an array as first argument")
                if not isinstance(index, int) or isinstance(index, bool):
                    raise OperationFailure("$arrayElemAt requires an integer index")
                if index < 0:
                    index += len(values)
                if index < 0 or index >= len(values):
                    return None
                return values[index]
            if operator == "$isNumber":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                return isinstance(value, (int, float)) and not isinstance(value, bool)
            if operator == "$type":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                return _aggregation_type_name(value)
            if operator == "$toString":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = evaluate_expression(document, args[0], variables, dialect=dialect)
                return None if value is None else _stringify_aggregation_value(value)
            if operator == "$let":
                if not isinstance(spec, dict) or "vars" not in spec or "in" not in spec or not isinstance(spec["vars"], dict):
                    raise OperationFailure("$let requires vars and in")
                scoped = dict(variables)
                for name, value_expression in spec["vars"].items():
                    scoped[name] = evaluate_expression(
                        document,
                        value_expression,
                        variables,
                        dialect=dialect,
                    )
                return evaluate_expression(document, spec["in"], scoped, dialect=dialect)
            if operator == "$first":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if isinstance(value, list):
                    return value[0] if value else None
                return value
            if operator == "$concat":
                args = _require_expression_args(operator, spec, min_args=1)
                parts: list[str] = []
                for item in args:
                    value = evaluate_expression(document, item, variables, dialect=dialect)
                    if value is None:
                        return None
                    if not isinstance(value, str):
                        raise OperationFailure("$concat requires string arguments")
                    parts.append(value)
                return "".join(parts)
            if operator in {"$trim", "$ltrim", "$rtrim"}:
                if not isinstance(spec, dict) or "input" not in spec:
                    raise OperationFailure(f"{operator} requires an input field")
                value = evaluate_expression(document, spec["input"], variables, dialect=dialect)
                if value is None:
                    return None
                if not isinstance(value, str):
                    raise OperationFailure(f"{operator} requires a string input")
                chars = spec.get("chars")
                if chars is not None:
                    chars = evaluate_expression(document, chars, variables, dialect=dialect)
                    if chars is None:
                        return None
                    if not isinstance(chars, str):
                        raise OperationFailure(f"{operator} chars must evaluate to a string")
                mode = {"$trim": "both", "$ltrim": "left", "$rtrim": "right"}[operator]
                return _trim_string(value, chars, mode=mode)
            if operator in {"$replaceOne", "$replaceAll"}:
                if not isinstance(spec, dict) or not {"input", "find", "replacement"} <= set(spec):
                    raise OperationFailure(f"{operator} requires input, find and replacement")
                source = evaluate_expression(document, spec["input"], variables, dialect=dialect)
                find = evaluate_expression(document, spec["find"], variables, dialect=dialect)
                replacement = evaluate_expression(document, spec["replacement"], variables, dialect=dialect)
                if source is None or find is None or replacement is None:
                    return None
                if not isinstance(source, str) or not isinstance(find, str) or not isinstance(replacement, str):
                    raise OperationFailure(f"{operator} requires string input, find and replacement")
                return source.replace(find, replacement, 1 if operator == "$replaceOne" else -1)
            if operator == "$strcasecmp":
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                left = evaluate_expression(document, args[0], variables, dialect=dialect)
                right = evaluate_expression(document, args[1], variables, dialect=dialect)
                if left is None or right is None:
                    return None
                if not isinstance(left, str) or not isinstance(right, str):
                    raise OperationFailure("$strcasecmp requires string arguments")
                return _compare_strings_case_insensitive(left, right)
            if operator in {"$substr", "$substrBytes", "$substrCP"}:
                args = _require_expression_args(operator, spec, min_args=3, max_args=3)
                source = evaluate_expression(document, args[0], variables, dialect=dialect)
                start = evaluate_expression(document, args[1], variables, dialect=dialect)
                length = evaluate_expression(document, args[2], variables, dialect=dialect)
                if source is None:
                    return ""
                if not isinstance(source, str):
                    raise OperationFailure(f"{operator} requires the first argument to evaluate to a string")
                if not isinstance(start, int) or isinstance(start, bool):
                    raise OperationFailure(f"{operator} start must be an integer")
                if not isinstance(length, int) or isinstance(length, bool):
                    raise OperationFailure(f"{operator} length must be an integer")
                if operator == "$substrCP":
                    return _substr_code_points(source, start, length)
                return _substr_string(source, start, length)
            if operator in {"$strLenBytes", "$strLenCP"}:
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                if value is _MISSING or value is None:
                    raise OperationFailure(f"{operator} requires a string argument, found: null")
                if not isinstance(value, str):
                    raise OperationFailure(f"{operator} requires a string argument")
                return len(value.encode("utf-8")) if operator == "$strLenBytes" else len(value)
            if operator in {"$toBool", "$toDate", "$toInt", "$toDouble", "$toLong", "$toObjectId", "$toDecimal"}:
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                target = {
                    "$toBool": "bool",
                    "$toDate": "date",
                    "$toDecimal": "decimal",
                    "$toInt": "int",
                    "$toDouble": "double",
                    "$toLong": "long",
                    "$toObjectId": "objectId",
                }[operator]
                return _convert_aggregation_scalar(operator, value, target)
            if operator in {"$toLower", "$toUpper"}:
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if value is None:
                    return ""
                if not isinstance(value, str):
                    raise OperationFailure(f"{operator} requires a string argument")
                return value.lower() if operator == "$toLower" else value.upper()
            if operator == "$split":
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                source = evaluate_expression(document, args[0], variables, dialect=dialect)
                delimiter = evaluate_expression(document, args[1], variables, dialect=dialect)
                if source is None or delimiter is None:
                    return None
                if not isinstance(source, str) or not isinstance(delimiter, str):
                    raise OperationFailure("$split requires string arguments")
                if delimiter == "":
                    raise OperationFailure("$split delimiter must not be an empty string")
                return source.split(delimiter)
            if operator == "$concatArrays":
                args = _require_expression_args(operator, spec, min_args=1)
                result: list[Any] = []
                for item in args:
                    value = evaluate_expression(document, item, variables, dialect=dialect)
                    if value is None:
                        return None
                    result.extend(deepcopy(_require_array(operator, value)))
                return result
            if operator == "$reverseArray":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if value is None:
                    return None
                return list(reversed(deepcopy(_require_array(operator, value))))
            if operator in {"$allElementsTrue", "$anyElementTrue"}:
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if value is None:
                    return None
                values = _require_array(operator, value)
                predicate = all if operator == "$allElementsTrue" else any
                return predicate(_expression_truthy(item, dialect=dialect) for item in values)
            if operator == "$setUnion":
                args = _require_expression_args(operator, spec, min_args=1)
                result: list[Any] = []
                for item in args:
                    value = evaluate_expression(document, item, variables, dialect=dialect)
                    if value is None:
                        return None
                    _append_unique_values(
                        result,
                        _require_array(operator, value),
                        dialect=dialect,
                    )
                return result
            if operator in {"$setDifference", "$setIntersection"}:
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                left = evaluate_expression(document, args[0], variables, dialect=dialect)
                right = evaluate_expression(document, args[1], variables, dialect=dialect)
                if left is None or right is None:
                    return None
                left_values = _require_array(operator, left)
                right_values = _require_array(operator, right)
                left_unique: list[Any] = []
                right_unique: list[Any] = []
                _append_unique_values(left_unique, left_values, dialect=dialect)
                _append_unique_values(right_unique, right_values, dialect=dialect)
                result: list[Any] = []
                for item in left_unique:
                    in_right = any(QueryEngine._values_equal(item, candidate, dialect=dialect) for candidate in right_unique)
                    if operator == "$setDifference" and not in_right:
                        result.append(deepcopy(item))
                    if operator == "$setIntersection" and in_right:
                        result.append(deepcopy(item))
                return result
            if operator in {"$setEquals", "$setIsSubset"}:
                args = _require_expression_args(
                    operator,
                    spec,
                    min_args=2,
                    max_args=2 if operator == "$setIsSubset" else None,
                )
                sets: list[list[Any]] = []
                for item in args:
                    value = evaluate_expression(document, item, variables, dialect=dialect)
                    if value is None:
                        return None
                    normalized: list[Any] = []
                    _append_unique_values(normalized, _require_array(operator, value), dialect=dialect)
                    sets.append(normalized)
                base = sets[0]
                if operator == "$setEquals":
                    for candidate in sets[1:]:
                        if len(base) != len(candidate):
                            return False
                        if any(
                            not any(QueryEngine._values_equal(item, other, dialect=dialect) for other in candidate)
                            for item in base
                        ):
                            return False
                    return True
                for candidate in sets[1:]:
                    if any(
                        not any(QueryEngine._values_equal(item, other, dialect=dialect) for other in candidate)
                        for item in base
                    ):
                        return False
                return True
            if operator == "$slice":
                args = _require_expression_args(operator, spec, min_args=2, max_args=3)
                raw_value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if raw_value is None:
                    return None
                values = _require_array(operator, raw_value)
                return _slice_array(values, args, document, variables, dialect=dialect)
            if operator == "$isArray":
                args = _require_expression_args(
                    operator,
                    [spec] if not isinstance(spec, list) else spec,
                    min_args=1,
                    max_args=1,
                )
                value = evaluate_expression(document, args[0], variables, dialect=dialect)
                return isinstance(value, list)
            if operator == "$cmp":
                args = _require_expression_args(operator, spec, min_args=2, max_args=2)
                left = evaluate_expression(document, args[0], variables, dialect=dialect)
                right = evaluate_expression(document, args[1], variables, dialect=dialect)
                comparison = dialect.compare_values(left, right)
                if comparison < 0:
                    return -1
                if comparison > 0:
                    return 1
                return 0
            if operator == "$map":
                if not isinstance(spec, dict) or "input" not in spec or "in" not in spec:
                    raise OperationFailure("$map requires input and in")
                source = evaluate_expression(document, spec["input"], variables, dialect=dialect)
                if source is None:
                    return None
                source = _require_array(operator, source)
                alias = spec.get("as", "this")
                if not isinstance(alias, str):
                    raise OperationFailure("$map as must be a string")
                result = []
                for item in source:
                    scoped = dict(variables)
                    scoped_item = deepcopy(item)
                    scoped[alias] = scoped_item
                    if alias == "this":
                        scoped["this"] = scoped_item
                    else:
                        scoped.pop("this", None)
                    result.append(evaluate_expression(document, spec["in"], scoped, dialect=dialect))
                return result
            if operator == "$filter":
                if not isinstance(spec, dict) or "input" not in spec or "cond" not in spec:
                    raise OperationFailure("$filter requires input and cond")
                source = evaluate_expression(document, spec["input"], variables, dialect=dialect)
                if source is None:
                    return None
                source = _require_array(operator, source)
                alias = spec.get("as", "this")
                if not isinstance(alias, str):
                    raise OperationFailure("$filter as must be a string")
                result = []
                for item in source:
                    scoped = dict(variables)
                    scoped[alias] = item
                    scoped["this"] = item
                    if _expression_truthy(
                        evaluate_expression(document, spec["cond"], scoped, dialect=dialect),
                        dialect=dialect,
                    ):
                        result.append(deepcopy(item))
                return result
            if operator == "$reduce":
                if not isinstance(spec, dict) or not {"input", "initialValue", "in"} <= set(spec):
                    raise OperationFailure("$reduce requires input, initialValue and in")
                source = evaluate_expression(document, spec["input"], variables, dialect=dialect)
                if source is None:
                    return None
                source = _require_array(operator, source)
                accumulated = evaluate_expression(
                    document,
                    spec["initialValue"],
                    variables,
                    dialect=dialect,
                )
                for item in source:
                    scoped = dict(variables)
                    scoped["value"] = accumulated
                    scoped["this"] = item
                    accumulated = evaluate_expression(document, spec["in"], scoped, dialect=dialect)
                return accumulated
            if operator == "$objectToArray":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                raw_value = evaluate_expression(document, args[0], variables, dialect=dialect)
                if raw_value is None:
                    return None
                if not isinstance(raw_value, dict):
                    raise OperationFailure("$objectToArray requires a document input")
                return [{"k": key, "v": deepcopy(value)} for key, value in raw_value.items()]
            if operator == "$arrayToObject":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                raw_values = evaluate_expression(document, args[0], variables, dialect=dialect)
                if raw_values is None:
                    return None
                values = _require_array(operator, raw_values)
                result: dict[str, Any] = {}
                for item in values:
                    if isinstance(item, list) and len(item) == 2:
                        key, value = item
                    elif isinstance(item, dict) and set(item) == {"k", "v"}:
                        key, value = item["k"], item["v"]
                    else:
                        raise OperationFailure("$arrayToObject requires [key, value] pairs or {k, v} documents")
                    if not isinstance(key, str):
                        raise OperationFailure("$arrayToObject keys must be strings")
                    result[key] = deepcopy(value)
                return result
            if operator == "$zip":
                if not isinstance(spec, dict) or "inputs" not in spec:
                    raise OperationFailure("$zip requires inputs")
                raw_inputs = evaluate_expression(document, spec["inputs"], variables, dialect=dialect)
                if raw_inputs is None:
                    return None
                inputs = _require_array(operator, raw_inputs)
                arrays: list[list[Any]] = []
                for item in inputs:
                    resolved = evaluate_expression(document, item, variables, dialect=dialect) if not isinstance(item, list) else item
                    if resolved is None or resolved is _MISSING:
                        return None
                    arrays.append(_require_array(operator, resolved))
                use_longest = (
                    evaluate_expression(document, spec["useLongestLength"], variables, dialect=dialect)
                    if "useLongestLength" in spec
                    else False
                )
                if not isinstance(use_longest, bool):
                    raise OperationFailure("$zip useLongestLength must evaluate to a boolean")
                defaults = (
                    evaluate_expression(document, spec["defaults"], variables, dialect=dialect)
                    if "defaults" in spec
                    else None
                )
                if defaults is not None and not isinstance(defaults, list):
                    raise OperationFailure("$zip defaults must evaluate to an array")
                if defaults is not None and len(defaults) != len(arrays):
                    raise OperationFailure("$zip defaults length must match inputs length")
                target_length = max((len(array) for array in arrays), default=0) if use_longest else min((len(array) for array in arrays), default=0)
                result: list[list[Any]] = []
                for index in range(target_length):
                    row: list[Any] = []
                    for array_index, array in enumerate(arrays):
                        if index < len(array):
                            row.append(deepcopy(array[index]))
                        elif defaults is not None:
                            row.append(deepcopy(defaults[array_index]))
                        else:
                            row.append(None)
                    result.append(row)
                return result
            if operator == "$indexOfArray":
                args = _require_expression_args(operator, spec, min_args=2, max_args=4)
                values = evaluate_expression(document, args[0], variables, dialect=dialect)
                if values is None:
                    return None
                values = _require_array(operator, values)
                needle = evaluate_expression(document, args[1], variables, dialect=dialect)
                start = 0
                end = len(values)
                if len(args) >= 3:
                    start = evaluate_expression(document, args[2], variables, dialect=dialect)
                    if not isinstance(start, int) or isinstance(start, bool):
                        raise OperationFailure("$indexOfArray start must be an integer")
                if len(args) == 4:
                    end = evaluate_expression(document, args[3], variables, dialect=dialect)
                    if not isinstance(end, int) or isinstance(end, bool):
                        raise OperationFailure("$indexOfArray end must be an integer")
                for index, value in enumerate(values[max(start, 0):max(end, 0)], start=max(start, 0)):
                    if QueryEngine._values_equal(value, needle, dialect=dialect):
                        return index
                return -1
            if operator in {"$indexOfBytes", "$indexOfCP"}:
                args = _require_expression_args(operator, spec, min_args=2, max_args=4)
                source = evaluate_expression(document, args[0], variables, dialect=dialect)
                substring = evaluate_expression(document, args[1], variables, dialect=dialect)
                if source is None:
                    return None
                if not isinstance(source, str) or not isinstance(substring, str):
                    raise OperationFailure(f"{operator} requires string arguments")
                start = evaluate_expression(document, args[2], variables, dialect=dialect) if len(args) >= 3 else None
                end = evaluate_expression(document, args[3], variables, dialect=dialect) if len(args) == 4 else None
                if operator == "$indexOfBytes":
                    source_bytes = source.encode("utf-8")
                    substring_bytes = substring.encode("utf-8")
                    start_index, end_index = _normalize_index_bounds(operator, start, end, len(source_bytes))
                    if start_index > end_index:
                        return -1
                    found = source_bytes.find(substring_bytes, start_index, end_index)
                    return found
                start_index, end_index = _normalize_index_bounds(operator, start, end, len(source))
                if start_index > end_index:
                    return -1
                return source.find(substring, start_index, end_index)
            if operator == "$binarySize":
                args = _require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, min_args=1, max_args=1)
                value = _evaluate_expression_with_missing(document, args[0], variables, dialect=dialect)
                if value is _MISSING or value is None:
                    return None
                if isinstance(value, (bytes, bytearray)):
                    return len(value)
                if isinstance(value, uuid.UUID):
                    return len(value.bytes)
                raise OperationFailure("$binarySize requires a BinData argument")
            if operator in {"$regexMatch", "$regexFind", "$regexFindAll"}:
                if not isinstance(spec, dict) or "input" not in spec or "regex" not in spec:
                    raise OperationFailure(f"{operator} requires input and regex")
                input_value = _evaluate_expression_with_missing(document, spec["input"], variables, dialect=dialect)
                if input_value is _MISSING or input_value is None:
                    if operator == "$regexMatch":
                        return False
                    if operator == "$regexFindAll":
                        return []
                    return None
                if not isinstance(input_value, str):
                    raise OperationFailure(f"{operator} input must resolve to a string")
                regex_value = evaluate_expression(document, spec["regex"], variables, dialect=dialect)
                options_value = (
                    evaluate_expression(document, spec["options"], variables, dialect=dialect)
                    if "options" in spec
                    else None
                )
                compiled = _compile_aggregation_regex(regex_value, options_value, operator=operator)
                if operator == "$regexMatch":
                    return compiled.search(input_value) is not None
                if operator == "$regexFind":
                    match = compiled.search(input_value)
                    if match is None:
                        return None
                    return _build_regex_match_result(match)
                return [_build_regex_match_result(match) for match in compiled.finditer(input_value)]
            if operator == "$sortArray":
                if not isinstance(spec, dict) or "input" not in spec or "sortBy" not in spec:
                    raise OperationFailure("$sortArray requires input and sortBy")
                input_value = evaluate_expression(document, spec["input"], variables, dialect=dialect)
                if input_value is None:
                    return None
                values = deepcopy(_require_array(operator, input_value))
                sort_by = _normalize_sort_array_spec(spec["sortBy"])
                if isinstance(sort_by, int):
                    return sorted(
                        values,
                        key=cmp_to_key(dialect.compare_values),
                        reverse=sort_by == -1,
                    )
                return sort_documents(values, sort_by, dialect=dialect)
            if operator == "$dateTrunc":
                if not isinstance(spec, dict) or "date" not in spec or "unit" not in spec:
                    raise OperationFailure("$dateTrunc requires date and unit")
                value = evaluate_expression(document, spec["date"], variables, dialect=dialect)
                if value is None:
                    return None
                if not isinstance(value, datetime.datetime):
                    raise OperationFailure("$dateTrunc requires a datetime value")
                unit = evaluate_expression(document, spec["unit"], variables, dialect=dialect)
                if not isinstance(unit, str):
                    raise OperationFailure("$dateTrunc unit must be a string")
                bin_size = (
                    evaluate_expression(document, spec["binSize"], variables, dialect=dialect)
                    if "binSize" in spec
                    else 1
                )
                if not isinstance(bin_size, int) or isinstance(bin_size, bool):
                    raise OperationFailure("$dateTrunc binSize must be an integer")
                timezone = (
                    evaluate_expression(document, spec["timezone"], variables, dialect=dialect)
                    if "timezone" in spec
                    else None
                )
                if timezone not in (None, "UTC"):
                    raise OperationFailure("$dateTrunc timezone is not supported")
                start_of_week = (
                    evaluate_expression(document, spec["startOfWeek"], variables, dialect=dialect)
                    if "startOfWeek" in spec
                    else "sunday"
                )
                if not isinstance(start_of_week, str):
                    raise OperationFailure("$dateTrunc startOfWeek must be a string")
                return _truncate_datetime(value, unit, bin_size, start_of_week)
            if operator in {"$dateAdd", "$dateSubtract"}:
                if not isinstance(spec, dict) or not {"startDate", "unit", "amount"} <= set(spec):
                    raise OperationFailure(f"{operator} requires startDate, unit and amount")
                value = evaluate_expression(document, spec["startDate"], variables, dialect=dialect)
                if value is None:
                    return None
                if not isinstance(value, datetime.datetime):
                    raise OperationFailure(f"{operator} requires a datetime startDate")
                unit = _require_date_unit(
                    operator,
                    evaluate_expression(document, spec["unit"], variables, dialect=dialect),
                )
                amount = evaluate_expression(document, spec["amount"], variables, dialect=dialect)
                if amount is None:
                    return None
                if not isinstance(amount, int) or isinstance(amount, bool):
                    raise OperationFailure(f"{operator} amount must evaluate to an integer")
                timezone = _resolve_timezone(
                    operator,
                    evaluate_expression(document, spec["timezone"], variables, dialect=dialect)
                    if "timezone" in spec
                    else None,
                )
                localized = _localize_datetime(value, timezone)
                signed_amount = amount if operator == "$dateAdd" else -amount
                updated = _add_date_unit(localized, unit, signed_amount)
                return _restore_datetime_timezone(updated, value)
            if operator == "$dateToString":
                if not isinstance(spec, dict) or "date" not in spec:
                    raise OperationFailure("$dateToString requires date")
                value = _evaluate_expression_with_missing(document, spec["date"], variables, dialect=dialect)
                if value is _MISSING or value is None:
                    return evaluate_expression(document, spec["onNull"], variables, dialect=dialect) if "onNull" in spec else None
                if not isinstance(value, datetime.datetime):
                    raise OperationFailure("$dateToString requires a date input")
                timezone = _resolve_timezone(
                    operator,
                    evaluate_expression(document, spec["timezone"], variables, dialect=dialect)
                    if "timezone" in spec
                    else None,
                )
                fmt = (
                    evaluate_expression(document, spec["format"], variables, dialect=dialect)
                    if "format" in spec
                    else _default_date_to_string_format(timezone)
                )
                if not isinstance(fmt, str):
                    raise OperationFailure("$dateToString format must evaluate to a string")
                localized = _localize_datetime(value, timezone)
                return _mongo_format_datetime(localized, fmt)
            if operator == "$dateToParts":
                if not isinstance(spec, dict) or "date" not in spec:
                    raise OperationFailure("$dateToParts requires date")
                value = _evaluate_expression_with_missing(document, spec["date"], variables, dialect=dialect)
                if value is _MISSING or value is None:
                    return None
                if not isinstance(value, datetime.datetime):
                    raise OperationFailure("$dateToParts requires a date input")
                timezone = _resolve_timezone(
                    operator,
                    evaluate_expression(document, spec["timezone"], variables, dialect=dialect)
                    if "timezone" in spec
                    else None,
                )
                localized = _localize_datetime(value, timezone)
                iso8601 = (
                    evaluate_expression(document, spec["iso8601"], variables, dialect=dialect)
                    if "iso8601" in spec
                    else False
                )
                if not isinstance(iso8601, bool):
                    raise OperationFailure("$dateToParts iso8601 must evaluate to a boolean")
                if iso8601:
                    iso_parts = localized.isocalendar()
                    return {
                        "isoWeekYear": iso_parts.year,
                        "isoWeek": iso_parts.week,
                        "isoDayOfWeek": iso_parts.weekday,
                        "hour": localized.hour,
                        "minute": localized.minute,
                        "second": localized.second,
                        "millisecond": localized.microsecond // 1000,
                    }
                return {
                    "year": localized.year,
                    "month": localized.month,
                    "day": localized.day,
                    "hour": localized.hour,
                    "minute": localized.minute,
                    "second": localized.second,
                    "millisecond": localized.microsecond // 1000,
                }
            if operator == "$dateFromString":
                if not isinstance(spec, dict) or "dateString" not in spec:
                    raise OperationFailure("$dateFromString requires dateString")
                raw_value = _evaluate_expression_with_missing(document, spec["dateString"], variables, dialect=dialect)
                if raw_value is _MISSING or raw_value is None:
                    return evaluate_expression(document, spec["onNull"], variables, dialect=dialect) if "onNull" in spec else None
                if not isinstance(raw_value, str):
                    raise OperationFailure("$dateFromString dateString must evaluate to a string")
                timezone = _resolve_timezone(
                    operator,
                    evaluate_expression(document, spec["timezone"], variables, dialect=dialect)
                    if "timezone" in spec
                    else None,
                )
                try:
                    if "format" in spec:
                        fmt = evaluate_expression(document, spec["format"], variables, dialect=dialect)
                        if not isinstance(fmt, str):
                            raise OperationFailure("$dateFromString format must evaluate to a string")
                        parsed = datetime.datetime.strptime(raw_value, _python_strptime_format(operator, fmt))
                    else:
                        parsed = datetime.datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
                except OperationFailure:
                    raise
                except Exception as exc:
                    if "onError" in spec:
                        return evaluate_expression(document, spec["onError"], variables, dialect=dialect)
                    raise OperationFailure("$dateFromString could not parse dateString") from exc
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone)
                return _to_utc_naive(parsed)
            if operator == "$dateFromParts":
                if not isinstance(spec, dict):
                    raise OperationFailure("$dateFromParts requires a document specification")
                return _build_date_from_parts(operator, spec, document, variables, dialect=dialect)
            if operator in {
                "$year",
                "$month",
                "$dayOfMonth",
                "$dayOfWeek",
                "$dayOfYear",
                "$hour",
                "$minute",
                "$second",
                "$millisecond",
                "$isoDayOfWeek",
            }:
                localized = _evaluate_localized_date_operand(
                    operator,
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                )
                if localized is None:
                    return None
                if operator == "$year":
                    return localized.year
                if operator == "$month":
                    return localized.month
                if operator == "$dayOfMonth":
                    return localized.day
                if operator == "$dayOfWeek":
                    return ((localized.weekday() + 1) % 7) + 1
                if operator == "$dayOfYear":
                    return int(localized.strftime("%j"))
                if operator == "$hour":
                    return localized.hour
                if operator == "$minute":
                    return localized.minute
                if operator == "$second":
                    return localized.second
                if operator == "$millisecond":
                    return localized.microsecond // 1000
                return localized.isoweekday()
            if operator in {"$week", "$isoWeek", "$isoWeekYear"}:
                localized = _evaluate_localized_date_operand(
                    operator,
                    document,
                    spec,
                    variables,
                    dialect=dialect,
                )
                if localized is None:
                    return None
                if operator == "$week":
                    return int(localized.strftime("%U"))
                iso_parts = localized.isocalendar()
                if operator == "$isoWeek":
                    return iso_parts.week
                return iso_parts.year
            if operator == "$dateDiff":
                if not isinstance(spec, dict) or not {"startDate", "endDate", "unit"} <= set(spec):
                    raise OperationFailure("$dateDiff requires startDate, endDate and unit")
                start_date = evaluate_expression(document, spec["startDate"], variables, dialect=dialect)
                end_date = evaluate_expression(document, spec["endDate"], variables, dialect=dialect)
                if start_date is None or end_date is None:
                    return None
                if not isinstance(start_date, datetime.datetime) or not isinstance(end_date, datetime.datetime):
                    raise OperationFailure("$dateDiff requires datetime startDate and endDate")
                unit = _require_date_unit(
                    operator,
                    evaluate_expression(document, spec["unit"], variables, dialect=dialect),
                )
                timezone = _resolve_timezone(
                    operator,
                    evaluate_expression(document, spec["timezone"], variables, dialect=dialect)
                    if "timezone" in spec
                    else None,
                )
                start_of_week = (
                    evaluate_expression(document, spec["startOfWeek"], variables, dialect=dialect)
                    if "startOfWeek" in spec
                    else "sunday"
                )
                if not isinstance(start_of_week, str):
                    raise OperationFailure("$dateDiff startOfWeek must be a string")
                localized_start = _localize_datetime(start_date, timezone)
                localized_end = _localize_datetime(end_date, timezone)
                return _date_diff_units(localized_start, localized_end, unit, start_of_week=start_of_week)
            if operator == "$mergeObjects":
                args = _require_expression_args(operator, spec, min_args=1)
                merged: dict[str, Any] = {}
                for item in args:
                    value = evaluate_expression(document, item, variables, dialect=dialect)
                    if value is None:
                        continue
                    if not isinstance(value, dict):
                        raise OperationFailure("$mergeObjects requires document operands")
                    merged.update(deepcopy(value))
                return merged
            if operator == "$getField":
                if isinstance(spec, str):
                    field_name = spec
                    source = document
                elif isinstance(spec, dict):
                    if "field" not in spec:
                        raise OperationFailure("$getField requires field")
                    field_name = evaluate_expression(document, spec["field"], variables, dialect=dialect)
                    source = evaluate_expression(
                        document,
                        spec.get("input", "$$CURRENT"),
                        variables,
                        dialect=dialect,
                    )
                else:
                    raise OperationFailure("$getField requires a string or document specification")
                if field_name is None:
                    return None
                if not isinstance(field_name, str):
                    raise OperationFailure("$getField field must evaluate to a string")
                if not isinstance(source, dict):
                    return None
                return deepcopy(source.get(field_name))
            raise OperationFailure(f"Unsupported aggregation expression: {operator}")

    return {
        key: evaluate_expression(document, value, variables, dialect=dialect)
        for key, value in expression.items()
    }


def _apply_match(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$match requires a document specification")

    def _match_spec(document: Document, match_spec: dict[str, Any]) -> bool:
        expr = match_spec.get("$expr")
        filter_spec = {key: value for key, value in match_spec.items() if key != "$expr"}
        if "$and" in filter_spec:
            clauses = filter_spec.pop("$and")
            if not isinstance(clauses, list):
                raise OperationFailure("$and in $match requires a list")
            if not all(_match_spec(document, clause) for clause in clauses):
                return False
        if "$or" in filter_spec:
            clauses = filter_spec.pop("$or")
            if not isinstance(clauses, list):
                raise OperationFailure("$or in $match requires a list")
            if not any(_match_spec(document, clause) for clause in clauses):
                return False
        if "$nor" in filter_spec:
            clauses = filter_spec.pop("$nor")
            if not isinstance(clauses, list):
                raise OperationFailure("$nor in $match requires a list")
            if any(_match_spec(document, clause) for clause in clauses):
                return False
        if filter_spec:
            plan = compile_filter(filter_spec, dialect=dialect)
            if not QueryEngine.match_plan(document, plan, dialect=dialect):
                return False
        if expr is not None and not _expression_truthy(
            evaluate_expression(document, expr, variables, dialect=dialect),
            dialect=dialect,
        ):
            return False
        return True

    if _match_spec_contains_expr(spec):
        return [document for document in documents if _match_spec(document, spec)]

    plan = compile_filter(spec, dialect=dialect) if spec else None
    result: list[Document] = []
    for document in documents:
        if plan is not None and not QueryEngine.match_plan(document, plan, dialect=dialect):
            continue
        result.append(document)
    return result


def _apply_add_fields(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$addFields requires a document specification")
    result: list[Document] = []
    for document in documents:
        enriched = deepcopy(document)
        evaluated = {
            path: evaluate_expression(document, expression, variables, dialect=dialect)
            for path, expression in spec.items()
        }
        for path, expression in spec.items():
            if not isinstance(path, str):
                raise OperationFailure("$addFields field names must be strings")
            if evaluated[path] is _REMOVE:
                delete_document_value(enriched, path)
                continue
            set_document_value(enriched, path, evaluated[path])
        result.append(enriched)
    return result


def _apply_unset(
    documents: list[Document],
    spec: object,
) -> list[Document]:
    fields = _require_unset_spec(spec)
    result: list[Document] = []
    for document in documents:
        trimmed = deepcopy(document)
        for field in fields:
            delete_document_value(trimmed, field)
        result.append(trimmed)
    return result


def _apply_sample(documents: list[Document], spec: object) -> list[Document]:
    size = _require_sample_spec(spec)
    if size == 0:
        return []
    sample_size = min(size, len(documents))
    return random.sample(documents, sample_size)


def _apply_project(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    projection = _require_projection_for_dialect(spec, dialect=dialect)
    computed_fields = {
        key: value
        for key, value in projection.items()
        if _projection_flag(value, dialect=dialect) is None
    }
    if not computed_fields:
        return [
            apply_projection(document, projection, dialect=dialect)
            for document in documents
        ]

    include_fields = {
        key: _projection_flag(value, dialect=dialect)
        for key, value in projection.items()
        if _projection_flag(value, dialect=dialect) is not None
    }
    include_id = include_fields.get("_id", 1) != 0
    result: list[Document] = []
    for document in documents:
        projected: Document = {}
        include_mode = any(value == 1 for key, value in include_fields.items() if key != "_id")
        if include_mode:
            projected = apply_projection(document, include_fields, dialect=dialect)
        elif include_id and "_id" in document:
            projected["_id"] = deepcopy(document["_id"])
        for path, expression in computed_fields.items():
            value = evaluate_expression(document, expression, variables, dialect=dialect)
            if value is _REMOVE:
                delete_document_value(projected, path)
                continue
            set_document_value(projected, path, value)
        result.append(projected)
    return result


def _apply_group(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "_id" not in spec:
        raise OperationFailure("$group requires a document specification with _id")

    accumulator_specs = {key: value for key, value in spec.items() if key != "_id"}
    _initialize_accumulators(
        accumulator_specs,
        dialect=dialect,
        support_checker=dialect.supports_group_accumulator,
        unsupported_message="Unsupported $group accumulator",
    )
    groups: dict[Any, dict[object, Any]] = {}

    for document in documents:
        group_id = evaluate_expression(document, spec["_id"], variables, dialect=dialect)
        group_key = _aggregation_key(group_id)
        if group_key not in groups:
            groups[group_key] = {"_id": deepcopy(group_id), **_initialize_accumulators(
                accumulator_specs,
                dialect=dialect,
                support_checker=dialect.supports_group_accumulator,
                unsupported_message="Unsupported $group accumulator",
            )}

        bucket = groups[group_key]
        _apply_accumulators(
            bucket,
            accumulator_specs,
            document,
            variables,
            dialect=dialect,
        )

    result: list[Document] = []
    for bucket in groups.values():
        result.append(_finalize_accumulators(bucket))
    return result


def _initialize_accumulators(
    accumulator_specs: dict[str, object] | None,
    *,
    default_sum: bool = False,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    support_checker: Callable[[str], bool] | None = None,
    unsupported_message: str = "Unsupported accumulator",
) -> dict[str, Any]:
    initialized: dict[object, Any] = {_ACCUMULATOR_FLAGS_KEY: {}}
    flags = _accumulator_flags(initialized)
    specs = {"count": {"$sum": 1}} if accumulator_specs is None and default_sum else (accumulator_specs or {})
    if support_checker is None:
        support_checker = dialect.supports_group_accumulator
    for field, accumulator in specs.items():
        if not isinstance(accumulator, dict) or len(accumulator) != 1:
            raise OperationFailure("Accumulator must be a single-key document")
        operator, _ = next(iter(accumulator.items()))
        if not support_checker(operator):
            raise OperationFailure(f"{unsupported_message}: {operator}")
        _validate_accumulator_expression(operator, accumulator[operator])
        if operator in {"$sum", "$count"}:
            initialized[field] = 0
        elif operator in {"$min", "$max", "$first", "$last"}:
            initialized[field] = None
            flags[field] = False
        elif operator == "$avg":
            initialized[field] = _AverageAccumulator()
        elif operator == "$stdDevPop":
            initialized[field] = _StdDevAccumulator(population=True)
        elif operator == "$stdDevSamp":
            initialized[field] = _StdDevAccumulator(population=False)
        elif operator in {"$push", "$addToSet"}:
            initialized[field] = []
        elif operator == "$mergeObjects":
            initialized[field] = {}
        else:
            raise OperationFailure(f"{unsupported_message}: {operator}")
    return initialized  # type: ignore[return-value]


def _apply_accumulators(
    bucket: dict[str, Any],
    accumulator_specs: dict[str, object] | None,
    document: Document,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> None:
    specs = {"count": {"$sum": 1}} if accumulator_specs is None else accumulator_specs
    flags = _accumulator_flags(bucket)
    for field, accumulator in specs.items():
        operator, expression = next(iter(accumulator.items()))
        value = None if operator == "$count" else evaluate_expression(document, expression, variables, dialect=dialect)
        if operator in {"$sum", "$count"}:
            if operator == "$count":
                bucket[field] += 1
                continue
            if value is None:
                continue
            numeric_value = _sum_accumulator_operand(value)
            if numeric_value is None:
                continue
            bucket[field] += numeric_value
        elif operator == "$min":
            if value is None:
                continue
            if not flags[field] or dialect.compare_values(value, bucket[field]) < 0:
                bucket[field] = deepcopy(value)
                flags[field] = True
        elif operator == "$max":
            if value is None:
                continue
            if not flags[field] or dialect.compare_values(value, bucket[field]) > 0:
                bucket[field] = deepcopy(value)
                flags[field] = True
        elif operator == "$avg":
            if value is None:
                continue
            if not _is_numeric(value):
                continue
            bucket[field].total += value
            bucket[field].count += 1
        elif operator in {"$stdDevPop", "$stdDevSamp"}:
            operand = _stddev_accumulator_operand(value)
            if operand is None:
                continue
            if not math.isfinite(operand):
                bucket[field].invalid = True
                continue
            bucket[field].total += operand
            bucket[field].sum_of_squares += operand * operand
            bucket[field].count += 1
        elif operator == "$push":
            bucket[field].append(deepcopy(value))
        elif operator == "$addToSet":
            _append_unique_values(
                bucket[field],
                [value],
                dialect=dialect,
            )
        elif operator == "$mergeObjects":
            if value is None:
                continue
            if not isinstance(value, dict):
                raise OperationFailure("$mergeObjects accumulator requires document operands")
            bucket[field].update(deepcopy(value))
        elif operator == "$first":
            if not flags[field]:
                bucket[field] = deepcopy(value)
                flags[field] = True
        elif operator == "$last":
            bucket[field] = deepcopy(value)
            flags[field] = True


def _finalize_accumulators(bucket: dict[str, Any]) -> Document:
    document: Document = {}
    for field, value in bucket.items():
        if field == _ACCUMULATOR_FLAGS_KEY:
            continue
        if isinstance(value, _AverageAccumulator):
            document[field] = None if value.count == 0 else value.total / value.count
        elif isinstance(value, _StdDevAccumulator):
            if value.invalid or value.count == 0:
                document[field] = None
            elif value.population:
                mean = value.total / value.count
                variance = max((value.sum_of_squares / value.count) - (mean * mean), 0.0)
                document[field] = math.sqrt(variance)
            elif value.count < 2:
                document[field] = None
            else:
                variance = max(
                    (value.sum_of_squares - ((value.total * value.total) / value.count))
                    / (value.count - 1),
                    0.0,
                )
                document[field] = math.sqrt(variance)
        else:
            document[field] = value
    return document


def _apply_bucket(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "groupBy" not in spec or "boundaries" not in spec:
        raise OperationFailure("$bucket requires groupBy and boundaries")
    boundaries = spec["boundaries"]
    if not isinstance(boundaries, list) or len(boundaries) < 2:
        raise OperationFailure("$bucket boundaries must be a list with at least two values")
    for index in range(len(boundaries) - 1):
        if dialect.compare_values(boundaries[index], boundaries[index + 1]) >= 0:
            raise OperationFailure("$bucket boundaries must be strictly increasing")

    output = spec.get("output")
    if output is not None and not isinstance(output, dict):
        raise OperationFailure("$bucket output must be a document")

    default_bucket = spec.get("default")
    buckets: list[dict[str, Any]] = []
    for lower in boundaries[:-1]:
        bucket = {"_id": deepcopy(lower)}
        bucket.update(
            _initialize_accumulators(
                output,
                default_sum=output is None,
                dialect=dialect,
            )
        )
        buckets.append(bucket)

    default_state: dict[str, Any] | None = None
    if "default" in spec:
        default_state = {"_id": deepcopy(default_bucket)}
        default_state.update(
            _initialize_accumulators(
                output,
                default_sum=output is None,
                dialect=dialect,
            )
        )

    for document in documents:
        value = evaluate_expression(document, spec["groupBy"], variables, dialect=dialect)
        matched = False
        for index, lower in enumerate(boundaries[:-1]):
            upper = boundaries[index + 1]
            if dialect.compare_values(value, lower) >= 0 and dialect.compare_values(value, upper) < 0:
                _apply_accumulators(
                    buckets[index],
                    output,
                    document,
                    variables,
                    dialect=dialect,
                )
                matched = True
                break
        if matched:
            continue
        if default_state is not None:
            _apply_accumulators(
                default_state,
                output,
                document,
                variables,
                dialect=dialect,
            )
            continue
        raise OperationFailure("$bucket found a document outside of the specified boundaries")

    result = [_finalize_accumulators(bucket) for bucket in buckets]
    if default_state is not None:
        result.append(_finalize_accumulators(default_state))
    return result


def _apply_bucket_auto(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "groupBy" not in spec or "buckets" not in spec:
        raise OperationFailure("$bucketAuto requires groupBy and buckets")
    buckets = spec["buckets"]
    if not isinstance(buckets, int) or isinstance(buckets, bool) or buckets <= 0:
        raise OperationFailure("$bucketAuto buckets must be a positive integer")
    if "granularity" in spec:
        raise OperationFailure("$bucketAuto granularity is not supported")

    output = spec.get("output")
    if output is not None and not isinstance(output, dict):
        raise OperationFailure("$bucketAuto output must be a document")

    evaluated = [
        (evaluate_expression(document, spec["groupBy"], variables, dialect=dialect), document)
        for document in documents
    ]
    if not evaluated:
        return []

    evaluated.sort(key=lambda item: cmp_to_key(dialect.compare_values)(item[0]))
    bucket_count = min(buckets, len(evaluated))
    base = len(evaluated) // bucket_count
    remainder = len(evaluated) % bucket_count
    sizes = [base + (1 if index < remainder else 0) for index in range(bucket_count)]

    result: list[Document] = []
    start = 0
    for size_index, size in enumerate(sizes):
        chunk = evaluated[start:start + size]
        start += size
        lower = deepcopy(chunk[0][0])
        if size_index + 1 < len(sizes):
            upper = deepcopy(evaluated[start][0])
        else:
            upper = deepcopy(chunk[-1][0])

        bucket = {"_id": {"min": lower, "max": upper}}
        bucket.update(
            _initialize_accumulators(
                output,
                default_sum=output is None,
                dialect=dialect,
            )
        )
        for _, document in chunk:
            _apply_accumulators(bucket, output, document, variables, dialect=dialect)
        result.append(_finalize_accumulators(bucket))
    return result


def _require_window_output_spec(spec: object) -> tuple[str, object, dict[str, object] | None]:
    if not isinstance(spec, dict):
        raise OperationFailure("$setWindowFields output entries must be documents")
    window = spec.get("window")
    if window is not None and not isinstance(window, dict):
        raise OperationFailure("$setWindowFields window must be a document")
    operator_items = [(key, value) for key, value in spec.items() if key != "window"]
    if len(operator_items) != 1:
        raise OperationFailure("$setWindowFields output entries require exactly one accumulator")
    operator, expression = operator_items[0]
    return operator, expression, window


def _resolve_window_index(bound: object, current_index: int, last_index: int, *, lower: bool) -> int:
    if bound == "unbounded":
        return 0 if lower else last_index
    if bound == "current":
        return current_index
    if isinstance(bound, int) and not isinstance(bound, bool):
        return current_index + bound
    raise OperationFailure("$setWindowFields documents bounds must be integers, 'current' or 'unbounded'")


def _require_range_bound(bound: object) -> int | float | str:
    if bound in {"current", "unbounded"}:
        return bound
    if isinstance(bound, (int, float)) and not isinstance(bound, bool):
        return bound
    raise OperationFailure("$setWindowFields range bounds must be numeric, 'current' or 'unbounded'")


def _resolve_range_value(current_value: object, bound: int | float | str, *, lower: bool) -> float:
    if bound == "unbounded":
        return float("-inf") if lower else float("inf")
    if bound == "current":
        return float(current_value)  # type: ignore[arg-type]
    return float(current_value) + float(bound)  # type: ignore[arg-type]


def _apply_set_window_fields(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "output" not in spec:
        raise OperationFailure("$setWindowFields requires output")
    output = spec["output"]
    if not isinstance(output, dict):
        raise OperationFailure("$setWindowFields output must be a document")

    sort_spec = None
    if "sortBy" in spec:
        sort_spec = _require_sort(spec["sortBy"])

    partitions: dict[Any, list[Document]] = {}
    for document in documents:
        partition_key = (
            evaluate_expression(document, spec["partitionBy"], variables, dialect=dialect)
            if "partitionBy" in spec
            else None
        )
        partitions.setdefault(_aggregation_key(partition_key), []).append(deepcopy(document))

    result: list[Document] = []
    for partition_documents in partitions.values():
        ordered = (
            sort_documents(partition_documents, sort_spec, dialect=dialect)
            if sort_spec is not None
            else partition_documents
        )
        last_index = len(ordered) - 1
        for current_index, document in enumerate(ordered):
            enriched = deepcopy(document)
            for field, field_spec in output.items():
                if not isinstance(field, str):
                    raise OperationFailure("$setWindowFields output field names must be strings")
                operator, expression, window = _require_window_output_spec(field_spec)
                if not dialect.supports_window_accumulator(operator):
                    raise OperationFailure(f"Unsupported $setWindowFields accumulator: {operator}")
                if window is None:
                    start = 0
                    end = last_index
                    window_documents = ordered[start:end + 1]
                else:
                    has_documents = "documents" in window
                    has_range = "range" in window
                    if has_documents == has_range:
                        raise OperationFailure("$setWindowFields window must contain exactly one of documents or range")
                    if has_documents:
                        documents_window = window.get("documents")
                        if not isinstance(documents_window, list) or len(documents_window) != 2:
                            raise OperationFailure("$setWindowFields requires a two-item documents window")
                        start = _resolve_window_index(documents_window[0], current_index, last_index, lower=True)
                        end = _resolve_window_index(documents_window[1], current_index, last_index, lower=False)
                        start = max(0, start)
                        end = min(last_index, end)
                        window_documents = ordered[start:end + 1] if start <= end else []
                    else:
                        if sort_spec is None or len(sort_spec) != 1:
                            raise OperationFailure("$setWindowFields range windows require exactly one sort field")
                        range_window = window.get("range")
                        if not isinstance(range_window, list) or len(range_window) != 2:
                            raise OperationFailure("$setWindowFields requires a two-item range window")
                        lower_bound = _require_range_bound(range_window[0])
                        upper_bound = _require_range_bound(range_window[1])
                        sort_field = sort_spec[0][0]
                        found_current, current_value = get_document_value(document, sort_field)
                        if not found_current or not isinstance(current_value, (int, float)) or isinstance(current_value, bool):
                            raise OperationFailure("$setWindowFields numeric range windows require numeric sort values")
                        lower_value = _resolve_range_value(current_value, lower_bound, lower=True)
                        upper_value = _resolve_range_value(current_value, upper_bound, lower=False)
                        window_documents = []
                        for candidate in ordered:
                            found_candidate, candidate_value = get_document_value(candidate, sort_field)
                            if not found_candidate or not isinstance(candidate_value, (int, float)) or isinstance(candidate_value, bool):
                                raise OperationFailure("$setWindowFields numeric range windows require numeric sort values")
                            numeric_candidate = float(candidate_value)
                            if lower_value <= numeric_candidate <= upper_value:
                                window_documents.append(candidate)
                state = _initialize_accumulators(
                    {field: {operator: expression}},
                    dialect=dialect,
                    support_checker=dialect.supports_window_accumulator,
                    unsupported_message="Unsupported $setWindowFields accumulator",
                )
                for window_document in window_documents:
                    _apply_accumulators(
                        state,
                        {field: {operator: expression}},
                        window_document,
                        variables,
                        dialect=dialect,
                    )
                set_document_value(enriched, field, _finalize_accumulators(state)[field])
            result.append(enriched)
    return result


def _apply_lookup(
    documents: list[Document],
    spec: object,
    collection_resolver,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    lookup = _require_lookup_spec(spec)
    if collection_resolver is None:
        raise OperationFailure("$lookup requires collection resolver support")

    resolved_foreign_documents = collection_resolver(lookup["from"]) or []
    foreign_documents = [deepcopy(document) for document in resolved_foreign_documents]
    result: list[Document] = []
    for document in documents:
        candidate_documents = foreign_documents
        if "localField" in lookup and "foreignField" in lookup:
            local_values = QueryEngine.extract_values(document, lookup["localField"])
            candidate_documents = [
                deepcopy(foreign_document)
                for foreign_document in foreign_documents
                if _lookup_matches(
                    local_values,
                    QueryEngine.extract_values(foreign_document, lookup["foreignField"]),
                    dialect=dialect,
                )
            ]
        if "pipeline" in lookup:
            scoped = dict(variables or {})
            for name, expression in lookup["let"].items():
                scoped[name] = evaluate_expression(
                    document,
                    expression,
                    variables,
                    dialect=dialect,
                )
            matches = apply_pipeline(
                candidate_documents,
                lookup["pipeline"],
                collection_resolver=collection_resolver,
                variables=scoped,
                dialect=dialect,
            )
        else:
            matches = candidate_documents
        joined = deepcopy(document)
        joined[lookup["as"]] = deepcopy(matches)
        result.append(joined)
    return result


def _apply_union_with(
    documents: list[Document],
    spec: object,
    collection_resolver,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    union_with = _require_union_with_spec(spec)
    if collection_resolver is None:
        raise OperationFailure("$unionWith requires collection resolver support")

    resolver_key = (
        union_with["coll"]
        if union_with["coll"] is not None
        else _CURRENT_COLLECTION_RESOLVER_KEY
    )
    resolved_foreign_documents = collection_resolver(resolver_key)
    if resolved_foreign_documents is None:
        resolved_foreign_documents = [deepcopy(document) for document in documents]
    foreign_documents = [deepcopy(document) for document in resolved_foreign_documents]
    if union_with["pipeline"]:
        foreign_documents = apply_pipeline(
            foreign_documents,
            union_with["pipeline"],
            collection_resolver=collection_resolver,
            variables=variables,
            dialect=dialect,
        )
    return [deepcopy(document) for document in documents] + foreign_documents


def _apply_replace_root(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "newRoot" not in spec:
        raise OperationFailure("$replaceRoot requires a document with newRoot")
    result: list[Document] = []
    for document in documents:
        new_root = evaluate_expression(document, spec["newRoot"], variables, dialect=dialect)
        if not isinstance(new_root, dict):
            raise OperationFailure("$replaceRoot newRoot must evaluate to a document")
        result.append(deepcopy(new_root))
    return result


def _apply_facet(
    documents: list[Document],
    spec: object,
    collection_resolver,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$facet requires a document specification")
    faceted: Document = {}
    for field, pipeline in spec.items():
        if not isinstance(field, str):
            raise OperationFailure("$facet field names must be strings")
        faceted[field] = apply_pipeline(
            documents,
            _require_pipeline_spec("$facet", pipeline),
            collection_resolver=collection_resolver,
            variables=variables,
            dialect=dialect,
        )
    return [faceted]


def _apply_count(documents: list[Document], spec: object) -> list[Document]:
    if (
        not isinstance(spec, str)
        or not spec
        or spec.startswith("$")
        or "." in spec
    ):
        raise OperationFailure("$count requires a non-empty field name")
    return [{spec: len(documents)}]


def _apply_sort_by_count(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    grouped = _apply_group(
        documents,
        {"_id": spec, "count": {"$sum": 1}},
        variables,
        dialect=dialect,
    )
    return sort_documents(grouped, [("count", -1), ("_id", 1)], dialect=dialect)


def split_pushdown_pipeline(
    pipeline: Pipeline,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> AggregationPushdown:
    filter_spec: Filter = {}
    projection: Projection | None = None
    sort: SortSpec | None = None
    skip = 0
    limit: int | None = None
    phase = "match"

    for index, stage in enumerate(pipeline):
        operator, spec = _require_stage(stage)

        if operator == "$match" and phase == "match":
            if not isinstance(spec, dict):
                raise OperationFailure("$match requires a document specification")
            if _match_spec_contains_expr(spec):
                return AggregationPushdown(
                    filter_spec=filter_spec,
                    projection=projection,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                    remaining_pipeline=pipeline[index:],
                )
            filter_spec = _merge_match_filters(filter_spec, spec)
            continue

        if operator == "$sort" and phase in {"match", "sort"} and sort is None:
            sort = _require_sort(spec)
            phase = "sort"
            continue

        if operator == "$skip" and phase in {"match", "sort", "skip"}:
            skip += _require_non_negative_int("$skip", spec)
            phase = "skip"
            continue

        if operator == "$limit" and phase in {"match", "sort", "skip", "limit"}:
            value = _require_non_negative_int("$limit", spec)
            limit = value if limit is None else min(limit, value)
            phase = "limit"
            continue

        if operator == "$project" and phase in {"match", "sort", "skip", "limit"} and projection is None:
            checked_projection = _require_projection_for_dialect(spec, dialect=dialect)
            if not _is_simple_projection_for_dialect(checked_projection, dialect=dialect):
                return AggregationPushdown(
                    filter_spec=filter_spec,
                    projection=projection,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                    remaining_pipeline=pipeline[index:],
                )
            projection = checked_projection
            phase = "project"
            continue

        return AggregationPushdown(
            filter_spec=filter_spec,
            projection=projection,
            sort=sort,
            skip=skip,
            limit=limit,
            remaining_pipeline=pipeline[index:],
        )

    return AggregationPushdown(
        filter_spec=filter_spec,
        projection=projection,
        sort=sort,
        skip=skip,
        limit=limit,
        remaining_pipeline=[],
    )


def apply_pipeline(
    documents: Iterable[Document],
    pipeline: Pipeline,
    *,
    collection_resolver=None,
    variables: dict[str, Any] | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    result = [deepcopy(document) for document in documents]
    for stage in pipeline:
        operator, spec = _require_stage(stage)
        if not dialect.supports_aggregation_stage(operator):
            raise OperationFailure(f"Unsupported aggregation stage: {operator}")
        if operator == "$match":
            result = _apply_match(result, spec, variables, dialect=dialect)
            continue
        if operator == "$project":
            result = _apply_project(result, spec, variables, dialect=dialect)
            continue
        if operator == "$unset":
            result = _apply_unset(result, spec)
            continue
        if operator == "$sample":
            result = _apply_sample(result, spec)
            continue
        if operator == "$sort":
            result = sort_documents(result, _require_sort(spec), dialect=dialect)
            continue
        if operator == "$skip":
            result = result[_require_non_negative_int("$skip", spec):]
            continue
        if operator == "$limit":
            result = result[:_require_non_negative_int("$limit", spec)]
            continue
        if operator in {"$addFields", "$set"}:
            result = _apply_add_fields(result, spec, variables, dialect=dialect)
            continue
        if operator == "$unwind":
            result = _apply_unwind(result, spec)
            continue
        if operator == "$group":
            result = _apply_group(result, spec, variables, dialect=dialect)
            continue
        if operator == "$bucket":
            result = _apply_bucket(result, spec, variables, dialect=dialect)
            continue
        if operator == "$bucketAuto":
            result = _apply_bucket_auto(result, spec, variables, dialect=dialect)
            continue
        if operator == "$lookup":
            result = _apply_lookup(
                result,
                spec,
                collection_resolver,
                variables,
                dialect=dialect,
            )
            continue
        if operator == "$unionWith":
            result = _apply_union_with(
                result,
                spec,
                collection_resolver,
                variables,
                dialect=dialect,
            )
            continue
        if operator == "$replaceRoot":
            result = _apply_replace_root(result, spec, variables, dialect=dialect)
            continue
        if operator == "$replaceWith":
            result = _apply_replace_root(
                result,
                {"newRoot": spec},
                variables,
                dialect=dialect,
            )
            continue
        if operator == "$facet":
            result = _apply_facet(
                result,
                spec,
                collection_resolver,
                variables,
                dialect=dialect,
            )
            continue
        if operator == "$count":
            result = _apply_count(result, spec)
            continue
        if operator == "$sortByCount":
            result = _apply_sort_by_count(result, spec, variables, dialect=dialect)
            continue
        if operator == "$setWindowFields":
            result = _apply_set_window_fields(result, spec, variables, dialect=dialect)
            continue
        raise OperationFailure(f"Unsupported aggregation stage: {operator}")
    return result
