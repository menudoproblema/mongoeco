from __future__ import annotations

import datetime
import math

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_scalars import bson_numeric_alias
from mongoeco.error_catalog import (
    DENSIFY_FIELD_PREFIX_ERROR,
    DENSIFY_PARTITION_PREFIX_ERROR,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import (  # noqa: TC001 - public runtime annotations
    Document, Filter, Projection, SortSpec,
)


type PipelineStage = dict[str, Any]
type Pipeline = list[PipelineStage]
_DENSIFY_BOUNDS_COUNT = 2


@dataclass(frozen=True, slots=True)
class AggregationPushdown:
    filter_spec: Filter
    projection: Projection | None
    sort: SortSpec | None
    skip: int
    limit: int | None
    remaining_pipeline: Pipeline


def _densify_range_error(message, code):
    code_name = {9: "FailedToParse", 14: "TypeMismatch"}.get(code, f"Location{code}")
    raise OperationFailure(message, code=code, details={"codeName": code_name})


def _validate_densify_range(range_spec):
    """Validate literals independently of whether any input row exists."""
    if not isinstance(range_spec, dict):
        return
    step = range_spec.get("step")
    alias = bson_numeric_alias(step)
    if alias is None or isinstance(step, bool):
        _densify_range_error("$densify range.step must be a positive number", 14)
    if not step > 0:
        _densify_range_error("$densify range.step must be a positive number", 5733401)
    unit = range_spec.get("unit")
    if unit is not None:
        if not isinstance(unit, str):
            _densify_range_error("$densify range.unit must be a string", 14)
        if unit not in {
            "millisecond", "second", "minute", "hour", "day", "week",
            "month", "quarter", "year",
        }:
            _densify_range_error(f"unknown time unit value: {unit}", 9)
        if not math.isfinite(step) or step != int(step):
            _densify_range_error("$densify date step must be a whole number", 6586400)
    bounds = range_spec.get("bounds")
    _validate_densify_bounds(bounds, alias, unit)


def _validate_densify_bounds(bounds, alias, unit):
    if isinstance(bounds, str) and bounds in {"full", "partition"}:
        return
    if not isinstance(bounds, list) or len(bounds) != _DENSIFY_BOUNDS_COUNT:
        _densify_range_error(
            "$densify bounds must be full, partition, or a pair", 5946802
        )
    dates = all(isinstance(value, datetime.datetime) for value in bounds)
    numbers = all(bson_numeric_alias(value) is not None for value in bounds)
    if not dates and not numbers:
        _densify_range_error("$densify bounds must both be dates or numeric", 5733406)
    if bounds[0] > bounds[1]:
        _densify_range_error("$densify bounds must be ascending", 5733402)
    if numbers:
        if unit is not None:
            _densify_range_error(
                "$densify numeric bounds may not specify a unit", 5733409
            )
        if any(bson_numeric_alias(value) != alias for value in bounds):
            _densify_range_error(
                "$densify bounds and step must have the same type", 5876900
            )
    elif unit is None:
        _densify_range_error("$densify date bounds require a unit", 5733410)


def _validate_densify_partition_paths(spec: object, *, dialect: MongoDialect) -> None:
    if not dialect.behavior_flag('validates_densify_partition_paths', default=False):
        return
    if not isinstance(spec, dict):
        return
    field = spec.get('field')
    partitions = spec.get('partitionByFields', [])
    if not isinstance(field, str) or not isinstance(partitions, list):
        return
    for partition in partitions:
        if not isinstance(partition, str):
            continue
        if partition == field or partition.startswith(field + '.'):
            message = '$densify field must not be a prefix of a partitionByFields path'
            raise OperationFailure(message, descriptor=DENSIFY_FIELD_PREFIX_ERROR)
        if field.startswith(partition + '.'):
            message = (
                '$densify partitionByFields must not be a prefix of the field path'
            )
            raise OperationFailure(message, descriptor=DENSIFY_PARTITION_PREFIX_ERROR)


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
    return dialect.policy.projection_flag(value)


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


def _require_documents_stage(spec: object) -> list[Document]:
    if not isinstance(spec, list):
        raise OperationFailure("$documents requires an array of documents")
    if not all(isinstance(item, dict) for item in spec):
        raise OperationFailure("$documents requires an array of documents")
    return [deepcopy(item) for item in spec]


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
