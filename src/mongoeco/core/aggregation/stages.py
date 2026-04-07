from __future__ import annotations

from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
import datetime
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.geo import parse_geo_geometry, parse_geo_point, planar_distance_to_geometry
from mongoeco.core.sorting import sort_documents, sort_documents_window
from mongoeco.errors import OperationFailure
from mongoeco.types import Document

from mongoeco.core.aggregation.grouping_stages import (
    _apply_bucket,
    _apply_bucket_auto,
    _apply_count,
    _apply_group,
    _apply_set_window_fields,
    _apply_sort_by_count,
)
from mongoeco.core.aggregation.extensions import (
    AggregationStageExecutionMode,
    get_registered_aggregation_stage_registration,
)
from mongoeco.core.aggregation.join_stages import (
    _apply_facet,
    _apply_lookup,
    _apply_union_with,
)
from mongoeco.core.aggregation.compiled_pipeline import compile_pipeline
from mongoeco.core.aggregation.planning import (
    Pipeline,
    _require_documents_stage,
    _require_non_negative_int,
    _require_sort,
    _require_stage,
)
from mongoeco.core.aggregation.spill import AggregationSpillPolicy
from mongoeco.core.aggregation.runtime import (
    AggregationStageContext,
    _apply_unwind,
)
from mongoeco.core.paths import get_document_value, set_document_value
from mongoeco.core.aggregation.transform_stages import (
    _apply_add_fields,
    _apply_match,
    _apply_project,
    _apply_replace_root,
    _apply_sample,
    _apply_unset,
)


type AggregationStageHandler = Callable[[list[Document], object, AggregationStageContext], list[Document]]


@dataclass(frozen=True, slots=True)
class AggregationStageSpec:
    handler: AggregationStageHandler
    execution_mode: AggregationStageExecutionMode


def _stage_documents(
    _documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    if context.stage_index != 0:
        raise OperationFailure("$documents is only valid as the first pipeline stage")
    return _require_documents_stage(spec)


def _stage_match(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_match(
        documents,
        spec,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
    )


def _stage_project(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_project(documents, spec, context.variables, dialect=context.dialect)


def _stage_unset(documents: list[Document], spec: object, _context: AggregationStageContext) -> list[Document]:
    return _apply_unset(documents, spec)


def _stage_sample(documents: list[Document], spec: object, _context: AggregationStageContext) -> list[Document]:
    return _apply_sample(documents, spec)


def _stage_sort(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    sort_spec = _require_sort(spec)
    sort_with_spill = getattr(context.spill_policy, "sort_with_spill", None)
    if callable(sort_with_spill):
        return sort_with_spill(
            documents,
            sort_spec,
            dialect=context.dialect,
            collation=context.collation,
        )
    return sort_documents(
        documents,
        sort_spec,
        dialect=context.dialect,
        collation=context.collation,
    )


def _stage_skip(documents: list[Document], spec: object, _context: AggregationStageContext) -> list[Document]:
    return documents[_require_non_negative_int("$skip", spec):]


def _stage_limit(documents: list[Document], spec: object, _context: AggregationStageContext) -> list[Document]:
    return documents[:_require_non_negative_int("$limit", spec)]


def _stage_add_fields(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_add_fields(documents, spec, context.variables, dialect=context.dialect)


def _stage_unwind(documents: list[Document], spec: object, _context: AggregationStageContext) -> list[Document]:
    return _apply_unwind(documents, spec)


def _stage_group(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_group(
        documents,
        spec,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
    )


def _stage_bucket(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_bucket(
        documents,
        spec,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
    )


def _stage_bucket_auto(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_bucket_auto(
        documents,
        spec,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
    )


def _stage_lookup(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_lookup(
        documents,
        spec,
        context.collection_resolver,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
        spill_policy=context.spill_policy,
    )


def _stage_union_with(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_union_with(
        documents,
        spec,
        context.collection_resolver,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
        spill_policy=context.spill_policy,
    )


def _stage_replace_root(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_replace_root(documents, spec, context.variables, dialect=context.dialect)


def _stage_replace_with(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_replace_root(
        documents,
        {"newRoot": spec},
        context.variables,
        dialect=context.dialect,
    )


def _stage_facet(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_facet(
        documents,
        spec,
        context.collection_resolver,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
        spill_policy=context.spill_policy,
    )


def _stage_count(documents: list[Document], spec: object, _context: AggregationStageContext) -> list[Document]:
    return _apply_count(documents, spec)


def _stage_sort_by_count(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_sort_by_count(documents, spec, context.variables, dialect=context.dialect)


def _stage_set_window_fields(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_set_window_fields(
        documents,
        spec,
        context.variables,
        dialect=context.dialect,
        collation=context.collation,
    )


def _stage_densify(
    documents: list[Document],
    spec: object,
    _context: AggregationStageContext,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$densify requires a document specification")
    field = spec.get("field")
    if not isinstance(field, str) or not field:
        raise OperationFailure("$densify field must be a non-empty string")
    partition_by_fields = spec.get("partitionByFields", [])
    if partition_by_fields is None:
        partition_by_fields = []
    if not isinstance(partition_by_fields, list) or not all(isinstance(item, str) and item for item in partition_by_fields):
        raise OperationFailure("$densify partitionByFields must be a list of non-empty strings")
    range_spec = spec.get("range")
    if not isinstance(range_spec, dict):
        raise OperationFailure("$densify range must be a document")
    step = range_spec.get("step")
    if not isinstance(step, (int, float)) or isinstance(step, bool) or step <= 0:
        raise OperationFailure("$densify range.step must be a positive number")
    bounds = range_spec.get("bounds")
    unit = range_spec.get("unit")
    if unit is not None and not isinstance(unit, str):
        raise OperationFailure("$densify range.unit must be a string")

    grouped: dict[tuple[object, ...], list[Document]] = {}
    for document in documents:
        key = tuple(_partition_value(document, path) for path in partition_by_fields)
        grouped.setdefault(key, []).append(document)

    result: list[Document] = []
    for partition_key, partition_documents in grouped.items():
        ordered = sorted(
            partition_documents,
            key=lambda document: _require_densify_value(document, field),
        )
        present_values = {
            _require_densify_value(document, field): document
            for document in ordered
        }
        ordered_values = list(present_values)
        lower, upper = _resolve_densify_bounds(bounds, ordered_values)
        current = lower
        while _densify_value_leq(current, upper):
            existing = present_values.get(current)
            if existing is not None:
                result.append(existing)
            else:
                synthetic: Document = {}
                for path, value in zip(partition_by_fields, partition_key, strict=False):
                    if value is not None:
                        set_document_value(synthetic, path, deepcopy(value))
                set_document_value(synthetic, field, current)
                result.append(synthetic)
            current = _advance_densify_value(current, step, unit)
    return result


def _stage_fill(
    documents: list[Document],
    spec: object,
    _context: AggregationStageContext,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$fill requires a document specification")
    sort_by = spec.get("sortBy")
    if not isinstance(sort_by, dict) or len(sort_by) != 1:
        raise OperationFailure("$fill sortBy must be a single-field document")
    sort_field, sort_direction = next(iter(sort_by.items()))
    if sort_direction not in (1, -1) or isinstance(sort_direction, bool):
        raise OperationFailure("$fill sortBy directions must be 1 or -1")
    partition_by_fields = spec.get("partitionByFields", [])
    if partition_by_fields is None:
        partition_by_fields = []
    if not isinstance(partition_by_fields, list) or not all(isinstance(item, str) and item for item in partition_by_fields):
        raise OperationFailure("$fill partitionByFields must be a list of non-empty strings")
    output = spec.get("output")
    if not isinstance(output, dict) or not output:
        raise OperationFailure("$fill output must be a non-empty document")

    grouped: dict[tuple[object, ...], list[Document]] = {}
    for document in documents:
        key = tuple(_partition_value(document, path) for path in partition_by_fields)
        grouped.setdefault(key, []).append(deepcopy(document))

    result: list[Document] = []
    for partition_documents in grouped.values():
        ordered = sorted(
            partition_documents,
            key=lambda document: _partition_value(document, sort_field),
            reverse=sort_direction == -1,
        )
        for field, field_spec in output.items():
            _apply_fill_output(ordered, field, field_spec)
        result.extend(ordered)
    return result


def _stage_geo_near(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$geoNear requires a document specification")
    near = spec.get("near")
    if near is None:
        raise OperationFailure("$geoNear requires near")
    near_point = parse_geo_point(near, label="$geoNear.near")
    distance_field = spec.get("distanceField")
    if not isinstance(distance_field, str) or not distance_field:
        raise OperationFailure("$geoNear distanceField must be a non-empty string")
    key = spec.get("key")
    if not isinstance(key, str) or not key:
        raise OperationFailure("$geoNear key must be a non-empty string in the local runtime")
    query_spec = spec.get("query", {})
    if not isinstance(query_spec, dict):
        raise OperationFailure("$geoNear query must be a document")
    include_locs = spec.get("includeLocs")
    if include_locs is not None and (not isinstance(include_locs, str) or not include_locs):
        raise OperationFailure("$geoNear includeLocs must be a non-empty string")
    min_distance = spec.get("minDistance")
    if min_distance is not None and (
        not isinstance(min_distance, (int, float)) or isinstance(min_distance, bool) or min_distance < 0
    ):
        raise OperationFailure("$geoNear minDistance must be a non-negative number")
    max_distance = spec.get("maxDistance")
    if max_distance is not None and (
        not isinstance(max_distance, (int, float)) or isinstance(max_distance, bool) or max_distance < 0
    ):
        raise OperationFailure("$geoNear maxDistance must be a non-negative number")

    matches: list[tuple[float, Document]] = []
    for document in documents:
        if query_spec and not QueryEngine.match(
            document,
            query_spec,
            dialect=context.dialect,
            collation=context.collation,
        ):
            continue
        found, location = get_document_value(document, key)
        if not found:
            continue
        try:
            _geometry_kind, geometry = parse_geo_geometry(location, label=f"$geoNear key {key}")
        except OperationFailure:
            continue
        distance = planar_distance_to_geometry(near_point, geometry)
        if min_distance is not None and distance < float(min_distance):
            continue
        if max_distance is not None and distance > float(max_distance):
            continue
        enriched = deepcopy(document)
        set_document_value(enriched, distance_field, distance)
        if include_locs is not None:
            set_document_value(enriched, include_locs, deepcopy(location))
        matches.append((distance, enriched))
    matches.sort(key=lambda item: (item[0], item[1].get("_id")))
    return [document for _distance, document in matches]


def _stage_coll_stats(
    _documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    if context.stage_index != 0:
        raise OperationFailure("$collStats is only valid as the first pipeline stage")
    if context.collection_stats_resolver is None:
        raise OperationFailure("$collStats requires a collection stats resolver in the local runtime")
    if not isinstance(spec, dict):
        raise OperationFailure("$collStats requires a document specification")
    unsupported = sorted(set(spec) - {"count", "storageStats"})
    if unsupported:
        raise OperationFailure(
            "$collStats local runtime supports only count and storageStats; unsupported keys: "
            + ", ".join(unsupported)
        )
    if not spec:
        raise OperationFailure("$collStats requires at least one of count or storageStats")

    include_count = False
    scale = 1
    if "count" in spec:
        count_spec = spec.get("count")
        if count_spec != {}:
            raise OperationFailure("$collStats.count must be an empty document")
        include_count = True
    if "storageStats" in spec:
        storage_spec = spec.get("storageStats")
        if not isinstance(storage_spec, dict):
            raise OperationFailure("$collStats.storageStats must be a document")
        unsupported_storage = sorted(set(storage_spec) - {"scale"})
        if unsupported_storage:
            raise OperationFailure(
                "$collStats.storageStats local runtime supports only scale; unsupported keys: "
                + ", ".join(unsupported_storage)
            )
        scale_value = storage_spec.get("scale", 1)
        if not isinstance(scale_value, int) or isinstance(scale_value, bool) or scale_value <= 0:
            raise OperationFailure("$collStats.storageStats.scale must be a positive integer")
        scale = scale_value

    snapshot = context.collection_stats_resolver(scale)
    result: Document = {"ns": snapshot.get("ns")}
    if include_count:
        result["count"] = {"count": snapshot.get("count", 0)}
    if "storageStats" in spec:
        storage_stats = deepcopy(snapshot)
        storage_stats.pop("ok", None)
        result["storageStats"] = storage_stats
    return [result]


def _partition_value(document: Document, path: str) -> object | None:
    found, value = get_document_value(document, path)
    return value if found else None


def _require_densify_value(document: Document, field: str) -> object:
    found, value = get_document_value(document, field)
    if not found:
        raise OperationFailure("$densify requires the densified field to exist in all input documents")
    if not isinstance(value, (int, float, datetime.datetime)) or isinstance(value, bool):
        raise OperationFailure("$densify currently supports numeric or date values only")
    return value


def _resolve_densify_bounds(bounds: object, values: list[object]) -> tuple[object, object]:
    if bounds == "full":
        return values[0], values[-1]
    if isinstance(bounds, list) and len(bounds) == 2:
        return bounds[0], bounds[1]
    raise OperationFailure("$densify range.bounds must be 'full' or a [lower, upper] pair")


def _advance_densify_value(value: object, step: int | float, unit: str | None) -> object:
    if isinstance(value, datetime.datetime):
        delta = _densify_datetime_delta(step, unit)
        return value + delta
    return value + step


def _densify_datetime_delta(step: int | float, unit: str | None) -> datetime.timedelta:
    if unit is None:
        raise OperationFailure("$densify date ranges require a unit")
    if unit == "millisecond":
        return datetime.timedelta(milliseconds=step)
    if unit == "second":
        return datetime.timedelta(seconds=step)
    if unit == "minute":
        return datetime.timedelta(minutes=step)
    if unit == "hour":
        return datetime.timedelta(hours=step)
    if unit == "day":
        return datetime.timedelta(days=step)
    raise OperationFailure("$densify currently supports millisecond/second/minute/hour/day units")


def _densify_value_leq(left: object, right: object) -> bool:
    return bool(left <= right)


def _apply_fill_output(documents: list[Document], field: str, field_spec: object) -> None:
    if not isinstance(field_spec, dict):
        raise OperationFailure("$fill output fields must be documents")
    if "value" in field_spec:
        replacement = field_spec["value"]
        for document in documents:
            found, current = get_document_value(document, field)
            if not found or current is None:
                set_document_value(document, field, deepcopy(replacement))
        return
    method = field_spec.get("method")
    if method == "locf":
        previous: object | None = None
        for document in documents:
            found, current = get_document_value(document, field)
            if found and current is not None:
                previous = current
                continue
            if previous is not None:
                set_document_value(document, field, deepcopy(previous))
        return
    if method == "linear":
        _apply_linear_fill(documents, field)
        return
    raise OperationFailure("$fill output supports only value, locf or linear")


def _apply_linear_fill(documents: list[Document], field: str) -> None:
    known_points: list[tuple[int, object]] = []
    for index, document in enumerate(documents):
        found, current = get_document_value(document, field)
        if found and current is not None:
            known_points.append((index, current))
    for point_index, (left_index, left_value) in enumerate(known_points[:-1]):
        right_index, right_value = known_points[point_index + 1]
        gap = right_index - left_index
        if gap <= 1:
            continue
        if isinstance(left_value, datetime.datetime) and isinstance(right_value, datetime.datetime):
            total = (right_value - left_value).total_seconds()
            for offset in range(1, gap):
                set_document_value(
                    documents[left_index + offset],
                    field,
                    left_value + datetime.timedelta(seconds=(total * offset) / gap),
                )
            continue
        if not isinstance(left_value, (int, float)) or not isinstance(right_value, (int, float)):
            raise OperationFailure("$fill linear currently supports only numeric or date values")
        step = (right_value - left_value) / gap
        for offset in range(1, gap):
            set_document_value(documents[left_index + offset], field, left_value + (step * offset))


AGGREGATION_STAGE_SPECS: dict[str, AggregationStageSpec] = {
    "$documents": AggregationStageSpec(_stage_documents, "materializing"),
    "$match": AggregationStageSpec(_stage_match, "streamable"),
    "$project": AggregationStageSpec(_stage_project, "streamable"),
    "$unset": AggregationStageSpec(_stage_unset, "streamable"),
    "$sample": AggregationStageSpec(_stage_sample, "materializing"),
    "$sort": AggregationStageSpec(_stage_sort, "materializing"),
    "$skip": AggregationStageSpec(_stage_skip, "streamable"),
    "$limit": AggregationStageSpec(_stage_limit, "streamable"),
    "$addFields": AggregationStageSpec(_stage_add_fields, "streamable"),
    "$set": AggregationStageSpec(_stage_add_fields, "streamable"),
    "$unwind": AggregationStageSpec(_stage_unwind, "streamable"),
    "$group": AggregationStageSpec(_stage_group, "materializing"),
    "$bucket": AggregationStageSpec(_stage_bucket, "materializing"),
    "$bucketAuto": AggregationStageSpec(_stage_bucket_auto, "materializing"),
    "$lookup": AggregationStageSpec(_stage_lookup, "streamable"),
    "$unionWith": AggregationStageSpec(_stage_union_with, "materializing"),
    "$replaceRoot": AggregationStageSpec(_stage_replace_root, "streamable"),
    "$replaceWith": AggregationStageSpec(_stage_replace_with, "streamable"),
    "$facet": AggregationStageSpec(_stage_facet, "materializing"),
    "$count": AggregationStageSpec(_stage_count, "materializing"),
    "$sortByCount": AggregationStageSpec(_stage_sort_by_count, "materializing"),
    "$setWindowFields": AggregationStageSpec(_stage_set_window_fields, "materializing"),
    "$densify": AggregationStageSpec(_stage_densify, "materializing"),
    "$fill": AggregationStageSpec(_stage_fill, "materializing"),
    "$geoNear": AggregationStageSpec(_stage_geo_near, "materializing"),
    "$collStats": AggregationStageSpec(_stage_coll_stats, "materializing"),
}

AGGREGATION_STAGE_HANDLERS: dict[str, AggregationStageHandler] = {
    operator: spec.handler for operator, spec in AGGREGATION_STAGE_SPECS.items()
}


def get_aggregation_stage_spec(
    operator: str,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> AggregationStageSpec:
    registration = get_registered_aggregation_stage_registration(operator)
    if registration is not None:
        return AggregationStageSpec(
            registration.handler,
            registration.execution_mode,
        )
    if not dialect.supports_aggregation_stage(operator):
        raise OperationFailure(f"Unsupported aggregation stage: {operator}")
    spec = AGGREGATION_STAGE_SPECS.get(operator)
    if spec is None:
        raise OperationFailure(f"Unsupported aggregation stage: {operator}")
    return spec


def is_streamable_aggregation_stage(
    operator: str,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> bool:
    return get_aggregation_stage_spec(operator, dialect=dialect).execution_mode == "streamable"


def has_materializing_aggregation_stage(
    pipeline: Pipeline,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> bool:
    return any(
        not is_streamable_aggregation_stage(operator, dialect=dialect)
        for operator, _spec in (_require_stage(stage) for stage in pipeline)
    )


def apply_pipeline(
    documents: Iterable[Document],
    pipeline: Pipeline,
    *,
    collection_resolver=None,
    collection_stats_resolver=None,
    variables: dict[str, Any] | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    spill_policy: AggregationSpillPolicy | None = None,
) -> list[Document]:
    compiled_plan = compile_pipeline(
        pipeline,
        dialect=dialect,
        collation=collation,
        spill_policy=spill_policy,
    )
    if compiled_plan is not None:
        return compiled_plan.execute(
            documents,
            variables=variables,
            collection_resolver=collection_resolver,
            spill_policy=spill_policy,
        )

    result = list(documents)
    for index, stage in enumerate(pipeline):
        operator, spec = _require_stage(stage)
        stage_spec = get_aggregation_stage_spec(operator, dialect=dialect)
        if operator == "$sort":
            optimized_window = _sort_window_for_following_slices(pipeline, index)
            if optimized_window is not None:
                result = sort_documents_window(
                    result,
                    _require_sort(spec),
                    window=optimized_window,
                    dialect=dialect,
                    collation=collation,
                )
                if spill_policy is not None:
                    result = spill_policy.maybe_spill(operator, result)
                continue
        result = stage_spec.handler(
            result,
            spec,
            AggregationStageContext(
                stage_index=index,
                collection_resolver=collection_resolver,
                collection_stats_resolver=collection_stats_resolver,
                variables=variables,
                dialect=dialect,
                collation=collation,
                spill_policy=spill_policy,
            ),
        )
        if spill_policy is not None:
            result = spill_policy.maybe_spill(operator, result)
    return result


def _sort_window_for_following_slices(pipeline: Pipeline, stage_index: int) -> int | None:
    remaining_skip = 0
    limit: int | None = None
    seen_slice = False
    for stage in pipeline[stage_index + 1 :]:
        operator, spec = _require_stage(stage)
        if operator == "$skip":
            if limit is not None:
                return None
            remaining_skip += _require_non_negative_int("$skip", spec)
            seen_slice = True
            continue
        if operator == "$limit":
            value = _require_non_negative_int("$limit", spec)
            limit = value if limit is None else min(limit, value)
            seen_slice = True
            continue
        break
    if not seen_slice or limit is None:
        return None
    return remaining_skip + limit
