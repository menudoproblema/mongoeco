from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
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
    return _apply_group(documents, spec, context.variables, dialect=context.dialect)


def _stage_bucket(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_bucket(documents, spec, context.variables, dialect=context.dialect)


def _stage_bucket_auto(
    documents: list[Document],
    spec: object,
    context: AggregationStageContext,
) -> list[Document]:
    return _apply_bucket_auto(documents, spec, context.variables, dialect=context.dialect)


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
    return _apply_set_window_fields(documents, spec, context.variables, dialect=context.dialect)


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
