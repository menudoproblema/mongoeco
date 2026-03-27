from collections.abc import Callable, Iterable
from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.sorting import sort_documents
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
from mongoeco.core.aggregation.extensions import get_registered_aggregation_stage
from mongoeco.core.aggregation.join_stages import (
    _apply_facet,
    _apply_lookup,
    _apply_union_with,
)
from mongoeco.core.aggregation.planning import (
    Pipeline,
    _require_documents_stage,
    _require_non_negative_int,
    _require_sort,
    _require_stage,
)
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
    return _apply_match(documents, spec, context.variables, dialect=context.dialect)


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
    return sort_documents(documents, _require_sort(spec), dialect=context.dialect)


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


AGGREGATION_STAGE_HANDLERS: dict[str, AggregationStageHandler] = {
    "$documents": _stage_documents,
    "$match": _stage_match,
    "$project": _stage_project,
    "$unset": _stage_unset,
    "$sample": _stage_sample,
    "$sort": _stage_sort,
    "$skip": _stage_skip,
    "$limit": _stage_limit,
    "$addFields": _stage_add_fields,
    "$set": _stage_add_fields,
    "$unwind": _stage_unwind,
    "$group": _stage_group,
    "$bucket": _stage_bucket,
    "$bucketAuto": _stage_bucket_auto,
    "$lookup": _stage_lookup,
    "$unionWith": _stage_union_with,
    "$replaceRoot": _stage_replace_root,
    "$replaceWith": _stage_replace_with,
    "$facet": _stage_facet,
    "$count": _stage_count,
    "$sortByCount": _stage_sort_by_count,
    "$setWindowFields": _stage_set_window_fields,
}


def apply_pipeline(
    documents: Iterable[Document],
    pipeline: Pipeline,
    *,
    collection_resolver=None,
    variables: dict[str, Any] | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    result = [deepcopy(document) for document in documents]
    for index, stage in enumerate(pipeline):
        operator, spec = _require_stage(stage)
        handler = get_registered_aggregation_stage(operator)
        if handler is None:
            if not dialect.supports_aggregation_stage(operator):
                raise OperationFailure(f"Unsupported aggregation stage: {operator}")
            handler = AGGREGATION_STAGE_HANDLERS.get(operator)
        if handler is None:
            raise OperationFailure(f"Unsupported aggregation stage: {operator}")
        result = handler(
            result,
            spec,
            AggregationStageContext(
                stage_index=index,
                collection_resolver=collection_resolver,
                variables=variables,
                dialect=dialect,
            ),
        )
    return result
