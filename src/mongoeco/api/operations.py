from dataclasses import dataclass, replace

from mongoeco.api._async.cursor import (
    HintSpec,
    _validate_batch_size,
    _validate_hint_spec,
    _validate_max_time_ms,
    _validate_sort_spec,
)
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.core.validation import is_filter, is_projection
from mongoeco.types import Filter, Projection, SortSpec


@dataclass(frozen=True, slots=True)
class FindOperation:
    filter_spec: Filter
    plan: QueryNode
    projection: Projection | None = None
    sort: SortSpec | None = None
    skip: int = 0
    limit: int | None = None
    hint: HintSpec | None = None
    comment: object | None = None
    max_time_ms: int | None = None
    batch_size: int | None = None

    def with_overrides(self, **changes: object) -> "FindOperation":
        return replace(self, **changes)


def compile_find_operation(
    filter_spec: object | None = None,
    *,
    projection: object | None = None,
    sort: object | None = None,
    skip: int = 0,
    limit: int | None = None,
    hint: object | None = None,
    comment: object | None = None,
    max_time_ms: object | None = None,
    batch_size: object | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables: dict[str, object] | None = None,
    plan: QueryNode | None = None,
) -> FindOperation:
    normalized_filter = _normalize_filter(filter_spec)
    normalized_projection = _normalize_projection(projection)
    normalized_sort = _normalize_sort(sort)
    normalized_hint = _normalize_hint(hint)
    normalized_max_time_ms = _normalize_max_time_ms(max_time_ms)
    normalized_batch_size = _normalize_batch_size(batch_size)
    normalized_skip = _normalize_skip(skip)
    normalized_limit = _normalize_limit(limit)
    return FindOperation(
        filter_spec=normalized_filter,
        plan=compile_filter(normalized_filter, dialect=dialect, variables=variables)
        if plan is None
        else plan,
        projection=normalized_projection,
        sort=normalized_sort,
        skip=normalized_skip,
        limit=normalized_limit,
        hint=normalized_hint,
        comment=comment,
        max_time_ms=normalized_max_time_ms,
        batch_size=normalized_batch_size,
    )


def _normalize_filter(filter_spec: object | None) -> Filter:
    if filter_spec is None:
        return {}
    if not is_filter(filter_spec):
        raise TypeError("filter_spec must be a dict")
    return filter_spec


def _normalize_projection(projection: object | None) -> Projection | None:
    if projection is None:
        return None
    if not is_projection(projection):
        raise TypeError("projection must be a dict")
    return projection


def _normalize_sort(sort: object | None) -> SortSpec | None:
    if sort is None:
        return None
    _validate_sort_spec(sort)
    return sort


def _normalize_hint(hint: object | None) -> HintSpec | None:
    if hint is None:
        return None
    _validate_hint_spec(hint)
    return hint


def _normalize_batch_size(batch_size: object | None) -> int | None:
    if batch_size is None:
        return None
    _validate_batch_size(batch_size)
    return batch_size


def _normalize_max_time_ms(max_time_ms: object | None) -> int | None:
    if max_time_ms is None:
        return None
    _validate_max_time_ms(max_time_ms)
    return max_time_ms


def _normalize_skip(skip: object) -> int:
    if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
        raise TypeError("skip must be a non-negative integer")
    return skip


def _normalize_limit(limit: object | None) -> int | None:
    if limit is None:
        return None
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise TypeError("limit must be a non-negative integer")
    return limit
