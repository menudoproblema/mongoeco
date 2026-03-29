from dataclasses import dataclass, replace

from mongoeco.api._async.cursor import (
    HintSpec,
    _normalize_sort_spec,
    _validate_batch_size,
    _validate_max_time_ms,
)
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation import Pipeline
from mongoeco.core.aggregation.extensions import get_registered_aggregation_stage
from mongoeco.core.collation import normalize_collation
from mongoeco.core.operators import CompiledUpdatePlan, UpdateEngine
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.core.search import validate_search_stage_pipeline
from mongoeco.core.validation import is_filter, is_projection
from mongoeco.errors import OperationFailure
from mongoeco.types import ArrayFilters, CollationDocument, Filter, PlanningIssue, PlanningMode, Projection, SortSpec


@dataclass(frozen=True, slots=True)
class FindOperation:
    filter_spec: Filter
    plan: QueryNode
    projection: Projection | None = None
    collation: CollationDocument | None = None
    sort: SortSpec | None = None
    skip: int = 0
    limit: int | None = None
    hint: HintSpec | None = None
    comment: object | None = None
    max_time_ms: int | None = None
    batch_size: int | None = None
    planning_mode: PlanningMode = PlanningMode.STRICT
    planning_issues: tuple[PlanningIssue, ...] = ()

    def with_overrides(self, **changes: object) -> "FindOperation":
        return replace(self, **changes)


@dataclass(frozen=True, slots=True)
class UpdateOperation:
    filter_spec: Filter
    plan: QueryNode
    update_spec: dict[str, object] | None = None
    compiled_update_plan: CompiledUpdatePlan | None = None
    compiled_upsert_plan: CompiledUpdatePlan | None = None
    collation: CollationDocument | None = None
    sort: SortSpec | None = None
    array_filters: ArrayFilters | None = None
    hint: HintSpec | None = None
    comment: object | None = None
    max_time_ms: int | None = None
    let: dict[str, object] | None = None
    planning_mode: PlanningMode = PlanningMode.STRICT
    planning_issues: tuple[PlanningIssue, ...] = ()

    def with_overrides(self, **changes: object) -> "UpdateOperation":
        return replace(self, **changes)


@dataclass(frozen=True, slots=True)
class AggregateOperation:
    pipeline: Pipeline
    collation: CollationDocument | None = None
    hint: HintSpec | None = None
    comment: object | None = None
    max_time_ms: int | None = None
    batch_size: int | None = None
    allow_disk_use: bool | None = None
    let: dict[str, object] | None = None
    planning_mode: PlanningMode = PlanningMode.STRICT
    planning_issues: tuple[PlanningIssue, ...] = ()

    def with_overrides(self, **changes: object) -> "AggregateOperation":
        return replace(self, **changes)


def compile_find_selection_from_update_operation(
    operation: UpdateOperation,
    *,
    projection: Projection | None = None,
    limit: int | None = None,
) -> FindOperation:
    return FindOperation(
        filter_spec=operation.filter_spec,
        plan=operation.plan,
        projection=projection,
        collation=operation.collation,
        sort=operation.sort,
        limit=limit,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        planning_mode=operation.planning_mode,
        planning_issues=operation.planning_issues,
    )


def compile_find_operation(
    filter_spec: object | None = None,
    *,
    projection: object | None = None,
    collation: object | None = None,
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
    planning_mode: PlanningMode = PlanningMode.STRICT,
) -> FindOperation:
    normalized_filter = _normalize_filter(filter_spec)
    normalized_projection = _normalize_projection(projection)
    normalized_collation = _normalize_collation(collation)
    normalized_sort = _normalize_sort(sort)
    normalized_hint = _normalize_hint(hint)
    normalized_max_time_ms = _normalize_max_time_ms(max_time_ms)
    normalized_batch_size = _normalize_batch_size(batch_size)
    normalized_skip = _normalize_skip(skip)
    normalized_limit = _normalize_limit(limit)
    return FindOperation(
        filter_spec=normalized_filter,
        plan=compile_filter(normalized_filter, dialect=dialect, variables=variables, planning_mode=planning_mode)
        if plan is None
        else plan,
        projection=normalized_projection,
        collation=normalized_collation,
        sort=normalized_sort,
        skip=normalized_skip,
        limit=normalized_limit,
        hint=normalized_hint,
        comment=comment,
        max_time_ms=normalized_max_time_ms,
        batch_size=normalized_batch_size,
        planning_mode=planning_mode,
        planning_issues=_collect_query_planning_issues(normalized_filter, dialect=dialect, variables=variables, planning_mode=planning_mode),
    )


def compile_update_operation(
    filter_spec: object | None = None,
    *,
    collation: object | None = None,
    sort: object | None = None,
    array_filters: object | None = None,
    hint: object | None = None,
    comment: object | None = None,
    max_time_ms: object | None = None,
    let: object | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    plan: QueryNode | None = None,
    update_spec: dict[str, object] | None = None,
    planning_mode: PlanningMode = PlanningMode.STRICT,
) -> UpdateOperation:
    normalized_filter = _normalize_filter(filter_spec)
    normalized_sort = _normalize_sort(sort)
    normalized_collation = _normalize_collation(collation)
    normalized_array_filters = _normalize_array_filters(array_filters)
    normalized_hint = _normalize_hint(hint)
    normalized_max_time_ms = _normalize_max_time_ms(max_time_ms)
    normalized_let = _normalize_let(let)
    compiled_update_plan, compiled_upsert_plan = _compile_update_plans(
        update_spec,
        dialect=dialect,
        selector_filter=normalized_filter,
        array_filters=normalized_array_filters,
        planning_mode=planning_mode,
    )
    return UpdateOperation(
        filter_spec=normalized_filter,
        plan=compile_filter(
            normalized_filter,
            dialect=dialect,
            variables=normalized_let,
            planning_mode=planning_mode,
        )
        if plan is None
        else plan,
        update_spec=update_spec,
        compiled_update_plan=compiled_update_plan,
        compiled_upsert_plan=compiled_upsert_plan,
        collation=normalized_collation,
        sort=normalized_sort,
        array_filters=normalized_array_filters,
        hint=normalized_hint,
        comment=comment,
        max_time_ms=normalized_max_time_ms,
        let=normalized_let,
        planning_mode=planning_mode,
        planning_issues=(
            *_collect_query_planning_issues(
                normalized_filter,
                dialect=dialect,
                variables=normalized_let,
                planning_mode=planning_mode,
            ),
            *_collect_update_planning_issues(update_spec, dialect=dialect, planning_mode=planning_mode),
        ),
    )


def compile_aggregate_operation(
    pipeline: object,
    *,
    collation: object | None = None,
    hint: object | None = None,
    comment: object | None = None,
    max_time_ms: object | None = None,
    batch_size: object | None = None,
    allow_disk_use: object | None = None,
    let: object | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    planning_mode: PlanningMode = PlanningMode.STRICT,
) -> AggregateOperation:
    if not isinstance(pipeline, list):
        raise TypeError("pipeline must be a list")
    validate_search_stage_pipeline(pipeline)
    return AggregateOperation(
        pipeline=pipeline,
        collation=_normalize_collation(collation),
        hint=_normalize_hint(hint),
        comment=comment,
        max_time_ms=_normalize_max_time_ms(max_time_ms),
        batch_size=_normalize_batch_size(batch_size),
        allow_disk_use=_normalize_allow_disk_use(allow_disk_use),
        let=_normalize_let(let),
        planning_mode=planning_mode,
        planning_issues=_collect_aggregate_planning_issues(pipeline, dialect=dialect, planning_mode=planning_mode),
    )


def _collect_query_planning_issues(
    filter_spec: Filter,
    *,
    dialect: MongoDialect,
    variables: dict[str, object] | None,
    planning_mode: PlanningMode,
) -> tuple[PlanningIssue, ...]:
    if planning_mode is not PlanningMode.RELAXED:
        return ()
    plan = compile_filter(filter_spec, dialect=dialect, variables=variables, planning_mode=planning_mode)
    issue = getattr(plan, "issue", None)
    if issue is None:
        return ()
    return (issue,)


def _collect_update_planning_issues(
    update_spec: dict[str, object] | None,
    *,
    dialect: MongoDialect,
    planning_mode: PlanningMode,
) -> tuple[PlanningIssue, ...]:
    if planning_mode is not PlanningMode.RELAXED or update_spec is None:
        return ()
    issues: list[PlanningIssue] = []
    if not isinstance(update_spec, dict):
        return (PlanningIssue(scope="update", message="update specification must be a document"),)
    for operator in update_spec:
        if isinstance(operator, str) and operator.startswith("$") and not dialect.supports_update_operator(operator):
            issues.append(PlanningIssue(scope="update", message=f"Unsupported update operator: {operator}"))
    return tuple(issues)


def _compile_update_plans(
    update_spec: dict[str, object] | None,
    *,
    dialect: MongoDialect,
    selector_filter: Filter,
    array_filters: ArrayFilters | None,
    planning_mode: PlanningMode,
) -> tuple[CompiledUpdatePlan | None, CompiledUpdatePlan | None]:
    if not isinstance(update_spec, dict) or not update_spec:
        return None, None
    if not all(isinstance(operator, str) and operator.startswith("$") for operator in update_spec):
        return None, None
    if not all(isinstance(params, dict) for params in update_spec.values()):
        return None, None
    try:
        return (
            UpdateEngine.compile_update_plan(
                update_spec,
                dialect=dialect,
                selector_filter=selector_filter,
                array_filters=array_filters,
            ),
            UpdateEngine.compile_update_plan(
                update_spec,
                dialect=dialect,
                selector_filter=selector_filter,
                array_filters=array_filters,
                is_upsert_insert=True,
            ),
        )
    except OperationFailure:
        if planning_mode is PlanningMode.RELAXED:
            return None, None
        raise


def _collect_aggregate_planning_issues(
    pipeline: object,
    *,
    dialect: MongoDialect,
    planning_mode: PlanningMode,
) -> tuple[PlanningIssue, ...]:
    if planning_mode is not PlanningMode.RELAXED:
        return ()
    if not isinstance(pipeline, list):
        return (PlanningIssue(scope="aggregate", message="pipeline must be a list"),)
    issues: list[PlanningIssue] = []
    for stage in pipeline:
        if not isinstance(stage, dict) or len(stage) != 1:
            issues.append(PlanningIssue(scope="aggregate", message="Each pipeline stage must be a single-key document"))
            continue
        operator = next(iter(stage))
        if not isinstance(operator, str) or not operator.startswith("$"):
            issues.append(PlanningIssue(scope="aggregate", message="Pipeline stage operator must start with '$'"))
            continue
        if get_registered_aggregation_stage(operator) is None and not dialect.supports_aggregation_stage(operator):
            issues.append(PlanningIssue(scope="aggregate", message=f"Unsupported aggregation stage: {operator}"))
    return tuple(issues)


def _normalize_filter(filter_spec: object | None) -> Filter:
    if filter_spec is None:
        return {}
    if not is_filter(filter_spec):
        raise TypeError("filter_spec must be a dict")
    return filter_spec


def _normalize_collation(collation: object | None) -> CollationDocument | None:
    normalized = normalize_collation(collation)
    if normalized is None:
        return None
    return normalized.to_document()


def _normalize_projection(projection: object | None) -> Projection | None:
    if projection is None:
        return None
    if not is_projection(projection):
        raise TypeError("projection must be a dict")
    return projection


def _normalize_sort(sort: object | None) -> SortSpec | None:
    return _normalize_sort_spec(sort)


def _normalize_hint(hint: object | None) -> HintSpec | None:
    if hint is None:
        return None
    if isinstance(hint, str):
        if not hint:
            raise ValueError("hint string must not be empty")
        return hint
    return _normalize_sort_spec(hint)


def _normalize_array_filters(array_filters: object | None) -> ArrayFilters | None:
    if array_filters is None:
        return None
    if not isinstance(array_filters, list):
        raise TypeError("array_filters must be a list of dicts")
    if not all(is_filter(item) for item in array_filters):
        raise TypeError("array_filters must be a list of dicts")
    return array_filters


def _normalize_let(let: object | None) -> dict[str, object] | None:
    if let is None:
        return None
    if not isinstance(let, dict):
        raise TypeError("let must be a dict")
    return let


def _normalize_batch_size(batch_size: object | None) -> int | None:
    if batch_size is None:
        return None
    _validate_batch_size(batch_size)
    return batch_size


def _normalize_allow_disk_use(allow_disk_use: object | None) -> bool | None:
    if allow_disk_use is None:
        return None
    if not isinstance(allow_disk_use, bool):
        raise TypeError("allow_disk_use must be a bool")
    return allow_disk_use


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
