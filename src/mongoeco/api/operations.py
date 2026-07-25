from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, replace
import re
import threading

from mongoeco.api.argument_validation import (
    HintSpec,
    normalize_sort_spec as _normalize_sort_spec,
    validate_batch_size as _validate_batch_size,
    validate_max_time_ms as _validate_max_time_ms,
)
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation import Pipeline
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.aggregation.extensions import get_registered_aggregation_stage
from mongoeco.core.collation import normalize_collation
from mongoeco.core.operators import (
    CompiledExecutableUpdatePlan,
    CompiledUpdateOperator,
    CompiledUpdatePlan,
    UpdateEngine,
)
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.json_compat import json_dumps_compact
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.core.search import (
    TEXT_SCORE_FIELD,
    ClassicTextQuery,
    split_classic_text_filter,
    validate_search_stage_pipeline,
)
from mongoeco.core.update_paths import CompiledUpdateInstruction
from mongoeco.core.validation import is_filter, is_projection
from mongoeco.errors import OperationFailure
from mongoeco.types import ArrayFilters, CollationDocument, Filter, PlanningIssue, PlanningMode, Projection, SortSpec, Update


@dataclass(frozen=True, slots=True)
class FindOperation:
    filter_spec: Filter
    selector_filter: Filter
    plan: QueryNode
    text_query: ClassicTextQuery | None = None
    projection: Projection | None = None
    collation: CollationDocument | None = None
    sort: SortSpec | None = None
    skip: int = 0
    limit: int | None = None
    hint: HintSpec | None = None
    comment: object | None = None
    max_time_ms: int | None = None
    batch_size: int | None = None
    let: Mapping[str, object] | None = None
    planning_mode: PlanningMode = PlanningMode.STRICT
    planning_issues: tuple[PlanningIssue, ...] = ()

    def with_overrides(self, **changes: object) -> "FindOperation":
        return replace(self, **changes)


@dataclass(frozen=True, slots=True)
class UpdateOperation:
    filter_spec: Filter
    plan: QueryNode
    update_spec: Update | None = None
    compiled_update_plan: CompiledExecutableUpdatePlan | None = None
    compiled_upsert_plan: CompiledExecutableUpdatePlan | None = None
    collation: CollationDocument | None = None
    sort: SortSpec | None = None
    array_filters: ArrayFilters | None = None
    hint: HintSpec | None = None
    comment: object | None = None
    max_time_ms: int | None = None
    let: Mapping[str, object] | None = None
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
    let: Mapping[str, object] | None = None
    planning_mode: PlanningMode = PlanningMode.STRICT
    planning_issues: tuple[PlanningIssue, ...] = ()

    def with_overrides(self, **changes: object) -> "AggregateOperation":
        return replace(self, **changes)


_CACHEABLE_UPDATE_OPERATORS = frozenset(
    {
        "$set",
        "$unset",
        "$inc",
        "$mul",
        "$min",
        "$max",
    }
)
_UPDATE_PLAN_TEMPLATE_CACHE_MAX_SIZE = 2_048
_UPDATE_PLAN_TEMPLATE_CACHE_LOCK = threading.RLock()
_UPDATE_PLAN_TEMPLATE_CACHE: OrderedDict[
    tuple[object, ...],
    tuple["_UpdatePlanTemplate", "_UpdatePlanTemplate"],
] = OrderedDict()

_LET_VARIABLE_RE = re.compile(r"^(?:[a-z]|[^\x00-\x7f])(?:[A-Za-z0-9_]|[^\x00-\x7f])*$")


@dataclass(frozen=True, slots=True)
class _UpdateInstructionTemplate:
    operator: str
    path: object
    value_slot: int
    target_path: object | None = None

    def bind(self, values: tuple[object, ...]) -> CompiledUpdateInstruction:
        return CompiledUpdateInstruction(
            operator=self.operator,
            path=self.path,
            value=values[self.value_slot],
            target_path=self.target_path,
        )


@dataclass(frozen=True, slots=True)
class _UpdateOperatorTemplate:
    operator: str
    instructions: tuple[_UpdateInstructionTemplate, ...]
    handler: object

    def bind(self, values: tuple[object, ...]) -> CompiledUpdateOperator:
        return CompiledUpdateOperator(
            operator=self.operator,
            instructions=tuple(
                instruction.bind(values) for instruction in self.instructions
            ),
            handler=self.handler,
        )


@dataclass(frozen=True, slots=True)
class _UpdatePlanTemplate:
    operators: tuple[_UpdateOperatorTemplate, ...]
    touches_document_id: bool
    is_upsert_insert: bool

    @classmethod
    def from_plan(
        cls,
        plan: CompiledExecutableUpdatePlan,
        *,
        slot_map: dict[tuple[str, str], int],
        is_upsert_insert: bool,
    ) -> "_UpdatePlanTemplate":
        if not isinstance(plan, CompiledUpdatePlan):
            raise AssertionError("cached update templates require classic plans")
        return cls(
            operators=tuple(
                _UpdateOperatorTemplate(
                    operator=operator.operator,
                    instructions=tuple(
                        _UpdateInstructionTemplate(
                            operator=instruction.operator,
                            path=instruction.path,
                            value_slot=slot_map[
                                (instruction.operator, instruction.path.raw)
                            ],
                            target_path=instruction.target_path,
                        )
                        for instruction in operator.instructions
                    ),
                    handler=operator.handler,
                )
                for operator in plan.compiled_operators
            ),
            touches_document_id=plan.touches_document_id,
            is_upsert_insert=is_upsert_insert,
        )

    def bind(
        self,
        update_spec: Update,
        values: tuple[object, ...],
        *,
        dialect: MongoDialect,
        selector_filter: Filter,
    ) -> CompiledUpdatePlan:
        context = UpdateEngine.build_execution_context(
            dialect=dialect,
            selector_filter=selector_filter,
            is_upsert_insert=self.is_upsert_insert,
        )
        return CompiledUpdatePlan(
            update_spec=update_spec,
            compiled_operators=tuple(
                operator.bind(values) for operator in self.operators
            ),
            context=context,
            touches_document_id=self.touches_document_id,
        )


def _clear_update_plan_template_cache() -> None:
    with _UPDATE_PLAN_TEMPLATE_CACHE_LOCK:
        _UPDATE_PLAN_TEMPLATE_CACHE.clear()


def compile_find_selection_from_update_operation(
    operation: UpdateOperation,
    *,
    projection: Projection | None = None,
    limit: int | None = None,
) -> FindOperation:
    return FindOperation(
        filter_spec=operation.filter_spec,
        selector_filter=operation.filter_spec,
        plan=operation.plan,
        projection=projection,
        collation=operation.collation,
        sort=operation.sort,
        limit=limit,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        let=operation.let,
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
    selector_filter, text_query = split_classic_text_filter(normalized_filter)
    normalized_projection = _normalize_projection(projection)
    normalized_collation = _normalize_collation(collation)
    normalized_sort = _normalize_sort(sort)
    normalized_hint = _normalize_hint(hint)
    normalized_max_time_ms = _normalize_max_time_ms(max_time_ms)
    normalized_batch_size = _normalize_batch_size(batch_size)
    normalized_skip = _normalize_skip(skip)
    normalized_limit = _normalize_limit(limit)
    normalized_let = _normalize_let(variables)
    if text_query is None:
        if _projection_requests_text_score(normalized_projection):
            raise OperationFailure("$meta textScore projection requires a $text query")
        if _sort_requests_text_score(normalized_sort):
            raise OperationFailure("$meta textScore sort requires a $text query")
    return FindOperation(
        filter_spec=normalized_filter,
        selector_filter=selector_filter,
        plan=compile_filter(
            selector_filter,
            dialect=dialect,
            planning_mode=planning_mode,
        )
        if plan is None
        else plan,
        text_query=text_query,
        projection=normalized_projection,
        collation=normalized_collation,
        sort=normalized_sort,
        skip=normalized_skip,
        limit=normalized_limit,
        hint=normalized_hint,
        comment=comment,
        max_time_ms=normalized_max_time_ms,
        batch_size=normalized_batch_size,
        let=normalized_let,
        planning_mode=planning_mode,
        planning_issues=_collect_query_planning_issues(
            selector_filter,
            dialect=dialect,
            variables=normalized_let,
            planning_mode=planning_mode,
        ),
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
    update_spec: Update | None = None,
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
        collation=normalized_collation,
        array_filters=normalized_array_filters,
        variables=normalized_let,
        planning_mode=planning_mode,
    )
    return UpdateOperation(
        filter_spec=normalized_filter,
        plan=compile_filter(
            normalized_filter,
            dialect=dialect,
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
    variables: Mapping[str, object] | None,
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
    update_spec: Update | None,
    *,
    dialect: MongoDialect,
    planning_mode: PlanningMode,
) -> tuple[PlanningIssue, ...]:
    if planning_mode is not PlanningMode.RELAXED or update_spec is None:
        return ()
    shape_issue = _validate_update_spec_shape(update_spec)
    if shape_issue is not None:
        return (PlanningIssue(scope="update", message=shape_issue),)
    issues: list[PlanningIssue] = []
    if isinstance(update_spec, list):
        for stage in update_spec:
            if not isinstance(stage, dict) or len(stage) != 1:
                issues.append(PlanningIssue(scope="update", message="Each update pipeline stage must be a single-key document"))
                continue
            operator = next(iter(stage))
            if not isinstance(operator, str) or not operator.startswith("$"):
                issues.append(PlanningIssue(scope="update", message="Update pipeline stage operator must start with '$'"))
                continue
            if operator not in UpdateEngine._UPDATE_PIPELINE_STAGE_OPERATORS:
                issues.append(PlanningIssue(scope="update", message=f"Unsupported update pipeline stage: {operator}"))
        if issues:
            return tuple(issues)
        try:
            UpdateEngine.validate_update_pipeline(update_spec, dialect=dialect)
        except OperationFailure as exc:
            issues.append(PlanningIssue(scope="update", message=str(exc)))
        return tuple(issues)
    for operator in update_spec:
        if isinstance(operator, str) and operator.startswith("$") and not dialect.supports_update_operator(operator):
            issues.append(PlanningIssue(scope="update", message=f"Unsupported update operator: {operator}"))
    return tuple(issues)


def _update_dialect_cache_key(dialect: MongoDialect) -> tuple[object, ...]:
    policy = dialect.catalog_policy_spec
    update_operators = dialect.catalog_update_operators
    return (
        type(dialect),
        dialect.key,
        dialect.server_version,
        tuple(sorted(dialect.catalog_behavior_flags.items())),
        (
            policy.null_query_matches_undefined,
            policy.expression_truthiness,
            policy.projection_flag_mode,
            policy.update_path_sort_mode,
            policy.equality_mode,
            policy.comparison_mode,
        ),
        tuple(sorted(dialect.catalog_capabilities)),
        None if update_operators is None else tuple(sorted(update_operators)),
    )


def _field_only_update_path(path: object) -> str | None:
    if not isinstance(path, str) or path == "":
        return None
    segments = path.split(".")
    if any(segment == "" for segment in segments):
        return None
    if any(
        segment.isdigit()
        or segment == "$"
        or segment == "$[]"
        or (segment.startswith("$[") and segment.endswith("]"))
        for segment in segments
    ):
        return None
    return path


def _build_update_plan_template_cache_key(
    update_spec: Update,
    *,
    dialect: MongoDialect,
    collation: CollationDocument | None,
    array_filters: ArrayFilters | None,
    variables: dict[str, object] | None,
    planning_mode: PlanningMode,
) -> tuple[tuple[object, ...], tuple[object, ...]] | None:
    if planning_mode is not PlanningMode.STRICT:
        return None
    if collation is not None or array_filters is not None:
        return None
    if not isinstance(update_spec, dict):
        return None

    values: list[object] = []
    operator_shapes: list[tuple[str, tuple[str, ...]]] = []
    for operator, params in update_spec.items():
        if operator not in _CACHEABLE_UPDATE_OPERATORS:
            return None
        if not dialect.supports_update_operator(operator):
            return None
        try:
            ordered_items = UpdateEngine._iter_ordered_update_items(
                params,
                dialect=dialect,
            )
        except OperationFailure:
            return None
        if not ordered_items:
            return None
        paths: list[str] = []
        for path, value in ordered_items:
            compiled_path = _field_only_update_path(path)
            if compiled_path is None:
                return None
            paths.append(compiled_path)
            values.append(value)
        operator_shapes.append((operator, tuple(paths)))

    if not operator_shapes:
        return None
    user_let = (
        variables.bindings
        if isinstance(variables, ExpressionExecutionContext)
        else variables
    )
    try:
        let_fingerprint = (
            None
            if user_let is None
            else json_dumps_compact(DocumentCodec.encode(dict(user_let)), sort_keys=True)
        )
    except (TypeError, ValueError, OperationFailure):
        return None
    cache_key = (
        _update_dialect_cache_key(dialect),
        tuple(operator_shapes),
        let_fingerprint,
    )
    return cache_key, tuple(values)


def _slot_map_from_update_plan_cache_key(
    cache_key: tuple[object, ...],
) -> dict[tuple[str, str], int]:
    _dialect_key, operator_shapes, _let_fingerprint = cache_key
    slot_map: dict[tuple[str, str], int] = {}
    slot = 0
    for operator, paths in operator_shapes:
        for path in paths:
            slot_map[(operator, path)] = slot
            slot += 1
    return slot_map


def _get_or_compile_update_plan_templates(
    cache_key: tuple[object, ...],
    *,
    update_spec: Update,
    dialect: MongoDialect,
    selector_filter: Filter,
) -> tuple[_UpdatePlanTemplate, _UpdatePlanTemplate]:
    with _UPDATE_PLAN_TEMPLATE_CACHE_LOCK:
        cached = _UPDATE_PLAN_TEMPLATE_CACHE.get(cache_key)
        if cached is not None:
            _UPDATE_PLAN_TEMPLATE_CACHE.move_to_end(cache_key)
            return cached

        compiled_update_plan = UpdateEngine.compile_update_plan(
            update_spec,
            dialect=dialect,
            selector_filter=selector_filter,
        )
        compiled_upsert_plan = UpdateEngine.compile_update_plan(
            update_spec,
            dialect=dialect,
            selector_filter=selector_filter,
            is_upsert_insert=True,
        )
        slot_map = _slot_map_from_update_plan_cache_key(cache_key)
        templates = (
            _UpdatePlanTemplate.from_plan(
                compiled_update_plan,
                slot_map=slot_map,
                is_upsert_insert=False,
            ),
            _UpdatePlanTemplate.from_plan(
                compiled_upsert_plan,
                slot_map=slot_map,
                is_upsert_insert=True,
            ),
        )
        _UPDATE_PLAN_TEMPLATE_CACHE[cache_key] = templates
        if len(_UPDATE_PLAN_TEMPLATE_CACHE) > _UPDATE_PLAN_TEMPLATE_CACHE_MAX_SIZE:
            _UPDATE_PLAN_TEMPLATE_CACHE.popitem(last=False)
        return templates


def _compile_update_plans(
    update_spec: Update | None,
    *,
    dialect: MongoDialect,
    selector_filter: Filter,
    collation: CollationDocument | None,
    array_filters: ArrayFilters | None,
    variables: Mapping[str, object] | None,
    planning_mode: PlanningMode,
) -> tuple[CompiledExecutableUpdatePlan | None, CompiledExecutableUpdatePlan | None]:
    if update_spec is None:
        return None, None
    shape_issue = _validate_update_spec_shape(update_spec)
    if shape_issue is not None:
        if planning_mode is PlanningMode.RELAXED:
            return None, None
        raise OperationFailure(shape_issue)
    try:
        template_cache_entry = _build_update_plan_template_cache_key(
            update_spec,
            dialect=dialect,
            collation=collation,
            array_filters=array_filters,
            variables=variables,
            planning_mode=planning_mode,
        )
        if template_cache_entry is not None:
            template_cache_key, values = template_cache_entry
            plan_template, upsert_template = _get_or_compile_update_plan_templates(
                template_cache_key,
                update_spec=update_spec,
                dialect=dialect,
                selector_filter=selector_filter,
            )
            return (
                plan_template.bind(
                    update_spec,
                    values,
                    dialect=dialect,
                    selector_filter=selector_filter,
                ),
                upsert_template.bind(
                    update_spec,
                    values,
                    dialect=dialect,
                    selector_filter=selector_filter,
                ),
            )
        return (
            UpdateEngine.compile_update_plan(
                update_spec,
                dialect=dialect,
                selector_filter=selector_filter,
                collation=collation,
                array_filters=array_filters,
                variables=variables,
            ),
            UpdateEngine.compile_update_plan(
                update_spec,
                dialect=dialect,
                selector_filter=selector_filter,
                collation=collation,
                array_filters=array_filters,
                is_upsert_insert=True,
                variables=variables,
            ),
        )
    except OperationFailure:
        if planning_mode is PlanningMode.RELAXED:
            return None, None
        raise


def _validate_update_spec_shape(update_spec: Update) -> str | None:
    if isinstance(update_spec, list):
        if not update_spec:
            return "update pipeline must be a non-empty list"
        return None
    if not isinstance(update_spec, dict):
        return "update specification must be a document or pipeline"
    if not update_spec:
        return "update_spec must not be empty"
    if not all(isinstance(operator, str) and operator.startswith("$") for operator in update_spec):
        return "update_spec must contain only update operators"
    invalid_operator = next(
        (
            operator
            for operator, params in update_spec.items()
            if not isinstance(params, dict)
        ),
        None,
    )
    if invalid_operator is not None:
        return f"{invalid_operator} value must be a dict"
    return None


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


def _projection_requests_text_score(projection: Projection | None) -> bool:
    if projection is None:
        return False
    return any(
        isinstance(value, dict) and value == {"$meta": "textScore"}
        for value in projection.values()
    )


def _sort_requests_text_score(sort: SortSpec | None) -> bool:
    if sort is None:
        return False
    return any(field == TEXT_SCORE_FIELD for field, _direction in sort)


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


def _normalize_let(let: object | None) -> Mapping[str, object] | None:
    if let is None:
        return None
    if isinstance(let, ExpressionExecutionContext):
        return let
    if not isinstance(let, dict):
        raise TypeError("let must be a dict")
    for name in let:
        if not isinstance(name, str) or not _LET_VARIABLE_RE.match(name):
            raise OperationFailure(
                "$lookup let variable names must begin with a lowercase letter or non-ascii character"
            )
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
