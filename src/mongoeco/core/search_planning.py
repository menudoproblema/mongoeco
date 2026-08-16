from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum

from mongoeco.core.aggregation.planning import Pipeline, _match_spec_contains_expr


class SearchPlanningMode(StrEnum):
    OPTIMIZED = "optimized"
    REFERENCE = "reference"


class SearchPipelineStrategy(StrEnum):
    FULL = "full"
    DIRECT_WINDOW = "direct-window"
    PREFIX_ITERATIVE = "prefix-iterative"
    EMPTY = "empty"


class CardinalityEffect(StrEnum):
    PRESERVE = "preserve"
    FILTER = "filter"
    REDUCE = "reduce"
    EXPAND = "expand"
    UNKNOWN = "unknown"


class OrderEffect(StrEnum):
    PRESERVE = "preserve"
    REORDER = "reorder"
    UNKNOWN = "unknown"


class SearchStageDomain(StrEnum):
    DOCUMENT = "document"
    STREAM = "stream"
    FULL_SET = "full-set"
    WRITEBACK = "writeback"
    UNKNOWN = "unknown"


class SearchPlanRule(StrEnum):
    DIRECT_WINDOW = "search.direct-window"
    PREFIX_MONOTONIC = "search.prefix-monotonic"
    DOWNSTREAM_FILTER = "search.downstream-filter"
    EMPTY_OUTPUT = "search.empty-output"
    FULL_EXECUTION = "search.full-execution"
    REFERENCE_ORACLE = "search.reference-oracle"


class SearchPlanRejection(StrEnum):
    METADATA_MODE = "search.metadata-mode"
    WRITEBACK = "search.writeback"
    COLLECTOR_FULL_SET = "search.collector-full-set"
    HIT_METADATA = "search.hit-metadata"
    VECTOR_POST_TOPK = "search.vector-post-top-k"
    EXPRESSION_FILTER = "search.expression-filter"
    UNSAFE_STAGE = "search.unsafe-stage"
    REFERENCE_MODE = "search.reference-mode"


class SearchPlanPhase(StrEnum):
    SEARCH = "search"
    DOWNSTREAM_FILTER = "downstream-filter"
    PIPELINE = "pipeline"
    WRITEBACK = "writeback"


class SearchPhaseOwner(StrEnum):
    ENGINE = "engine"
    SEMANTIC_CORE = "semantic-core"
    CURSOR = "cursor"


@dataclass(frozen=True, slots=True)
class SearchStageEffect:
    operator: str
    domain: SearchStageDomain
    cardinality: CardinalityEffect
    order: OrderEffect
    monotonic: bool
    topk_safe: bool
    downstream_filter_safe: bool
    materializes: bool = False
    writeback: bool = False
    requires_full_set: bool = False
    metadata_dependencies: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.operator, str) or not self.operator:
            message = "Search stage operator must be a non-empty string"
            raise ValueError(message)
        for field_name, expected_type in (
            ("domain", SearchStageDomain),
            ("cardinality", CardinalityEffect),
            ("order", OrderEffect),
        ):
            if not isinstance(getattr(self, field_name), expected_type):
                message = f"Search stage {field_name} has an invalid type"
                raise TypeError(message)
        for field_name in (
            "monotonic",
            "topk_safe",
            "downstream_filter_safe",
            "materializes",
            "writeback",
            "requires_full_set",
        ):
            if not isinstance(getattr(self, field_name), bool):
                message = f"Search stage {field_name} must be a bool"
                raise TypeError(message)
        if not isinstance(self.metadata_dependencies, tuple) or not all(
            isinstance(item, str) and item for item in self.metadata_dependencies
        ):
            message = "Search stage metadata dependencies must be strings"
            raise TypeError(message)
        if len(self.metadata_dependencies) != len(set(self.metadata_dependencies)):
            message = "Search stage metadata dependencies must be unique"
            raise ValueError(message)
        if self.writeback and self.domain is not SearchStageDomain.WRITEBACK:
            message = "Search writeback stage must use the writeback domain"
            raise ValueError(message)
        if self.requires_full_set and self.domain not in {
            SearchStageDomain.FULL_SET,
            SearchStageDomain.WRITEBACK,
        }:
            message = "full-set Search stage has an incompatible domain"
            raise ValueError(message)

    def to_document(self) -> dict[str, object]:
        return {
            "operator": self.operator,
            "domain": self.domain.value,
            "cardinality": self.cardinality.value,
            "order": self.order.value,
            "monotonic": self.monotonic,
            "topKSafe": self.topk_safe,
            "downstreamFilterSafe": self.downstream_filter_safe,
            "materializes": self.materializes,
            "writeback": self.writeback,
            "requiresFullSet": self.requires_full_set,
            "metadataDependencies": list(self.metadata_dependencies),
        }


@dataclass(frozen=True, slots=True)
class SearchWindow:
    skip: int = 0
    limit: int | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.skip, int)
            or isinstance(self.skip, bool)
            or self.skip < 0
            or (
                self.limit is not None
                and (
                    not isinstance(self.limit, int)
                    or isinstance(self.limit, bool)
                    or self.limit < 0
                )
            )
        ):
            message = "Search window values must be non-negative"
            raise ValueError(message)

    @property
    def fetch_limit(self) -> int | None:
        return None if self.limit is None else self.skip + self.limit


@dataclass(frozen=True, slots=True)
class SearchPhaseOwnership:
    phase: SearchPlanPhase
    owner: SearchPhaseOwner

    def __post_init__(self) -> None:
        if not isinstance(self.phase, SearchPlanPhase):
            message = "Search plan phase must be SearchPlanPhase"
            raise TypeError(message)
        if not isinstance(self.owner, SearchPhaseOwner):
            message = "Search phase owner must be SearchPhaseOwner"
            raise TypeError(message)

    def to_document(self) -> dict[str, str]:
        return {"phase": self.phase.value, "owner": self.owner.value}


@dataclass(frozen=True, slots=True)
class SearchPipelinePlan:
    strategy: SearchPipelineStrategy = SearchPipelineStrategy.FULL
    mode: SearchPlanningMode = SearchPlanningMode.OPTIMIZED
    window: SearchWindow | None = None
    result_limit_hint: int | None = None
    downstream_filter_spec: dict[str, object] | None = None
    prefix_output_limit: int | None = None
    residual_pipeline: tuple[dict[str, object], ...] = ()
    collectors: tuple[str, ...] = ()
    metadata_dependencies: tuple[str, ...] = ()
    effects: tuple[SearchStageEffect, ...] = ()
    applied_rules: tuple[SearchPlanRule, ...] = ()
    rejection_reasons: tuple[SearchPlanRejection, ...] = ()
    degradations: tuple[str, ...] = ()
    ownership: tuple[SearchPhaseOwnership, ...] = ()

    def __post_init__(self) -> None:
        self._validate_scalar_contracts()
        self._validate_tuple_contracts()
        self._validate_unique_contracts()
        self._validate_strategy_contract()
        self._validate_reference_contract()
        object.__setattr__(
            self,
            "downstream_filter_spec",
            deepcopy(self.downstream_filter_spec),
        )
        object.__setattr__(
            self,
            "residual_pipeline",
            deepcopy(self.residual_pipeline),
        )

    def _validate_scalar_contracts(self) -> None:
        if not isinstance(self.strategy, SearchPipelineStrategy):
            message = "Search strategy must be SearchPipelineStrategy"
            raise TypeError(message)
        if not isinstance(self.mode, SearchPlanningMode):
            message = "Search planning mode must be SearchPlanningMode"
            raise TypeError(message)
        if self.window is not None and not isinstance(self.window, SearchWindow):
            message = "Search plan window must be SearchWindow or None"
            raise TypeError(message)
        for field_name in ("result_limit_hint", "prefix_output_limit"):
            value = getattr(self, field_name)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                message = f"Search plan {field_name} must be non-negative"
                raise ValueError(message)
        if self.downstream_filter_spec is not None and not isinstance(
            self.downstream_filter_spec,
            dict,
        ):
            message = "Search downstream filter must be a document or None"
            raise TypeError(message)

    def _validate_tuple_contracts(self) -> None:
        tuple_contracts = (
            ("residual_pipeline", dict),
            ("collectors", str),
            ("metadata_dependencies", str),
            ("effects", SearchStageEffect),
            ("applied_rules", SearchPlanRule),
            ("rejection_reasons", SearchPlanRejection),
            ("degradations", str),
            ("ownership", SearchPhaseOwnership),
        )
        for field_name, item_type in tuple_contracts:
            value = getattr(self, field_name)
            if not isinstance(value, tuple) or not all(
                isinstance(item, item_type)
                and (not isinstance(item, str) or bool(item))
                for item in value
            ):
                message = f"Search plan {field_name} has an invalid shape"
                raise TypeError(message)

    def _validate_unique_contracts(self) -> None:
        for field_name in (
            "collectors",
            "metadata_dependencies",
            "applied_rules",
            "rejection_reasons",
            "degradations",
        ):
            value = getattr(self, field_name)
            if len(value) != len(set(value)):
                label = {
                    "applied_rules": "rules",
                    "rejection_reasons": "rejections",
                }.get(field_name, field_name)
                message = f"Search plan {label} must be unique"
                raise ValueError(message)
        phases = [item.phase for item in self.ownership]
        if len(phases) != len(set(phases)):
            message = "Search plan phase ownership must be unique"
            raise ValueError(message)

    def _validate_strategy_contract(self) -> None:
        if self.strategy is SearchPipelineStrategy.DIRECT_WINDOW:
            if self.result_limit_hint is None or self.result_limit_hint <= 0:
                message = "direct Search window requires a positive limit hint"
                raise ValueError(message)
        elif self.strategy is SearchPipelineStrategy.PREFIX_ITERATIVE:
            if self.prefix_output_limit is None or self.prefix_output_limit <= 0:
                message = "iterative Search prefix requires a positive output limit"
                raise ValueError(message)
        elif self.strategy is SearchPipelineStrategy.EMPTY:
            if self.result_limit_hint != 0 or self.prefix_output_limit is not None:
                message = "empty Search plan requires a zero result limit hint"
                raise ValueError(message)
        elif self.result_limit_hint is not None or self.prefix_output_limit is not None:
            message = "full Search plan cannot carry a limit optimization"
            raise ValueError(message)

    def _validate_reference_contract(self) -> None:
        if self.mode is SearchPlanningMode.REFERENCE and (
            self.strategy is not SearchPipelineStrategy.FULL
            or self.downstream_filter_spec is not None
        ):
            message = "reference Search plan cannot carry optimizations"
            raise ValueError(message)

    @property
    def explain_limit_hint(self) -> int | None:
        if self.strategy is SearchPipelineStrategy.PREFIX_ITERATIVE:
            return self.prefix_output_limit
        return self.result_limit_hint

    def to_document(self) -> dict[str, object]:
        return {
            "mode": self.mode.value,
            "strategy": self.strategy.value,
            "window": (
                {"skip": self.window.skip, "limit": self.window.limit}
                if self.window is not None
                else None
            ),
            "resultLimitHint": self.result_limit_hint,
            "downstreamFilter": deepcopy(self.downstream_filter_spec),
            "prefixOutputLimit": self.prefix_output_limit,
            "residualPipeline": deepcopy(list(self.residual_pipeline)),
            "collectors": list(self.collectors),
            "metadataDependencies": list(self.metadata_dependencies),
            "effects": [effect.to_document() for effect in self.effects],
            "appliedRules": [rule.value for rule in self.applied_rules],
            "rejectionReasons": [reason.value for reason in self.rejection_reasons],
            "degradations": list(self.degradations),
            "ownership": [item.to_document() for item in self.ownership],
        }


@dataclass(frozen=True, slots=True)
class _PlanInputs:
    residual_pipeline: tuple[dict[str, object], ...]
    collectors: tuple[str, ...]
    metadata_dependencies: tuple[str, ...]
    effects: tuple[SearchStageEffect, ...]
    ownership: tuple[SearchPhaseOwnership, ...]


_TRANSFORM_OPERATORS = frozenset(
    {"$project", "$unset", "$addFields", "$set", "$replaceRoot", "$replaceWith"},
)


def search_stage_effect(stage: object) -> SearchStageEffect:  # noqa: PLR0911
    if not isinstance(stage, dict) or len(stage) != 1:
        return SearchStageEffect(
            operator="<invalid>",
            domain=SearchStageDomain.UNKNOWN,
            cardinality=CardinalityEffect.UNKNOWN,
            order=OrderEffect.UNKNOWN,
            monotonic=False,
            topk_safe=False,
            downstream_filter_safe=False,
        )
    operator, spec = next(iter(stage.items()))
    if operator in _TRANSFORM_OPERATORS:
        return SearchStageEffect(
            operator=operator,
            domain=SearchStageDomain.DOCUMENT,
            cardinality=CardinalityEffect.PRESERVE,
            order=OrderEffect.PRESERVE,
            monotonic=True,
            topk_safe=True,
            downstream_filter_safe=False,
            materializes=True,
            metadata_dependencies=_metadata_dependencies(spec),
        )
    if operator == "$match":
        expression_filter = isinstance(spec, dict) and _match_spec_contains_expr(spec)
        return SearchStageEffect(
            operator=operator,
            domain=SearchStageDomain.DOCUMENT,
            cardinality=CardinalityEffect.FILTER,
            order=OrderEffect.PRESERVE,
            monotonic=True,
            topk_safe=False,
            downstream_filter_safe=not expression_filter,
            metadata_dependencies=_metadata_dependencies(spec),
        )
    if operator in {"$skip", "$limit"}:
        return SearchStageEffect(
            operator=operator,
            domain=SearchStageDomain.STREAM,
            cardinality=CardinalityEffect.REDUCE,
            order=OrderEffect.PRESERVE,
            monotonic=True,
            topk_safe=True,
            downstream_filter_safe=False,
        )
    if operator == "$unwind":
        return SearchStageEffect(
            operator=operator,
            domain=SearchStageDomain.DOCUMENT,
            cardinality=CardinalityEffect.EXPAND,
            order=OrderEffect.PRESERVE,
            monotonic=False,
            topk_safe=False,
            downstream_filter_safe=False,
            materializes=True,
        )
    if operator == "$merge":
        return SearchStageEffect(
            operator=operator,
            domain=SearchStageDomain.WRITEBACK,
            cardinality=CardinalityEffect.PRESERVE,
            order=OrderEffect.PRESERVE,
            monotonic=False,
            topk_safe=False,
            downstream_filter_safe=False,
            materializes=True,
            writeback=True,
            requires_full_set=True,
        )
    full_set = operator in {
        "$sort",
        "$group",
        "$facet",
        "$bucket",
        "$bucketAuto",
        "$sortByCount",
        "$count",
        "$setWindowFields",
    }
    return SearchStageEffect(
        operator=str(operator),
        domain=(SearchStageDomain.FULL_SET if full_set else SearchStageDomain.UNKNOWN),
        cardinality=CardinalityEffect.UNKNOWN,
        order=OrderEffect.UNKNOWN,
        monotonic=False,
        topk_safe=False,
        downstream_filter_safe=False,
        materializes=True,
        requires_full_set=full_set,
        metadata_dependencies=_metadata_dependencies(spec),
    )


def search_result_limit_hint(pipeline: Pipeline) -> int | None:
    effects = tuple(search_stage_effect(stage) for stage in pipeline)
    window = _trailing_window(pipeline, effects)
    return None if window is None else window.fetch_limit


def search_prefix_output_limit(pipeline: Pipeline) -> int | None:
    effects = tuple(search_stage_effect(stage) for stage in pipeline)
    return _prefix_output_limit(pipeline, effects)


def leading_search_downstream_filter_spec(
    pipeline: Pipeline,
) -> dict[str, object] | None:
    effects = tuple(search_stage_effect(stage) for stage in pipeline)
    return _leading_downstream_filter(pipeline, effects)


def compile_search_pipeline_plan(  # noqa: PLR0911, PLR0912
    operator: str,
    specification: object,
    pipeline: Pipeline,
    *,
    writeback: bool = False,
    mode: SearchPlanningMode = SearchPlanningMode.OPTIMIZED,
) -> SearchPipelinePlan:
    effects = tuple(search_stage_effect(stage) for stage in pipeline)
    residual = tuple(deepcopy(stage) for stage in pipeline if isinstance(stage, dict))
    collectors = tuple(
        name
        for name in ("count", "facet")
        if isinstance(specification, dict) and specification.get(name) is not None
    )
    metadata_dependencies = tuple(
        name
        for name in ("highlight", *collectors)
        if isinstance(specification, dict) and specification.get(name) is not None
    )
    ownership = (
        SearchPhaseOwnership(SearchPlanPhase.SEARCH, SearchPhaseOwner.ENGINE),
        SearchPhaseOwnership(SearchPlanPhase.PIPELINE, SearchPhaseOwner.SEMANTIC_CORE),
        *(
            (SearchPhaseOwnership(SearchPlanPhase.WRITEBACK, SearchPhaseOwner.CURSOR),)
            if writeback
            else ()
        ),
    )
    inputs = _PlanInputs(
        residual_pipeline=residual,
        collectors=collectors,
        metadata_dependencies=metadata_dependencies,
        effects=effects,
        ownership=ownership,
    )
    if mode is SearchPlanningMode.REFERENCE:
        return SearchPipelinePlan(
            mode=mode,
            residual_pipeline=residual,
            collectors=collectors,
            metadata_dependencies=metadata_dependencies,
            effects=effects,
            applied_rules=(SearchPlanRule.REFERENCE_ORACLE,),
            rejection_reasons=(SearchPlanRejection.REFERENCE_MODE,),
            ownership=ownership,
        )

    rejections: list[SearchPlanRejection] = []
    if operator == "$searchMeta":
        rejections.append(SearchPlanRejection.METADATA_MODE)
    if writeback:
        rejections.append(SearchPlanRejection.WRITEBACK)
    if collectors:
        rejections.append(SearchPlanRejection.COLLECTOR_FULL_SET)

    downstream_filter = None
    if not rejections:
        if operator == "$vectorSearch":
            rejections.append(SearchPlanRejection.VECTOR_POST_TOPK)
        elif "highlight" in metadata_dependencies:
            rejections.append(SearchPlanRejection.HIT_METADATA)
        else:
            downstream_filter = _leading_downstream_filter(pipeline, effects)

    if rejections and any(
        reason
        in {
            SearchPlanRejection.METADATA_MODE,
            SearchPlanRejection.WRITEBACK,
            SearchPlanRejection.COLLECTOR_FULL_SET,
        }
        for reason in rejections
    ):
        return _full_plan(
            inputs,
            rejections,
        )

    window = _trailing_window(pipeline, effects)
    if window is not None and window.fetch_limit == 0:
        return SearchPipelinePlan(
            strategy=SearchPipelineStrategy.EMPTY,
            window=window,
            result_limit_hint=0,
            downstream_filter_spec=downstream_filter,
            residual_pipeline=residual,
            collectors=collectors,
            metadata_dependencies=metadata_dependencies,
            effects=effects,
            applied_rules=(SearchPlanRule.EMPTY_OUTPUT,),
            rejection_reasons=tuple(rejections),
            ownership=ownership,
        )
    if window is not None and window.fetch_limit is not None:
        return SearchPipelinePlan(
            strategy=SearchPipelineStrategy.DIRECT_WINDOW,
            window=window,
            result_limit_hint=window.fetch_limit,
            downstream_filter_spec=downstream_filter,
            residual_pipeline=residual,
            collectors=collectors,
            metadata_dependencies=metadata_dependencies,
            effects=effects,
            applied_rules=(
                SearchPlanRule.DIRECT_WINDOW,
                *((SearchPlanRule.DOWNSTREAM_FILTER,) if downstream_filter else ()),
            ),
            rejection_reasons=tuple(rejections),
            ownership=ownership,
        )

    prefix_limit = _prefix_output_limit(pipeline, effects)
    if prefix_limit == 0:
        return SearchPipelinePlan(
            strategy=SearchPipelineStrategy.EMPTY,
            result_limit_hint=0,
            residual_pipeline=residual,
            collectors=collectors,
            metadata_dependencies=metadata_dependencies,
            effects=effects,
            applied_rules=(SearchPlanRule.EMPTY_OUTPUT,),
            rejection_reasons=tuple(rejections),
            ownership=ownership,
        )
    if prefix_limit is not None:
        return SearchPipelinePlan(
            strategy=SearchPipelineStrategy.PREFIX_ITERATIVE,
            downstream_filter_spec=downstream_filter,
            prefix_output_limit=prefix_limit,
            residual_pipeline=residual,
            collectors=collectors,
            metadata_dependencies=metadata_dependencies,
            effects=effects,
            applied_rules=(
                SearchPlanRule.PREFIX_MONOTONIC,
                *((SearchPlanRule.DOWNSTREAM_FILTER,) if downstream_filter else ()),
            ),
            rejection_reasons=tuple(rejections),
            ownership=ownership,
        )
    if any(not effect.monotonic for effect in effects):
        rejections.append(SearchPlanRejection.UNSAFE_STAGE)
    return _full_plan(
        inputs,
        rejections,
        downstream_filter=downstream_filter,
    )


def _full_plan(
    inputs: _PlanInputs,
    rejections: list[SearchPlanRejection],
    *,
    downstream_filter: dict[str, object] | None = None,
) -> SearchPipelinePlan:
    return SearchPipelinePlan(
        downstream_filter_spec=downstream_filter,
        residual_pipeline=inputs.residual_pipeline,
        collectors=inputs.collectors,
        metadata_dependencies=inputs.metadata_dependencies,
        effects=inputs.effects,
        applied_rules=(
            SearchPlanRule.FULL_EXECUTION,
            *((SearchPlanRule.DOWNSTREAM_FILTER,) if downstream_filter else ()),
        ),
        rejection_reasons=tuple(dict.fromkeys(rejections)),
        ownership=inputs.ownership,
    )


def _trailing_window(
    pipeline: Pipeline,
    effects: tuple[SearchStageEffect, ...],
) -> SearchWindow | None:
    skip = 0
    limit: int | None = None
    seen_window = False
    for stage, effect in zip(pipeline, effects, strict=True):
        operator, spec = (
            next(iter(stage.items()))
            if isinstance(stage, dict) and len(stage) == 1
            else (None, None)
        )
        if operator == "$skip":
            seen_window = True
            skip += int(spec)
        elif operator == "$limit":
            seen_window = True
            value = int(spec)
            limit = value if limit is None else min(limit, value)
        elif seen_window or not effect.topk_safe:
            return None
    return SearchWindow(skip, limit) if limit is not None else None


def _prefix_output_limit(
    pipeline: Pipeline,
    effects: tuple[SearchStageEffect, ...],
) -> int | None:
    output_limit: int | None = None
    seen_limit = False
    for stage, effect in zip(pipeline, effects, strict=True):
        if not effect.monotonic:
            return None
        operator, spec = (
            next(iter(stage.items()))
            if isinstance(stage, dict) and len(stage) == 1
            else (None, None)
        )
        if operator == "$limit":
            seen_limit = True
            value = int(spec)
            output_limit = value if output_limit is None else min(output_limit, value)
        elif operator == "$skip" and output_limit is not None:
            output_limit = max(output_limit - int(spec), 0)
    return output_limit if seen_limit else None


def _leading_downstream_filter(
    pipeline: Pipeline,
    effects: tuple[SearchStageEffect, ...],
) -> dict[str, object] | None:
    clauses: list[dict[str, object]] = []
    for stage, effect in zip(pipeline, effects, strict=True):
        if effect.operator != "$match":
            break
        spec = stage.get("$match") if isinstance(stage, dict) else None
        if not effect.downstream_filter_safe or not isinstance(spec, dict):
            return None
        clauses.append(deepcopy(spec))
    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else {"$and": clauses}


def _metadata_dependencies(value: object) -> tuple[str, ...]:
    found: set[str] = set()

    def visit(current: object) -> None:
        if isinstance(current, dict):
            if current.get("$meta") in {
                "searchHighlights",
                "textScore",
                "vectorSearchScore",
            }:
                found.add(str(current["$meta"]))
            for key, item in current.items():
                if isinstance(key, str) and (
                    key == "searchHighlights" or key.startswith("searchHighlights.")
                ):
                    found.add("searchHighlights")
                visit(item)
        elif isinstance(current, list):
            for item in current:
                visit(item)
        elif isinstance(current, str) and (
            current == "$searchHighlights" or current.startswith("$searchHighlights.")
        ):
            found.add("searchHighlights")

    visit(value)
    return tuple(sorted(found))
