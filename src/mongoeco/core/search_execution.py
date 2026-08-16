from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.search import SearchQuery, compile_search_stage
from mongoeco.core.search_models import SearchExecutionMode
from mongoeco.core.search_planning import SearchPipelinePlan, SearchPipelineStrategy


@dataclass(frozen=True, slots=True)
class SearchRequest:
    operator: str
    specification: object
    query: SearchQuery
    mode: SearchExecutionMode
    operation_context: OperationContext
    runtime_operator: str | None = None
    runtime_specification: object | None = None
    max_time_ms: int | None = None
    result_limit_hint: int | None = None
    downstream_filter_spec: dict[str, object] | None = None
    pipeline_plan: SearchPipelinePlan | None = None

    def __post_init__(self) -> None:
        if self.operator not in {"$search", "$searchMeta", "$vectorSearch"}:
            message = "search operator is not supported"
            raise ValueError(message)
        if self.runtime_operator is not None and self.runtime_operator not in {
            "$search",
            "$vectorSearch",
        }:
            message = "runtime search operator is not supported"
            raise ValueError(message)
        if not isinstance(self.mode, SearchExecutionMode):
            message = "search mode must be a SearchExecutionMode"
            raise TypeError(message)
        if not isinstance(self.operation_context, OperationContext):
            message = "search requests require OperationContext"
            raise TypeError(message)
        if self.pipeline_plan is not None and not isinstance(
            self.pipeline_plan,
            SearchPipelinePlan,
        ):
            message = "search pipeline_plan must be SearchPipelinePlan or None"
            raise TypeError(message)
        self._validate_execution_shape()
        self._validate_limits()
        self._validate_pipeline_plan()
        self._own_inputs()
        if compile_search_stage(self.operator, self.specification) != self.query:
            msg = "compiled search query diverges from its specification"
            raise ValueError(msg)

    def _validate_execution_shape(self) -> None:
        if self.operator == "$searchMeta":
            if self.mode is not SearchExecutionMode.METADATA:
                msg = "$searchMeta requires metadata execution mode"
                raise ValueError(msg)
            if self.runtime_operator != "$search":
                msg = "$searchMeta must lower through $search"
                raise ValueError(msg)
            if (
                self.result_limit_hint is not None
                or self.downstream_filter_spec is not None
            ):
                msg = "$searchMeta cannot receive hit-domain optimizations"
                raise ValueError(msg)
        elif self.mode is not SearchExecutionMode.HITS:
            msg = "$search and $vectorSearch require hits execution mode"
            raise ValueError(msg)
        elif (
            self.runtime_operator is not None or self.runtime_specification is not None
        ):
            msg = "runtime overrides are reserved for $searchMeta lowering"
            raise ValueError(msg)
        if self.downstream_filter_spec is None:
            return
        if self.operator == "$vectorSearch":
            msg = "$vectorSearch cannot receive a downstream pipeline filter"
            raise ValueError(msg)
        stage_options = getattr(self.query, "stage_options", None)
        if stage_options is not None and (
            getattr(stage_options, "highlight", None) is not None
            or getattr(stage_options, "count", None) is not None
            or getattr(stage_options, "facet", None) is not None
        ):
            msg = "Search metadata and collectors cannot receive a downstream filter"
            raise ValueError(msg)

    def _validate_limits(self) -> None:
        if self.max_time_ms is not None and (
            not isinstance(self.max_time_ms, int)
            or isinstance(self.max_time_ms, bool)
            or self.max_time_ms <= 0
        ):
            message = "max_time_ms must be a positive integer"
            raise ValueError(message)
        if self.result_limit_hint is not None and (
            not isinstance(self.result_limit_hint, int)
            or isinstance(self.result_limit_hint, bool)
            or self.result_limit_hint < 0
        ):
            message = "result_limit_hint must be a non-negative integer"
            raise ValueError(message)

    def _validate_pipeline_plan(self) -> None:
        if self.pipeline_plan is None:
            return
        if self.pipeline_plan.downstream_filter_spec != self.downstream_filter_spec:
            message = "search request filter must come from its pipeline plan"
            raise ValueError(message)
        strategy = self.pipeline_plan.strategy
        if strategy in {
            SearchPipelineStrategy.DIRECT_WINDOW,
            SearchPipelineStrategy.EMPTY,
        } and (self.result_limit_hint != self.pipeline_plan.result_limit_hint):
            message = "search request limit must come from its pipeline plan"
            raise ValueError(message)
        if strategy is SearchPipelineStrategy.PREFIX_ITERATIVE and (
            self.result_limit_hint is None
            or self.pipeline_plan.prefix_output_limit is None
            or self.result_limit_hint < self.pipeline_plan.prefix_output_limit
        ):
            message = "iterative Search fetch must cover its planned output limit"
            raise ValueError(message)
        if (
            strategy is SearchPipelineStrategy.FULL
            and self.result_limit_hint is not None
        ):
            message = "full Search plan cannot carry an execution limit"
            raise ValueError(message)

    def _own_inputs(self) -> None:
        object.__setattr__(self, "query", deepcopy(self.query))
        object.__setattr__(
            self,
            "specification",
            DocumentCodec.to_internal(self.specification),
        )
        if self.runtime_specification is not None:
            object.__setattr__(
                self,
                "runtime_specification",
                DocumentCodec.to_internal(self.runtime_specification),
            )
        if self.downstream_filter_spec is not None:
            object.__setattr__(
                self,
                "downstream_filter_spec",
                DocumentCodec.to_internal(self.downstream_filter_spec),
            )

    @property
    def effective_operator(self) -> str:
        return self.runtime_operator or self.operator

    @property
    def effective_specification(self) -> object:
        if self.runtime_specification is not None:
            return self.runtime_specification
        return self.specification
