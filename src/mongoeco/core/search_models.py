from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
    RuntimeMetadata,
    runtime_state_from_legacy_document,
)


if TYPE_CHECKING:
    from mongoeco.types import Document


class SearchExecutionMode(StrEnum):
    HITS = "hits"
    METADATA = "metadata"


class SearchExplainVerbosity(StrEnum):
    QUERY_PLANNER = "queryPlanner"
    EXECUTION_STATS = "executionStats"


@dataclass(frozen=True, slots=True)
class SearchFacetDefinition:
    name: str | None
    path: str
    facet_type: str = "string"
    num_buckets: int = 10
    include_meta: bool = False

    def __post_init__(self) -> None:
        if self.name is not None and (not isinstance(self.name, str) or not self.name):
            message = "facet name must be None or a non-empty string"
            raise ValueError(message)
        if not isinstance(self.path, str) or not self.path:
            message = "facet path must be a non-empty string"
            raise ValueError(message)
        if not isinstance(self.facet_type, str) or not self.facet_type:
            message = "facet type must be a non-empty string"
            raise ValueError(message)
        if (
            not isinstance(self.num_buckets, int)
            or isinstance(self.num_buckets, bool)
            or self.num_buckets <= 0
        ):
            message = "facet num_buckets must be a positive integer"
            raise ValueError(message)
        if not isinstance(self.include_meta, bool):
            message = "facet include_meta must be a bool"
            raise TypeError(message)


@dataclass(frozen=True, slots=True)
class SearchCountResult:
    mode: str
    value: int
    exact: bool
    threshold: int | None = None
    capped_by_threshold: bool = False

    def __post_init__(self) -> None:
        if self.mode not in {"total", "lowerBound"}:
            msg = "count mode must be total or lowerBound"
            raise ValueError(msg)
        if (
            not isinstance(self.value, int)
            or isinstance(self.value, bool)
            or self.value < 0
        ):
            msg = "count value must be a non-negative integer"
            raise ValueError(msg)
        if not isinstance(self.exact, bool) or not isinstance(
            self.capped_by_threshold,
            bool,
        ):
            msg = "count flags must be bool values"
            raise TypeError(msg)
        if self.threshold is not None and (
            not isinstance(self.threshold, int)
            or isinstance(self.threshold, bool)
            or self.threshold <= 0
        ):
            msg = "count threshold must be a positive integer"
            raise ValueError(msg)
        if self.mode == "total" and (
            self.threshold is not None or self.capped_by_threshold
        ):
            msg = "total count cannot carry lower-bound state"
            raise ValueError(msg)
        if self.capped_by_threshold and (
            self.mode != "lowerBound" or self.threshold is None or self.exact
        ):
            msg = "capped lower-bound count must be inexact and thresholded"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class SearchFacetBucket:
    value: object
    count: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.count, int)
            or isinstance(self.count, bool)
            or self.count <= 0
        ):
            msg = "facet bucket count must be a positive integer"
            raise ValueError(msg)
        object.__setattr__(self, "value", deepcopy(self.value))


@dataclass(frozen=True, slots=True)
class SearchFacetResult:
    definition: SearchFacetDefinition
    buckets: tuple[SearchFacetBucket, ...]
    distinct_value_count: int | None = None
    counted_value_count: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.definition, SearchFacetDefinition):
            msg = "facet result requires SearchFacetDefinition"
            raise TypeError(msg)
        if not isinstance(self.buckets, tuple) or not all(
            isinstance(bucket, SearchFacetBucket) for bucket in self.buckets
        ):
            msg = "facet buckets must be a tuple of SearchFacetBucket"
            raise TypeError(msg)
        for name, value in (
            ("distinct_value_count", self.distinct_value_count),
            ("counted_value_count", self.counted_value_count),
        ):
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                msg = f"{name} must be a non-negative integer"
                raise ValueError(msg)
        if self.distinct_value_count is not None and self.distinct_value_count < len(
            self.buckets
        ):
            msg = "distinct_value_count cannot be smaller than bucket count"
            raise ValueError(msg)
        if self.counted_value_count is not None and self.counted_value_count < sum(
            bucket.count for bucket in self.buckets
        ):
            msg = "counted_value_count cannot be smaller than bucket counts"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True, order=True)
class SearchHighlightSpan:
    start: int
    end: int

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, int) or isinstance(value, bool)
            for value in (self.start, self.end)
        ):
            msg = "highlight span offsets must be integers"
            raise TypeError(msg)
        if self.start < 0 or self.end <= self.start:
            message = "highlight span requires 0 <= start < end"
            raise ValueError(message)


@dataclass(frozen=True, slots=True)
class SearchHighlightSegment:
    segment_type: str
    value: str
    start: int
    end: int

    def __post_init__(self) -> None:
        if not isinstance(self.segment_type, str):
            msg = "highlight segment type must be a string"
            raise TypeError(msg)
        if self.segment_type not in {"hit", "text"}:
            msg = "highlight segment type must be hit or text"
            raise ValueError(msg)
        if not isinstance(self.value, str):
            msg = "highlight segment value must be a string"
            raise TypeError(msg)
        if any(
            not isinstance(value, int) or isinstance(value, bool)
            for value in (self.start, self.end)
        ):
            msg = "highlight segment offsets must be integers"
            raise TypeError(msg)
        if self.start < 0 or self.end <= self.start:
            msg = "highlight segment requires 0 <= start < end"
            raise ValueError(msg)
        if len(self.value) != self.end - self.start:
            msg = "highlight segment offsets must cover its value"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class SearchHighlightPassage:
    text: str
    start: int
    end: int
    segments: tuple[SearchHighlightSegment, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.text, str):
            msg = "highlight passage text must be a string"
            raise TypeError(msg)
        if any(
            not isinstance(value, int) or isinstance(value, bool)
            for value in (self.start, self.end)
        ):
            msg = "highlight passage offsets must be integers"
            raise TypeError(msg)
        if self.start < 0 or self.end <= self.start:
            msg = "highlight passage requires 0 <= start < end"
            raise ValueError(msg)
        if len(self.text) != self.end - self.start:
            msg = "highlight passage offsets must cover its text"
            raise ValueError(msg)
        if not isinstance(self.segments, tuple) or not all(
            isinstance(segment, SearchHighlightSegment) for segment in self.segments
        ):
            msg = "highlight passage segments must be a tuple of segments"
            raise TypeError(msg)
        if not self.segments:
            msg = "highlight passage requires segments"
            raise ValueError(msg)
        cursor = self.start
        for segment in self.segments:
            if segment.start != cursor or segment.end > self.end:
                msg = "highlight segments must be contiguous and bounded"
                raise ValueError(msg)
            cursor = segment.end
        if cursor != self.end:
            msg = "highlight segments must cover the complete passage"
            raise ValueError(msg)
        if "".join(segment.value for segment in self.segments) != self.text:
            msg = "highlight segment values must reconstruct the passage text"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class SearchMetadata:
    count: SearchCountResult | None = None
    facets: tuple[SearchFacetResult, ...] = ()

    def __post_init__(self) -> None:
        if self.count is not None and not isinstance(self.count, SearchCountResult):
            msg = "search metadata count must be SearchCountResult"
            raise TypeError(msg)
        if not isinstance(self.facets, tuple) or not all(
            isinstance(facet, SearchFacetResult) for facet in self.facets
        ):
            msg = "search metadata facets must be a tuple"
            raise TypeError(msg)
        names = [facet.definition.name for facet in self.facets]
        named = [name for name in names if name is not None]
        if named and len(named) != len(names):
            msg = "search metadata cannot mix named and unnamed facets"
            raise ValueError(msg)
        if len(named) != len(set(named)):
            msg = "search metadata facet names must be unique"
            raise ValueError(msg)
        if not named and len(names) > 1:
            msg = "search metadata supports only one unnamed facet"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class SearchCollectorPlan:
    backend: str
    pushed_down: bool
    candidate_exact: bool | None
    count_strategy: str | None = None
    facet_strategy: str | None = None
    fallback_reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.backend, str) or not self.backend:
            msg = "collector backend must be a non-empty string"
            raise ValueError(msg)
        if not isinstance(self.pushed_down, bool):
            msg = "collector pushed_down must be a bool"
            raise TypeError(msg)
        if self.candidate_exact is not None and not isinstance(
            self.candidate_exact,
            bool,
        ):
            msg = "collector candidate_exact must be a bool or None"
            raise TypeError(msg)
        for field_name in (
            "count_strategy",
            "facet_strategy",
            "fallback_reason",
        ):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                msg = f"collector {field_name} must be a non-empty string or None"
                raise ValueError(msg)
        if self.pushed_down and self.candidate_exact is not True:
            msg = "collector pushdown requires an exact candidate set"
            raise ValueError(msg)

    def to_document(self) -> dict[str, object | None]:
        return {
            "backend": self.backend,
            "pushedDown": self.pushed_down,
            "candidateExact": self.candidate_exact,
            "countStrategy": self.count_strategy,
            "facetStrategy": self.facet_strategy,
            "fallbackReason": self.fallback_reason,
        }


class SearchExecutionState(StrEnum):
    PLANNED = "planned"
    EXECUTED = "executed"
    REJECTED = "rejected"
    UNAVAILABLE = "unavailable"


class SearchExecutionPhase(StrEnum):
    QUERY = "query"
    RESIDUAL_FILTER = "residual-filter"
    COLLECTOR = "collector"


class SearchMetricName(StrEnum):
    QUERY_MATCHED_COUNT = "queryMatchedCount"
    RETURNED_HIT_COUNT = "returnedHitCount"
    DOWNSTREAM_FILTERED_COUNT = "downstreamFilteredCount"
    CANDIDATE_COUNT = "candidateCount"
    DOCUMENTS_SCANNED = "documentsScanned"
    COLLECTOR_COUNT = "collectorCount"
    COLLECTOR_DOCUMENT_COUNT = "collectorDocumentCount"
    PIPELINE_OUTPUT_COUNT = "pipelineOutputCount"


class SearchMetricDomain(StrEnum):
    QUERY = "query"
    RESULT = "result"
    PIPELINE = "pipeline"
    CANDIDATE = "candidate"
    STORAGE = "storage"
    COLLECTOR = "collector"


class SearchMetricExactness(StrEnum):
    EXACT = "exact"
    LOWER_BOUND = "lower-bound"
    ESTIMATE = "estimate"
    UNKNOWN = "unknown"


class SearchMetricOrigin(StrEnum):
    ENGINE = "engine"
    SEMANTIC_CORE = "semantic-core"
    PLANNER = "planner"
    ADAPTER = "adapter"


class SearchMetricAvailability(StrEnum):
    AVAILABLE = "available"
    NOT_APPLICABLE = "not-applicable"
    UNAVAILABLE = "unavailable"


_METRIC_DOMAINS = {
    SearchMetricName.QUERY_MATCHED_COUNT: SearchMetricDomain.QUERY,
    SearchMetricName.RETURNED_HIT_COUNT: SearchMetricDomain.RESULT,
    SearchMetricName.DOWNSTREAM_FILTERED_COUNT: SearchMetricDomain.PIPELINE,
    SearchMetricName.CANDIDATE_COUNT: SearchMetricDomain.CANDIDATE,
    SearchMetricName.DOCUMENTS_SCANNED: SearchMetricDomain.STORAGE,
    SearchMetricName.COLLECTOR_COUNT: SearchMetricDomain.COLLECTOR,
    SearchMetricName.COLLECTOR_DOCUMENT_COUNT: SearchMetricDomain.COLLECTOR,
    SearchMetricName.PIPELINE_OUTPUT_COUNT: SearchMetricDomain.PIPELINE,
}


@dataclass(frozen=True, slots=True)
class SearchExecutionMetric:
    name: SearchMetricName
    domain: SearchMetricDomain
    value: int | None
    exactness: SearchMetricExactness = SearchMetricExactness.EXACT
    origin: SearchMetricOrigin = SearchMetricOrigin.ENGINE
    availability: SearchMetricAvailability = SearchMetricAvailability.AVAILABLE

    def __post_init__(self) -> None:
        if not isinstance(self.name, SearchMetricName):
            message = "Search metric name must be SearchMetricName"
            raise TypeError(message)
        if not isinstance(self.domain, SearchMetricDomain):
            message = "Search metric domain must be SearchMetricDomain"
            raise TypeError(message)
        if self.domain is not _METRIC_DOMAINS[self.name]:
            message = "Search metric domain does not match its name"
            raise ValueError(message)
        if not isinstance(self.exactness, SearchMetricExactness):
            message = "Search metric exactness must be SearchMetricExactness"
            raise TypeError(message)
        if not isinstance(self.origin, SearchMetricOrigin):
            message = "Search metric origin must be SearchMetricOrigin"
            raise TypeError(message)
        if not isinstance(self.availability, SearchMetricAvailability):
            message = "Search metric availability must be SearchMetricAvailability"
            raise TypeError(message)
        if self.availability is SearchMetricAvailability.AVAILABLE:
            if (
                not isinstance(self.value, int)
                or isinstance(self.value, bool)
                or self.value < 0
            ):
                message = "available Search metric requires a non-negative integer"
                raise ValueError(message)
        elif self.value is not None:
            message = "unavailable Search metric cannot carry a value"
            raise ValueError(message)
        if (
            self.availability is not SearchMetricAvailability.AVAILABLE
            and self.exactness is not SearchMetricExactness.UNKNOWN
        ):
            message = "unavailable Search metric must have unknown exactness"
            raise ValueError(message)

    def to_document(self) -> dict[str, object | None]:
        return {
            "name": self.name.value,
            "domain": self.domain.value,
            "value": self.value,
            "exactness": self.exactness.value,
            "origin": self.origin.value,
            "availability": self.availability.value,
        }


@dataclass(frozen=True, slots=True)
class SearchDegradation:
    code: str
    message: str
    origin: SearchMetricOrigin = SearchMetricOrigin.ENGINE

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code:
            message = "Search degradation code must be a non-empty string"
            raise ValueError(message)
        if not isinstance(self.message, str) or not self.message:
            message = "Search degradation message must be a non-empty string"
            raise ValueError(message)
        if not isinstance(self.origin, SearchMetricOrigin):
            message = "Search degradation origin must be SearchMetricOrigin"
            raise TypeError(message)

    def to_document(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "origin": self.origin.value,
        }


@dataclass(frozen=True, slots=True)
class SearchExecutionTrace:
    backend: str
    operation_id: str | None = None
    matched_count: int | None = None
    query_matched_count: int | None = None
    returned_hit_count: int | None = None
    downstream_filtered_count: int | None = None
    candidate_count: int | None = None
    documents_scanned: int | None = None
    collector_document_count: int | None = None
    pipeline_output_count: int | None = None
    executed: bool | None = None
    execution_state: SearchExecutionState | None = None
    context_bound: bool | None = None
    snapshot_captured: bool | None = None
    phases: tuple[SearchExecutionPhase, ...] = ()
    metrics: tuple[SearchExecutionMetric, ...] = ()
    collector_backend: str | None = None
    collector_pushdown: bool = False
    collector_count: int = 0
    collector_plan: SearchCollectorPlan | None = None
    degradations: tuple[str, ...] = ()
    degradation_details: tuple[SearchDegradation, ...] = ()
    engine_details: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._validate_scalar_fields()
        self._normalize_execution_state()
        self._validate_structured_fields()
        object.__setattr__(self, "engine_details", deepcopy(self.engine_details))
        self._normalize_metrics()
        self._normalize_degradations()
        self._normalize_phases()
        self._validate_metric_relationships()
        if self.collector_pushdown and (
            self.collector_plan is None or not self.collector_plan.pushed_down
        ):
            msg = "collector pushdown trace requires a pushed-down collector plan"
            raise ValueError(msg)

    def _validate_scalar_fields(self) -> None:
        if not isinstance(self.backend, str) or not self.backend:
            msg = "search trace backend must be a non-empty string"
            raise ValueError(msg)
        if self.operation_id is not None and (
            not isinstance(self.operation_id, str) or not self.operation_id
        ):
            msg = "search trace operation_id must be non-empty or None"
            raise ValueError(msg)
        for field_name in (
            "matched_count",
            "query_matched_count",
            "returned_hit_count",
            "downstream_filtered_count",
            "candidate_count",
            "documents_scanned",
            "collector_document_count",
            "pipeline_output_count",
        ):
            value = getattr(self, field_name)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                msg = f"search trace {field_name} must be non-negative or None"
                raise ValueError(msg)
        if (
            not isinstance(self.collector_count, int)
            or isinstance(self.collector_count, bool)
            or self.collector_count < 0
        ):
            msg = "search trace collector_count must be non-negative"
            raise ValueError(msg)
        if not isinstance(self.collector_pushdown, bool):
            msg = "search trace collector_pushdown must be a bool"
            raise TypeError(msg)
        for field_name in ("context_bound", "snapshot_captured"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, bool):
                msg = f"search trace {field_name} must be a bool or None"
                raise TypeError(msg)

    def _normalize_execution_state(self) -> None:
        state = self.execution_state
        if state is not None and not isinstance(state, SearchExecutionState):
            msg = "search trace state must be SearchExecutionState or None"
            raise TypeError(msg)
        if self.executed is not None and not isinstance(self.executed, bool):
            msg = "search trace executed must be a bool or None"
            raise TypeError(msg)
        if state is None:
            state = (
                SearchExecutionState.EXECUTED
                if self.executed is not False
                else SearchExecutionState.PLANNED
            )
        derived_executed = state is SearchExecutionState.EXECUTED
        if self.executed is not None and self.executed is not derived_executed:
            msg = "search trace executed alias contradicts execution_state"
            raise ValueError(msg)
        object.__setattr__(self, "execution_state", state)
        object.__setattr__(self, "executed", derived_executed)
        context_bound = self.context_bound
        if context_bound is None and self.operation_id is not None:
            context_bound = True
        if context_bound is True and self.operation_id is None:
            msg = "bound Search execution context requires operation_id"
            raise ValueError(msg)
        if state is SearchExecutionState.EXECUTED and context_bound is False:
            msg = "executed Search trace requires a bound context"
            raise ValueError(msg)
        if state is SearchExecutionState.EXECUTED and self.snapshot_captured is False:
            msg = "executed Search trace requires a captured snapshot"
            raise ValueError(msg)
        object.__setattr__(self, "context_bound", context_bound)

    def _validate_structured_fields(self) -> None:
        if self.collector_backend is not None and (
            not isinstance(self.collector_backend, str) or not self.collector_backend
        ):
            msg = "search trace collector_backend must be non-empty or None"
            raise ValueError(msg)
        if self.collector_plan is not None and not isinstance(
            self.collector_plan,
            SearchCollectorPlan,
        ):
            msg = "search trace collector_plan must be SearchCollectorPlan or None"
            raise TypeError(msg)
        if not isinstance(self.degradations, tuple) or not all(
            isinstance(item, str) and item for item in self.degradations
        ):
            msg = "search trace degradations must be non-empty strings in a tuple"
            raise TypeError(msg)
        if len(self.degradations) != len(set(self.degradations)):
            msg = "search trace degradations must be unique"
            raise ValueError(msg)
        if not isinstance(self.metrics, tuple) or not all(
            isinstance(item, SearchExecutionMetric) for item in self.metrics
        ):
            msg = "search trace metrics must be SearchExecutionMetric values"
            raise TypeError(msg)
        if not isinstance(self.phases, tuple) or not all(
            isinstance(item, SearchExecutionPhase) for item in self.phases
        ):
            msg = "search trace phases must be SearchExecutionPhase values"
            raise TypeError(msg)
        if len(self.phases) != len(set(self.phases)):
            msg = "search trace phases must be unique"
            raise ValueError(msg)
        if len({metric.name for metric in self.metrics}) != len(self.metrics):
            msg = "search trace metric names must be unique"
            raise ValueError(msg)
        if not isinstance(self.degradation_details, tuple) or not all(
            isinstance(item, SearchDegradation) for item in self.degradation_details
        ):
            msg = "search degradation details must be SearchDegradation values"
            raise TypeError(msg)
        if not isinstance(self.engine_details, dict):
            msg = "search engine_details must be a document"
            raise TypeError(msg)

    def _normalize_metrics(self) -> None:
        aliases = {
            SearchMetricName.QUERY_MATCHED_COUNT: "query_matched_count",
            SearchMetricName.RETURNED_HIT_COUNT: "returned_hit_count",
            SearchMetricName.DOWNSTREAM_FILTERED_COUNT: "downstream_filtered_count",
            SearchMetricName.CANDIDATE_COUNT: "candidate_count",
            SearchMetricName.DOCUMENTS_SCANNED: "documents_scanned",
            SearchMetricName.COLLECTOR_COUNT: "collector_count",
            SearchMetricName.COLLECTOR_DOCUMENT_COUNT: "collector_document_count",
            SearchMetricName.PIPELINE_OUTPUT_COUNT: "pipeline_output_count",
        }
        metrics = list(self.metrics)
        by_name = {metric.name: metric for metric in metrics}
        for name, field_name in aliases.items():
            alias_value = getattr(self, field_name)
            metric = by_name.get(name)
            if metric is not None:
                alias_was_supplied = alias_value is not None and not (
                    field_name == "collector_count" and alias_value == 0
                )
                if alias_was_supplied and alias_value != metric.value:
                    message = f"search trace {field_name} contradicts canonical metric"
                    raise ValueError(message)
                object.__setattr__(self, field_name, metric.value)
                continue
            if alias_value is None or (
                field_name == "collector_count" and alias_value == 0
            ):
                continue
            origin = (
                SearchMetricOrigin.SEMANTIC_CORE
                if name is SearchMetricName.COLLECTOR_COUNT
                and self.collector_backend == "semantic-core"
                else SearchMetricOrigin.ENGINE
            )
            metrics.append(
                SearchExecutionMetric(
                    name=name,
                    domain=_METRIC_DOMAINS[name],
                    value=alias_value,
                    origin=origin,
                )
            )
        if self.matched_count is None:
            object.__setattr__(self, "matched_count", self.query_matched_count)
        elif self.query_matched_count is None:
            object.__setattr__(self, "query_matched_count", self.matched_count)
            if SearchMetricName.QUERY_MATCHED_COUNT not in {
                item.name for item in metrics
            }:
                metrics.append(
                    SearchExecutionMetric(
                        name=SearchMetricName.QUERY_MATCHED_COUNT,
                        domain=SearchMetricDomain.QUERY,
                        value=self.matched_count,
                    )
                )
        represented = {metric.name for metric in metrics}
        for name, domain in _METRIC_DOMAINS.items():
            if name in represented:
                continue
            metrics.append(
                SearchExecutionMetric(
                    name=name,
                    domain=domain,
                    value=None,
                    exactness=SearchMetricExactness.UNKNOWN,
                    origin=SearchMetricOrigin.ENGINE,
                    availability=SearchMetricAvailability.UNAVAILABLE,
                ),
            )
        object.__setattr__(self, "metrics", tuple(metrics))
        if self.execution_state is not SearchExecutionState.EXECUTED and any(
            metric.availability is SearchMetricAvailability.AVAILABLE
            for metric in metrics
        ):
            message = "non-executed Search trace cannot carry available metrics"
            raise ValueError(message)

    def _normalize_degradations(self) -> None:
        details = list(self.degradation_details)
        known_codes = {item.code for item in details}
        if (
            self.collector_plan is not None
            and self.collector_plan.fallback_reason is not None
            and "search.collector-fallback" not in known_codes
        ):
            details.append(
                SearchDegradation(
                    code="search.collector-fallback",
                    message=self.collector_plan.fallback_reason,
                )
            )
            known_codes.add("search.collector-fallback")
        for value in self.degradations:
            if value not in known_codes:
                details.append(SearchDegradation(code=value, message=value))
                known_codes.add(value)
        if len(known_codes) != len(details):
            message = "search degradation codes must be unique"
            raise ValueError(message)
        object.__setattr__(self, "degradation_details", tuple(details))
        object.__setattr__(self, "degradations", tuple(item.code for item in details))

    def _normalize_phases(self) -> None:
        phases = list(self.phases)
        if self.execution_state is SearchExecutionState.EXECUTED and not phases:
            phases.append(SearchExecutionPhase.QUERY)
            if self.downstream_filtered_count is not None:
                phases.append(SearchExecutionPhase.RESIDUAL_FILTER)
            if self.collector_plan is not None or self.collector_count:
                phases.append(SearchExecutionPhase.COLLECTOR)
        if self.execution_state is not SearchExecutionState.EXECUTED and phases:
            message = "non-executed Search trace cannot carry execution phases"
            raise ValueError(message)
        if (
            self.downstream_filtered_count is not None
            and SearchExecutionPhase.RESIDUAL_FILTER not in phases
        ):
            message = "downstream filtering requires a residual execution phase"
            raise ValueError(message)
        if (
            self.collector_plan is not None or self.collector_count
        ) and SearchExecutionPhase.COLLECTOR not in phases:
            message = "collector evidence requires a collector execution phase"
            raise ValueError(message)
        object.__setattr__(self, "phases", tuple(phases))

    def _validate_metric_relationships(self) -> None:
        if (
            self.query_matched_count is not None
            and self.returned_hit_count is not None
            and self.returned_hit_count > self.query_matched_count
        ):
            message = "returned Search hits cannot exceed query matches"
            raise ValueError(message)

    def to_document(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "operationId": self.operation_id,
            "matchedCount": self.matched_count,
            "queryMatchedCount": self.query_matched_count,
            "returnedHitCount": self.returned_hit_count,
            "downstreamFilteredCount": self.downstream_filtered_count,
            "candidateCount": self.candidate_count,
            "documentsScanned": self.documents_scanned,
            "collectorDocumentCount": self.collector_document_count,
            "pipelineOutputCount": self.pipeline_output_count,
            "executed": self.executed,
            "state": self.execution_state.value,
            "executionContext": {
                "bound": self.context_bound,
                "snapshotCaptured": self.snapshot_captured,
            },
            "phases": [phase.value for phase in self.phases],
            "metrics": [metric.to_document() for metric in self.metrics],
            "collectorBackend": self.collector_backend,
            "collectorPushdown": self.collector_pushdown,
            "collectorCount": self.collector_count,
            "collectorPlan": (
                self.collector_plan.to_document()
                if self.collector_plan is not None
                else None
            ),
            "degradations": list(self.degradations),
            "degradationDetails": [
                degradation.to_document() for degradation in self.degradation_details
            ],
            "engineDetails": deepcopy(self.engine_details),
        }

    @classmethod
    def from_explain_details(
        cls,
        details: dict[str, object],
        *,
        default_backend: str,
        operation_id: str | None = None,
    ) -> SearchExecutionTrace:
        """Normalize native explain evidence without discarding backend detail."""

        def _first_count(*names: str) -> int | None:
            for name in names:
                value = details.get(name)
                if (
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and value >= 0
                ):
                    return value
            return None

        backend_value = details.get("backend")
        backend = (
            backend_value
            if isinstance(backend_value, str) and backend_value
            else default_backend
        )
        degradations: list[SearchDegradation] = []
        observed_messages: set[str] = set()
        for name, suffix in (
            ("exactFallbackReason", "exact-fallback"),
            ("fallbackReason", "fallback"),
        ):
            value = details.get(name)
            if isinstance(value, str) and value and value not in observed_messages:
                degradations.append(
                    SearchDegradation(
                        code=f"{backend.lower()}.{suffix}",
                        message=value,
                        origin=SearchMetricOrigin.ENGINE,
                    )
                )
                observed_messages.add(value)
        query_matched_count = _first_count(
            "queryMatchedCount",
            "documentsMatchedBeforeLimit",
            "matchedCount",
        )
        returned_hit_count = _first_count(
            "returnedHitCount",
            "resultCount",
        )
        return cls(
            backend=backend,
            operation_id=operation_id,
            matched_count=query_matched_count,
            query_matched_count=query_matched_count,
            returned_hit_count=returned_hit_count,
            downstream_filtered_count=_first_count("downstreamFilteredCount"),
            candidate_count=_first_count(
                "candidatesEvaluated",
                "candidateCount",
                "candidateCountBeforeTopK",
            ),
            documents_scanned=_first_count("documentsScanned"),
            collector_document_count=_first_count("collectorDocumentCount"),
            pipeline_output_count=_first_count("pipelineOutputCount"),
            snapshot_captured=True,
            degradation_details=tuple(degradations),
            engine_details=details,
        )


@dataclass(frozen=True, slots=True)
class SearchHit:
    document: Document
    runtime_metadata: RuntimeMetadata | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.document, dict):
            msg = "search hit document must be a document"
            raise TypeError(msg)
        if self.runtime_metadata is not None and not isinstance(
            self.runtime_metadata,
            RuntimeMetadata,
        ):
            msg = "search hit runtime_metadata must be RuntimeMetadata or None"
            raise TypeError(msg)
        if self.runtime_metadata is None:
            state = runtime_state_from_legacy_document(self.document)
        else:
            state = RuntimeDocumentState(self.document, self.runtime_metadata)
        object.__setattr__(self, "document", state.persistence_document())
        object.__setattr__(self, "runtime_metadata", state.metadata)

    @property
    def runtime_state(self) -> RuntimeDocumentState:
        return RuntimeDocumentState(self.document, self.runtime_metadata)


@dataclass(frozen=True, slots=True)
class SearchExecutionOutcome:
    hits: tuple[SearchHit, ...] = ()
    metadata: SearchMetadata = field(default_factory=SearchMetadata)
    trace: SearchExecutionTrace | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.hits, tuple) or not all(
            isinstance(hit, SearchHit) for hit in self.hits
        ):
            msg = "search outcome hits must be a tuple of SearchHit"
            raise TypeError(msg)
        if not isinstance(self.metadata, SearchMetadata):
            msg = "search outcome metadata must be SearchMetadata"
            raise TypeError(msg)
        if self.trace is not None and not isinstance(self.trace, SearchExecutionTrace):
            msg = "search outcome trace must be SearchExecutionTrace"
            raise TypeError(msg)
        if self.hits and (self.metadata.count is not None or self.metadata.facets):
            msg = "search outcome cannot mix hits and collector metadata"
            raise ValueError(msg)

    @classmethod
    def from_documents(
        cls,
        documents: (
            list[Document | RuntimeDocumentState]
            | tuple[Document | RuntimeDocumentState, ...]
        ),
        *,
        backend: str,
        operation_id: str | None = None,
    ) -> SearchExecutionOutcome:
        return cls(
            hits=tuple(
                SearchHit(
                    document=(
                        document.document
                        if isinstance(document, RuntimeDocumentState)
                        else document
                    ),
                    runtime_metadata=(
                        document.metadata
                        if isinstance(document, RuntimeDocumentState)
                        else None
                    ),
                )
                for document in documents
            ),
            trace=SearchExecutionTrace(
                backend=backend,
                operation_id=operation_id,
                snapshot_captured=True,
                matched_count=len(documents),
                query_matched_count=len(documents),
                returned_hit_count=len(documents),
            ),
        )

    @property
    def documents(self) -> list[Document]:
        return [hit.runtime_state.public_document() for hit in self.hits]

    @property
    def runtime_states(self) -> list[RuntimeDocumentState]:
        return [hit.runtime_state for hit in self.hits]
