from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING


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


@dataclass(frozen=True, slots=True)
class SearchExecutionTrace:
    backend: str
    matched_count: int | None = None
    candidate_count: int | None = None
    documents_scanned: int | None = None
    collector_backend: str | None = None
    collector_pushdown: bool = False
    collector_count: int = 0
    collector_plan: SearchCollectorPlan | None = None
    degradations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.backend, str) or not self.backend:
            msg = "search trace backend must be a non-empty string"
            raise ValueError(msg)
        for field_name in (
            "matched_count",
            "candidate_count",
            "documents_scanned",
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
        if self.collector_pushdown and (
            self.collector_plan is None or not self.collector_plan.pushed_down
        ):
            msg = "collector pushdown trace requires a pushed-down collector plan"
            raise ValueError(msg)

    def to_document(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "matchedCount": self.matched_count,
            "candidateCount": self.candidate_count,
            "documentsScanned": self.documents_scanned,
            "collectorBackend": self.collector_backend,
            "collectorPushdown": self.collector_pushdown,
            "collectorCount": self.collector_count,
            "collectorPlan": (
                self.collector_plan.to_document()
                if self.collector_plan is not None
                else None
            ),
            "degradations": list(self.degradations),
        }

    @classmethod
    def from_explain_details(
        cls,
        details: dict[str, object],
        *,
        default_backend: str,
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
        degradations: list[str] = []
        for name in ("exactFallbackReason", "fallbackReason"):
            value = details.get(name)
            if isinstance(value, str) and value and value not in degradations:
                degradations.append(value)
        return cls(
            backend=backend,
            matched_count=_first_count(
                "documentsMatchedBeforeLimit",
                "matchedCount",
                "resultCount",
            ),
            candidate_count=_first_count(
                "candidatesEvaluated",
                "candidateCount",
                "candidateCountBeforeTopK",
            ),
            documents_scanned=_first_count("documentsScanned"),
            degradations=tuple(degradations),
        )


@dataclass(frozen=True, slots=True)
class SearchHit:
    document: Document

    def __post_init__(self) -> None:
        if not isinstance(self.document, dict):
            msg = "search hit document must be a document"
            raise TypeError(msg)
        object.__setattr__(self, "document", deepcopy(self.document))


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
        documents: list[Document] | tuple[Document, ...],
        *,
        backend: str,
    ) -> SearchExecutionOutcome:
        return cls(
            hits=tuple(SearchHit(document=document) for document in documents),
            trace=SearchExecutionTrace(
                backend=backend,
                matched_count=len(documents),
            ),
        )

    @property
    def documents(self) -> list[Document]:
        return [deepcopy(hit.document) for hit in self.hits]
