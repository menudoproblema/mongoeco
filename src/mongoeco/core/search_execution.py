from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.search import SearchQuery, compile_search_stage
from mongoeco.core.search_models import SearchExecutionMode


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
        self._validate_execution_shape()
        self._validate_limits()
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
            or self.result_limit_hint <= 0
        ):
            message = "result_limit_hint must be a positive integer"
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
