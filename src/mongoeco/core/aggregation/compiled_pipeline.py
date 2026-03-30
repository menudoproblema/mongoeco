from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
from mongoeco.core.compiled_query import CompiledQuery
from mongoeco.core.aggregation.extensions import get_registered_aggregation_stage_registration
from mongoeco.core.aggregation.planning import (
    Pipeline,
    _match_spec_contains_expr,
    _require_non_negative_int,
    _require_sort,
    _require_stage,
)
from mongoeco.core.aggregation.spill import AggregationSpillPolicy
from mongoeco.core.aggregation.transform_stages import (
    _apply_add_fields,
    _apply_match,
    _apply_project,
    _apply_unset,
)
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.sorting import sort_documents, sort_documents_window
from mongoeco.types import Document


type _CompiledDocumentStep = Callable[[Document, dict[str, Any] | None], Document | None]

_STREAMABLE_COMPILED_OPERATORS = frozenset({"$match", "$project", "$addFields", "$set", "$unset"})
_COMPILED_OPERATORS = _STREAMABLE_COMPILED_OPERATORS | frozenset({"$sort", "$skip", "$limit"})


@dataclass(frozen=True, slots=True)
class _CompiledStreamBlock:
    operators: tuple[str, ...]
    steps: tuple[_CompiledDocumentStep, ...]

    def apply(
        self,
        documents: list[Document],
        *,
        variables: dict[str, Any] | None = None,
    ) -> list[Document]:
        result: list[Document] = []
        for document in documents:
            current: Document | None = document
            for step in self.steps:
                if current is None:
                    break
                current = step(current, variables)
            if current is not None:
                result.append(current)
        return result

    def explain(self) -> dict[str, Any]:
        return {"kind": "streamable_block", "operators": list(self.operators)}


@dataclass(frozen=True, slots=True)
class _CompiledSortStage:
    sort_spec: list[tuple[str, int]]
    window: int | None
    dialect: MongoDialect

    def apply(self, documents: list[Document]) -> list[Document]:
        if self.window is not None:
            return sort_documents_window(
                documents,
                self.sort_spec,
                window=self.window,
                dialect=self.dialect,
                collation=None,
            )
        return sort_documents(
            documents,
            self.sort_spec,
            dialect=self.dialect,
            collation=None,
        )

    def explain(self) -> dict[str, Any]:
        return {
            "kind": "sort",
            "sort": list(self.sort_spec),
            "window": self.window,
        }


@dataclass(frozen=True, slots=True)
class _CompiledSkipStage:
    value: int

    def apply(self, documents: list[Document]) -> list[Document]:
        return documents[self.value :]

    def explain(self) -> dict[str, Any]:
        return {"kind": "skip", "value": self.value}


@dataclass(frozen=True, slots=True)
class _CompiledLimitStage:
    value: int

    def apply(self, documents: list[Document]) -> list[Document]:
        return documents[: self.value]

    def explain(self) -> dict[str, Any]:
        return {"kind": "limit", "value": self.value}


type _CompiledPipelineNode = _CompiledStreamBlock | _CompiledSortStage | _CompiledSkipStage | _CompiledLimitStage


@dataclass(frozen=True, slots=True)
class CompiledPipelinePlan:
    pipeline: Pipeline
    nodes: tuple[_CompiledPipelineNode, ...]
    dialect: MongoDialect = MONGODB_DIALECT_70

    @classmethod
    def supports(
        cls,
        pipeline: Pipeline,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        spill_policy: AggregationSpillPolicy | None = None,
    ) -> bool:
        try:
            return compile_pipeline(
                pipeline,
                dialect=dialect,
                collation=collation,
                spill_policy=spill_policy,
            ) is not None
        except Exception:
            return False

    def execute(
        self,
        documents: Iterable[Document],
        *,
        variables: dict[str, Any] | None = None,
        collection_resolver: Callable[[str], list[Document]] | None = None,
        spill_policy: AggregationSpillPolicy | None = None,
    ) -> list[Document]:
        del collection_resolver
        del spill_policy

        result = list(documents)
        for node in self.nodes:
            result = node.apply(result, variables=variables) if isinstance(node, _CompiledStreamBlock) else node.apply(result)
        return result

    def explain(self) -> dict[str, Any]:
        return {
            "type": "compiled_pipeline_v1",
            "nodes": [node.explain() for node in self.nodes],
            "dialect": self.dialect.key,
        }


def compile_pipeline(
    pipeline: Pipeline,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    spill_policy: AggregationSpillPolicy | None = None,
) -> CompiledPipelinePlan | None:
    if collation is not None or spill_policy is not None:
        return None

    nodes: list[_CompiledPipelineNode] = []
    stream_operators: list[str] = []
    stream_steps: list[_CompiledDocumentStep] = []

    def _flush_stream_block() -> None:
        if not stream_steps:
            return
        nodes.append(
            _CompiledStreamBlock(
                operators=tuple(stream_operators),
                steps=tuple(stream_steps),
            )
        )
        stream_operators.clear()
        stream_steps.clear()

    for index, stage in enumerate(pipeline):
        operator, spec = _require_stage(stage)
        if not dialect.supports_aggregation_stage(operator):
            return None
        if get_registered_aggregation_stage_registration(operator) is not None:
            return None
        if operator not in _COMPILED_OPERATORS:
            return None

        if operator in _STREAMABLE_COMPILED_OPERATORS:
            stream_operators.append(operator)
            stream_steps.append(_compile_document_step(operator, spec, dialect=dialect))
            continue

        _flush_stream_block()
        if operator == "$sort":
            nodes.append(
                _CompiledSortStage(
                    sort_spec=_require_sort(spec),
                    window=_sort_window_for_following_slices(pipeline, index),
                    dialect=dialect,
                )
            )
            continue
        if operator == "$skip":
            nodes.append(_CompiledSkipStage(_require_non_negative_int("$skip", spec)))
            continue
        nodes.append(_CompiledLimitStage(_require_non_negative_int("$limit", spec)))

    _flush_stream_block()
    return CompiledPipelinePlan(pipeline=list(pipeline), nodes=tuple(nodes), dialect=dialect)


def _compile_document_step(
    operator: str,
    spec: object,
    *,
    dialect: MongoDialect,
) -> _CompiledDocumentStep:
    if operator == "$match":
        return _compile_match_step(spec, dialect=dialect)
    if operator == "$project":
        _apply_project([], spec, None, dialect=dialect)

        def _project_step(document: Document, variables: dict[str, Any] | None) -> Document:
            return _apply_project([document], spec, variables, dialect=dialect)[0]

        return _project_step
    if operator in {"$addFields", "$set"}:
        _apply_add_fields([], spec, None, dialect=dialect)

        def _add_fields_step(document: Document, variables: dict[str, Any] | None) -> Document:
            return _apply_add_fields([document], spec, variables, dialect=dialect)[0]

        return _add_fields_step
    if operator == "$unset":
        _apply_unset([], spec)

        def _unset_step(document: Document, _variables: dict[str, Any] | None) -> Document:
            return _apply_unset([document], spec)[0]

        return _unset_step
    raise AssertionError(f"unsupported compiled document step: {operator}")


def _compile_match_step(
    spec: object,
    *,
    dialect: MongoDialect,
) -> _CompiledDocumentStep:
    if not isinstance(spec, dict):
        # Preserve the exact validation path from the reference runtime.
        _apply_match([], spec, None, dialect=dialect)
        raise AssertionError("unreachable")

    if _match_spec_contains_expr(spec):
        def _expr_match_step(document: Document, variables: dict[str, Any] | None) -> Document | None:
            matched = _apply_match([document], spec, variables, dialect=dialect)
            return document if matched else None

        return _expr_match_step

    plan = compile_filter(spec, dialect=dialect) if spec else None
    matcher = CompiledQuery(plan, dialect=dialect) if plan is not None else None

    def _match_step(document: Document, _variables: dict[str, Any] | None) -> Document | None:
        if matcher is None or matcher.match(document):
            return document
        return None

    return _match_step


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
