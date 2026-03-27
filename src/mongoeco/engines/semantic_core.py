from dataclasses import dataclass
from typing import Iterable

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import MatchAll, QueryNode, ensure_query_plan
from mongoeco.core.sorting import sort_documents
from mongoeco.types import (
    Document,
    ExecutionLineageStep,
    Filter,
    Projection,
    QueryPlanExplanation,
    SortSpec,
)


@dataclass(frozen=True, slots=True)
class EngineFindSemantics:
    filter_spec: Filter | None
    query_plan: QueryNode
    projection: Projection | None
    sort: SortSpec | None
    skip: int
    limit: int | None
    hint: str | object | None
    comment: object | None
    max_time_ms: int | None
    dialect: MongoDialect

    @property
    def deadline(self) -> float | None:
        return operation_deadline(self.max_time_ms)


@dataclass(frozen=True, slots=True)
class EngineReadExecutionPlan:
    semantics: EngineFindSemantics
    strategy: str
    execution_lineage: tuple[ExecutionLineageStep, ...]
    fallback_reason: str | None = None


def compile_find_semantics(
    filter_spec: Filter | None = None,
    *,
    plan: QueryNode | None = None,
    projection: Projection | None = None,
    sort: SortSpec | None = None,
    skip: int = 0,
    limit: int | None = None,
    hint: str | object | None = None,
    comment: object | None = None,
    max_time_ms: int | None = None,
    dialect: MongoDialect | None = None,
) -> EngineFindSemantics:
    effective_dialect = dialect or MONGODB_DIALECT_70
    if skip < 0:
        raise ValueError("skip must be >= 0")
    if limit is not None and limit < 0:
        raise ValueError("limit must be >= 0")
    return EngineFindSemantics(
        filter_spec=filter_spec,
        query_plan=ensure_query_plan(filter_spec, plan, dialect=effective_dialect),
        projection=projection,
        sort=sort,
        skip=skip,
        limit=limit,
        hint=hint,
        comment=comment,
        max_time_ms=max_time_ms,
        dialect=effective_dialect,
    )


def filter_documents(
    documents: Iterable[Document],
    semantics: EngineFindSemantics,
) -> list[Document]:
    return list(iter_filtered_documents(documents, semantics))


def iter_filtered_documents(
    documents: Iterable[Document],
    semantics: EngineFindSemantics,
) -> Iterable[Document]:
    deadline = semantics.deadline
    if isinstance(semantics.query_plan, MatchAll):
        for document in documents:
            enforce_deadline(deadline)
            yield document
        enforce_deadline(deadline)
        return
    for document in documents:
        enforce_deadline(deadline)
        if QueryEngine.match_plan(
            document,
            semantics.query_plan,
            dialect=semantics.dialect,
        ):
            yield document
    enforce_deadline(deadline)


def finalize_documents(
    documents: Iterable[Document],
    semantics: EngineFindSemantics,
    *,
    apply_sort_phase: bool = True,
    apply_skip_limit_phase: bool = True,
) -> list[Document]:
    deadline = semantics.deadline
    result = list(documents)
    if apply_sort_phase:
        result = sort_documents(result, semantics.sort, dialect=semantics.dialect)
        enforce_deadline(deadline)
    if apply_skip_limit_phase:
        if semantics.skip:
            result = result[semantics.skip :]
        if semantics.limit is not None:
            result = result[: semantics.limit]
    projected: list[Document] = []
    for document in stream_finalize_documents(
        result,
        semantics,
        apply_skip_limit_phase=False,
    ):
        projected.append(document)
    return projected


def stream_finalize_documents(
    documents: Iterable[Document],
    semantics: EngineFindSemantics,
    *,
    apply_skip_limit_phase: bool = True,
) -> Iterable[Document]:
    deadline = semantics.deadline
    remaining_skip = semantics.skip if apply_skip_limit_phase else 0
    remaining_limit = semantics.limit if apply_skip_limit_phase else None
    for document in documents:
        enforce_deadline(deadline)
        if remaining_skip:
            remaining_skip -= 1
            continue
        yield apply_projection(
            document,
            semantics.projection,
            dialect=semantics.dialect,
        )
        if remaining_limit is not None:
            remaining_limit -= 1
            if remaining_limit == 0:
                return


def build_query_plan_explanation(
    *,
    engine: str,
    strategy: str,
    semantics: EngineFindSemantics,
    hinted_index: str | None = None,
    details: object | None = None,
    indexes: list[object] | None = None,
    execution_lineage: tuple[ExecutionLineageStep, ...] = (),
    fallback_reason: str | None = None,
) -> QueryPlanExplanation:
    return QueryPlanExplanation(
        engine=engine,
        strategy=strategy,
        plan=repr(semantics.query_plan),
        details=details,
        sort=semantics.sort,
        skip=semantics.skip,
        limit=semantics.limit,
        hint=semantics.hint,
        hinted_index=hinted_index,
        comment=semantics.comment,
        max_time_ms=semantics.max_time_ms,
        indexes=indexes,
        execution_lineage=execution_lineage,
        fallback_reason=fallback_reason,
    )
