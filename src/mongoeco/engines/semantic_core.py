from dataclasses import dataclass, field
from typing import Iterable

from mongoeco.api.operations import FindOperation, UpdateOperation
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.compiled_query import CompiledQuery
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import CollationSpec, normalize_collation
from mongoeco.core.operators import CompiledUpdatePlan
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import MatchAll, QueryNode, ensure_query_plan
from mongoeco.core.schema_validation import (
    CompiledCollectionValidator,
    SchemaValidationResult,
    compile_collection_validator,
)
from mongoeco.core.sorting import sort_documents, sort_documents_limited
from mongoeco.errors import DocumentValidationFailure
from mongoeco.types import (
    Document,
    ExecutionLineageStep,
    PhysicalPlanStep,
    Filter,
    Projection,
    QueryPlanExplanation,
    SortSpec,
    CollationDocument,
)


@dataclass(frozen=True, slots=True)
class EngineFindSemantics:
    filter_spec: Filter | None
    query_plan: QueryNode
    projection: Projection | None
    collation: CollationSpec | None
    sort: SortSpec | None
    skip: int
    limit: int | None
    hint: str | object | None
    comment: object | None
    max_time_ms: int | None
    dialect: MongoDialect
    compiled_query: CompiledQuery | None = field(default=None, compare=False, hash=False)

    @property
    def deadline(self) -> float | None:
        return operation_deadline(self.max_time_ms)


@dataclass(frozen=True, slots=True)
class EngineReadExecutionPlan:
    semantics: EngineFindSemantics
    strategy: str
    execution_lineage: tuple[ExecutionLineageStep, ...]
    physical_plan: tuple[PhysicalPlanStep, ...] = ()
    fallback_reason: str | None = None


@dataclass(frozen=True, slots=True)
class EngineUpdateSemantics:
    filter_spec: Filter
    query_plan: QueryNode
    compiled_update_plan: CompiledUpdatePlan
    compiled_upsert_plan: CompiledUpdatePlan
    selector_filter: Filter
    collation: CollationSpec | None
    dialect: MongoDialect


def compile_collection_validation_semantics(
    options: dict[str, object] | None,
    *,
    dialect: MongoDialect | None = None,
) -> CompiledCollectionValidator | None:
    return compile_collection_validator(options, dialect=dialect)


def validate_collection_document(
    document: Document,
    *,
    options: dict[str, object] | None,
    original_document: Document | None = None,
    is_upsert_insert: bool = False,
    dialect: MongoDialect | None = None,
) -> SchemaValidationResult:
    validator = compile_collection_validation_semantics(options, dialect=dialect)
    if validator is None:
        return SchemaValidationResult(valid=True)
    return validator.validate_document(
        document,
        original_document=original_document,
        is_upsert_insert=is_upsert_insert,
        dialect=dialect,
    )


def enforce_collection_document_validation(
    document: Document,
    *,
    options: dict[str, object] | None,
    original_document: Document | None = None,
    is_upsert_insert: bool = False,
    dialect: MongoDialect | None = None,
) -> None:
    validator = compile_collection_validation_semantics(options, dialect=dialect)
    if validator is None:
        return
    result = validator.validate_document(
        document,
        original_document=original_document,
        is_upsert_insert=is_upsert_insert,
        dialect=dialect,
    )
    if result.valid or validator.validation_action == "warn":
        return
    raise DocumentValidationFailure(
        f"Document failed validation: {result.first_message}",
        details={
            "failingDocumentId": document.get("_id"),
            "schemaRulesNotSatisfied": [issue.render() for issue in result.issues],
        },
    )


def compile_find_semantics(
    filter_spec: Filter | None = None,
    *,
    plan: QueryNode | None = None,
    projection: Projection | None = None,
    collation: CollationDocument | None = None,
    sort: SortSpec | None = None,
    skip: int = 0,
    limit: int | None = None,
    hint: str | object | None = None,
    comment: object | None = None,
    max_time_ms: int | None = None,
    dialect: MongoDialect | None = None,
    compiled_query: CompiledQuery | None = None,
) -> EngineFindSemantics:
    effective_dialect = dialect or MONGODB_DIALECT_70
    if skip < 0:
        raise ValueError("skip must be >= 0")
    if limit is not None and limit < 0:
        raise ValueError("limit must be >= 0")
    effective_collation = normalize_collation(collation)
    query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)

    return EngineFindSemantics(
        filter_spec=filter_spec,
        query_plan=query_plan,
        projection=projection,
        collation=effective_collation,
        sort=sort,
        skip=skip,
        limit=limit,
        hint=hint,
        comment=comment,
        max_time_ms=max_time_ms,
        dialect=effective_dialect,
        compiled_query=compiled_query
        if compiled_query is not None
        else CompiledQuery(
            query_plan,
            dialect=effective_dialect,
            collation=effective_collation,
        ),
    )


def compile_find_semantics_from_operation(
    operation: FindOperation,
    *,
    dialect: MongoDialect | None = None,
    compiled_query: CompiledQuery | None = None,
) -> EngineFindSemantics:
    return compile_find_semantics(
        operation.filter_spec,
        plan=operation.plan,
        projection=operation.projection,
        collation=operation.collation,
        sort=operation.sort,
        skip=operation.skip,
        limit=operation.limit,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        dialect=dialect,
        compiled_query=compiled_query,
    )


def compile_update_semantics(
    operation: UpdateOperation,
    *,
    dialect: MongoDialect | None = None,
    selector_filter: Filter | None = None,
) -> EngineUpdateSemantics:
    effective_dialect = dialect or MONGODB_DIALECT_70
    if operation.compiled_update_plan is None or operation.compiled_upsert_plan is None:
        raise ValueError("UpdateOperation must include compiled update plans")
    return EngineUpdateSemantics(
        filter_spec=operation.filter_spec,
        query_plan=ensure_query_plan(operation.filter_spec, operation.plan, dialect=effective_dialect),
        compiled_update_plan=operation.compiled_update_plan,
        compiled_upsert_plan=operation.compiled_upsert_plan,
        selector_filter=selector_filter or operation.filter_spec,
        collation=normalize_collation(operation.collation),
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
        if deadline is None:
            for document in documents:
                yield document
            return
        for document in documents:
            enforce_deadline(deadline)
            yield document
        enforce_deadline(deadline)
        return

    compiled = semantics.compiled_query
    if compiled is not None:
        if deadline is None:
            for document in documents:
                if compiled.match(document):
                    yield document
            return
        for document in documents:
            enforce_deadline(deadline)
            if compiled.match(document):
                yield document
        enforce_deadline(deadline)
        return

    if deadline is None:
        for document in documents:
            if QueryEngine.match_plan(
                document,
                semantics.query_plan,
                dialect=semantics.dialect,
                collation=semantics.collation,
            ):
                yield document
        return

    for document in documents:
        enforce_deadline(deadline)
        if QueryEngine.match_plan(
            document,
            semantics.query_plan,
            dialect=semantics.dialect,
            collation=semantics.collation,
        ):
            yield document
    enforce_deadline(deadline)


def finalize_documents(
    documents: Iterable[Document],
    semantics: EngineFindSemantics,
    *,
    apply_sort_phase: bool = True,
    apply_skip_limit_phase: bool = True,
    emit_public_documents: bool = True,
) -> list[Document]:
    deadline = semantics.deadline
    if apply_sort_phase:
        if apply_skip_limit_phase and semantics.limit is not None:
            result = sort_documents_limited(
                documents,
                semantics.sort,
                skip=semantics.skip,
                limit=semantics.limit,
                dialect=semantics.dialect,
                collation=semantics.collation,
            )
            apply_skip_limit_phase = False
        else:
            result = list(documents)
            result = sort_documents(
                result,
                semantics.sort,
                dialect=semantics.dialect,
                collation=semantics.collation,
            )
        enforce_deadline(deadline)
    else:
        result = list(documents)
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
        emit_public_documents=emit_public_documents,
    ):
        projected.append(document)
    return projected


def stream_finalize_documents(
    documents: Iterable[Document],
    semantics: EngineFindSemantics,
    *,
    apply_skip_limit_phase: bool = True,
    emit_public_documents: bool = True,
) -> Iterable[Document]:
    deadline = semantics.deadline
    projection = semantics.projection
    dialect = semantics.dialect
    remaining_skip = semantics.skip if apply_skip_limit_phase else 0
    remaining_limit = semantics.limit if apply_skip_limit_phase else None
    if deadline is None:
        for document in documents:
            if remaining_skip:
                remaining_skip -= 1
                continue
            projected = document if projection is None else apply_projection(document, projection, dialect=dialect)
            yield DocumentCodec.to_public(projected) if emit_public_documents else projected
            if remaining_limit is not None:
                remaining_limit -= 1
                if remaining_limit == 0:
                    return
        return

    for document in documents:
        enforce_deadline(deadline)
        if remaining_skip:
            remaining_skip -= 1
            continue
        projected = document if projection is None else apply_projection(document, projection, dialect=dialect)
        yield DocumentCodec.to_public(projected) if emit_public_documents else projected
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
    physical_plan: tuple[PhysicalPlanStep, ...] = (),
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
        physical_plan=physical_plan,
        fallback_reason=fallback_reason,
    )
