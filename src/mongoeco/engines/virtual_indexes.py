from __future__ import annotations

from copy import deepcopy

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    ExistsCondition,
    MatchAll,
    OrCondition,
    QueryNode,
    compile_filter,
)
from mongoeco.types import Document, EngineIndexRecord, Filter


def normalize_partial_filter_expression(
    partial_filter_expression: object | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Filter | None:
    if partial_filter_expression is None:
        return None
    if not isinstance(partial_filter_expression, dict):
        raise TypeError("partial_filter_expression must be a dict or None")
    compile_filter(partial_filter_expression, dialect=dialect)
    return deepcopy(partial_filter_expression)


def is_virtual_index(index: EngineIndexRecord) -> bool:
    return bool(_index_sparse(index) or _index_partial_filter_expression(index) is not None)


def document_in_virtual_index(
    document: Document,
    index: EngineIndexRecord,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> bool:
    fields = _index_fields(index)
    if _index_sparse(index) and not any(QueryEngine.extract_values(document, field) for field in fields):
        return False
    partial_filter_expression = _index_partial_filter_expression(index)
    if partial_filter_expression is not None:
        partial_plan = compile_filter(partial_filter_expression, dialect=dialect)
        if not QueryEngine.match_plan(document, partial_plan, dialect=dialect):
            return False
    return True


def query_can_use_index(
    index: EngineIndexRecord,
    query_plan: QueryNode,
) -> bool:
    if _index_sparse(index) and not any(
        _plan_implies_exists_true(query_plan, field)
        for field in _index_fields(index)
    ):
        return False
    partial_filter_expression = _index_partial_filter_expression(index)
    if partial_filter_expression is None:
        return True
    partial_plan = compile_filter(partial_filter_expression)
    return _plan_implies(query_plan, partial_plan)


def _index_fields(index: EngineIndexRecord | dict[str, object]) -> list[str]:
    if isinstance(index, EngineIndexRecord):
        return index.fields
    return list(index.get("fields", []))


def _index_sparse(index: EngineIndexRecord | dict[str, object]) -> bool:
    if isinstance(index, EngineIndexRecord):
        return index.sparse
    return bool(index.get("sparse", False))


def _index_partial_filter_expression(index: EngineIndexRecord | dict[str, object]) -> Filter | None:
    if isinstance(index, EngineIndexRecord):
        return index.partial_filter_expression
    value = index.get("partial_filter_expression")
    if value is None:
        value = index.get("partialFilterExpression")
    return value if isinstance(value, dict) else None


def _plan_implies(query_plan: QueryNode, requirement: QueryNode) -> bool:
    if isinstance(requirement, MatchAll):
        return True
    if query_plan == requirement:
        return True
    if isinstance(query_plan, OrCondition):
        return all(_plan_implies(clause, requirement) for clause in query_plan.clauses)
    if isinstance(requirement, OrCondition):
        return any(_plan_implies(query_plan, clause) for clause in requirement.clauses)
    if isinstance(requirement, AndCondition):
        return all(_plan_implies(query_plan, clause) for clause in requirement.clauses)
    if isinstance(query_plan, AndCondition):
        return any(_plan_implies(clause, requirement) for clause in query_plan.clauses)
    if isinstance(requirement, ExistsCondition) and requirement.value:
        return _plan_implies_exists_true(query_plan, requirement.field)
    return False


def _plan_implies_exists_true(query_plan: QueryNode, field: str) -> bool:
    if isinstance(query_plan, ExistsCondition):
        return query_plan.field == field and query_plan.value
    if isinstance(query_plan, EqualsCondition):
        return query_plan.field == field
    if isinstance(query_plan, AndCondition):
        return any(_plan_implies_exists_true(clause, field) for clause in query_plan.clauses)
    if isinstance(query_plan, OrCondition):
        return all(_plan_implies_exists_true(clause, field) for clause in query_plan.clauses)
    return False
