from __future__ import annotations

from copy import deepcopy

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    ExistsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
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


def is_virtual_index(index: EngineIndexRecord | dict[str, object]) -> bool:
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
    index: EngineIndexRecord | dict[str, object],
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


def collect_usable_virtual_index_names(
    indexes: list[EngineIndexRecord] | list[dict[str, object]],
    query_plan: QueryNode,
) -> list[str]:
    names: list[str] = []
    for index in indexes:
        if not is_virtual_index(index):
            continue
        if query_can_use_index(index, query_plan):
            name = _index_name(index)
            if name:
                names.append(name)
    return names


def describe_virtual_index_usage(
    indexes: list[EngineIndexRecord] | list[dict[str, object]],
    query_plan: QueryNode,
    *,
    hinted_index_name: str | None = None,
) -> dict[str, object] | None:
    usable = collect_usable_virtual_index_names(indexes, query_plan)
    hinted_virtual: bool | None = None
    if hinted_index_name is not None:
        for index in indexes:
            if _index_name(index) != hinted_index_name:
                continue
            hinted_virtual = is_virtual_index(index)
            break
    if not usable and hinted_virtual is None:
        return None
    details: dict[str, object] = {}
    if usable:
        details["usableVirtualIndexes"] = usable
    if hinted_virtual is not None:
        details["hintedIndexIsVirtual"] = hinted_virtual
    return details


def _index_fields(index: EngineIndexRecord | dict[str, object]) -> list[str]:
    if isinstance(index, EngineIndexRecord):
        return index.fields
    return list(index.get("fields", []))


def _index_name(index: EngineIndexRecord | dict[str, object]) -> str | None:
    if isinstance(index, EngineIndexRecord):
        return index.name
    value = index.get("name")
    return value if isinstance(value, str) else None


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
    if _implies_same_field_condition(query_plan, requirement):
        return True
    if isinstance(requirement, ExistsCondition) and requirement.value:
        return _plan_implies_exists_true(query_plan, requirement.field)
    return False


def _plan_implies_exists_true(query_plan: QueryNode, field: str) -> bool:
    if isinstance(query_plan, ExistsCondition):
        return query_plan.field == field and query_plan.value
    if isinstance(query_plan, EqualsCondition):
        return query_plan.field == field and query_plan.value is not None
    if isinstance(query_plan, InCondition):
        return query_plan.field == field and bool(query_plan.values) and all(value is not None for value in query_plan.values)
    if isinstance(
        query_plan,
        GreaterThanCondition
        | GreaterThanOrEqualCondition
        | LessThanCondition
        | LessThanOrEqualCondition
        | RegexCondition
        | SizeCondition
        | ModCondition
        | TypeCondition,
    ):
        return query_plan.field == field
    if isinstance(query_plan, AndCondition):
        return any(_plan_implies_exists_true(clause, field) for clause in query_plan.clauses)
    if isinstance(query_plan, OrCondition):
        return all(_plan_implies_exists_true(clause, field) for clause in query_plan.clauses)
    return False


def _implies_same_field_condition(query_plan: QueryNode, requirement: QueryNode) -> bool:
    if isinstance(requirement, EqualsCondition):
        if isinstance(query_plan, EqualsCondition):
            return query_plan.field == requirement.field and query_plan.value == requirement.value
        if isinstance(query_plan, InCondition):
            return (
                query_plan.field == requirement.field
                and len(query_plan.values) == 1
                and query_plan.values[0] == requirement.value
            )
        return False
    if isinstance(requirement, InCondition):
        if isinstance(query_plan, EqualsCondition):
            return query_plan.field == requirement.field and query_plan.value in requirement.values
        if isinstance(query_plan, InCondition):
            return query_plan.field == requirement.field and set(query_plan.values).issubset(set(requirement.values))
        return False
    if isinstance(requirement, GreaterThanCondition):
        return _same_field_ordering_implies(query_plan, requirement.field, minimum=requirement.value, inclusive=False)
    if isinstance(requirement, GreaterThanOrEqualCondition):
        return _same_field_ordering_implies(query_plan, requirement.field, minimum=requirement.value, inclusive=True)
    if isinstance(requirement, LessThanCondition):
        return _same_field_ordering_implies(query_plan, requirement.field, maximum=requirement.value, inclusive=False)
    if isinstance(requirement, LessThanOrEqualCondition):
        return _same_field_ordering_implies(query_plan, requirement.field, maximum=requirement.value, inclusive=True)
    return False


def _same_field_ordering_implies(
    query_plan: QueryNode,
    field: str,
    *,
    minimum: object | None = None,
    maximum: object | None = None,
    inclusive: bool,
) -> bool:
    if isinstance(query_plan, EqualsCondition):
        if query_plan.field != field:
            return False
        return _value_satisfies_bounds(query_plan.value, minimum=minimum, maximum=maximum, inclusive=inclusive)
    if isinstance(query_plan, InCondition):
        if query_plan.field != field or not query_plan.values:
            return False
        return all(
            _value_satisfies_bounds(value, minimum=minimum, maximum=maximum, inclusive=inclusive)
            for value in query_plan.values
        )
    comparisons = {
        GreaterThanCondition: ("min", False),
        GreaterThanOrEqualCondition: ("min", True),
        LessThanCondition: ("max", False),
        LessThanOrEqualCondition: ("max", True),
    }
    for comparison_type, (kind, query_inclusive) in comparisons.items():
        if isinstance(query_plan, comparison_type):
            if query_plan.field != field:
                return False
            if kind == "min" and minimum is not None:
                return _compare_bounds(query_plan.value, minimum, query_inclusive, inclusive)
            if kind == "max" and maximum is not None:
                return _compare_bounds(query_plan.value, maximum, query_inclusive, inclusive, reverse=True)
    return False


def _compare_bounds(
    left: object,
    right: object,
    left_inclusive: bool,
    right_inclusive: bool,
    *,
    reverse: bool = False,
) -> bool:
    try:
        if reverse:
            if left < right:
                return True
            if left == right:
                return (not left_inclusive) or right_inclusive
            return False
        if left > right:
            return True
        if left == right:
            return left_inclusive or not right_inclusive
        return False
    except TypeError:
        return False


def _value_satisfies_bounds(
    value: object,
    *,
    minimum: object | None = None,
    maximum: object | None = None,
    inclusive: bool,
) -> bool:
    try:
        if minimum is not None:
            if value < minimum or (value == minimum and not inclusive):
                return False
        if maximum is not None:
            if value > maximum or (value == maximum and not inclusive):
                return False
        return True
    except TypeError:
        return False
