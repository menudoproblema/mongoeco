from __future__ import annotations

from copy import deepcopy
import re

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import (
    CollationSpec,
    compare_with_collation,
    normalize_collation,
    values_equal_with_collation,
)
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
from mongoeco.types import Document, EngineIndexRecord, Filter, is_ordered_index_spec


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
        if not QueryEngine.match_plan(
            document,
            partial_plan,
            dialect=dialect,
            collation=_index_collation(index),
        ):
            return False
    return True


def query_can_use_index(
    index: EngineIndexRecord | dict[str, object],
    query_plan: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | Filter | None = None,
) -> bool:
    if _index_hidden(index):
        return False
    keys = _index_key(index)
    if not isinstance(keys, list) or not is_ordered_index_spec(keys):
        return False
    if _index_sparse(index) and not any(
        _plan_implies_exists_true(query_plan, field)
        for field in _index_fields(index)
    ):
        return False
    partial_filter_expression = _index_partial_filter_expression(index)
    if partial_filter_expression is None:
        return True
    operation_collation = (
        normalize_collation(collation) if isinstance(collation, dict) else collation
    )
    index_collation = _index_collation(index)
    if operation_collation != index_collation:
        return False
    partial_plan = compile_filter(partial_filter_expression, dialect=dialect)
    return _plan_implies(
        query_plan,
        partial_plan,
        dialect=dialect,
        collation=index_collation,
    )


def collect_usable_virtual_index_names(
    indexes: list[EngineIndexRecord] | list[dict[str, object]],
    query_plan: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | Filter | None = None,
) -> list[str]:
    names: list[str] = []
    for index in indexes:
        if not is_virtual_index(index):
            continue
        if query_can_use_index(
            index,
            query_plan,
            dialect=dialect,
            collation=collation,
        ):
            name = _index_name(index)
            if name:
                names.append(name)
    return names


def describe_virtual_index_usage(
    indexes: list[EngineIndexRecord] | list[dict[str, object]],
    query_plan: QueryNode,
    *,
    hinted_index_name: str | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | Filter | None = None,
) -> dict[str, object] | None:
    usable = collect_usable_virtual_index_names(
        indexes,
        query_plan,
        dialect=dialect,
        collation=collation,
    )
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


def _index_key(index: EngineIndexRecord | dict[str, object]) -> list[tuple[str, int]] | object:
    if isinstance(index, EngineIndexRecord):
        return index.key
    key = index.get("key")
    if key is not None:
        return key
    fields = _index_fields(index)
    if fields:
        return [(field, 1) for field in fields]
    return None


def _index_name(index: EngineIndexRecord | dict[str, object]) -> str | None:
    if isinstance(index, EngineIndexRecord):
        return index.name
    value = index.get("name")
    return value if isinstance(value, str) else None


def _index_sparse(index: EngineIndexRecord | dict[str, object]) -> bool:
    if isinstance(index, EngineIndexRecord):
        return index.sparse
    return bool(index.get("sparse", False))


def _index_hidden(index: EngineIndexRecord | dict[str, object]) -> bool:
    if isinstance(index, EngineIndexRecord):
        return index.hidden
    return bool(index.get("hidden", False))


def _index_collation(
    index: EngineIndexRecord | dict[str, object],
) -> CollationSpec | None:
    if isinstance(index, EngineIndexRecord):
        value = index.collation
    else:
        value = index.get("collation")
    return normalize_collation(value) if isinstance(value, dict) else None


def _index_partial_filter_expression(index: EngineIndexRecord | dict[str, object]) -> Filter | None:
    if isinstance(index, EngineIndexRecord):
        return index.partial_filter_expression
    value = index.get("partial_filter_expression")
    if value is None:
        value = index.get("partialFilterExpression")
    return value if isinstance(value, dict) else None


def _plan_implies(
    query_plan: QueryNode,
    requirement: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if isinstance(requirement, MatchAll):
        return True
    if isinstance(query_plan, OrCondition):
        return all(_plan_implies(clause, requirement, dialect=dialect, collation=collation) for clause in query_plan.clauses)
    if isinstance(requirement, OrCondition):
        return any(_plan_implies(query_plan, clause, dialect=dialect, collation=collation) for clause in requirement.clauses)
    if isinstance(requirement, AndCondition):
        return all(_plan_implies(query_plan, clause, dialect=dialect, collation=collation) for clause in requirement.clauses)
    if isinstance(query_plan, AndCondition):
        return any(_plan_implies(clause, requirement, dialect=dialect, collation=collation) for clause in query_plan.clauses)
    if _implies_same_field_condition(
        query_plan,
        requirement,
        dialect=dialect,
        collation=collation,
    ):
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


def _implies_same_field_condition(
    query_plan: QueryNode,
    requirement: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if isinstance(requirement, ExistsCondition):
        return requirement.value and _plan_implies_exists_true(query_plan, requirement.field)
    if isinstance(requirement, EqualsCondition):
        if isinstance(query_plan, EqualsCondition):
            return (
                query_plan.field == requirement.field
                and values_equal_with_collation(
                    query_plan.value,
                    requirement.value,
                    dialect=dialect,
                    collation=collation,
                )
            )
        if isinstance(query_plan, InCondition):
            return (
                query_plan.field == requirement.field
                and len(query_plan.values) == 1
                and values_equal_with_collation(
                    query_plan.values[0],
                    requirement.value,
                    dialect=dialect,
                    collation=collation,
                )
            )
        return False
    if isinstance(requirement, InCondition):
        if isinstance(query_plan, EqualsCondition):
            return query_plan.field == requirement.field and any(
                values_equal_with_collation(query_plan.value, candidate, dialect=dialect, collation=collation)
                for candidate in requirement.values
            )
        if isinstance(query_plan, InCondition):
            return query_plan.field == requirement.field and all(
                any(
                    values_equal_with_collation(query_value, required_value, dialect=dialect, collation=collation)
                    for required_value in requirement.values
                )
                for query_value in query_plan.values
            )
        return False
    if isinstance(requirement, TypeCondition):
        return _same_field_type_implies(query_plan, requirement.field, requirement.values)
    if isinstance(requirement, RegexCondition):
        return _same_field_regex_implies(query_plan, requirement)
    if isinstance(requirement, GreaterThanCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            minimum=requirement.value,
            inclusive=False,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(requirement, GreaterThanOrEqualCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            minimum=requirement.value,
            inclusive=True,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(requirement, LessThanCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            maximum=requirement.value,
            inclusive=False,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(requirement, LessThanOrEqualCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            maximum=requirement.value,
            inclusive=True,
            dialect=dialect,
            collation=collation,
        )
    return False


def _same_field_type_implies(query_plan: QueryNode, field: str, type_spec: object) -> bool:
    aliases: set[str] = set()
    if isinstance(type_spec, tuple):
        for candidate in type_spec:
            aliases.update(QueryEngine._normalize_type_specifier(candidate))
    else:
        aliases.update(QueryEngine._normalize_type_specifier(type_spec))
    if isinstance(query_plan, EqualsCondition):
        return query_plan.field == field and any(
            QueryEngine._matches_bson_type(query_plan.value, alias)
            for alias in aliases
        )
    if isinstance(query_plan, InCondition):
        return query_plan.field == field and bool(query_plan.values) and all(
            any(QueryEngine._matches_bson_type(value, alias) for alias in aliases)
            for value in query_plan.values
        )
    if isinstance(query_plan, TypeCondition):
        query_aliases: set[str] = set()
        for candidate in query_plan.values:
            query_aliases.update(QueryEngine._normalize_type_specifier(candidate))
        return query_plan.field == field and query_aliases.issubset(aliases)
    return False


def _same_field_regex_implies(query_plan: QueryNode, requirement: RegexCondition) -> bool:
    if isinstance(query_plan, EqualsCondition):
        if query_plan.field != requirement.field or not isinstance(query_plan.value, str):
            return False
        return _compile_regex_condition(requirement).search(query_plan.value) is not None
    if isinstance(query_plan, InCondition):
        if query_plan.field != requirement.field or not query_plan.values:
            return False
        compiled = _compile_regex_condition(requirement)
        return all(isinstance(value, str) and compiled.search(value) is not None for value in query_plan.values)
    if isinstance(query_plan, RegexCondition):
        return (
            query_plan.field == requirement.field
            and query_plan.pattern == requirement.pattern
            and query_plan.options == requirement.options
        )
    return False


def _compile_regex_condition(condition: RegexCondition) -> re.Pattern[str]:
    flags = 0
    for option in condition.options:
        if option == "i":
            flags |= re.IGNORECASE
        elif option == "m":
            flags |= re.MULTILINE
        elif option == "s":
            flags |= re.DOTALL
        elif option == "a":
            flags |= re.ASCII
        elif option == "u":
            flags |= re.UNICODE
        elif option == "x":
            flags |= re.VERBOSE
    return re.compile(condition.pattern, flags)


def _same_field_ordering_implies(
    query_plan: QueryNode,
    field: str,
    *,
    minimum: object | None = None,
    maximum: object | None = None,
    inclusive: bool,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if isinstance(query_plan, EqualsCondition):
        if query_plan.field != field:
            return False
        return _value_satisfies_bounds(
            query_plan.value,
            minimum=minimum,
            maximum=maximum,
            inclusive=inclusive,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(query_plan, InCondition):
        if query_plan.field != field or not query_plan.values:
            return False
        return all(
            _value_satisfies_bounds(
                value,
                minimum=minimum,
                maximum=maximum,
                inclusive=inclusive,
                dialect=dialect,
                collation=collation,
            )
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
                return _compare_bounds(
                    query_plan.value,
                    minimum,
                    query_inclusive,
                    inclusive,
                    dialect=dialect,
                    collation=collation,
                )
            if kind == "max" and maximum is not None:
                return _compare_bounds(
                    query_plan.value,
                    maximum,
                    query_inclusive,
                    inclusive,
                    reverse=True,
                    dialect=dialect,
                    collation=collation,
                )
    return False


def _compare_bounds(
    left: object,
    right: object,
    left_inclusive: bool,
    right_inclusive: bool,
    *,
    reverse: bool = False,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    comparison = compare_with_collation(
        left,
        right,
        dialect=dialect,
        collation=collation,
    )
    if reverse:
        if comparison < 0:
            return True
        if comparison == 0:
            return (not left_inclusive) or right_inclusive
        return False
    if comparison > 0:
        return True
    if comparison == 0:
        return left_inclusive or not right_inclusive
    return False


def _value_satisfies_bounds(
    value: object,
    *,
    minimum: object | None = None,
    maximum: object | None = None,
    inclusive: bool,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if minimum is not None:
        minimum_comparison = compare_with_collation(
            value,
            minimum,
            dialect=dialect,
            collation=collation,
        )
        if minimum_comparison < 0 or (minimum_comparison == 0 and not inclusive):
            return False
    if maximum is not None:
        maximum_comparison = compare_with_collation(
            value,
            maximum,
            dialect=dialect,
            collation=collation,
        )
        if maximum_comparison > 0 or (maximum_comparison == 0 and not inclusive):
            return False
    return True
