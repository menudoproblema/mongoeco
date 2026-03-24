from dataclasses import dataclass
from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.types import Filter


class QueryNode:
    """Nodo canónico de consulta para evaluación y futuras traducciones."""


@dataclass(frozen=True)
class MatchAll(QueryNode):
    pass


@dataclass(frozen=True)
class EqualsCondition(QueryNode):
    field: str
    value: Any


@dataclass(frozen=True)
class NotEqualsCondition(QueryNode):
    field: str
    value: Any


@dataclass(frozen=True)
class GreaterThanCondition(QueryNode):
    field: str
    value: Any


@dataclass(frozen=True)
class GreaterThanOrEqualCondition(QueryNode):
    field: str
    value: Any


@dataclass(frozen=True)
class LessThanCondition(QueryNode):
    field: str
    value: Any


@dataclass(frozen=True)
class LessThanOrEqualCondition(QueryNode):
    field: str
    value: Any


@dataclass(frozen=True)
class InCondition(QueryNode):
    field: str
    values: tuple[Any, ...]


@dataclass(frozen=True)
class NotInCondition(QueryNode):
    field: str
    values: tuple[Any, ...]


@dataclass(frozen=True)
class ExistsCondition(QueryNode):
    field: str
    value: bool


@dataclass(frozen=True)
class AndCondition(QueryNode):
    clauses: tuple[QueryNode, ...]


@dataclass(frozen=True)
class OrCondition(QueryNode):
    clauses: tuple[QueryNode, ...]


def _compile_field_condition(field: str, condition: Any) -> QueryNode:
    if not isinstance(condition, dict) or not any(isinstance(key, str) and key.startswith("$") for key in condition):
        return EqualsCondition(field, condition)

    clauses: list[QueryNode] = []
    for operator, value in condition.items():
        if operator == "$eq":
            clauses.append(EqualsCondition(field, value))
        elif operator == "$ne":
            clauses.append(NotEqualsCondition(field, value))
        elif operator == "$gt":
            clauses.append(GreaterThanCondition(field, value))
        elif operator == "$gte":
            clauses.append(GreaterThanOrEqualCondition(field, value))
        elif operator == "$lt":
            clauses.append(LessThanCondition(field, value))
        elif operator == "$lte":
            clauses.append(LessThanOrEqualCondition(field, value))
        elif operator == "$in":
            if not isinstance(value, (list, tuple)):
                raise ValueError("$in necesita una lista")
            clauses.append(InCondition(field, tuple(value)))
        elif operator == "$nin":
            if not isinstance(value, (list, tuple)):
                raise ValueError("$nin necesita una lista")
            clauses.append(NotInCondition(field, tuple(value)))
        elif operator == "$exists":
            clauses.append(ExistsCondition(field, bool(value)))
        else:
            raise OperationFailure(f"Unsupported query operator: {operator}")

    if len(clauses) == 1:
        return clauses[0]
    return AndCondition(tuple(clauses))


def compile_filter(filter_spec: Filter) -> QueryNode:
    if not filter_spec:
        return MatchAll()

    clauses: list[QueryNode] = []
    for key, value in filter_spec.items():
        if key == "$and":
            if not isinstance(value, list):
                raise ValueError("$and necesita una lista de filtros")
            clauses.append(AndCondition(tuple(compile_filter(clause) for clause in value)))
            continue
        if key == "$or":
            if not isinstance(value, list):
                raise ValueError("$or necesita una lista de filtros")
            clauses.append(OrCondition(tuple(compile_filter(clause) for clause in value)))
            continue
        clauses.append(_compile_field_condition(key, value))

    if len(clauses) == 1:
        return clauses[0]
    return AndCondition(tuple(clauses))
