from dataclasses import dataclass
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
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
    null_matches_undefined: bool = False


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
    null_matches_undefined: bool = False


@dataclass(frozen=True)
class NotInCondition(QueryNode):
    field: str
    values: tuple[Any, ...]


@dataclass(frozen=True)
class AllCondition(QueryNode):
    field: str
    values: tuple[Any, ...]


@dataclass(frozen=True)
class SizeCondition(QueryNode):
    field: str
    value: int


@dataclass(frozen=True)
class ModCondition(QueryNode):
    field: str
    divisor: int | float
    remainder: int | float


@dataclass(frozen=True)
class RegexCondition(QueryNode):
    field: str
    pattern: str
    options: str = ""


@dataclass(frozen=True)
class NotCondition(QueryNode):
    clause: QueryNode


@dataclass(frozen=True)
class ElemMatchCondition(QueryNode):
    field: str
    condition: Any
    dialect: MongoDialect = MONGODB_DIALECT_70


@dataclass(frozen=True)
class ExistsCondition(QueryNode):
    field: str
    value: bool


@dataclass(frozen=True)
class ExprCondition(QueryNode):
    expression: Any
    variables: dict[str, Any]


@dataclass(frozen=True)
class AndCondition(QueryNode):
    clauses: tuple[QueryNode, ...]


@dataclass(frozen=True)
class OrCondition(QueryNode):
    clauses: tuple[QueryNode, ...]


def _compile_field_condition(
    field: str,
    condition: Any,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> QueryNode:
    if not isinstance(condition, dict) or not any(isinstance(key, str) and key.startswith("$") for key in condition):
        return EqualsCondition(
            field,
            condition,
            null_matches_undefined=condition is None and dialect.null_query_matches_undefined(),
        )

    clauses: list[QueryNode] = []
    regex_value: Any = None
    regex_options = ""
    for operator, value in condition.items():
        if not dialect.supports_query_field_operator(operator):
            raise OperationFailure(f"Unsupported query operator: {operator}")
        if operator == "$eq":
            clauses.append(
                EqualsCondition(
                    field,
                    value,
                    null_matches_undefined=value is None and dialect.null_query_matches_undefined(),
                )
            )
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
            clauses.append(
                InCondition(
                    field,
                    tuple(value),
                    null_matches_undefined=any(item is None for item in value)
                    and dialect.null_query_matches_undefined(),
                )
            )
        elif operator == "$nin":
            if not isinstance(value, (list, tuple)):
                raise ValueError("$nin necesita una lista")
            clauses.append(NotInCondition(field, tuple(value)))
        elif operator == "$all":
            if not isinstance(value, (list, tuple)):
                raise ValueError("$all necesita una lista")
            clauses.append(AllCondition(field, tuple(value)))
        elif operator == "$size":
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError("$size necesita un entero no negativo")
            clauses.append(SizeCondition(field, value))
        elif operator == "$mod":
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ValueError("$mod necesita una lista de dos numeros")
            divisor, remainder = value
            if not isinstance(divisor, (int, float)) or isinstance(divisor, bool) or divisor == 0:
                raise ValueError("$mod necesita un divisor numerico distinto de cero")
            if not isinstance(remainder, (int, float)) or isinstance(remainder, bool):
                raise ValueError("$mod necesita un resto numerico")
            clauses.append(ModCondition(field, divisor, remainder))
        elif operator == "$regex":
            if not isinstance(value, str):
                raise ValueError("$regex necesita un patron string")
            regex_value = value
        elif operator == "$options":
            if not isinstance(value, str):
                raise ValueError("$options necesita un string")
            regex_options = value
        elif operator == "$not":
            if not isinstance(value, dict) or not any(isinstance(key, str) and key.startswith("$") for key in value):
                raise ValueError("$not necesita una expresion de operador")
            clauses.append(NotCondition(_compile_field_condition(field, value, dialect=dialect)))
        elif operator == "$elemMatch":
            if not isinstance(value, dict):
                raise ValueError("$elemMatch necesita una expresion de filtro")
            clauses.append(ElemMatchCondition(field, value, dialect=dialect))
        elif operator == "$exists":
            clauses.append(ExistsCondition(field, bool(value)))
        else:
            raise OperationFailure(f"Unsupported query operator: {operator}")

    if regex_options and regex_value is None:
        raise OperationFailure("$options requires $regex")
    if regex_value is not None:
        clauses.append(RegexCondition(field, regex_value, regex_options))

    if len(clauses) == 1:
        return clauses[0]
    return AndCondition(tuple(clauses))


def compile_filter(
    filter_spec: Filter,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables: dict[str, Any] | None = None,
) -> QueryNode:
    if filter_spec is None:
        return MatchAll()
    if not isinstance(filter_spec, dict):
        raise ValueError("filter_spec must be a document")
    if not filter_spec:
        return MatchAll()

    clauses: list[QueryNode] = []
    for key, value in filter_spec.items():
        if isinstance(key, str) and key.startswith("$") and not dialect.supports_query_top_level_operator(key):
            raise OperationFailure(f"Unsupported top-level query operator: {key}")
        if key == "$expr":
            clauses.append(ExprCondition(value, {} if variables is None else dict(variables)))
            continue
        if key == "$and":
            if not isinstance(value, list):
                raise ValueError("$and necesita una lista de filtros")
            clauses.append(
                AndCondition(
                    tuple(
                        compile_filter(
                            clause,
                            dialect=dialect,
                            variables=variables,
                        )
                        for clause in value
                    )
                )
            )
            continue
        if key == "$or":
            if not isinstance(value, list):
                raise ValueError("$or necesita una lista de filtros")
            clauses.append(
                OrCondition(
                    tuple(
                        compile_filter(
                            clause,
                            dialect=dialect,
                            variables=variables,
                        )
                        for clause in value
                    )
                )
            )
            continue
        if key == "$nor":
            if not isinstance(value, list):
                raise ValueError("$nor necesita una lista de filtros")
            clauses.append(
                NotCondition(
                    OrCondition(
                        tuple(
                            compile_filter(
                                clause,
                                dialect=dialect,
                                variables=variables,
                            )
                            for clause in value
                        )
                    )
                )
            )
            continue
        if isinstance(key, str) and key.startswith("$"):
            raise OperationFailure(f"Unsupported top-level query operator: {key}")
        clauses.append(_compile_field_condition(key, value, dialect=dialect))

    if len(clauses) == 1:
        return clauses[0]
    return AndCondition(tuple(clauses))


def ensure_query_plan(
    filter_spec: Filter | None = None,
    plan: QueryNode | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables: dict[str, Any] | None = None,
) -> QueryNode:
    if plan is not None:
        return plan
    if filter_spec is None:
        return MatchAll()
    return compile_filter(filter_spec, dialect=dialect, variables=variables)
