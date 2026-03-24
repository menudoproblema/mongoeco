from typing import Any

from mongoeco.types import ObjectId
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
    NotEqualsCondition,
    NotInCondition,
    OrCondition,
    QueryNode,
    compile_filter,
)


class BSONComparator:
    """Reglas de comparación de MongoDB (Type Brackets)."""
    TYPE_ORDER: dict[type, int] = {
        type(None): 1,
        int: 2, float: 2,
        str: 3,
        dict: 4,
        list: 5,
        bytes: 6,
        ObjectId: 7,
        bool: 8,
    }

    @staticmethod
    def compare(a: Any, b: Any) -> int:
        type_a = BSONComparator.TYPE_ORDER.get(type(a), 100)
        type_b = BSONComparator.TYPE_ORDER.get(type(b), 100)

        if type_a != type_b:
            return -1 if type_a < type_b else 1

        if a == b:
            return 0

        try:
            if a < b: return -1
            if a > b: return 1
            return 0
        except TypeError:
            str_a, str_b = str(a), str(b)
            if str_a == str_b: return 0
            return -1 if str_a < str_b else 1

class QueryEngine:
    """Motor central de filtrado de MongoDB."""

    @staticmethod
    def match(document: dict[str, Any], filter_spec: dict[str, Any]) -> bool:
        return QueryEngine.match_plan(document, compile_filter(filter_spec))

    @staticmethod
    def match_plan(document: dict[str, Any], plan: QueryNode) -> bool:
        if isinstance(plan, MatchAll):
            return True
        if isinstance(plan, EqualsCondition):
            return QueryEngine._evaluate_equals(document, plan.field, plan.value)
        if isinstance(plan, NotEqualsCondition):
            return QueryEngine._evaluate_not_equals(document, plan.field, plan.value)
        if isinstance(plan, GreaterThanCondition):
            return QueryEngine._evaluate_comparison(document, plan.field, plan.value, "gt")
        if isinstance(plan, GreaterThanOrEqualCondition):
            return QueryEngine._evaluate_comparison(document, plan.field, plan.value, "gte")
        if isinstance(plan, LessThanCondition):
            return QueryEngine._evaluate_comparison(document, plan.field, plan.value, "lt")
        if isinstance(plan, LessThanOrEqualCondition):
            return QueryEngine._evaluate_comparison(document, plan.field, plan.value, "lte")
        if isinstance(plan, InCondition):
            return QueryEngine._evaluate_in(document, plan.field, plan.values)
        if isinstance(plan, NotInCondition):
            return QueryEngine._evaluate_not_in(document, plan.field, plan.values)
        if isinstance(plan, ExistsCondition):
            return QueryEngine._evaluate_exists(document, plan.field, plan.value)
        if isinstance(plan, AndCondition):
            return all(QueryEngine.match_plan(document, clause) for clause in plan.clauses)
        if isinstance(plan, OrCondition):
            return any(QueryEngine.match_plan(document, clause) for clause in plan.clauses)
        raise TypeError(f"Unsupported query plan node: {type(plan)!r}")

    @staticmethod
    def _extract_values(doc: Any, path: str) -> list[Any]:
        """
        Resuelve dot notation sobre dicts y listas.
        Devuelve candidatos observables para aproximar la semántica de Mongo.
        """
        if isinstance(doc, list):
            values: list[Any] = [doc] if not path else []
            for item in doc:
                values.extend(QueryEngine._extract_values(item, path))
            return values

        if not isinstance(doc, dict):
            return []

        if "." not in path:
            if path not in doc:
                return []

            value = doc[path]
            if isinstance(value, list):
                return [value, *value]
            return [value]

        first, rest = path.split(".", 1)
        if first not in doc:
            return []
        return QueryEngine._extract_values(doc[first], rest)

    @staticmethod
    def extract_values(doc: Any, path: str) -> list[Any]:
        """API publica para resolver valores observables por dot notation."""
        return QueryEngine._extract_values(doc, path)

    @staticmethod
    def _evaluate_equals(doc: dict[str, Any], field: str, condition: Any) -> bool:
        values = QueryEngine._extract_values(doc, field)
        candidates = values or [None]
        return any(BSONComparator.compare(value, condition) == 0 for value in candidates)

    @staticmethod
    def _evaluate_not_equals(doc: dict[str, Any], field: str, condition: Any) -> bool:
        values = QueryEngine._extract_values(doc, field)
        candidates = values or [None]
        return all(BSONComparator.compare(value, condition) != 0 for value in candidates)

    @staticmethod
    def _evaluate_comparison(doc: dict[str, Any], field: str, target: Any, operator: str) -> bool:
        candidates = QueryEngine._extract_values(doc, field)
        if not candidates:
            return False
        if operator == "gt":
            return any(BSONComparator.compare(value, target) > 0 for value in candidates)
        if operator == "gte":
            return any(BSONComparator.compare(value, target) >= 0 for value in candidates)
        if operator == "lt":
            return any(BSONComparator.compare(value, target) < 0 for value in candidates)
        if operator == "lte":
            return any(BSONComparator.compare(value, target) <= 0 for value in candidates)
        raise ValueError(f"Unsupported comparison operator kind: {operator}")

    @staticmethod
    def _evaluate_in(doc: dict[str, Any], field: str, values: tuple[Any, ...]) -> bool:
        candidates = QueryEngine._extract_values(doc, field) or [None]
        return any(
            BSONComparator.compare(candidate, item) == 0
            for candidate in candidates
            for item in values
        )

    @staticmethod
    def _evaluate_not_in(doc: dict[str, Any], field: str, values: tuple[Any, ...]) -> bool:
        candidates = QueryEngine._extract_values(doc, field) or [None]
        return not any(
            BSONComparator.compare(candidate, item) == 0
            for candidate in candidates
            for item in values
        )

    @staticmethod
    def _evaluate_exists(doc: dict[str, Any], field: str, expected: bool) -> bool:
        values = QueryEngine._extract_values(doc, field)
        exists = bool(values)
        return exists == expected
