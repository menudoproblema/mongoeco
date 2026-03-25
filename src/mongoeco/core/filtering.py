import datetime
import math
import re
from typing import Any
import uuid

from mongoeco.core.identity import canonical_document_id
from mongoeco.errors import OperationFailure
from mongoeco.types import ObjectId
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExistsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NotEqualsCondition,
    NotCondition,
    NotInCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
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
        uuid.UUID: 6,
        ObjectId: 7,
        bool: 8,
        datetime.datetime: 9,
    }

    @staticmethod
    def compare(a: Any, b: Any) -> int:
        type_a = BSONComparator.TYPE_ORDER.get(type(a), 100)
        type_b = BSONComparator.TYPE_ORDER.get(type(b), 100)

        if type_a != type_b:
            return -1 if type_a < type_b else 1

        if isinstance(a, dict) and isinstance(b, dict):
            left_items = list(a.items())
            right_items = list(b.items())
            for (left_key, left_value), (right_key, right_value) in zip(left_items, right_items):
                if left_key != right_key:
                    return -1 if left_key < right_key else 1
                value_comparison = BSONComparator.compare(left_value, right_value)
                if value_comparison != 0:
                    return value_comparison
            if len(left_items) == len(right_items):
                return 0
            return -1 if len(left_items) < len(right_items) else 1

        if isinstance(a, list) and isinstance(b, list):
            for left_value, right_value in zip(a, b):
                comparison = BSONComparator.compare(left_value, right_value)
                if comparison != 0:
                    return comparison
            if len(a) == len(b):
                return 0
            return -1 if len(a) < len(b) else 1

        if isinstance(a, float) and math.isnan(a):
            return 0 if isinstance(b, float) and math.isnan(b) else -1
        if isinstance(b, float) and math.isnan(b):
            return 1

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
        if isinstance(plan, AllCondition):
            return QueryEngine._evaluate_all(document, plan.field, plan.values)
        if isinstance(plan, SizeCondition):
            return QueryEngine._evaluate_size(document, plan.field, plan.value)
        if isinstance(plan, ModCondition):
            return QueryEngine._evaluate_mod(document, plan.field, plan.divisor, plan.remainder)
        if isinstance(plan, RegexCondition):
            return QueryEngine._evaluate_regex(document, plan.field, plan.pattern, plan.options)
        if isinstance(plan, NotCondition):
            return not QueryEngine.match_plan(document, plan.clause)
        if isinstance(plan, ElemMatchCondition):
            return QueryEngine._evaluate_elem_match(document, plan.field, plan.condition)
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
            if not path:
                values: list[Any] = [doc]
                for item in doc:
                    values.extend(QueryEngine._extract_values(item, path))
                return values

            if "." in path:
                first, rest = path.split(".", 1)
            else:
                first, rest = path, ""

            if first.isdigit():
                index = int(first)
                if index >= len(doc):
                    return []
                value = doc[index]
                if not rest:
                    if isinstance(value, list):
                        return [value, *value]
                    return [value]
                return QueryEngine._extract_values(value, rest)

            values = []
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
    def _get_field_value(doc: Any, path: str) -> tuple[bool, Any]:
        if isinstance(doc, list):
            if "." not in path:
                if not path.isdigit():
                    return False, None
                index = int(path)
                if index >= len(doc):
                    return False, None
                return True, doc[index]
            first, rest = path.split(".", 1)
            if not first.isdigit():
                return False, None
            index = int(first)
            if index >= len(doc):
                return False, None
            if not isinstance(doc[index], (dict, list)):
                return False, None
            return QueryEngine._get_field_value(doc[index], rest)

        if not isinstance(doc, dict):
            return False, None

        if "." not in path:
            if path not in doc:
                return False, None
            return True, doc[path]

        first, rest = path.split(".", 1)
        if first not in doc or not isinstance(doc[first], (dict, list)):
            return False, None
        return QueryEngine._get_field_value(doc[first], rest)

    @staticmethod
    def extract_values(doc: Any, path: str) -> list[Any]:
        """API publica para resolver valores observables por dot notation."""
        return QueryEngine._extract_values(doc, path)

    @staticmethod
    def _values_equal(left: Any, right: Any) -> bool:
        if isinstance(left, (dict, list)) and isinstance(right, (dict, list)):
            return canonical_document_id(left) == canonical_document_id(right)
        return BSONComparator.compare(left, right) == 0

    @staticmethod
    def _evaluate_equals(doc: dict[str, Any], field: str, condition: Any) -> bool:
        values = QueryEngine._extract_values(doc, field)
        candidates = values or [None]
        return any(QueryEngine._values_equal(value, condition) for value in candidates)

    @staticmethod
    def _evaluate_not_equals(doc: dict[str, Any], field: str, condition: Any) -> bool:
        values = QueryEngine._extract_values(doc, field)
        candidates = values or [None]
        return all(not QueryEngine._values_equal(value, condition) for value in candidates)

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
            QueryEngine._values_equal(candidate, item)
            for candidate in candidates
            for item in values
        )

    @staticmethod
    def _evaluate_not_in(doc: dict[str, Any], field: str, values: tuple[Any, ...]) -> bool:
        candidates = QueryEngine._extract_values(doc, field) or [None]
        return not any(
            QueryEngine._values_equal(candidate, item)
            for candidate in candidates
            for item in values
        )

    @staticmethod
    def _evaluate_exists(doc: dict[str, Any], field: str, expected: bool) -> bool:
        values = QueryEngine._extract_values(doc, field)
        exists = bool(values)
        return exists == expected

    @staticmethod
    def _evaluate_all(doc: dict[str, Any], field: str, expected_values: tuple[Any, ...]) -> bool:
        found, value = QueryEngine._get_field_value(doc, field)
        if found and isinstance(value, list):
            candidates = value
        elif found:
            candidates = [value]
        else:
            candidates = QueryEngine._extract_values(doc, field)
            if not candidates:
                return False
        for expected in expected_values:
            if isinstance(expected, dict) and set(expected) == {"$elemMatch"}:
                if not any(
                    QueryEngine._match_elem_match_candidate(candidate, expected["$elemMatch"])
                    for candidate in candidates
                ):
                    return False
                continue
            if not any(QueryEngine._values_equal(candidate, expected) for candidate in candidates):
                return False
        return True

    @staticmethod
    def _evaluate_size(doc: dict[str, Any], field: str, expected_size: int) -> bool:
        found, value = QueryEngine._get_field_value(doc, field)
        return found and isinstance(value, list) and len(value) == expected_size

    @staticmethod
    def _evaluate_mod(doc: dict[str, Any], field: str, divisor: int | float, remainder: int | float) -> bool:
        values = QueryEngine._extract_values(doc, field)
        return any(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
            and math.isfinite(divisor)
            and divisor != 0
            and (value - divisor * int(value / divisor)) == remainder
            for value in values
        )

    @staticmethod
    def _evaluate_regex(doc: dict[str, Any], field: str, pattern: str, options: str) -> bool:
        flags = 0
        supported = {"i": re.IGNORECASE, "m": re.MULTILINE, "s": re.DOTALL, "x": re.VERBOSE}
        for option in options:
            if option not in supported:
                raise OperationFailure(f"Unsupported regex option: {option}")
            flags |= supported[option]
        regex = re.compile(pattern, flags)
        return any(isinstance(value, str) and regex.search(value) is not None for value in QueryEngine._extract_values(doc, field))

    @staticmethod
    def _evaluate_elem_match(doc: dict[str, Any], field: str, condition: Any) -> bool:
        values = QueryEngine._extract_values(doc, field)
        array_candidates = [value for value in values if isinstance(value, list)]
        if not array_candidates:
            return False
        for array_candidate in array_candidates:
            if any(QueryEngine._match_elem_match_candidate(candidate, condition) for candidate in array_candidate):
                return True
        return False

    @staticmethod
    def _match_elem_match_candidate(candidate: Any, condition: Any) -> bool:
        if not isinstance(condition, dict):
            return QueryEngine._values_equal(candidate, condition)
        if any(isinstance(key, str) and key.startswith("$") for key in condition):
            wrapper = {"value": candidate}
            return QueryEngine.match(wrapper, {"value": condition})
        if not isinstance(candidate, dict):
            return False
        return QueryEngine.match(candidate, condition)
