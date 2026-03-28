import datetime
import decimal
import math
import re
import uuid
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_scalars import bson_numeric_alias
from mongoeco.core.collation import CollationSpec, compare_with_collation, values_equal_with_collation
from mongoeco.core.identity import canonical_document_id
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, Decimal128, ObjectId, Regex, Timestamp, UndefinedType
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExprCondition,
    ExistsCondition,
    DeferredQueryNode,
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
    TypeCondition,
    compile_filter,
)


class BSONComparator:
    """Reglas de comparación de MongoDB (Type Brackets)."""
    TYPE_ORDER = MONGODB_DIALECT_70.bson_type_order

    @staticmethod
    def compare(a: Any, b: Any) -> int:
        return MONGODB_DIALECT_70.policy.compare_values(a, b)

class QueryEngine:
    """Motor central de filtrado de MongoDB."""

    @staticmethod
    def match(
        document: dict[str, Any],
        filter_spec: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        return QueryEngine.match_plan(
            document,
            compile_filter(filter_spec, dialect=dialect),
            dialect=dialect,
            collation=collation,
        )

    @staticmethod
    def match_plan(
        document: dict[str, Any],
        plan: QueryNode,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if isinstance(plan, MatchAll):
            return True
        if isinstance(plan, DeferredQueryNode):
            raise OperationFailure(f"query plan contains deferred validation issues: {plan.issue.message}")
        if isinstance(plan, EqualsCondition):
            return QueryEngine._evaluate_equals(
                document,
                plan.field,
                plan.value,
                null_matches_undefined=plan.null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, NotEqualsCondition):
            return QueryEngine._evaluate_not_equals(
                document,
                plan.field,
                plan.value,
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, GreaterThanCondition):
            return QueryEngine._evaluate_comparison(
                document,
                plan.field,
                plan.value,
                "gt",
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, GreaterThanOrEqualCondition):
            return QueryEngine._evaluate_comparison(
                document,
                plan.field,
                plan.value,
                "gte",
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, LessThanCondition):
            return QueryEngine._evaluate_comparison(
                document,
                plan.field,
                plan.value,
                "lt",
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, LessThanOrEqualCondition):
            return QueryEngine._evaluate_comparison(
                document,
                plan.field,
                plan.value,
                "lte",
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, InCondition):
            return QueryEngine._evaluate_in(
                document,
                plan.field,
                plan.values,
                null_matches_undefined=plan.null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, NotInCondition):
            return QueryEngine._evaluate_not_in(
                document,
                plan.field,
                plan.values,
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, AllCondition):
            return QueryEngine._evaluate_all(
                document,
                plan.field,
                plan.values,
                dialect=dialect,
                collation=collation,
            )
        if isinstance(plan, SizeCondition):
            return QueryEngine._evaluate_size(document, plan.field, plan.value)
        if isinstance(plan, ModCondition):
            return QueryEngine._evaluate_mod(document, plan.field, plan.divisor, plan.remainder)
        if isinstance(plan, RegexCondition):
            return QueryEngine._evaluate_regex(document, plan.field, plan.pattern, plan.options)
        if isinstance(plan, NotCondition):
            return not QueryEngine.match_plan(document, plan.clause, dialect=dialect, collation=collation)
        if isinstance(plan, ElemMatchCondition):
            return QueryEngine._evaluate_elem_match(
                document,
                plan.field,
                plan.condition,
                dialect=plan.dialect,
                collation=collation,
            )
        if isinstance(plan, ExistsCondition):
            return QueryEngine._evaluate_exists(document, plan.field, plan.value)
        if isinstance(plan, TypeCondition):
            return QueryEngine._evaluate_type(document, plan.field, plan.values)
        if isinstance(plan, BitwiseCondition):
            return QueryEngine._evaluate_bitwise(document, plan.field, plan.operator, plan.operand)
        if isinstance(plan, ExprCondition):
            from mongoeco.core.aggregation import _expression_truthy, evaluate_expression

            value = evaluate_expression(
                document,
                plan.expression,
                plan.variables,
                dialect=dialect,
            )
            return _expression_truthy(value, dialect=dialect)
        if isinstance(plan, AndCondition):
            return all(QueryEngine.match_plan(document, clause, dialect=dialect, collation=collation) for clause in plan.clauses)
        if isinstance(plan, OrCondition):
            return any(QueryEngine.match_plan(document, clause, dialect=dialect, collation=collation) for clause in plan.clauses)
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
    def _values_equal(
        left: Any,
        right: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        return values_equal_with_collation(left, right, dialect=dialect, collation=collation)

    @staticmethod
    def _regex_item_matches_candidate(candidate: Any, pattern: re.Pattern[str]) -> bool:
        return isinstance(candidate, str) and pattern.search(candidate) is not None

    @staticmethod
    def _in_item_matches_candidate(
        candidate: Any,
        item: Any,
        *,
        null_matches_undefined: bool,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if isinstance(item, Regex):
            return QueryEngine._regex_item_matches_candidate(candidate, item.compile())
        if isinstance(item, re.Pattern):
            return QueryEngine._regex_item_matches_candidate(candidate, item)
        return QueryEngine._query_equality_matches(
            candidate,
            item,
            null_matches_undefined=null_matches_undefined,
            dialect=dialect,
            collation=collation,
        )

    @staticmethod
    def _query_equality_matches(
        candidate: Any,
        expected: Any,
        *,
        null_matches_undefined: bool,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if expected is None:
            if candidate is None:
                return True
            if null_matches_undefined and isinstance(candidate, UndefinedType):
                return True
        if null_matches_undefined and candidate is None and isinstance(expected, UndefinedType):
            return True
        return QueryEngine._values_equal(candidate, expected, dialect=dialect, collation=collation)

    @staticmethod
    def _evaluate_equals(
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        null_matches_undefined: bool = False,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        values = QueryEngine._extract_values(doc, field)
        candidates = values or [None]
        return any(
            QueryEngine._query_equality_matches(
                value,
                condition,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
            for value in candidates
        )

    @staticmethod
    def _evaluate_not_equals(
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        values = QueryEngine._extract_values(doc, field)
        if not values:
            return not (condition is None and dialect.policy.null_query_matches_undefined())
        candidates = values or [None]
        return all(
            not QueryEngine._values_equal(value, condition, dialect=dialect, collation=collation)
            for value in candidates
        )

    @staticmethod
    def _evaluate_comparison(
        doc: dict[str, Any],
        field: str,
        target: Any,
        operator: str,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        candidates = QueryEngine._extract_values(doc, field)
        if not candidates:
            return False
        policy = dialect.policy
        if operator == "gt":
            return any(compare_with_collation(value, target, dialect=dialect, collation=collation) > 0 for value in candidates)
        if operator == "gte":
            return any(compare_with_collation(value, target, dialect=dialect, collation=collation) >= 0 for value in candidates)
        if operator == "lt":
            return any(compare_with_collation(value, target, dialect=dialect, collation=collation) < 0 for value in candidates)
        if operator == "lte":
            return any(compare_with_collation(value, target, dialect=dialect, collation=collation) <= 0 for value in candidates)
        raise ValueError(f"Unsupported comparison operator kind: {operator}")

    @staticmethod
    def _evaluate_in(
        doc: dict[str, Any],
        field: str,
        values: tuple[Any, ...],
        *,
        null_matches_undefined: bool = False,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        candidates = QueryEngine._extract_values(doc, field) or [None]
        return any(
            QueryEngine._in_item_matches_candidate(
                candidate,
                item,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
            for candidate in candidates
            for item in values
        )

    @staticmethod
    def _evaluate_not_in(
        doc: dict[str, Any],
        field: str,
        values: tuple[Any, ...],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        candidates = QueryEngine._extract_values(doc, field)
        if not candidates:
            has_null = any(item is None for item in values)
            return not (has_null and dialect.policy.null_query_matches_undefined())
        return not any(
            QueryEngine._in_item_matches_candidate(
                candidate,
                item,
                null_matches_undefined=False,
                dialect=dialect,
                collation=collation,
            )
            for candidate in candidates
            for item in values
        )

    @staticmethod
    def _evaluate_exists(doc: dict[str, Any], field: str, expected: bool) -> bool:
        values = QueryEngine._extract_values(doc, field)
        exists = bool(values)
        return exists == expected

    @staticmethod
    def _normalize_type_specifier(type_spec: Any) -> tuple[str, ...]:
        if isinstance(type_spec, bool):
            raise ValueError("$type no acepta booleanos como identificadores de tipo")
        if isinstance(type_spec, int):
            numeric_mapping = {
                1: ("double",),
                2: ("string",),
                3: ("object",),
                4: ("array",),
                5: ("binData",),
                7: ("objectId",),
                8: ("bool",),
                9: ("date",),
                10: ("null",),
                11: ("regex",),
                16: ("int",),
                17: ("timestamp",),
                18: ("long",),
                19: ("decimal",),
            }
            if type_spec not in numeric_mapping:
                raise ValueError("$type usa un codigo BSON no soportado")
            return numeric_mapping[type_spec]
        if not isinstance(type_spec, str):
            raise ValueError("$type necesita alias string o codigo entero BSON")
        alias_mapping = {
            "double": ("double",),
            "string": ("string",),
            "object": ("object",),
            "array": ("array",),
            "binData": ("binData",),
            "objectId": ("objectId",),
            "bool": ("bool",),
            "date": ("date",),
            "null": ("null",),
            "regex": ("regex",),
            "int": ("int",),
            "timestamp": ("timestamp",),
            "long": ("long",),
            "decimal": ("decimal",),
            "undefined": ("undefined",),
            "number": ("double", "int", "long", "decimal"),
        }
        normalized = type_spec.strip()
        if normalized not in alias_mapping:
            raise ValueError("$type usa un alias BSON no soportado")
        return alias_mapping[normalized]

    @staticmethod
    def _matches_bson_type(candidate: Any, alias: str) -> bool:
        numeric_alias = bson_numeric_alias(candidate)
        if numeric_alias is not None:
            if alias == "number":
                return True
            return numeric_alias == alias
        if alias == "double":
            return isinstance(candidate, float) and not isinstance(candidate, bool)
        if alias == "decimal":
            return isinstance(candidate, (decimal.Decimal, Decimal128))
        if alias in {"int", "long"}:
            return isinstance(candidate, int) and not isinstance(candidate, bool)
        if alias == "string":
            return isinstance(candidate, str)
        if alias == "object":
            return isinstance(candidate, dict)
        if alias == "array":
            return isinstance(candidate, list)
        if alias == "binData":
            return isinstance(candidate, (bytes, Binary, uuid.UUID))
        if alias == "objectId":
            return isinstance(candidate, ObjectId)
        if alias == "bool":
            return isinstance(candidate, bool)
        if alias == "date":
            return isinstance(candidate, datetime.datetime)
        if alias == "timestamp":
            return isinstance(candidate, Timestamp)
        if alias == "null":
            return candidate is None
        if alias == "regex":
            return isinstance(candidate, (re.Pattern, Regex))
        if alias == "undefined":
            return isinstance(candidate, UndefinedType)
        return False

    @staticmethod
    def _evaluate_type(
        doc: dict[str, Any],
        field: str,
        type_specs: tuple[Any, ...],
    ) -> bool:
        values = QueryEngine._extract_values(doc, field)
        if not values:
            return False
        aliases: set[str] = set()
        for type_spec in type_specs:
            aliases.update(QueryEngine._normalize_type_specifier(type_spec))
        return any(
            any(QueryEngine._matches_bson_type(candidate, alias) for alias in aliases)
            for candidate in values
        )

    @staticmethod
    def _coerce_bitwise_mask(operand: Any) -> int:
        int64_min = -(1 << 63)
        int64_max = (1 << 63) - 1
        if isinstance(operand, bool):
            raise ValueError("bitwise query operators do not accept boolean masks")
        if isinstance(operand, int):
            if operand < 0 or operand > int64_max:
                raise ValueError("numeric bitmasks must be non-negative signed 64-bit integers")
            return operand
        if isinstance(operand, bytes):
            return int.from_bytes(operand, byteorder="little", signed=False)
        if isinstance(operand, uuid.UUID):
            return int.from_bytes(operand.bytes, byteorder="little", signed=False)
        if isinstance(operand, list):
            mask = 0
            for position in operand:
                if not isinstance(position, int) or isinstance(position, bool) or position < 0:
                    raise ValueError("bit position lists must contain non-negative integers")
                mask |= 1 << position
            return mask
        raise ValueError("bitwise query operators require a numeric mask, BinData, or list of bit positions")

    @staticmethod
    def _coerce_bitwise_candidate(candidate: Any) -> int | None:
        int64_min = -(1 << 63)
        int64_max = (1 << 63) - 1
        if isinstance(candidate, bool):
            return None
        if isinstance(candidate, int):
            if candidate < int64_min or candidate > int64_max:
                return None
            return candidate
        if isinstance(candidate, float):
            if not math.isfinite(candidate) or not candidate.is_integer():
                return None
            integer = int(candidate)
            if integer < int64_min or integer > int64_max:
                return None
            return integer
        if isinstance(candidate, bytes):
            return int.from_bytes(candidate, byteorder="little", signed=False)
        if isinstance(candidate, uuid.UUID):
            return int.from_bytes(candidate.bytes, byteorder="little", signed=False)
        return None

    @staticmethod
    def _evaluate_bitwise(
        doc: dict[str, Any],
        field: str,
        operator: str,
        operand: Any,
    ) -> bool:
        found, candidate = QueryEngine._get_field_value(doc, field)
        if not found:
            return False
        candidate_value = QueryEngine._coerce_bitwise_candidate(candidate)
        if candidate_value is None:
            return False
        mask = QueryEngine._coerce_bitwise_mask(operand)
        if operator == "$bitsAllSet":
            return (candidate_value & mask) == mask
        if operator == "$bitsAnySet":
            return (candidate_value & mask) != 0
        if operator == "$bitsAllClear":
            return (candidate_value & mask) == 0
        if operator == "$bitsAnyClear":
            return (~candidate_value & mask) != 0
        raise ValueError(f"Unsupported bitwise query operator: {operator}")

    @staticmethod
    def _evaluate_all(
        doc: dict[str, Any],
        field: str,
        expected_values: tuple[Any, ...],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
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
                    QueryEngine._match_elem_match_candidate(candidate, expected["$elemMatch"], dialect=dialect, collation=collation)
                    for candidate in candidates
                ):
                    return False
                continue
            if not any(
                QueryEngine._values_equal(candidate, expected, dialect=dialect, collation=collation)
                for candidate in candidates
            ):
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
            and QueryEngine._mongo_remainder(value, divisor) == remainder
            for value in values
        )

    @staticmethod
    def _mongo_remainder(value: int | float, divisor: int | float) -> int | float:
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and isinstance(divisor, int)
            and not isinstance(divisor, bool)
        ):
            quotient = abs(value) // abs(divisor)
            if (value < 0) != (divisor < 0):
                quotient = -quotient
            return value - divisor * quotient
        quotient = math.trunc(value / divisor)
        return value - divisor * quotient

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
    def _evaluate_elem_match(
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        values = QueryEngine._extract_values(doc, field)
        array_candidates = [value for value in values if isinstance(value, list)]
        if not array_candidates:
            return False
        for array_candidate in array_candidates:
            if any(
                QueryEngine._match_elem_match_candidate(candidate, condition, dialect=dialect, collation=collation)
                for candidate in array_candidate
            ):
                return True
        return False

    @staticmethod
    def _match_elem_match_candidate(
        candidate: Any,
        condition: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if not isinstance(condition, dict):
            return QueryEngine._values_equal(candidate, condition, dialect=dialect, collation=collation)
        if any(isinstance(key, str) and key.startswith("$") for key in condition):
            wrapper = {"value": candidate}
            return QueryEngine.match(wrapper, {"value": condition}, dialect=dialect, collation=collation)
        if not isinstance(candidate, dict):
            return False
        return QueryEngine.match(candidate, condition, dialect=dialect, collation=collation)
