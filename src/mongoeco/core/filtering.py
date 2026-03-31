import datetime
import decimal
from functools import lru_cache
import math
import re
import uuid
from typing import Any, assert_never

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_scalars import bson_numeric_alias
from mongoeco.core.collation import CollationSpec, compare_with_collation, values_equal_with_collation
from mongoeco.core.identity import canonical_document_id
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, DBRef, Decimal128, ObjectId, Regex, Timestamp, UndefinedType
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExprCondition,
    ExistsCondition,
    DeferredQueryNode,
    JsonSchemaCondition,
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
    is_concrete_query_node,
)


HANDLED_QUERY_NODE_TYPES: tuple[type[QueryNode], ...] = (
    MatchAll,
    DeferredQueryNode,
    EqualsCondition,
    NotEqualsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    InCondition,
    NotInCondition,
    AllCondition,
    SizeCondition,
    ModCondition,
    RegexCondition,
    NotCondition,
    ElemMatchCondition,
    ExistsCondition,
    TypeCondition,
    BitwiseCondition,
    ExprCondition,
    JsonSchemaCondition,
    AndCondition,
    OrCondition,
)

_FAST_SCALAR_TYPES = (str, bytes, bool, int, float)
_FAST_NUMERIC_TYPES = (int, float)


@lru_cache(maxsize=4096)
def _split_path(path: str) -> tuple[str, ...]:
    """Divide una ruta dot-notation y cachea el resultado."""
    if not path:
        return ()
    return tuple(path.split("."))


@lru_cache(maxsize=256)
def _compile_regex(pattern: str, options: str) -> re.Pattern[str]:
    flags = 0
    supported = {
        "i": re.IGNORECASE,
        "m": re.MULTILINE,
        "s": re.DOTALL,
        "x": re.VERBOSE,
    }
    seen: set[str] = set()
    for option in options:
        if option not in supported:
            raise OperationFailure(f"Unsupported regex option: {option}")
        if option in seen:
            raise OperationFailure(f"Duplicate regex option: {option}")
        seen.add(option)
        flags |= supported[option]
    return re.compile(pattern, flags)


class BSONComparator:
    """Reglas de comparación de MongoDB (Type Brackets)."""
    TYPE_ORDER = MONGODB_DIALECT_70.bson_type_order

    @staticmethod
    def compare(
        a: Any,
        b: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> int:
        return dialect.policy.compare_values(a, b)

class QueryEngine:
    """Motor central de filtrado de MongoDB."""

    @staticmethod
    def _hashable_in_lookup_key(
        value: Any,
        *,
        collation: CollationSpec | None = None,
    ) -> tuple[str, Any] | None:
        if value is None:
            return ("none", None)
        if isinstance(value, bool):
            return ("bool", value)
        if isinstance(value, bytes):
            return ("bytes", value)
        if isinstance(value, datetime.datetime):
            return ("date", value)
        if isinstance(value, uuid.UUID):
            return ("uuid", value)
        if isinstance(value, ObjectId):
            return ("objectid", value)
        if isinstance(value, Timestamp):
            return ("timestamp", value)
        return None

    @staticmethod
    def _path_mapping(value: Any) -> dict[str, Any] | None:
        if isinstance(value, DBRef):
            return value.as_document()
        if isinstance(value, dict):
            return value
        return None

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
        if not is_concrete_query_node(plan):
            raise TypeError(f"Unsupported query plan node: {type(plan)!r}")
        match plan:
            case MatchAll():
                return True
            case DeferredQueryNode(issue=issue):
                raise OperationFailure(f"query plan contains deferred validation issues: {issue.message}")
            case EqualsCondition(field=field, value=value, null_matches_undefined=null_matches_undefined):
                return QueryEngine._evaluate_equals(
                    document,
                    field,
                    value,
                    null_matches_undefined=null_matches_undefined,
                    dialect=dialect,
                    collation=collation,
                )
            case NotEqualsCondition(field=field, value=value):
                return QueryEngine._evaluate_not_equals(
                    document,
                    field,
                    value,
                    dialect=dialect,
                    collation=collation,
                )
            case GreaterThanCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "gt",
                    dialect=dialect,
                    collation=collation,
                )
            case GreaterThanOrEqualCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "gte",
                    dialect=dialect,
                    collation=collation,
                )
            case LessThanCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "lt",
                    dialect=dialect,
                    collation=collation,
                )
            case LessThanOrEqualCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "lte",
                    dialect=dialect,
                    collation=collation,
                )
            case InCondition(field=field, values=values, null_matches_undefined=null_matches_undefined):
                return QueryEngine._evaluate_in(
                    document,
                    field,
                    values,
                    null_matches_undefined=null_matches_undefined,
                    dialect=dialect,
                    collation=collation,
                )
            case NotInCondition(field=field, values=values):
                return QueryEngine._evaluate_not_in(
                    document,
                    field,
                    values,
                    dialect=dialect,
                    collation=collation,
                )
            case AllCondition(field=field, values=values):
                return QueryEngine._evaluate_all(
                    document,
                    field,
                    values,
                    dialect=dialect,
                    collation=collation,
                )
            case SizeCondition(field=field, value=value):
                return QueryEngine._evaluate_size(document, field, value)
            case ModCondition(field=field, divisor=divisor, remainder=remainder):
                return QueryEngine._evaluate_mod(document, field, divisor, remainder)
            case RegexCondition(field=field, pattern=pattern, options=options):
                return QueryEngine._evaluate_regex(document, field, pattern, options)
            case NotCondition(clause=clause):
                return not QueryEngine.match_plan(document, clause, dialect=dialect, collation=collation)
            case ElemMatchCondition(
                field=field,
                condition=condition,
                dialect=elem_dialect,
                compiled_plan=compiled_plan,
                wrap_value=wrap_value,
            ):
                return QueryEngine._evaluate_elem_match(
                    document,
                    field,
                    condition,
                    dialect=elem_dialect,
                    collation=collation,
                    compiled_plan=compiled_plan,
                    wrap_value=wrap_value,
                )
            case ExistsCondition(field=field, value=value):
                return QueryEngine._evaluate_exists(document, field, value)
            case TypeCondition(field=field, values=values, aliases=aliases):
                return QueryEngine._evaluate_type(document, field, values, aliases=aliases)
            case BitwiseCondition(field=field, operator=operator, operand=operand, mask=mask):
                return QueryEngine._evaluate_bitwise(document, field, operator, operand, mask=mask)
            case ExprCondition(expression=expression, variables=variables):
                from mongoeco.core.aggregation import _expression_truthy, evaluate_expression

                value = evaluate_expression(
                    document,
                    expression,
                    variables,
                    dialect=dialect,
                )
                return _expression_truthy(value, dialect=dialect)
            case JsonSchemaCondition(schema=schema):
                from mongoeco.core.schema_validation import CompiledJsonSchema

                return CompiledJsonSchema(schema).validate(document).valid
            case AndCondition(clauses=clauses):
                return all(
                    QueryEngine.match_plan(document, clause, dialect=dialect, collation=collation)
                    for clause in clauses
                )
            case OrCondition(clauses=clauses):
                return any(
                    QueryEngine.match_plan(document, clause, dialect=dialect, collation=collation)
                    for clause in clauses
                )
            case _:
                assert_never(plan)

    @staticmethod
    def _extract_values(doc: Any, path: str) -> list[Any]:
        """
        Resuelve dot notation sobre dicts y listas de forma iterativa.
        """
        doc_mapping = QueryEngine._path_mapping(doc)
        if doc_mapping is not None and doc_mapping is not doc:
            return QueryEngine._extract_values(doc_mapping, path)

        parts = _split_path(path)
        if not parts:
            if isinstance(doc, list):
                values: list[Any] = [doc]
                for item in doc:
                    values.extend(QueryEngine._extract_values(item, path))
                return values
            return []

        current_level = [doc]
        for index, part in enumerate(parts):
            next_level = []
            is_digit = part.isdigit()
            idx = int(part) if is_digit else -1
            is_terminal = index == len(parts) - 1

            for item in current_level:
                if isinstance(item, list):
                    if is_digit:
                        if 0 <= idx < len(item):
                            value = item[idx]
                            if isinstance(value, list):
                                next_level.append(value)
                                if is_terminal:
                                    next_level.extend(value)
                            else:
                                next_level.append(value)
                    else:
                        for subitem in item:
                            mapping = QueryEngine._path_mapping(subitem)
                            if mapping is not None and part in mapping:
                                val = mapping[part]
                                if isinstance(val, list):
                                    next_level.append(val)
                                    if is_terminal:
                                        next_level.extend(val)
                                else:
                                    next_level.append(val)
                else:
                    mapping = QueryEngine._path_mapping(item)
                    if mapping is None or part not in mapping:
                        continue
                    val = mapping[part]
                    if isinstance(val, list):
                        next_level.append(val)
                        if is_terminal:
                            next_level.extend(val)
                    else:
                        next_level.append(val)

            if not next_level:
                return []
            current_level = next_level

        return current_level

    @staticmethod
    def _get_field_value(doc: Any, path: str) -> tuple[bool, Any]:
        """Versión rápida de acceso a un único valor (sin expansión de arrays)."""
        doc_mapping = QueryEngine._path_mapping(doc)
        if doc_mapping is not None and doc_mapping is not doc:
            return QueryEngine._get_field_value(doc_mapping, path)

        parts = _split_path(path)
        if not parts:
            return True, doc

        current = doc
        for part in parts:
            if isinstance(current, list):
                if not part.isdigit():
                    return False, None
                idx = int(part)
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return False, None
            else:
                mapping = QueryEngine._path_mapping(current)
                if mapping is None or part not in mapping:
                    return False, None
                current = mapping[part]
        return True, current

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
    def _extract_all_candidates(doc: Any, field: str) -> list[Any]:
        parts = _split_path(field)
        if not parts:
            return []

        current_level = [doc]
        for index, part in enumerate(parts):
            next_level: list[Any] = []
            is_digit = part.isdigit()
            idx = int(part) if is_digit else -1
            is_last = index == len(parts) - 1

            for item in current_level:
                if isinstance(item, list):
                    if is_digit:
                        if 0 <= idx < len(item):
                            value = item[idx]
                            if is_last:
                                if isinstance(value, list):
                                    next_level.extend(value)
                                else:
                                    next_level.append(value)
                            elif isinstance(value, list):
                                next_level.append(value)
                                next_level.extend(value)
                            else:
                                next_level.append(value)
                    else:
                        for subitem in item:
                            mapping = QueryEngine._path_mapping(subitem)
                            if mapping is not None and part in mapping:
                                value = mapping[part]
                                if is_last:
                                    if isinstance(value, list):
                                        next_level.extend(value)
                                    else:
                                        next_level.append(value)
                                elif isinstance(value, list):
                                    next_level.append(value)
                                    next_level.extend(value)
                                else:
                                    next_level.append(value)
                else:
                    mapping = QueryEngine._path_mapping(item)
                    if mapping is None or part not in mapping:
                        continue
                    value = mapping[part]
                    if is_last:
                        if isinstance(value, list):
                            next_level.extend(value)
                        else:
                            next_level.append(value)
                    elif isinstance(value, list):
                        next_level.append(value)
                        next_level.extend(value)
                    else:
                        next_level.append(value)

            if not next_level:
                return []
            current_level = next_level

        return current_level

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
    def _prepare_membership_values(
        values: tuple[Any, ...],
        *,
        null_matches_undefined: bool,
        collation: CollationSpec | None = None,
    ) -> tuple[set[tuple[str, Any]], list[re.Pattern[str]], list[Any]]:
        literal_lookup: set[tuple[str, Any]] = set()
        regex_values: list[re.Pattern[str]] = []
        residual_values: list[Any] = []
        for item in values:
            if isinstance(item, Regex):
                regex_values.append(item.compile())
                continue
            if isinstance(item, re.Pattern):
                regex_values.append(item)
                continue
            if item is None and null_matches_undefined:
                residual_values.append(item)
                continue
            key = QueryEngine._hashable_in_lookup_key(item, collation=collation)
            if key is not None:
                literal_lookup.add(key)
                continue
            residual_values.append(item)
        return literal_lookup, regex_values, residual_values

    @staticmethod
    def _candidate_matches_membership(
        candidate: Any,
        *,
        literal_lookup: set[tuple[str, Any]],
        regex_values: list[re.Pattern[str]],
        residual_values: list[Any],
        null_matches_undefined: bool,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        key = QueryEngine._hashable_in_lookup_key(candidate, collation=collation)
        if key is not None and key in literal_lookup:
            return True
        if any(QueryEngine._regex_item_matches_candidate(candidate, pattern) for pattern in regex_values):
            return True
        return any(
            QueryEngine._query_equality_matches(
                candidate,
                item,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
            for item in residual_values
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
        if collation is None and dialect is MONGODB_DIALECT_70:
            candidate_type = type(candidate)
            expected_type = type(expected)
            if (
                candidate_type is expected_type
                and candidate_type in _FAST_SCALAR_TYPES
                and not (candidate_type is float and (math.isnan(candidate) or math.isnan(expected)))
            ):
                return candidate == expected
            if (
                candidate_type in _FAST_NUMERIC_TYPES
                and expected_type in _FAST_NUMERIC_TYPES
                and candidate_type is not bool
                and expected_type is not bool
                and not (
                    (candidate_type is float and math.isnan(candidate))
                    or (expected_type is float and math.isnan(expected))
                )
            ):
                return candidate == expected
        return QueryEngine._values_equal(candidate, expected, dialect=dialect, collation=collation)

    @staticmethod
    def _comparison_matches_candidate(
        candidate: Any,
        target: Any,
        operator: str,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if collation is None and dialect is MONGODB_DIALECT_70:
            candidate_type = type(candidate)
            target_type = type(target)
            if (
                candidate_type is target_type
                and candidate_type in _FAST_SCALAR_TYPES
                and not (candidate_type is float and (math.isnan(candidate) or math.isnan(target)))
            ):
                comparison = -1 if candidate < target else 1 if candidate > target else 0
            elif (
                candidate_type in _FAST_NUMERIC_TYPES
                and target_type in _FAST_NUMERIC_TYPES
                and candidate_type is not bool
                and target_type is not bool
                and not (
                    (candidate_type is float and math.isnan(candidate))
                    or (target_type is float and math.isnan(target))
                )
            ):
                comparison = -1 if candidate < target else 1 if candidate > target else 0
            else:
                comparison = compare_with_collation(
                    candidate,
                    target,
                    dialect=dialect,
                    collation=collation,
                )
        else:
            comparison = compare_with_collation(
                candidate,
                target,
                dialect=dialect,
                collation=collation,
            )

        if operator == "gt" or operator == ">":
            return comparison > 0
        if operator == "gte" or operator == ">=":
            return comparison >= 0
        if operator == "lt" or operator == "<":
            return comparison < 0
        if operator == "lte" or operator == "<=":
            return comparison <= 0
        raise ValueError(f"Unsupported comparison operator kind: {operator}")

    @staticmethod
    def _match_top_level_comparison(
        doc: dict[str, Any],
        field: str,
        target: Any,
        operator: str,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if field not in doc:
            return False
        value = doc[field]
        if collation is None and dialect is MONGODB_DIALECT_70 and not isinstance(value, list):
            value_type = type(value)
            target_type = type(target)
            if (
                value_type is target_type
                and value_type in _FAST_SCALAR_TYPES
                and not (value_type is float and (math.isnan(value) or math.isnan(target)))
            ):
                if operator == "gt" or operator == ">":
                    return value > target
                if operator == "gte" or operator == ">=":
                    return value >= target
                if operator == "lt" or operator == "<":
                    return value < target
                if operator == "lte" or operator == "<=":
                    return value <= target
            elif (
                value_type in _FAST_NUMERIC_TYPES
                and target_type in _FAST_NUMERIC_TYPES
                and value_type is not bool
                and target_type is not bool
                and not (
                    (value_type is float and math.isnan(value))
                    or (target_type is float and math.isnan(target))
                )
            ):
                if operator == "gt" or operator == ">":
                    return value > target
                if operator == "gte" or operator == ">=":
                    return value >= target
                if operator == "lt" or operator == "<":
                    return value < target
                if operator == "lte" or operator == "<=":
                    return value <= target
        if isinstance(value, list):
            if QueryEngine._comparison_matches_candidate(
                value,
                target,
                operator,
                dialect=dialect,
                collation=collation,
            ):
                return True
            return any(
                QueryEngine._comparison_matches_candidate(
                    item,
                    target,
                    operator,
                    dialect=dialect,
                    collation=collation,
                )
                for item in value
            )
        return QueryEngine._comparison_matches_candidate(
            value,
            target,
            operator,
            dialect=dialect,
            collation=collation,
        )

    @staticmethod
    def _match_top_level_equals(
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        null_matches_undefined: bool = False,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if field in doc:
            value = doc[field]
            if (
                collation is None
                and dialect is MONGODB_DIALECT_70
                and not null_matches_undefined
                and not isinstance(value, list)
            ):
                value_type = type(value)
                condition_type = type(condition)
                if (
                    value_type is condition_type
                    and value_type in _FAST_SCALAR_TYPES
                    and not (value_type is float and (math.isnan(value) or math.isnan(condition)))
                ):
                    return value == condition
                if (
                    value_type in _FAST_NUMERIC_TYPES
                    and condition_type in _FAST_NUMERIC_TYPES
                    and value_type is not bool
                    and condition_type is not bool
                    and not (
                        (value_type is float and math.isnan(value))
                        or (condition_type is float and math.isnan(condition))
                    )
                ):
                    return value == condition
            if isinstance(value, list):
                if QueryEngine._query_equality_matches(
                    value,
                    condition,
                    null_matches_undefined=null_matches_undefined,
                    dialect=dialect,
                    collation=collation,
                ):
                    return True
                return any(
                    QueryEngine._query_equality_matches(
                        item,
                        condition,
                        null_matches_undefined=null_matches_undefined,
                        dialect=dialect,
                        collation=collation,
                    )
                    for item in value
                )
            return QueryEngine._query_equality_matches(
                value,
                condition,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
        return QueryEngine._query_equality_matches(
            None,
            condition,
            null_matches_undefined=null_matches_undefined,
            dialect=dialect,
            collation=collation,
        )

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
        if "." not in field:
            return QueryEngine._match_top_level_equals(
                doc,
                field,
                condition,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
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
        return all(
            not QueryEngine._query_equality_matches(
                value,
                condition,
                null_matches_undefined=dialect.policy.null_query_matches_undefined(),
                dialect=dialect,
                collation=collation,
            )
            for value in values
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
        if "." not in field:
            return QueryEngine._match_top_level_comparison(
                doc,
                field,
                target,
                operator,
                dialect=dialect,
                collation=collation,
            )
        candidates = QueryEngine._extract_values(doc, field)
        if not candidates:
            return False
        if len(candidates) == 1:
            return QueryEngine._comparison_matches_candidate(
                candidates[0],
                target,
                operator,
                dialect=dialect,
                collation=collation,
            )
        if operator == "gt":
            return any(
                QueryEngine._comparison_matches_candidate(
                    value,
                    target,
                    operator,
                    dialect=dialect,
                    collation=collation,
                )
                for value in candidates
            )
        if operator == "gte":
            return any(
                QueryEngine._comparison_matches_candidate(
                    value,
                    target,
                    operator,
                    dialect=dialect,
                    collation=collation,
                )
                for value in candidates
            )
        if operator == "lt":
            return any(
                QueryEngine._comparison_matches_candidate(
                    value,
                    target,
                    operator,
                    dialect=dialect,
                    collation=collation,
                )
                for value in candidates
            )
        if operator == "lte":
            return any(
                QueryEngine._comparison_matches_candidate(
                    value,
                    target,
                    operator,
                    dialect=dialect,
                    collation=collation,
                )
                for value in candidates
            )
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
        literal_lookup, regex_values, residual_values = QueryEngine._prepare_membership_values(
            values,
            null_matches_undefined=null_matches_undefined,
            collation=collation,
        )
        return any(
            QueryEngine._candidate_matches_membership(
                candidate,
                literal_lookup=literal_lookup,
                regex_values=regex_values,
                residual_values=residual_values,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
            for candidate in candidates
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
        literal_lookup, regex_values, residual_values = QueryEngine._prepare_membership_values(
            values,
            null_matches_undefined=False,
            collation=collation,
        )
        return not any(
            QueryEngine._candidate_matches_membership(
                candidate,
                literal_lookup=literal_lookup,
                regex_values=regex_values,
                residual_values=residual_values,
                null_matches_undefined=False,
                dialect=dialect,
                collation=collation,
            )
            for candidate in candidates
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
            "bindata": ("binData",),
            "objectid": ("objectId",),
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
        normalized = type_spec.strip().casefold()
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
        *,
        aliases: frozenset[str] | None = None,
    ) -> bool:
        values = QueryEngine._extract_values(doc, field)
        if not values:
            return False
        if aliases is None:
            resolved_aliases: set[str] = set()
            for type_spec in type_specs:
                resolved_aliases.update(QueryEngine._normalize_type_specifier(type_spec))
            aliases = frozenset(resolved_aliases)
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
                if position > 63:
                    raise ValueError("bit position lists must target signed 64-bit integers")
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
        *,
        mask: int | None = None,
    ) -> bool:
        found, value = QueryEngine._get_field_value(doc, field)
        candidates = [value] if found else QueryEngine._extract_values(doc, field)
        if not candidates:
            return False
        resolved_mask = QueryEngine._coerce_bitwise_mask(operand) if mask is None else mask
        for candidate in candidates:
            candidate_value = QueryEngine._coerce_bitwise_candidate(candidate)
            if candidate_value is None:
                continue
            if operator == "$bitsAllSet" and (candidate_value & resolved_mask) == resolved_mask:
                return True
            if operator == "$bitsAnySet" and (candidate_value & resolved_mask) != 0:
                return True
            if operator == "$bitsAllClear" and (candidate_value & resolved_mask) == 0:
                return True
            if operator == "$bitsAnyClear" and (~candidate_value & resolved_mask) != 0:
                return True
        if operator in {"$bitsAllSet", "$bitsAnySet", "$bitsAllClear", "$bitsAnyClear"}:
            return False
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
        if not expected_values:
            return False
        candidates = QueryEngine._extract_all_candidates(doc, field)
        if not candidates:
            return False
        literal_lookup: set[tuple[str, Any]] | None = None
        for expected in expected_values:
            if isinstance(expected, dict) and set(expected) == {"$elemMatch"}:
                if not any(
                    QueryEngine._match_elem_match_candidate(candidate, expected["$elemMatch"], dialect=dialect, collation=collation)
                    for candidate in candidates
                ):
                    return False
                continue
            lookup_key = QueryEngine._hashable_in_lookup_key(expected, collation=collation)
            if lookup_key is not None:
                if literal_lookup is None:
                    literal_lookup = {
                        candidate_key
                        for candidate in candidates
                        if (candidate_key := QueryEngine._hashable_in_lookup_key(candidate, collation=collation)) is not None
                    }
                if lookup_key in literal_lookup:
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
        if found:
            return isinstance(value, list) and len(value) == expected_size
        return any(
            isinstance(value, list) and len(value) == expected_size
            for value in QueryEngine._extract_values(doc, field)
        )

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
        regex = _compile_regex(pattern, options)
        return any(isinstance(value, str) and regex.search(value) is not None for value in QueryEngine._extract_values(doc, field))

    @staticmethod
    def _evaluate_elem_match(
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        compiled_plan: QueryNode | None = None,
        wrap_value: bool = False,
    ) -> bool:
        values = QueryEngine._extract_values(doc, field)
        for array_candidate in (value for value in values if isinstance(value, list)):
            if any(
                QueryEngine._match_elem_match_candidate(
                    candidate,
                    condition,
                    dialect=dialect,
                    collation=collation,
                    compiled_plan=compiled_plan,
                    wrap_value=wrap_value,
                )
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
        compiled_plan: QueryNode | None = None,
        wrap_value: bool = False,
    ) -> bool:
        if not isinstance(condition, dict):
            return QueryEngine._values_equal(candidate, condition, dialect=dialect, collation=collation)
        if compiled_plan is not None:
            if wrap_value:
                return QueryEngine.match_plan(
                    {"value": candidate},
                    compiled_plan,
                    dialect=dialect,
                    collation=collation,
                )
            if not isinstance(candidate, dict):
                return False
            return QueryEngine.match_plan(
                candidate,
                compiled_plan,
                dialect=dialect,
                collation=collation,
            )
        operator_keys = [
            key
            for key in condition
            if isinstance(key, str) and key.startswith("$")
        ]
        if operator_keys:
            if len(operator_keys) != len(condition):
                raise OperationFailure("$elemMatch cannot mix operator and field conditions")
            wrapper = {"value": candidate}
            return QueryEngine.match(wrapper, {"value": condition}, dialect=dialect, collation=collation)
        if not isinstance(candidate, dict):
            return False
        return QueryEngine.match(candidate, condition, dialect=dialect, collation=collation)
