from __future__ import annotations

import math
import re
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec, compare_with_collation
from mongoeco.core._filtering_support import extract_all_candidates as _extract_all_candidates
from mongoeco.types import Regex, UndefinedType


_FAST_SCALAR_TYPES = (str, bytes, bool, int, float)
_FAST_NUMERIC_TYPES = (int, float)


class FilteringMatchingMixin:
    @classmethod
    def _regex_item_matches_candidate(cls, candidate: Any, pattern: re.Pattern[str]) -> bool:
        return isinstance(candidate, str) and pattern.search(candidate) is not None

    @classmethod
    def _extract_all_candidates(cls, doc: Any, field: str) -> list[Any]:
        return _extract_all_candidates(doc, field)

    @classmethod
    def _in_item_matches_candidate(
        cls,
        candidate: Any,
        item: Any,
        *,
        null_matches_undefined: bool,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if isinstance(item, Regex):
            return cls._regex_item_matches_candidate(candidate, item.compile())
        if isinstance(item, re.Pattern):
            return cls._regex_item_matches_candidate(candidate, item)
        return cls._query_equality_matches(
            candidate,
            item,
            null_matches_undefined=null_matches_undefined,
            dialect=dialect,
            collation=collation,
        )

    @classmethod
    def _prepare_membership_values(
        cls,
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
            key = cls._hashable_in_lookup_key(item, collation=collation)
            if key is not None:
                literal_lookup.add(key)
                continue
            residual_values.append(item)
        return literal_lookup, regex_values, residual_values

    @classmethod
    def _candidate_matches_membership(
        cls,
        candidate: Any,
        *,
        literal_lookup: set[tuple[str, Any]],
        regex_values: list[re.Pattern[str]],
        residual_values: list[Any],
        null_matches_undefined: bool,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        key = cls._hashable_in_lookup_key(candidate, collation=collation)
        if key is not None and key in literal_lookup:
            return True
        if any(cls._regex_item_matches_candidate(candidate, pattern) for pattern in regex_values):
            return True
        return any(
            cls._query_equality_matches(
                candidate,
                item,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
            for item in residual_values
        )

    @classmethod
    def _query_equality_matches(
        cls,
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
        return cls._values_equal(candidate, expected, dialect=dialect, collation=collation)

    @classmethod
    def _comparison_matches_candidate(
        cls,
        candidate: Any,
        target: Any,
        operator: str,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if isinstance(candidate, list) and not isinstance(target, list):
            return False
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
        if operator in {"gt", ">"}:
            return comparison > 0
        if operator in {"gte", ">="}:
            return comparison >= 0
        if operator in {"lt", "<"}:
            return comparison < 0
        if operator in {"lte", "<="}:
            return comparison <= 0
        raise ValueError(f"Unsupported comparison operator kind: {operator}")

    @classmethod
    def _match_top_level_comparison(
        cls,
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
                if operator in {"gt", ">"}:
                    return value > target
                if operator in {"gte", ">="}:
                    return value >= target
                if operator in {"lt", "<"}:
                    return value < target
                if operator in {"lte", "<="}:
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
                if operator in {"gt", ">"}:
                    return value > target
                if operator in {"gte", ">="}:
                    return value >= target
                if operator in {"lt", "<"}:
                    return value < target
                if operator in {"lte", "<="}:
                    return value <= target
        if isinstance(value, list):
            if cls._comparison_matches_candidate(
                value,
                target,
                operator,
                dialect=dialect,
                collation=collation,
            ):
                return True
            return any(
                cls._comparison_matches_candidate(
                    item,
                    target,
                    operator,
                    dialect=dialect,
                    collation=collation,
                )
                for item in value
            )
        return cls._comparison_matches_candidate(
            value,
            target,
            operator,
            dialect=dialect,
            collation=collation,
        )

    @classmethod
    def _match_top_level_equals(
        cls,
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
                if cls._query_equality_matches(
                    value,
                    condition,
                    null_matches_undefined=null_matches_undefined,
                    dialect=dialect,
                    collation=collation,
                ):
                    return True
                return any(
                    cls._query_equality_matches(
                        item,
                        condition,
                        null_matches_undefined=null_matches_undefined,
                        dialect=dialect,
                        collation=collation,
                    )
                    for item in value
                )
            return cls._query_equality_matches(
                value,
                condition,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
        return cls._query_equality_matches(
            None,
            condition,
            null_matches_undefined=null_matches_undefined,
            dialect=dialect,
            collation=collation,
        )

    @classmethod
    def _evaluate_equals(
        cls,
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        null_matches_undefined: bool = False,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if "." not in field:
            return cls._match_top_level_equals(
                doc,
                field,
                condition,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
        values = cls._extract_values(doc, field)
        candidates = values or [None]
        return any(
            cls._query_equality_matches(
                value,
                condition,
                null_matches_undefined=null_matches_undefined,
                dialect=dialect,
                collation=collation,
            )
            for value in candidates
        )

    @classmethod
    def _evaluate_not_equals(
        cls,
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        values = cls._extract_values(doc, field)
        if not values:
            return not (condition is None and dialect.policy.null_query_matches_undefined())
        return all(
            not cls._query_equality_matches(
                value,
                condition,
                null_matches_undefined=dialect.policy.null_query_matches_undefined(),
                dialect=dialect,
                collation=collation,
            )
            for value in values
        )

    @classmethod
    def _evaluate_comparison(
        cls,
        doc: dict[str, Any],
        field: str,
        target: Any,
        operator: str,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if "." not in field:
            return cls._match_top_level_comparison(
                doc,
                field,
                target,
                operator,
                dialect=dialect,
                collation=collation,
            )
        candidates = cls._extract_values(doc, field)
        if not candidates:
            return False
        if len(candidates) == 1:
            return cls._comparison_matches_candidate(
                candidates[0],
                target,
                operator,
                dialect=dialect,
                collation=collation,
            )
        if operator in {"gt", "gte", "lt", "lte"}:
            return any(
                cls._comparison_matches_candidate(
                    value,
                    target,
                    operator,
                    dialect=dialect,
                    collation=collation,
                )
                for value in candidates
            )
        raise ValueError(f"Unsupported comparison operator kind: {operator}")

    @classmethod
    def _evaluate_in(
        cls,
        doc: dict[str, Any],
        field: str,
        values: tuple[Any, ...],
        *,
        null_matches_undefined: bool = False,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        candidates = cls._extract_values(doc, field) or [None]
        literal_lookup, regex_values, residual_values = cls._prepare_membership_values(
            values,
            null_matches_undefined=null_matches_undefined,
            collation=collation,
        )
        return any(
            cls._candidate_matches_membership(
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

    @classmethod
    def _evaluate_not_in(
        cls,
        doc: dict[str, Any],
        field: str,
        values: tuple[Any, ...],
        *,
        null_matches_undefined: bool = False,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        candidates = cls._extract_values(doc, field)
        if not candidates:
            has_null = any(item is None for item in values)
            return not (has_null and null_matches_undefined)
        literal_lookup, regex_values, residual_values = cls._prepare_membership_values(
            values,
            null_matches_undefined=null_matches_undefined,
            collation=collation,
        )
        return not any(
            cls._candidate_matches_membership(
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
