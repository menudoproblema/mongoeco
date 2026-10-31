from __future__ import annotations

import math
import uuid

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core._filtering_support import (
    ArrayElementDocument,
    compile_regex as _compile_regex,
    query_field_values,
)
from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.bson_scalars import (
    truncate_bson_int64,
    unwrap_bson_numeric,
)
from mongoeco.core.bson_values import document_mapping, matches_bson_type
from mongoeco.core.query_operators import is_elem_match_value_document
from mongoeco.core.query_plan import _normalize_type_specifier


if TYPE_CHECKING:
    from mongoeco.core.collation import CollationSpec


class FilteringSpecialOperatorsMixin:
    @classmethod
    def _evaluate_exists(cls, doc: dict[str, Any], field: str, expected: bool) -> bool:
        values = cls._extract_values(doc, field)
        exists = any(value is not MISSING for value in values)
        return exists == expected

    @classmethod
    def _normalize_type_specifier(cls, type_spec: Any) -> tuple[str, ...]:
        return _normalize_type_specifier(type_spec)

    @classmethod
    def _matches_bson_type(cls, candidate: Any, alias: str) -> bool:
        return matches_bson_type(candidate, alias)

    @classmethod
    def _evaluate_type(
        cls,
        doc: dict[str, Any],
        field: str,
        type_specs: tuple[Any, ...],
        *,
        aliases: frozenset[str] | None = None,
    ) -> bool:
        values = cls._extract_values(doc, field)
        if not values:
            return False
        if aliases is None:
            resolved_aliases: set[str] = set()
            for type_spec in type_specs:
                resolved_aliases.update(cls._normalize_type_specifier(type_spec))
            aliases = frozenset(resolved_aliases)
        return any(
            any(cls._matches_bson_type(candidate, alias) for alias in aliases)
            for candidate in values
        )

    @classmethod
    def _coerce_bitwise_mask(cls, operand: Any) -> int:
        int64_max = (1 << 63) - 1
        operand = unwrap_bson_numeric(operand)
        if isinstance(operand, bool):
            message = "bitwise query operators do not accept boolean masks"
            raise ValueError(message)
        if isinstance(operand, int):
            if operand < 0 or operand > int64_max:
                raise ValueError(
                    "numeric bitmasks must be non-negative signed 64-bit integers"
                )
            return operand
        if isinstance(operand, bytes):
            return int.from_bytes(operand, byteorder="little", signed=False)
        if isinstance(operand, uuid.UUID):
            return int.from_bytes(operand.bytes, byteorder="little", signed=False)
        if isinstance(operand, list):
            mask = 0
            for position in operand:
                if (
                    not isinstance(position, int)
                    or isinstance(position, bool)
                    or position < 0
                ):
                    raise ValueError(
                        "bit position lists must contain non-negative integers"
                    )
                if position > 63:
                    raise ValueError(
                        "bit position lists must target signed 64-bit integers"
                    )
                mask |= 1 << position
            return mask
        raise ValueError(
            "bitwise query operators require a numeric mask, BinData, or list of bit positions"
        )

    @classmethod
    def _coerce_bitwise_candidate(cls, candidate: Any) -> int | None:
        int64_min = -(1 << 63)
        int64_max = (1 << 63) - 1
        candidate = unwrap_bson_numeric(candidate)
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

    @classmethod
    def _evaluate_bitwise(
        cls,
        doc: dict[str, Any],
        field: str,
        operator: str,
        operand: Any,
        *,
        mask: int | None = None,
    ) -> bool:
        candidates = cls._extract_values(doc, field)
        if not candidates:
            return False
        resolved_mask = cls._coerce_bitwise_mask(operand) if mask is None else mask
        for candidate in candidates:
            candidate_value = cls._coerce_bitwise_candidate(candidate)
            if candidate_value is None:
                continue
            candidate_bits = (
                candidate_value & ((1 << 64) - 1)
                if isinstance(candidate_value, int) and candidate_value < 0
                else candidate_value
            )
            if (
                operator == "$bitsAllSet"
                and (candidate_bits & resolved_mask) == resolved_mask
            ):
                return True
            if operator == "$bitsAnySet" and (candidate_bits & resolved_mask) != 0:
                return True
            if operator == "$bitsAllClear" and (candidate_bits & resolved_mask) == 0:
                return True
            if operator == "$bitsAnyClear" and (~candidate_bits & resolved_mask) != 0:
                return True
        if operator in {
            "$bitsAllSet",
            "$bitsAnySet",
            "$bitsAllClear",
            "$bitsAnyClear",
        }:
            return False
        message = f"Unsupported bitwise query operator: {operator}"
        raise ValueError(message)

    @classmethod
    def _evaluate_all(
        cls,
        doc: dict[str, Any],
        field: str,
        expected_values: tuple[Any, ...],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if not expected_values:
            return False
        for expected in expected_values:
            if isinstance(expected, dict) and set(expected) == {"$elemMatch"}:
                if not cls._evaluate_elem_match(
                    doc,
                    field,
                    expected["$elemMatch"],
                    dialect=dialect,
                    collation=collation,
                ):
                    return False
            elif not cls._evaluate_in(
                doc,
                field,
                (expected,),
                null_matches_undefined=dialect.policy.null_query_matches_undefined(),
                dialect=dialect,
                collation=collation,
            ):
                return False
        return True

    @classmethod
    def _evaluate_size(
        cls, doc: dict[str, Any], field: str, expected_size: int
    ) -> bool:
        return any(
            isinstance(value, list) and len(value) == expected_size
            for value, _ in query_field_values(doc, field)
        )

    @classmethod
    def _evaluate_mod(
        cls, doc: dict[str, Any], field: str, divisor: int, remainder: int
    ) -> bool:
        return any(
            (integer := truncate_bson_int64(value)) is not None
            and cls._mongo_remainder(integer, divisor) == remainder
            for value in cls._extract_values(doc, field)
        )

    @classmethod
    def _mongo_remainder(cls, value: int, divisor: int) -> int:
        remainder = abs(value) % abs(divisor)
        return -remainder if value < 0 else remainder

    @classmethod
    def _evaluate_regex(
        cls, doc: dict[str, Any], field: str, pattern: str, options: str
    ) -> bool:
        regex = _compile_regex(pattern, options)
        return any(
            cls._regex_item_matches_candidate(value, regex)
            for value in cls._extract_values(doc, field)
        )

    @classmethod
    def _evaluate_elem_match(
        cls,
        doc: dict[str, Any],
        field: str,
        condition: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        compiled_plan=None,
        compiled_dialect_key: str | None = None,
        wrap_value: bool = False,
        variables: Mapping[str, Any] | None = None,
    ) -> bool:
        for array_candidate, _ in query_field_values(doc, field):
            if not isinstance(array_candidate, list):
                continue
            if any(
                cls._match_elem_match_candidate(
                    candidate,
                    condition,
                    dialect=dialect,
                    collation=collation,
                    compiled_plan=compiled_plan,
                    compiled_dialect_key=compiled_dialect_key,
                    wrap_value=wrap_value,
                    variables=variables,
                )
                for candidate in array_candidate
            ):
                return True
        return False

    @classmethod
    def _match_elem_match_candidate(
        cls,
        candidate: Any,
        condition: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        compiled_plan=None,
        compiled_dialect_key: str | None = None,
        wrap_value: bool = False,
        variables: Mapping[str, Any] | None = None,
    ) -> bool:
        if not isinstance(condition, dict):
            return cls._values_equal(
                candidate, condition, dialect=dialect, collation=collation
            )
        if compiled_plan is not None and compiled_dialect_key == dialect.key:
            if wrap_value:
                return cls.match_plan(
                    ArrayElementDocument(value=candidate),
                    compiled_plan,
                    dialect=dialect,
                    collation=collation,
                    variables=variables,
                )
            candidate = document_mapping(candidate)
            if candidate is None:
                return False
            return cls.match_plan(
                candidate,
                compiled_plan,
                dialect=dialect,
                collation=collation,
                variables=variables,
            )
        if is_elem_match_value_document(condition):
            wrapper = ArrayElementDocument(value=candidate)
            return cls.match(
                wrapper,
                {"value": condition},
                dialect=dialect,
                collation=collation,
            )
        candidate = document_mapping(candidate)
        if candidate is None:
            return False
        return cls.match(
            candidate,
            condition,
            dialect=dialect,
            collation=collation,
            variables=variables,
        )
