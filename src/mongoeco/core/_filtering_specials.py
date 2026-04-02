from __future__ import annotations

import datetime
import decimal
import math
import re
import uuid
from typing import Any

try:
    from bson.code import Code as BsonCode
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
except Exception:  # pragma: no cover - optional dependency
    BsonCode = None
    BsonMaxKey = None
    BsonMinKey = None

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core._filtering_support import compile_regex as _compile_regex
from mongoeco.core.bson_scalars import bson_numeric_alias, unwrap_bson_numeric
from mongoeco.core.collation import CollationSpec
from mongoeco.errors import OperationFailure
from mongoeco.types import (
    Binary,
    Decimal128,
    ObjectId,
    Regex,
    Timestamp,
    UndefinedType,
)


class FilteringSpecialOperatorsMixin:
    @classmethod
    def _evaluate_exists(cls, doc: dict[str, Any], field: str, expected: bool) -> bool:
        values = cls._extract_values(doc, field)
        exists = bool(values)
        return exists == expected

    @classmethod
    def _normalize_type_specifier(cls, type_spec: Any) -> tuple[str, ...]:
        if isinstance(type_spec, bool):
            raise ValueError("$type no acepta booleanos como identificadores de tipo")
        if isinstance(type_spec, int):
            numeric_mapping = {
                1: ("double",),
                2: ("string",),
                3: ("object",),
                4: ("array",),
                5: ("binData",),
                6: ("dbPointer",),
                7: ("objectId",),
                8: ("bool",),
                9: ("date",),
                10: ("null",),
                11: ("regex",),
                13: ("javascript",),
                14: ("symbol",),
                15: ("javascriptWithScope",),
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
            "dbpointer": ("dbPointer",),
            "javascript": ("javascript",),
            "symbol": ("symbol",),
            "javascriptwithscope": ("javascriptWithScope",),
            "minkey": ("minKey",),
            "maxkey": ("maxKey",),
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

    @classmethod
    def _matches_bson_type(cls, candidate: Any, alias: str) -> bool:
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
        if alias == "javascript":
            return BsonCode is not None and isinstance(candidate, BsonCode) and candidate.scope is None
        if alias == "javascriptWithScope":
            return BsonCode is not None and isinstance(candidate, BsonCode) and candidate.scope is not None
        if alias == "minKey":
            return BsonMinKey is not None and isinstance(candidate, BsonMinKey)
        if alias == "maxKey":
            return BsonMaxKey is not None and isinstance(candidate, BsonMaxKey)
        if alias == "undefined":
            return isinstance(candidate, UndefinedType)
        return False

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
        found, value = cls._get_field_value(doc, field)
        candidates = [value] if found else cls._extract_values(doc, field)
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
            if operator == "$bitsAllSet" and (candidate_bits & resolved_mask) == resolved_mask:
                return True
            if operator == "$bitsAnySet" and (candidate_bits & resolved_mask) != 0:
                return True
            if operator == "$bitsAllClear" and (candidate_bits & resolved_mask) == 0:
                return True
            if operator == "$bitsAnyClear" and (~candidate_bits & resolved_mask) != 0:
                return True
        if operator in {"$bitsAllSet", "$bitsAnySet", "$bitsAllClear", "$bitsAnyClear"}:
            return False
        raise ValueError(f"Unsupported bitwise query operator: {operator}")

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
        candidates = cls._extract_all_candidates(doc, field)
        if not candidates:
            return False
        literal_lookup: set[tuple[str, Any]] | None = None
        for expected in expected_values:
            if isinstance(expected, dict) and set(expected) == {"$elemMatch"}:
                if not any(
                    cls._match_elem_match_candidate(
                        candidate,
                        expected["$elemMatch"],
                        dialect=dialect,
                        collation=collation,
                    )
                    for candidate in candidates
                ):
                    return False
                continue
            lookup_key = cls._hashable_in_lookup_key(expected, collation=collation)
            if lookup_key is not None:
                if literal_lookup is None:
                    literal_lookup = {
                        candidate_key
                        for candidate in candidates
                        if (candidate_key := cls._hashable_in_lookup_key(candidate, collation=collation)) is not None
                    }
                if lookup_key in literal_lookup:
                    continue
            if not any(
                cls._values_equal(candidate, expected, dialect=dialect, collation=collation)
                for candidate in candidates
            ):
                return False
        return True

    @classmethod
    def _evaluate_size(cls, doc: dict[str, Any], field: str, expected_size: int) -> bool:
        found, value = cls._get_field_value(doc, field)
        if found:
            return isinstance(value, list) and len(value) == expected_size
        return any(
            isinstance(value, list) and len(value) == expected_size
            for value in cls._extract_values(doc, field)
        )

    @classmethod
    def _evaluate_mod(cls, doc: dict[str, Any], field: str, divisor: int | float, remainder: int | float) -> bool:
        values = cls._extract_values(doc, field)
        return any(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
            and math.isfinite(divisor)
            and divisor != 0
            and cls._mod_remainder_matches(cls._mongo_remainder(value, divisor), remainder)
            for value in values
        )

    @classmethod
    def _mod_remainder_matches(cls, actual: int | float, expected: int | float) -> bool:
        if isinstance(actual, float) or isinstance(expected, float):
            return math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-12)
        return actual == expected

    @classmethod
    def _mongo_remainder(cls, value: int | float, divisor: int | float) -> int | float:
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

    @classmethod
    def _evaluate_regex(cls, doc: dict[str, Any], field: str, pattern: str, options: str) -> bool:
        regex = _compile_regex(pattern, options)
        return any(isinstance(value, str) and regex.search(value) is not None for value in cls._extract_values(doc, field))

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
    ) -> bool:
        values = cls._extract_values(doc, field)
        for array_candidate in (value for value in values if isinstance(value, list)):
            if any(
                cls._match_elem_match_candidate(
                    candidate,
                    condition,
                    dialect=dialect,
                    collation=collation,
                    compiled_plan=compiled_plan,
                    compiled_dialect_key=compiled_dialect_key,
                    wrap_value=wrap_value,
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
    ) -> bool:
        if not isinstance(condition, dict):
            return cls._values_equal(candidate, condition, dialect=dialect, collation=collation)
        if compiled_plan is not None and compiled_dialect_key == dialect.key:
            if wrap_value:
                return cls.match_plan(
                    {"value": candidate},
                    compiled_plan,
                    dialect=dialect,
                    collation=collation,
                )
            if not isinstance(candidate, dict):
                return False
            return cls.match_plan(
                candidate,
                compiled_plan,
                dialect=dialect,
                collation=collation,
            )
        operator_keys = [key for key in condition if isinstance(key, str) and key.startswith("$")]
        if operator_keys:
            if len(operator_keys) != len(condition):
                raise OperationFailure("$elemMatch cannot mix operator and field conditions")
            wrapper = {"value": candidate}
            return cls.match(wrapper, {"value": condition}, dialect=dialect, collation=collation)
        if not isinstance(candidate, dict):
            return False
        return cls.match(candidate, condition, dialect=dialect, collation=collation)
