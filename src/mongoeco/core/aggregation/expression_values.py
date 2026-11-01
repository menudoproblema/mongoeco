"""Internal absence and comparison rules for aggregation expression values."""

from enum import Enum
from typing import Any

from mongoeco.compat import MongoDialect
from mongoeco.core.bson_size import bson_string_size
from mongoeco.core.bson_values import is_bson_string
from mongoeco.core.collation import (
    CollationSpec,
    compare_with_collation,
    values_equal_with_collation,
)
from mongoeco.core.server_errors import server_error


class _Absent(Enum):
    VALUE = "missing"


MISSING = _Absent.VALUE
_STRING_CAPACITY_BYTES = 16 * 1024 * 1024
_MAX_UTF8_BYTES_PER_CHARACTER = 4


def require_expression_string_capacity(value: Any, dialect: MongoDialect) -> Any:
    """Enforce Value String capacity without treating JavaScript as String."""
    if (
        not is_bson_string(value)
        or len(value) < _STRING_CAPACITY_BYTES // _MAX_UTF8_BYTES_PER_CHARACTER
    ):
        return value
    size = bson_string_size(value) - 5
    if size >= _STRING_CAPACITY_BYTES:
        message = (
            "Tried to create string longer than 16MB"
            if dialect.key == "7.0"
            else (
                f"RCString too large. Requires size={size} "
                f"< limit={_STRING_CAPACITY_BYTES}"
            )
        )
        raise server_error(
            message, code=10334 if dialect.key == "9.0" else 16493,
            reason="expression_type",
        )
    return value


def null_if_missing(value: Any) -> Any:
    """Materialize an absent value only at a nullable consumer boundary."""
    return None if value is MISSING else value


def compare_expression_values(
    left: Any,
    right: Any,
    *,
    dialect: MongoDialect,
    collation: CollationSpec | None = None,
) -> int:
    """Keep absence distinct from null while delegating ordinary BSON order."""
    if left is MISSING:
        if right is MISSING:
            return 0
        order = compare_with_collation(
            None, right, dialect=dialect, collation=collation
        )
        return order or -1
    if right is MISSING:
        return -compare_expression_values(
            right, left, dialect=dialect, collation=collation
        )
    return compare_with_collation(left, right, dialect=dialect, collation=collation)


def expression_values_equal(
    left: Any,
    right: Any,
    *,
    dialect: MongoDialect,
    collation: CollationSpec | None = None,
) -> bool:
    """Keep missing distinct without replacing custom equality policies."""
    if left is MISSING or right is MISSING:
        return left is right
    return values_equal_with_collation(
        left, right, dialect=dialect, collation=collation
    )
