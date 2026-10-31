"""Internal absence and comparison rules for aggregation expression values."""

from enum import Enum
from typing import Any

from mongoeco.compat import MongoDialect
from mongoeco.core.collation import (
    CollationSpec,
    compare_with_collation,
    values_equal_with_collation,
)


class _Absent(Enum):
    VALUE = "missing"


MISSING = _Absent.VALUE


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
