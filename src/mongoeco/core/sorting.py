from functools import cmp_to_key
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.types import Document, SortSpec


def sort_value(
    document: Document,
    field: str,
    direction: int,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Any:
    values = QueryEngine.extract_values(document, field)
    if not values:
        return None

    primary = values[0]
    if isinstance(primary, list):
        if not primary:
            return []
        members = values[1:] or primary
        ordered = sorted(members, key=cmp_to_key(dialect.compare_values))
        return ordered[0] if direction == 1 else ordered[-1]
    if len(values) > 1:
        ordered = sorted(values, key=cmp_to_key(dialect.compare_values))
        return ordered[0] if direction == 1 else ordered[-1]
    return primary


def compare_documents(
    left: Document,
    right: Document,
    sort: SortSpec,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> int:
    for field, direction in sort:
        result = dialect.compare_values(
            sort_value(left, field, direction, dialect=dialect),
            sort_value(right, field, direction, dialect=dialect),
        )
        if result != 0:
            return result if direction == 1 else -result
    return 0


def sort_documents(
    documents: list[Document],
    sort: SortSpec | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not sort:
        return documents
    return sorted(
        documents,
        key=cmp_to_key(
            lambda left, right: compare_documents(
                left,
                right,
                sort,
                dialect=dialect,
            )
        ),
    )
