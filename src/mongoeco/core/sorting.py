from functools import cmp_to_key
import heapq
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec, compare_with_collation
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.types import Document, SortSpec


def sort_value(
    document: Document,
    field: str,
    direction: int,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> Any:
    values = QueryEngine.extract_values(document, field)
    if not values:
        return None

    primary = values[0]
    if isinstance(primary, list):
        if not primary:
            return []
        members = values[1:] or primary
        ordered = sorted(
            members,
            key=cmp_to_key(
                lambda left, right: compare_with_collation(
                    left,
                    right,
                    dialect=dialect,
                    collation=collation,
                )
            ),
        )
        return ordered[0] if direction == 1 else ordered[-1]
    if len(values) > 1:
        ordered = sorted(
            values,
            key=cmp_to_key(
                lambda left, right: compare_with_collation(
                    left,
                    right,
                    dialect=dialect,
                    collation=collation,
                )
            ),
        )
        return ordered[0] if direction == 1 else ordered[-1]
    return primary


def compare_documents(
    left: Document,
    right: Document,
    sort: SortSpec,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> int:
    for field, direction in sort:
        result = compare_with_collation(
            sort_value(left, field, direction, dialect=dialect, collation=collation),
            sort_value(right, field, direction, dialect=dialect, collation=collation),
            dialect=dialect,
            collation=collation,
        )
        if result != 0:
            return result if direction == 1 else -result
    return 0


def sort_documents(
    documents: list[Document],
    sort: SortSpec | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Document]:
    if not sort:
        return documents

    # Precompute sort keys so comparisons do not repeatedly traverse the document.
    decorated = [
        (
            [
                sort_value(doc, field, direction, dialect=dialect, collation=collation)
                for field, direction in sort
            ],
            doc,
        )
        for doc in documents
    ]

    def _compare_decorated(left_tuple: tuple[list[Any], Document], right_tuple: tuple[list[Any], Document]) -> int:
        left_keys, _ = left_tuple
        right_keys, _ = right_tuple
        for index, (_field, direction) in enumerate(sort):
            result = compare_with_collation(
                left_keys[index],
                right_keys[index],
                dialect=dialect,
                collation=collation,
            )
            if result != 0:
                return result if direction == 1 else -result
        return 0

    decorated.sort(key=cmp_to_key(_compare_decorated))
    return [doc for _keys, doc in decorated]


def sort_documents_window(
    documents: list[Document] | tuple[Document, ...] | Any,
    sort: SortSpec | None,
    *,
    window: int | None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Document]:
    if not sort:
        result = list(documents)
        return result if window is None else result[:window]
    if window is None:
        return sort_documents(list(documents), sort, dialect=dialect, collation=collation)
    if window <= 0:
        return []
    comparator = cmp_to_key(
        lambda left, right: compare_documents(
            left,
            right,
            sort,
            dialect=dialect,
            collation=collation,
        )
    )
    return heapq.nsmallest(window, documents, key=comparator)


def sort_documents_limited(
    documents: list[Document] | tuple[Document, ...] | Any,
    sort: SortSpec | None,
    *,
    skip: int = 0,
    limit: int | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Document]:
    window = None if limit is None else skip + limit
    result = sort_documents_window(
        documents,
        sort,
        window=window,
        dialect=dialect,
        collation=collation,
    )
    if skip:
        result = result[skip:]
    if limit is not None:
        result = result[:limit]
    return result
