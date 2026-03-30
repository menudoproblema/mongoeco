from functools import cmp_to_key
import heapq
import math
from typing import Any, Iterable

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec, compare_with_collation
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.types import Document, SortSpec


def _document_sort_keys(
    document: Document,
    sort: SortSpec,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Any]:
    return [
        sort_value(document, field, direction, dialect=dialect, collation=collation)
        for field, direction in sort
    ]


def _compare_sort_keys(
    left_keys: list[Any],
    right_keys: list[Any],
    sort: SortSpec,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> int:
    if len(sort) == 1:
        result = _compare_sort_values(
            left_keys[0],
            right_keys[0],
            dialect=dialect,
            collation=collation,
        )
        direction = sort[0][1]
        if result != 0:
            return result if direction == 1 else -result
        return 0
    for index, (_field, direction) in enumerate(sort):
        result = _compare_sort_values(
            left_keys[index],
            right_keys[index],
            dialect=dialect,
            collation=collation,
        )
        if result != 0:
            return result if direction == 1 else -result
    return 0


def _compare_sort_values(
    left: Any,
    right: Any,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> int:
    if collation is None and dialect is MONGODB_DIALECT_70:
        native_result = _compare_native_sort_scalars(left, right)
        if native_result is not None:
            return native_result
    return compare_with_collation(
        left,
        right,
        dialect=dialect,
        collation=collation,
    )


def _compare_native_sort_scalars(left: Any, right: Any) -> int | None:
    if left is right:
        return 0
    left_type = type(left)
    if left_type is not type(right):
        return None
    if left_type is int or left_type is bool or left_type is str or left_type is bytes:
        if left < right:
            return -1
        if left > right:
            return 1
        return 0
    if left_type is float:
        if math.isnan(left):
            return 0 if math.isnan(right) else -1
        if math.isnan(right):
            return 1
        if left < right:
            return -1
        if left > right:
            return 1
        return 0
    return None


def _extreme_value(
    values: Iterable[Any],
    *,
    prefer_max: bool,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> Any:
    iterator = iter(values)
    try:
        best = next(iterator)
    except StopIteration:
        return None
    for candidate in iterator:
        result = _compare_sort_values(
            candidate,
            best,
            dialect=dialect,
            collation=collation,
        )
        if (prefer_max and result > 0) or (not prefer_max and result < 0):
            best = candidate
    return best


def sort_value(
    document: Document,
    field: str,
    direction: int,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> Any:
    if "." not in field:
        if field not in document:
            return None
        primary = document[field]
        if isinstance(primary, list):
            if not primary:
                return []
            return _extreme_value(
                primary,
                prefer_max=direction == -1,
                dialect=dialect,
                collation=collation,
            )
        return primary

    values = QueryEngine.extract_values(document, field)
    if not values:
        return None

    primary = values[0]
    if isinstance(primary, list):
        if not primary:
            return []
        members = values[1:] or primary
        return _extreme_value(
            members,
            prefer_max=direction == -1,
            dialect=dialect,
            collation=collation,
        )
    if len(values) > 1:
        return _extreme_value(
            values,
            prefer_max=direction == -1,
            dialect=dialect,
            collation=collation,
        )
    return primary


def compare_documents(
    left: Document,
    right: Document,
    sort: SortSpec,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> int:
    return _compare_sort_keys(
        _document_sort_keys(left, sort, dialect=dialect, collation=collation),
        _document_sort_keys(right, sort, dialect=dialect, collation=collation),
        sort,
        dialect=dialect,
        collation=collation,
    )


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
            _document_sort_keys(doc, sort, dialect=dialect, collation=collation),
            doc,
        )
        for doc in documents
    ]

    def _compare_decorated(left_tuple: tuple[list[Any], Document], right_tuple: tuple[list[Any], Document]) -> int:
        left_keys, _ = left_tuple
        right_keys, _ = right_tuple
        return _compare_sort_keys(
            left_keys,
            right_keys,
            sort,
            dialect=dialect,
            collation=collation,
        )

    decorated.sort(key=cmp_to_key(_compare_decorated))
    return [doc for _keys, doc in decorated]


def sort_documents_window(
    documents: Iterable[Document],
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

    def _compare_decorated_values(
        left_keys: list[Any],
        right_keys: list[Any],
        left_index: int,
        right_index: int,
        sort_spec: SortSpec,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> int:
        result = _compare_sort_keys(
            left_keys,
            right_keys,
            sort_spec,
            dialect=dialect,
            collation=collation,
        )
        if result != 0:
            return result
        if left_index < right_index:
            return -1
        if left_index > right_index:
            return 1
        return 0

    class _HeapItem:
        __slots__ = ("keys", "doc", "index")

        def __init__(self, keys: list[Any], doc: Document, index: int):
            self.keys = keys
            self.doc = doc
            self.index = index

        def __lt__(self, other: "_HeapItem") -> bool:
            return _compare_decorated_values(
                self.keys,
                other.keys,
                self.index,
                other.index,
                sort,
                dialect=dialect,
                collation=collation,
            ) > 0

    heap: list[_HeapItem] = []
    for index, doc in enumerate(documents):
        item = _HeapItem(
            _document_sort_keys(doc, sort, dialect=dialect, collation=collation),
            doc,
            index,
        )
        if len(heap) < window:
            heapq.heappush(heap, item)
            continue
        if _compare_decorated_values(
            item.keys,
            heap[0].keys,
            item.index,
            heap[0].index,
            sort,
            dialect=dialect,
            collation=collation,
        ) < 0:
            heapq.heapreplace(heap, item)

    ordered = sorted(
        heap,
        key=cmp_to_key(
            lambda left, right: _compare_decorated_values(
                left.keys,
                right.keys,
                left.index,
                right.index,
                sort,
                dialect=dialect,
                collation=collation,
            )
        ),
    )
    return [item.doc for item in ordered]


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
