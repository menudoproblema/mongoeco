from __future__ import annotations

import heapq
import math

from functools import cmp_to_key
from typing import TYPE_CHECKING, Any, Iterable

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core._filtering_support import query_field_values
from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.bson_ordering import bson_type_rank
from mongoeco.core.bson_values import binary_order_key, document_mapping
from mongoeco.core.collation import CollationSpec, compare_with_collation
from mongoeco.core.operation_limits import enforce_deadline
from mongoeco.core.paths import array_paths_for_field
from mongoeco.core.server_errors import server_error
from mongoeco.core.work_control import (
    DEADLINE_CHECK_INTERVAL,
    DeadlineCheckpoint,
    iter_with_deadline,
)
from mongoeco.types import UNDEFINED, UndefinedType


if TYPE_CHECKING:
    from mongoeco.types import Document, SortSpec

_EMPTY_ARRAY_SORT_KEY = UNDEFINED


def natural_scan_direction(sort, hint=None):
    def direction(spec):
        if spec is None or isinstance(spec, str):
            return None
        pairs = list(spec.items()) if isinstance(spec, dict) else list(spec)
        natural = [value for key, value in pairs if key == "$natural"]
        if not natural:
            return None
        if len(pairs) != 1 or isinstance(natural[0], bool) or natural[0] not in (1, -1):
            message = "$natural requires a single direction of 1 or -1"
            raise server_error(message, code=2)
        return natural[0]

    sort_direction, hint_direction = direction(sort), direction(hint)
    if (
        sort_direction is not None
        and hint is not None
        and hint_direction != sort_direction
    ):
        message = "$natural sort cannot be combined with a conflicting hint"
        raise server_error(message, code=2)
    return sort_direction if sort_direction is not None else hint_direction


def _document_sort_keys(
    document: Document,
    sort: SortSpec,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Any]:
    for field, _ in sort:
        if "." in field and any(part.isdigit() for part in field.split(".")):
            _validate_numeric_sort_path(document, field.split("."))
    paths = (
        [array_paths_for_field(document, field) for field, _ in sort]
        if len(sort) > 1
        else []
    )
    for position, left in enumerate(paths):
        for right in paths[position + 1 :]:
            if any(
                a != b and not a.startswith(b + ".") and not b.startswith(a + ".")
                for a in left
                for b in right
            ):
                message = "cannot sort with keys that are parallel arrays"
                raise server_error(message, code=2)
    return [
        sort_value(document, field, direction, dialect=dialect, collation=collation)
        for field, direction in sort
    ]


def _validate_numeric_sort_path(value, parts):
    if not parts:
        return
    part, *rest = parts
    mapping = document_mapping(value)
    if mapping is not None:
        if part in mapping:
            _validate_numeric_sort_path(mapping[part], rest)
    elif isinstance(value, list):
        if part.isdigit():
            if any(
                (mapping := document_mapping(item)) is not None and part in mapping
                for item in value
            ):
                message = "Ambiguous numeric field name in array sort path"
                raise server_error(message, code=16746)
            index = int(part)
            if index < len(value):
                _validate_numeric_sort_path(value[index], rest)
        else:
            for item in value:
                if document_mapping(item) is not None:
                    _validate_numeric_sort_path(item, parts)


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
    if isinstance(left, UndefinedType) or isinstance(right, UndefinedType):
        if isinstance(left, UndefinedType) and isinstance(right, UndefinedType):
            return 0
        other = right if isinstance(left, UndefinedType) else left
        result = 1 if bson_type_rank(other, dialect.bson_type_order) < 1 else -1
        return result if isinstance(left, UndefinedType) else -result
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
    if left_type is bytes:
        left_key, right_key = binary_order_key(left), binary_order_key(right)
        return (left_key > right_key) - (left_key < right_key)
    if left_type is int or left_type is bool or left_type is str:
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
                return _EMPTY_ARRAY_SORT_KEY
            return _extreme_value(
                primary,
                prefer_max=direction == -1,
                dialect=dialect,
                collation=collation,
            )
        return primary

    values = []
    for value, _traverse in query_field_values(
        document, field, numeric_array_fields=False
    ):
        if value is MISSING:
            values.append(None)
        elif isinstance(value, list):
            values.extend(value or [_EMPTY_ARRAY_SORT_KEY])
        else:
            values.append(value)
    return _extreme_value(
        values, prefer_max=direction == -1, dialect=dialect, collation=collation
    )


def sort_array_items(values, sort, *, dialect=MONGODB_DIALECT_70, collation=None):
    """SortArray reads document fields without expanding arrays or indexing them."""
    decorated = []
    for value in values:
        keys = []
        for field, _direction in sort:
            current = value
            for part in field.split("."):
                mapping = document_mapping(current)
                current = None if mapping is None else mapping.get(part)
            keys.append(current)
        decorated.append((keys, value))
    decorated.sort(
        key=cmp_to_key(
            lambda left, right: _compare_sort_keys(
                left[0], right[0], sort, dialect=dialect, collation=collation
            )
        )
    )
    return [value for _, value in decorated]


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
    deadline: float | None = None,
) -> list[Document]:
    if not sort:
        return documents

    # Precompute sort keys so comparisons do not repeatedly traverse the document.
    decorated = [
        (
            _document_sort_keys(doc, sort, dialect=dialect, collation=collation),
            doc,
        )
        for doc in iter_with_deadline(documents, deadline)
    ]

    # Exact Python integers already use native ordering in the comparator.
    # Check that eligibility once rather than dispatching it per comparison.
    # Mixed BSON types, custom dialects/collations and deadlines keep that path.
    if (
        len(sort) == 1
        and dialect is MONGODB_DIALECT_70
        and collation is None
        and deadline is None
        and all(type(keys[0]) is int for keys, _document in decorated)
    ):
        decorated.sort(key=lambda item: item[0][0], reverse=sort[0][1] != 1)
        return [document for _keys, document in decorated]

    def _compare_decorated(
        left_tuple: tuple[list[Any], Document], right_tuple: tuple[list[Any], Document]
    ) -> int:
        left_keys, _ = left_tuple
        right_keys, _ = right_tuple
        return _compare_sort_keys(
            left_keys,
            right_keys,
            sort,
            dialect=dialect,
            collation=collation,
        )

    if deadline is None:
        comparator = _compare_decorated
    else:
        comparison_iterations = 0

        def _compare_decorated_with_deadline(
            left_tuple: tuple[list[Any], Document],
            right_tuple: tuple[list[Any], Document],
        ) -> int:
            nonlocal comparison_iterations
            if comparison_iterations % DEADLINE_CHECK_INTERVAL == 0:
                enforce_deadline(deadline)
            comparison_iterations += 1
            return _compare_decorated(left_tuple, right_tuple)

        comparator = _compare_decorated_with_deadline

    decorated.sort(key=cmp_to_key(comparator))
    return [doc for _keys, doc in iter_with_deadline(decorated, deadline)]


def sort_documents_window(  # noqa: PLR0913
    documents: Iterable[Document],
    sort: SortSpec | None,
    *,
    window: int | None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    deadline: float | None = None,
) -> list[Document]:
    if not sort:
        result = list(iter_with_deadline(documents, deadline))
        return result if window is None else result[:window]
    if window is None:
        return sort_documents(
            list(iter_with_deadline(documents, deadline)),
            sort,
            dialect=dialect,
            collation=collation,
            deadline=deadline,
        )
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
        return (left_index > right_index) - (left_index < right_index)

    comparison_checkpoint = DeadlineCheckpoint(deadline)

    class _HeapItem:
        __slots__ = ("keys", "doc", "index")

        def __init__(self, keys: list[Any], doc: Document, index: int):
            self.keys = keys
            self.doc = doc
            self.index = index

        def __lt__(self, other: "_HeapItem") -> bool:
            if deadline is not None:
                comparison_checkpoint()
            return (
                _compare_decorated_values(
                    self.keys,
                    other.keys,
                    self.index,
                    other.index,
                    sort,
                    dialect=dialect,
                    collation=collation,
                )
                > 0
            )

    heap: list[_HeapItem] = []
    for index, doc in enumerate(iter_with_deadline(documents, deadline)):
        item = _HeapItem(
            _document_sort_keys(doc, sort, dialect=dialect, collation=collation),
            doc,
            index,
        )
        if len(heap) < window:
            heapq.heappush(heap, item)
            continue
        if deadline is not None:
            comparison_checkpoint()
        if (
            _compare_decorated_values(
                item.keys,
                heap[0].keys,
                item.index,
                heap[0].index,
                sort,
                dialect=dialect,
                collation=collation,
            )
            < 0
        ):
            heapq.heapreplace(heap, item)

    def _compare_heap_items(left: _HeapItem, right: _HeapItem) -> int:
        if deadline is not None:
            comparison_checkpoint()
        return _compare_decorated_values(
            left.keys,
            right.keys,
            left.index,
            right.index,
            sort,
            dialect=dialect,
            collation=collation,
        )

    ordered = sorted(
        heap,
        key=cmp_to_key(_compare_heap_items),
    )
    return [item.doc for item in iter_with_deadline(ordered, deadline)]


def sort_documents_limited(
    documents: list[Document] | tuple[Document, ...] | Any,
    sort: SortSpec | None,
    *,
    skip: int = 0,
    limit: int | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    deadline: float | None = None,
) -> list[Document]:
    window = None if limit is None else skip + limit
    result = sort_documents_window(
        documents,
        sort,
        window=window,
        dialect=dialect,
        collation=collation,
        deadline=deadline,
    )
    if skip:
        result = result[skip:]
    if limit is not None:
        result = result[:limit]
    return result
