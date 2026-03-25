from functools import cmp_to_key
from typing import Any

from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.types import Document, SortSpec


def sort_value(document: Document, field: str, direction: int) -> Any:
    values = QueryEngine.extract_values(document, field)
    if not values:
        return None

    primary = values[0]
    if isinstance(primary, list):
        if not primary:
            return []
        members = values[1:] or primary
        ordered = sorted(members, key=cmp_to_key(BSONComparator.compare))
        return ordered[0] if direction == 1 else ordered[-1]
    if len(values) > 1:
        ordered = sorted(values, key=cmp_to_key(BSONComparator.compare))
        return ordered[0] if direction == 1 else ordered[-1]
    return primary


def compare_documents(left: Document, right: Document, sort: SortSpec) -> int:
    for field, direction in sort:
        result = BSONComparator.compare(
            sort_value(left, field, direction),
            sort_value(right, field, direction),
        )
        if result != 0:
            return result if direction == 1 else -result
    return 0


def sort_documents(documents: list[Document], sort: SortSpec | None) -> list[Document]:
    if not sort:
        return documents
    return sorted(documents, key=cmp_to_key(lambda left, right: compare_documents(left, right, sort)))
