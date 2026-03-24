from typing import Any, TypeIs

from mongoeco.types import Document, Filter, Projection, Update


def is_document(value: Any) -> TypeIs[Document]:
    return isinstance(value, dict)


def is_filter(value: Any) -> TypeIs[Filter]:
    return isinstance(value, dict)


def is_projection(value: Any) -> TypeIs[Projection]:
    return isinstance(value, dict)


def is_update(value: Any) -> TypeIs[Update]:
    return isinstance(value, dict)
