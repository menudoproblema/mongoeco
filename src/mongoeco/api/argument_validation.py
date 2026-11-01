from __future__ import annotations

from collections.abc import Mapping, Sequence, Set as AbstractSet
from typing import Any

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.search import TEXT_SCORE_FIELD
from mongoeco.types import Projection, SortSpec


type HintSpec = str | SortSpec
type ProjectionInput = Mapping[str, Any] | Sequence[str] | AbstractSet[str]


def projection_document(projection: object | None) -> Mapping[str, Any] | None:
    if projection is None:
        return None
    if isinstance(projection, Mapping):
        document = projection
    elif isinstance(projection, Sequence | AbstractSet):
        if not all(isinstance(field, str) for field in projection):
            message = "projection must be a list of key names, each an instance of str"
            raise TypeError(message)
        document = dict.fromkeys(projection, 1)
    else:
        message = "projection must be a mapping or list of key names"
        raise TypeError(message)
    return document


def normalize_projection_spec(projection: object | None) -> Projection | None:
    document = projection_document(projection)
    return None if document is None else DocumentCodec.to_internal(dict(document))


def normalize_sort_spec(sort: object | None) -> SortSpec | None:
    if sort is None:
        return None
    if isinstance(sort, Mapping):
        normalized = []
        for field, direction in sort.items():
            if (
                isinstance(field, str)
                and isinstance(direction, Mapping)
                and dict(direction) == {"$meta": "textScore"}
            ):
                normalized.append((TEXT_SCORE_FIELD, -1))
                continue
            normalized.append((field, direction))
        validate_sort_spec(normalized)
        return normalized
    if (
        isinstance(sort, tuple)
        and len(sort) == 2
        and isinstance(sort[0], str)
        and sort[1] in (1, -1)
        and not isinstance(sort[1], bool)
    ):
        normalized = [sort]
        validate_sort_spec(normalized)
        return normalized
    if isinstance(sort, Sequence) and not isinstance(sort, (str, bytes, bytearray, list)):
        normalized = list(sort)
        validate_sort_spec(normalized)
        return normalized
    validate_sort_spec(sort)
    return sort


def validate_sort_spec(sort: object) -> None:
    if not isinstance(sort, list):
        raise TypeError("sort must be a list of (field, direction) tuples")
    for item in sort:
        if not isinstance(item, tuple) or len(item) != 2:
            raise TypeError("sort must be a list of (field, direction) tuples")
        field, direction = item
        if not isinstance(field, str):
            raise TypeError("sort fields must be strings")
        if direction not in (1, -1) or isinstance(direction, bool):
            raise ValueError("sort directions must be 1 or -1")


def validate_hint_spec(hint: HintSpec) -> None:
    if isinstance(hint, str):
        if not hint:
            raise ValueError("hint string must not be empty")
        return
    normalize_sort_spec(hint)


def validate_batch_size(batch_size: int) -> None:
    if not isinstance(batch_size, int) or isinstance(batch_size, bool):
        raise TypeError("batch_size must be an integer")
    if batch_size < 0:
        raise ValueError("batch_size must be >= 0")


def validate_max_time_ms(max_time_ms: int) -> None:
    if not isinstance(max_time_ms, int) or isinstance(max_time_ms, bool):
        raise TypeError("max_time_ms must be an integer")
    if max_time_ms < 0:
        raise ValueError("max_time_ms must be >= 0")


def update_document(update):
    """Validate client update shape without normalizing its BSON values."""
    if not isinstance(update, Mapping | list):
        message = "update_spec must be a dict or list"
        raise TypeError(message)
    if not update:
        message = "update_spec must not be empty"
        raise ValueError(message)
    if not isinstance(update, list) and not next(iter(update)).startswith("$"):
        message = "update_spec must begin with an update operator"
        raise ValueError(message)
    return update


def replacement_document(replacement):
    """Only the first key decides the driver's replacement/operator grammar."""
    if not isinstance(replacement, Mapping):
        message = "replacement must be a dict"
        raise TypeError(message)
    if replacement and next(iter(replacement)).startswith("$"):
        message = "replacement must not begin with an update operator"
        raise ValueError(message)
    return replacement


def filter_document(filter_spec):
    """Require the client write filter, where None is not an empty query."""
    if not isinstance(filter_spec, Mapping):
        message = "filter_spec must be a document"
        raise TypeError(message)
    return filter_spec
