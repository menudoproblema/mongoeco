from collections.abc import Mapping, Sequence

from mongoeco.core.search import TEXT_SCORE_FIELD
from mongoeco.types import SortSpec


type HintSpec = str | SortSpec


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
