import math
from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.types import DBRef


DEFAULT_MAX_ARRAY_INDEX = 10_000
MAX_ARRAY_INDEX = DEFAULT_MAX_ARRAY_INDEX


def _parse_index(segment: str) -> int | None:
    if segment.isdigit():
        return int(segment)
    return None


def _make_container(next_path: str) -> dict[str, Any] | list[Any]:
    first_segment = next_path.split(".", 1)[0]
    return [] if _parse_index(first_segment) is not None else {}


def _ensure_array_index_within_limit(index: int) -> None:
    if index > MAX_ARRAY_INDEX:
        raise OperationFailure(f"Array index {index} exceeds the maximum supported index of {MAX_ARRAY_INDEX}")


def _path_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, DBRef):
        return value.as_document()
    if isinstance(value, dict):
        return value
    return None


def _same_value_for_update(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        if list(left.keys()) != list(right.keys()):
            return False
        return all(_same_value_for_update(left[key], right[key]) for key in left)
    if isinstance(left, list):
        if len(left) != len(right):
            return False
        return all(_same_value_for_update(left_item, right_item) for left_item, right_item in zip(left, right))
    if isinstance(left, float):
        return math.isnan(left) and math.isnan(right) if isinstance(right, float) else False
    return left == right


def get_max_array_index() -> int:
    return MAX_ARRAY_INDEX


def set_max_array_index(limit: int) -> None:
    global MAX_ARRAY_INDEX
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise ValueError("max array index must be a non-negative integer")
    MAX_ARRAY_INDEX = limit


def get_document_value(doc: dict[str, Any] | list[Any], path: str) -> tuple[bool, Any]:
    doc_mapping = _path_mapping(doc)
    if doc_mapping is not None and doc_mapping is not doc:
        return get_document_value(doc_mapping, path)

    if isinstance(doc, list):
        if "." not in path:
            index = _parse_index(path)
            if index is None or index >= len(doc):
                return False, None
            return True, doc[index]

        first, rest = path.split(".", 1)
        index = _parse_index(first)
        if index is None or index >= len(doc):
            return False, None
        nested = _path_mapping(doc[index])
        if nested is not None:
            return get_document_value(nested, rest)
        if not isinstance(doc[index], (dict, list)):
            return False, None
        return get_document_value(doc[index], rest)

    if "." not in path:
        if path not in doc:
            return False, None
        return True, doc[path]

    first, rest = path.split(".", 1)
    if first not in doc:
        return False, None
    nested = _path_mapping(doc[first])
    if nested is not None:
        return get_document_value(nested, rest)
    if not isinstance(doc[first], (dict, list)):
        return False, None
    return get_document_value(doc[first], rest)


def set_document_value(doc: dict[str, Any] | list[Any], path: str, value: Any) -> bool:
    if isinstance(doc, list):
        if "." not in path:
            index = _parse_index(path)
            if index is None:
                raise OperationFailure(f"Cannot create field '{path}' in array element")
            _ensure_array_index_within_limit(index)
            if index >= len(doc):
                doc.extend([None] * (index - len(doc) + 1))
            if not _same_value_for_update(doc[index], value):
                doc[index] = value
                return True
            return False

        first, rest = path.split(".", 1)
        index = _parse_index(first)
        if index is None:
            raise OperationFailure(f"Cannot create field '{rest}' in array element")
        _ensure_array_index_within_limit(index)
        if index >= len(doc):
            doc.extend([None] * (index - len(doc) + 1))
        if doc[index] is None:
            doc[index] = _make_container(rest)
        elif not isinstance(doc[index], (dict, list)):
            raise OperationFailure(f"Cannot create field '{rest}' in element {{{first}: {doc[index]!r}}}")
        return set_document_value(doc[index], rest, value)

    if "." not in path:
        if path not in doc or not _same_value_for_update(doc[path], value):
            doc[path] = value
            return True
        return False

    first, rest = path.split(".", 1)
    if first not in doc:
        doc[first] = {}
    elif not isinstance(doc[first], (dict, list)):
        raise OperationFailure(f"Cannot create field '{rest}' in element {{{first}: {doc[first]!r}}}")

    return set_document_value(doc[first], rest, value)


def delete_document_value(doc: dict[str, Any] | list[Any], path: str) -> bool:
    if isinstance(doc, list):
        if "." not in path:
            index = _parse_index(path)
            if index is None or index >= len(doc):
                return False
            if doc[index] is not None:
                doc[index] = None
                return True
            return False

        first, rest = path.split(".", 1)
        index = _parse_index(first)
        if index is None or index >= len(doc):
            return False
        if isinstance(doc[index], (dict, list)):
            return delete_document_value(doc[index], rest)
        return False

    if "." not in path:
        if path in doc:
            del doc[path]
            return True
        return False

    first, rest = path.split(".", 1)
    if first in doc and isinstance(doc[first], (dict, list)):
        return delete_document_value(doc[first], rest)
    return False
