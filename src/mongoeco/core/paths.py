from typing import Any

from mongoeco.core.bson_identity import same_bson_update_value
from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
)
from mongoeco.core.server_errors import server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import DBRef


DEFAULT_MAX_ARRAY_INDEX = 10_000
MAX_ARRAY_INDEX = DEFAULT_MAX_ARRAY_INDEX


def _parse_index(segment: str) -> int | None:
    if segment.isdigit():
        return int(segment)
    return None


def _ensure_array_index_within_limit(index: int) -> None:
    if index > MAX_ARRAY_INDEX:
        raise OperationFailure(
            f"Array index {index} exceeds the maximum supported index of {MAX_ARRAY_INDEX}"
        )


def _path_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, DBRef):
        return value.as_document()
    if isinstance(value, dict):
        return value
    return None


def get_max_array_index() -> int:
    return MAX_ARRAY_INDEX


def set_max_array_index(limit: int) -> None:
    global MAX_ARRAY_INDEX
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise ValueError("max array index must be a non-negative integer")
    MAX_ARRAY_INDEX = limit


def get_document_value(  # noqa: PLR0912 - BSON path traversal cases
    doc: dict[str, Any] | list[Any] | RuntimeDocumentState,
    path: str,
) -> tuple[bool, Any]:
    if isinstance(doc, RuntimeDocumentState):
        return doc.resolve(path)
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
    first_value = doc[first]
    nested = _path_mapping(first_value)
    if nested is not None:
        return get_document_value(nested, rest)
    if not isinstance(first_value, (dict, list)):
        return False, None
    return get_document_value(first_value, rest)


def set_document_value(doc: dict[str, Any] | list[Any], path: str, value: Any) -> bool:
    if isinstance(doc, list):
        if "." not in path:
            index = _parse_index(path)
            if index is None:
                raise OperationFailure(
                    f"Cannot create field '{path}' in array element", code=28
                )
            _ensure_array_index_within_limit(index)
            if index >= len(doc):
                doc.extend([None] * (index - len(doc) + 1))
            if not same_bson_update_value(doc[index], value):
                doc[index] = value
                return True
            return False

        first, rest = path.split(".", 1)
        index = _parse_index(first)
        if index is None:
            raise OperationFailure(
                f"Cannot create field '{rest}' in array element", code=28
            )
        _ensure_array_index_within_limit(index)
        if index >= len(doc):
            doc.extend([None] * (index - len(doc) + 1))
            doc[index] = {}
        elif not isinstance(doc[index], (dict, list)):
            raise OperationFailure(
                f"Cannot create field '{rest}' in element {{{first}: {doc[index]!r}}}",
                code=28,
            )
        return set_document_value(doc[index], rest, value)

    if "." not in path:
        if path not in doc or not same_bson_update_value(doc[path], value):
            doc[path] = value
            return True
        return False

    first, rest = path.split(".", 1)
    if first not in doc:
        doc[first] = {}
    elif not isinstance(doc[first], (dict, list)):
        raise OperationFailure(
            f"Cannot create field '{rest}' in element {{{first}: {doc[first]!r}}}",
            code=28,
        )

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


def validate_projection_paths(paths: set[str]) -> None:
    ordered_paths = sorted(paths)
    for index, left in enumerate(ordered_paths):
        for right in ordered_paths[index + 1 :]:
            if right.startswith(left + ".") or left.startswith(right + "."):
                message = f"Path collision in projection between {left!r} and {right!r}"
                raise server_error(
                    message,
                    code=31249,
                )
