from typing import Any


def _parse_index(segment: str) -> int | None:
    if segment.isdigit():
        return int(segment)
    return None


def _make_container(next_path: str) -> dict[str, Any] | list[Any]:
    first_segment = next_path.split(".", 1)[0]
    return [] if _parse_index(first_segment) is not None else {}


def set_document_value(doc: dict[str, Any] | list[Any], path: str, value: Any) -> bool:
    if isinstance(doc, list):
        if "." not in path:
            index = _parse_index(path)
            if index is None:
                return False
            if index >= len(doc):
                doc.extend([None] * (index - len(doc) + 1))
            if doc[index] != value:
                doc[index] = value
                return True
            return False

        first, rest = path.split(".", 1)
        index = _parse_index(first)
        if index is None:
            return False
        if index >= len(doc):
            doc.extend([None] * (index - len(doc) + 1))
        if doc[index] is None or not isinstance(doc[index], (dict, list)):
            doc[index] = _make_container(rest)
        return set_document_value(doc[index], rest, value)

    if "." not in path:
        if path not in doc or doc[path] != value:
            doc[path] = value
            return True
        return False

    first, rest = path.split(".", 1)
    if first not in doc or not isinstance(doc[first], (dict, list)):
        doc[first] = _make_container(rest)

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
