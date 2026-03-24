from typing import Any


def canonical_document_id(value: Any) -> Any:
    try:
        hash(value)
        return value
    except TypeError:
        if isinstance(value, dict):
            return ("dict", tuple((key, canonical_document_id(item)) for key, item in value.items()))
        if isinstance(value, list):
            return ("list", tuple(canonical_document_id(item) for item in value))
        return ("repr", repr(value))
