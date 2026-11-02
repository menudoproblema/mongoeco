"""BSON framing sizes over the shared value views, without SDK encoding."""

from collections.abc import Mapping
from typing import Any

from mongoeco.core.bson_scalars import bson_numeric_alias
from mongoeco.core.bson_values import (
    binary_order_key,
    code_parts,
    document_mapping,
    matches_bson_type,
    regex_parts,
)
from mongoeco.types import UndefinedType


_NUMERIC_BYTES = {"int": 4, "long": 8, "double": 8, "decimal": 16}


def bson_cstring_size(value: str | bytes) -> int:
    encoded = value.encode("utf-8") if isinstance(value, str) else value
    if b"\x00" in encoded:
        message = "$bsonSize cannot encode strings containing NUL bytes"
        raise ValueError(message)
    return len(encoded) + 1


def bson_string_size(value: str) -> int:
    return 5 + len(value.encode("utf-8"))


def bson_value_size(value: Any) -> int:  # noqa: PLR0911, PLR0912 - BSON physical families
    value_type = type(value)
    if value_type is str:
        return bson_string_size(value)
    if value_type is dict:
        return bson_document_size(value)
    if value_type is list:
        return _bson_array_size(value)
    if value_type is bool:
        return 1
    if value is None:
        return 0
    if value_type is float:
        return _NUMERIC_BYTES["double"]
    numeric = bson_numeric_alias(value)
    if numeric is not None:
        return _NUMERIC_BYTES[numeric]
    code = code_parts(value)
    if code is not None:
        source, scope = code
        size = bson_string_size(source)
        return size if scope is None else 4 + size + bson_document_size(scope)
    if isinstance(value, str):
        return bson_string_size(value)
    document = document_mapping(value)
    if document is not None:
        return bson_document_size(document)
    if isinstance(value, list):
        return _bson_array_size(value)
    binary = binary_order_key(value)
    if binary is not None:
        return 5 + binary[0]
    if isinstance(value, bytearray):
        # Legacy size adapter; admission of bytearray remains a BSON boundary policy.
        return 5 + len(value)
    regex = regex_parts(value)
    if regex is not None:
        return sum(bson_cstring_size(part) for part in regex)
    if isinstance(value, bool):
        return 1
    if value is None or isinstance(value, UndefinedType):
        return 0
    if matches_bson_type(value, "objectId"):
        return 12
    if matches_bson_type(value, "date") or matches_bson_type(value, "timestamp"):
        return 8
    if matches_bson_type(value, "minKey") or matches_bson_type(value, "maxKey"):
        return 0
    message = f"$bsonSize cannot encode value of type {type(value).__name__}"
    raise TypeError(message)


def _bson_array_size(value: list[Any]) -> int:
    return 5 + sum(bson_element_size(str(i), item) for i, item in enumerate(value))


def bson_element_size(key: str, value: Any) -> int:
    return 1 + bson_cstring_size(key) + bson_value_size(value)


def bson_document_size(document: Mapping[str, Any]) -> int:
    return 5 + sum(bson_element_size(key, value) for key, value in document.items())
