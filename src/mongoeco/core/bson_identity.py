"""Physical BSON identity for writes; independent of MQL and collation."""

import struct

from typing import Any

from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    bson_numeric_alias,
    wrap_bson_numeric,
)
from mongoeco.core.bson_values import binary_parts, code_parts, document_mapping


def same_bson_update_value(left: Any, right: Any) -> bool:  # noqa: PLR0911 - physical BSON representations
    """BSON identity for assignment, distinct from query/arithmetic equality."""
    left_binary, right_binary = binary_parts(left), binary_parts(right)
    if left_binary is not None or right_binary is not None:
        return left_binary is not None and left_binary == right_binary
    left_code, right_code = code_parts(left), code_parts(right)
    if left_code is not None or right_code is not None:
        if left_code is None or right_code is None or left_code[0] != right_code[0]:
            return False
        if left_code[1] is None or right_code[1] is None:
            return left_code[1] is right_code[1]
        return same_bson_update_value(left_code[1], right_code[1])
    left_kind, right_kind = bson_numeric_alias(left), bson_numeric_alias(right)
    if left_kind is not None or right_kind is not None:
        numeric_left, numeric_right = wrap_bson_numeric(left), wrap_bson_numeric(right)
        if left_kind != right_kind or numeric_left is None or numeric_right is None:
            return False
        if isinstance(numeric_left, BsonDouble) and isinstance(
            numeric_right, BsonDouble
        ):
            identical = struct.pack("!d", numeric_left.value) == struct.pack(
                "!d", numeric_right.value
            )
        elif isinstance(numeric_left, BsonDecimal128) and isinstance(
            numeric_right, BsonDecimal128
        ):
            identical = numeric_left.value.as_tuple() == numeric_right.value.as_tuple()
        else:
            identical = numeric_left.value == numeric_right.value
        return identical
    left_document, right_document = document_mapping(left), document_mapping(right)
    if left_document is not None or right_document is not None:
        return (
            left_document is not None
            and right_document is not None
            and (
                list(left_document) == list(right_document)
                and all(
                    same_bson_update_value(left_document[key], right_document[key])
                    for key in left_document
                )
            )
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            same_bson_update_value(left_item, right_item)
            for left_item, right_item in zip(left, right, strict=True)
        )
    return type(left) is type(right) and left == right
