"""BSON value views independent of matching, storage and SDK ownership."""

import datetime
import decimal
import re
import uuid

from collections.abc import Mapping
from typing import Any

from mongoeco.core.bson_scalars import bson_numeric_alias
from mongoeco.types import (
    Binary,
    DBRef,
    Decimal128,
    Regex,
    Timestamp,
    UndefinedType,
    is_object_id_like,
)


try:
    from bson.code import Code as BsonCode
    from bson.dbref import DBRef as DriverDBRef
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
    from bson.regex import Regex as DriverRegex
    from bson.timestamp import Timestamp as DriverTimestamp
except ImportError:  # pragma: no cover - optional SDK
    BsonCode = BsonMaxKey = BsonMinKey = None
    DriverDBRef = ()
    DriverRegex = ()
    DriverTimestamp = Timestamp


_OLD_BINARY_SUBTYPE = 2


def binary_parts(value: Any) -> tuple[int, bytes] | None:
    """View bytes/Binary and the canonical standard UUID storage representation."""
    if isinstance(value, uuid.UUID):
        return 4, value.bytes
    if not isinstance(value, bytes):
        return None
    return getattr(value, "subtype", 0), bytes(value)


def binary_order_key(value: Any) -> tuple[int, int, bytes] | None:
    """Order the stored BSON payload, including old subtype 2's inner length."""
    parts = binary_parts(value)
    if parts is None:
        return None
    subtype, payload = parts
    embedded_length_bytes = 4 if subtype == _OLD_BINARY_SUBTYPE else 0
    return len(payload) + embedded_length_bytes, subtype, payload


def document_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, DBRef):
        return value.as_document()
    if isinstance(value, DriverDBRef):
        return value.as_doc()
    if isinstance(value, Mapping):
        return value
    return None


def matches_bson_type(candidate: Any, alias: str) -> bool:  # noqa: PLR0911, PLR0912 - closed BSON alias dispatch
    numeric_alias = bson_numeric_alias(candidate)
    if numeric_alias is not None:
        if alias == "number":
            return True
        return numeric_alias == alias
    if alias == "double":
        return isinstance(candidate, float) and not isinstance(candidate, bool)
    if alias == "decimal":
        return isinstance(candidate, (decimal.Decimal, Decimal128))
    if alias in {"int", "long"}:
        return isinstance(candidate, int) and not isinstance(candidate, bool)
    if alias == "string":
        return isinstance(candidate, str) and not (
            BsonCode is not None and isinstance(candidate, BsonCode)
        )
    if alias == "object":
        return document_mapping(candidate) is not None
    if alias == "array":
        return isinstance(candidate, list)
    if alias == "binData":
        return isinstance(candidate, (bytes, Binary, uuid.UUID))
    if alias == "objectId":
        return is_object_id_like(candidate)
    if alias == "bool":
        return isinstance(candidate, bool)
    if alias == "date":
        return isinstance(candidate, datetime.datetime)
    if alias == "timestamp":
        return isinstance(candidate, (Timestamp, DriverTimestamp))
    if alias == "null":
        return candidate is None
    if alias == "regex":
        return regex_parts(candidate) is not None
    if alias == "javascript":
        return (
            BsonCode is not None
            and isinstance(candidate, BsonCode)
            and candidate.scope is None
        )
    if alias == "javascriptWithScope":
        return (
            BsonCode is not None
            and isinstance(candidate, BsonCode)
            and candidate.scope is not None
        )
    if alias == "minKey":
        return BsonMinKey is not None and isinstance(candidate, BsonMinKey)
    if alias == "maxKey":
        return BsonMaxKey is not None and isinstance(candidate, BsonMaxKey)
    if alias == "undefined":
        return isinstance(candidate, UndefinedType)
    return False


def code_parts(value: Any) -> tuple[str, Mapping[str, Any] | None] | None:
    if BsonCode is None or not isinstance(value, BsonCode):
        return None
    return str(value), value.scope


def regex_options(flags: int | str) -> str:
    """Render the six BSON regex option bits without dropping locale/unicode."""
    if isinstance(flags, str):
        return flags
    return "".join(
        flag
        for flag, mask in (
            ("i", re.IGNORECASE),
            ("l", re.LOCALE),
            ("m", re.MULTILINE),
            ("s", re.DOTALL),
            ("u", re.UNICODE),
            ("x", re.VERBOSE),
        )
        if flags & mask
    )


def regex_parts(value: Any) -> tuple[str | bytes, str] | None:
    """Pattern and canonical BSON option letters across SDK/local representations."""
    if isinstance(value, Regex):
        return value.pattern, value.flags
    if not isinstance(value, re.Pattern) and not isinstance(value, DriverRegex):
        return None
    return value.pattern, regex_options(value.flags)
