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
    ObjectId,
    Regex,
    Timestamp,
    UndefinedType,
)


try:
    from bson.code import Code as BsonCode
    from bson.dbref import DBRef as DriverDBRef
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
except ImportError:  # pragma: no cover - optional SDK
    BsonCode = BsonMaxKey = BsonMinKey = None
    DriverDBRef = ()


def binary_parts(value: Any) -> tuple[int, bytes] | None:
    """Recognize bytes (subtype zero), local, SDK and owned Binary uniformly."""
    if not isinstance(value, bytes):
        return None
    return getattr(value, "subtype", 0), bytes(value)


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
        return isinstance(candidate, ObjectId)
    if alias == "bool":
        return isinstance(candidate, bool)
    if alias == "date":
        return isinstance(candidate, datetime.datetime)
    if alias == "timestamp":
        return isinstance(candidate, Timestamp)
    if alias == "null":
        return candidate is None
    if alias == "regex":
        return isinstance(candidate, (re.Pattern, Regex))
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
