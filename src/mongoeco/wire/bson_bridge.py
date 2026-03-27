from __future__ import annotations

import decimal
import re
import uuid
from typing import Any

from bson import Decimal128
from bson.binary import Binary, STANDARD
from bson.int64 import Int64
from bson.objectid import ObjectId as BsonObjectId
from bson.regex import Regex

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.types import ObjectId, UndefinedType


def decode_wire_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: decode_wire_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [decode_wire_value(item) for item in value]
    if isinstance(value, BsonObjectId):
        return ObjectId(str(value))
    if isinstance(value, Decimal128):
        return decimal.Decimal(str(value.to_decimal()))
    if isinstance(value, Int64):
        return int(value)
    if isinstance(value, Binary):
        if value.subtype == STANDARD and len(value) == 16:
            try:
                return value.as_uuid(uuid_representation=STANDARD)
            except Exception:
                return bytes(value)
        return bytes(value)
    if isinstance(value, Regex):
        return re.compile(value.pattern, value.flags)
    return value


def encode_wire_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: encode_wire_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode_wire_value(item) for item in value]
    if isinstance(value, ObjectId):
        return BsonObjectId(str(value))
    if isinstance(value, BsonInt64):
        return Int64(value.value)
    if isinstance(value, BsonInt32):
        return value.value
    if isinstance(value, BsonDouble):
        return value.value
    if isinstance(value, BsonDecimal128):
        return Decimal128(value.value)
    if isinstance(value, decimal.Decimal):
        return Decimal128(value)
    if isinstance(value, uuid.UUID):
        return Binary.from_uuid(value, uuid_representation=STANDARD)
    if isinstance(value, bytes):
        return Binary(value)
    if isinstance(value, UndefinedType):
        return None
    return value
