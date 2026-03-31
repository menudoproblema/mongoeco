from __future__ import annotations

import decimal
import re
import uuid
from typing import Any

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, DBRef, Decimal128, ObjectId, Regex, SON, Timestamp, UndefinedType

try:  # pragma: no cover - optional dependency
    from bson import Decimal128 as BsonDecimal128Public
    from bson.binary import Binary as BsonBinary, STANDARD
    from bson.code import Code as BsonCode
    from bson.dbref import DBRef as BsonDBRef
    from bson.int64 import Int64
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
    from bson.objectid import ObjectId as BsonObjectId
    from bson.regex import Regex as BsonRegex
    from bson.son import SON as BsonSON
    from bson.timestamp import Timestamp as BsonTimestamp
except Exception:  # pragma: no cover - bson is optional
    BsonDecimal128Public = type("_MissingBsonDecimal128Public", (), {})
    BsonBinary = type("_MissingBsonBinary", (bytes,), {})
    STANDARD = 4
    BsonCode = type("_MissingBsonCode", (str,), {"scope": None})
    BsonDBRef = type("_MissingBsonDBRef", (), {})
    Int64 = type("_MissingInt64", (), {})
    BsonMaxKey = type("_MissingBsonMaxKey", (), {})
    BsonMinKey = type("_MissingBsonMinKey", (), {})
    BsonObjectId = type("_MissingBsonObjectId", (), {})
    BsonRegex = type("_MissingBsonRegex", (), {})
    BsonSON = type("_MissingBsonSON", (dict,), {})
    BsonTimestamp = type("_MissingBsonTimestamp", (), {})
    _HAS_BSON = False
else:  # pragma: no cover - exercised when bson is installed
    _HAS_BSON = True


def _require_bson() -> None:
    if not _HAS_BSON:  # pragma: no cover - guarded by callers
        raise OperationFailure("wire BSON bridge requires the optional 'pymongo'/'bson' dependency")


def decode_wire_value(value: Any) -> Any:
    _require_bson()
    if isinstance(value, BsonSON):
        return SON((key, decode_wire_value(item)) for key, item in value.items())
    if isinstance(value, dict):
        return {key: decode_wire_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [decode_wire_value(item) for item in value]
    if isinstance(value, BsonObjectId):
        return ObjectId(str(value))
    if isinstance(value, BsonMinKey | BsonMaxKey):
        return value
    if isinstance(value, BsonDecimal128Public):
        return Decimal128(value.to_decimal())
    if isinstance(value, Int64):
        return int(value)
    if isinstance(value, BsonDBRef):
        return DBRef(
            value.collection,
            decode_wire_value(value.id),
            database=value.database,
            extras={key: decode_wire_value(item) for key, item in value.as_doc().items() if key not in {"$ref", "$id", "$db"}},
        )
    if isinstance(value, BsonBinary):
        if value.subtype == STANDARD and len(value) == 16:
            try:
                return value.as_uuid(uuid_representation=STANDARD)
            except (TypeError, ValueError):
                return Binary(bytes(value), subtype=value.subtype)
        return Binary(bytes(value), subtype=value.subtype)
    if isinstance(value, BsonRegex):
        return Regex(value.pattern, _regex_flags_to_string(value.flags))
    if isinstance(value, BsonTimestamp):
        return Timestamp(value.time, value.inc)
    if isinstance(value, BsonCode):
        if value.scope is None:
            return BsonCode(str(value))
        return BsonCode(str(value), decode_wire_value(value.scope))
    return value


def encode_wire_value(value: Any) -> Any:
    _require_bson()
    if isinstance(value, SON):
        return BsonSON((key, encode_wire_value(item)) for key, item in value.items())
    if isinstance(value, dict):
        return {key: encode_wire_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode_wire_value(item) for item in value]
    if isinstance(value, ObjectId):
        return BsonObjectId(str(value))
    if isinstance(value, BsonMinKey | BsonMaxKey):
        return value
    if isinstance(value, DBRef):
        return BsonDBRef(
            value.collection,
            encode_wire_value(value.id),
            database=value.database,
            **{key: encode_wire_value(item) for key, item in value.extras.items()},
        )
    if isinstance(value, BsonInt64):
        return Int64(value.value)
    if isinstance(value, BsonInt32):
        return value.value
    if isinstance(value, BsonDouble):
        return value.value
    if isinstance(value, BsonDecimal128):
        return BsonDecimal128Public(value.value)
    if isinstance(value, Decimal128):
        return BsonDecimal128Public(value.to_decimal())
    if isinstance(value, decimal.Decimal):
        return BsonDecimal128Public(value)
    if isinstance(value, uuid.UUID):
        return BsonBinary.from_uuid(value, uuid_representation=STANDARD)
    if isinstance(value, Binary):
        return BsonBinary(bytes(value), subtype=value.subtype)
    if isinstance(value, Regex):
        return BsonRegex(value.pattern, value.flags)
    if isinstance(value, Timestamp):
        return BsonTimestamp(value.time, value.inc)
    if isinstance(value, BsonCode):
        if value.scope is None:
            return BsonCode(str(value))
        return BsonCode(str(value), encode_wire_value(value.scope))
    if isinstance(value, bytes):
        return BsonBinary(value)
    if isinstance(value, UndefinedType):
        return None
    return value


def _regex_flags_to_string(flags: int | str) -> str:
    if isinstance(flags, str):
        return flags
    rendered = ""
    if flags & re.IGNORECASE:
        rendered += "i"
    if flags & re.MULTILINE:
        rendered += "m"
    if flags & re.DOTALL:
        rendered += "s"
    if flags & re.VERBOSE:
        rendered += "x"
    return rendered
