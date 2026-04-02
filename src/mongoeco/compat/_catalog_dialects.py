from __future__ import annotations

import datetime
import decimal
import re
import uuid
from types import MappingProxyType

try:
    from bson.code import Code as BsonCode
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
except Exception:  # pragma: no cover - optional dependency
    BsonCode = None
    BsonMaxKey = None
    BsonMinKey = None

from mongoeco.compat._catalog_constants import MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED
from mongoeco.compat._catalog_models import MongoBehaviorPolicySpec, MongoDialectCatalogEntry
from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.types import Binary, DBRef, Decimal128, ObjectId, Regex, SON, Timestamp, UndefinedType

_DEFAULT_BSON_TYPE_ORDER: dict[type[object], int] = {
    type(None): 1,
    UndefinedType: 1,
    int: 2,
    float: 2,
    decimal.Decimal: 2,
    Decimal128: 2,
    BsonInt32: 2,
    BsonInt64: 2,
    BsonDouble: 2,
    BsonDecimal128: 2,
    str: 3,
    dict: 4,
    SON: 4,
    DBRef: 4,
    list: 5,
    bytes: 6,
    Binary: 6,
    uuid.UUID: 6,
    ObjectId: 7,
    bool: 8,
    datetime.datetime: 9,
    Timestamp: 10,
    re.Pattern: 11,
    Regex: 11,
}
if BsonMinKey is not None:
    _DEFAULT_BSON_TYPE_ORDER[BsonMinKey] = 0
if BsonCode is not None:
    _DEFAULT_BSON_TYPE_ORDER[BsonCode] = 12
if BsonMaxKey is not None:
    _DEFAULT_BSON_TYPE_ORDER[BsonMaxKey] = 127
DEFAULT_BSON_TYPE_ORDER = MappingProxyType(_DEFAULT_BSON_TYPE_ORDER)

MONGODB_DIALECT_CATALOG = MappingProxyType(
    {
        "7.0": MongoDialectCatalogEntry(
            key="7.0",
            server_version="7.0",
            label="MongoDB 7.0",
            aliases=("7", "7.0"),
            behavior_flags=MappingProxyType({"null_query_matches_undefined": True}),
            policy_spec=MongoBehaviorPolicySpec(null_query_matches_undefined=True),
            capabilities=frozenset({MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED}),
        ),
        "8.0": MongoDialectCatalogEntry(
            key="8.0",
            server_version="8.0",
            label="MongoDB 8.0",
            aliases=("8", "8.0"),
            behavior_flags=MappingProxyType({"null_query_matches_undefined": False}),
            policy_spec=MongoBehaviorPolicySpec(null_query_matches_undefined=False),
            capabilities=frozenset(),
        ),
    }
)

MONGODB_DIALECT_ALIASES = MappingProxyType(
    {alias: entry.key for entry in MONGODB_DIALECT_CATALOG.values() for alias in entry.aliases}
)

SUPPORTED_MONGODB_MAJORS = frozenset(int(key.split(".", 1)[0]) for key in MONGODB_DIALECT_CATALOG)
