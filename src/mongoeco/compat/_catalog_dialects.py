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
SUPPORTED_SYSTEM_VARIABLES = frozenset({"$$NOW"})

MONGODB_DIALECT_CATALOG = MappingProxyType(
    {
        "7.0": MongoDialectCatalogEntry(
            key="7.0",
            server_version="7.0",
            label="MongoDB 7.0",
            aliases=("7", "7.0"),
            behavior_flags=MappingProxyType({
                "null_query_matches_undefined": True,
                "uses_server_densify_bounds": True,
                "densify_equal_bounds_are_empty": False,
                "densify_full_nonadvancing_step_errors": False,
                "densify_empty_partition_generates_values": True,
                "documents_join_omits_collection_namespace": False,
            }),
            policy_spec=MongoBehaviorPolicySpec(null_query_matches_undefined=True),
            capabilities=frozenset({MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED}),
            system_variables=SUPPORTED_SYSTEM_VARIABLES,
        ),
        "8.0": MongoDialectCatalogEntry(
            key="8.0",
            server_version="8.0",
            label="MongoDB 8.0",
            aliases=("8", "8.0"),
            behavior_flags=MappingProxyType({
                "null_query_matches_undefined": False,
                "uses_server_densify_bounds": True,
                "densify_equal_bounds_are_empty": True,
                "densify_full_nonadvancing_step_errors": True,
                "densify_empty_partition_generates_values": False,
                "documents_join_omits_collection_namespace": True,
            }),
            policy_spec=MongoBehaviorPolicySpec(null_query_matches_undefined=False),
            capabilities=frozenset(),
            system_variables=SUPPORTED_SYSTEM_VARIABLES,
        ),
        "9.0": MongoDialectCatalogEntry(
            key="9.0",
            server_version="9.0",
            label="MongoDB 9.0",
            aliases=("9", "9.0"),
            behavior_flags=MappingProxyType({
                "null_query_matches_undefined": False,
                "rejects_empty_group_fields": True,
                "validates_densify_partition_paths": True,
                "validates_aggregation_syntax_early": True,
                "documents_join_omits_collection_namespace": True,
                "supports_array_index_variables": True,
                "uses_extended_conversions": True,
                "uses_server_densify_bounds": True,
                "densify_equal_bounds_are_empty": True,
                "densify_full_nonadvancing_step_errors": True,
                "densify_empty_partition_generates_values": False,
                "supports_date_range_windows": True,
                "limits_trim_chars": True,
                "rejects_cluster_time_expression": True,
                "list_indexes_includes_simple_collation": True,
                "rejects_unimplemented_wildcard_projection": True,
                "merge_insert_preserves_incoming_field_order": True,
            }),
            policy_spec=MongoBehaviorPolicySpec(null_query_matches_undefined=False),
            capabilities=frozenset({
                "aggregation.array_index_variables",
                "aggregation.convert.base",
                "aggregation.to_string.extended",
                "aggregation.date_range_windows",
                "indexes.simple_collation_metadata",
            }),
            system_variables=SUPPORTED_SYSTEM_VARIABLES,
        ),
    }
)

MONGODB_DIALECT_ALIASES = MappingProxyType(
    {alias: entry.key for entry in MONGODB_DIALECT_CATALOG.values() for alias in entry.aliases}
)

SUPPORTED_MONGODB_MAJORS = frozenset(int(key.split(".", 1)[0]) for key in MONGODB_DIALECT_CATALOG)
