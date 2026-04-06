from mongoeco.api import AsyncMongoClient, MongoClient
from mongoeco._version import __version__
from mongoeco.change_streams import ChangeStreamBackendInfo
from mongoeco.cxp import (
    MONGODB_CATALOG,
    MONGODB_INTERFACE,
)
from mongoeco.core.collation import (
    CollationBackendInfo,
    CollationCapabilitiesInfo,
    collation_backend_info,
    collation_capabilities_info,
)
from mongoeco.errors import CollectionInvalid
from mongoeco.driver import (
    MongoClientOptions,
    MongoUri,
    SdamCapabilitiesInfo,
    build_read_concern_from_uri,
    build_read_preference_from_uri,
    build_write_concern_from_uri,
    sdam_capabilities_info,
    parse_mongo_uri,
)
from mongoeco.compat import (
    AUTO_INSTALLED_PYMONGO_PROFILE,
    DEFAULT_MONGODB_DIALECT,
    DEFAULT_PYMONGO_PROFILE,
    detect_installed_pymongo_profile,
    detect_installed_pymongo_profile_resolution,
    MONGODB_DIALECT_HOOK_NAMES,
    MONGODB_DIALECT_70,
    MONGODB_DIALECT_80,
    MONGODB_DIALECT_ALIASES,
    MONGODB_DIALECT_BEHAVIOR_FLAGS,
    MONGODB_DIALECT_CAPABILITIES,
    MONGODB_DIALECT_POLICY_SPECS,
    MONGODB_DIALECTS,
    MongoBehaviorPolicySpec,
    MongoDialect,
    MongoDialectResolution,
    MongoDialect70,
    MongoDialect80,
    PYMONGO_PROFILE_49,
    PYMONGO_PROFILE_411,
    PYMONGO_PROFILE_413,
    PYMONGO_PROFILE_HOOK_NAMES,
    PYMONGO_PROFILE_ALIASES,
    PYMONGO_PROFILE_BEHAVIOR_FLAGS,
    PYMONGO_PROFILE_CAPABILITIES,
    PYMONGO_PROFILES,
    PyMongoProfile,
    PyMongoProfile49,
    PyMongoProfile411,
    PyMongoProfile413,
    PyMongoProfileResolution,
    get_operation_option_support,
    is_operation_option_effective,
    OPERATION_OPTION_SUPPORT,
    OperationOptionSupport,
    OptionSupportStatus,
    export_full_compat_catalog,
    export_cxp_catalog,
    export_cxp_operation_catalog,
    export_cxp_profile_catalog,
    export_cxp_profile_support_catalog,
    export_mongodb_dialect_catalog,
    export_operation_option_catalog,
    export_pymongo_profile_catalog,
    UNSUPPORTED_OPERATION_OPTION,
    resolve_mongodb_dialect_resolution,
    resolve_mongodb_dialect,
    resolve_pymongo_profile,
    resolve_pymongo_profile_resolution,
    STRICT_AUTO_INSTALLED_PYMONGO_PROFILE,
    SUPPORTED_MONGODB_MAJORS,
    SUPPORTED_PYMONGO_MAJORS,
)
from mongoeco.session import ClientSession
from mongoeco.types import (
    Binary, BulkWriteResult, CodecOptions, DBRef, Decimal128, DeleteMany, DeleteOne, IndexDefinition,
    IndexModel, InsertOne, ObjectId, ReadConcern, ReadPreference, ReadPreferenceMode, Regex, ReplaceOne,
    ReturnDocument, SearchIndexDefinition, SearchIndexModel, SON, Timestamp, TransactionOptions, UNDEFINED,
    UndefinedType, UpdateMany, UpdateOne, WriteConcern,
)

_CLIENT_EXPORTS = (
    "AsyncMongoClient",
    "MongoClient",
    "ClientSession",
    "CollectionInvalid",
    "ChangeStreamBackendInfo",
    "__version__",
)

_COMPAT_EXPORTS = (
    "DEFAULT_MONGODB_DIALECT",
    "DEFAULT_PYMONGO_PROFILE",
    "AUTO_INSTALLED_PYMONGO_PROFILE",
    "STRICT_AUTO_INSTALLED_PYMONGO_PROFILE",
    "MongoDialect",
    "MongoDialectResolution",
    "MongoDialect70",
    "MongoDialect80",
    "MongoBehaviorPolicySpec",
    "PyMongoProfile",
    "PyMongoProfileResolution",
    "PyMongoProfile49",
    "PyMongoProfile411",
    "PyMongoProfile413",
    "MONGODB_DIALECT_HOOK_NAMES",
    "MONGODB_DIALECT_70",
    "MONGODB_DIALECT_80",
    "MONGODB_DIALECTS",
    "MONGODB_DIALECT_ALIASES",
    "MONGODB_DIALECT_CAPABILITIES",
    "MONGODB_DIALECT_BEHAVIOR_FLAGS",
    "MONGODB_DIALECT_POLICY_SPECS",
    "PYMONGO_PROFILE_HOOK_NAMES",
    "PYMONGO_PROFILE_49",
    "PYMONGO_PROFILE_411",
    "PYMONGO_PROFILE_413",
    "PYMONGO_PROFILES",
    "PYMONGO_PROFILE_ALIASES",
    "PYMONGO_PROFILE_CAPABILITIES",
    "PYMONGO_PROFILE_BEHAVIOR_FLAGS",
    "SUPPORTED_MONGODB_MAJORS",
    "SUPPORTED_PYMONGO_MAJORS",
    "OptionSupportStatus",
    "OperationOptionSupport",
    "OPERATION_OPTION_SUPPORT",
    "UNSUPPORTED_OPERATION_OPTION",
    "get_operation_option_support",
    "is_operation_option_effective",
    "export_mongodb_dialect_catalog",
    "export_pymongo_profile_catalog",
    "export_operation_option_catalog",
    "export_full_compat_catalog",
    "export_cxp_catalog",
    "export_cxp_operation_catalog",
    "export_cxp_profile_catalog",
    "export_cxp_profile_support_catalog",
    "resolve_mongodb_dialect_resolution",
    "resolve_mongodb_dialect",
    "resolve_pymongo_profile",
    "resolve_pymongo_profile_resolution",
    "detect_installed_pymongo_profile",
    "detect_installed_pymongo_profile_resolution",
)

_CXP_EXPORTS = (
    "MONGODB_INTERFACE",
    "MONGODB_CATALOG",
)

_RUNTIME_METADATA_EXPORTS = (
    "CollationBackendInfo",
    "CollationCapabilitiesInfo",
    "collation_backend_info",
    "collation_capabilities_info",
    "SdamCapabilitiesInfo",
    "sdam_capabilities_info",
)

_CONFIG_EXPORTS = (
    "MongoClientOptions",
    "MongoUri",
    "parse_mongo_uri",
)

_TYPE_EXPORTS = (
    "InsertOne",
    "UpdateOne",
    "UpdateMany",
    "ReplaceOne",
    "DeleteOne",
    "DeleteMany",
    "IndexDefinition",
    "IndexModel",
    "SearchIndexDefinition",
    "SearchIndexModel",
    "BulkWriteResult",
    "WriteConcern",
    "ReadConcern",
    "ReadPreference",
    "ReadPreferenceMode",
    "CodecOptions",
    "TransactionOptions",
    "Binary",
    "Regex",
    "Timestamp",
    "Decimal128",
    "SON",
    "DBRef",
    "ObjectId",
    "ReturnDocument",
    "UndefinedType",
    "UNDEFINED",
)

__all__ = [
    *_CLIENT_EXPORTS,
    *_COMPAT_EXPORTS,
    *_CXP_EXPORTS,
    *_RUNTIME_METADATA_EXPORTS,
    *_CONFIG_EXPORTS,
    *_TYPE_EXPORTS,
]


def __getattr__(name: str):
    if name in {
        "CallbackCommandTransport",
        "LocalCommandTransport",
        "WireProtocolCommandTransport",
    }:
        from mongoeco.driver import __getattr__ as _driver_getattr

        value = _driver_getattr(name)
        globals()[name] = value
        return value
    raise AttributeError(name)
