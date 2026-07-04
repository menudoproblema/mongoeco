from importlib import import_module

from mongoeco._version import __version__ as __version__

_CLIENT_EXPORTS = (
    "AsyncMongoClient",
    "MongoClient",
    "ClientSession",
    "__version__",
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
    "UuidRepresentation",
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
    *_CONFIG_EXPORTS,
    *_TYPE_EXPORTS,
]

_EXPORT_MODULES = {
    "AsyncMongoClient": "mongoeco.api",
    "MongoClient": "mongoeco.api",
    "ClientSession": "mongoeco.session",
    "MongoClientOptions": "mongoeco.driver",
    "MongoUri": "mongoeco.driver",
    "build_read_concern_from_uri": "mongoeco.driver",
    "build_read_preference_from_uri": "mongoeco.driver",
    "build_write_concern_from_uri": "mongoeco.driver",
    "parse_mongo_uri": "mongoeco.driver",
    **dict.fromkeys(_TYPE_EXPORTS, "mongoeco._types"),
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
