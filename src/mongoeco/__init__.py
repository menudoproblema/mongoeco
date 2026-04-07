from mongoeco.api import AsyncMongoClient, MongoClient
from mongoeco._version import __version__
from mongoeco.driver import (
    MongoClientOptions,
    MongoUri,
    build_read_concern_from_uri,
    build_read_preference_from_uri,
    build_write_concern_from_uri,
    parse_mongo_uri,
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


def __getattr__(name: str):
    raise AttributeError(name)
