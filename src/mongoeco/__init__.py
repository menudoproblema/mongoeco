from importlib import import_module
from typing import TYPE_CHECKING, Any

from mongoeco._version import __version__ as __version__


if TYPE_CHECKING:
    from mongoeco._types import (
        SON as SON,
        Binary as Binary,
        BulkWriteResult as BulkWriteResult,
        CodecOptions as CodecOptions,
        DBRef as DBRef,
        Decimal128 as Decimal128,
        DeleteMany as DeleteMany,
        DeleteOne as DeleteOne,
        IndexDefinition as IndexDefinition,
        IndexModel as IndexModel,
        InsertOne as InsertOne,
        ObjectId as ObjectId,
        ObjectIdLike as ObjectIdLike,
        ReadConcern as ReadConcern,
        ReadPreference as ReadPreference,
        ReadPreferenceMode as ReadPreferenceMode,
        Regex as Regex,
        ReplaceOne as ReplaceOne,
        ReturnDocument as ReturnDocument,
        SearchIndexDefinition as SearchIndexDefinition,
        SearchIndexModel as SearchIndexModel,
        Timestamp as Timestamp,
        TransactionOptions as TransactionOptions,
        UndefinedType as UndefinedType,
        UpdateMany as UpdateMany,
        UpdateOne as UpdateOne,
        UuidRepresentation as UuidRepresentation,
        WriteConcern as WriteConcern,
    )
    from mongoeco.api import (
        AsyncMongoClient as AsyncMongoClient,
        MongoClient as MongoClient,
        NowFactory as NowFactory,
    )
    from mongoeco.driver import (
        MongoClientOptions as MongoClientOptions,
        MongoUri as MongoUri,
        parse_mongo_uri as parse_mongo_uri,
    )
    from mongoeco.session import ClientSession as ClientSession

_CLIENT_EXPORTS = (
    "AsyncMongoClient",
    "NowFactory",
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
    "ObjectIdLike",
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
    "NowFactory": "mongoeco.api",
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


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
