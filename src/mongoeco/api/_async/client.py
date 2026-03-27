import datetime

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
    resolve_mongodb_dialect_resolution,
    resolve_pymongo_profile_resolution,
)
from mongoeco.core.aggregation import _bson_document_size
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.validation import is_filter
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import CollectionInvalid, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    CodecOptions,
    Document,
    Filter,
    IndexModel,
    ReadConcern,
    ReadPreference,
    TransactionOptions,
    WriteConcern,
    normalize_codec_options,
    normalize_read_concern,
    normalize_read_preference,
    normalize_transaction_options,
    normalize_write_concern,
)


class AsyncDatabase:
    """Representa una base de datos de MongoDB."""

    def __init__(
        self,
        engine: AsyncStorageEngine,
        db_name: str,
        *,
        mongodb_dialect: MongoDialect | str | None = None,
        mongodb_dialect_resolution: MongoDialectResolution | None = None,
        pymongo_profile: PyMongoProfile | str | None = None,
        pymongo_profile_resolution: PyMongoProfileResolution | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ):
        self._engine = engine
        self._db_name = db_name
        self._mongodb_dialect_resolution = (
            mongodb_dialect_resolution
            if mongodb_dialect_resolution is not None
            else resolve_mongodb_dialect_resolution(mongodb_dialect)
        )
        self._mongodb_dialect = self._mongodb_dialect_resolution.resolved_dialect
        self._pymongo_profile_resolution = (
            pymongo_profile_resolution
            if pymongo_profile_resolution is not None
            else resolve_pymongo_profile_resolution(pymongo_profile)
        )
        self._pymongo_profile = self._pymongo_profile_resolution.resolved_profile
        self._write_concern = normalize_write_concern(write_concern)
        self._read_concern = normalize_read_concern(read_concern)
        self._read_preference = normalize_read_preference(read_preference)
        self._codec_options = normalize_codec_options(codec_options)

    def __getattr__(self, name: str) -> AsyncCollection:
        return self.get_collection(name)

    def __getitem__(self, name: str) -> AsyncCollection:
        return self.get_collection(name)

    def get_collection(
        self,
        name: str,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ) -> AsyncCollection:
        return AsyncCollection(
            self._engine,
            self._db_name,
            name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
            write_concern=self._write_concern if write_concern is None else write_concern,
            read_concern=self._read_concern if read_concern is None else read_concern,
            read_preference=self._read_preference if read_preference is None else read_preference,
            codec_options=self._codec_options if codec_options is None else codec_options,
        )

    def with_options(
        self,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ) -> "AsyncDatabase":
        return type(self)(
            self._engine,
            self._db_name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
            write_concern=self._write_concern if write_concern is None else write_concern,
            read_concern=self._read_concern if read_concern is None else read_concern,
            read_preference=self._read_preference if read_preference is None else read_preference,
            codec_options=self._codec_options if codec_options is None else codec_options,
        )

    @staticmethod
    def _normalize_filter(filter_spec: object | None) -> Filter:
        if filter_spec is None:
            return {}
        if not is_filter(filter_spec):
            raise TypeError("filter_spec must be a dict")
        return filter_spec

    async def list_collection_names(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> list[str]:
        if filter_spec is None:
            return await self._engine.list_collections(self._db_name, context=session)
        documents = await self.list_collections(filter_spec, session=session).to_list()
        return [str(document["name"]) for document in documents]

    def list_collections(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> AsyncListingCursor:
        normalized_filter = self._normalize_filter(filter_spec)

        async def _load() -> list[Document]:
            names = await self._engine.list_collections(self._db_name, context=session)
            documents: list[Document] = []
            for name in names:
                documents.append(
                    {
                        "name": name,
                        "type": "collection",
                        "options": await self._engine.collection_options(
                            self._db_name,
                            name,
                            context=session,
                        ),
                        "info": {"readOnly": False},
                    }
                )
            return [
                document
                for document in documents
                if QueryEngine.match(
                    document,
                    normalized_filter,
                    dialect=self._mongodb_dialect,
                )
            ]

        return AsyncListingCursor(_load)

    async def create_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
        **options: object,
    ) -> AsyncCollection:
        await self._engine.create_collection(
            self._db_name,
            name,
            options=dict(options),
            context=session,
        )
        return self.get_collection(name)

    async def drop_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
    ) -> None:
        await self._engine.drop_collection(self._db_name, name, context=session)

    @staticmethod
    def _normalize_command(command: object, kwargs: dict[str, object]) -> dict[str, object]:
        if isinstance(command, str):
            if not command:
                raise TypeError("command name must be a non-empty string")
            return {command: 1, **kwargs}
        if not isinstance(command, dict) or not command:
            raise TypeError("command must be a non-empty string or dict")
        if kwargs:
            raise TypeError("keyword arguments are only supported when command is a string")
        return command

    @staticmethod
    def _require_collection_name(value: object, field_name: str) -> str:
        if not isinstance(value, str) or not value:
            raise TypeError(f"{field_name} must be a non-empty string")
        return value

    @staticmethod
    def _resolve_collection_reference(value: object, field_name: str) -> str:
        if isinstance(value, str):
            return AsyncDatabase._require_collection_name(value, field_name)
        name = getattr(value, "name", None)
        if isinstance(name, str) and name:
            return name
        raise TypeError(f"{field_name} must be a collection name or collection object")

    @staticmethod
    def _normalize_index_models_from_command(indexes: object) -> list[IndexModel]:
        if not isinstance(indexes, list) or not indexes:
            raise TypeError("indexes must be a non-empty list")

        normalized: list[IndexModel] = []
        for raw_index in indexes:
            if not isinstance(raw_index, dict):
                raise TypeError("each index specification must be a dict")
            if "key" not in raw_index:
                raise OperationFailure("index specification must contain 'key'")

            unsupported = set(raw_index) - {"key", "name", "unique"}
            if unsupported:
                unsupported_names = ", ".join(sorted(unsupported))
                raise TypeError(
                    f"unsupported createIndexes options in command: {unsupported_names}"
                )

            kwargs: dict[str, object] = {}
            if "name" in raw_index:
                kwargs["name"] = raw_index["name"]
            if "unique" in raw_index:
                kwargs["unique"] = raw_index["unique"]
            normalized.append(IndexModel(raw_index["key"], **kwargs))
        return normalized

    async def _collection_stats(
        self,
        collection_name: str,
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection = self.get_collection(collection_name)
        documents = await collection.find({}, session=session).to_list()
        indexes = await collection.list_indexes(session=session).to_list()
        data_size = sum(_bson_document_size(document) for document in documents)
        count = len(documents)
        return {
            "ns": f"{self._db_name}.{collection_name}",
            "count": count,
            "size": data_size,
            "avgObjSize": (data_size / count) if count else 0,
            "storageSize": data_size,
            "nindexes": len(indexes),
            "totalIndexSize": 0,
            "ok": 1.0,
        }

    async def _database_stats(
        self,
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_names = await self._engine.list_collections(self._db_name, context=session)
        objects = 0
        data_size = 0
        indexes = 0
        for collection_name in collection_names:
            stats = await self._collection_stats(collection_name, session=session)
            objects += int(stats["count"])
            data_size += int(stats["size"])
            indexes += int(stats["nindexes"])
        return {
            "db": self._db_name,
            "collections": len(collection_names),
            "objects": objects,
            "avgObjSize": (data_size / objects) if objects else 0,
            "dataSize": data_size,
            "storageSize": data_size,
            "indexes": indexes,
            "indexSize": 0,
            "ok": 1.0,
        }

    async def _list_database_documents(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[Document]:
        database_names = await self._engine.list_databases(context=session)
        documents: list[Document] = []
        for database_name in database_names:
            database = type(self)(
                self._engine,
                database_name,
                mongodb_dialect=self._mongodb_dialect,
                mongodb_dialect_resolution=self._mongodb_dialect_resolution,
                pymongo_profile=self._pymongo_profile,
                pymongo_profile_resolution=self._pymongo_profile_resolution,
                write_concern=self._write_concern,
                read_concern=self._read_concern,
                read_preference=self._read_preference,
                codec_options=self._codec_options,
            )
            stats = await database._database_stats(session=session)
            documents.append(
                {
                    "name": database_name,
                    "sizeOnDisk": stats["storageSize"],
                    "empty": (
                        int(stats["collections"]) == 0
                        and int(stats["objects"]) == 0
                    ),
                }
            )
        return documents

    async def validate_collection(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> dict[str, object]:
        if not isinstance(scandata, bool):
            raise TypeError("scandata must be a bool")
        if not isinstance(full, bool):
            raise TypeError("full must be a bool")
        if background is not None and not isinstance(background, bool):
            raise TypeError("background must be a bool or None")
        if comment is not None and not isinstance(comment, str):
            raise TypeError("comment must be a string")

        collection_name = self._resolve_collection_reference(
            name_or_collection,
            "name_or_collection",
        )
        collection_names = await self._engine.list_collections(self._db_name, context=session)
        if collection_name not in collection_names:
            raise CollectionInvalid(f"collection '{collection_name}' does not exist")

        indexes = await self.get_collection(collection_name).list_indexes(session=session).to_list()
        return {
            "ns": f"{self._db_name}.{collection_name}",
            "valid": True,
            "nrecords": await self.get_collection(collection_name).count_documents(
                {},
                session=session,
            ),
            "nIndexes": len(indexes),
            "keysPerIndex": {
                str(index["name"]): len(index.get("fields", []))
                for index in indexes
            },
            "warnings": [],
            "ok": 1.0,
        }

    async def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        spec = self._normalize_command(command, kwargs)
        command_name = next(iter(spec))
        if not isinstance(command_name, str):
            raise TypeError("command name must be a string")

        if command_name == "ping":
            return {"ok": 1.0}

        if command_name == "buildInfo":
            return _build_info_document(self._mongodb_dialect)

        if command_name in {"hello", "isMaster", "ismaster"}:
            return _hello_document(self._mongodb_dialect, legacy_name=command_name != "hello")

        if command_name == "listCollections":
            filter_spec = self._normalize_filter(spec.get("filter"))
            first_batch = await self.list_collections(
                filter_spec,
                session=session,
            ).to_list()
            return {
                "cursor": {
                    "id": 0,
                    "ns": f"{self._db_name}.$cmd.listCollections",
                    "firstBatch": first_batch,
                },
                "ok": 1.0,
            }

        if command_name == "listDatabases":
            name_only = spec.get("nameOnly", False)
            if not isinstance(name_only, bool):
                raise TypeError("nameOnly must be a bool")
            filter_spec = self._normalize_filter(spec.get("filter"))
            databases = await self._list_database_documents(session=session)
            filtered = [
                document
                for document in databases
                if QueryEngine.match(
                    document,
                    filter_spec,
                    dialect=self._mongodb_dialect,
                )
            ]
            total_size = sum(int(document.get("sizeOnDisk", 0)) for document in filtered)
            if name_only:
                filtered = [{"name": str(document["name"])} for document in filtered]
            return {
                "databases": filtered,
                "totalSize": total_size,
                "ok": 1.0,
            }

        if command_name == "create":
            collection_name = self._require_collection_name(spec.get("create"), "create")
            options = {
                key: value
                for key, value in spec.items()
                if key not in {"create", "$db"}
            }
            await self.create_collection(
                collection_name,
                session=session,
                **options,
            )
            return {"ok": 1.0}

        if command_name == "drop":
            collection_name = self._require_collection_name(spec.get("drop"), "drop")
            await self.drop_collection(collection_name, session=session)
            return {
                "ns": f"{self._db_name}.{collection_name}",
                "ok": 1.0,
            }

        if command_name == "count":
            collection_name = self._require_collection_name(spec.get("count"), "count")
            query = self._normalize_filter(spec.get("query"))
            count = await self.get_collection(collection_name).count_documents(
                query,
                session=session,
            )
            return {"n": count, "ok": 1.0}

        if command_name == "distinct":
            collection_name = self._require_collection_name(
                spec.get("distinct"),
                "distinct",
            )
            key = spec.get("key")
            if not isinstance(key, str) or not key:
                raise TypeError("key must be a non-empty string")
            query = self._normalize_filter(spec.get("query"))
            values = await self.get_collection(collection_name).distinct(
                key,
                query,
                session=session,
            )
            return {"values": values, "ok": 1.0}

        if command_name == "listIndexes":
            collection_name = self._require_collection_name(
                spec.get("listIndexes"),
                "listIndexes",
            )
            first_batch = await self.get_collection(collection_name).list_indexes(
                session=session,
            ).to_list()
            return {
                "cursor": {
                    "id": 0,
                    "ns": f"{self._db_name}.{collection_name}",
                    "firstBatch": first_batch,
                },
                "ok": 1.0,
            }

        if command_name == "createIndexes":
            collection_name = self._require_collection_name(
                spec.get("createIndexes"),
                "createIndexes",
            )
            indexes = self._normalize_index_models_from_command(spec.get("indexes"))
            collection_names_before = set(
                await self._engine.list_collections(self._db_name, context=session)
            )
            collection = self.get_collection(collection_name)
            info_before = await collection.index_information(session=session)
            await collection.create_indexes(indexes, session=session)
            info_after = await collection.index_information(session=session)
            result: dict[str, object] = {
                "numIndexesBefore": len(info_before),
                "numIndexesAfter": len(info_after),
                "createdCollectionAutomatically": collection_name not in collection_names_before,
                "ok": 1.0,
            }
            if len(info_before) == len(info_after):
                result["note"] = "all indexes already exist"
            return result

        if command_name == "dropIndexes":
            collection_name = self._require_collection_name(
                spec.get("dropIndexes"),
                "dropIndexes",
            )
            target = spec.get("index")
            collection = self.get_collection(collection_name)
            info_before = await collection.index_information(session=session)
            if target == "*":
                await collection.drop_indexes(session=session)
            elif isinstance(target, (str, list, tuple, dict)):
                await collection.drop_index(target, session=session)
            else:
                raise TypeError("index must be '*', a name, or a key specification")
            info_after = await collection.index_information(session=session)
            return {
                "nIndexesWas": len(info_before),
                "ok": 1.0,
                "msg": f"non-_id indexes for collection {self._db_name}.{collection_name} dropped",
            } if target == "*" else {
                "nIndexesWas": len(info_before),
                "ok": 1.0,
            }

        if command_name == "dropDatabase":
            collection_names = await self._engine.list_collections(
                self._db_name,
                context=session,
            )
            for collection_name in collection_names:
                await self._engine.drop_collection(
                    self._db_name,
                    collection_name,
                    context=session,
                )
            return {"dropped": self._db_name, "ok": 1.0}

        if command_name == "collStats":
            collection_name = self._require_collection_name(spec.get("collStats"), "collStats")
            return await self._collection_stats(collection_name, session=session)

        if command_name == "dbStats":
            return await self._database_stats(session=session)

        if command_name == "validate":
            collection_name = self._require_collection_name(spec.get("validate"), "validate")
            return await self.validate_collection(
                collection_name,
                scandata=bool(spec.get("scandata", False)),
                full=bool(spec.get("full", False)),
                background=spec.get("background"),
                session=session,
            )

        raise OperationFailure(f"Unsupported command: {command_name}")

    @property
    def mongodb_dialect(self) -> MongoDialect:
        return self._mongodb_dialect

    @property
    def mongodb_dialect_resolution(self) -> MongoDialectResolution:
        return self._mongodb_dialect_resolution

    @property
    def pymongo_profile(self) -> PyMongoProfile:
        return self._pymongo_profile

    @property
    def pymongo_profile_resolution(self) -> PyMongoProfileResolution:
        return self._pymongo_profile_resolution

    @property
    def write_concern(self) -> WriteConcern:
        return self._write_concern

    @property
    def read_concern(self) -> ReadConcern:
        return self._read_concern

    @property
    def read_preference(self) -> ReadPreference:
        return self._read_preference

    @property
    def codec_options(self) -> CodecOptions:
        return self._codec_options


class AsyncMongoClient:
    """
    Cliente principal para mongoeco.
    """

    def __init__(
        self,
        engine: AsyncStorageEngine | None = None,
        *,
        mongodb_dialect: MongoDialect | str | None = None,
        pymongo_profile: PyMongoProfile | str | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        transaction_options: TransactionOptions | None = None,
    ):
        self._engine = engine or self._create_default_engine()
        self._mongodb_dialect_resolution = resolve_mongodb_dialect_resolution(
            mongodb_dialect
        )
        self._mongodb_dialect = self._mongodb_dialect_resolution.resolved_dialect
        self._pymongo_profile_resolution = resolve_pymongo_profile_resolution(
            pymongo_profile
        )
        self._pymongo_profile = self._pymongo_profile_resolution.resolved_profile
        self._write_concern = normalize_write_concern(write_concern)
        self._read_concern = normalize_read_concern(read_concern)
        self._read_preference = normalize_read_preference(read_preference)
        self._codec_options = normalize_codec_options(codec_options)
        self._transaction_options = normalize_transaction_options(transaction_options)

    @staticmethod
    def _create_default_engine() -> AsyncStorageEngine:
        from mongoeco.engines.memory import MemoryEngine

        return MemoryEngine()

    async def __aenter__(self):
        await self._engine.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._engine.disconnect()

    def __getattr__(self, name: str) -> AsyncDatabase:
        return self.get_database(name)

    def __getitem__(self, name: str) -> AsyncDatabase:
        return self.get_database(name)

    def with_options(
        self,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        transaction_options: TransactionOptions | None = None,
    ) -> "AsyncMongoClient":
        return type(self)(
            self._engine,
            mongodb_dialect=self._mongodb_dialect,
            pymongo_profile=self._pymongo_profile,
            write_concern=self._write_concern if write_concern is None else write_concern,
            read_concern=self._read_concern if read_concern is None else read_concern,
            read_preference=self._read_preference if read_preference is None else read_preference,
            codec_options=self._codec_options if codec_options is None else codec_options,
            transaction_options=(
                self._transaction_options
                if transaction_options is None
                else transaction_options
            ),
        )

    def get_database(
        self,
        name: str,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ) -> AsyncDatabase:
        return AsyncDatabase(
            self._engine,
            name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
            write_concern=self._write_concern if write_concern is None else write_concern,
            read_concern=self._read_concern if read_concern is None else read_concern,
            read_preference=self._read_preference if read_preference is None else read_preference,
            codec_options=self._codec_options if codec_options is None else codec_options,
        )

    def start_session(
        self,
        *,
        default_transaction_options: TransactionOptions | None = None,
    ) -> ClientSession:
        session = ClientSession(
            default_transaction_options=(
                self._transaction_options
                if default_transaction_options is None
                else normalize_transaction_options(default_transaction_options)
            )
        )
        self._engine.create_session_state(session)
        return session

    async def list_database_names(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[str]:
        return await self._engine.list_databases(context=session)

    async def drop_database(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
    ) -> None:
        collection_names = await self._engine.list_collections(name, context=session)
        for collection_name in collection_names:
            await self._engine.drop_collection(name, collection_name, context=session)

    async def server_info(self) -> dict[str, object]:
        return _build_info_document(self._mongodb_dialect)

    @property
    def mongodb_dialect(self) -> MongoDialect:
        return self._mongodb_dialect

    @property
    def mongodb_dialect_resolution(self) -> MongoDialectResolution:
        return self._mongodb_dialect_resolution

    @property
    def pymongo_profile(self) -> PyMongoProfile:
        return self._pymongo_profile

    @property
    def pymongo_profile_resolution(self) -> PyMongoProfileResolution:
        return self._pymongo_profile_resolution

    @property
    def write_concern(self) -> WriteConcern:
        return self._write_concern

    @property
    def read_concern(self) -> ReadConcern:
        return self._read_concern

    @property
    def read_preference(self) -> ReadPreference:
        return self._read_preference

    @property
    def codec_options(self) -> CodecOptions:
        return self._codec_options

    @property
    def transaction_options(self) -> TransactionOptions:
        return self._transaction_options


def _build_info_document(mongodb_dialect: MongoDialect) -> dict[str, object]:
    version_parts = [int(part) for part in mongodb_dialect.server_version.split(".")]
    while len(version_parts) < 2:
        version_parts.append(0)
    major, minor = version_parts[:2]
    return {
        "version": f"{major}.{minor}.0",
        "versionArray": [major, minor, 0, 0],
        "gitVersion": "mongoeco",
        "ok": 1.0,
    }


def _hello_document(
    mongodb_dialect: MongoDialect,
    *,
    legacy_name: bool = False,
) -> dict[str, object]:
    document: dict[str, object] = {
        "helloOk": True,
        "isWritablePrimary": True,
        "maxBsonObjectSize": 16 * 1024 * 1024,
        "maxMessageSizeBytes": 48_000_000,
        "maxWriteBatchSize": 100_000,
        "localTime": datetime.datetime.now(datetime.UTC),
        "logicalSessionTimeoutMinutes": 30,
        "connectionId": 1,
        "minWireVersion": 0,
        # La documentación oficial de hello para 7.0/8.0 sigue mostrando 20
        # en ejemplos de despliegue standalone; usamos ese baseline estable.
        "maxWireVersion": 20,
        "readOnly": False,
        "ok": 1.0,
    }
    if legacy_name:
        document["ismaster"] = True
    else:
        document["isWritablePrimary"] = True
    document.update(_build_info_document(mongodb_dialect))
    return document
