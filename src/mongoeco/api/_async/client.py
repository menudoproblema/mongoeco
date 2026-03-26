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
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.validation import is_filter
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    CodecOptions,
    Document,
    Filter,
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
