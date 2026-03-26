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
from mongoeco.session import ClientSession
from mongoeco.types import Document, Filter


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

    def __getattr__(self, name: str) -> AsyncCollection:
        return self.get_collection(name)

    def __getitem__(self, name: str) -> AsyncCollection:
        return self.get_collection(name)

    def get_collection(self, name: str) -> AsyncCollection:
        return AsyncCollection(
            self._engine,
            self._db_name,
            name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
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
            documents: list[Document] = [
                {
                    "name": name,
                    "type": "collection",
                    "options": {},
                    "info": {"readOnly": False},
                }
                for name in names
            ]
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
    ) -> AsyncCollection:
        await self._engine.create_collection(self._db_name, name, context=session)
        return self.get_collection(name)

    async def drop_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
    ) -> None:
        await self._engine.drop_collection(self._db_name, name, context=session)

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

    def get_database(self, name: str) -> AsyncDatabase:
        return AsyncDatabase(
            self._engine,
            name,
            mongodb_dialect=self._mongodb_dialect,
            mongodb_dialect_resolution=self._mongodb_dialect_resolution,
            pymongo_profile=self._pymongo_profile,
            pymongo_profile_resolution=self._pymongo_profile_resolution,
        )

    def start_session(self) -> ClientSession:
        session = ClientSession()
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
