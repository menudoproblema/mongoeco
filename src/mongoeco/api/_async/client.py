from mongoeco.api._async.collection import AsyncCollection
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.session import ClientSession


class AsyncDatabase:
    """Representa una base de datos de MongoDB."""

    def __init__(self, engine: AsyncStorageEngine, db_name: str):
        self._engine = engine
        self._db_name = db_name

    def __getattr__(self, name: str) -> AsyncCollection:
        return self.get_collection(name)

    def __getitem__(self, name: str) -> AsyncCollection:
        return self.get_collection(name)

    def get_collection(self, name: str) -> AsyncCollection:
        return AsyncCollection(self._engine, self._db_name, name)

    async def list_collection_names(self) -> list[str]:
        return await self._engine.list_collections(self._db_name)


class AsyncMongoClient:
    """
    Cliente principal para mongoeco.
    """

    def __init__(self, engine: AsyncStorageEngine | None = None):
        self._engine = engine or self._create_default_engine()

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
        return AsyncDatabase(self._engine, name)

    def start_session(self) -> ClientSession:
        session = ClientSession()
        self._engine.create_session_state(session)
        return session

    async def list_database_names(self) -> list[str]:
        return await self._engine.list_databases()
