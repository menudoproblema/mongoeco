import asyncio

from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
)
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.api._sync.collection import Collection


class _SyncRunner:
    """Ejecuta la API async en un loop dedicado y estable."""

    def __init__(self):
        self._runner = asyncio.Runner()
        self._closed = False

    def _cleanup_pending_tasks(self) -> None:
        if self._closed:
            return
        get_loop = getattr(self._runner, "get_loop", None)
        if not callable(get_loop):
            return
        try:
            loop = get_loop()
        except Exception:
            return
        if loop.is_closed():
            return
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        if not pending:
            return
        for task in pending:
            task.cancel()

        async def _drain_pending() -> None:
            await asyncio.gather(*pending, return_exceptions=True)

        self._runner.run(_drain_pending())

    def run(self, awaitable):
        if self._closed:
            raise InvalidOperation("El cliente sincronico ya esta cerrado")

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self._runner.run(awaitable)

        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        raise InvalidOperation("MongoClient no puede usarse dentro de un event loop activo")

    def close(self) -> None:
        if not self._closed:
            try:
                try:
                    asyncio.get_running_loop()
                    running_loop_active = True
                except RuntimeError:
                    running_loop_active = False

                if running_loop_active:
                    get_loop = getattr(self._runner, "get_loop", None)
                    if callable(get_loop):
                        try:
                            loop = get_loop()
                        except Exception:
                            loop = None
                        if loop is not None and not loop.is_closed():
                            loop.close()
                else:
                    self._cleanup_pending_tasks()
                    self._runner.close()
            finally:
                self._closed = True

    def __del__(self):
        try:
            self.close()
        except Exception:
            self._closed = True


class Database:
    """Representa una base de datos de MongoDB."""

    def __init__(self, client: "MongoClient", name: str):
        self._client = client
        self._name = name

    def __getattr__(self, name: str) -> Collection:
        return self.get_collection(name)

    def __getitem__(self, name: str) -> Collection:
        return self.get_collection(name)

    def get_collection(self, name: str) -> Collection:
        return Collection(self._client, self._name, name)

    def list_collection_names(self) -> list[str]:
        self._client._ensure_connected()
        async_database = self._client._async_client.get_database(self._name)
        return self._client._run(async_database.list_collection_names())

    def drop_collection(self, name: str) -> None:
        self._client._ensure_connected()
        async_database = self._client._async_client.get_database(self._name)
        self._client._run(async_database.drop_collection(name))

    @property
    def mongodb_dialect(self) -> MongoDialect:
        return self._client.mongodb_dialect

    @property
    def mongodb_dialect_resolution(self) -> MongoDialectResolution:
        return self._client.mongodb_dialect_resolution

    @property
    def pymongo_profile(self) -> PyMongoProfile:
        return self._client.pymongo_profile

    @property
    def pymongo_profile_resolution(self) -> PyMongoProfileResolution:
        return self._client.pymongo_profile_resolution


class MongoClient:
    """Cliente sincronico que adapta la implementacion async."""

    def __init__(
        self,
        engine: AsyncStorageEngine | None = None,
        *,
        mongodb_dialect: MongoDialect | str | None = None,
        pymongo_profile: PyMongoProfile | str | None = None,
    ):
        self._async_client = AsyncMongoClient(
            engine,
            mongodb_dialect=mongodb_dialect,
            pymongo_profile=pymongo_profile,
        )
        self._runner = _SyncRunner()
        self._connected = False
        self._closed = False

    def _run(self, awaitable):
        return self._runner.run(awaitable)

    def _ensure_connected(self) -> None:
        if self._closed:
            raise InvalidOperation("El cliente sincronico ya esta cerrado")
        if not self._connected:
            self._run(self._async_client.__aenter__())
            self._connected = True

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self._connected:
                self._run(self._async_client.__aexit__(None, None, None))
                self._connected = False
        finally:
            try:
                self._runner.close()
            finally:
                self._closed = True

    def __enter__(self):
        self._ensure_connected()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._closed:
            return False

        try:
            if self._connected:
                self._run(self._async_client.__aexit__(exc_type, exc_val, exc_tb))
                self._connected = False
        finally:
            try:
                self._runner.close()
            finally:
                self._closed = True
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def __getattr__(self, name: str) -> Database:
        return self.get_database(name)

    def __getitem__(self, name: str) -> Database:
        return self.get_database(name)

    def get_database(self, name: str) -> Database:
        return Database(self, name)

    def start_session(self) -> ClientSession:
        return self._async_client.start_session()

    def list_database_names(self) -> list[str]:
        self._ensure_connected()
        return self._run(self._async_client.list_database_names())

    def drop_database(self, name: str) -> None:
        self._ensure_connected()
        self._run(self._async_client.drop_database(name))

    @property
    def mongodb_dialect(self) -> MongoDialect:
        return self._async_client.mongodb_dialect

    @property
    def mongodb_dialect_resolution(self) -> MongoDialectResolution:
        return self._async_client.mongodb_dialect_resolution

    @property
    def pymongo_profile(self) -> PyMongoProfile:
        return self._async_client.pymongo_profile

    @property
    def pymongo_profile_resolution(self) -> PyMongoProfileResolution:
        return self._async_client.pymongo_profile_resolution
