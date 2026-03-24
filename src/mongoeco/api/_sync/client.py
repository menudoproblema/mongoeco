import asyncio

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


class MongoClient:
    """Cliente sincronico que adapta la implementacion async."""

    def __init__(self, engine: AsyncStorageEngine | None = None):
        self._async_client = AsyncMongoClient(engine)
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
