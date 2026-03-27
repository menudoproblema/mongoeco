import asyncio

from mongoeco.change_streams import ChangeStreamCursor
from mongoeco.api._sync.database_admin import DatabaseAdminService
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
from mongoeco.api._sync.listing_cursor import ListingCursor
from mongoeco.types import (
    BuildInfoDocument,
    CollectionValidationDocument,
    CodecOptions,
    Filter,
    ReadConcern,
    ReadPreference,
    TransactionOptions,
    WriteConcern,
)


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

    def __init__(
        self,
        client: "MongoClient",
        name: str,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ):
        self._client = client
        self._name = name
        self._write_concern = (
            client.write_concern if write_concern is None else write_concern
        )
        self._read_concern = (
            client.read_concern if read_concern is None else read_concern
        )
        self._read_preference = (
            client.read_preference if read_preference is None else read_preference
        )
        self._codec_options = (
            client.codec_options if codec_options is None else codec_options
        )
        self._admin = DatabaseAdminService(self)

    def _async_database(self):
        self._client._ensure_connected()
        return self._client._async_client.get_database(
            self._name,
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
        )

    def __getattr__(self, name: str) -> Collection:
        return self.get_collection(name)

    def __getitem__(self, name: str) -> Collection:
        return self.get_collection(name)

    def get_collection(
        self,
        name: str,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ) -> Collection:
        return Collection(
            self._client,
            self._name,
            name,
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
    ) -> "Database":
        return type(self)(
            self._client,
            self._name,
            write_concern=self._write_concern if write_concern is None else write_concern,
            read_concern=self._read_concern if read_concern is None else read_concern,
            read_preference=self._read_preference if read_preference is None else read_preference,
            codec_options=self._codec_options if codec_options is None else codec_options,
        )

    def list_collection_names(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> list[str]:
        return self._admin.list_collection_names(filter_spec, session=session)

    def list_collections(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> ListingCursor:
        return self._admin.list_collections(filter_spec, session=session)

    def create_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
        **options: object,
    ) -> Collection:
        return self._admin.create_collection(name, session=session, **options)

    def drop_collection(self, name: str, *, session: ClientSession | None = None) -> None:
        self._admin.drop_collection(name, session=session)

    def validate_collection(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> CollectionValidationDocument:
        return self._admin.validate_collection(
            name_or_collection,
            scandata=scandata,
            full=full,
            background=background,
            session=session,
            comment=comment,
        )

    def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        return self._admin.command(command, session=session, **kwargs)

    def watch(
        self,
        pipeline: object | None = None,
        *,
        max_await_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> ChangeStreamCursor:
        return ChangeStreamCursor(
            self._client,
            self._async_database().watch(
                pipeline,
                max_await_time_ms=max_await_time_ms,
                session=session,
            ),
        )

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


class MongoClient:
    """Cliente sincronico que adapta la implementacion async."""

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
        self._async_client = AsyncMongoClient(
            engine,
            mongodb_dialect=mongodb_dialect,
            pymongo_profile=pymongo_profile,
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
            codec_options=codec_options,
            transaction_options=transaction_options,
        )
        self._runner = _SyncRunner()
        self._connected = False
        self._closed = False

    def _run(self, awaitable):
        return self._runner.run(awaitable)

    def _run_resource(self, awaitable, factory):
        self._run(awaitable)
        return factory()

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

    def with_options(
        self,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        transaction_options: TransactionOptions | None = None,
    ) -> "MongoClient":
        return type(self)(
            self._async_client._engine,
            mongodb_dialect=self.mongodb_dialect,
            pymongo_profile=self.pymongo_profile,
            write_concern=self.write_concern if write_concern is None else write_concern,
            read_concern=self.read_concern if read_concern is None else read_concern,
            read_preference=self.read_preference if read_preference is None else read_preference,
            codec_options=self.codec_options if codec_options is None else codec_options,
            transaction_options=(
                self.transaction_options
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
    ) -> Database:
        return Database(
            self,
            name,
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
            codec_options=codec_options,
        )

    def start_session(
        self,
        *,
        default_transaction_options: TransactionOptions | None = None,
    ) -> ClientSession:
        return self._async_client.start_session(
            default_transaction_options=default_transaction_options
        )

    def list_database_names(self, *, session: ClientSession | None = None) -> list[str]:
        self._ensure_connected()
        return self._run(self._async_client.list_database_names(session=session))

    def drop_database(self, name: str, *, session: ClientSession | None = None) -> None:
        self._ensure_connected()
        self._run(self._async_client.drop_database(name, session=session))

    def server_info(self) -> BuildInfoDocument:
        self._ensure_connected()
        return self._run(self._async_client.server_info())

    def watch(
        self,
        pipeline: object | None = None,
        *,
        max_await_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> ChangeStreamCursor:
        self._ensure_connected()
        return ChangeStreamCursor(
            self,
            self._async_client.watch(
                pipeline,
                max_await_time_ms=max_await_time_ms,
                session=session,
            ),
        )

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

    @property
    def write_concern(self) -> WriteConcern:
        return self._async_client.write_concern

    @property
    def read_concern(self) -> ReadConcern:
        return self._async_client.read_concern

    @property
    def read_preference(self) -> ReadPreference:
        return self._async_client.read_preference

    @property
    def codec_options(self) -> CodecOptions:
        return self._async_client.codec_options

    @property
    def transaction_options(self) -> TransactionOptions:
        return self._async_client.transaction_options
