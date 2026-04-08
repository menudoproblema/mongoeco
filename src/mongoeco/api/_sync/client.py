from __future__ import annotations

import asyncio
import inspect
import threading
from typing import TYPE_CHECKING

from mongoeco.api.public_api import (
    ARG_UNSET,
    DATABASE_LIST_COLLECTION_NAMES_SPEC,
    DATABASE_LIST_COLLECTIONS_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.change_streams import ChangeStreamCursor
from mongoeco.api._sync.database_admin import DatabaseAdminService
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
)
from mongoeco.driver import AsyncCommandTransport, RequestExecutionResult, TopologyDescription
from mongoeco.driver.monitoring import DriverMonitor
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import ExecutionTimeout, InvalidOperation, ServerSelectionTimeoutError
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

if TYPE_CHECKING:
    from mongoeco.driver.transports import WireProtocolCommandTransport

_FILTER_UNSET = ARG_UNSET


class _SyncRunner:
    """Ejecuta la API async en un loop dedicado y estable."""

    def __init__(self):
        self._runner = asyncio.Runner()
        self._closed = False
        self._closing = False
        self._state_condition = threading.Condition()
        self._active_runs = 0
        self._runner_lock = threading.Lock()

    def _run_direct(self, awaitable):
        try:
            with self._runner_lock:
                return self._runner.run(awaitable)
        except ExecutionTimeout as exc:
            raise ExecutionTimeout(
                f"sync operation timed out: {exc}",
                code=exc.code,
                details=exc.details,
                error_labels=exc.error_labels,
            ) from exc
        except ServerSelectionTimeoutError as exc:
            raise ServerSelectionTimeoutError(f"sync server selection timed out: {exc}") from exc

    @staticmethod
    def _rethrow_helper_error(outcome: dict[str, object]) -> None:
        error = outcome.get("error")
        if isinstance(error, BaseException):
            raise error

    def _invoke_on_helper_thread(self, operation):
        outcome: dict[str, object] = {}
        done = threading.Event()

        def _worker() -> None:
            try:
                outcome["result"] = operation()
            except BaseException as exc:  # pragma: no cover - rethrown synchronously below
                outcome["error"] = exc
            finally:
                done.set()

        worker = threading.Thread(
            target=_worker,
            name="mongoeco-sync-runner-helper",
            daemon=True,
        )
        worker.start()
        done.wait()
        self._rethrow_helper_error(outcome)
        return outcome.get("result")

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
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=0.25,
                )
            except TimeoutError:
                pass

        with self._runner_lock:
            self._runner.run(_drain_pending())
        shutdown_asyncgens = getattr(loop, "shutdown_asyncgens", None)
        if callable(shutdown_asyncgens):
            with self._runner_lock:
                self._runner.run(shutdown_asyncgens())
        shutdown_default_executor = getattr(loop, "shutdown_default_executor", None)
        if callable(shutdown_default_executor):
            with self._runner_lock:
                self._runner.run(shutdown_default_executor())

    def run(self, awaitable):
        with self._state_condition:
            if self._closed or self._closing:
                raise InvalidOperation("El cliente sincronico ya esta cerrado")
            self._active_runs += 1

        try:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return self._run_direct(awaitable)

            return self._invoke_on_helper_thread(lambda: self._run_direct(awaitable))
        finally:
            with self._state_condition:
                self._active_runs -= 1
                self._state_condition.notify_all()

    def _close_runner_resources(self) -> None:
        self._cleanup_pending_tasks()
        close_runner = getattr(self._runner, "close", None)
        if not callable(close_runner):
            return
        with self._runner_lock:
            close_runner()

    def close(self) -> None:
        with self._state_condition:
            if self._closed:
                return
            self._closing = True
            while self._active_runs > 0:
                self._state_condition.wait()
        if not self._closed:
            try:
                try:
                    asyncio.get_running_loop()
                    running_loop_active = True
                except RuntimeError:
                    running_loop_active = False

                if running_loop_active:
                    self._invoke_on_helper_thread(self._close_runner_resources)
                else:
                    self._close_runner_resources()
            finally:
                with self._state_condition:
                    self._closed = True
                    self._closing = False
                    self._state_condition.notify_all()

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
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> list[str]:
        options = normalize_public_operation_arguments(
            DATABASE_LIST_COLLECTION_NAMES_SPEC,
            explicit={"filter_spec": filter_spec, "session": session},
            extra_kwargs={"filter": filter, **kwargs},
        )
        return self._admin.list_collection_names(
            options.get("filter_spec", _FILTER_UNSET),
            session=options.get("session"),
        )

    def list_collections(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> ListingCursor:
        options = normalize_public_operation_arguments(
            DATABASE_LIST_COLLECTIONS_SPEC,
            explicit={"filter_spec": filter_spec, "session": session},
            extra_kwargs={"filter": filter, **kwargs},
        )
        return self._admin.list_collections(
            options.get("filter_spec", _FILTER_UNSET),
            session=options.get("session"),
        )

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
        resume_after: dict[str, object] | None = None,
        start_after: dict[str, object] | None = None,
        start_at_operation_time: int | None = None,
        full_document: str = "default",
        session: ClientSession | None = None,
    ) -> ChangeStreamCursor:
        return ChangeStreamCursor(
            self._client,
            self._async_database().watch(
                pipeline,
                max_await_time_ms=max_await_time_ms,
                resume_after=resume_after,
                start_after=start_after,
                start_at_operation_time=start_at_operation_time,
                full_document=full_document,
                session=session,
            ),
        )

    def change_stream_state(self) -> dict[str, object]:
        return self._async_database().change_stream_state()

    def change_stream_backend_info(self) -> dict[str, object]:
        return self._async_database().change_stream_backend_info()

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

    @property
    def name(self) -> str:
        return self._name

    @property
    def change_stream_history_size(self) -> int | None:
        return self._client.change_stream_history_size

    @property
    def change_stream_journal_path(self) -> str | None:
        return self._client.change_stream_journal_path

    @property
    def change_stream_journal_fsync(self) -> bool:
        return self._client.change_stream_journal_fsync

    @property
    def change_stream_journal_max_bytes(self) -> int | None:
        return self._client.change_stream_journal_max_bytes


class MongoClient:
    """Cliente sincronico que adapta la implementacion async."""

    def __init__(
        self,
        engine: AsyncStorageEngine | None = None,
        *,
        uri: str | None = None,
        mongodb_dialect: MongoDialect | str | None = None,
        pymongo_profile: PyMongoProfile | str | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        transaction_options: TransactionOptions | None = None,
        change_stream_history_size: int | None = 10_000,
        change_stream_journal_path: str | None = None,
        change_stream_journal_fsync: bool = False,
        change_stream_journal_max_bytes: int | None = 1_048_576,
    ):
        self._async_client = AsyncMongoClient(
            engine,
            uri=uri,
            mongodb_dialect=mongodb_dialect,
            pymongo_profile=pymongo_profile,
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
            codec_options=codec_options,
            transaction_options=transaction_options,
            change_stream_history_size=change_stream_history_size,
            change_stream_journal_path=change_stream_journal_path,
            change_stream_journal_fsync=change_stream_journal_fsync,
            change_stream_journal_max_bytes=change_stream_journal_max_bytes,
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
            uri=self.client_uri.original,
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
            change_stream_history_size=self.change_stream_history_size,
            change_stream_journal_path=self.change_stream_journal_path,
            change_stream_journal_fsync=self.change_stream_journal_fsync,
            change_stream_journal_max_bytes=self.change_stream_journal_max_bytes,
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

    def get_default_database(
        self,
        default: str | None = None,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ) -> Database:
        name = self.client_uri.default_database or default
        if not isinstance(name, str) or not name:
            raise InvalidOperation("No default database name defined or provided")
        return self.get_database(
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
        causal_consistency: bool = True,
    ) -> ClientSession:
        return self._async_client.start_session(
            default_transaction_options=default_transaction_options,
            causal_consistency=causal_consistency,
        )

    def with_transaction(
        self,
        callback,
        *args,
        read_concern: ReadConcern | None = None,
        write_concern: WriteConcern | None = None,
        read_preference: ReadPreference | None = None,
        max_commit_time_ms: int | None = None,
        default_transaction_options: TransactionOptions | None = None,
        causal_consistency: bool = True,
        **kwargs: object,
    ) -> object:
        self._ensure_connected()
        session = self.start_session(
            default_transaction_options=default_transaction_options,
            causal_consistency=causal_consistency,
        )
        try:
            result = session.with_transaction(
                callback,
                *args,
                read_concern=read_concern,
                write_concern=write_concern,
                read_preference=read_preference,
                max_commit_time_ms=max_commit_time_ms,
                **kwargs,
            )
            if inspect.isawaitable(result):
                return self._runner.run(result)
            return result
        finally:
            session.close()

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
        resume_after: dict[str, object] | None = None,
        start_after: dict[str, object] | None = None,
        start_at_operation_time: int | None = None,
        full_document: str = "default",
        session: ClientSession | None = None,
    ) -> ChangeStreamCursor:
        self._ensure_connected()
        return ChangeStreamCursor(
            self,
            self._async_client.watch(
                pipeline,
                max_await_time_ms=max_await_time_ms,
                resume_after=resume_after,
                start_after=start_after,
                start_at_operation_time=start_at_operation_time,
                full_document=full_document,
                session=session,
            ),
        )

    def change_stream_state(self) -> dict[str, object]:
        self._ensure_connected()
        return self._async_client.change_stream_state()

    def change_stream_backend_info(self) -> dict[str, object]:
        self._ensure_connected()
        return self._async_client.change_stream_backend_info()

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

    @property
    def change_stream_history_size(self) -> int | None:
        return self._async_client.change_stream_history_size

    @property
    def change_stream_journal_path(self) -> str | None:
        return self._async_client.change_stream_journal_path

    @property
    def change_stream_journal_fsync(self) -> bool:
        return self._async_client.change_stream_journal_fsync

    @property
    def change_stream_journal_max_bytes(self) -> int | None:
        return self._async_client.change_stream_journal_max_bytes

    @property
    def client_uri(self):
        return self._async_client.client_uri

    @property
    def topology_description(self):
        return self._async_client.topology_description

    def sdam_capabilities(self) -> dict[str, object]:
        return self._async_client.sdam_capabilities()

    @property
    def effective_client_uri(self):
        return self._async_client.effective_client_uri

    @property
    def timeout_policy(self):
        return self._async_client.timeout_policy

    @property
    def retry_policy(self):
        return self._async_client.retry_policy

    @property
    def selection_policy(self):
        return self._async_client.selection_policy

    @property
    def concern_policy(self):
        return self._async_client.concern_policy

    @property
    def auth_policy(self):
        return self._async_client.auth_policy

    @property
    def tls_policy(self):
        return self._async_client.tls_policy

    @property
    def srv_resolution(self):
        return self._async_client.srv_resolution

    @property
    def driver_runtime(self):
        return self._async_client.driver_runtime

    @property
    def driver_monitor(self) -> DriverMonitor:
        return self._async_client.driver_monitor

    def execute_driver_command(
        self,
        database: str,
        command_name: str,
        payload: dict[str, object],
        *,
        session: ClientSession | None = None,
        read_only: bool = False,
        transport: AsyncCommandTransport | None = None,
    ) -> RequestExecutionResult:
        self._ensure_connected()
        return self._runner.run(
            self._async_client.execute_driver_command(
                database,
                command_name,
                payload,
                session=session,
                read_only=read_only,
                transport=transport,
            )
        )

    def execute_network_command(
        self,
        database: str,
        command_name: str,
        payload: dict[str, object],
        *,
        session: ClientSession | None = None,
        read_only: bool = False,
        transport: WireProtocolCommandTransport | None = None,
    ) -> RequestExecutionResult:
        self._ensure_connected()
        return self._runner.run(
            self._async_client.execute_network_command(
                database,
                command_name,
                payload,
                session=session,
                read_only=read_only,
                transport=transport,
            )
        )

    def refresh_topology(self, *, transport: WireProtocolCommandTransport | None = None) -> TopologyDescription:
        self._ensure_connected()
        return self._runner.run(self._async_client.refresh_topology(transport=transport))

    def start_topology_monitoring(
        self,
        *,
        transport: WireProtocolCommandTransport | None = None,
    ) -> None:
        self._ensure_connected()
        self._runner.run(self._async_client.start_topology_monitoring(transport=transport))

    def stop_topology_monitoring(self) -> None:
        self._runner.run(self._async_client.stop_topology_monitoring())

    @property
    def network_transport(self) -> WireProtocolCommandTransport:
        return self._async_client.network_transport
