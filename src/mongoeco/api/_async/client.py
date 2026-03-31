from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
from mongoeco.api.public_api import ARG_UNSET
from mongoeco.api._async.database_commands import build_info_document
from mongoeco.change_streams import AsyncChangeStreamCursor, ChangeStreamHub, ChangeStreamScope
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
    resolve_mongodb_dialect_resolution,
    resolve_pymongo_profile_resolution,
)
from mongoeco.driver import (
    AsyncCommandTransport,
    AuthPolicy,
    ConcernPolicy,
    DriverRuntime,
    MongoUri,
    PreparedRequestExecution,
    RequestExecutionResult,
    RequestExecutionPlan,
    SelectionPolicy,
    SrvResolution,
    TlsPolicy,
    TimeoutPolicy,
    TopologyDescription,
    RetryPolicy,
)
from mongoeco.driver.monitoring import DriverMonitor
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.types import (
    BuildInfoDocument,
    CodecOptions,
    CollectionValidationDocument,
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

if TYPE_CHECKING:
    from mongoeco.driver.transports import WireProtocolCommandTransport


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
        change_hub: ChangeStreamHub | None = None,
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
        self._change_hub = change_hub
        self._admin = AsyncDatabaseAdminService(self)

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
            change_hub=self._change_hub,
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
            change_hub=self._change_hub,
        )

    async def list_collection_names(
        self,
        filter_spec: Filter | object = ARG_UNSET,
        *,
        filter: Filter | object = ARG_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> list[str]:
        return await self._admin.list_collection_names(
            filter_spec,
            filter=filter,
            session=session,
            **kwargs,
        )

    def list_collections(
        self,
        filter_spec: Filter | object = ARG_UNSET,
        *,
        filter: Filter | object = ARG_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ):
        return self._admin.list_collections(
            filter_spec,
            filter=filter,
            session=session,
            **kwargs,
        )

    async def create_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
        **options: object,
    ) -> AsyncCollection:
        return await self._admin.create_collection(name, session=session, **options)

    async def drop_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
    ) -> None:
        await self._admin.drop_collection(name, session=session)

    async def validate_collection(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> CollectionValidationDocument:
        return await self._admin.validate_collection(
            name_or_collection,
            scandata=scandata,
            full=full,
            background=background,
            session=session,
            comment=comment,
        )

    async def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        return await self._admin.command(command, session=session, **kwargs)

    def watch(
        self,
        pipeline: object | None = None,
        *,
        max_await_time_ms: int | None = None,
        resume_after: dict[str, object] | None = None,
        start_after: dict[str, object] | None = None,
        start_at_operation_time: int | None = None,
        session: ClientSession | None = None,
    ) -> AsyncChangeStreamCursor:
        del session
        if max_await_time_ms is not None and (
            not isinstance(max_await_time_ms, int)
            or isinstance(max_await_time_ms, bool)
            or max_await_time_ms < 0
        ):
            raise TypeError("max_await_time_ms must be a non-negative integer")
        return AsyncChangeStreamCursor(
            self._change_hub or ChangeStreamHub(),
            scope=ChangeStreamScope(db_name=self._db_name),
            pipeline=pipeline,
            max_await_time_ms=max_await_time_ms,
            resume_after=resume_after,
            start_after=start_after,
            start_at_operation_time=start_at_operation_time,
        )

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
    def name(self) -> str:
        return self._db_name


class AsyncMongoClient:
    """
    Cliente principal para mongoeco.
    """

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
        self._driver_runtime = DriverRuntime(
            uri=uri,
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
        )
        self._write_concern = self._driver_runtime.concern_policy.write_concern
        self._read_concern = self._driver_runtime.concern_policy.read_concern
        self._read_preference = self._driver_runtime.concern_policy.read_preference
        self._change_hub = ChangeStreamHub()

    @staticmethod
    def _create_default_engine() -> AsyncStorageEngine:
        from mongoeco.engines.memory import MemoryEngine

        return MemoryEngine()

    async def __aenter__(self):
        await self._engine.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._driver_runtime.stop_topology_monitoring()
        self._driver_runtime.clear_connections()
        await self._engine.disconnect()

    async def close(self) -> None:
        await self.__aexit__(None, None, None)

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
            uri=self.client_uri.original,
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
            change_hub=self._change_hub,
        )

    def get_default_database(
        self,
        default: str | None = None,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
    ) -> AsyncDatabase:
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

    async def server_info(self) -> BuildInfoDocument:
        return build_info_document(self._mongodb_dialect)

    def plan_command_request(
        self,
        database: str,
        command_name: str,
        payload: dict[str, object],
        *,
        session: ClientSession | None = None,
        read_only: bool = False,
    ) -> RequestExecutionPlan:
        return self._driver_runtime.plan_command_request(
            database,
            command_name,
            payload,
            session=session,
            read_only=read_only,
        )

    async def prepare_command_request_execution(
        self,
        database: str,
        command_name: str,
        payload: dict[str, object],
        *,
        session: ClientSession | None = None,
        read_only: bool = False,
    ) -> PreparedRequestExecution:
        plan = self.plan_command_request(
            database,
            command_name,
            payload,
            session=session,
            read_only=read_only,
        )
        return await self._driver_runtime.prepare_request_execution(plan)

    async def complete_command_request_execution(self, execution: PreparedRequestExecution) -> None:
        await self._driver_runtime.complete_request_execution(execution)

    async def execute_driver_command(
        self,
        database: str,
        command_name: str,
        payload: dict[str, object],
        *,
        session: ClientSession | None = None,
        read_only: bool = False,
        transport: AsyncCommandTransport | None = None,
    ) -> RequestExecutionResult:
        from mongoeco.driver.transports import LocalCommandTransport

        plan = self.plan_command_request(
            database,
            command_name,
            payload,
            session=session,
            read_only=read_only,
        )
        return await self._driver_runtime.execute_request(
            plan,
            LocalCommandTransport(self) if transport is None else transport,
        )

    async def execute_network_command(
        self,
        database: str,
        command_name: str,
        payload: dict[str, object],
        *,
        session: ClientSession | None = None,
        read_only: bool = False,
        transport: WireProtocolCommandTransport | None = None,
    ) -> RequestExecutionResult:
        plan = self.plan_command_request(
            database,
            command_name,
            payload,
            session=session,
            read_only=read_only,
        )
        return await self._driver_runtime.execute_request(
            plan,
            self._driver_runtime.create_network_transport() if transport is None else transport,
        )

    async def refresh_topology(self, *, transport: WireProtocolCommandTransport | None = None) -> TopologyDescription:
        return await self._driver_runtime.refresh_topology(transport=transport)

    async def start_topology_monitoring(
        self,
        *,
        transport: WireProtocolCommandTransport | None = None,
    ) -> None:
        await self._driver_runtime.start_topology_monitoring(transport=transport)

    async def stop_topology_monitoring(self) -> None:
        await self._driver_runtime.stop_topology_monitoring()

    def watch(
        self,
        pipeline: object | None = None,
        *,
        max_await_time_ms: int | None = None,
        resume_after: dict[str, object] | None = None,
        start_after: dict[str, object] | None = None,
        start_at_operation_time: int | None = None,
        session: ClientSession | None = None,
    ) -> AsyncChangeStreamCursor:
        del session
        if max_await_time_ms is not None and (
            not isinstance(max_await_time_ms, int)
            or isinstance(max_await_time_ms, bool)
            or max_await_time_ms < 0
        ):
            raise TypeError("max_await_time_ms must be a non-negative integer")
        return AsyncChangeStreamCursor(
            self._change_hub,
            scope=ChangeStreamScope(),
            pipeline=pipeline,
            max_await_time_ms=max_await_time_ms,
            resume_after=resume_after,
            start_after=start_after,
            start_at_operation_time=start_at_operation_time,
        )

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

    @property
    def client_uri(self) -> MongoUri:
        return self._driver_runtime.uri

    @property
    def topology_description(self) -> TopologyDescription:
        return self._driver_runtime.topology

    @property
    def effective_client_uri(self) -> MongoUri:
        return self._driver_runtime.effective_uri

    @property
    def timeout_policy(self) -> TimeoutPolicy:
        return self._driver_runtime.timeout_policy

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._driver_runtime.retry_policy

    @property
    def selection_policy(self) -> SelectionPolicy:
        return self._driver_runtime.selection_policy

    @property
    def concern_policy(self) -> ConcernPolicy:
        return self._driver_runtime.concern_policy

    @property
    def auth_policy(self) -> AuthPolicy:
        return self._driver_runtime.auth_policy

    @property
    def tls_policy(self) -> TlsPolicy:
        return self._driver_runtime.tls_policy

    @property
    def srv_resolution(self) -> SrvResolution | None:
        return self._driver_runtime.srv_resolution

    @property
    def driver_runtime(self) -> DriverRuntime:
        return self._driver_runtime

    @property
    def driver_monitor(self) -> DriverMonitor:
        return self._driver_runtime.monitor

    @property
    def network_transport(self) -> WireProtocolCommandTransport:
        return self._driver_runtime.create_network_transport()
