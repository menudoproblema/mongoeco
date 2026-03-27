import datetime
import os
import platform
import socket
import sys
import time
from typing import TYPE_CHECKING, Generic, TypeVar
from dataclasses import dataclass

from mongoeco.api.admin_parsing import (
    FindAndModifyCommandOptions,
    normalize_command_document,
    normalize_find_and_modify_options,
    normalize_command_scale,
    normalize_validate_command_options,
    require_collection_name,
)
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    BuildInfoDocument,
    CollectionStatsSnapshot,
    CollectionValidationSnapshot,
    CmdLineOptsDocument,
    CommandCursorResult,
    ConnectionStatusDocument,
    CountCommandResult,
    DatabaseStatsSnapshot,
    DistinctCommandResult,
    FindAndModifyCommandResult,
    HelloDocument,
    HostInfoDocument,
    ListCommandsDocument,
    OkResult,
    ProfilingCommandResult,
    ServerStatusDocument,
    WhatsMyUriDocument,
)

if TYPE_CHECKING:
    from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
    from mongoeco.compat import MongoDialect


_PROCESS_STARTED_AT = datetime.datetime.now(datetime.UTC)
CommandResultT = TypeVar("CommandResultT")

SUPPORTED_DATABASE_COMMANDS: tuple[str, ...] = (
    "aggregate",
    "buildInfo",
    "collStats",
    "connectionStatus",
    "count",
    "create",
    "createIndexes",
    "dbStats",
    "delete",
    "distinct",
    "drop",
    "dropDatabase",
    "dropIndexes",
    "explain",
    "find",
    "findAndModify",
    "getCmdLineOpts",
    "hello",
    "hostInfo",
    "insert",
    "isMaster",
    "ismaster",
    "listCollections",
    "listCommands",
    "listDatabases",
    "listIndexes",
    "ping",
    "profile",
    "renameCollection",
    "serverStatus",
    "update",
    "validate",
    "whatsmyuri",
)


@dataclass(frozen=True, slots=True)
class BuildInfoResult:
    version: str
    version_array: tuple[int, int, int, int]
    git_version: str = "mongoeco"

    def to_document(self) -> BuildInfoDocument:
        return {
            "version": self.version,
            "versionArray": list(self.version_array),
            "gitVersion": self.git_version,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class HelloResult:
    build_info: BuildInfoResult
    legacy_name: bool = False

    def to_document(self) -> HelloDocument:
        document: HelloDocument = {
            "helloOk": True,
            "isWritablePrimary": True,
            "maxBsonObjectSize": 16 * 1024 * 1024,
            "maxMessageSizeBytes": 48_000_000,
            "maxWriteBatchSize": 100_000,
            "localTime": datetime.datetime.now(datetime.UTC),
            "logicalSessionTimeoutMinutes": 30,
            "connectionId": 1,
            "minWireVersion": 0,
            "maxWireVersion": 20,
            "readOnly": False,
            "ok": 1.0,
        }
        if self.legacy_name:
            document["ismaster"] = True
        else:
            document["isWritablePrimary"] = True
        document.update(self.build_info.to_document())
        return document


@dataclass(frozen=True, slots=True)
class ServerStatusResult:
    host: str
    version: str
    process: str
    pid: int
    uptime: float
    uptime_millis: int
    uptime_estimate: int
    local_time: datetime.datetime
    storage_engine_name: str

    def to_document(self) -> ServerStatusDocument:
        return {
            "host": self.host,
            "version": self.version,
            "process": self.process,
            "pid": self.pid,
            "uptime": self.uptime,
            "uptimeMillis": self.uptime_millis,
            "uptimeEstimate": self.uptime_estimate,
            "localTime": self.local_time,
            "connections": {
                "current": 1,
                "available": 8388607,
                "totalCreated": 1,
            },
            "storageEngine": {
                "name": self.storage_engine_name,
            },
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class HostInfoResult:
    hostname: str
    cpu_arch: str
    num_cores: int
    os_type: str
    os_name: str
    os_version: str
    python_version: str

    def to_document(self) -> HostInfoDocument:
        return {
            "system": {
                "hostname": self.hostname,
                "cpuArch": self.cpu_arch,
                "numCores": self.num_cores,
                "memSizeMB": 0,
            },
            "os": {
                "type": self.os_type,
                "name": self.os_name,
                "version": self.os_version,
            },
            "extra": {
                "pythonVersion": self.python_version,
            },
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class WhatsMyUriResult:
    address: str

    def to_document(self) -> WhatsMyUriDocument:
        return {
            "you": self.address,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class CmdLineOptsResult:
    argv: tuple[str, ...]
    bind_ip: str
    port: int

    def to_document(self) -> CmdLineOptsDocument:
        return {
            "argv": list(self.argv),
            "parsed": {
                "net": {"bindIp": self.bind_ip, "port": self.port},
                "storage": {},
            },
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class ListCommandsResult:
    command_names: tuple[str, ...]

    def to_document(self) -> ListCommandsDocument:
        return {
            "commands": {
                command_name: {
                    "help": f"mongoeco local support for the {command_name} command",
                }
                for command_name in self.command_names
            },
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class ConnectionStatusResult:
    show_privileges: bool

    def to_document(self) -> ConnectionStatusDocument:
        auth_info: dict[str, object] = {
            "authenticatedUsers": [],
            "authenticatedUserRoles": [],
        }
        if self.show_privileges:
            auth_info["authenticatedUserPrivileges"] = []
        return {
            "authInfo": auth_info,
            "ok": 1.0,
        }


def build_info_document(mongodb_dialect: "MongoDialect") -> BuildInfoDocument:
    return build_info_result(mongodb_dialect).to_document()


def build_info_result(mongodb_dialect: "MongoDialect") -> BuildInfoResult:
    version_parts = [int(part) for part in mongodb_dialect.server_version.split(".")]
    while len(version_parts) < 2:
        version_parts.append(0)
    major, minor = version_parts[:2]
    return BuildInfoResult(
        version=f"{major}.{minor}.0",
        version_array=(major, minor, 0, 0),
    )


def hello_document(
    mongodb_dialect: "MongoDialect",
    *,
    legacy_name: bool = False,
) -> HelloDocument:
    return hello_result(mongodb_dialect, legacy_name=legacy_name).to_document()


def hello_result(
    mongodb_dialect: "MongoDialect",
    *,
    legacy_name: bool = False,
) -> HelloResult:
    return HelloResult(
        build_info=build_info_result(mongodb_dialect),
        legacy_name=legacy_name,
    )


def _storage_engine_name(engine: AsyncStorageEngine) -> str:
    engine_name = type(engine).__name__.lower()
    if "sqlite" in engine_name:
        return "sqlite"
    if "memory" in engine_name:
        return "memory"
    return type(engine).__name__


def server_status_document(
    mongodb_dialect: "MongoDialect",
    *,
    engine: AsyncStorageEngine,
) -> ServerStatusDocument:
    return server_status_result(mongodb_dialect, engine=engine).to_document()


def server_status_result(
    mongodb_dialect: "MongoDialect",
    *,
    engine: AsyncStorageEngine,
) -> ServerStatusResult:
    local_time = datetime.datetime.now(datetime.UTC)
    uptime_delta = local_time - _PROCESS_STARTED_AT
    uptime_seconds = max(uptime_delta.total_seconds(), 0.0)
    return ServerStatusResult(
        host=socket.gethostname(),
        version=build_info_result(mongodb_dialect).version,
        process="mongod",
        pid=os.getpid(),
        uptime=uptime_seconds,
        uptime_millis=int(uptime_seconds * 1000),
        uptime_estimate=int(uptime_seconds),
        local_time=local_time,
        storage_engine_name=_storage_engine_name(engine),
    )


def host_info_document() -> HostInfoDocument:
    return host_info_result().to_document()


def host_info_result() -> HostInfoResult:
    return HostInfoResult(
        hostname=socket.gethostname(),
        cpu_arch=platform.machine() or "unknown",
        num_cores=os.cpu_count() or 1,
        os_type=platform.system() or "unknown",
        os_name=platform.platform(),
        os_version=platform.version(),
        python_version=platform.python_version(),
    )


def whats_my_uri_document() -> WhatsMyUriDocument:
    return whats_my_uri_result().to_document()


def whats_my_uri_result() -> WhatsMyUriResult:
    return WhatsMyUriResult(address="127.0.0.1:0")


def cmd_line_opts_document() -> CmdLineOptsDocument:
    return cmd_line_opts_result().to_document()


def cmd_line_opts_result() -> CmdLineOptsResult:
    return CmdLineOptsResult(
        argv=tuple(sys.argv),
        bind_ip="127.0.0.1",
        port=0,
    )


def list_commands_document() -> ListCommandsDocument:
    return list_commands_result().to_document()


def list_commands_result() -> ListCommandsResult:
    return ListCommandsResult(command_names=SUPPORTED_DATABASE_COMMANDS)


def connection_status_document(*, show_privileges: bool) -> ConnectionStatusDocument:
    return connection_status_result(show_privileges=show_privileges).to_document()


def connection_status_result(*, show_privileges: bool) -> ConnectionStatusResult:
    return ConnectionStatusResult(show_privileges=show_privileges)


class AsyncDatabaseCommandService:
    @dataclass(frozen=True, slots=True)
    class AdminCommand(Generic[CommandResultT]):
        db_name: str
        command_name: str
        spec: dict[str, object]

    @dataclass(frozen=True, slots=True)
    class StaticAdminCommand(AdminCommand[object]):
        pass

    @dataclass(frozen=True, slots=True)
    class ConnectionStatusCommand(AdminCommand[ConnectionStatusDocument]):
        show_privileges: bool = False

    @dataclass(frozen=True, slots=True)
    class CollectionStatsCommand(AdminCommand[CollectionStatsSnapshot]):
        collection_name: str = ""
        scale: int = 1

    @dataclass(frozen=True, slots=True)
    class DatabaseStatsCommand(AdminCommand[DatabaseStatsSnapshot]):
        scale: int = 1

    @dataclass(frozen=True, slots=True)
    class ProfileCommand(AdminCommand[ProfilingCommandResult]):
        level: int = -1
        slow_ms: int | None = None

    @dataclass(frozen=True, slots=True)
    class ValidateCollectionCommand(AdminCommand[CollectionValidationSnapshot]):
        collection_name: str = ""
        scandata: bool = False
        full: bool = False
        background: bool | None = None

    @dataclass(frozen=True, slots=True)
    class FindAndModifyCommand(AdminCommand[FindAndModifyCommandResult]):
        options: FindAndModifyCommandOptions | None = None

    @dataclass(frozen=True, slots=True)
    class FindCommand(AdminCommand[CommandCursorResult]):
        collection_name: str = ""
        operation: object | None = None

    @dataclass(frozen=True, slots=True)
    class AggregateCommand(AdminCommand[CommandCursorResult]):
        collection_name: str = ""
        operation: object | None = None

    @dataclass(frozen=True, slots=True)
    class CountCommand(AdminCommand[CountCommandResult]):
        collection_name: str = ""
        operation: object | None = None

    @dataclass(frozen=True, slots=True)
    class DistinctCommand(AdminCommand[DistinctCommandResult]):
        collection_name: str = ""
        key: str = ""
        operation: object | None = None

    @dataclass(frozen=True, slots=True)
    class ListIndexesCommand(AdminCommand[CommandCursorResult]):
        collection_name: str = ""
        comment: object | None = None

    @dataclass(frozen=True, slots=True)
    class DelegatedAdminCommand(AdminCommand[object]):
        route: "AsyncDatabaseCommandService.Route | None" = None

    @dataclass(frozen=True, slots=True)
    class Route:
        handler_name: str
        passes_spec: bool = True

    _DELEGATED_COMMAND_HANDLERS: dict[str, Route] = {
        "listCollections": Route("_command_list_collections"),
        "listDatabases": Route("_command_list_databases"),
        "create": Route("_command_create"),
        "drop": Route("_command_drop"),
        "renameCollection": Route("_command_rename_collection"),
        "count": Route("_command_count"),
        "distinct": Route("_command_distinct"),
        "insert": Route("_command_insert"),
        "update": Route("_command_update"),
        "delete": Route("_command_delete"),
        "explain": Route("_command_explain"),
        "createIndexes": Route("_command_create_indexes"),
        "dropIndexes": Route("_command_drop_indexes"),
        "dropDatabase": Route("_command_drop_database", passes_spec=False),
    }

    def __init__(self, admin: "AsyncDatabaseAdminService"):
        self._admin = admin

    @property
    def _engine(self) -> AsyncStorageEngine:
        return self._admin._engine

    @property
    def _mongodb_dialect(self) -> "MongoDialect":
        return self._admin._mongodb_dialect

    def parse_raw_command(
        self,
        command: object,
        **kwargs: object,
    ) -> "AsyncDatabaseCommandService.AdminCommand[object]":
        spec = normalize_command_document(command, kwargs)
        command_name = next(iter(spec))
        if not isinstance(command_name, str):
            raise TypeError("command name must be a string")

        if command_name in {
            "ping",
            "buildInfo",
            "serverStatus",
            "hostInfo",
            "whatsmyuri",
            "getCmdLineOpts",
            "hello",
            "isMaster",
            "ismaster",
            "listCommands",
        }:
            return self.StaticAdminCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
            )
        if command_name == "connectionStatus":
            show_privileges = spec.get("showPrivileges", False)
            if not isinstance(show_privileges, bool):
                raise TypeError("showPrivileges must be a bool")
            return self.ConnectionStatusCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                show_privileges=show_privileges,
            )
        if command_name == "collStats":
            collection_name = require_collection_name(
                spec.get("collStats"),
                "collStats",
            )
            scale = normalize_command_scale(spec.get("scale"))
            return self.CollectionStatsCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                scale=scale,
            )
        if command_name == "dbStats":
            scale = normalize_command_scale(spec.get("scale"))
            return self.DatabaseStatsCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                scale=scale,
            )
        if command_name == "profile":
            level = spec.get("profile")
            if not isinstance(level, int) or isinstance(level, bool):
                raise TypeError("profile level must be an integer")
            slow_ms = spec.get("slowms")
            if slow_ms is not None and (
                not isinstance(slow_ms, int) or isinstance(slow_ms, bool)
            ):
                raise TypeError("slowms must be an integer")
            return self.ProfileCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                level=level,
                slow_ms=slow_ms,
            )
        if command_name == "validate":
            collection_name = require_collection_name(
                spec.get("validate"),
                "validate",
            )
            options = normalize_validate_command_options(
                scandata=spec.get("scandata"),
                full=spec.get("full"),
                background=spec.get("background"),
                comment=spec.get("comment"),
            )
            return self.ValidateCollectionCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                scandata=options.scandata,
                full=options.full,
                background=options.background,
            )
        if command_name == "findAndModify":
            return self.FindAndModifyCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                options=normalize_find_and_modify_options(spec),
            )
        if command_name == "find":
            collection_name, operation = self._admin._compile_command_find_operation(
                spec,
                collection_field="find",
            )
            return self.FindCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                operation=operation,
            )
        if command_name == "aggregate":
            collection_name, operation = self._admin._compile_command_aggregate_operation(spec)
            return self.AggregateCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                operation=operation,
            )
        if command_name == "count":
            collection_name, operation = self._admin._compile_command_count_operation(spec)
            return self.CountCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                operation=operation,
            )
        if command_name == "distinct":
            collection_name, key, operation = self._admin._compile_command_distinct_operation(
                spec
            )
            return self.DistinctCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                key=key,
                operation=operation,
            )
        if command_name == "listIndexes":
            return self.ListIndexesCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=require_collection_name(
                    spec.get("listIndexes"),
                    "listIndexes",
                ),
                comment=spec.get("comment"),
            )

        route = self._DELEGATED_COMMAND_HANDLERS.get(command_name)
        if route is None:
            raise OperationFailure(f"Unsupported command: {command_name}")
        return self.DelegatedAdminCommand(
            db_name=self._admin._db_name,
            command_name=command_name,
            spec=spec,
            route=route,
        )

    async def execute(
        self,
        command: "AsyncDatabaseCommandService.AdminCommand[CommandResultT]",
        *,
        session: ClientSession | None = None,
    ) -> CommandResultT:
        if isinstance(command, self.StaticAdminCommand):
            return self._execute_static(command)  # type: ignore[return-value]
        if isinstance(command, self.ConnectionStatusCommand):
            return connection_status_result(
                show_privileges=command.show_privileges
            )  # type: ignore[return-value]
        if isinstance(command, self.CollectionStatsCommand):
            return await self._admin._collection_stats(
                command.collection_name,
                scale=command.scale,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.DatabaseStatsCommand):
            return await self._admin._database_stats(
                scale=command.scale,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.ProfileCommand):
            return await self._engine.set_profiling_level(
                command.db_name,
                command.level,
                slow_ms=command.slow_ms,
                context=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.ValidateCollectionCommand):
            return await self._admin._build_collection_validation_snapshot(
                command.collection_name,
                scandata=command.scandata,
                full=command.full,
                background=command.background,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.FindAndModifyCommand):
            assert command.options is not None
            return await self._admin._execute_find_and_modify(
                command.options,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.FindCommand):
            return await self._admin._execute_find_command(
                command.collection_name,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.AggregateCommand):
            return await self._admin._execute_aggregate_command(
                command.collection_name,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.CountCommand):
            return await self._admin._execute_count_command(
                command.collection_name,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.DistinctCommand):
            return await self._admin._execute_distinct_command(
                command.collection_name,
                command.key,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.ListIndexesCommand):
            return await self._admin._execute_list_indexes_command(
                command.collection_name,
                comment=command.comment,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.DelegatedAdminCommand):
            assert command.route is not None
            handler = getattr(self._admin, command.route.handler_name)
            if command.route.passes_spec:
                return await handler(command.spec, session=session)
            return await handler(session=session)
        raise AssertionError(f"Unexpected admin command type: {type(command)!r}")

    def _execute_static(
        self,
        command: "AsyncDatabaseCommandService.StaticAdminCommand",
    ) -> object:
        if command.command_name == "ping":
            return OkResult()
        if command.command_name == "buildInfo":
            return build_info_result(self._mongodb_dialect)
        if command.command_name == "serverStatus":
            return server_status_result(
                self._mongodb_dialect,
                engine=self._engine,
            )
        if command.command_name == "hostInfo":
            return host_info_result()
        if command.command_name == "whatsmyuri":
            return whats_my_uri_result()
        if command.command_name == "getCmdLineOpts":
            return cmd_line_opts_result()
        if command.command_name in {"hello", "isMaster", "ismaster"}:
            return hello_result(
                self._mongodb_dialect,
                legacy_name=command.command_name != "hello",
            )
        if command.command_name == "listCommands":
            return list_commands_result()
        raise AssertionError(f"Unexpected static admin command: {command.command_name}")

    @staticmethod
    def serialize_result(result: object) -> dict[str, object]:
        to_document = getattr(result, "to_document", None)
        if callable(to_document):
            return to_document()
        if isinstance(result, dict):
            return result
        raise TypeError(f"Unsupported admin command result type: {type(result)!r}")

    async def execute_document(
        self,
        command: object | "AsyncDatabaseCommandService.AdminCommand[object]",
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        parsed = (
            command
            if isinstance(command, self.AdminCommand)
            else self.parse_raw_command(command, **kwargs)
        )
        started_at = time.perf_counter_ns()
        try:
            serialized = self.serialize_result(await self.execute(parsed, session=session))
        except Exception as exc:
            recorder = getattr(self._engine, "_record_profile_event", None)
            if callable(recorder) and parsed.command_name != "profile":
                recorder(
                    parsed.db_name,
                    op="command",
                    command=dict(parsed.spec),
                    duration_micros=max(1, (time.perf_counter_ns() - started_at) // 1000),
                    ok=0.0,
                    errmsg=str(exc),
                )
            raise
        recorder = getattr(self._engine, "_record_profile_event", None)
        if callable(recorder) and parsed.command_name != "profile":
            recorder(
                parsed.db_name,
                op="command",
                command=dict(parsed.spec),
                duration_micros=max(1, (time.perf_counter_ns() - started_at) // 1000),
            )
        return serialized

    async def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        return await self.execute_document(
            command,
            session=session,
            **kwargs,
        )
