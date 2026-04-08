from __future__ import annotations

import asyncio
import datetime
import os
import platform
import socket
import sys
import threading
import time
from contextlib import contextmanager
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
from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.api._async._database_command_contract import (
    admin_family_counts,
    explainable_command_count,
    list_commands_document_payload,
    wire_command_surface_count,
)
from mongoeco.core.collation import collation_backend_info
from mongoeco.core.json_compat import get_json_backend_name
from mongoeco.driver.topology import sdam_capabilities_info
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import ConnectionFailure, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    BuildInfoDocument,
    CollectionStatsSnapshot,
    CollectionValidationSnapshot,
    CmdLineOptsDocument,
    CommandCursorResult,
    ConnectionStatusDocument,
    CountCommandResult,
    DatabaseHashCommandResult,
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
_FAIL_COMMAND_DEFAULT_CODE = 10107
_FAIL_COMMAND_DEFAULT_MESSAGE = "failCommand failpoint triggered"


@contextmanager
def _null_operation_tracker():
    yield None

SUPPORTED_DATABASE_COMMANDS: tuple[str, ...] = (
    "aggregate",
    "buildInfo",
    "collStats",
    "configureFailPoint",
    "connectionStatus",
    "count",
    "create",
    "createIndexes",
    "currentOp",
    "dbHash",
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
    "killOp",
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
    admin_command_surface_count: int
    wire_command_surface_count: int
    dialect_version: str
    json_backend: str
    admin_family_counts: dict[str, int]
    explainable_command_count: int
    collation_info: dict[str, object]
    sdam_info: dict[str, object]
    change_stream_info: dict[str, object]
    engine_runtime_info: dict[str, object]
    opcounters: dict[str, int]
    profiler_tracked_databases: int = 0
    profiler_visible_namespaces: int = 0
    profiler_entry_count: int = 0

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
            "asserts": {
                "regular": 0,
                "warning": 0,
                "msg": 0,
                "user": 0,
                "rollovers": 0,
            },
            "opcounters": {
                "insert": int(self.opcounters.get("insert", 0)),
                "query": int(self.opcounters.get("query", 0)),
                "update": int(self.opcounters.get("update", 0)),
                "delete": int(self.opcounters.get("delete", 0)),
                "getmore": int(self.opcounters.get("getmore", 0)),
                "command": int(self.opcounters.get("command", 0)),
            },
            "mongoeco": {
                "embedded": True,
                "dialectVersion": self.dialect_version,
                "adminCommandSurfaceCount": self.admin_command_surface_count,
                "wireCommandSurfaceCount": self.wire_command_surface_count,
                "adminFamilies": dict(self.admin_family_counts),
                "explainableCommandCount": self.explainable_command_count,
                "jsonBackend": self.json_backend,
                "collation": dict(self.collation_info),
                "sdam": dict(self.sdam_info),
                "changeStreams": dict(self.change_stream_info),
                "engineRuntime": dict(self.engine_runtime_info),
                "profile": {
                    "trackedDatabases": self.profiler_tracked_databases,
                    "visibleNamespaces": self.profiler_visible_namespaces,
                    "entryCount": self.profiler_entry_count,
                },
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
            "commands": list_commands_document_payload(self.command_names),
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


@dataclass(slots=True)
class _FailCommandInjection:
    command_names: frozenset[str]
    error_code: int
    error_message: str
    error_labels: tuple[str, ...]
    namespaces: frozenset[str] | None = None
    close_connection: bool = False
    block_time_ms: int = 0
    remaining_times: int | None = None


class _CommandFailPointState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._fail_command: _FailCommandInjection | None = None

    def configure_fail_command(
        self,
        *,
        mode: str,
        times: int | None,
        fail_commands: tuple[str, ...],
        error_code: int,
        error_message: str,
        error_labels: tuple[str, ...],
        namespaces: tuple[str, ...] | None = None,
        close_connection: bool = False,
        block_time_ms: int = 0,
    ) -> dict[str, object]:
        with self._lock:
            if mode == "off":
                self._fail_command = None
                return {
                    "ok": 1.0,
                    "failPoint": "failCommand",
                    "mode": "off",
                    "enabled": False,
                }
            normalized_namespaces = (
                frozenset(namespaces) if namespaces is not None else None
            )
            self._fail_command = _FailCommandInjection(
                command_names=frozenset(fail_commands),
                error_code=error_code,
                error_message=error_message,
                error_labels=error_labels,
                namespaces=normalized_namespaces,
                close_connection=close_connection,
                block_time_ms=block_time_ms,
                remaining_times=times if mode == "times" else None,
            )
            data: dict[str, object] = {
                "failCommands": list(fail_commands),
                "errorCode": error_code,
                "errorMessage": error_message,
                "errorLabels": list(error_labels),
                "closeConnection": close_connection,
                "blockConnection": block_time_ms > 0,
                "blockTimeMS": block_time_ms,
            }
            if normalized_namespaces:
                sorted_namespaces = sorted(normalized_namespaces)
                if len(sorted_namespaces) == 1:
                    data["namespace"] = sorted_namespaces[0]
                else:
                    data["namespaces"] = sorted_namespaces
            return {
                "ok": 1.0,
                "failPoint": "failCommand",
                "mode": {"times": times} if mode == "times" else "alwaysOn",
                "enabled": True,
                "data": data,
            }

    def consume_fail_command(
        self,
        command_name: str,
        *,
        namespace: str | None = None,
    ) -> _FailCommandInjection | None:
        with self._lock:
            fail_command = self._fail_command
            if fail_command is None or command_name not in fail_command.command_names:
                return None
            if (
                fail_command.namespaces is not None
                and namespace not in fail_command.namespaces
            ):
                return None
            consumed = _FailCommandInjection(
                command_names=fail_command.command_names,
                error_code=fail_command.error_code,
                error_message=fail_command.error_message,
                error_labels=fail_command.error_labels,
                namespaces=fail_command.namespaces,
                close_connection=fail_command.close_connection,
                block_time_ms=fail_command.block_time_ms,
                remaining_times=fail_command.remaining_times,
            )
            if fail_command.remaining_times is not None:
                fail_command.remaining_times -= 1
                if fail_command.remaining_times <= 0:
                    self._fail_command = None
            return consumed


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
    change_stream_info: dict[str, object] | None = None,
) -> ServerStatusDocument:
    return server_status_result(
        mongodb_dialect,
        engine=engine,
        change_stream_info=change_stream_info,
    ).to_document()


def server_status_result(
    mongodb_dialect: "MongoDialect",
    *,
    engine: AsyncStorageEngine,
    change_stream_info: dict[str, object] | None = None,
) -> ServerStatusResult:
    local_time = datetime.datetime.now(datetime.UTC)
    uptime_delta = local_time - _PROCESS_STARTED_AT
    uptime_seconds = max(uptime_delta.total_seconds(), 0.0)
    profiler = getattr(engine, "_profiler", None)
    profile_status = (
        profiler.status_snapshot()
        if profiler is not None and hasattr(profiler, "status_snapshot")
        else {"tracked_databases": 0, "visible_namespaces": 0, "entry_count": 0}
    )
    resolved_change_stream_info = (
        {
            "implementation": "local",
            "persistent": False,
            "boundedHistory": True,
            "maxRetainedEvents": 10_000,
            "journalEnabled": False,
            "journalFsync": False,
            "journalMaxLogBytes": 1_048_576,
            "retainedEvents": 0,
            "currentOffset": 0,
            "nextToken": 1,
        }
        if change_stream_info is None
        else dict(change_stream_info)
    )
    runtime_info_fn = getattr(engine, "_runtime_diagnostics_info", None)
    engine_runtime_info = (
        dict(runtime_info_fn())
        if callable(runtime_info_fn)
        else {}
    )
    runtime_opcounters_fn = getattr(engine, "_runtime_opcounters_snapshot", None)
    runtime_opcounters = (
        runtime_opcounters_fn()
        if callable(runtime_opcounters_fn)
        else {"insert": 0, "query": 0, "update": 0, "delete": 0, "getmore": 0, "command": 0}
    )
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
        admin_command_surface_count=len(SUPPORTED_DATABASE_COMMANDS),
        wire_command_surface_count=wire_command_surface_count(),
        dialect_version=mongodb_dialect.server_version,
        json_backend=get_json_backend_name(),
        admin_family_counts=admin_family_counts(),
        explainable_command_count=explainable_command_count(),
        collation_info=collation_backend_info().to_document(),
        sdam_info=sdam_capabilities_info().to_document(),
        change_stream_info=resolved_change_stream_info,
        engine_runtime_info=engine_runtime_info,
        opcounters=runtime_opcounters,
        profiler_tracked_databases=int(profile_status["tracked_databases"]),
        profiler_visible_namespaces=int(profile_status["visible_namespaces"]),
        profiler_entry_count=int(profile_status["entry_count"]),
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


class _LegacyAdminCommandCompilerAdapter:
    def __init__(self, admin) -> None:
        self._admin = admin

    def compile_find_operation(self, spec: dict[str, object], **kwargs):
        return self._admin._compile_command_find_operation(spec, **kwargs)

    def compile_aggregate_operation(self, spec: dict[str, object], **kwargs):
        return self._admin._compile_command_aggregate_operation(spec, **kwargs)

    def compile_count_operation(self, spec: dict[str, object]):
        return self._admin._compile_command_count_operation(spec)

    def compile_distinct_operation(self, spec: dict[str, object]):
        return self._admin._compile_command_distinct_operation(spec)


class _LegacyAdminRoutingAdapter:
    def __init__(self, admin) -> None:
        self._admin = admin

    async def execute_find_and_modify(self, options, *, session=None):
        return await self._admin._execute_find_and_modify(options, session=session)

    async def execute_find_command(self, collection_name: str, operation, *, session=None):
        return await self._admin._execute_find_command(collection_name, operation, session=session)

    async def execute_aggregate_command(self, collection_name: str, operation, *, session=None):
        return await self._admin._execute_aggregate_command(collection_name, operation, session=session)

    async def execute_count_command(self, collection_name: str, operation, *, session=None):
        return await self._admin._execute_count_command(collection_name, operation, session=session)

    async def execute_db_hash_command(self, collections: tuple[str, ...], *, comment=None, session=None):
        return await self._admin._execute_db_hash_command(
            collections,
            comment=comment,
            session=session,
        )

    async def execute_distinct_command(self, collection_name: str, key: str, operation, *, session=None):
        return await self._admin._execute_distinct_command(collection_name, key, operation, session=session)

    async def execute_list_indexes_command(self, collection_name: str, *, comment=None, session=None):
        return await self._admin._execute_list_indexes_command(collection_name, comment=comment, session=session)

    def __getattr__(self, name: str):
        return getattr(self._admin, name)


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
    class DatabaseHashCommand(AdminCommand[DatabaseHashCommandResult]):
        collections: tuple[str, ...] = ()

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
    class CurrentOpCommand(AdminCommand[object]):
        pass

    @dataclass(frozen=True, slots=True)
    class KillOpCommand(AdminCommand[object]):
        opid: str = ""

    @dataclass(frozen=True, slots=True)
    class ConfigureFailPointCommand(AdminCommand[object]):
        mode: str = "off"
        times: int | None = None
        fail_commands: tuple[str, ...] = ()
        error_code: int = _FAIL_COMMAND_DEFAULT_CODE
        error_message: str = _FAIL_COMMAND_DEFAULT_MESSAGE
        error_labels: tuple[str, ...] = ()
        namespaces: tuple[str, ...] | None = None
        close_connection: bool = False
        block_time_ms: int = 0

    @dataclass(frozen=True, slots=True)
    class DelegatedAdminCommand(AdminCommand[object]):
        route: AsyncDatabaseCommandService.Route | None = None

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
        "dbHash": Route("_command_db_hash"),
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

    @property
    def _failpoints(self) -> _CommandFailPointState:
        state = getattr(self._engine, "_command_failpoint_state", None)
        if isinstance(state, _CommandFailPointState):
            return state
        state = _CommandFailPointState()
        setattr(self._engine, "_command_failpoint_state", state)
        return state

    @staticmethod
    def _normalize_fail_point_mode(mode_spec: object) -> tuple[str, int | None]:
        if mode_spec == "off":
            return "off", None
        if mode_spec == "alwaysOn":
            return "alwaysOn", None
        if isinstance(mode_spec, dict):
            if set(mode_spec) != {"times"}:
                raise TypeError(
                    "configureFailPoint mode must be 'off', 'alwaysOn' or {'times': <int>}"
                )
            times = mode_spec.get("times")
            if not isinstance(times, int) or isinstance(times, bool) or times <= 0:
                raise TypeError(
                    "configureFailPoint mode.times must be a positive integer"
                )
            return "times", times
        raise TypeError(
            "configureFailPoint mode must be 'off', 'alwaysOn' or {'times': <int>}"
        )

    @staticmethod
    def _normalize_fail_command_data(
        data_spec: object | None,
        *,
        required: bool,
        db_name: str,
    ) -> tuple[
        tuple[str, ...],
        int,
        str,
        tuple[str, ...],
        tuple[str, ...] | None,
        bool,
        int,
    ]:
        if data_spec is None:
            if required:
                raise TypeError(
                    "configureFailPoint failCommand requires a data document"
                )
            return (
                (),
                _FAIL_COMMAND_DEFAULT_CODE,
                _FAIL_COMMAND_DEFAULT_MESSAGE,
                (),
                None,
                False,
                0,
            )
        if not isinstance(data_spec, dict):
            raise TypeError("configureFailPoint data must be a document")

        fail_commands_spec = data_spec.get("failCommands")
        if (
            not isinstance(fail_commands_spec, list | tuple)
            or not fail_commands_spec
            or any(
                not isinstance(command_name, str) or not command_name
                for command_name in fail_commands_spec
            )
        ):
            raise TypeError(
                "configureFailPoint data.failCommands must be a non-empty list of command names"
            )
        fail_commands = tuple(dict.fromkeys(fail_commands_spec))

        error_code = data_spec.get("errorCode", _FAIL_COMMAND_DEFAULT_CODE)
        if not isinstance(error_code, int) or isinstance(error_code, bool):
            raise TypeError("configureFailPoint data.errorCode must be an integer")

        error_message = data_spec.get("errorMessage", _FAIL_COMMAND_DEFAULT_MESSAGE)
        if not isinstance(error_message, str) or not error_message:
            raise TypeError(
                "configureFailPoint data.errorMessage must be a non-empty string"
            )

        labels_spec = data_spec.get("errorLabels", ())
        if labels_spec is None:
            labels_spec = ()
        if not isinstance(labels_spec, list | tuple) or any(
            not isinstance(label, str) or not label for label in labels_spec
        ):
            raise TypeError("configureFailPoint data.errorLabels must be a list of strings")
        error_labels = tuple(labels_spec)
        namespace = data_spec.get("namespace")
        namespaces_spec = data_spec.get("namespaces")
        if namespace is not None and namespaces_spec is not None:
            raise TypeError(
                "configureFailPoint data.namespace and data.namespaces are mutually exclusive"
            )

        namespaces: tuple[str, ...] | None = None
        if namespace is not None:
            if not isinstance(namespace, str) or not namespace:
                raise TypeError(
                    "configureFailPoint data.namespace must be a non-empty string"
                )
            if "." not in namespace:
                namespace = f"{db_name}.{namespace}"
            namespaces = (namespace,)
        elif namespaces_spec is not None:
            if (
                not isinstance(namespaces_spec, list | tuple)
                or not namespaces_spec
                or any(
                    not isinstance(namespace_name, str) or not namespace_name
                    for namespace_name in namespaces_spec
                )
            ):
                raise TypeError(
                    "configureFailPoint data.namespaces must be a non-empty list of strings"
                )
            normalized_namespaces: list[str] = []
            for namespace_name in namespaces_spec:
                if "." not in namespace_name:
                    namespace_name = f"{db_name}.{namespace_name}"
                normalized_namespaces.append(namespace_name)
            namespaces = tuple(dict.fromkeys(normalized_namespaces))
        close_connection = data_spec.get("closeConnection", False)
        if not isinstance(close_connection, bool):
            raise TypeError("configureFailPoint data.closeConnection must be a bool")

        block_connection = data_spec.get("blockConnection", False)
        if not isinstance(block_connection, bool):
            raise TypeError("configureFailPoint data.blockConnection must be a bool")

        block_time_ms = data_spec.get("blockTimeMS", 0)
        if not isinstance(block_time_ms, int) or isinstance(block_time_ms, bool):
            raise TypeError("configureFailPoint data.blockTimeMS must be an integer")
        if block_time_ms < 0:
            raise TypeError("configureFailPoint data.blockTimeMS must be >= 0")
        if not block_connection and block_time_ms:
            raise TypeError(
                "configureFailPoint data.blockTimeMS requires blockConnection=true"
            )
        if block_connection and block_time_ms <= 0:
            raise TypeError(
                "configureFailPoint data.blockConnection=true requires blockTimeMS > 0"
            )

        return (
            fail_commands,
            error_code,
            error_message,
            error_labels,
            namespaces,
            close_connection,
            block_time_ms,
        )

    def parse_raw_command(
        self,
        command: object,
        **kwargs: object,
    ) -> AsyncDatabaseCommandService.AdminCommand[object]:
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
        if command_name == "dbHash":
            if spec.get("dbHash") not in (True, 1):
                raise TypeError("dbHash command value must be 1")
            collections = spec.get("collections")
            normalized_collections: tuple[str, ...] = ()
            if collections is not None:
                if not isinstance(collections, list) or any(
                    not isinstance(name, str) or not name for name in collections
                ):
                    raise TypeError("collections must be a list of non-empty strings")
                normalized_collections = tuple(dict.fromkeys(collections))
            comment = spec.get("comment")
            if comment is not None and not isinstance(comment, str):
                raise TypeError("comment must be a string")
            return self.DatabaseHashCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collections=normalized_collections,
            )
        if command_name == "currentOp":
            if spec.get("currentOp") not in (True, 1):
                raise TypeError("currentOp command value must be 1")
            return self.CurrentOpCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
            )
        if command_name == "killOp":
            if spec.get("killOp") not in (True, 1):
                raise TypeError("killOp command value must be 1")
            opid = spec.get("op")
            if not isinstance(opid, str) or not opid:
                raise TypeError("killOp requires a non-empty string op")
            return self.KillOpCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                opid=opid,
            )
        if command_name == "configureFailPoint":
            if spec.get("configureFailPoint") != "failCommand":
                raise OperationFailure(
                    "configureFailPoint local runtime only supports failCommand"
                )
            mode, times = self._normalize_fail_point_mode(spec.get("mode"))
            (
                fail_commands,
                error_code,
                error_message,
                error_labels,
                namespaces,
                close_connection,
                block_time_ms,
            ) = (
                self._normalize_fail_command_data(
                    spec.get("data"),
                    required=mode != "off",
                    db_name=self._admin._db_name,
                )
            )
            return self.ConfigureFailPointCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                mode=mode,
                times=times,
                fail_commands=fail_commands,
                error_code=error_code,
                error_message=error_message,
                error_labels=error_labels,
                namespaces=namespaces,
                close_connection=close_connection,
                block_time_ms=block_time_ms,
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
            collection_name, operation = self._compiler.compile_find_operation(
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
            collection_name, operation = self._compiler.compile_aggregate_operation(spec)
            return self.AggregateCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                operation=operation,
            )
        if command_name == "count":
            collection_name, operation = self._compiler.compile_count_operation(spec)
            return self.CountCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=collection_name,
                operation=operation,
            )
        if command_name == "distinct":
            collection_name, key, operation = self._compiler.compile_distinct_operation(
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
            comment = spec.get("comment")
            if comment is not None and not isinstance(comment, str):
                raise TypeError("comment must be a string")
            return self.ListIndexesCommand(
                db_name=self._admin._db_name,
                command_name=command_name,
                spec=spec,
                collection_name=require_collection_name(
                    spec.get("listIndexes"),
                    "listIndexes",
                ),
                comment=comment,
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
        command: AsyncDatabaseCommandService.AdminCommand[CommandResultT],
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
        if isinstance(command, self.DatabaseHashCommand):
            return await self._routing.execute_db_hash_command(
                command.collections,
                comment=command.spec.get("comment"),
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.CurrentOpCommand):
            snapshot_fn = getattr(self._engine, "_snapshot_active_operations", None)
            operations = snapshot_fn() if callable(snapshot_fn) else []
            operations = [
                operation
                for operation in operations
                if operation.get("command") != "currentOp"
            ]
            return {"inprog": operations, "ok": 1.0}  # type: ignore[return-value]
        if isinstance(command, self.KillOpCommand):
            cancel_fn = getattr(self._engine, "_cancel_active_operation", None)
            killed = bool(cancel_fn(command.opid)) if callable(cancel_fn) else False
            return {
                "ok": 1.0,
                "numKilled": 1 if killed else 0,
                "info": "operation cancelled" if killed else "operation not found or not killable",
            }  # type: ignore[return-value]
        if isinstance(command, self.ConfigureFailPointCommand):
            return self._failpoints.configure_fail_command(
                mode=command.mode,
                times=command.times,
                fail_commands=command.fail_commands,
                error_code=command.error_code,
                error_message=command.error_message,
                error_labels=command.error_labels,
                namespaces=command.namespaces,
                close_connection=command.close_connection,
                block_time_ms=command.block_time_ms,
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
            return await self._routing.execute_find_and_modify(
                command.options,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.FindCommand):
            return await self._routing.execute_find_command(
                command.collection_name,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.AggregateCommand):
            return await self._routing.execute_aggregate_command(
                command.collection_name,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.CountCommand):
            return await self._routing.execute_count_command(
                command.collection_name,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.DistinctCommand):
            return await self._routing.execute_distinct_command(
                command.collection_name,
                command.key,
                command.operation,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.ListIndexesCommand):
            return await self._routing.execute_list_indexes_command(
                command.collection_name,
                comment=command.comment,
                session=session,
            )  # type: ignore[return-value]
        if isinstance(command, self.DelegatedAdminCommand):
            assert command.route is not None
            handler = getattr(self._routing, command.route.handler_name)
            if command.route.passes_spec:
                return await handler(command.spec, session=session)
            return await handler(session=session)
        raise AssertionError(f"Unexpected admin command type: {type(command)!r}")

    def _execute_static(
        self,
        command: AsyncDatabaseCommandService.StaticAdminCommand,
    ) -> object:
        if command.command_name == "ping":
            return OkResult()
        if command.command_name == "buildInfo":
            return build_info_result(self._mongodb_dialect)
        if command.command_name == "serverStatus":
            change_hub = getattr(self._admin._database, "_change_hub", None)
            change_stream_info: dict[str, object] = {}
            if change_hub is not None:
                backend = change_hub.backend_info.to_document()
                state = change_hub.state.to_document()
                change_stream_info = {
                    "implementation": backend["implementation"],
                    "persistent": backend["persistent"],
                    "boundedHistory": backend["boundedHistory"],
                    "maxRetainedEvents": backend["maxRetainedEvents"],
                    "journalEnabled": backend["journalEnabled"],
                    "journalFsync": backend["journalFsync"],
                    "journalMaxLogBytes": backend["journalMaxLogBytes"],
                    "retainedEvents": state["retainedEvents"],
                    "currentOffset": state["currentOffset"],
                    "nextToken": state["nextToken"],
                }
            return server_status_result(
                self._mongodb_dialect,
                engine=self._engine,
                change_stream_info=change_stream_info,
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

    @property
    def _compiler(self):
        compiler = getattr(self._admin, "_command_compiler", None)
        if compiler is not None:
            return compiler
        return _LegacyAdminCommandCompilerAdapter(self._admin)

    @property
    def _routing(self):
        routing = getattr(self._admin, "_routing", None)
        if routing is not None:
            return routing
        return _LegacyAdminRoutingAdapter(self._admin)

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
        command: object | AsyncDatabaseCommandService.AdminCommand[object],
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
        should_track = parsed.command_name not in {"currentOp", "killOp"}
        command_namespace = self._command_namespace(parsed)
        try:
            if parsed.command_name != "configureFailPoint":
                fail_command = self._failpoints.consume_fail_command(
                    parsed.command_name,
                    namespace=command_namespace,
                )
                if fail_command is not None:
                    if fail_command.block_time_ms > 0:
                        await asyncio.sleep(float(fail_command.block_time_ms) / 1000.0)
                    if fail_command.close_connection:
                        raise ConnectionFailure(
                            fail_command.error_message,
                            error_labels=fail_command.error_labels,
                        )
                    raise OperationFailure(
                        fail_command.error_message,
                        code=fail_command.error_code,
                        details={
                            "failPoint": "failCommand",
                            "commandName": parsed.command_name,
                        },
                        error_labels=fail_command.error_labels,
                    )
            with track_active_operation(
                self._engine,
                command_name=parsed.command_name,
                operation_type="admin",
                namespace=f"{parsed.db_name}.$cmd",
                session=session,
                comment=parsed.spec.get("comment") if isinstance(parsed.spec, dict) else None,
                max_time_ms=parsed.spec.get("maxTimeMS") if isinstance(parsed.spec, dict) else None,
                killable=should_track,
                metadata={"db": parsed.db_name},
            ) if should_track else _null_operation_tracker() as _opid:
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

    @staticmethod
    def _command_namespace(
        command: AdminCommand[object],
    ) -> str | None:
        target = command.spec.get(command.command_name)
        if not isinstance(target, str) or not target:
            return None
        if "." in target:
            return target
        return f"{command.db_name}.{target}"
