import datetime
import os
import platform
import socket
import sys
from typing import TYPE_CHECKING
from dataclasses import dataclass

from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    BuildInfoDocument,
    CmdLineOptsDocument,
    ConnectionStatusDocument,
    HelloDocument,
    HostInfoDocument,
    ListCommandsDocument,
    ServerStatusDocument,
    WhatsMyUriDocument,
)

if TYPE_CHECKING:
    from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
    from mongoeco.compat import MongoDialect


_PROCESS_STARTED_AT = datetime.datetime.now(datetime.UTC)

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
    "renameCollection",
    "serverStatus",
    "update",
    "validate",
    "whatsmyuri",
)


def build_info_document(mongodb_dialect: "MongoDialect") -> BuildInfoDocument:
    version_parts = [int(part) for part in mongodb_dialect.server_version.split(".")]
    while len(version_parts) < 2:
        version_parts.append(0)
    major, minor = version_parts[:2]
    return {
        "version": f"{major}.{minor}.0",
        "versionArray": [major, minor, 0, 0],
        "gitVersion": "mongoeco",
        "ok": 1.0,
    }


def hello_document(
    mongodb_dialect: "MongoDialect",
    *,
    legacy_name: bool = False,
) -> HelloDocument:
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
    if legacy_name:
        document["ismaster"] = True
    else:
        document["isWritablePrimary"] = True
    document.update(build_info_document(mongodb_dialect))
    return document


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
    local_time = datetime.datetime.now(datetime.UTC)
    uptime_delta = local_time - _PROCESS_STARTED_AT
    uptime_seconds = max(uptime_delta.total_seconds(), 0.0)
    return {
        "host": socket.gethostname(),
        "version": build_info_document(mongodb_dialect)["version"],
        "process": "mongod",
        "pid": os.getpid(),
        "uptime": uptime_seconds,
        "uptimeMillis": int(uptime_seconds * 1000),
        "uptimeEstimate": int(uptime_seconds),
        "localTime": local_time,
        "connections": {
            "current": 1,
            "available": 8388607,
            "totalCreated": 1,
        },
        "storageEngine": {
            "name": _storage_engine_name(engine),
        },
        "ok": 1.0,
    }


def host_info_document() -> HostInfoDocument:
    return {
        "system": {
            "hostname": socket.gethostname(),
            "cpuArch": platform.machine() or "unknown",
            "numCores": os.cpu_count() or 1,
            "memSizeMB": 0,
        },
        "os": {
            "type": platform.system() or "unknown",
            "name": platform.platform(),
            "version": platform.version(),
        },
        "extra": {
            "pythonVersion": platform.python_version(),
        },
        "ok": 1.0,
    }


def whats_my_uri_document() -> WhatsMyUriDocument:
    return {
        "you": "127.0.0.1:0",
        "ok": 1.0,
    }


def cmd_line_opts_document() -> CmdLineOptsDocument:
    return {
        "argv": list(sys.argv),
        "parsed": {
            "net": {"bindIp": "127.0.0.1", "port": 0},
            "storage": {},
        },
        "ok": 1.0,
    }


def list_commands_document() -> ListCommandsDocument:
    return {
        "commands": {
            command_name: {
                "help": f"mongoeco local support for the {command_name} command",
            }
            for command_name in SUPPORTED_DATABASE_COMMANDS
        },
        "ok": 1.0,
    }


def connection_status_document(*, show_privileges: bool) -> ConnectionStatusDocument:
    auth_info: dict[str, object] = {
        "authenticatedUsers": [],
        "authenticatedUserRoles": [],
    }
    if show_privileges:
        auth_info["authenticatedUserPrivileges"] = []
    return {
        "authInfo": auth_info,
        "ok": 1.0,
    }


class AsyncDatabaseCommandService:
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
        "find": Route("_command_find"),
        "aggregate": Route("_command_aggregate"),
        "explain": Route("_command_explain"),
        "findAndModify": Route("_command_find_and_modify"),
        "listIndexes": Route("_command_list_indexes"),
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

    async def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        spec = self._admin._normalize_command(command, kwargs)
        command_name = next(iter(spec))
        if not isinstance(command_name, str):
            raise TypeError("command name must be a string")

        if command_name == "ping":
            return {"ok": 1.0}
        if command_name == "buildInfo":
            return build_info_document(self._mongodb_dialect)
        if command_name == "serverStatus":
            return server_status_document(
                self._mongodb_dialect,
                engine=self._engine,
            )
        if command_name == "hostInfo":
            return host_info_document()
        if command_name == "whatsmyuri":
            return whats_my_uri_document()
        if command_name == "getCmdLineOpts":
            return cmd_line_opts_document()
        if command_name in {"hello", "isMaster", "ismaster"}:
            return hello_document(
                self._mongodb_dialect,
                legacy_name=command_name != "hello",
            )
        if command_name == "listCommands":
            return list_commands_document()
        if command_name == "connectionStatus":
            show_privileges = spec.get("showPrivileges", False)
            if not isinstance(show_privileges, bool):
                raise TypeError("showPrivileges must be a bool")
            return connection_status_document(show_privileges=show_privileges)
        if command_name == "collStats":
            collection_name = self._admin._require_collection_name(
                spec.get("collStats"),
                "collStats",
            )
            scale = self._admin._normalize_scale_from_command(spec.get("scale"))
            return await self._admin._collection_stats(
                collection_name,
                scale=scale,
                session=session,
            )
        if command_name == "dbStats":
            scale = self._admin._normalize_scale_from_command(spec.get("scale"))
            return await self._admin._database_stats(scale=scale, session=session)
        if command_name == "validate":
            collection_name = self._admin._require_collection_name(
                spec.get("validate"),
                "validate",
            )
            return await self._admin.validate_collection(
                collection_name,
                scandata=bool(spec.get("scandata", False)),
                full=bool(spec.get("full", False)),
                background=spec.get("background"),
                session=session,
            )

        handler_entry = self._DELEGATED_COMMAND_HANDLERS.get(command_name)
        if handler_entry is None:
            raise OperationFailure(f"Unsupported command: {command_name}")
        handler = getattr(self._admin, handler_entry.handler_name)
        if handler_entry.passes_spec:
            return await handler(spec, session=session)
        return await handler(session=session)
