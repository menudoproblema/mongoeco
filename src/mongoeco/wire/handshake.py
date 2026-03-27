from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any

from mongoeco.api._async.database_commands import build_info_result
from mongoeco.compat import MongoDialect
from mongoeco.types import HelloDocument
from mongoeco.wire.connections import WireConnectionContext


@dataclass(frozen=True, slots=True)
class WireHelloCapabilities:
    logical_session_timeout_minutes: int = 30
    min_wire_version: int = 0
    max_wire_version: int = 20
    max_bson_object_size: int = 16 * 1024 * 1024
    max_message_size_bytes: int = 48_000_000
    max_write_batch_size: int = 100_000
    compression: tuple[str, ...] = ()


class WireHandshakeService:
    def __init__(
        self,
        mongodb_dialect: MongoDialect,
        *,
        capabilities: WireHelloCapabilities | None = None,
    ) -> None:
        self._mongodb_dialect = mongodb_dialect
        self._capabilities = capabilities or WireHelloCapabilities()

    def build_hello_response(
        self,
        *,
        command_name: str,
        body: dict[str, Any],
        connection: WireConnectionContext,
    ) -> HelloDocument:
        build_info = build_info_result(self._mongodb_dialect)
        response: HelloDocument = {
            "helloOk": True,
            "isWritablePrimary": True,
            "maxBsonObjectSize": self._capabilities.max_bson_object_size,
            "maxMessageSizeBytes": self._capabilities.max_message_size_bytes,
            "maxWriteBatchSize": self._capabilities.max_write_batch_size,
            "logicalSessionTimeoutMinutes": self._capabilities.logical_session_timeout_minutes,
            "connectionId": connection.connection_id,
            "minWireVersion": self._capabilities.min_wire_version,
            "maxWireVersion": self._capabilities.max_wire_version,
            "readOnly": False,
            "compression": list(self._capabilities.compression or connection.compression),
            "localTime": datetime.datetime.now(datetime.UTC),
            "ok": 1.0,
            "version": build_info.version,
            "versionArray": list(build_info.version_array),
            "gitVersion": build_info.git_version,
        }
        if command_name == "hello":
            response["isWritablePrimary"] = True
        else:
            response["ismaster"] = True
        return response
