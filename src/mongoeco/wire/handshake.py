from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from mongoeco.api._async.database_commands import build_info_result
from mongoeco.wire.surface import WireSurface

if TYPE_CHECKING:
    from mongoeco.compat import MongoDialect
    from mongoeco.types import HelloDocument
    from mongoeco.wire.connections import WireConnectionContext


class WireHandshakeService:
    def __init__(
        self,
        mongodb_dialect: MongoDialect,
        *,
        surface: WireSurface | None = None,
    ) -> None:
        self._mongodb_dialect = mongodb_dialect
        self._surface = surface or WireSurface()

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
            "maxBsonObjectSize": self._surface.max_bson_object_size,
            "maxMessageSizeBytes": self._surface.max_message_size_bytes,
            "maxWriteBatchSize": self._surface.max_write_batch_size,
            "logicalSessionTimeoutMinutes": self._surface.logical_session_timeout_minutes,
            "connectionId": connection.connection_id,
            "minWireVersion": self._surface.min_wire_version,
            "maxWireVersion": self._surface.max_wire_version,
            "readOnly": False,
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
