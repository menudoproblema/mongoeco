from __future__ import annotations

import asyncio
from dataclasses import dataclass
import itertools
from typing import Any

from mongoeco.api import AsyncMongoClient
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import MongoEcoError, OperationFailure, PyMongoError
from mongoeco.wire.protocol import (
    OP_MSG,
    OP_QUERY,
    decode_op_msg,
    decode_op_query,
    encode_op_msg_response,
    encode_op_reply,
    parse_message_header,
)


_WIRE_INTERNAL_KEYS = frozenset(
    {
        "$db",
        "$readPreference",
        "$clusterTime",
        "lsid",
        "txnNumber",
        "autocommit",
        "startTransaction",
        "$audit",
    }
)


@dataclass(frozen=True, slots=True)
class ProxyAddress:
    host: str
    port: int

    @property
    def uri(self) -> str:
        return f"mongodb://{self.host}:{self.port}/?directConnection=true"


class AsyncMongoEcoProxyServer:
    def __init__(
        self,
        *,
        client: AsyncMongoClient | None = None,
        engine: AsyncStorageEngine | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
        mongodb_dialect: object | None = None,
        pymongo_profile: object | None = None,
    ) -> None:
        if client is not None and engine is not None:
            raise TypeError("client and engine are mutually exclusive")
        self._client = client or AsyncMongoClient(
            engine,
            mongodb_dialect=mongodb_dialect,
            pymongo_profile=pymongo_profile,
        )
        self._owns_client = client is None
        self._host = host
        self._port = port
        self._server: asyncio.AbstractServer | None = None
        self._request_ids = itertools.count(1)

    async def __aenter__(self) -> "AsyncMongoEcoProxyServer":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    @property
    def address(self) -> ProxyAddress:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("proxy server is not running")
        host, port = self._server.sockets[0].getsockname()[:2]
        return ProxyAddress(host=host, port=port)

    async def start(self) -> ProxyAddress:
        if self._server is not None:
            return self.address
        if self._owns_client:
            await self._client._engine.connect()
        self._server = await asyncio.start_server(self._handle_connection, self._host, self._port)
        return self.address

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._owns_client:
            await self._client._engine.disconnect()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while not reader.at_eof():
                try:
                    raw_header = await reader.readexactly(16)
                except asyncio.IncompleteReadError:
                    break
                header = parse_message_header(raw_header)
                payload = await reader.readexactly(header.message_length - 16)
                response = await self._dispatch_wire_request(header, payload)
                writer.write(response)
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def _dispatch_wire_request(self, header, payload: bytes) -> bytes:
        request_id = next(self._request_ids)
        try:
            if header.op_code == OP_MSG:
                request = decode_op_msg(header, payload)
                result = await self._execute_command(request.body)
                return encode_op_msg_response(
                    result,
                    request_id=request_id,
                    response_to=header.request_id,
                )
            if header.op_code == OP_QUERY:
                request = decode_op_query(header, payload)
                result = await self._execute_legacy_query(request.full_collection_name, request.query)
                return encode_op_reply(
                    [result],
                    request_id=request_id,
                    response_to=header.request_id,
                )
            raise OperationFailure(f"unsupported wire opCode: {header.op_code}")
        except Exception as exc:
            result = self._error_document(exc)
            if header.op_code == OP_QUERY:
                return encode_op_reply(
                    [result],
                    request_id=request_id,
                    response_to=header.request_id,
                )
            return encode_op_msg_response(
                result,
                request_id=request_id,
                response_to=header.request_id,
            )

    async def _execute_command(self, body: dict[str, Any], *, db_name: str | None = None) -> dict[str, Any]:
        if db_name is None:
            db_name = body.get("$db")
        if not isinstance(db_name, str) or not db_name:
            raise OperationFailure("$db must be a non-empty string")
        command_document = {
            key: value
            for key, value in body.items()
            if key not in _WIRE_INTERNAL_KEYS
        }
        if not command_document:
            raise OperationFailure("wire command document must contain an executable command")
        command_name = next(iter(command_document))
        if command_name == "endSessions":
            return {"ok": 1.0}
        if command_name == "getMore":
            collection_name = command_document.get("collection", "")
            if not isinstance(collection_name, str):
                collection_name = ""
            return {
                "cursor": {
                    "id": 0,
                    "ns": f"{db_name}.{collection_name}",
                    "nextBatch": [],
                },
                "ok": 1.0,
            }
        database = self._client.get_database(db_name)
        result = await database.command(command_document)
        if not isinstance(result, dict):
            raise OperationFailure("wire command must resolve to a document response")
        return result

    async def _execute_legacy_query(
        self,
        full_collection_name: str,
        query: dict[str, Any],
    ) -> dict[str, Any]:
        if not full_collection_name.endswith(".$cmd"):
            raise OperationFailure("legacy OP_QUERY only supports command namespaces")
        db_name = full_collection_name[:-5]
        return await self._execute_command(query, db_name=db_name)

    @staticmethod
    def _error_document(exc: Exception) -> dict[str, Any]:
        if isinstance(exc, PyMongoError):
            document: dict[str, Any] = {
                "ok": 0.0,
                "errmsg": str(exc),
            }
            code = getattr(exc, "code", None)
            if code is not None:
                document["code"] = code
            code_name = getattr(exc, "code_name", None)
            if code_name is not None:
                document["codeName"] = code_name
            details = getattr(exc, "details", None)
            if isinstance(details, dict):
                document.update(details)
            return document
        if isinstance(exc, MongoEcoError):
            return {"ok": 0.0, "errmsg": str(exc)}
        return {"ok": 0.0, "errmsg": str(exc)}
