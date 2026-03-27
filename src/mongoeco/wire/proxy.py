from __future__ import annotations

import asyncio
from dataclasses import dataclass
import itertools
from typing import Any

from mongoeco.api import AsyncMongoClient
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import OperationFailure
from mongoeco.wire.cursors import WireCursorStore
from mongoeco.wire.executor import WireCommandExecutor
from mongoeco.wire.protocol import (
    OP_MSG,
    OP_QUERY,
    decode_op_msg,
    decode_op_query,
    encode_op_msg_response,
    encode_op_reply,
    parse_message_header,
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
        self._cursor_store = WireCursorStore()
        self._executor = WireCommandExecutor(self._client, self._cursor_store)

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
        self._cursor_store.clear()
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
                result = await self._executor.execute_command(request.body)
                return encode_op_msg_response(
                    result,
                    request_id=request_id,
                    response_to=header.request_id,
                )
            if header.op_code == OP_QUERY:
                request = decode_op_query(header, payload)
                result = await self._executor.execute_legacy_query(request.full_collection_name, request.query)
                return encode_op_reply(
                    [result],
                    request_id=request_id,
                    response_to=header.request_id,
                )
            raise OperationFailure(f"unsupported wire opCode: {header.op_code}")
        except Exception as exc:
            result = self._executor.error_document(exc)
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
