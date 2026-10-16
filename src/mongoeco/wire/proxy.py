from __future__ import annotations

import asyncio
import itertools

from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mongoeco.api import AsyncMongoClient
from mongoeco.errors import OperationFailure
from mongoeco.wire._lifecycle import finish_cleanup
from mongoeco.wire.auth import WireAuthenticationService, WireAuthUser
from mongoeco.wire.connections import WireConnectionRegistry
from mongoeco.wire.cursors import WireCursorStore
from mongoeco.wire.executor import WireCommandExecutor
from mongoeco.wire.protocol import (
    OP_MSG,
    OP_MSG_MORE_TO_COME,
    OP_QUERY,
    decode_op_msg,
    decode_op_query,
    encode_op_msg_response,
    encode_op_reply,
    parse_message_header,
)
from mongoeco.wire.sessions import WireSessionStore
from mongoeco.wire.surface import WireSurface


if TYPE_CHECKING:
    from mongoeco.engines.base import AsyncStorageEngine


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
        auth_users: tuple[WireAuthUser, ...] = (),
    ) -> None:
        if client is not None and engine is not None:
            raise TypeError("client and engine are mutually exclusive")
        self._client = client or AsyncMongoClient(
            engine,
            mongodb_dialect=mongodb_dialect,
            pymongo_profile=pymongo_profile,
        )
        self._owns_client = client is None
        self._engine_connected = False
        self._lifecycle_lock = asyncio.Lock()
        self._handlers: set[asyncio.Task[None]] = set()
        self._host = host
        self._port = port
        self._surface = WireSurface()
        self._server: asyncio.AbstractServer | None = None
        self._request_ids = itertools.count(1)
        self._connections = WireConnectionRegistry()
        self._cursor_store = WireCursorStore()
        self._session_store = WireSessionStore()
        self._auth = WireAuthenticationService(auth_users)
        self._executor = WireCommandExecutor(
            self._client,
            self._cursor_store,
            self._session_store,
            surface=self._surface,
            auth=self._auth,
        )

    async def __aenter__(self) -> AsyncMongoEcoProxyServer:
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
        async with self._lifecycle_lock:
            if self._server is not None:
                return self.address
            try:
                if self._owns_client:
                    self._engine_connected = True
                    await self._client._engine.connect()
                self._server = await asyncio.start_server(
                    self._accept_connection, self._host, self._port, start_serving=False
                )
                # Own the bound listener before the first serving yield: a
                # cancellation there must be able to roll back its socket.
                await self._server.start_serving()
            except BaseException as error:
                try:
                    await finish_cleanup(self._close_resources())
                except BaseException as cleanup_error:
                    error.add_note(f"proxy startup cleanup failed: {cleanup_error}")
                raise
            return self.address

    async def close(self) -> None:
        # Cancellation while waiting for a concurrent start must still close
        # the resources that start is about to publish.
        await finish_cleanup(self._close_with_lock())

    async def _close_with_lock(self) -> None:
        async with self._lifecycle_lock:
            await self._close_resources()

    async def _close_resources(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()
            # Teardown must not wait for a peer to drain buffered response bytes.
            server.abort_clients()
        while self._handlers:
            handlers = tuple(self._handlers)
            for handler in handlers:
                handler.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)
            # gather() can complete synchronously for already-finished tasks.
            # Do not depend on queued done callbacks getting a loop turn here.
            self._handlers.difference_update(handlers)
        errors = []
        if server is not None:
            try:
                await server.wait_closed()
            except Exception as error:
                errors.append(error)
        for clear in (self._cursor_store.clear, self._session_store.clear):
            try:
                clear()
            except Exception as error:
                errors.append(error)
        if self._engine_connected:
            try:
                await self._client._engine.disconnect()
                self._engine_connected = False
            except Exception as error:
                errors.append(error)
        if errors:
            message = "proxy cleanup failed"
            raise ExceptionGroup(message, errors)

    def _accept_connection(self, reader, writer) -> None:
        # An accept already queued by the loop can finish after abort_clients().
        # Do not let that peer start an untracked, permanently waiting handler.
        if self._server is None:
            writer.close()
            return
        handler = asyncio.create_task(self._handle_connection(reader, writer))
        self._handlers.add(handler)
        handler.add_done_callback(self._connection_done)

    def _connection_done(self, handler: asyncio.Task[None]) -> None:
        self._handlers.discard(handler)
        if not handler.cancelled():
            handler.exception()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        connection = self._connections.create(writer.get_extra_info("peername"))
        try:
            while not reader.at_eof():
                try:
                    raw_header = await reader.readexactly(16)
                except asyncio.IncompleteReadError:
                    break
                header = parse_message_header(raw_header)
                payload = await reader.readexactly(header.message_length - 16)
                response = await self._dispatch_wire_request(
                    header, payload, connection=connection
                )
                if response is not None:
                    writer.write(response)
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self._connections.close(connection)
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()

    async def _dispatch_wire_request(
        self, header, payload: bytes, *, connection
    ) -> bytes | None:
        request_id = next(self._request_ids)
        more_to_come = False
        try:
            if not self._surface.supports_opcode(header.op_code):
                raise OperationFailure(f"unsupported wire opCode: {header.op_code}")
            if header.op_code == OP_MSG:
                request = decode_op_msg(header, payload)
                more_to_come = bool(request.flag_bits & OP_MSG_MORE_TO_COME)
                result = await self._executor.execute_command(
                    request.body, connection=connection
                )
                if more_to_come:
                    return None
                return encode_op_msg_response(
                    result,
                    request_id=request_id,
                    response_to=header.request_id,
                )
            if header.op_code == OP_QUERY:
                request = decode_op_query(header, payload)
                result = await self._executor.execute_legacy_query(
                    request.full_collection_name,
                    request.query,
                    connection=connection,
                    number_to_return=request.number_to_return,
                )
                return encode_op_reply(
                    [result],
                    request_id=request_id,
                    response_to=header.request_id,
                )
            raise OperationFailure(f"unsupported wire opCode: {header.op_code}")
        except Exception as exc:
            # Unacknowledged requests must not leave an error response waiting
            # for the next command on this connection either.
            if more_to_come:
                return None
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
