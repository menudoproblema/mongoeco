from __future__ import annotations

import asyncio
import itertools
import time

from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mongoeco.api import AsyncMongoClient
from mongoeco.errors import OperationFailure
from mongoeco.wire._diagnostics import WireDiagnostics
from mongoeco.wire._executor_support import normalize_legacy_query_body
from mongoeco.wire._framing import WireProtocolError, op_msg_flags, valid_message_length
from mongoeco.wire._lifecycle import finish_cleanup
from mongoeco.wire._request_state import WireRequestState
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
        diagnostic_capacity: int = 0,
    ) -> None:
        self._diagnostics = WireDiagnostics(diagnostic_capacity)
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
        self._update_diagnostic_resources()

    @property
    def diagnostics(self) -> dict[str, object]:
        """Return a detached metadata snapshot without opening any resources."""
        return self._diagnostics.snapshot()

    def _update_diagnostic_resources(self) -> None:
        if self._diagnostics.capacity:
            self._diagnostics.resources(
                listener=self._server is not None,
                handlers=len(self._handlers),
                connections=len(self._connections),
                cursors=len(self._cursor_store),
                sessions=len(self._session_store),
                engine_connected=self._engine_connected,
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
                self._diagnostics.record("server_started")
                self._update_diagnostic_resources()
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
        self._update_diagnostic_resources()
        self._diagnostics.record(
            "shutdown_started",
            pending_connections=len(self._connections),
            pending_cursors=len(self._cursor_store),
            pending_sessions=len(self._session_store),
        )
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
        self._update_diagnostic_resources()
        self._diagnostics.record("shutdown_finished", failed=bool(errors))
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
        self._update_diagnostic_resources()
        handler.add_done_callback(self._connection_done)

    def _connection_done(self, handler: asyncio.Task[None]) -> None:
        self._handlers.discard(handler)
        if not handler.cancelled():
            # The handler recorded context before propagating its error.
            handler.exception()
        self._update_diagnostic_resources()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        connection = self._connections.create(writer.get_extra_info("peername"))
        self._diagnostics.record(
            "connection_opened", connection_id=connection.connection_id
        )
        self._update_diagnostic_resources()
        close_reason = "peer_closed"
        phase = "header"
        try:
            while not reader.at_eof():
                phase = "header"
                try:
                    raw_header = await reader.readexactly(16)
                except asyncio.IncompleteReadError:
                    break
                header = parse_message_header(raw_header)
                if not valid_message_length(
                    header.message_length, self._surface.max_message_size_bytes
                ):
                    message = "invalid wire message length"
                    raise WireProtocolError(message, fatal=True)
                self._diagnostics.record(
                    "body_wait_started",
                    connection_id=connection.connection_id,
                    request_id=header.request_id,
                    opcode=header.op_code,
                )
                phase = "body"
                payload = await reader.readexactly(header.message_length - 16)
                self._diagnostics.record(
                    "body_received",
                    connection_id=connection.connection_id,
                    request_id=header.request_id,
                )
                phase = "dispatch"
                response = await self._dispatch_wire_request(
                    header, payload, connection=connection
                )
                if response is not None:
                    phase = "send"
                    writer.write(response)
                    await writer.drain()
                    self._diagnostics.record(
                        "response_sent",
                        connection_id=connection.connection_id,
                        request_id=header.request_id,
                    )
        except (asyncio.IncompleteReadError, ConnectionError):
            close_reason = "connection_lost"
            self._diagnostics.record(
                "transport_failed",
                connection_id=connection.connection_id,
                outcome="error",
                error_family="connection",
                phase=phase,
            )
        except asyncio.CancelledError:
            close_reason = "server_shutdown"
            raise
        except Exception as error:
            family = "protocol" if isinstance(error, WireProtocolError) else "internal"
            close_reason = "protocol_error" if family == "protocol" else "handler_error"
            self._diagnostics.record(
                "handler_failed",
                connection_id=connection.connection_id,
                outcome="error",
                error_family=family,
                phase=phase,
            )
            raise
        finally:
            self._connections.close(connection)
            self._diagnostics.record(
                "connection_closed",
                connection_id=connection.connection_id,
                reason=close_reason,
            )
            self._update_diagnostic_resources()
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()

    def _decode_wire_context(self, header, payload, connection, state):
        db_name = None
        if header.op_code == OP_MSG:
            # Learn reply policy before BSON decoding can fail.
            if op_msg_flags(payload) & OP_MSG_MORE_TO_COME:
                state.disposition = "suppress"
                state.expects_reply = False
            body = decode_op_msg(header, payload).body
        else:
            request = decode_op_query(header, payload)
            if not request.full_collection_name.endswith(".$cmd"):
                message = "legacy OP_QUERY only supports command namespaces"
                raise WireProtocolError(message, code=5739101, code_name=None)
            db_name = request.full_collection_name[:-5]
            body = normalize_legacy_query_body(
                request.query, number_to_return=request.number_to_return
            )
        state.phase = "prepare"
        return self._executor._build_request_context(
            body,
            connection=connection,
            db_name=db_name,
            on_normalized=state.normalized,
        )

    def _render_wire_result(self, header, request_id, state):
        if state.disposition == "suppress":
            state.response = "suppressed"
            return None
        state.phase = "encode"
        if header.op_code == OP_QUERY:
            response = encode_op_reply(
                [state.result],
                request_id=request_id,
                response_to=header.request_id,
                response_flags=state.legacy_reply_flags,
            )
        else:
            response = encode_op_msg_response(
                state.result, request_id=request_id, response_to=header.request_id
            )
        state.response = "ready"
        return response

    def _wire_failure(self, error, header, state):
        state.outcome = "error"
        if (
            state.phase == "decode"
            and header.op_code == OP_QUERY
            and isinstance(error, WireProtocolError)
        ):
            state.result = {"ok": 0.0, "code": error.code, "$err": str(error)}
            state.legacy_reply_flags = 2
            return
        if state.phase != "decode" or header.op_code != OP_MSG:
            state.result = self._executor.error_document(error)
            return
        protocol_error = (
            error
            if isinstance(error, WireProtocolError)
            else WireProtocolError(str(error))
        )
        state.result = {
            "ok": 0.0,
            "code": protocol_error.code,
            "codeName": protocol_error.code_name,
            "errmsg": str(protocol_error),
        }
        # MongoDB 9 closes parse failures that 7/8 return as command errors.
        if protocol_error.fatal or self._client.mongodb_dialect.key == "9.0":
            state.disposition = "close"
            state.response = "closed"
            if protocol_error is error:
                raise error
            raise protocol_error from error

    async def _dispatch_wire_request(
        self, header, payload: bytes, *, connection
    ) -> bytes | None:
        request_id = next(self._request_ids)
        state = WireRequestState()
        started = time.monotonic() if self._diagnostics.capacity else 0.0
        try:
            if not self._surface.supports_opcode(header.op_code):
                raise OperationFailure(f"unsupported wire opCode: {header.op_code}")
            if header.op_code not in (OP_MSG, OP_QUERY):
                raise OperationFailure(f"unsupported wire opCode: {header.op_code}")
            state.context = self._decode_wire_context(
                header, payload, connection, state
            )
            state.received(self._diagnostics, header, connection.connection_id)
            state.phase = "execute"
            state.result = await self._executor.execute_prepared(state.context)
            state.outcome = "completed"
            return self._render_wire_result(header, request_id, state)
        except Exception as error:
            self._wire_failure(error, header, state)
            return self._render_wire_result(header, request_id, state)
        except asyncio.CancelledError:
            state.outcome = "cancelled"
            raise
        finally:
            if self._diagnostics.capacity:
                state.completed(
                    self._diagnostics,
                    header,
                    connection.connection_id,
                    round((time.monotonic() - started) * 1000, 3),
                )
                self._update_diagnostic_resources()
