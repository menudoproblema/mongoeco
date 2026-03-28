from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import itertools
import ssl
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from mongoeco.driver.connections import ConnectionRegistry, DriverConnection
from mongoeco.driver.security import TlsPolicy
from mongoeco.driver.requests import PreparedRequestExecution
from mongoeco.errors import OperationFailure
from mongoeco.wire.protocol import (
    OP_MSG,
    OP_REPLY,
    decode_op_msg,
    decode_op_reply,
    encode_op_msg_request,
    parse_message_header,
)

if TYPE_CHECKING:
    from mongoeco.api._async.client import AsyncMongoClient


@dataclass(frozen=True, slots=True)
class CallbackCommandTransport:
    callback: Callable[[PreparedRequestExecution], Awaitable[dict[str, Any]]]

    async def send(self, execution: PreparedRequestExecution) -> dict[str, Any]:
        return await self.callback(execution)


@dataclass(slots=True)
class StreamConnectionResource:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    request_ids: itertools.count = field(default_factory=lambda: itertools.count(1))
    authenticated: bool = False


class LocalCommandTransport:
    def __init__(self, client: "AsyncMongoClient"):
        self._client = client

    async def send(self, execution: PreparedRequestExecution) -> dict[str, Any]:
        request = execution.plan.request
        database = self._client.get_database(request.database)
        response = await database.command(
            request.payload,
            session=request.session,
        )
        if not isinstance(response, dict):
            raise OperationFailure("driver local transport expected a document response")
        return response


class WireProtocolCommandTransport:
    def __init__(
        self,
        registry: ConnectionRegistry,
        *,
        tls_policy: TlsPolicy,
        connect_timeout_ms: int,
    ) -> None:
        self._registry = registry
        self._tls_policy = tls_policy
        self._connect_timeout_ms = connect_timeout_ms

    async def send(self, execution: PreparedRequestExecution) -> dict[str, Any]:
        connection = self._registry.get_connection(execution.connection)
        if connection is None:
            raise OperationFailure("driver connection lease is no longer valid")
        resource = await self._ensure_resource(connection)
        if execution.plan.auth_policy.enabled and not resource.authenticated:
            await self._authenticate_resource(resource, execution)
        request_document = dict(execution.plan.request.payload)
        request_document.setdefault("$db", execution.plan.request.database)
        result = await self._roundtrip(resource, request_document, lease=execution.connection)
        self._raise_if_error_document(result)
        return result

    async def _ensure_resource(self, connection: DriverConnection) -> StreamConnectionResource:
        resource = connection.resource
        if isinstance(resource, StreamConnectionResource):
            return resource
        ssl_context = None
        if self._tls_policy.enabled:
            ssl_context = ssl.create_default_context(cafile=self._tls_policy.ca_file)
            if not self._tls_policy.verify_certificates:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
        connect_coro = asyncio.open_connection(
            connection.server.address.rsplit(":", 1)[0],
            int(connection.server.address.rsplit(":", 1)[1]),
            ssl=ssl_context,
        )
        timeout = self._connect_timeout_ms / 1000
        reader, writer = await asyncio.wait_for(connect_coro, timeout=timeout)
        resource = StreamConnectionResource(reader=reader, writer=writer)
        connection.attach_resource(resource)
        return resource

    async def _authenticate_resource(
        self,
        resource: StreamConnectionResource,
        execution: PreparedRequestExecution,
    ) -> None:
        auth = execution.plan.auth_policy
        command = {
            "authenticate": 1,
            "mechanism": auth.mechanism or "SCRAM-SHA-256",
            "user": auth.username,
            "pwd": auth.password,
            "db": auth.source or execution.plan.request.database,
            "$db": auth.source or execution.plan.request.database,
        }
        if command["user"] is None:
            raise OperationFailure("wire authentication requires a username")
        try:
            result = await self._roundtrip(resource, command, lease=execution.connection)
            self._raise_if_error_document(result)
        except Exception:  # noqa: BLE001
            self._registry.discard(execution.connection)
            raise
        resource.authenticated = True

    async def _roundtrip(
        self,
        resource: StreamConnectionResource,
        request_document: dict[str, Any],
        *,
        lease,
    ) -> dict[str, Any]:
        request_id = next(resource.request_ids)
        resource.writer.write(encode_op_msg_request(request_document, request_id=request_id))
        await resource.writer.drain()
        try:
            raw_header = await resource.reader.readexactly(16)
            header = parse_message_header(raw_header)
            payload = await resource.reader.readexactly(header.message_length - 16)
        except Exception:  # noqa: BLE001
            self._registry.discard(lease)
            raise
        if header.op_code == OP_MSG:
            return decode_op_msg(header, payload).body
        if header.op_code == OP_REPLY:
            reply = decode_op_reply(header, payload)
            return reply.documents[0] if reply.documents else {"ok": 1.0}
        raise OperationFailure(f"unsupported wire response opCode: {header.op_code}")

    @staticmethod
    def _raise_if_error_document(result: dict[str, Any]) -> None:
        ok = result.get("ok")
        if ok not in {0, 0.0, False}:
            return
        labels = result.get("errorLabels")
        error_labels = tuple(labels) if isinstance(labels, list) else ()
        raise OperationFailure(
            str(result.get("errmsg", "wire command failed")),
            code=result.get("code") if isinstance(result.get("code"), int) else None,
            details=result,
            error_labels=error_labels,
        )
