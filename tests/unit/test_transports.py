import asyncio
import ssl
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from mongoeco.driver.connections import DriverConnection, PoolKey
from mongoeco.driver.security import TlsPolicy
from mongoeco.driver.topology import ServerDescription
from mongoeco.driver.transports import (
    CallbackCommandTransport,
    LocalCommandTransport,
    StreamConnectionResource,
    WireProtocolCommandTransport,
)
from mongoeco.errors import OperationFailure
from mongoeco.wire.protocol import OP_REPLY


class _FakeRegistry:
    def __init__(self, connection=None):
        self._connection = connection
        self.discarded: list[object] = []

    def get_connection(self, lease):
        del lease
        return self._connection

    def discard(self, lease) -> None:
        self.discarded.append(lease)


class _FakeDatabase:
    def __init__(self, response):
        self._response = response
        self.calls: list[tuple[object, object]] = []

    async def command(self, payload, *, session=None):
        self.calls.append((payload, session))
        return self._response


class _FakeClient:
    def __init__(self, database):
        self._database = database

    def get_database(self, name):
        del name
        return self._database


class _FakeWriter:
    def __init__(self):
        self.written: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.written.append(data)

    async def drain(self) -> None:
        return None


class _FakeReader:
    def __init__(self, chunks=None, *, error: Exception | None = None):
        self._chunks = list(chunks or [])
        self._error = error

    async def readexactly(self, size: int) -> bytes:
        del size
        if self._error is not None:
            raise self._error
        return self._chunks.pop(0)


class CommandTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_callback_command_transport_forwards_execution(self):
        execution = object()
        callback = AsyncMock(return_value={"ok": 1.0})

        response = await CallbackCommandTransport(callback).send(execution)

        self.assertEqual(response, {"ok": 1.0})
        callback.assert_awaited_once_with(execution)

    async def test_local_command_transport_rejects_non_document_responses(self):
        database = _FakeDatabase(["bad"])
        client = _FakeClient(database)
        execution = SimpleNamespace(
            plan=SimpleNamespace(
                request=SimpleNamespace(
                    database="admin",
                    payload={"ping": 1},
                    session="session-1",
                )
            )
        )

        with self.assertRaises(OperationFailure):
            await LocalCommandTransport(client).send(execution)

        self.assertEqual(database.calls, [({"ping": 1}, "session-1")])

    async def test_wire_transport_rejects_invalid_connection_leases(self):
        transport = WireProtocolCommandTransport(
            _FakeRegistry(),
            tls_policy=TlsPolicy(enabled=False, verify_certificates=True),
            connect_timeout_ms=250,
        )

        with self.assertRaises(OperationFailure):
            await transport.send(SimpleNamespace(connection="missing"))

    async def test_wire_transport_authenticate_requires_username_and_discards_failures(self):
        registry = _FakeRegistry()
        transport = WireProtocolCommandTransport(
            registry,
            tls_policy=TlsPolicy(enabled=False, verify_certificates=True),
            connect_timeout_ms=250,
        )
        resource = StreamConnectionResource(
            reader=asyncio.StreamReader(),
            writer=_FakeWriter(),
        )
        missing_user_execution = SimpleNamespace(
            connection="lease-1",
            plan=SimpleNamespace(
                auth_policy=SimpleNamespace(
                    mechanism="SCRAM-SHA-256",
                    username=None,
                    password="secret",
                    source=None,
                ),
                request=SimpleNamespace(database="admin"),
            ),
        )

        with self.assertRaises(OperationFailure):
            await transport._authenticate_resource(resource, missing_user_execution)

        execution = SimpleNamespace(
            connection="lease-2",
            plan=SimpleNamespace(
                auth_policy=SimpleNamespace(
                    mechanism=None,
                    username="ada",
                    password="secret",
                    source=None,
                ),
                request=SimpleNamespace(database="admin"),
            ),
        )

        with patch.object(transport, "_roundtrip", new=AsyncMock(side_effect=RuntimeError("boom"))):
            with self.assertRaises(RuntimeError):
                await transport._authenticate_resource(resource, execution)

        self.assertEqual(registry.discarded, ["lease-2"])

    async def test_wire_transport_authenticate_marks_resource_authenticated(self):
        transport = WireProtocolCommandTransport(
            _FakeRegistry(),
            tls_policy=TlsPolicy(enabled=False, verify_certificates=True),
            connect_timeout_ms=250,
        )
        resource = StreamConnectionResource(
            reader=asyncio.StreamReader(),
            writer=_FakeWriter(),
        )
        execution = SimpleNamespace(
            connection="lease-1",
            plan=SimpleNamespace(
                auth_policy=SimpleNamespace(
                    mechanism=None,
                    username="ada",
                    password="secret",
                    source=None,
                ),
                request=SimpleNamespace(database="admin"),
            ),
        )

        with patch.object(transport, "_roundtrip", new=AsyncMock(return_value={"ok": 1.0})) as roundtrip:
            await transport._authenticate_resource(resource, execution)

        self.assertTrue(resource.authenticated)
        roundtrip.assert_awaited_once()
        command = roundtrip.await_args.args[1]
        self.assertEqual(command["authenticate"], 1)
        self.assertEqual(command["mechanism"], "SCRAM-SHA-256")
        self.assertEqual(command["db"], "admin")
        self.assertEqual(command["$db"], "admin")

    async def test_wire_transport_roundtrip_discards_broken_leases(self):
        registry = _FakeRegistry()
        transport = WireProtocolCommandTransport(
            registry,
            tls_policy=TlsPolicy(enabled=False, verify_certificates=True),
            connect_timeout_ms=250,
        )
        resource = StreamConnectionResource(
            reader=_FakeReader(error=EOFError("socket closed")),
            writer=_FakeWriter(),
        )

        with patch("mongoeco.driver.transports.encode_op_msg_request", return_value=b"encoded"):
            with self.assertRaises(EOFError):
                await transport._roundtrip(resource, {"ping": 1}, lease="lease-1")

        self.assertEqual(registry.discarded, ["lease-1"])
        self.assertEqual(resource.writer.written, [b"encoded"])

    async def test_wire_transport_roundtrip_handles_empty_op_reply(self):
        transport = WireProtocolCommandTransport(
            _FakeRegistry(),
            tls_policy=TlsPolicy(enabled=False, verify_certificates=True),
            connect_timeout_ms=250,
        )
        resource = StreamConnectionResource(
            reader=_FakeReader([b"h" * 16, b"body"]),
            writer=_FakeWriter(),
        )

        with patch("mongoeco.driver.transports.encode_op_msg_request", return_value=b"encoded"), patch(
            "mongoeco.driver.transports.parse_message_header",
            return_value=SimpleNamespace(message_length=20, op_code=OP_REPLY),
        ), patch(
            "mongoeco.driver.transports.decode_op_reply",
            return_value=SimpleNamespace(documents=[]),
        ):
            response = await transport._roundtrip(resource, {"ping": 1}, lease="lease-1")

        self.assertEqual(response, {"ok": 1.0})

    async def test_wire_transport_roundtrip_rejects_unsupported_wire_opcode(self):
        transport = WireProtocolCommandTransport(
            _FakeRegistry(),
            tls_policy=TlsPolicy(enabled=False, verify_certificates=True),
            connect_timeout_ms=250,
        )
        resource = StreamConnectionResource(
            reader=_FakeReader([b"h" * 16, b"body"]),
            writer=_FakeWriter(),
        )

        with patch("mongoeco.driver.transports.encode_op_msg_request", return_value=b"encoded"), patch(
            "mongoeco.driver.transports.parse_message_header",
            return_value=SimpleNamespace(message_length=20, op_code=999),
        ):
            with self.assertRaises(OperationFailure):
                await transport._roundtrip(resource, {"ping": 1}, lease="lease-1")

    async def test_wire_transport_ensure_resource_reuses_existing_streams_and_can_open_tls_connections(self):
        transport = WireProtocolCommandTransport(
            _FakeRegistry(),
            tls_policy=TlsPolicy(enabled=True, verify_certificates=False, ca_file="/tmp/ca.pem"),
            connect_timeout_ms=250,
        )
        existing = StreamConnectionResource(
            reader=asyncio.StreamReader(),
            writer=_FakeWriter(),
        )
        connection = DriverConnection(
            connection_id="conn-1",
            server=ServerDescription("db1:27017"),
            pool_key=PoolKey(address="db1:27017", tls=True),
            created_at_monotonic=0.0,
            last_used_at_monotonic=0.0,
            resource=existing,
        )

        self.assertIs(await transport._ensure_resource(connection), existing)

        connection.resource = None
        created_context = SimpleNamespace(check_hostname=True, verify_mode=None)
        reader = asyncio.StreamReader()
        writer = _FakeWriter()

        async def fake_wait_for(coro, timeout):
            self.assertEqual(timeout, 0.25)
            return await coro

        async def fake_open_connection(host, port, ssl=None):
            self.assertEqual(host, "db1")
            self.assertEqual(port, 27017)
            self.assertIs(ssl, created_context)
            return reader, writer

        with patch(
            "mongoeco.driver.transports.ssl.create_default_context",
            return_value=created_context,
        ), patch(
            "mongoeco.driver.transports.asyncio.open_connection",
            side_effect=fake_open_connection,
        ), patch(
            "mongoeco.driver.transports.asyncio.wait_for",
            side_effect=fake_wait_for,
        ):
            resource = await transport._ensure_resource(connection)

        self.assertIs(connection.resource, resource)
        self.assertFalse(created_context.check_hostname)
        self.assertEqual(created_context.verify_mode, ssl.CERT_NONE)

    def test_wire_transport_raise_if_error_document_preserves_labels_and_code(self):
        with self.assertRaises(OperationFailure) as exc_info:
            WireProtocolCommandTransport._raise_if_error_document(
                {
                    "ok": 0,
                    "errmsg": "boom",
                    "code": 42,
                    "errorLabels": ["RetryableWriteError"],
                }
            )

        self.assertEqual(exc_info.exception.code, 42)
        self.assertEqual(exc_info.exception.error_labels, ("RetryableWriteError",))
        self.assertEqual(exc_info.exception.details["errmsg"], "boom")


if __name__ == "__main__":
    unittest.main()
