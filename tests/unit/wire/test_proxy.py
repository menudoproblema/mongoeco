import unittest
from unittest.mock import patch
import uuid
import struct

from mongoeco.errors import ExecutionTimeout, MongoEcoError, OperationFailure, PyMongoError
from mongoeco.wire.protocol import OP_MSG, OP_QUERY, parse_message_header
from mongoeco.types import ObjectId
from mongoeco.wire.auth import WireAuthenticationService, WireAuthUser
from mongoeco.wire.connections import WireConnectionContext, WireConnectionRegistry
from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.handshake import WireHandshakeService
from mongoeco.wire.executor import WireCommandExecutor
from mongoeco.wire.proxy import AsyncMongoEcoProxyServer
from mongoeco.wire.surface import WireSurface
from mongoeco.wire.sessions import WireSessionStore


class WireProxyUnitTests(unittest.TestCase):
    def test_proxy_requires_client_or_engine_not_both(self):
        with self.assertRaisesRegex(TypeError, "mutually exclusive"):
            AsyncMongoEcoProxyServer(client=AsyncMongoEcoProxyServer()._client, engine=object())  # type: ignore[arg-type]

    def test_capability_registry_resolves_special_commands(self):
        self.assertEqual(resolve_wire_command_capability("getMore").kind, "get_more")
        self.assertEqual(resolve_wire_command_capability("killCursors").kind, "kill_cursors")
        self.assertEqual(resolve_wire_command_capability("commitTransaction").kind, "commit_transaction")
        self.assertEqual(resolve_wire_command_capability("hello").kind, "handshake")
        self.assertEqual(resolve_wire_command_capability("find").kind, "passthrough")

    def test_error_document_includes_catalog_metadata_and_labels(self):
        exc = ExecutionTimeout("took too long")

        document = WireCommandExecutor.error_document(exc)

        self.assertEqual(document["ok"], 0.0)
        self.assertEqual(document["errmsg"], "took too long")
        self.assertEqual(document["code"], 50)
        self.assertEqual(document["codeName"], "MaxTimeMSExpired")
        self.assertNotIn("errorLabels", document)

    def test_get_more_with_unknown_cursor_returns_empty_batch(self):
        proxy = AsyncMongoEcoProxyServer()

        response = proxy._cursor_store.get_more({"getMore": 99, "collection": "events"}, db_name="alpha")

        self.assertEqual(
            response,
            {
                "cursor": {
                    "id": 0,
                    "ns": "alpha.events",
                    "nextBatch": [],
                },
                "ok": 1.0,
            },
        )

    def test_cursor_store_splits_first_batch_and_serves_get_more(self):
        proxy = AsyncMongoEcoProxyServer()
        result = proxy._cursor_store.materialize_command_result(
            {"find": "events", "batchSize": 2},
            {
                "cursor": {
                    "id": 0,
                    "ns": "alpha.events",
                    "firstBatch": [{"seq": 1}, {"seq": 2}, {"seq": 3}],
                },
                "ok": 1.0,
            },
        )

        cursor_id = result["cursor"]["id"]
        self.assertNotEqual(cursor_id, 0)
        self.assertEqual(result["cursor"]["firstBatch"], [{"seq": 1}, {"seq": 2}])

        next_page = proxy._cursor_store.get_more(
            {"getMore": cursor_id, "collection": "events", "batchSize": 2},
            db_name="alpha",
        )
        self.assertEqual(next_page["cursor"]["id"], 0)
        self.assertEqual(next_page["cursor"]["nextBatch"], [{"seq": 3}])

    def test_kill_cursors_reports_killed_and_unknown_ids(self):
        proxy = AsyncMongoEcoProxyServer()
        result = proxy._cursor_store.materialize_command_result(
            {"find": "events", "batchSize": 1},
            {
                "cursor": {
                    "id": 0,
                    "ns": "alpha.events",
                    "firstBatch": [{"seq": 1}, {"seq": 2}],
                },
                "ok": 1.0,
            },
        )
        cursor_id = result["cursor"]["id"]

        response = proxy._cursor_store.kill_cursors(
            {"killCursors": "events", "cursors": [cursor_id, 999]}
        )

        self.assertEqual(response["cursorsKilled"], [cursor_id])
        self.assertEqual(response["cursorsUnknown"], [999])

    def test_session_store_reuses_lsid_and_starts_transaction_when_requested(self):
        proxy = AsyncMongoEcoProxyServer()
        store = WireSessionStore()
        capability = resolve_wire_command_capability("find")
        lsid = {"id": "session-1"}

        session = store.resolve_for_command(
            proxy._client,
            {"lsid": lsid, "find": "events", "startTransaction": True, "autocommit": False},
            capability=capability,
        )
        self.assertIsNotNone(session)
        assert session is not None
        self.assertTrue(session.transaction_active)

        same_session = store.resolve_for_command(
            proxy._client,
            {"lsid": lsid, "find": "events"},
            capability=capability,
        )
        self.assertIs(session, same_session)

        committed = store.commit_transaction({"lsid": lsid})
        self.assertEqual(committed, {"ok": 1.0})
        self.assertFalse(session.transaction_active)

    def test_wire_surface_declares_supported_commands_and_opcodes(self):
        surface = WireSurface()

        self.assertTrue(surface.supports_command("find"))
        self.assertTrue(surface.supports_command("getMore"))
        self.assertTrue(surface.supports_opcode(2004))
        self.assertTrue(surface.supports_opcode(2013))
        self.assertFalse(surface.supports_opcode(2012))

    def test_proxy_address_requires_running_server(self):
        proxy = AsyncMongoEcoProxyServer()
        with self.assertRaisesRegex(RuntimeError, "not running"):
            _ = proxy.address

    def test_error_document_handles_generic_mongoeco_and_generic_exception(self):
        mongoeco_document = WireCommandExecutor.error_document(MongoEcoError("broken"))
        generic_document = WireCommandExecutor.error_document(RuntimeError("boom"))

        self.assertEqual(mongoeco_document, {"ok": 0.0, "errmsg": "broken"})
        self.assertEqual(generic_document, {"ok": 0.0, "errmsg": "boom"})

    def test_error_document_copies_labels_for_pymongo_errors(self):
        error = PyMongoError("bad")
        error.code = 999
        error.code_name = "BadThing"
        error.details = {}
        error.error_labels = ("RetryableWriteError",)

        document = WireCommandExecutor.error_document(error)

        self.assertEqual(document["code"], 999)
        self.assertEqual(document["codeName"], "BadThing")
        self.assertEqual(document["errorLabels"], ["RetryableWriteError"])

    def test_session_store_returns_none_when_capability_does_not_bind_or_lsid_missing(self):
        proxy = AsyncMongoEcoProxyServer()
        store = WireSessionStore()

        session = store.resolve_for_command(
            proxy._client,
            {"hello": 1},
            capability=resolve_wire_command_capability("hello"),
        )
        self.assertIsNone(session)

        session = store.resolve_for_command(
            proxy._client,
            {"find": "events"},
            capability=resolve_wire_command_capability("find"),
        )
        self.assertIsNone(session)

    def test_session_store_validates_end_sessions_and_transaction_requirements(self):
        store = WireSessionStore()

        with self.assertRaisesRegex(TypeError, "list"):
            store.end_sessions("bad")

        with self.assertRaisesRegex(OperationFailure, "requires lsid"):
            store.commit_transaction({})

        with self.assertRaisesRegex(OperationFailure, "requires an active session"):
            store.abort_transaction({"lsid": {"id": "missing"}})

    def test_session_store_rejects_invalid_autocommit_and_can_end_sessions(self):
        proxy = AsyncMongoEcoProxyServer()
        store = WireSessionStore()
        capability = resolve_wire_command_capability("find")
        lsid = {"id": "session-2"}

        with self.assertRaisesRegex(OperationFailure, "autocommit=false"):
            store.resolve_for_command(
                proxy._client,
                {"lsid": lsid, "find": "events", "startTransaction": True, "autocommit": True},
                capability=capability,
            )

        session = store.resolve_for_command(
            proxy._client,
            {"lsid": lsid, "find": "events"},
            capability=capability,
        )
        assert session is not None
        self.assertTrue(session.active)
        self.assertEqual(store.end_sessions([lsid]), {"ok": 1.0})
        self.assertFalse(session.active)

    def test_session_store_freezes_supported_lsid_scalar_types(self):
        proxy = AsyncMongoEcoProxyServer()
        store = WireSessionStore()
        capability = resolve_wire_command_capability("find")
        lsid = {
            "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "bytes": b"abc",
            "oid": ObjectId("507f1f77bcf86cd799439011"),
            "nested": [{"a": 1}],
        }

        session = store.resolve_for_command(
            proxy._client,
            {"lsid": lsid, "find": "events"},
            capability=capability,
        )
        self.assertIsNotNone(session)

    def test_handshake_service_uses_surface_limits(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = WireConnectionContext(connection_id=7, compression=("zlib",))
        surface = WireSurface(
            min_wire_version=1,
            max_wire_version=9,
            max_bson_object_size=123,
            max_message_size_bytes=456,
            max_write_batch_size=789,
            logical_session_timeout_minutes=12,
            compression=("snappy",),
        )
        service = WireHandshakeService(proxy._client.mongodb_dialect, surface=surface)

        document = service.build_hello_response(command_name="ismaster", body={}, connection=connection)

        self.assertEqual(document["connectionId"], 7)
        self.assertEqual(document["minWireVersion"], 1)
        self.assertEqual(document["maxWireVersion"], 9)
        self.assertEqual(document["maxBsonObjectSize"], 123)
        self.assertEqual(document["maxMessageSizeBytes"], 456)
        self.assertEqual(document["maxWriteBatchSize"], 789)
        self.assertEqual(document["logicalSessionTimeoutMinutes"], 12)
        self.assertEqual(document["compression"], ["snappy"])
        self.assertTrue(document["ismaster"])

    def test_authentication_service_authenticates_logout_and_requires_auth(self):
        service = WireAuthenticationService(
            (
                WireAuthUser(
                    username="ada",
                    password="secret",
                    source="admin",
                    mechanisms=("SCRAM-SHA-256",),
                    roles=({"role": "root", "db": "admin"},),
                ),
            )
        )
        connection = WireConnectionContext(connection_id=1)

        response = service.authenticate(
            {
                "authenticate": 1,
                "mechanism": "SCRAM-SHA-256",
                "user": "ada",
                "pwd": "secret",
                "db": "admin",
            },
            connection=connection,
        )

        self.assertEqual(response, {"ok": 1.0})
        self.assertEqual(
            connection.authenticated_users,
            [{"user": "ada", "db": "admin", "mechanism": "SCRAM-SHA-256"}],
        )
        self.assertEqual(connection.authenticated_roles, [{"role": "root", "db": "admin"}])
        service.require_authenticated(connection, "ping")
        self.assertEqual(service.logout(connection), {"ok": 1.0})
        self.assertEqual(connection.authenticated_users, [])
        with self.assertRaisesRegex(OperationFailure, "Authentication required"):
            service.require_authenticated(connection, "ping")

    def test_authentication_service_validates_input_and_x509(self):
        service = WireAuthenticationService(
            (
                WireAuthUser(
                    username="client",
                    password=None,
                    source="$external",
                    mechanisms=("MONGODB-X509",),
                ),
            )
        )
        connection = WireConnectionContext(connection_id=2)

        with self.assertRaisesRegex(OperationFailure, "requires mechanism"):
            service.authenticate({"authenticate": 1, "user": "client", "db": "$external"}, connection=connection)
        with self.assertRaisesRegex(OperationFailure, "requires user"):
            service.authenticate({"authenticate": 1, "mechanism": "SCRAM-SHA-256", "db": "admin"}, connection=connection)
        with self.assertRaisesRegex(OperationFailure, "requires db"):
            service.authenticate({"authenticate": 1, "mechanism": "SCRAM-SHA-256", "user": "client", "db": ""}, connection=connection)
        with self.assertRaisesRegex(OperationFailure, "Authentication failed"):
            service.authenticate(
                {
                    "authenticate": 1,
                    "mechanism": "SCRAM-SHA-256",
                    "user": "client",
                    "pwd": "wrong",
                    "db": "$external",
                },
                connection=connection,
            )

        response = service.authenticate(
            {
                "authenticate": 1,
                "mechanism": "MONGODB-X509",
                "user": "client",
                "db": "$external",
            },
            connection=connection,
        )

        self.assertEqual(response, {"ok": 1.0})


class WireProxyAsyncUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_routes_hello_through_handshake_service_and_connection_context(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        result = await proxy._executor.execute_command(
            {
                "hello": 1,
                "$db": "admin",
                "client": {"application": {"name": "wire-tests"}},
                "compression": ["noop"],
            },
            connection=connection,
        )

        self.assertEqual(result["ok"], 1.0)
        self.assertEqual(result["connectionId"], connection.connection_id)
        self.assertEqual(connection.hello_count, 1)
        self.assertEqual(connection.last_hello_command, "hello")
        self.assertEqual(connection.client_metadata, {"application": {"name": "wire-tests"}})
        self.assertEqual(connection.compression, ("noop",))

    async def test_executor_rejects_commands_outside_wire_surface(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        with self.assertRaisesRegex(Exception, "unsupported wire command"):
            await proxy._executor.execute_command(
                {"__unsupported__": 1, "$db": "admin"},
                connection=connection,
            )

    async def test_executor_validates_db_and_legacy_namespace(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        with self.assertRaisesRegex(OperationFailure, "non-empty string"):
            await proxy._executor.execute_command({"ping": 1}, connection=connection)

        with self.assertRaisesRegex(OperationFailure, "legacy OP_QUERY only supports command namespaces"):
            await proxy._executor.execute_legacy_query(
                "alpha.events",
                {"ping": 1},
                connection=connection,
            )

    async def test_executor_normalizes_legacy_query_wrappers_and_batch_size(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))
        captured: dict[str, object] = {}

        class _Database:
            async def command(self, command_document, session=None):
                captured.update(command_document)
                return {
                    "cursor": {
                        "id": 0,
                        "ns": "alpha.events",
                        "firstBatch": [{"seq": 1}, {"seq": 2}, {"seq": 3}],
                    },
                    "ok": 1.0,
                }

        with patch.object(proxy._client, "get_database", return_value=_Database()):
            result = await proxy._executor.execute_legacy_query(
                "alpha.$cmd",
                {
                    "$query": {"find": "events", "filter": {"kind": "view"}},
                    "$orderby": {"seq": 1},
                    "$readPreference": {"mode": "primaryPreferred"},
                },
                connection=connection,
                number_to_return=2,
            )

        self.assertEqual(captured["find"], "events")
        self.assertEqual(captured["sort"], {"seq": 1})
        self.assertEqual(captured["batchSize"], 2)
        self.assertEqual(result["cursor"]["firstBatch"], [{"seq": 1}, {"seq": 2}])

    async def test_executor_applies_legacy_number_to_return_to_cursor_commands(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))
        captured: dict[str, object] = {}

        class _Database:
            async def command(self, command_document, session=None):
                captured.update(command_document)
                return {
                    "cursor": {
                        "id": 0,
                        "ns": "alpha.$cmd.listCollections",
                        "firstBatch": [{"name": "events"}],
                    },
                    "ok": 1.0,
                }

        with patch.object(proxy._client, "get_database", return_value=_Database()):
            await proxy._executor.execute_legacy_query(
                "alpha.$cmd",
                {"listCollections": 1},
                connection=connection,
                number_to_return=7,
            )

        self.assertEqual(captured["cursor"], {"batchSize": 7})

    async def test_executor_rejects_empty_command_document(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        with self.assertRaisesRegex(OperationFailure, "executable command"):
            await proxy._executor.execute_command(
                {"$db": "admin", "$audit": {}},
                connection=connection,
            )

    async def test_executor_passthrough_requires_document_response(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        class _BadDatabase:
            async def command(self, command_document, session=None):
                return ["not-a-document"]

        with patch.object(proxy._client, "get_database", return_value=_BadDatabase()):
            with self.assertRaisesRegex(OperationFailure, "document response"):
                await proxy._executor.execute_command(
                    {"ping": 1, "$db": "admin"},
                    connection=connection,
                )

    async def test_proxy_start_returns_existing_address_when_already_running(self):
        async with AsyncMongoEcoProxyServer() as proxy:
            first = proxy.address
            second = await proxy.start()
            self.assertEqual(first, second)

    async def test_dispatch_wire_request_returns_error_reply_for_unsupported_opcode_and_query_errors(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        bad_header = parse_message_header(struct.pack("<iiii", 16, 7, 0, 9999))
        response = await proxy._dispatch_wire_request(bad_header, b"", connection=connection)
        decoded = proxy._executor.error_document(OperationFailure("unsupported"))
        self.assertEqual(response[12:16], struct.pack("<i", OP_MSG))
        self.assertTrue(response)

        query_header = parse_message_header(struct.pack("<iiii", 16, 9, 0, OP_QUERY))
        query_payload = b"\x00" * 12
        query_response = await proxy._dispatch_wire_request(query_header, query_payload, connection=connection)
        self.assertEqual(query_response[12:16], struct.pack("<i", 1))

    async def test_proxy_handle_connection_closes_writer_on_incomplete_read(self):
        proxy = AsyncMongoEcoProxyServer()

        class _Reader:
            def at_eof(self):
                return False

            async def readexactly(self, n):
                raise __import__("asyncio").IncompleteReadError(partial=b"", expected=n)

        class _Writer:
            def __init__(self):
                self.closed = False

            def get_extra_info(self, name):
                return ("127.0.0.1", 27017)

            def write(self, data):
                raise AssertionError("write should not be called")

            async def drain(self):
                return None

            def close(self):
                self.closed = True

            async def wait_closed(self):
                return None

        writer = _Writer()
        await proxy._handle_connection(_Reader(), writer)
        self.assertTrue(writer.closed)

    def test_connection_context_and_registry_support_peer_and_hello_tracking(self):
        registry = WireConnectionRegistry()
        context = registry.create(("10.0.0.1", 27018))
        self.assertEqual(context.peer_address, "10.0.0.1:27018")

        registry.register_hello(
            context,
            "hello",
            {"client": {"driver": {"name": "pymongo"}}, "compression": ["zstd"]},
        )
        self.assertEqual(context.hello_count, 1)
        self.assertEqual(context.client_metadata, {"driver": {"name": "pymongo"}})
        self.assertEqual(context.compression, ("zstd",))

    def test_cursor_store_validates_batch_and_cursor_shapes(self):
        proxy = AsyncMongoEcoProxyServer()
        store = proxy._cursor_store

        result = store.materialize_command_result({"find": "events"}, {"ok": 1.0})
        self.assertEqual(result, {"ok": 1.0})

        result = store.materialize_command_result(
            {"find": "events"},
            {"cursor": {"id": 0, "ns": "alpha.events", "firstBatch": "bad"}, "ok": 1.0},
        )
        self.assertEqual(result["cursor"]["firstBatch"], "bad")

        with self.assertRaisesRegex(TypeError, "batchSize"):
            store.materialize_command_result(
                {"find": "events", "batchSize": -1},
                {"cursor": {"id": 0, "ns": "alpha.events", "firstBatch": []}, "ok": 1.0},
            )

        with self.assertRaisesRegex(TypeError, "collection must be a string"):
            store.get_more({"getMore": 1, "collection": 1}, db_name="alpha")

        with self.assertRaisesRegex(TypeError, "batchSize"):
            store.get_more({"getMore": 1, "collection": "events", "batchSize": -1}, db_name="alpha")

        with self.assertRaisesRegex(TypeError, "killCursors must name a collection"):
            store.kill_cursors({"killCursors": "", "cursors": []})

        with self.assertRaisesRegex(TypeError, "cursors must be a list"):
            store.kill_cursors({"killCursors": "events", "cursors": "bad"})

        with self.assertRaisesRegex(TypeError, "cursor ids must be integers"):
            store.kill_cursors({"killCursors": "events", "cursors": ["bad"]})
