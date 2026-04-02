import unittest
from unittest.mock import patch
import uuid
import struct
import ast
from pathlib import Path

from mongoeco.errors import ExecutionTimeout, MongoEcoError, OperationFailure, PyMongoError
from mongoeco.wire.protocol import OP_MSG, OP_QUERY, parse_message_header
from mongoeco.types import Binary, ObjectId
from mongoeco.wire.auth import WireAuthenticationService, WireAuthUser
from mongoeco.wire.connections import WireConnectionContext, WireConnectionRegistry
from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.handshake import WireHandshakeService
from mongoeco.wire.executor import WireCommandExecutor
from mongoeco.wire.proxy import AsyncMongoEcoProxyServer
from mongoeco.wire.scram import build_scram_client_final
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
        self.assertEqual(resolve_wire_command_capability("saslStart").kind, "sasl_start")
        self.assertEqual(resolve_wire_command_capability("saslContinue").kind, "sasl_continue")
        self.assertEqual(resolve_wire_command_capability("find").kind, "passthrough")
        self.assertEqual(resolve_wire_command_capability("find").family, "admin_read")
        self.assertEqual(resolve_wire_command_capability("buildInfo").family, "admin_introspection")
        self.assertEqual(resolve_wire_command_capability("dbHash").family, "admin_introspection")
        self.assertEqual(resolve_wire_command_capability("serverStatus").family, "admin_status")
        self.assertEqual(resolve_wire_command_capability("profile").family, "admin_control")
        self.assertEqual(resolve_wire_command_capability("listCollections").family, "admin_namespace")
        self.assertEqual(resolve_wire_command_capability("createIndexes").family, "admin_index")
        self.assertEqual(resolve_wire_command_capability("findAndModify").family, "admin_find_and_modify")
        self.assertEqual(resolve_wire_command_capability("connectionStatus").family, "admin_status")

    def test_wire_executor_module_stays_thin_coordinator(self):
        module_path = Path(__file__).resolve().parents[3] / "src" / "mongoeco" / "wire" / "executor.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        imported_modules = {
            node.module
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }

        self.assertIn("mongoeco.wire._executor_support", imported_modules)
        self.assertIn("mongoeco.wire._executor_handlers", imported_modules)

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

    def test_session_store_clear_closes_sessions_and_aborts_active_transactions(self):
        proxy = AsyncMongoEcoProxyServer()
        store = WireSessionStore()
        capability = resolve_wire_command_capability("find")
        transactional_lsid = "session-3"

        session = store.resolve_for_command(
            proxy._client,
            {
                "lsid": transactional_lsid,
                "find": "events",
                "startTransaction": True,
                "autocommit": False,
            },
            capability=capability,
        )
        assert session is not None
        self.assertTrue(session.transaction_active)
        self.assertEqual(store.abort_transaction({"lsid": transactional_lsid}), {"ok": 1.0})
        self.assertFalse(session.transaction_active)

        second = store.resolve_for_command(
            proxy._client,
            {"lsid": {"id": "session-4"}, "find": "events"},
            capability=capability,
        )
        assert second is not None

        store.clear()
        self.assertFalse(session.active)
        self.assertFalse(second.active)

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

    def test_authentication_service_supports_scram_sasl_conversation(self):
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
        connection = WireConnectionContext(connection_id=9)

        start = service.sasl_start(
            {
                "saslStart": 1,
                "mechanism": "SCRAM-SHA-256",
                "payload": Binary(b"n,,n=ada,r=clientnonce"),
                "db": "admin",
            },
            connection=connection,
        )

        self.assertEqual(start["ok"], 1.0)
        self.assertFalse(start["done"])
        conversation_id = start["conversationId"]
        server_first = bytes(start["payload"]).decode("utf-8")
        self.assertIn("r=clientnonce", server_first)
        client_final = build_scram_client_final(
            password="secret",
            mechanism="SCRAM-SHA-256",
            client_first_bare="n=ada,r=clientnonce",
            server_first_message=server_first,
            combined_nonce=next(
                part[2:]
                for part in server_first.split(",")
                if part.startswith("r=")
            ),
        )

        continue_response = service.sasl_continue(
            {
                "saslContinue": 1,
                "conversationId": conversation_id,
                "payload": Binary(client_final.payload),
            },
            connection=connection,
        )

        self.assertEqual(continue_response["ok"], 1.0)
        self.assertTrue(continue_response["done"])
        self.assertEqual(
            connection.authenticated_users,
            [{"user": "ada", "db": "admin", "mechanism": "SCRAM-SHA-256"}],
        )
        self.assertEqual(connection.authenticated_roles, [{"role": "root", "db": "admin"}])
        self.assertEqual(connection.auth_conversations, {})

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

    def test_authentication_service_accepts_dollar_db_and_default_admin_database(self):
        service = WireAuthenticationService(
            (
                WireAuthUser(
                    username="ada",
                    password="secret",
                    source="admin",
                    mechanisms=("SCRAM-SHA-256",),
                ),
            )
        )
        connection = WireConnectionContext(connection_id=3)

        with self.assertRaisesRegex(OperationFailure, "Authentication failed"):
            service.authenticate(
                {
                    "authenticate": 1,
                    "mechanism": "SCRAM-SHA-256",
                    "user": "ada",
                    "pwd": "secret",
                    "$db": "$external",
                },
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "Authentication failed"):
            service.authenticate(
                {
                    "authenticate": 1,
                    "mechanism": "SCRAM-SHA-256",
                    "user": "ada",
                    "pwd": "wrong",
                    "$db": "admin",
                },
                connection=connection,
            )

        response = service.authenticate(
            {
                "authenticate": 1,
                "mechanism": "SCRAM-SHA-256",
                "user": "ada",
                "pwd": "secret",
                "$db": "admin",
            },
            connection=connection,
        )
        self.assertEqual(response, {"ok": 1.0})
        self.assertEqual(connection.authenticated_users[0]["db"], "admin")

        service.logout(connection)
        response = service.authenticate(
            {
                "authenticate": 1,
                "mechanism": "SCRAM-SHA-256",
                "user": "ada",
                "pwd": "secret",
            },
            connection=connection,
        )
        self.assertEqual(response, {"ok": 1.0})
        self.assertEqual(connection.authenticated_users[0]["db"], "admin")


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

        with self.assertRaisesRegex(OperationFailure, "wire command name must be a non-empty string"):
            await proxy._executor.execute_command({1: "ping", "$db": "admin"}, connection=connection)  # type: ignore[dict-item]

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

    async def test_executor_validates_passthrough_command_payload_shapes_early(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        with self.assertRaisesRegex(OperationFailure, "wire find requires a non-empty collection name"):
            await proxy._executor.execute_command(
                {"find": "", "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire listDatabases requires the command value 1"):
            await proxy._executor.execute_command(
                {"listDatabases": 0, "$db": "admin"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire explain requires a command document"):
            await proxy._executor.execute_command(
                {"explain": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire explain verbosity must be a string"):
            await proxy._executor.execute_command(
                {"explain": {"find": "events"}, "verbosity": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire aggregate requires a pipeline list"):
            await proxy._executor.execute_command(
                {"aggregate": "events", "pipeline": {}, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire aggregate cursor must be a document"):
            await proxy._executor.execute_command(
                {"aggregate": "events", "pipeline": [], "cursor": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire aggregate allowDiskUse must be a bool"):
            await proxy._executor.execute_command(
                {"aggregate": "events", "pipeline": [], "allowDiskUse": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire aggregate let must be a document"):
            await proxy._executor.execute_command(
                {"aggregate": "events", "pipeline": [], "let": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire insert requires a non-empty documents list"):
            await proxy._executor.execute_command(
                {"insert": "events", "documents": [], "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire update requires a non-empty updates list"):
            await proxy._executor.execute_command(
                {"update": "events", "updates": {}, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire delete requires a non-empty deletes list"):
            await proxy._executor.execute_command(
                {"delete": "events", "deletes": [], "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire count skip must be a non-negative integer"):
            await proxy._executor.execute_command(
                {"count": "events", "skip": -1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire count filter_spec must be a dict"):
            await proxy._executor.execute_command(
                {"count": "events", "query": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire distinct requires a non-empty key string"):
            await proxy._executor.execute_command(
                {"distinct": "events", "key": "", "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire find projection must be a dict"):
            await proxy._executor.execute_command(
                {"find": "events", "projection": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire find sort must be a document"):
            await proxy._executor.execute_command(
                {"find": "events", "sort": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire find batch_size must be >= 0"):
            await proxy._executor.execute_command(
                {"find": "events", "batchSize": -1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire validate comment must be a string"):
            await proxy._executor.execute_command(
                {"validate": "events", "comment": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire connectionStatus showPrivileges must be a bool"):
            await proxy._executor.execute_command(
                {"connectionStatus": 1, "showPrivileges": 1, "$db": "admin"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire dbStats scale must be a positive integer"):
            await proxy._executor.execute_command(
                {"dbStats": 1, "scale": 0, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire dbHash collections must be a list of non-empty strings"):
            await proxy._executor.execute_command(
                {"dbHash": 1, "collections": ["events", ""], "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire dbHash comment must be a string"):
            await proxy._executor.execute_command(
                {"dbHash": 1, "comment": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire collStats scale must be a positive integer"):
            await proxy._executor.execute_command(
                {"collStats": "events", "scale": 0, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire profile level must be an integer"):
            await proxy._executor.execute_command(
                {"profile": "bad", "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire profile slowms must be an integer"):
            await proxy._executor.execute_command(
                {"profile": 2, "slowms": "bad", "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire listCollections comment must be a string"):
            await proxy._executor.execute_command(
                {"listCollections": 1, "comment": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire listIndexes comment must be a string"):
            await proxy._executor.execute_command(
                {"listIndexes": "events", "comment": 1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire createIndexes each index specification must be a dict"):
            await proxy._executor.execute_command(
                {"createIndexes": "events", "indexes": [1], "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire dropIndexes index must be '\\*', a name, or a key specification"):
            await proxy._executor.execute_command(
                {"dropIndexes": "events", "index": 1.5, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire findAndModify let must be a dict"):
            await proxy._executor.execute_command(
                {"findAndModify": "events", "query": {}, "update": {"$set": {"a": 1}}, "let": 1, "$db": "alpha"},
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

    async def test_executor_passthrough_admin_commands_materialize_cursor_and_document_shapes(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))

        class _Database:
            async def command(self, command_document, session=None):
                command_name = next(iter(command_document))
                if command_name == "listCollections":
                    return {
                        "cursor": {
                            "id": 0,
                            "ns": "alpha.$cmd.listCollections",
                            "firstBatch": [{"name": "events", "type": "collection"}],
                        },
                        "ok": 1.0,
                    }
                if command_name == "listIndexes":
                    return {
                        "cursor": {
                            "id": 0,
                            "ns": "alpha.$cmd.listIndexes.events",
                            "firstBatch": [{"name": "_id_", "key": {"_id": 1}}],
                        },
                        "ok": 1.0,
                    }
                if command_name == "dbStats":
                    return {"db": "alpha", "collections": 1, "ok": 1.0}
                if command_name == "findAndModify":
                    return {"value": {"_id": 1, "kind": "view"}, "ok": 1.0}
                raise AssertionError(command_name)

        with patch.object(proxy._client, "get_database", return_value=_Database()):
            collections = await proxy._executor.execute_command(
                {"listCollections": 1, "$db": "alpha"},
                connection=connection,
            )
            indexes = await proxy._executor.execute_command(
                {"listIndexes": "events", "$db": "alpha"},
                connection=connection,
            )
            db_stats = await proxy._executor.execute_command(
                {"dbStats": 1, "$db": "alpha"},
                connection=connection,
            )
            find_and_modify = await proxy._executor.execute_command(
                {"findAndModify": "events", "query": {}, "remove": True, "$db": "alpha"},
                connection=connection,
            )

        self.assertEqual(collections["cursor"]["firstBatch"], [{"name": "events", "type": "collection"}])
        self.assertEqual(indexes["cursor"]["firstBatch"], [{"name": "_id_", "key": {"_id": 1}}])
        self.assertEqual(db_stats, {"db": "alpha", "collections": 1, "ok": 1.0})
        self.assertEqual(find_and_modify, {"value": {"_id": 1, "kind": "view"}, "ok": 1.0})

    async def test_executor_passthrough_requires_auth_when_wire_auth_is_enabled(self):
        proxy = AsyncMongoEcoProxyServer(
            auth_users=(
                WireAuthUser(
                    username="ada",
                    password="secret",
                    source="admin",
                    mechanisms=("SCRAM-SHA-256",),
                ),
            )
        )
        connection = proxy._connections.create(("127.0.0.1", 27017))

        class _Database:
            async def command(self, command_document, session=None):
                return {"ok": 1.0}

        with patch.object(proxy._client, "get_database", return_value=_Database()):
            with self.assertRaisesRegex(OperationFailure, "Authentication required for listDatabases"):
                await proxy._executor.execute_command(
                    {"listDatabases": 1, "$db": "admin"},
                    connection=connection,
                )

            result = await proxy._executor.execute_command(
                {"connectionStatus": 1, "$db": "admin"},
                connection=connection,
            )

        self.assertEqual(result["ok"], 1.0)

    async def test_executor_connection_status_rewrites_auth_info_from_connection(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))
        connection.authenticate(
            username="ada",
            db="admin",
            mechanism="SCRAM-SHA-256",
            roles=({"role": "root", "db": "admin"},),
        )

        class _Database:
            async def command(self, command_document, session=None):
                return {
                    "authInfo": {
                        "authenticatedUsers": [{"user": "ignored"}],
                        "authenticatedUserRoles": [{"role": "ignored"}],
                        "authenticatedUserPrivileges": [{"resource": {"db": "admin"}}],
                    },
                    "ok": 1.0,
                }

        with patch.object(proxy._client, "get_database", return_value=_Database()):
            result = await proxy._executor.execute_command(
                {"connectionStatus": 1, "$db": "admin"},
                connection=connection,
            )

        self.assertEqual(
            result["authInfo"]["authenticatedUsers"],
            [{"user": "ada", "db": "admin", "mechanism": "SCRAM-SHA-256"}],
        )
        self.assertEqual(
            result["authInfo"]["authenticatedUserRoles"],
            [{"role": "root", "db": "admin"}],
        )
        self.assertEqual(result["authInfo"]["authenticatedUserPrivileges"], [])

    async def test_executor_handles_kill_logout_and_transaction_commands(self):
        proxy = AsyncMongoEcoProxyServer()
        connection = proxy._connections.create(("127.0.0.1", 27017))
        connection.authenticate(
            username="ada",
            db="admin",
            mechanism="SCRAM-SHA-256",
        )
        cursor_result = proxy._cursor_store.materialize_command_result(
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
        cursor_id = cursor_result["cursor"]["id"]
        lsid = {"id": "wire-txn"}
        session = proxy._session_store.resolve_for_command(
            proxy._client,
            {"lsid": lsid, "find": "events", "startTransaction": True, "autocommit": False},
            capability=resolve_wire_command_capability("find"),
        )
        assert session is not None

        killed = await proxy._executor.execute_command(
            {"killCursors": "events", "cursors": [cursor_id], "$db": "alpha"},
            connection=connection,
        )
        self.assertEqual(killed["cursorsKilled"], [cursor_id])

        committed = await proxy._executor.execute_command(
            {"commitTransaction": 1, "$db": "admin", "lsid": lsid},
            connection=connection,
        )
        self.assertEqual(committed, {"ok": 1.0})
        self.assertFalse(session.transaction_active)

        proxy._session_store.resolve_for_command(
            proxy._client,
            {"lsid": lsid, "find": "events", "startTransaction": True, "autocommit": False},
            capability=resolve_wire_command_capability("find"),
        )
        aborted = await proxy._executor.execute_command(
            {"abortTransaction": 1, "$db": "admin", "lsid": lsid},
            connection=connection,
        )
        self.assertEqual(aborted, {"ok": 1.0})
        self.assertFalse(session.transaction_active)

        logged_out = await proxy._executor.execute_command(
            {"logout": 1, "$db": "admin"},
            connection=connection,
        )
        self.assertEqual(logged_out, {"ok": 1.0})
        self.assertEqual(connection.authenticated_users, [])

    async def test_executor_validates_auth_session_and_cursor_shapes_early(self):
        proxy = AsyncMongoEcoProxyServer(
            auth_users=(
                WireAuthUser(
                    username="ada",
                    password="secret",
                    source="admin",
                    mechanisms=("SCRAM-SHA-256",),
                ),
            )
        )
        connection = proxy._connections.create(("127.0.0.1", 27017))

        with self.assertRaisesRegex(OperationFailure, "wire authenticate requires the command value 1"):
            await proxy._executor.execute_command(
                {"authenticate": 0, "mechanism": "SCRAM-SHA-256", "user": "ada", "pwd": "secret", "$db": "admin"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "authenticate requires mechanism"):
            await proxy._executor.execute_command(
                {"authenticate": 1, "user": "ada", "pwd": "secret", "$db": "admin"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire saslContinue requires the command value 1"):
            await proxy._executor.execute_command(
                {"saslContinue": 0, "conversationId": 1, "payload": Binary(b"abc"), "$db": "admin"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "saslContinue requires conversationId"):
            await proxy._executor.execute_command(
                {"saslContinue": 1, "conversationId": "bad", "payload": Binary(b"abc"), "$db": "admin"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire endSessions requires a list"):
            await proxy._executor.execute_command(
                {"endSessions": "bad", "$db": "admin"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire getMore cursor id must be an integer"):
            await proxy._executor.execute_command(
                {"getMore": "bad", "collection": "events", "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire getMore requires a non-empty collection name"):
            await proxy._executor.execute_command(
                {"getMore": 1, "collection": "", "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire getMore batchSize must be a non-negative integer"):
            await proxy._executor.execute_command(
                {"getMore": 1, "collection": "events", "batchSize": -1, "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire killCursors requires a cursors list"):
            await proxy._executor.execute_command(
                {"killCursors": "events", "cursors": "bad", "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire killCursors cursor ids must be integers"):
            await proxy._executor.execute_command(
                {"killCursors": "events", "cursors": ["bad"], "$db": "alpha"},
                connection=connection,
            )

        with self.assertRaisesRegex(OperationFailure, "wire transaction command requires lsid"):
            await proxy._executor.execute_command(
                {"commitTransaction": 1, "$db": "admin"},
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

    async def test_executor_normalizes_legacy_queries_rejects_invalid_shapes_and_updates_cursor_specs(self):
        with self.assertRaisesRegex(OperationFailure, "must be a document"):
            WireCommandExecutor._normalize_legacy_query_body([], number_to_return=None)  # type: ignore[arg-type]

        with self.assertRaisesRegex(OperationFailure, "wrapper must contain a document"):
            WireCommandExecutor._normalize_legacy_query_body(
                {"$query": []},
                number_to_return=None,
            )

        with self.assertRaisesRegex(OperationFailure, "must contain a document"):
            WireCommandExecutor._normalize_legacy_query_body(
                {},
                number_to_return=None,
            )

        normalized = WireCommandExecutor._normalize_legacy_query_body(
            {"aggregate": "events", "pipeline": [], "cursor": {}},
            number_to_return=5,
        )
        self.assertEqual(normalized["cursor"], {"batchSize": 5})

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
