import unittest

from mongoeco.errors import ExecutionTimeout
from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.handshake import WireHandshakeService
from mongoeco.wire.executor import WireCommandExecutor
from mongoeco.wire.proxy import AsyncMongoEcoProxyServer
from mongoeco.wire.surface import WireSurface
from mongoeco.wire.sessions import WireSessionStore


class WireProxyUnitTests(unittest.TestCase):
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
