import unittest

from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession


class ClientSessionTests(unittest.TestCase):
    def test_session_can_bind_engine_state_and_track_transaction(self):
        session = ClientSession()

        session.bind_engine_state("memory", {"connection": 1})
        session.start_transaction()

        self.assertEqual(session.get_engine_state("memory"), {"connection": 1})
        self.assertTrue(session.transaction_active)
        self.assertTrue(session.in_transaction)
        self.assertEqual(session.transaction_number, 1)

        session.commit_transaction()
        self.assertFalse(session.transaction_active)

    def test_closed_session_rejects_state_access(self):
        session = ClientSession()
        session.close()

        with self.assertRaises(InvalidOperation):
            session.bind_engine_state("memory", {})

        with self.assertRaises(InvalidOperation):
            session.get_engine_state("memory")

    def test_async_client_start_session_binds_engine_state(self):
        engine = MemoryEngine()
        client = AsyncMongoClient(engine)

        session = client.start_session()

        self.assertEqual(
            session.get_engine_state(f"memory:{id(engine)}"),
            {"connected": False, "supports_transactions": False},
        )

    def test_async_client_start_session_binds_sqlite_engine_state(self):
        engine = SQLiteEngine()
        client = AsyncMongoClient(engine)

        session = client.start_session()

        self.assertEqual(
            session.get_engine_state(f"sqlite:{id(engine)}"),
            {
                "connected": False,
                "path": ":memory:",
                "supports_transactions": True,
                "transaction_active": False,
            },
        )

    def test_session_distinguishes_multiple_engine_instances_of_same_kind(self):
        session = ClientSession()
        memory_a = MemoryEngine()
        memory_b = MemoryEngine()
        sqlite_a = SQLiteEngine(path=":memory:")
        sqlite_b = SQLiteEngine(path=":memory:")

        memory_a.create_session_state(session)
        memory_b.create_session_state(session)
        sqlite_a.create_session_state(session)
        sqlite_b.create_session_state(session)

        self.assertIn(f"memory:{id(memory_a)}", session.engine_state)
        self.assertIn(f"memory:{id(memory_b)}", session.engine_state)
        self.assertIn(f"sqlite:{id(sqlite_a)}", session.engine_state)
        self.assertIn(f"sqlite:{id(sqlite_b)}", session.engine_state)

    def test_session_context_manager_closes_session(self):
        session = ClientSession()

        with session as active:
            self.assertIs(active, session)
            self.assertTrue(session.active)

        self.assertFalse(session.active)
        self.assertTrue(session.has_ended)

    def test_session_transaction_round_trip(self):
        session = ClientSession()

        session.start_transaction()
        self.assertTrue(session.transaction_active)

        session.abort_transaction()
        self.assertFalse(session.transaction_active)

    def test_session_rejects_nested_transactions(self):
        session = ClientSession()

        session.start_transaction()

        with self.assertRaises(InvalidOperation):
            session.start_transaction()

    def test_session_rejects_commit_without_active_transaction(self):
        session = ClientSession()

        with self.assertRaises(InvalidOperation):
            session.commit_transaction()

    def test_session_rejects_abort_without_active_transaction(self):
        session = ClientSession()

        with self.assertRaises(InvalidOperation):
            session.abort_transaction()

    def test_session_with_transaction_commits_on_success(self):
        session = ClientSession()

        result = session.with_transaction(lambda active: ("ok", active.transaction_number))

        self.assertEqual(result, ("ok", 1))
        self.assertFalse(session.in_transaction)
        self.assertEqual(session.transaction_number, 1)

    def test_session_with_transaction_aborts_on_error(self):
        session = ClientSession()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            session.with_transaction(lambda active: (_ for _ in ()).throw(RuntimeError("boom")))

        self.assertFalse(session.in_transaction)
        self.assertEqual(session.transaction_number, 1)

    def test_session_close_aborts_active_transaction(self):
        session = ClientSession()
        phases: list[str] = []
        session.register_transaction_hooks("memory", abort=lambda active: phases.append(active.session_id))

        session.start_transaction()
        session.close()

        self.assertEqual(phases, [session.session_id])
        self.assertFalse(session.active)
        self.assertFalse(session.in_transaction)
