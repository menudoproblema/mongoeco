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

        session.end_transaction()
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

        self.assertEqual(session.get_engine_state(f"memory:{id(engine)}"), {"connected": False})

    def test_async_client_start_session_binds_sqlite_engine_state(self):
        engine = SQLiteEngine()
        client = AsyncMongoClient(engine)

        session = client.start_session()

        self.assertEqual(
            session.get_engine_state(f"sqlite:{id(engine)}"),
            {"connected": False, "path": ":memory:"},
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

    def test_session_transaction_round_trip(self):
        session = ClientSession()

        session.start_transaction()
        self.assertTrue(session.transaction_active)

        session.end_transaction()
        self.assertFalse(session.transaction_active)
