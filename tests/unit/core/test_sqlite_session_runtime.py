import asyncio
from unittest.mock import Mock
import unittest

from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession


class SQLiteSessionRuntimeTests(unittest.TestCase):
    def test_session_runtime_tracks_transaction_ownership_and_bindings(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        try:
            runtime = engine._session_runtime
            session = ClientSession()
            other = ClientSession()

            runtime.create_session_state(session)
            runtime.create_session_state(other)

            state = session.get_engine_context(engine._engine_key())
            self.assertIsNotNone(state)
            self.assertTrue(state.connected)
            self.assertTrue(state.supports_transactions)
            self.assertFalse(state.transaction_active)
            self.assertIn("snapshot_version", state.metadata)

            session.start_transaction()
            self.assertEqual(engine._transaction_owner_session_id, session.session_id)
            self.assertTrue(session.get_engine_context(engine._engine_key()).transaction_active)

            with self.assertRaisesRegex(InvalidOperation, "another session"):
                runtime.require_connection(other)

            conn = runtime.require_connection(session)
            with runtime.bind_connection(conn):
                self.assertIs(runtime.require_connection(None), conn)

            session.commit_transaction()
            self.assertIsNone(engine._transaction_owner_session_id)
            self.assertFalse(session.get_engine_context(engine._engine_key()).transaction_active)
        finally:
            asyncio.run(engine.disconnect())

    def test_write_helpers_skip_manual_begin_commit_when_session_owns_transaction(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        try:
            runtime = engine._session_runtime
            session = ClientSession()
            runtime.create_session_state(session)
            session.start_transaction()

            conn = Mock()
            runtime.begin_write(conn, session)
            runtime.commit_write(conn, session)
            runtime.rollback_write(conn, session)

            conn.execute.assert_not_called()
            conn.commit.assert_not_called()
            conn.rollback.assert_not_called()

            runtime.begin_write(conn, None)
            runtime.commit_write(conn, None)
            runtime.rollback_write(conn, None)

            conn.execute.assert_called_once_with("BEGIN")
            conn.commit.assert_called_once_with()
            conn.rollback.assert_called_once_with()
        finally:
            asyncio.run(engine.disconnect())

