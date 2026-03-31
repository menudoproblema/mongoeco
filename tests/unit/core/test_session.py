import asyncio
import unittest

from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.session import EngineTransactionContext, SessionState, TransactionState
from mongoeco.types import (
    ReadConcern,
    ReadPreference,
    ReadPreferenceMode,
    TransactionOptions,
    WriteConcern,
)


class ClientSessionTests(unittest.TestCase):
    def test_session_uses_explicit_internal_state_records(self):
        session = ClientSession()

        self.assertIsInstance(session._state, SessionState)
        self.assertIsInstance(session._state.transaction, TransactionState)

    def test_engine_context_round_trips_between_internal_and_public_state(self):
        session = ClientSession()
        session.bind_engine_context(
            EngineTransactionContext(
                engine_key="sqlite:test",
                connected=True,
                supports_transactions=True,
                metadata={"path": ":memory:"},
            )
        )

        internal = session.get_engine_context("sqlite:test")

        self.assertIsNotNone(internal)
        self.assertTrue(internal.connected)
        self.assertEqual(
            session.get_engine_state("sqlite:test"),
            {
                "connected": True,
                "supports_transactions": True,
                "path": ":memory:",
            },
        )

    def test_engine_transaction_context_handles_non_dicts_instances_and_updates(self):
        existing = EngineTransactionContext(engine_key="memory", connected=True)

        self.assertIs(
            EngineTransactionContext.from_document("memory", existing),
            existing,
        )
        self.assertEqual(
            EngineTransactionContext.from_document("memory", "bad-state").to_document(),
            {},
        )

        existing.update(
            connected=0,
            supports_transactions=1,
            transaction_active=1,
            snapshot_version=3,
        )
        self.assertEqual(
            existing.to_document(),
            {
                "connected": False,
                "supports_transactions": True,
                "transaction_active": True,
                "snapshot_version": 3,
            },
        )

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
        self.assertIsNone(session.transaction_options)

    def test_session_start_transaction_uses_default_transaction_options(self):
        session = ClientSession(
            default_transaction_options=TransactionOptions(
                read_concern=ReadConcern("majority"),
                write_concern=WriteConcern("majority", j=True),
                read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
                max_commit_time_ms=250,
            )
        )

        session.start_transaction()

        self.assertEqual(
            session.transaction_options,
            TransactionOptions(
                read_concern=ReadConcern("majority"),
                write_concern=WriteConcern("majority", j=True),
                read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
                max_commit_time_ms=250,
            ),
        )

    def test_session_start_transaction_allows_option_overrides(self):
        session = ClientSession(
            default_transaction_options=TransactionOptions(
                read_concern=ReadConcern("majority"),
                write_concern=WriteConcern("majority"),
                read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
            )
        )

        session.start_transaction(
            read_concern=ReadConcern("local"),
            write_concern=WriteConcern(1),
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
            max_commit_time_ms=500,
        )

        self.assertEqual(
            session.transaction_options,
            TransactionOptions(
                read_concern=ReadConcern("local"),
                write_concern=WriteConcern(1),
                read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
                max_commit_time_ms=500,
            ),
        )

    def test_closed_session_rejects_state_access(self):
        session = ClientSession()
        session.close()

        with self.assertRaises(InvalidOperation):
            session.bind_engine_state("memory", {})

        with self.assertRaises(InvalidOperation):
            session.get_engine_state("memory")

    def test_async_client_start_session_binds_engine_state(self):
        engine = MemoryEngine()
        client = AsyncMongoClient(
            engine,
            transaction_options=TransactionOptions(
                write_concern=WriteConcern("majority"),
                max_commit_time_ms=250,
            ),
        )

        session = client.start_session()

        self.assertEqual(
            session.get_engine_state(f"memory:{id(engine)}"),
            {
                "connected": False,
                "supports_transactions": True,
                "transaction_active": False,
                "snapshot_version": 0,
            },
        )
        self.assertEqual(
            session.default_transaction_options,
            TransactionOptions(
                write_concern=WriteConcern("majority"),
                max_commit_time_ms=250,
            ),
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
                "snapshot_version": 0,
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

    def test_end_transaction_is_commit_alias_and_close_is_idempotent(self):
        session = ClientSession()

        session.start_transaction()
        session.end_transaction()
        self.assertFalse(session.transaction_active)
        self.assertEqual(session.transaction_number, 1)

        session.close()
        session.close()
        self.assertTrue(session.has_ended)

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
        self.assertIsNone(session.transaction_options)

    def test_session_with_transaction_accepts_transaction_option_overrides(self):
        session = ClientSession()

        def _run(active: ClientSession):
            self.assertEqual(
                active.transaction_options,
                TransactionOptions(
                    read_concern=ReadConcern("local"),
                    write_concern=WriteConcern(1),
                    read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
                    max_commit_time_ms=50,
                ),
            )
            return "ok"

        result = session.with_transaction(
            _run,
            read_concern=ReadConcern("local"),
            write_concern=WriteConcern(1),
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
            max_commit_time_ms=50,
        )

        self.assertEqual(result, "ok")

    def test_session_with_transaction_respects_callback_committing_early(self):
        session = ClientSession()

        def _run(active: ClientSession):
            active.commit_transaction()
            return "done"

        self.assertEqual(session.with_transaction(_run), "done")
        self.assertFalse(session.in_transaction)

    def test_session_with_transaction_aborts_on_error(self):
        session = ClientSession()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            session.with_transaction(lambda active: (_ for _ in ()).throw(RuntimeError("boom")))

        self.assertFalse(session.in_transaction)
        self.assertEqual(session.transaction_number, 1)

    def test_async_session_with_transaction_respects_callback_ending_transaction(self):
        session = ClientSession()

        async def _run(active: ClientSession):
            active.abort_transaction()
            return "done"

        self.assertEqual(asyncio.run(session.with_transaction(_run)), "done")
        self.assertFalse(session.in_transaction)

    def test_async_session_with_transaction_aborts_on_async_error(self):
        session = ClientSession()

        async def _run(active: ClientSession):
            raise RuntimeError(f"boom:{active.session_id}")

        with self.assertRaisesRegex(RuntimeError, "boom:"):
            asyncio.run(session.with_transaction(_run))

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

    def test_start_transaction_rolls_back_counter_if_hook_fails(self):
        session = ClientSession()
        session.register_transaction_hooks("memory", start=lambda active: (_ for _ in ()).throw(RuntimeError("boom")))

        with self.assertRaisesRegex(RuntimeError, "boom"):
            session.start_transaction()

        self.assertEqual(session.transaction_number, 0)
        self.assertFalse(session.in_transaction)

    def test_commit_and_abort_keep_transaction_open_if_hook_fails(self):
        for phase in ("commit", "abort"):
            with self.subTest(phase=phase):
                session = ClientSession()
                session.register_transaction_hooks(
                    "memory",
                    **{phase: lambda active: (_ for _ in ()).throw(RuntimeError(phase))},
                )
                session.start_transaction()

                with self.assertRaisesRegex(RuntimeError, phase):
                    getattr(session, f"{phase}_transaction")()

                self.assertTrue(session.in_transaction)
                self.assertIsNotNone(session.transaction_options)

    def test_close_marks_session_closed_even_if_abort_hook_fails(self):
        session = ClientSession()
        session.register_transaction_hooks("memory", abort=lambda active: (_ for _ in ()).throw(RuntimeError("abort")))
        session.start_transaction()

        with self.assertRaisesRegex(RuntimeError, "abort"):
            session.close()

        self.assertTrue(session.has_ended)
        self.assertFalse(session.in_transaction)
        self.assertEqual(session.engine_state, {})
