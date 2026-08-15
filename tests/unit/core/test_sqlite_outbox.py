import asyncio
import json
import multiprocessing
import os
import queue
import sqlite3
import tempfile
import threading
import time
import unittest

from pathlib import Path
from unittest.mock import patch

# Outbox sequence values and synchronous cleanup are the contract exercised
# by this unittest-based module.
# ruff: noqa: ASYNC240, PLR2004, PT027
from mongoeco.change_streams import ChangeStreamHub
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.engines._sqlite_outbox import (
    acquire_consumer_lease,
    append_change,
    checkpoint_consumer,
    compact_change_outbox,
    consumer_checkpoint,
    ensure_change_outbox_schema,
    expire_ephemeral_consumers,
    read_committed_changes,
    register_consumer,
    release_consumer_lease,
    renew_consumer_lease,
    renew_ephemeral_consumers,
    unregister_consumer,
)
from mongoeco.engines.adapter import adapt_engine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.session import ClientSession


def _dispatch_outbox_in_subprocess(
    database_path,
    consumer_id,
    entered,
    release,
    deliveries,
):
    async def run():
        engine = SQLiteEngine(database_path)
        await engine.connect()
        try:
            def consume(change):
                deliveries.put((os.getpid(), change.sequence))
                entered.set()
                if release is not None:
                    release.wait(timeout=5)

            engine.dispatch_committed_changes(consumer_id, consume)
        finally:
            await engine.disconnect()

    asyncio.run(run())


def _crash_during_outbox_delivery(
    database_path,
    consumer_id,
    delivered_connection,
):
    async def run():
        engine = SQLiteEngine(database_path)
        await engine.connect()

        def crash(change):
            delivered_connection.send(change.sequence)
            delivered_connection.close()
            os._exit(23)

        engine.dispatch_committed_changes(consumer_id, crash)

    asyncio.run(run())


class SQLiteOutboxTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnected_change_delivery_lifecycle_is_explicit(self):
        engine = SQLiteEngine()

        with (
            self.assertRaisesRegex(RuntimeError, 'must be connected'),
            engine._change_delivery_storage(),
        ):
            pass

        engine.unregister_change_consumer('already-disconnected')

    async def test_ephemeral_consumer_registration_validates_ownership(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            with self.assertRaisesRegex(ValueError, 'durable'):
                register_consumer(
                    conn,
                    'durable-with-owner',
                    initial_checkpoint=0,
                    durable=True,
                    owner_instance='process',
                    ephemeral_ttl_seconds=60,
                )
            with self.assertRaisesRegex(TypeError, 'provided together'):
                register_consumer(
                    conn,
                    'owner-without-ttl',
                    initial_checkpoint=0,
                    durable=False,
                    owner_instance='process',
                )
            with self.assertRaisesRegex(ValueError, 'must be positive'):
                register_consumer(
                    conn,
                    'invalid-ttl',
                    initial_checkpoint=0,
                    durable=False,
                    owner_instance='process',
                    ephemeral_ttl_seconds=0,
                )
        finally:
            conn.close()

    async def test_file_outbox_control_plane_uses_a_dedicated_connection(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = SQLiteEngine(str(Path(temp_dir) / 'mongoeco.sqlite'))
            await engine.connect()
            try:
                data_connection = engine._connection
                control_connection = engine._change_delivery_connection
                assert data_connection is not None
                assert control_connection is not None
                assert control_connection is not data_connection

                data_connection.execute('BEGIN')
                engine.register_change_consumer(
                    'dedicated-control-plane',
                    initial_checkpoint=0,
                )

                assert data_connection.in_transaction
                data_connection.rollback()
                assert (
                    consumer_checkpoint(
                        control_connection,
                        'dedicated-control-plane',
                    )
                    == 0
                )
            finally:
                await engine.disconnect()

    async def test_memory_registration_does_not_commit_data_transaction(
        self,
    ):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            connection = engine._connection
            assert connection is not None
            connection.execute('BEGIN')

            engine.register_change_consumer('transactional-registration')

            assert connection.in_transaction
            connection.rollback()
            assert connection.execute(
                'SELECT COUNT(*) FROM change_outbox_consumers '
                'WHERE consumer_id = ?',
                ('transactional-registration',),
            ).fetchone() == (0,)

            with self.assertRaisesRegex(
                InvalidOperation,
                'cannot share an active SQLite data transaction',
            ):
                connection.execute('BEGIN')
                engine.dispatch_committed_changes(
                    'transactional-registration',
                    lambda _change: None,
                )
            connection.rollback()
        finally:
            await engine.disconnect()

    async def test_heartbeat_failure_prevents_checkpointing_success(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = SQLiteEngine(str(Path(temp_dir) / 'mongoeco.sqlite'))
            await engine.connect()
            try:
                engine.register_change_consumer(
                    'heartbeat-failure',
                    initial_checkpoint=0,
                    durable=True,
                )
                await engine.insert_document(
                    'db',
                    'items',
                    {'_id': 'one'},
                    overwrite=False,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=ChangePublicationPolicy.EMIT,
                        change_operation_type='insert',
                    ),
                )
                delivered: list[int] = []

                def slow_consumer(change):
                    delivered.append(change.sequence)
                    time.sleep(0.03)

                with (
                    patch(
                        'mongoeco.engines.sqlite.'
                        '_CHANGE_DISPATCH_HEARTBEAT_SECONDS',
                        0.001,
                    ),
                    patch(
                        'mongoeco.engines.sqlite.'
                        '_sqlite_renew_consumer_lease',
                        side_effect=RuntimeError('heartbeat failed'),
                    ),
                    self.assertRaisesRegex(
                        RuntimeError,
                        'heartbeat failed',
                    ),
                ):
                    engine.dispatch_committed_changes(
                        'heartbeat-failure',
                        slow_consumer,
                    )

                assert delivered == [1]
                control = engine._change_delivery_connection
                assert control is not None
                assert consumer_checkpoint(control, 'heartbeat-failure') == 0
            finally:
                await engine.disconnect()

    async def test_failed_schema_migration_rolls_back_to_outer_savepoint(self):
        conn = sqlite3.connect(':memory:')
        try:
            conn.execute('BEGIN')
            conn.execute('CREATE VIEW change_outbox_identities AS SELECT 1')

            with self.assertRaises(sqlite3.OperationalError):
                ensure_change_outbox_schema(conn)

            assert conn.in_transaction
            assert conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE "
                "type = 'table' AND name = 'change_outbox'",
            ).fetchone() == (0,)
            conn.rollback()
        finally:
            conn.close()

    async def test_failed_schema_migration_rolls_back_its_transaction(self):
        conn = sqlite3.connect(':memory:')
        try:
            conn.execute('CREATE VIEW change_outbox_identities AS SELECT 1')

            with self.assertRaises(sqlite3.OperationalError):
                ensure_change_outbox_schema(conn)

            assert not conn.in_transaction
            assert conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE "
                "type = 'table' AND name = 'change_outbox'",
            ).fetchone() == (0,)
        finally:
            conn.close()

    async def test_schema_migrates_and_backfills_legacy_live_identities(self):
        conn = sqlite3.connect(':memory:')
        try:
            conn.execute(
                '''
                CREATE TABLE change_outbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_id TEXT NOT NULL,
                    event_index INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    payload TEXT,
                    UNIQUE (operation_id, event_index)
                )
                ''',
            )
            conn.execute(
                '''
                CREATE TABLE change_outbox_identities (
                    operation_id TEXT NOT NULL,
                    event_index INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    PRIMARY KEY (operation_id, event_index),
                    UNIQUE (sequence)
                )
                ''',
            )
            conn.execute(
                'INSERT INTO change_outbox '
                "VALUES (1, 'legacy', 0, 'gap', NULL)",
            )

            ensure_change_outbox_schema(conn)

            columns = {
                row[1]
                for row in conn.execute(
                    'PRAGMA table_info(change_outbox_identities)',
                )
            }
            identity = conn.execute(
                'SELECT event_kind, effect_hash '
                'FROM change_outbox_identities WHERE operation_id = ?',
                ('legacy',),
            ).fetchone()
            assert {'event_kind', 'effect_hash'} <= columns
            assert identity is not None
            assert identity[0] == 'gap'
            assert identity[1] is None
            assert conn.execute(
                'SELECT version FROM mongoeco_schema_migrations '
                "WHERE component = 'change_outbox'",
            ).fetchone() == (4,)
            consumer_columns = {
                row[1]
                for row in conn.execute(
                    'PRAGMA table_info(change_outbox_consumers)',
                )
            }
            assert {
                'lease_owner',
                'lease_generation',
                'lease_expires_at_epoch',
                'owner_instance',
                'registration_expires_at_epoch',
            } <= consumer_columns
        finally:
            conn.close()

    async def test_expired_ephemeral_consumers_are_reclaimed_only_by_ttl(
        self,
    ):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            register_consumer(
                conn,
                'ephemeral',
                initial_checkpoint=0,
                durable=False,
                owner_instance='process-one',
                ephemeral_ttl_seconds=60,
            )
            register_consumer(
                conn,
                'durable',
                initial_checkpoint=0,
                durable=True,
            )
            conn.execute(
                'UPDATE change_outbox_consumers '
                'SET registration_expires_at_epoch = 0',
            )

            assert expire_ephemeral_consumers(conn) == 1
            assert conn.execute(
                'SELECT consumer_id FROM change_outbox_consumers',
            ).fetchall() == [('durable',)]
        finally:
            conn.close()

    async def test_active_dispatch_lease_protects_expired_registration(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            register_consumer(
                conn,
                'leased-ephemeral',
                initial_checkpoint=0,
                durable=False,
                owner_instance='process-one',
                ephemeral_ttl_seconds=60,
            )
            generation = acquire_consumer_lease(
                conn,
                'leased-ephemeral',
                owner='dispatcher',
                now_epoch=time.time(),
                ttl_seconds=60,
            )
            assert generation is not None
            conn.execute(
                'UPDATE change_outbox_consumers '
                'SET registration_expires_at_epoch = 0',
            )

            assert expire_ephemeral_consumers(conn) == 0
            assert consumer_checkpoint(conn, 'leased-ephemeral') == 0
            release_consumer_lease(
                conn,
                'leased-ephemeral',
                owner='dispatcher',
                generation=generation,
            )
            assert expire_ephemeral_consumers(conn) == 1
        finally:
            conn.close()

    async def test_ephemeral_registration_renewal_is_owner_scoped(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            for owner in ('process-one', 'process-two'):
                register_consumer(
                    conn,
                    owner,
                    initial_checkpoint=0,
                    durable=False,
                    owner_instance=owner,
                    ephemeral_ttl_seconds=60,
                )

            renewed = renew_ephemeral_consumers(
                conn,
                owner_instance='process-one',
                now_epoch=1_000,
                ttl_seconds=30,
            )

            assert renewed == 1
            rows = dict(
                conn.execute(
                    'SELECT consumer_id, registration_expires_at_epoch '
                    'FROM change_outbox_consumers',
                ).fetchall(),
            )
            assert rows['process-one'] == 1_030
            assert rows['process-two'] != 1_030
            with self.assertRaisesRegex(ValueError, 'owner'):
                renew_ephemeral_consumers(
                    conn,
                    owner_instance='',
                    now_epoch=1_000,
                    ttl_seconds=30,
                )
            with self.assertRaisesRegex(ValueError, 'TTL'):
                renew_ephemeral_consumers(
                    conn,
                    owner_instance='process-one',
                    now_epoch=1_000,
                    ttl_seconds=0,
                )
        finally:
            conn.close()

    async def test_schema_migration_is_serialized_across_connections(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / 'migration.sqlite')
            first = sqlite3.connect(path, check_same_thread=False)
            second = sqlite3.connect(path, check_same_thread=False)
            barrier = threading.Barrier(2)
            errors = []

            def migrate(conn):
                try:
                    barrier.wait()
                    ensure_change_outbox_schema(conn)
                except BaseException as exc:
                    errors.append(exc)

            workers = [
                threading.Thread(target=migrate, args=(first,)),
                threading.Thread(target=migrate, args=(second,)),
            ]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()

            try:
                assert errors == []
                assert first.execute(
                    'SELECT version FROM mongoeco_schema_migrations '
                    "WHERE component = 'change_outbox'",
                ).fetchone() == (4,)
            finally:
                first.close()
                second.close()

    async def test_schema_rejects_a_future_version_without_downgrade(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            conn.execute(
                'UPDATE mongoeco_schema_migrations SET version = 99 '
                "WHERE component = 'change_outbox'",
            )
            conn.commit()

            with self.assertRaisesRegex(
                OperationFailure,
                'newer than supported',
            ):
                ensure_change_outbox_schema(conn)

            assert conn.execute(
                'SELECT version FROM mongoeco_schema_migrations '
                "WHERE component = 'change_outbox'",
            ).fetchone() == (99,)
        finally:
            conn.close()

    async def test_schema_rejects_a_negative_version_without_mutation(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            conn.execute(
                'UPDATE mongoeco_schema_migrations SET version = -1 '
                "WHERE component = 'change_outbox'",
            )
            conn.commit()

            with self.assertRaisesRegex(
                OperationFailure,
                'cannot be negative',
            ):
                ensure_change_outbox_schema(conn)

            assert conn.execute(
                'SELECT version FROM mongoeco_schema_migrations '
                "WHERE component = 'change_outbox'",
            ).fetchone() == (-1,)
        finally:
            conn.close()

    async def test_schema_infers_current_and_v2_legacy_layouts(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            conn.execute(
                "DELETE FROM mongoeco_schema_migrations WHERE component = "
                "'change_outbox'",
            )
            ensure_change_outbox_schema(conn)
            assert conn.execute(
                'SELECT version FROM mongoeco_schema_migrations '
                "WHERE component = 'change_outbox'",
            ).fetchone() == (4,)

            conn.execute(
                'ALTER TABLE change_outbox_consumers '
                'DROP COLUMN owner_instance',
            )
            conn.execute(
                'ALTER TABLE change_outbox_consumers '
                'DROP COLUMN registration_expires_at_epoch',
            )
            conn.execute(
                "DELETE FROM mongoeco_schema_migrations WHERE component = "
                "'change_outbox'",
            )
            ensure_change_outbox_schema(conn)
            assert {
                'owner_instance',
                'registration_expires_at_epoch',
            } <= {
                row[1]
                for row in conn.execute(
                    'PRAGMA table_info(change_outbox_consumers)',
                )
            }

            conn.execute('DROP TABLE change_outbox_consumers')
            conn.execute(
                """
                CREATE TABLE change_outbox_consumers (
                    consumer_id TEXT PRIMARY KEY,
                    checkpoint INTEGER NOT NULL,
                    durable INTEGER NOT NULL,
                    updated_at_epoch REAL NOT NULL
                )
                """,
            )
            conn.execute(
                "DELETE FROM mongoeco_schema_migrations WHERE component = "
                "'change_outbox'",
            )
            ensure_change_outbox_schema(conn)

            columns = {
                row[1]
                for row in conn.execute(
                    'PRAGMA table_info(change_outbox_consumers)',
                )
            }
            assert 'lease_generation' in columns
            assert conn.execute(
                'SELECT version FROM mongoeco_schema_migrations '
                "WHERE component = 'change_outbox'",
            ).fetchone() == (4,)
        finally:
            conn.close()

    async def test_consumer_lease_fences_stale_checkpoint_and_renews_owner(
        self,
    ):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            register_consumer(
                conn,
                'leased',
                initial_checkpoint=0,
                durable=True,
            )
            generation = acquire_consumer_lease(
                conn,
                'leased',
                owner='owner',
                now_epoch=100,
                ttl_seconds=30,
            )
            assert generation is not None
            assert (
                acquire_consumer_lease(
                    conn,
                    'leased',
                    owner='competitor',
                    now_epoch=110,
                    ttl_seconds=30,
                )
                is None
            )
            assert renew_consumer_lease(
                conn,
                'leased',
                ('owner', generation),
                now_epoch=110,
                ttl_seconds=30,
            )

            with self.assertRaisesRegex(TypeError, 'provided together'):
                checkpoint_consumer(
                    conn,
                    'leased',
                    0,
                    lease_owner='owner',
                )
            release_consumer_lease(
                conn,
                'leased',
                owner='owner',
                generation=generation,
            )
            with self.assertRaisesRegex(OperationFailure, 'lease was lost'):
                checkpoint_consumer(
                    conn,
                    'leased',
                    0,
                    lease_owner='owner',
                    lease_generation=generation,
                )
            assert not renew_consumer_lease(
                conn,
                'leased',
                ('owner', generation),
                now_epoch=120,
                ttl_seconds=30,
            )
        finally:
            conn.close()

    async def test_legacy_compacted_identity_fails_closed_on_replay(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.RECORD_GAP,
                change_operation_type='insert',
            )
            conn.execute(
                'INSERT INTO change_outbox_identities '
                '(operation_id, event_index, sequence) VALUES (?, 0, 1)',
                (context.operation_id,),
            )

            with self.assertRaisesRegex(OperationFailure, 'legacy compacted'):
                append_change(
                    conn,
                    context=context,
                    operation_type='insert',
                    db_name='db',
                    coll_name='items',
                    document_key={'_id': 1},
                    full_document=None,
                    serialize_document=json.dumps,
                )
        finally:
            conn.close()

    async def test_outbox_consumer_errors_and_retirement_are_explicit(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            assert compact_change_outbox(conn, max_entries=10) == 0
            with self.assertRaisesRegex(RuntimeError, 'registered'):
                consumer_checkpoint(conn, 'missing')
            with self.assertRaisesRegex(RuntimeError, 'checkpointing'):
                checkpoint_consumer(conn, 'missing', 1)

            register_consumer(
                conn,
                'durable',
                initial_checkpoint=0,
                durable=True,
            )
            unregister_consumer(
                conn,
                'durable',
                include_durable=False,
            )
            assert consumer_checkpoint(conn, 'durable') == 0
            unregister_consumer(
                conn,
                'durable',
                include_durable=True,
            )
            with self.assertRaisesRegex(RuntimeError, 'registered'):
                consumer_checkpoint(conn, 'durable')
        finally:
            conn.close()

    async def test_read_rejects_checkpoint_behind_compacted_floor(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.RECORD_GAP,
                change_operation_type='insert',
            )
            append_change(
                conn,
                context=context,
                event_index=0,
                operation_type='insert',
                db_name='db',
                coll_name='items',
                document_key={'_id': 1},
                full_document=None,
                serialize_document=json.dumps,
            )

            with self.assertRaisesRegex(OperationFailure, 'pruned through'):
                read_committed_changes(
                    conn,
                    after_sequence=0,
                    deserialize_document=json.loads,
                )
        finally:
            conn.close()

    async def test_registration_rejects_checkpoint_behind_compacted_floor(
        self,
    ):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.RECORD_GAP,
                change_operation_type='insert',
            )
            append_change(
                conn,
                context=context,
                operation_type='insert',
                db_name='db',
                coll_name='items',
                document_key={'_id': 1},
                full_document=None,
                serialize_document=json.dumps,
            )

            with self.assertRaisesRegex(OperationFailure, 'behind pruned'):
                register_consumer(
                    conn,
                    'late',
                    initial_checkpoint=0,
                    durable=False,
                )
        finally:
            conn.close()

    async def test_transaction_abort_rolls_back_document_and_outbox_row(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            session = ClientSession()
            engine.create_session_state(session)
            session.start_transaction()
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                session=session,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type='insert',
            )

            outcome = await engine.insert_document(
                'db',
                'items',
                {'_id': 'rolled-back'},
                overwrite=False,
                operation_context=context,
            )

            assert outcome.commit_sequence == 1
            session.abort_transaction()
            conn = engine._require_connection()
            row = conn.execute(
                'SELECT COUNT(*) FROM change_outbox',
            ).fetchone()
            assert row[0] == 0
            document = await engine.get_document(
                'db',
                'items',
                'rolled-back',
            )
            assert document is None
        finally:
            await engine.disconnect()

    async def test_committed_row_replays_after_restart_exactly_once(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            database_path = str(temp_path / 'mongoeco.sqlite')
            journal_path = str(temp_path / 'changes.json')
            engine = SQLiteEngine(database_path)
            await engine.connect()
            try:
                hub = ChangeStreamHub(journal_path=journal_path)
                adapt_engine(engine).prepare_change_delivery(hub)
                context = OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                )
                outcome = await engine.insert_document(
                    'db',
                    'items',
                    {'_id': 'durable'},
                    overwrite=False,
                    operation_context=context,
                )
                assert outcome.commit_sequence == 1
            finally:
                await engine.disconnect()

            restarted = SQLiteEngine(database_path)
            await restarted.connect()
            try:
                hub = ChangeStreamHub(journal_path=journal_path)
                adapter = adapt_engine(restarted)
                adapter.prepare_change_delivery(hub)
                adapter.dispatch_committed_changes(hub)
                adapter.dispatch_committed_changes(hub)

                assert hub.state.next_token == 2
                assert hub.current_offset() == 1
                _offset, event = hub.wait_for_event(0, timeout_seconds=0)
                assert event is not None
                assert event.token == 1
                assert event.operation_type == 'insert'
                assert event.document_key == {'_id': 'durable'}
            finally:
                await restarted.disconnect()

    async def test_gap_rows_advance_sequence_without_creating_an_event(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            hub = ChangeStreamHub()
            adapter = adapt_engine(engine)
            adapter.prepare_change_delivery(hub)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.RECORD_GAP,
                change_operation_type='insert',
            )

            outcome = await engine.insert_document(
                'db',
                'items',
                {'_id': 'without-listener'},
                overwrite=False,
                operation_context=context,
            )
            adapter.dispatch_committed_changes(hub)

            assert outcome.commit_sequence == 1
            assert hub.state.next_token == 2
            assert hub.current_offset() == 0
        finally:
            await engine.disconnect()

    async def test_acknowledged_rows_are_compacted(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            hub = ChangeStreamHub()
            adapter = adapt_engine(engine)
            adapter.prepare_change_delivery(hub)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type='insert',
            )
            await engine.insert_document(
                'db',
                'items',
                {'_id': 'compact'},
                overwrite=False,
                operation_context=context,
            )

            adapter.dispatch_committed_changes(hub)

            delivery = engine._runtime_diagnostics_info()['changeDelivery']
            assert delivery['pendingEntries'] == 0
            assert delivery['prunedThrough'] == 1
            assert delivery['minimumCheckpoint'] == 1
        finally:
            await engine.disconnect()

    async def test_new_consumer_starts_after_fully_compacted_history(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            first_hub = ChangeStreamHub()
            adapter = adapt_engine(engine)
            adapter.prepare_change_delivery(first_hub)
            await engine.insert_document(
                'db',
                'items',
                {'_id': 'first'},
                overwrite=False,
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                ),
            )
            adapter.dispatch_committed_changes(first_hub)

            second_hub = ChangeStreamHub()
            adapter.prepare_change_delivery(second_hub)

            delivery = engine._runtime_diagnostics_info()['changeDelivery']
            assert delivery['pendingEntries'] == 0
            assert delivery['newestSequence'] == 1
            assert delivery['minimumCheckpoint'] == 1
        finally:
            await engine.disconnect()

    async def test_idempotent_outbox_append_does_not_consume_a_sequence(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            register_consumer(
                conn,
                'test-consumer',
                initial_checkpoint=0,
                durable=False,
            )
            first_context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type='insert',
            )
            arguments = {
                'event_index': 0,
                'operation_type': 'insert',
                'db_name': 'db',
                'coll_name': 'items',
                'document_key': {'_id': 'first'},
                'full_document': {'_id': 'first'},
                'serialize_document': json.dumps,
            }

            first = append_change(conn, context=first_context, **arguments)
            replay = append_change(conn, context=first_context, **arguments)
            second = append_change(
                conn,
                context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                ),
                **arguments,
            )

            assert first == replay == 1
            assert second == 2
        finally:
            conn.close()

    async def test_idempotent_identity_survives_outbox_compaction(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            register_consumer(
                conn,
                'test-consumer',
                initial_checkpoint=0,
                durable=False,
            )
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type='insert',
            )
            arguments = {
                'operation_type': 'insert',
                'db_name': 'db',
                'coll_name': 'items',
                'document_key': {'_id': 'first'},
                'full_document': {'_id': 'first'},
                'serialize_document': json.dumps,
            }

            first = append_change(conn, context=context, **arguments)
            checkpoint_consumer(conn, 'test-consumer', first)
            assert compact_change_outbox(conn, max_entries=10) == 1

            replay = append_change(conn, context=context, **arguments)
            second = append_change(
                conn,
                context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                ),
                **arguments,
            )

            assert first == replay == 1
            assert second == 2
            assert conn.execute(
                'SELECT COUNT(*) FROM change_outbox',
            ).fetchone()[0] == 1
        finally:
            conn.close()

    async def test_replay_rejects_a_conflicting_payload_after_compaction(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            register_consumer(
                conn,
                'test-consumer',
                initial_checkpoint=0,
                durable=False,
            )
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type='insert',
            )
            arguments = {
                'operation_type': 'insert',
                'db_name': 'db',
                'coll_name': 'items',
                'document_key': {'_id': 'first'},
                'full_document': {'_id': 'first', 'value': 1},
                'serialize_document': json.dumps,
            }
            sequence = append_change(conn, context=context, **arguments)
            checkpoint_consumer(conn, 'test-consumer', sequence)
            assert compact_change_outbox(conn, max_entries=10) == 1

            with self.assertRaisesRegex(OperationFailure, 'different'):
                append_change(
                    conn,
                    context=context,
                    **{
                        **arguments,
                        'full_document': {'_id': 'first', 'value': 2},
                    },
                )
            with self.assertRaisesRegex(OperationFailure, 'different'):
                append_change(
                    conn,
                    context=context.derive(
                        publication=ChangePublicationPolicy.RECORD_GAP,
                    ),
                    **arguments,
                )
        finally:
            conn.close()

    async def test_gap_replay_rejects_a_different_semantic_effect(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.RECORD_GAP,
                change_operation_type='insert',
            )
            arguments = {
                'operation_type': 'insert',
                'db_name': 'db',
                'coll_name': 'items',
                'document_key': {'_id': 'first'},
                'full_document': {'_id': 'first'},
                'serialize_document': json.dumps,
            }
            assert append_change(conn, context=context, **arguments) == 1
            assert append_change(conn, context=context, **arguments) == 1

            with self.assertRaisesRegex(OperationFailure, 'different'):
                append_change(
                    conn,
                    context=context,
                    **{
                        **arguments,
                        'coll_name': 'other-items',
                    },
                )
        finally:
            conn.close()

    async def test_dispatch_drains_more_than_one_storage_batch(self):
        engine = SQLiteEngine(change_outbox_max_entries=2_000)
        await engine.connect()
        try:
            engine.register_change_consumer(
                'batch-consumer',
                initial_checkpoint=0,
            )
            conn = engine._connection
            assert conn is not None
            for index in range(1_001):
                append_change(
                    conn,
                    context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=ChangePublicationPolicy.EMIT,
                        change_operation_type='insert',
                    ),
                    operation_type='insert',
                    db_name='db',
                    coll_name='items',
                    document_key={'_id': index},
                    full_document={'_id': index},
                    serialize_document=engine._serialize_document,
                    max_entries=2_000,
                )
            conn.commit()
            delivered = []

            engine.dispatch_committed_changes(
                'batch-consumer',
                delivered.append,
            )

            assert len(delivered) == 1_001
            assert consumer_checkpoint(conn, 'batch-consumer') == 1_001
            assert engine._runtime_diagnostics_info()['changeDelivery'][
                'pendingEntries'
            ] == 0
        finally:
            await engine.disconnect()

    async def test_dispatch_never_calls_consumer_under_storage_lock(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            engine.register_change_consumer(
                'lock-consumer',
                initial_checkpoint=0,
            )
            conn = engine._connection
            assert conn is not None
            append_change(
                conn,
                context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                ),
                operation_type='insert',
                db_name='db',
                coll_name='items',
                document_key={'_id': 1},
                full_document={'_id': 1},
                serialize_document=engine._serialize_document,
            )
            conn.commit()
            acquired = []

            def consumer(_change):
                def acquire_from_another_thread():
                    locked = engine._lock.acquire(timeout=0.5)
                    acquired.append(locked)
                    if locked:
                        engine._lock.release()

                worker = threading.Thread(target=acquire_from_another_thread)
                worker.start()
                worker.join()

            engine.dispatch_committed_changes('lock-consumer', consumer)

            assert acquired == [True]
        finally:
            await engine.disconnect()

    async def test_append_rejects_invalid_event_ordinal(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type='insert',
            )
            with self.assertRaisesRegex(ValueError, 'event_index'):
                append_change(
                    conn,
                    context=context,
                    event_index=True,
                    operation_type='insert',
                    db_name='db',
                    coll_name='items',
                    document_key={'_id': 1},
                    full_document={'_id': 1},
                    serialize_document=json.dumps,
                )
        finally:
            conn.close()

    async def test_idempotency_ledger_uses_the_outbox_retention_window(self):
        conn = sqlite3.connect(':memory:')
        try:
            ensure_change_outbox_schema(conn)
            contexts = []
            for index in range(3):
                context = OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                )
                contexts.append(context)
                append_change(
                    conn,
                    context=context,
                    operation_type='insert',
                    db_name='db',
                    coll_name='items',
                    document_key={'_id': index},
                    full_document={'_id': index},
                    serialize_document=json.dumps,
                    max_entries=2,
                )

            retained = conn.execute(
                'SELECT operation_id FROM change_outbox_identities '
                'ORDER BY sequence',
            ).fetchall()

            assert retained == [
                (contexts[1].operation_id,),
                (contexts[2].operation_id,),
            ]
        finally:
            conn.close()

    async def test_lagging_consumer_fails_after_capacity_compaction(self):
        engine = SQLiteEngine(change_outbox_max_entries=2)
        await engine.connect()
        try:
            hub = ChangeStreamHub(journal_path='lagging-consumer.json')
            adapter = adapt_engine(engine)
            adapter.prepare_change_delivery(hub)
            for index in range(3):
                context = OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                )
                await engine.insert_document(
                    'db',
                    'items',
                    {'_id': index},
                    overwrite=False,
                    operation_context=context,
                )

            with self.assertRaisesRegex(
                OperationFailure,
                'no longer available',
            ):
                adapter.dispatch_committed_changes(hub)

            delivery = engine._runtime_diagnostics_info()['changeDelivery']
            assert delivery['pendingEntries'] == 2
            assert delivery['prunedThrough'] == 1
        finally:
            await engine.disconnect()
            Path('lagging-consumer.json').unlink(missing_ok=True)

    async def test_consumer_checkpoint_cannot_start_ahead_of_history(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            with self.assertRaisesRegex(OperationFailure, 'ahead'):
                engine.register_change_consumer(
                    'future',
                    initial_checkpoint=1,
                )
        finally:
            await engine.disconnect()

    async def test_ephemeral_consumer_is_removed_on_disconnect(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / 'mongoeco.sqlite')
            engine = SQLiteEngine(database_path)
            await engine.connect()
            adapt_engine(engine).prepare_change_delivery(ChangeStreamHub())
            await engine.disconnect()

            reopened = SQLiteEngine(database_path)
            await reopened.connect()
            try:
                delivery = reopened._runtime_diagnostics_info()[
                    'changeDelivery'
                ]
                assert delivery['consumerCount'] == 0
            finally:
                await reopened.disconnect()

    async def test_file_ephemeral_registration_is_renewed_while_connected(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / 'mongoeco.sqlite')
            engine = SQLiteEngine(database_path)
            await engine.connect()
            try:
                with (
                    patch(
                        'mongoeco.engines.sqlite.'
                        '_EPHEMERAL_CHANGE_CONSUMER_HEARTBEAT_SECONDS',
                        0.01,
                    ),
                    patch(
                        'mongoeco.engines.sqlite.'
                        '_EPHEMERAL_CHANGE_CONSUMER_TTL_SECONDS',
                        2.0,
                    ),
                ):
                    engine.register_change_consumer(
                        'renewed-ephemeral',
                        initial_checkpoint=0,
                    )
                    control = engine._change_delivery_connection
                    assert control is not None
                    initial_expiry = control.execute(
                        'SELECT registration_expires_at_epoch '
                        'FROM change_outbox_consumers '
                        'WHERE consumer_id = ?',
                        ('renewed-ephemeral',),
                    ).fetchone()[0]
                    deadline = time.monotonic() + 1
                    renewed_expiry = initial_expiry
                    while (
                        renewed_expiry <= initial_expiry
                        and time.monotonic() < deadline
                    ):
                        await asyncio.sleep(0.01)
                        renewed_expiry = control.execute(
                            'SELECT registration_expires_at_epoch '
                            'FROM change_outbox_consumers '
                            'WHERE consumer_id = ?',
                            ('renewed-ephemeral',),
                        ).fetchone()[0]

                    assert renewed_expiry > initial_expiry
                    delivery = engine._runtime_diagnostics_info()[
                        'changeDelivery'
                    ]
                    assert delivery['registrationHeartbeatActive']
                    assert delivery['registrationHeartbeatHealthy']
            finally:
                await engine.disconnect()

    async def test_shared_file_waits_for_all_registered_consumers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / 'mongoeco.sqlite')
            first = SQLiteEngine(database_path)
            second = SQLiteEngine(database_path)
            await first.connect()
            await second.connect()
            try:
                first_hub = ChangeStreamHub()
                second_hub = ChangeStreamHub()
                first_adapter = adapt_engine(first)
                second_adapter = adapt_engine(second)
                first_adapter.prepare_change_delivery(first_hub)
                second_adapter.prepare_change_delivery(second_hub)
                context = OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    publication=ChangePublicationPolicy.EMIT,
                    change_operation_type='insert',
                )
                await first.insert_document(
                    'db',
                    'items',
                    {'_id': 'shared'},
                    overwrite=False,
                    operation_context=context,
                )

                first_adapter.dispatch_committed_changes(first_hub)
                first_delivery = first._runtime_diagnostics_info()[
                    'changeDelivery'
                ]
                assert first_delivery['pendingEntries'] == 1

                second_adapter.dispatch_committed_changes(second_hub)
                second_delivery = second._runtime_diagnostics_info()[
                    'changeDelivery'
                ]
                assert second_delivery['pendingEntries'] == 0
                assert first_hub.current_offset() == 1
                assert second_hub.current_offset() == 1
            finally:
                await first.disconnect()
                await second.disconnect()

    async def test_shared_file_serializes_the_same_durable_consumer(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / 'mongoeco.sqlite')
            first = SQLiteEngine(database_path)
            second = SQLiteEngine(database_path)
            await first.connect()
            await second.connect()
            try:
                first.register_change_consumer(
                    'shared-consumer', initial_checkpoint=0, durable=True,
                )
                second.register_change_consumer(
                    'shared-consumer', initial_checkpoint=0, durable=True,
                )
                await first.insert_document(
                    'db',
                    'items',
                    {'_id': 'shared'},
                    overwrite=False,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=ChangePublicationPolicy.EMIT,
                        change_operation_type='insert',
                    ),
                )
                entered = threading.Event()
                release = threading.Event()
                first_delivery = []
                second_delivery = []

                def slow(change):
                    first_delivery.append(change.sequence)
                    entered.set()
                    release.wait(1)

                first_worker = threading.Thread(
                    target=first.dispatch_committed_changes,
                    args=('shared-consumer', slow),
                )
                second_worker = threading.Thread(
                    target=second.dispatch_committed_changes,
                    args=(
                        'shared-consumer',
                        lambda change: second_delivery.append(change.sequence),
                    ),
                )
                first_worker.start()
                assert entered.wait(1)
                await second.insert_document(
                    'db',
                    'items',
                    {'_id': 'concurrent'},
                    overwrite=False,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=ChangePublicationPolicy.EMIT,
                        change_operation_type='insert',
                    ),
                )
                second_worker.start()
                second_worker.join(0.05)
                assert second_worker.is_alive()
                release.set()
                first_worker.join()
                second_worker.join()

                assert first_delivery == [1]
                assert second_delivery == [2]
                assert consumer_checkpoint(
                    first._require_connection(),
                    'shared-consumer',
                ) == 2
            finally:
                await first.disconnect()
                await second.disconnect()

    async def test_shared_file_rejects_cross_engine_dispatch_reentry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / 'mongoeco.sqlite')
            first = SQLiteEngine(database_path)
            second = SQLiteEngine(database_path)
            await first.connect()
            await second.connect()
            try:
                first.register_change_consumer(
                    'shared-consumer',
                    initial_checkpoint=0,
                    durable=True,
                )
                second.register_change_consumer(
                    'shared-consumer',
                    initial_checkpoint=0,
                    durable=True,
                )
                await first.insert_document(
                    'db',
                    'items',
                    {'_id': 'shared'},
                    overwrite=False,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=ChangePublicationPolicy.EMIT,
                        change_operation_type='insert',
                    ),
                )
                delivered = []

                def reentrant(change):
                    delivered.append(change.sequence)
                    with self.assertRaisesRegex(RuntimeError, 're-entered'):
                        second.dispatch_committed_changes(
                            'shared-consumer',
                            delivered.append,
                        )

                first.dispatch_committed_changes(
                    'shared-consumer',
                    reentrant,
                )

                assert delivered == [1]
                assert consumer_checkpoint(
                    first._require_connection(),
                    'shared-consumer',
                ) == 1
            finally:
                await first.disconnect()
                await second.disconnect()

    async def test_shared_file_serializes_dispatch_across_processes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / 'mongoeco.sqlite')
            engine = SQLiteEngine(database_path)
            await engine.connect()
            try:
                engine.register_change_consumer(
                    'process-consumer',
                    initial_checkpoint=0,
                    durable=True,
                )
                await engine.insert_document(
                    'db',
                    'items',
                    {'_id': 'shared'},
                    overwrite=False,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=ChangePublicationPolicy.EMIT,
                        change_operation_type='insert',
                    ),
                )
            finally:
                await engine.disconnect()

            context = multiprocessing.get_context('spawn')
            entered = context.Event()
            release = context.Event()
            second_entered = context.Event()
            deliveries = context.Queue()
            first = context.Process(
                target=_dispatch_outbox_in_subprocess,
                args=(
                    database_path,
                    'process-consumer',
                    entered,
                    release,
                    deliveries,
                ),
            )
            second = context.Process(
                target=_dispatch_outbox_in_subprocess,
                args=(
                    database_path,
                    'process-consumer',
                    second_entered,
                    None,
                    deliveries,
                ),
            )
            try:
                first.start()
                assert entered.wait(timeout=5)
                second.start()
                second.join(timeout=0.1)
                assert second.is_alive()
                release.set()
                first.join(timeout=5)
                second.join(timeout=5)

                assert first.exitcode == 0
                assert second.exitcode == 0
                assert deliveries.get(timeout=1)[1] == 1
                with self.assertRaises(queue.Empty):
                    deliveries.get(timeout=0.1)
                assert not second_entered.is_set()
            finally:
                release.set()
                for process in (first, second):
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=2)
                deliveries.close()

    async def test_crashed_dispatch_is_replayed_after_lease_expiry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / 'mongoeco.sqlite')
            engine = SQLiteEngine(database_path)
            await engine.connect()
            try:
                engine.register_change_consumer(
                    'crash-consumer',
                    initial_checkpoint=0,
                    durable=True,
                )
                await engine.insert_document(
                    'db',
                    'items',
                    {'_id': 'before-crash'},
                    overwrite=False,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=ChangePublicationPolicy.EMIT,
                        change_operation_type='insert',
                    ),
                )
            finally:
                await engine.disconnect()

            context = multiprocessing.get_context('spawn')
            parent_connection, child_connection = context.Pipe(
                duplex=False,
            )
            crashed = context.Process(
                target=_crash_during_outbox_delivery,
                args=(
                    database_path,
                    'crash-consumer',
                    child_connection,
                ),
            )
            try:
                crashed.start()
                child_connection.close()
                assert parent_connection.poll(5)
                assert parent_connection.recv() == 1
                crashed.join(timeout=5)
                assert crashed.exitcode == 23
            finally:
                if crashed.is_alive():
                    crashed.terminate()
                crashed.join(timeout=2)
                parent_connection.close()

            connection = sqlite3.connect(database_path)
            try:
                assert consumer_checkpoint(
                    connection,
                    'crash-consumer',
                ) == 0
                connection.execute(
                    'UPDATE change_outbox_consumers '
                    'SET lease_expires_at_epoch = 0 '
                    'WHERE consumer_id = ?',
                    ('crash-consumer',),
                )
                connection.commit()
            finally:
                connection.close()

            recovery = SQLiteEngine(database_path)
            await recovery.connect()
            try:
                replayed = []
                recovery.dispatch_committed_changes(
                    'crash-consumer',
                    lambda change: replayed.append(change.sequence),
                )
                assert replayed == [1]
                assert consumer_checkpoint(
                    recovery._require_connection(),
                    'crash-consumer',
                ) == 1
            finally:
                await recovery.disconnect()


if __name__ == '__main__':
    unittest.main()
