import json
import sqlite3
import tempfile
import unittest

from pathlib import Path

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
    append_change,
    checkpoint_consumer,
    compact_change_outbox,
    consumer_checkpoint,
    ensure_change_outbox_schema,
    read_committed_changes,
    register_consumer,
    unregister_consumer,
)
from mongoeco.engines.adapter import adapt_engine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession


class SQLiteOutboxTests(unittest.IsolatedAsyncioTestCase):
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


if __name__ == '__main__':
    unittest.main()
