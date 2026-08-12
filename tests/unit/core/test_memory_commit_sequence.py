import unittest

# Commit sequence values are the contract exercised by this module.
# ruff: noqa: PLR2004
from mongoeco.change_streams import ChangeStreamHub
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.engines.adapter import adapt_engine
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession


class MemoryCommitSequenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = MemoryEngine()
        await self.engine.connect()
        self.hub = ChangeStreamHub()
        self.adapter = adapt_engine(self.engine)
        self.adapter.prepare_change_delivery(self.hub)

    async def asyncTearDown(self):
        await self.engine.disconnect()

    def _context(self, session=None):
        return OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            session=session,
            publication=ChangePublicationPolicy.EMIT,
            change_operation_type='insert',
        )

    async def test_direct_commits_publish_in_monotonic_sequence(self):
        first = await self.engine.insert_document(
            'db',
            'items',
            {'_id': 'first'},
            overwrite=False,
            operation_context=self._context(),
        )
        second = await self.engine.insert_document(
            'db',
            'items',
            {'_id': 'second'},
            overwrite=False,
            operation_context=self._context(),
        )

        self.adapter.dispatch_committed_changes(self.hub)

        assert first.commit_sequence == 1
        assert second.commit_sequence == 2
        assert self.hub.state.next_token == 3
        _offset, first_event = self.hub.wait_for_event(0, timeout_seconds=0)
        _offset, second_event = self.hub.wait_for_event(1, timeout_seconds=0)
        assert first_event.token == 1
        assert second_event.token == 2

    async def test_transaction_assigns_sequence_only_at_commit(self):
        session = ClientSession()
        self.engine.create_session_state(session)
        session.start_transaction()

        pending = await self.engine.insert_document(
            'db',
            'items',
            {'_id': 'committed'},
            overwrite=False,
            operation_context=self._context(session),
        )
        assert pending.commit_sequence is None
        self.adapter.dispatch_committed_changes(self.hub)
        assert self.hub.current_offset() == 0

        session.commit_transaction()
        self.adapter.dispatch_committed_changes(self.hub)

        assert self.hub.current_offset() == 1
        _offset, event = self.hub.wait_for_event(0, timeout_seconds=0)
        assert event.document_key == {'_id': 'committed'}

    async def test_aborted_transaction_does_not_consume_sequence(self):
        session = ClientSession()
        self.engine.create_session_state(session)
        session.start_transaction()
        await self.engine.insert_document(
            'db',
            'items',
            {'_id': 'aborted'},
            overwrite=False,
            operation_context=self._context(session),
        )
        session.abort_transaction()

        committed = await self.engine.insert_document(
            'db',
            'items',
            {'_id': 'survives'},
            overwrite=False,
            operation_context=self._context(),
        )
        self.adapter.dispatch_committed_changes(self.hub)

        assert committed.commit_sequence == 1
        assert self.hub.current_offset() == 1

    async def test_acknowledged_changes_are_pruned(self):
        await self.engine.insert_document(
            'db',
            'items',
            {'_id': 'pruned'},
            overwrite=False,
            operation_context=self._context(),
        )

        self.adapter.dispatch_committed_changes(self.hub)

        delivery = self.engine._runtime_diagnostics_info()['changeDelivery']
        assert delivery['pendingEntries'] == 0
        assert delivery['prunedThrough'] == 1

    async def test_lagging_consumer_fails_after_bounded_history_is_pruned(self):
        await self.engine.disconnect()
        self.engine = MemoryEngine(change_log_max_entries=2)
        await self.engine.connect()
        self.hub = ChangeStreamHub()
        self.adapter = adapt_engine(self.engine)
        self.adapter.prepare_change_delivery(self.hub)

        for index in range(3):
            await self.engine.insert_document(
                'db',
                'items',
                {'_id': index},
                overwrite=False,
                operation_context=self._context(),
            )

        with self.assertRaisesRegex(OperationFailure, 'behind retained history'):
            self.adapter.dispatch_committed_changes(self.hub)

        delivery = self.engine._runtime_diagnostics_info()['changeDelivery']
        assert delivery['pendingEntries'] == 2
        assert delivery['prunedThrough'] == 1

    async def test_consumer_checkpoint_cannot_start_ahead_of_history(self):
        with self.assertRaisesRegex(OperationFailure, 'ahead'):
            self.engine.register_change_consumer(
                'future',
                initial_checkpoint=1,
            )


if __name__ == '__main__':
    unittest.main()
