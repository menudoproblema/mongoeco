import asyncio
import threading
import time
import unittest

import pytest

# Commit sequence values are the contract exercised by this module.
# ruff: noqa: PLR2004
from mongoeco.change_streams import ChangeStreamHub
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.engines._change_dispatch import ConsumerDispatchCoordinator
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

    async def test_concurrent_dispatch_is_serialized_per_consumer(self):
        for index in (1, 2):
            await self.engine.insert_document(
                'db',
                'items',
                {'_id': index},
                overwrite=False,
                operation_context=self._context(),
            )
        entered = threading.Event()
        release = threading.Event()
        delivered = []

        def slow(change):
            delivered.append(('slow', change.sequence))
            if change.sequence == 1:
                entered.set()
                release.wait(1)

        def fast(change):
            delivered.append(('fast', change.sequence))

        first = threading.Thread(
            target=self.engine.dispatch_committed_changes,
            args=(self.adapter._change_consumer_id(self.hub), slow),
        )
        second = threading.Thread(
            target=self.engine.dispatch_committed_changes,
            args=(self.adapter._change_consumer_id(self.hub), fast),
        )
        first.start()
        assert entered.wait(1)
        second.start()
        release.set()
        first.join()
        second.join()

        assert delivered == [('slow', 1), ('slow', 2)]
        assert self.engine._change_checkpoints[
            self.adapter._change_consumer_id(self.hub)
        ] == 2

    async def test_dispatch_coordinator_rejects_reentry_and_retires_idle_gate(
        self,
    ):
        coordinator = ConsumerDispatchCoordinator()

        with (
            coordinator.hold('consumer'),
            pytest.raises(RuntimeError, match='re-entered'),
            coordinator.hold('consumer'),
        ):
            pass
        with coordinator.hold('consumer'):
            coordinator.retire('consumer')
            assert 'consumer' in coordinator._gates

        coordinator.retire('consumer')
        coordinator.retire('missing')
        assert 'consumer' not in coordinator._gates
        with coordinator.hold('idle'):
            pass
        coordinator.retire('idle')
        assert 'idle' not in coordinator._gates

    async def test_retirement_waits_for_the_current_owner_and_waiters(self):
        coordinator = ConsumerDispatchCoordinator()
        owner_entered = threading.Event()
        release_owner = threading.Event()
        waiter_entered = threading.Event()
        release_waiter = threading.Event()

        def own_gate():
            with coordinator.hold('consumer'):
                owner_entered.set()
                release_owner.wait(timeout=2)

        def wait_for_gate():
            with coordinator.hold('consumer'):
                waiter_entered.set()
                release_waiter.wait(timeout=2)

        owner = threading.Thread(target=own_gate)
        waiter = threading.Thread(target=wait_for_gate)
        owner.start()
        assert owner_entered.wait(timeout=2)
        waiter.start()

        def waiter_is_registered():
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if coordinator._gates['consumer'].users == 2:
                    return True
                time.sleep(0.001)
            return False

        assert await asyncio.to_thread(waiter_is_registered)

        coordinator.retire('consumer')
        assert 'consumer' in coordinator._gates
        release_owner.set()
        assert waiter_entered.wait(timeout=2)
        owner.join(timeout=2)
        assert 'consumer' in coordinator._gates
        release_waiter.set()
        waiter.join(timeout=2)

        assert not owner.is_alive()
        assert not waiter.is_alive()
        assert 'consumer' not in coordinator._gates


if __name__ == '__main__':
    unittest.main()
