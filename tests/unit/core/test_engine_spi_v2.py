import asyncio
import unittest
import warnings

from types import SimpleNamespace

# Literal protocol versions are the values under contract in these tests.
# unittest is the repository's established contract-test harness.
# ruff: noqa: PLR2004, PT027
from mongoeco.engines.adapter import (
    EngineSpiAdapter,
    LegacyEngineAdapter,
    adapt_engine,
)
from mongoeco.engines.base import AsyncCrudEngine, AsyncReadSemanticsEngine
from mongoeco.engines.capabilities import (
    EngineCapabilities,
    resolve_engine_capabilities,
    validate_engine_contract,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.results import (
    CommittedChange,
    DeleteOutcome,
    EngineDeleteResult,
    EngineUpdateResult,
    InsertOutcome,
    MergeDocumentResult,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import DeleteResult, UpdateResult


class _EmptySource:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class _NativeEngine:
    capabilities = EngineCapabilities(
        batch_inserts=True,
        explicit_read_snapshots=True,
    )

    def __init__(self):
        self.calls = []

    async def insert_document(self, *args, **kwargs):
        self.calls.append(('insert', args, kwargs))
        return InsertOutcome(applied=True, document=args[2])

    async def insert_documents(self, *args, **kwargs):
        self.calls.append(('insert_many', args, kwargs))
        return tuple(
            InsertOutcome(applied=True, document=document)
            for document in args[2]
        )

    async def get_document(self, *args, **kwargs):
        self.calls.append(('get', args, kwargs))
        return {'_id': args[2]}

    async def count_find_semantics(self, *args, **kwargs):
        self.calls.append(('count', args, kwargs))
        return 3

    async def update_with_operation(self, *args, **kwargs):
        self.calls.append(('update', args, kwargs))
        return MutationOutcome(result=UpdateResult(1, 1))

    async def delete_with_operation(self, *args, **kwargs):
        self.calls.append(('delete', args, kwargs))
        return DeleteOutcome(result=DeleteResult(1))

    async def merge_document(self, *args, **kwargs):
        self.calls.append(('merge', args, kwargs))
        return MergeOutcome(matched=False, applied=True)

    def open_read_snapshot(self, *args, **kwargs):
        self.calls.append(('snapshot', args, kwargs))
        return ReadSnapshot(_EmptySource(), policy=SnapshotPolicy.STABLE)


class _LegacyEngine:
    supports_commit_callbacks = True

    def __init__(self):
        self.calls = []

    async def put_document(self, *args, **kwargs):
        self.calls.append(('insert', args, kwargs))
        callback = kwargs.get('on_commit')
        if callback is not None:
            callback(args[2])
        return True

    async def put_documents_bulk(self, *args, **kwargs):
        self.calls.append(('insert_many', args, kwargs))
        callback = kwargs.get('on_commit')
        if callback is not None:
            for document in args[2]:
                callback(document)
        return [True for _document in args[2]]

    async def get_document(self, *args, **kwargs):
        self.calls.append(('get', args, kwargs))
        return {'_id': args[2]}

    async def count_find_semantics(self, *args, **kwargs):
        self.calls.append(('count', args, kwargs))
        return 2

    async def update_with_operation(self, *args, **kwargs):
        self.calls.append(('update', args, kwargs))
        result = UpdateResult(1, 1)
        callback = kwargs.get('on_commit')
        if callback is not None:
            callback(MutationOutcome(result=result))
        return result

    async def delete_with_operation(self, *args, **kwargs):
        self.calls.append(('delete', args, kwargs))
        result = DeleteResult(1)
        callback = kwargs.get('on_commit')
        if callback is not None:
            callback(DeleteOutcome(result=result))
        return result

    async def merge_document(self, *args, **kwargs):
        self.calls.append(('merge', args, kwargs))
        outcome = MergeOutcome(matched=False, applied=True)
        callback = kwargs.get('on_commit')
        if callback is not None:
            callback(outcome)
        return outcome

    def scan_find_semantics(self, *args, **kwargs):
        self.calls.append(('snapshot', args, kwargs))
        return _EmptySource()


class EngineSpiV2ContractTests(unittest.TestCase):
    def test_capabilities_reject_invalid_version_and_delivery_mode(self):
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            EngineCapabilities(spi_version=True)
        with self.assertRaisesRegex(ValueError, 'delivery mode'):
            EngineCapabilities(change_delivery='future')
        with self.assertRaisesRegex(ValueError, 'native mutation outcomes'):
            EngineCapabilities(mutation_outcomes=False)
        with self.assertRaisesRegex(ValueError, 'legacy callback'):
            EngineCapabilities(change_delivery='legacy-callback')
        with self.assertRaisesRegex(TypeError, 'batch_inserts'):
            EngineCapabilities(batch_inserts=1)
        with self.assertRaisesRegex(ValueError, 'native mutation outcomes'):
            EngineCapabilities(spi_version=1)
        with self.assertRaisesRegex(ValueError, 'explicit read snapshots'):
            EngineCapabilities(
                spi_version=1,
                mutation_outcomes=False,
                explicit_read_snapshots=True,
            )
        with self.assertRaisesRegex(ValueError, 'sequenced'):
            EngineCapabilities(
                spi_version=1,
                mutation_outcomes=False,
                change_delivery='commit-sequence',
            )

        outbox = EngineCapabilities(change_delivery='transactional-outbox')
        assert outbox.transactional_outbox
        assert outbox.monotonic_commit_sequence

    def test_capabilities_can_be_declared_by_callable(self):
        declared = EngineCapabilities()

        class CallableCapabilities:
            @staticmethod
            def capabilities():
                return declared

        assert resolve_engine_capabilities(CallableCapabilities()) is declared

    def test_v1_methods_do_not_leak_into_canonical_v2_protocols(self):
        assert 'put_document' not in AsyncCrudEngine.__dict__
        assert 'delete_document' not in AsyncCrudEngine.__dict__
        assert 'scan_find_semantics' not in AsyncReadSemanticsEngine.__dict__

    def test_provisional_outcome_names_remain_exact_aliases(self):
        assert EngineUpdateResult is MutationOutcome
        assert EngineDeleteResult is DeleteOutcome
        assert MergeDocumentResult is MergeOutcome
        assert isinstance(
            EngineUpdateResult(result=UpdateResult(1, 1)),
            MutationOutcome,
        )
        assert isinstance(
            EngineDeleteResult(result=DeleteResult(1)),
            DeleteOutcome,
        )

    def test_legacy_capability_detection_is_centralized(self):
        class LegacyEngine:
            supports_injected_clock = True
            supports_commit_callbacks = True

        capabilities = resolve_engine_capabilities(LegacyEngine())

        assert capabilities.spi_version == 1
        assert capabilities.injected_clock
        assert not capabilities.mutation_outcomes
        assert capabilities.change_delivery == 'legacy-callback'

    def test_native_capabilities_are_returned_without_inference(self):
        declared = EngineCapabilities(
            injected_clock=True,
            explicit_read_snapshots=True,
            change_delivery='commit-sequence',
        )

        class NativeEngine:
            capabilities = declared
            supports_injected_clock = False

        assert resolve_engine_capabilities(NativeEngine()) is declared

    def test_inherited_capabilities_honor_explicit_legacy_clock_override(self):
        class ExternalMemoryEngine(MemoryEngine):
            supports_injected_clock = False

        capabilities = resolve_engine_capabilities(ExternalMemoryEngine())

        assert not capabilities.injected_clock
        assert capabilities.spi_version == 2

    def test_explicit_capabilities_take_precedence_over_legacy_flags(self):
        declared = EngineCapabilities(injected_clock=True)

        class ExternalMemoryEngine(MemoryEngine):
            capabilities = declared
            supports_injected_clock = False

        assert resolve_engine_capabilities(ExternalMemoryEngine()) is declared

    def test_builtin_engines_declare_spi_v2(self):
        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                capabilities = resolve_engine_capabilities(engine)
                assert capabilities.spi_version == 2
                assert capabilities.mutation_outcomes

    def test_adapter_selection_is_versioned(self):
        class LegacyEngine:
            pass

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            adapter = adapt_engine(LegacyEngine())
            adapt_engine(LegacyEngine())

        assert isinstance(adapter, LegacyEngineAdapter)
        assert len(captured) == 1
        assert issubclass(captured[0].category, DeprecationWarning)
        assert 'before MongoEco 5.0.0' in str(captured[0].message)
        assert isinstance(adapt_engine(MemoryEngine()), EngineSpiAdapter)

    def test_v2_capability_declarations_are_validated_eagerly(self):
        class IncompleteEngine:
            capabilities = EngineCapabilities()

        with self.assertRaisesRegex(TypeError, 'missing required methods'):
            validate_engine_contract(
                IncompleteEngine(),
                IncompleteEngine.capabilities,
            )

    def test_native_adapter_covers_all_typed_boundaries(self):
        async def _exercise():
            engine = _NativeEngine()
            adapter = EngineSpiAdapter(engine)
            context = SimpleNamespace(session='session', operation_id='op')
            callbacks = []

            inserted = await adapter.insert_outcome(
                'db', 'coll', {'_id': 1},
                operation_context=context,
                on_commit=callbacks.append,
            )
            batch = await adapter.insert_many_outcomes(
                'db', 'coll', [{'_id': 2}, {'_id': 3}],
                operation_context=context,
                on_commit=callbacks.append,
            )
            updated = await adapter.update_outcome(
                'db', 'coll', object(),
                operation_context=context,
                context='legacy',
                dialect='legacy',
                on_commit=callbacks.append,
            )
            deleted = await adapter.delete_outcome(
                'db', 'coll', object(),
                operation_context=context,
                context='legacy',
                dialect='legacy',
                on_commit=callbacks.append,
            )
            merged = await adapter.merge_outcome(
                'db', 'coll', {'_id': 4},
                operation_context=context,
                context='legacy',
                on_commit=callbacks.append,
            )
            document = await adapter.get_document(
                'db', 'coll', 1,
                operation_context=context,
            )
            count = await adapter.count_documents(
                'db', 'coll', object(),
                operation_context=context,
            )
            snapshot = adapter.open_read_snapshot(
                'db', 'coll', object(),
                operation_context=context,
            )

            assert inserted.applied
            assert len(batch) == 2
            assert updated.modified_count == 1
            assert deleted.deleted_count == 1
            assert merged.applied
            assert document == {'_id': 1}
            assert count == 3
            assert isinstance(snapshot, ReadSnapshot)
            assert len(callbacks) == 6
            update_call = next(
                call for call in engine.calls if call[0] == 'update'
            )
            assert 'context' not in update_call[2]
            assert 'dialect' not in update_call[2]

        asyncio.run(_exercise())

    def test_legacy_adapter_translates_context_callbacks_and_results(self):
        async def _exercise():
            engine = _LegacyEngine()
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', DeprecationWarning)
                adapter = adapt_engine(engine)
            context = SimpleNamespace(session='session', operation_id='op')
            callbacks = []

            await adapter.insert_outcome(
                'db', 'coll', {'_id': 1},
                operation_context=context,
                on_commit=callbacks.append,
            )
            await adapter.insert_many_outcomes(
                'db', 'coll', [{'_id': 2}, {'_id': 3}],
                operation_context=context,
                on_commit=callbacks.append,
            )
            updated = await adapter.update_outcome(
                'db', 'coll', object(),
                operation_context=context,
                on_commit=callbacks.append,
            )
            deleted = await adapter.delete_outcome(
                'db', 'coll', object(),
                operation_context=context,
                on_commit=callbacks.append,
            )
            await adapter.merge_outcome(
                'db', 'coll', {'_id': 4},
                operation_context=context,
                on_commit=callbacks.append,
            )
            await adapter.get_document(
                'db', 'coll', 1,
                operation_context=context,
            )
            count = await adapter.count_documents(
                'db', 'coll', object(),
                operation_context=context,
            )
            snapshot = adapter.open_read_snapshot(
                'db', 'coll', object(),
                operation_context=context,
            )

            assert isinstance(updated, MutationOutcome)
            assert isinstance(deleted, DeleteOutcome)
            assert count == 2
            assert snapshot.metadata.operation_id == 'op'
            assert len(callbacks) == 6
            for _name, _args, kwargs in engine.calls:
                assert kwargs.get('context') == 'session'

        asyncio.run(_exercise())

    def test_adapter_rejects_invalid_engine_results(self):
        adapter = EngineSpiAdapter(_NativeEngine())

        with self.assertRaisesRegex(TypeError, 'MutationOutcome'):
            adapter._require_mutation_outcome(UpdateResult(1, 1))
        with self.assertRaisesRegex(TypeError, 'DeleteOutcome'):
            adapter._require_delete_outcome(DeleteResult(1))
        with self.assertRaisesRegex(TypeError, 'InsertOutcome'):
            adapter._require_insert_outcome(result=True)
        with self.assertRaisesRegex(TypeError, 'MergeOutcome'):
            engine = _NativeEngine()

            async def invalid_merge(*_args, **_kwargs):
                return object()

            engine.merge_document = invalid_merge
            asyncio.run(EngineSpiAdapter(engine).merge_outcome('db', 'c', {}))

    def test_change_delivery_handles_replay_gap_event_and_unregister(self):
        class Engine:
            capabilities = EngineCapabilities(
                change_delivery='commit-sequence',
            )

            def __init__(self):
                self.registered = []
                self.unregistered = []
                self.changes = []

            def register_change_consumer(self, consumer_id, **kwargs):
                self.registered.append((consumer_id, kwargs))
                return 0

            def unregister_change_consumer(self, consumer_id):
                self.unregistered.append(consumer_id)

            def dispatch_committed_changes(self, _consumer_id, consumer):
                for change in self.changes:
                    consumer(change)

            async def insert_document(self, *_args, **_kwargs):
                return InsertOutcome(applied=True)

            async def insert_documents(self, *_args, **_kwargs):
                return ()

            async def get_document(self, *_args, **_kwargs):
                return None

            async def count_find_semantics(self, *_args, **_kwargs):
                return 0

            async def update_with_operation(self, *_args, **_kwargs):
                return MutationOutcome(UpdateResult(0, 0))

            async def delete_with_operation(self, *_args, **_kwargs):
                return DeleteOutcome(DeleteResult(0))

            async def merge_document(self, *_args, **_kwargs):
                return MergeOutcome(matched=False, applied=False)

        class Sink:
            journal_path = None

            def __init__(self):
                self.state = SimpleNamespace(next_token=2)
                self.aligned = []
                self.gaps = 0
                self.events = []

            def align_commit_sequence(self, sequence):
                self.aligned.append(sequence)

            def mark_gap(self):
                self.gaps += 1

            def publish(self, **payload):
                self.events.append(payload)

        engine = Engine()
        sink = Sink()
        engine.changes = [
            CommittedChange(1, {'operation_type': 'insert'}),
            CommittedChange(2, None),
            CommittedChange(3, {'operation_type': 'delete'}),
        ]
        adapter = EngineSpiAdapter(engine)

        adapter.prepare_change_delivery(sink)
        adapter.dispatch_committed_changes(sink)
        adapter.unregister_change_delivery(sink)
        adapter.unregister_change_delivery(None)

        assert sink.gaps == 1
        assert sink.events == [{'operation_type': 'delete'}]
        assert sink.aligned == [1, 2, 3]
        assert len(engine.unregistered) == 1


if __name__ == '__main__':
    unittest.main()
