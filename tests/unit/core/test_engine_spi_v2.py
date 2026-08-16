import asyncio
import unittest
import warnings

from types import SimpleNamespace

from mongoeco.api.operations import compile_update_operation

# Literal protocol versions are the values under contract in these tests.
# unittest is the repository's established contract-test harness.
# ruff: noqa: PLR2004, PT027
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.engines import adapter as adapter_module
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
    BulkOutcome,
    CommittedChange,
    DeleteOutcome,
    EngineDeleteResult,
    EngineUpdateResult,
    FindAndModifyOutcome,
    InsertOutcome,
    MergeDocumentResult,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.session import ClientSession
from mongoeco.types import BulkWriteResult, DeleteResult, UpdateResult


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
        self.calls.append(("insert", args, kwargs))
        return InsertOutcome(applied=True, document=args[2])

    async def insert_documents(self, *args, **kwargs):
        self.calls.append(("insert_many", args, kwargs))
        return tuple(
            InsertOutcome(applied=True, document=document) for document in args[2]
        )

    async def get_document(self, *args, **kwargs):
        self.calls.append(("get", args, kwargs))
        return {"_id": args[2]}

    async def count_find_semantics(self, *args, **kwargs):
        self.calls.append(("count", args, kwargs))
        return 3

    async def update_with_operation(self, *args, **kwargs):
        self.calls.append(("update", args, kwargs))
        return MutationOutcome(
            result=UpdateResult(1, 1),
            before_document={"_id": 1, "value": "before"},
            after_document={"_id": 1, "value": "after"},
        )

    async def delete_with_operation(self, *args, **kwargs):
        self.calls.append(("delete", args, kwargs))
        return DeleteOutcome(
            result=DeleteResult(1),
            deleted_document={"_id": 1},
        )

    async def merge_document(self, *args, **kwargs):
        self.calls.append(("merge", args, kwargs))
        return MergeOutcome(
            matched=False,
            applied=True,
            operation_type="insert",
            after_document=args[2],
        )

    def open_read_snapshot(self, *args, **kwargs):
        self.calls.append(("snapshot", args, kwargs))
        return ReadSnapshot(
            _EmptySource(),
            policy=SnapshotPolicy.STABLE,
            operation_id=kwargs["operation_context"].operation_id,
        )


class _LegacyEngine:
    supports_commit_callbacks = True

    def __init__(self):
        self.calls = []

    async def put_document(self, *args, **kwargs):
        self.calls.append(("insert", args, kwargs))
        callback = kwargs.get("on_commit")
        if callback is not None:
            callback(args[2])
        return True

    async def put_documents_bulk(self, *args, **kwargs):
        self.calls.append(("insert_many", args, kwargs))
        callback = kwargs.get("on_commit")
        if callback is not None:
            for document in args[2]:
                callback(document)
        return [True for _document in args[2]]

    async def get_document(self, *args, **kwargs):
        self.calls.append(("get", args, kwargs))
        return {"_id": args[2]}

    async def count_find_semantics(self, *args, **kwargs):
        self.calls.append(("count", args, kwargs))
        return 2

    async def update_with_operation(self, *args, **kwargs):
        self.calls.append(("update", args, kwargs))
        result = UpdateResult(1, 1)
        callback = kwargs.get("on_commit")
        if callback is not None:
            callback(MutationOutcome(result=result))
        return result

    async def delete_with_operation(self, *args, **kwargs):
        self.calls.append(("delete", args, kwargs))
        result = DeleteResult(1)
        callback = kwargs.get("on_commit")
        if callback is not None:
            callback(DeleteOutcome(result=result))
        return result

    async def merge_document(self, *args, **kwargs):
        self.calls.append(("merge", args, kwargs))
        outcome = MergeOutcome(
            matched=False,
            applied=True,
            operation_type="insert",
            after_document=args[2],
        )
        callback = kwargs.get("on_commit")
        if callback is not None:
            callback(outcome)
        return outcome

    def scan_find_semantics(self, *args, **kwargs):
        self.calls.append(("snapshot", args, kwargs))
        return _EmptySource()


class EngineSpiV2ContractTests(unittest.TestCase):
    def test_adapter_argument_resolution_has_explicit_missing_semantics(self):
        marker = object()

        assert (
            adapter_module._call_argument(
                (),
                {},
                name="value",
                index=2,
                default=marker,
            )
            is marker
        )
        with self.assertRaisesRegex(TypeError, "missing required"):
            adapter_module._call_argument(
                (),
                {},
                name="value",
                index=2,
            )

    def test_capabilities_reject_invalid_version_and_delivery_mode(self):
        with self.assertRaisesRegex(ValueError, "supported versions"):
            EngineCapabilities(spi_version=True)
        with self.assertRaisesRegex(ValueError, "supported versions"):
            EngineCapabilities(spi_version=3)
        with self.assertRaisesRegex(ValueError, "delivery mode"):
            EngineCapabilities(change_delivery="future")
        with self.assertRaisesRegex(ValueError, "native mutation outcomes"):
            EngineCapabilities(mutation_outcomes=False)
        with self.assertRaisesRegex(ValueError, "legacy callback"):
            EngineCapabilities(change_delivery="legacy-callback")
        assert not EngineCapabilities(
            explicit_read_snapshots=False,
        ).explicit_read_snapshots
        with self.assertRaisesRegex(TypeError, "batch_inserts"):
            EngineCapabilities(batch_inserts=1)
        with self.assertRaisesRegex(ValueError, "native mutation outcomes"):
            EngineCapabilities(spi_version=1)
        with self.assertRaisesRegex(ValueError, "explicit read snapshots"):
            EngineCapabilities(
                spi_version=1,
                mutation_outcomes=False,
                explicit_read_snapshots=True,
            )
        with self.assertRaisesRegex(ValueError, "sequenced"):
            EngineCapabilities(
                spi_version=1,
                mutation_outcomes=False,
                change_delivery="commit-sequence",
            )

        outbox = EngineCapabilities(change_delivery="transactional-outbox")
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
        assert "put_document" not in AsyncCrudEngine.__dict__
        assert "delete_document" not in AsyncCrudEngine.__dict__
        assert "scan_find_semantics" not in AsyncReadSemanticsEngine.__dict__

    def test_provisional_outcome_names_remain_exact_aliases(self):
        assert EngineUpdateResult is MutationOutcome
        assert EngineDeleteResult is DeleteOutcome
        assert MergeDocumentResult is MergeOutcome
        assert isinstance(
            EngineUpdateResult(
                result=UpdateResult(1, 1),
                before_document={"_id": 1},
                after_document={"_id": 1, "changed": True},
            ),
            MutationOutcome,
        )
        assert isinstance(
            EngineDeleteResult(
                result=DeleteResult(1),
                deleted_document={"_id": 1},
            ),
            DeleteOutcome,
        )

    def test_public_outcomes_take_defensive_ownership_of_documents(self):
        before = {"_id": 1, "nested": {"values": [1]}}
        after = {"_id": 1, "nested": {"values": [2]}}
        mutation = MutationOutcome(
            result=UpdateResult(1, 1),
            before_document=before,
            after_document=after,
        )
        deleted = DeleteOutcome(
            result=DeleteResult(1),
            deleted_document=before,
        )
        inserted = InsertOutcome(applied=True, document=after)
        merged = MergeOutcome(
            matched=True,
            applied=True,
            operation_type="replace",
            before_document=before,
            after_document=after,
        )
        find_and_modify = FindAndModifyOutcome(
            captured=mutation,
            value=after,
        )
        change = CommittedChange(sequence=1, payload=after)

        before["nested"]["values"].append(3)
        after["nested"]["values"].append(4)

        assert mutation.before_document == {"_id": 1, "nested": {"values": [1]}}
        assert mutation.after_document == {"_id": 1, "nested": {"values": [2]}}
        assert deleted.deleted_document == {"_id": 1, "nested": {"values": [1]}}
        assert inserted.document == {"_id": 1, "nested": {"values": [2]}}
        assert merged.before_document == {"_id": 1, "nested": {"values": [1]}}
        assert merged.after_document == {"_id": 1, "nested": {"values": [2]}}
        assert find_and_modify.value == {"_id": 1, "nested": {"values": [2]}}
        assert change.payload == {"_id": 1, "nested": {"values": [2]}}

        with self.assertRaisesRegex(TypeError, "MutationOutcome"):
            FindAndModifyOutcome(captured=object(), value=None)  # type: ignore[arg-type]

    def test_legacy_capability_detection_is_centralized(self):
        class LegacyEngine:
            supports_injected_clock = True
            supports_commit_callbacks = True

        capabilities = resolve_engine_capabilities(LegacyEngine())

        assert capabilities.spi_version == 1
        assert capabilities.injected_clock
        assert not capabilities.mutation_outcomes
        assert capabilities.change_delivery == "legacy-callback"

    def test_native_capabilities_are_returned_without_inference(self):
        declared = EngineCapabilities(
            injected_clock=True,
            explicit_read_snapshots=True,
            change_delivery="commit-sequence",
        )

        class NativeEngine:
            capabilities = declared
            supports_injected_clock = False

        assert resolve_engine_capabilities(NativeEngine()) is declared

    def test_inherited_v2_capabilities_ignore_legacy_clock_flags(self):
        class ExternalMemoryEngine(MemoryEngine):
            supports_injected_clock = False

        capabilities = resolve_engine_capabilities(ExternalMemoryEngine())

        assert capabilities.injected_clock
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
            warnings.simplefilter("always")
            adapter = adapt_engine(LegacyEngine())
            adapt_engine(LegacyEngine())

        assert isinstance(adapter, LegacyEngineAdapter)
        assert len(captured) == 1
        assert issubclass(captured[0].category, DeprecationWarning)
        assert "before MongoEco 5.0.0" in str(captured[0].message)
        assert isinstance(adapt_engine(MemoryEngine()), EngineSpiAdapter)

    def test_v2_capability_declarations_are_validated_eagerly(self):
        class IncompleteEngine:
            capabilities = EngineCapabilities()

        with self.assertRaisesRegex(TypeError, "missing required methods"):
            validate_engine_contract(
                IncompleteEngine(),
                IncompleteEngine.capabilities,
            )

    def test_native_adapter_covers_all_typed_boundaries(self):
        async def _exercise():
            engine = _NativeEngine()
            adapter = EngineSpiAdapter(engine)
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
            )
            callbacks = []

            inserted = await adapter.insert_outcome(
                "db",
                "coll",
                {"_id": 1},
                operation_context=context,
                on_commit=callbacks.append,
            )
            batch = await adapter.insert_many_outcomes(
                "db",
                "coll",
                [{"_id": 2}, {"_id": 3}],
                operation_context=context,
                on_commit=callbacks.append,
            )
            updated = await adapter.update_outcome(
                "db",
                "coll",
                object(),
                operation_context=context,
                context="legacy",
                dialect="legacy",
                on_commit=callbacks.append,
            )
            deleted = await adapter.delete_outcome(
                "db",
                "coll",
                object(),
                operation_context=context,
                context="legacy",
                dialect="legacy",
                on_commit=callbacks.append,
            )
            merged = await adapter.merge_outcome(
                "db",
                "coll",
                {"_id": 4},
                operation_context=context,
                context="legacy",
                on_commit=callbacks.append,
            )
            document = await adapter.get_document(
                "db",
                "coll",
                1,
                operation_context=context,
            )
            count = await adapter.count_documents(
                "db",
                "coll",
                object(),
                operation_context=context,
            )
            snapshot = adapter.open_read_snapshot(
                "db",
                "coll",
                object(),
                operation_context=context,
            )

            assert inserted.applied
            assert len(batch) == 2
            assert updated.modified_count == 1
            assert deleted.deleted_count == 1
            assert merged.applied
            assert document == {"_id": 1}
            assert count == 3
            assert isinstance(snapshot, ReadSnapshot)
            assert len(callbacks) == 6
            update_call = next(call for call in engine.calls if call[0] == "update")
            assert "context" not in update_call[2]
            assert "dialect" not in update_call[2]

        asyncio.run(_exercise())

    def test_legacy_adapter_translates_context_callbacks_and_results(self):
        async def _exercise():
            engine = _LegacyEngine()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                adapter = adapt_engine(engine)
            context = SimpleNamespace(session="session", operation_id="op")
            callbacks = []

            await adapter.insert_outcome(
                "db",
                "coll",
                {"_id": 1},
                operation_context=context,
                on_commit=callbacks.append,
            )
            await adapter.insert_many_outcomes(
                "db",
                "coll",
                [{"_id": 2}, {"_id": 3}],
                operation_context=context,
                on_commit=callbacks.append,
            )
            updated = await adapter.update_outcome(
                "db",
                "coll",
                object(),
                operation_context=context,
                on_commit=callbacks.append,
            )
            deleted = await adapter.delete_outcome(
                "db",
                "coll",
                object(),
                operation_context=context,
                on_commit=callbacks.append,
            )
            await adapter.merge_outcome(
                "db",
                "coll",
                {"_id": 4},
                operation_context=context,
                on_commit=callbacks.append,
            )
            await adapter.get_document(
                "db",
                "coll",
                1,
                operation_context=context,
            )
            count = await adapter.count_documents(
                "db",
                "coll",
                object(),
                operation_context=context,
            )
            snapshot = adapter.open_read_snapshot(
                "db",
                "coll",
                object(),
                operation_context=context,
            )

            assert isinstance(updated, MutationOutcome)
            assert isinstance(deleted, DeleteOutcome)
            assert count == 2
            assert snapshot.metadata.operation_id == "op"
            assert len(callbacks) == 6
            for _name, _args, kwargs in engine.calls:
                assert kwargs.get("context") == "session"

        asyncio.run(_exercise())

    def test_adapter_rejects_invalid_engine_results(self):
        adapter = EngineSpiAdapter(_NativeEngine())

        with self.assertRaisesRegex(TypeError, "MutationOutcome"):
            adapter._require_mutation_outcome(UpdateResult(1, 1))
        with self.assertRaisesRegex(TypeError, "DeleteOutcome"):
            adapter._require_delete_outcome(DeleteResult(1))
        with self.assertRaisesRegex(TypeError, "InsertOutcome"):
            adapter._require_insert_outcome(result=True)
        with self.assertRaisesRegex(TypeError, "MergeOutcome"):
            engine = _NativeEngine()

            async def invalid_merge(*_args, **_kwargs):
                return object()

            engine.merge_document = invalid_merge
            context = OperationContext.create(dialect=MONGODB_DIALECT_70)
            asyncio.run(
                EngineSpiAdapter(engine).merge_outcome(
                    "db",
                    "c",
                    {},
                    operation_context=context,
                ),
            )

    def test_v2_adapter_rejects_incomplete_applied_outcomes(self):
        adapter = EngineSpiAdapter(_NativeEngine())

        with self.assertRaisesRegex(RuntimeError, "after image"):
            adapter._require_mutation_outcome(
                MutationOutcome(
                    result=UpdateResult(1, 1),
                    before_document={"_id": 1},
                ),
            )
        with self.assertRaisesRegex(RuntimeError, "before image"):
            adapter._require_mutation_outcome(
                MutationOutcome(
                    result=UpdateResult(1, 1),
                    after_document={"_id": 1},
                ),
            )
        with self.assertRaisesRegex(RuntimeError, "after image"):
            adapter._require_mutation_outcome(
                MutationOutcome(
                    result=UpdateResult(1, 0),
                    before_document={"_id": 1},
                ),
            )
        with self.assertRaisesRegex(RuntimeError, "deleted image"):
            adapter._require_delete_outcome(
                DeleteOutcome(result=DeleteResult(1)),
            )
        with self.assertRaisesRegex(RuntimeError, "document"):
            adapter._require_insert_outcome(InsertOutcome(applied=True))

    def test_v2_adapter_rejects_invalid_batch_and_snapshot_contracts(self):
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)
        engine = _NativeEngine()

        async def incomplete_batch(*args, **_kwargs):
            return (InsertOutcome(applied=True, document=args[2][0]),)

        engine.insert_documents = incomplete_batch
        with self.assertRaisesRegex(RuntimeError, "cardinality"):
            asyncio.run(
                EngineSpiAdapter(engine).insert_many_outcomes(
                    "db",
                    "coll",
                    [{"_id": 1}, {"_id": 2}],
                    operation_context=context,
                ),
            )

        engine = _NativeEngine()
        engine.open_read_snapshot = lambda *_args, **_kwargs: object()
        with self.assertRaisesRegex(TypeError, "ReadSnapshot"):
            EngineSpiAdapter(engine).open_read_snapshot(
                "db",
                "coll",
                object(),
                operation_context=context,
            )

        engine = _NativeEngine()
        wrong_identity = ReadSnapshot(
            _EmptySource(),
            policy=SnapshotPolicy.STABLE,
            operation_id="another-operation",
        )
        engine.open_read_snapshot = lambda *_args, **_kwargs: wrong_identity
        with self.assertRaisesRegex(RuntimeError, "identity"):
            EngineSpiAdapter(engine).open_read_snapshot(
                "db",
                "coll",
                object(),
                operation_context=context,
            )
        assert wrong_identity.closed

        engine = _NativeEngine()
        live_snapshot = ReadSnapshot(
            _EmptySource(),
            policy=SnapshotPolicy.LIVE,
            operation_id=context.operation_id,
        )
        engine.open_read_snapshot = lambda *_args, **_kwargs: live_snapshot
        with self.assertRaisesRegex(RuntimeError, "stable"):
            EngineSpiAdapter(engine).open_read_snapshot(
                "db",
                "coll",
                object(),
                operation_context=context,
            )
        assert live_snapshot.closed

    def test_v2_adapter_falls_back_without_a_native_batch(self):
        class NonBatchEngine(_NativeEngine):
            capabilities = EngineCapabilities(
                batch_inserts=False,
                explicit_read_snapshots=True,
            )

            async def insert_document(
                self,
                db_name,
                coll_name,
                document,
                *,
                operation_context,
                bypass_document_validation=False,
            ):
                self.calls.append(
                    (
                        "insert",
                        (db_name, coll_name, document),
                        {
                            "operation_context": operation_context,
                            "bypass_document_validation": (bypass_document_validation),
                        },
                    ),
                )
                return InsertOutcome(applied=True, document=document)

            async def insert_documents(self, *_args, **_kwargs):
                message = "batch method must not be called"
                raise AssertionError(message)

        async def _exercise():
            engine = NonBatchEngine()
            adapter = EngineSpiAdapter(engine)
            context = OperationContext.create(dialect=MONGODB_DIALECT_70)
            published = []

            outcomes = await adapter.insert_many_outcomes(
                "db",
                "coll",
                [{"_id": 1}, {"_id": 2}],
                operation_context=context,
                bypass_document_validation=True,
                on_commit=published.append,
            )

            insert_calls = [call for call in engine.calls if call[0] == "insert"]
            assert [item.document for item in outcomes] == [
                {"_id": 1},
                {"_id": 2},
            ]
            assert [
                call[2]["operation_context"].change_event_index for call in insert_calls
            ] == [0, 1]
            assert all(
                call[2]["operation_context"].operation_id == context.operation_id
                for call in insert_calls
            )
            assert len(published) == 2

        asyncio.run(_exercise())

    def test_v2_batch_fallback_accepts_a_fully_keyword_call(self):
        class NonBatchEngine(_NativeEngine):
            capabilities = EngineCapabilities(
                batch_inserts=False,
                explicit_read_snapshots=True,
            )

            async def insert_document(
                self,
                db_name,
                coll_name,
                document,
                *,
                operation_context,
            ):
                self.calls.append((db_name, coll_name, document))
                return InsertOutcome(applied=True, document=document)

        async def _exercise():
            engine = NonBatchEngine()
            context = OperationContext.create(dialect=MONGODB_DIALECT_70)

            outcomes = await EngineSpiAdapter(engine).insert_many_outcomes(
                db_name="db",
                coll_name="items",
                documents=[{"_id": 1}, {"_id": 2}],
                operation_context=context,
            )

            assert [item.document for item in outcomes] == [
                {"_id": 1},
                {"_id": 2},
            ]
            assert engine.calls == [
                ("db", "items", {"_id": 1}),
                ("db", "items", {"_id": 2}),
            ]

        asyncio.run(_exercise())

    def test_outcome_invariants_reject_impossible_states(self):
        with self.assertRaisesRegex(ValueError, "counts"):
            MutationOutcome(result=UpdateResult(1, 2))
        with self.assertRaisesRegex(ValueError, "non-matching"):
            MutationOutcome(
                result=UpdateResult(0, 0),
                after_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "upsert"):
            MutationOutcome(
                result=UpdateResult(0, 0, upserted_id=1),
                before_document={"_id": 1},
                after_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "cannot also match"):
            MutationOutcome(
                result=UpdateResult(1, 0, upserted_id=1),
                after_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "unapplied mutation"):
            MutationOutcome(
                result=UpdateResult(1, 0),
                commit_sequence=1,
            )
        with self.assertRaisesRegex(ValueError, "zero or one"):
            DeleteOutcome(result=DeleteResult(2))
        with self.assertRaisesRegex(ValueError, "expose an image"):
            DeleteOutcome(
                result=DeleteResult(0),
                deleted_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "unapplied delete"):
            DeleteOutcome(result=DeleteResult(0), commit_sequence=1)
        with self.assertRaisesRegex(TypeError, "bool"):
            InsertOutcome(applied=1)
        with self.assertRaisesRegex(ValueError, "expose a document"):
            InsertOutcome(applied=False, document={"_id": 1})
        with self.assertRaisesRegex(ValueError, "unapplied insert"):
            InsertOutcome(applied=False, commit_sequence=1)
        with self.assertRaisesRegex(ValueError, "positive"):
            CommittedChange(0, None)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            InsertOutcome(applied=True, commit_sequence=True)

        mutation = MutationOutcome(result=UpdateResult(1, 0))
        deletion = DeleteOutcome(result=DeleteResult(0))
        insertion = InsertOutcome(applied=True, document={"_id": 1})
        assert mutation.matched_count == 1
        assert mutation.modified_count == 0
        assert mutation.upserted_id is None
        assert deletion.deleted_count == 0
        assert insertion
        assert CommittedChange(1, None).is_gap

        bulk_result = BulkWriteResult(
            inserted_count=1,
            matched_count=0,
            modified_count=0,
            deleted_count=0,
            upserted_count=0,
            upserted_ids={},
        )
        bulk = BulkOutcome(result=bulk_result, mutations=(insertion,))
        assert bulk.result is bulk_result
        with self.assertRaisesRegex(TypeError, "BulkWriteResult"):
            BulkOutcome(result=object())
        with self.assertRaisesRegex(TypeError, "tuple"):
            BulkOutcome(result=bulk_result, mutations=[insertion])

        delivered = CommittedChange(1, {"nested": {"value": 1}})
        isolated = delivered.for_delivery()
        isolated.payload["nested"]["value"] = 2
        assert delivered.payload == {"nested": {"value": 1}}

    def test_merge_outcome_invariants_reject_impossible_states(self):
        with self.assertRaisesRegex(TypeError, "bools"):
            MergeOutcome(matched=1, applied=False)
        with self.assertRaisesRegex(ValueError, "operation and after image"):
            MergeOutcome(matched=False, applied=True)
        with self.assertRaisesRegex(ValueError, "matched merge"):
            MergeOutcome(
                matched=True,
                applied=True,
                operation_type="insert",
                before_document={"_id": 1},
                after_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "unmatched merge"):
            MergeOutcome(
                matched=False,
                applied=True,
                operation_type="update",
                after_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "requires a before image"):
            MergeOutcome(
                matched=True,
                applied=True,
                operation_type="update",
                after_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "cannot expose an effect"):
            MergeOutcome(
                matched=False,
                applied=False,
                operation_type="insert",
                after_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "before image"):
            MergeOutcome(
                matched=False,
                applied=False,
                before_document={"_id": 1},
            )
        with self.assertRaisesRegex(ValueError, "unapplied merge"):
            MergeOutcome(
                matched=False,
                applied=False,
                commit_sequence=1,
            )

    def test_v2_adapter_rejects_missing_operation_context(self):
        adapter = EngineSpiAdapter(_NativeEngine())

        with self.assertRaisesRegex(TypeError, "OperationContext"):
            asyncio.run(adapter.insert_outcome("db", "coll", {"_id": 1}))

    def test_v2_adapter_rejects_a_divergent_embedded_update_context(self):
        adapter = EngineSpiAdapter(_NativeEngine())
        first = OperationContext.create(dialect=MONGODB_DIALECT_70)
        second = OperationContext.create(dialect=MONGODB_DIALECT_70)
        operation = compile_update_operation(
            {"_id": 1},
            update_spec={"$set": {"value": 1}},
            dialect=MONGODB_DIALECT_70,
        ).with_overrides(context=first, let=first.expressions)

        with self.assertRaisesRegex(RuntimeError, "divergent context"):
            asyncio.run(
                adapter.update_outcome(
                    "db",
                    "coll",
                    operation,
                    operation_context=second,
                ),
            )
        with self.assertRaisesRegex(RuntimeError, "divergent context"):
            asyncio.run(
                adapter.update_outcome(
                    db_name="db",
                    coll_name="coll",
                    operation=operation,
                    operation_context=second,
                ),
            )

    def test_bound_operation_rejects_duplicate_semantic_authorities(self):
        operation = compile_update_operation(
            {"_id": 1},
            update_spec={"$set": {"value": "$$tenant"}},
            collation={"locale": "en"},
            let={"tenant": "first"},
            dialect=MONGODB_DIALECT_70,
        )
        collation_context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            collation={"locale": "simple"},
            bindings={"tenant": "first"},
        )
        variables_context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            collation={"locale": "en"},
            bindings={"tenant": "second"},
        )

        with self.assertRaisesRegex(ValueError, "collation diverges"):
            operation.with_overrides(context=collation_context)
        with self.assertRaisesRegex(ValueError, "variables diverge"):
            operation.with_overrides(context=variables_context)

    def test_v2_adapter_rejects_divergent_read_semantics_context(self):
        adapter = EngineSpiAdapter(_NativeEngine())
        first = OperationContext.create(dialect=MONGODB_DIALECT_70)
        second = OperationContext.create(dialect=MONGODB_DIALECT_70)
        semantics = compile_find_semantics(
            {},
            operation_context=first,
        )

        with self.assertRaisesRegex(RuntimeError, "divergent context"):
            adapter.open_read_snapshot(
                "db",
                "coll",
                semantics,
                operation_context=second,
            )
        with self.assertRaisesRegex(RuntimeError, "divergent context"):
            adapter.open_read_snapshot(
                db_name="db",
                coll_name="coll",
                semantics=semantics,
                operation_context=second,
            )

    def test_v2_adapter_preserves_the_declared_scan_fallback(self):
        class ScanEngine(_NativeEngine):
            capabilities = EngineCapabilities(
                explicit_read_snapshots=False,
            )

            def scan_find_semantics(self, *args, **kwargs):
                self.calls.append(("scan", args, kwargs))
                return _EmptySource()

        engine = ScanEngine()
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)
        semantics = compile_find_semantics(
            {},
            operation_context=context,
        )

        snapshot = EngineSpiAdapter(engine).open_read_snapshot(
            "db",
            "coll",
            semantics,
            operation_context=context,
        )

        assert snapshot.metadata.policy is SnapshotPolicy.STABLE
        assert snapshot.metadata.operation_id == context.operation_id
        assert [call[0] for call in engine.calls] == ["scan"]
        assert engine.calls[0][2]["context"] is context.session

    def test_sequenced_outcome_may_defer_sequence_until_transaction_commit(
        self,
    ):
        class SequencedEngine(_NativeEngine):
            capabilities = EngineCapabilities(
                explicit_read_snapshots=True,
                change_delivery="commit-sequence",
            )

            def register_change_consumer(self, *_args, **_kwargs):
                return 0

            def unregister_change_consumer(self, *_args, **_kwargs):
                return None

            def dispatch_committed_changes(self, *_args, **_kwargs):
                return None

        session = ClientSession()
        session.start_transaction()
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            session=session,
            publication=ChangePublicationPolicy.EMIT,
        )

        outcome = asyncio.run(
            EngineSpiAdapter(SequencedEngine()).insert_outcome(
                "db",
                "coll",
                {"_id": 1},
                operation_context=context,
            ),
        )

        assert outcome.applied
        session.abort_transaction()
        with self.assertRaisesRegex(RuntimeError, "commit_sequence"):
            asyncio.run(
                EngineSpiAdapter(SequencedEngine()).insert_outcome(
                    "db",
                    "coll",
                    {"_id": 1},
                    operation_context=context,
                ),
            )

    def test_change_delivery_handles_replay_gap_event_and_unregister(self):
        class Engine:
            capabilities = EngineCapabilities(
                explicit_read_snapshots=True,
                change_delivery="commit-sequence",
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
                return InsertOutcome(applied=True, document={"_id": 1})

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

            def open_read_snapshot(self, *_args, **_kwargs):
                return ReadSnapshot(
                    _EmptySource(),
                    policy=SnapshotPolicy.STABLE,
                )

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
            CommittedChange(1, {"operation_type": "insert"}),
            CommittedChange(2, None),
            CommittedChange(3, {"operation_type": "delete"}),
        ]
        adapter = EngineSpiAdapter(engine)

        adapter.prepare_change_delivery(sink)
        adapter.dispatch_committed_changes(sink)
        adapter.unregister_change_delivery(sink)
        adapter.unregister_change_delivery(None)

        assert sink.gaps == 1
        assert sink.events == [{"operation_type": "delete"}]
        assert sink.aligned == [1, 2, 3]
        assert len(engine.unregistered) == 1


if __name__ == "__main__":
    unittest.main()
