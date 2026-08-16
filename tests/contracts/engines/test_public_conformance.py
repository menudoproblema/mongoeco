import unittest

from contextlib import suppress
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from jsonschema import Draft202012Validator

from mongoeco.conformance import (
    CONFORMANCE_REPORT_SCHEMA_VERSION,
    ConformanceCheckResult,
    ConformancePhase,
    ConformanceProfile,
    ConformanceReport,
    ConformanceStatus,
    EngineConformanceProvider,
    conformance_report_schema,
    run_engine_conformance,
)
from mongoeco.conformance.pytest import assert_conformance
from mongoeco.conformance.runner import (
    _check_atomic_mutation,
    _check_batch_outcomes,
    _check_capabilities,
    _check_change_delivery,
    _check_change_delivery_recovery,
    _check_crud_outcomes,
    _check_injected_clock,
    _check_operation_context,
    _check_search,
    _check_snapshot,
)
from mongoeco.core.search_models import (
    SearchCountResult,
    SearchExecutionMode,
    SearchExecutionOutcome,
    SearchExplainVerbosity,
    SearchMetadata,
)
from mongoeco.engines import EngineCapabilities, MemoryEngine, SQLiteEngine
from mongoeco.engines.results import (
    CommittedChange,
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from mongoeco.types import DeleteResult, UpdateResult


class _Documents:
    def __init__(self, documents):
        self._documents = iter(documents)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._documents)
        except StopIteration as error:
            raise StopAsyncIteration from error


class _FaultyOutcomeEngine:
    def __init__(self, fault):
        self.fault = fault

    async def insert_document(self, *_args, **_kwargs):
        if self.fault == "insert":
            return None
        return InsertOutcome(applied=True, document={"_id": "value"})

    async def update_with_operation(self, *_args, **_kwargs):
        if self.fault == "update":
            return None
        return MutationOutcome(
            result=UpdateResult(1, 1),
            before_document={"_id": "value", "revision": 1},
            after_document={"_id": "value", "revision": 2},
        )

    async def merge_document(self, *_args, **_kwargs):
        if self.fault == "merge":
            return None
        return MergeOutcome(
            matched=False,
            applied=True,
            operation_type="insert",
            after_document={"_id": "merged"},
        )

    async def delete_with_operation(self, *_args, **_kwargs):
        if self.fault == "delete":
            return None
        return DeleteOutcome(
            result=DeleteResult(1),
            deleted_document={"_id": "value"},
        )

    async def get_document(self, *_args, **_kwargs):
        if self.fault == "context":
            return None
        return {"_id": "context"}


class _SnapshotEngine:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    async def insert_document(self, *_args, **_kwargs):
        return InsertOutcome(applied=True, document={"_id": "snapshot"})

    def open_read_snapshot(self, *_args, **_kwargs):
        return self.snapshot


class _LiveDocuments:
    def __init__(self, documents):
        self.documents = documents
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.documents):
            raise StopAsyncIteration
        document = self.documents[self.index]
        self.index += 1
        return document


class _MutableReadSnapshot(ReadSnapshot):
    async def __anext__(self):
        try:
            return await self._source.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise


class _LiveSnapshotEngine:
    def __init__(self, *, mutable_items=False):
        self.documents = []
        self.mutable_items = mutable_items

    async def insert_document(self, _db, _coll, document, **_kwargs):
        owned = deepcopy(document)
        self.documents.append(owned)
        return InsertOutcome(applied=True, document=owned)

    def open_read_snapshot(self, *_args, **_kwargs):
        source = _LiveDocuments(
            list(self.documents) if self.mutable_items else self.documents,
        )
        snapshot_type = _MutableReadSnapshot if self.mutable_items else ReadSnapshot
        return snapshot_type(source, policy=SnapshotPolicy.STABLE)

    async def get_document(self, _db, _coll, identifier, **_kwargs):
        return next(
            (
                document
                for document in self.documents
                if document.get("_id") == identifier
            ),
            None,
        )


class PublicEngineConformanceTests(unittest.IsolatedAsyncioTestCase):
    def test_provider_validates_identity_and_factory(self):
        with self.assertRaisesRegex(ValueError, "provider name"):
            EngineConformanceProvider("", MemoryEngine)
        with self.assertRaisesRegex(TypeError, "factory"):
            EngineConformanceProvider("invalid", object())
        with self.assertRaisesRegex(ValueError, "namespace_prefix"):
            EngineConformanceProvider("invalid", MemoryEngine, namespace_prefix="")
        with self.assertRaisesRegex(TypeError, "cleanup"):
            EngineConformanceProvider("invalid", MemoryEngine, cleanup=object())

    async def test_provider_supports_async_factories_and_lifecycle(self):
        lifecycle = []

        class _Engine:
            async def connect(self):
                lifecycle.append("connect")

            async def disconnect(self):
                lifecycle.append("disconnect")

        async def factory():
            lifecycle.append("factory")
            return _Engine()

        provider = EngineConformanceProvider("async", factory)
        db_name, coll_name = provider.namespace("stable-snapshot")
        self.assertIn("stable_snapshot", db_name)
        self.assertEqual(coll_name, "items")

        async with provider.open_engine() as engine:
            self.assertIsInstance(engine, _Engine)
            lifecycle.append("body")

        self.assertEqual(
            lifecycle,
            ["factory", "connect", "body", "disconnect"],
        )

    async def test_provider_supports_synchronous_cleanup_callbacks(self):
        cleaned = []
        provider = EngineConformanceProvider(
            "sync-cleanup",
            MemoryEngine,
            cleanup=lambda engine, db_name: cleaned.append((engine, db_name)),
        )
        engine = MemoryEngine()

        await provider.cleanup_namespace(engine, "temporary")

        self.assertEqual(cleaned, [(engine, "temporary")])

    async def test_capability_and_batch_checks_reject_false_declarations(self):
        class _FalseCapabilityEngine:
            capabilities = EngineCapabilities()

        with self.assertRaisesRegex(AssertionError, "missing required methods"):
            await _check_capabilities(_FalseCapabilityEngine(), "db", "items")
        report = await run_engine_conformance(
            EngineConformanceProvider("false-capability", _FalseCapabilityEngine),
            profiles=(ConformanceProfile.SPI_V2_CORE,),
        )
        capability_check = next(
            check for check in report.checks if check.name == "capabilities"
        )
        self.assertIs(capability_check.status, ConformanceStatus.FAILED)

        engine = MemoryEngine()
        incomplete = (InsertOutcome(applied=True, document={"_id": "batch-1"}),)
        with (
            patch.object(engine, "insert_documents", return_value=incomplete),
            self.assertRaisesRegex(AssertionError, "cardinality is inconsistent"),
        ):
            await _check_batch_outcomes(engine, "db", "items")

    async def test_builtin_engines_pass_the_public_installed_contract(self):
        for factory in (MemoryEngine, SQLiteEngine):
            with self.subTest(engine=factory.__name__):
                report = await run_engine_conformance(
                    EngineConformanceProvider(factory.__name__, factory),
                )

                assert_conformance(report)
                Draft202012Validator(conformance_report_schema()).validate(
                    report.to_document(),
                )
                self.assertEqual(report.contract_version, "spi-v2")
                self.assertEqual(
                    report.schema_version,
                    CONFORMANCE_REPORT_SCHEMA_VERSION,
                )
                self.assertIn('"schemaVersion"', report.to_json(indent=None))
                self.assertIn(factory.__name__, report.human_summary())
                self.assertTrue(
                    any(
                        check.profile is ConformanceProfile.SEARCH_V1
                        for check in report.checks
                    ),
                )
                self.assertTrue(
                    {
                        ConformanceProfile.SPI_V2_ATOMICITY,
                        ConformanceProfile.SPI_V2_CLOCK,
                    }.issubset({check.profile for check in report.checks}),
                )

    async def test_report_accumulates_failure_without_pytest_dependency(self):
        class _LegacyEngine:
            pass

        report = await run_engine_conformance(
            EngineConformanceProvider("legacy", _LegacyEngine),
            profiles=(ConformanceProfile.SPI_V2_CORE,),
        )

        self.assertFalse(report.passed)
        self.assertGreaterEqual(len(report.failures), 1)
        with self.assertRaisesRegex(AssertionError, "failed conformance"):
            report.require_success()

    async def test_report_with_no_selected_checks_cannot_pass_vacuously(self):
        report = await run_engine_conformance(
            EngineConformanceProvider("memory", MemoryEngine),
            profiles=(),
        )

        self.assertFalse(report.passed)
        self.assertEqual(report.checks, ())
        with self.assertRaisesRegex(AssertionError, "no applicable"):
            report.require_success()

    async def test_report_exposes_inapplicable_partial_capabilities(self):
        class _LegacyEngine:
            pass

        report = await run_engine_conformance(
            EngineConformanceProvider("legacy", _LegacyEngine),
            profiles=(ConformanceProfile.SEARCH_V1,),
        )

        self.assertFalse(report.passed)
        self.assertEqual(len(report.inapplicable), 1)
        self.assertIs(
            report.inapplicable[0].status,
            ConformanceStatus.NOT_APPLICABLE,
        )
        self.assertEqual(
            report.inapplicable[0].to_document()["status"],
            "not-applicable",
        )
        self.assertEqual(report.inapplicable[0].capability, "search-v1")

    def test_check_result_retains_the_45_passed_constructor(self):
        passed = ConformanceCheckResult(
            ConformanceProfile.SPI_V2_CORE,
            "legacy-pass",
            passed=True,
        )
        failed = ConformanceCheckResult(
            ConformanceProfile.SPI_V2_CORE,
            "legacy-failure",
            passed=False,
        )

        self.assertIs(passed.status, ConformanceStatus.PASSED)
        self.assertIs(failed.status, ConformanceStatus.FAILED)
        self.assertEqual(failed.detail, "legacy conformance check failed")

    def test_check_result_rejects_contradictory_and_invalid_shapes(self):
        valid = {
            "profile": ConformanceProfile.SPI_V2_CORE,
            "name": "check",
            "status": ConformanceStatus.PASSED,
            "capability": "core",
        }
        cases = (
            (lambda: ConformanceCheckResult(valid["profile"], "check"), TypeError),
            (
                lambda: ConformanceCheckResult(
                    valid["profile"],
                    "check",
                    passed=False,
                    status=ConformanceStatus.PASSED,
                ),
                ValueError,
            ),
            (lambda: ConformanceCheckResult(**{**valid, "profile": "core"}), TypeError),
            (lambda: ConformanceCheckResult(**{**valid, "name": ""}), ValueError),
            (
                lambda: ConformanceCheckResult(**{**valid, "status": "passed"}),
                TypeError,
            ),
            (lambda: ConformanceCheckResult(**{**valid, "capability": ""}), ValueError),
            (
                lambda: ConformanceCheckResult(**{**valid, "phase": "contract"}),
                TypeError,
            ),
            (
                lambda: ConformanceCheckResult(**{**valid, "duration_ms": -1}),
                ValueError,
            ),
            (lambda: ConformanceCheckResult(**{**valid, "detail": ""}), ValueError),
            (lambda: ConformanceCheckResult(**{**valid, "evidence": []}), TypeError),
            (
                lambda: ConformanceCheckResult(**{**valid, "cleanup_error": ""}),
                ValueError,
            ),
            (lambda: ConformanceCheckResult(**{**valid, "check_id": 1}), ValueError),
            (
                lambda: ConformanceCheckResult(**{**valid, "detail": "failure"}),
                ValueError,
            ),
            (
                lambda: ConformanceCheckResult(
                    **{
                        **valid,
                        "status": ConformanceStatus.ERROR,
                    },
                ),
                ValueError,
            ),
        )
        for factory, error in cases:
            with self.subTest(factory=factory), self.assertRaises(error):
                factory()

    def test_report_rejects_invalid_identity_schema_checks_and_duplicates(self):
        check = ConformanceCheckResult(
            ConformanceProfile.SPI_V2_CORE,
            "check",
            status=ConformanceStatus.PASSED,
            capability="core",
            phase=ConformancePhase.CONTRACT,
        )
        cases = (
            lambda: ConformanceReport("", "spi-v2", (check,)),
            lambda: ConformanceReport("engine", "", (check,)),
            lambda: ConformanceReport(
                "engine",
                "spi-v2",
                (check,),
                schema_version="future",
            ),
            lambda: ConformanceReport("engine", "spi-v2", [check]),
            lambda: ConformanceReport("engine", "spi-v2", (check, check)),
        )
        for factory in cases:
            with (
                self.subTest(factory=factory),
                self.assertRaises((TypeError, ValueError)),
            ):
                factory()

    async def test_report_keeps_cleanup_failures_when_the_check_also_fails(self):
        async def cleanup(_engine, _db_name):
            msg = "cleanup failed"
            raise RuntimeError(msg)

        report = await run_engine_conformance(
            EngineConformanceProvider("legacy", object, cleanup=cleanup),
            profiles=(ConformanceProfile.SPI_V2_CORE,),
        )

        self.assertTrue(
            any(
                check.cleanup_error is not None
                and "cleanup failed" in check.cleanup_error
                for check in report.failures
            ),
        )

    async def test_report_attributes_cleanup_failure_to_successful_check(self):
        async def cleanup(_engine, _db_name):
            msg = "cleanup failed"
            raise RuntimeError(msg)

        report = await run_engine_conformance(
            EngineConformanceProvider("memory", MemoryEngine, cleanup=cleanup),
            profiles=(ConformanceProfile.SPI_V2_CORE,),
        )

        self.assertTrue(report.failures)
        self.assertTrue(
            all(
                not check.name.endswith(":cleanup")
                and "cleanup RuntimeError" in (check.detail or "")
                for check in report.failures
            ),
        )

    async def test_core_checks_report_each_invalid_outcome_boundary(self):
        for fault, expected in (
            ("insert", "InsertOutcome"),
            ("update", "MutationOutcome"),
            ("merge", "MergeOutcome"),
            ("delete", "DeleteOutcome"),
        ):
            with (
                self.subTest(fault=fault),
                self.assertRaisesRegex(AssertionError, expected),
            ):
                await _check_crud_outcomes(
                    _FaultyOutcomeEngine(fault),
                    "db",
                    "items",
                )

        with self.assertRaisesRegex(AssertionError, "OperationContext"):
            await _check_operation_context(
                _FaultyOutcomeEngine("context"),
                "db",
                "items",
            )

        with self.assertRaisesRegex(AssertionError, "exactly one winner"):
            await _check_atomic_mutation(
                _FaultyOutcomeEngine("atomicity"),
                "db",
                "items",
            )

        with self.assertRaisesRegex(AssertionError, "MutationOutcome"):
            await _check_atomic_mutation(
                _FaultyOutcomeEngine("update"),
                "db",
                "items",
            )

        divergent_state = _FaultyOutcomeEngine("atomicity")
        attempts = 0

        async def one_winner(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            matched = int(attempts == 1)
            return MutationOutcome(
                result=UpdateResult(matched, matched),
            )

        divergent_state.update_with_operation = one_winner
        with self.assertRaisesRegex(AssertionError, "winning mutation"):
            await _check_atomic_mutation(
                divergent_state,
                "db",
                "items",
            )

        with self.assertRaisesRegex(AssertionError, "clock update"):
            await _check_injected_clock(
                _FaultyOutcomeEngine("update"),
                "db",
                "items",
            )

        with self.assertRaisesRegex(AssertionError, "OperationContext clock"):
            await _check_injected_clock(
                _FaultyOutcomeEngine("clock"),
                "db",
                "items",
            )

    async def test_snapshot_check_rejects_type_policy_and_content_drift(self):
        with self.assertRaisesRegex(AssertionError, "ReadSnapshot"):
            await _check_snapshot(
                _SnapshotEngine(object()),
                "db",
                "items",
            )

        live_snapshot = ReadSnapshot(
            _Documents([{"_id": "snapshot"}]),
            policy=SnapshotPolicy.STABLE,
        )
        object.__setattr__(
            live_snapshot.metadata,
            "policy",
            SnapshotPolicy.LIVE,
        )
        with self.assertRaisesRegex(AssertionError, "STABLE"):
            await _check_snapshot(
                _SnapshotEngine(live_snapshot),
                "db",
                "items",
            )

        wrong_documents = ReadSnapshot(
            _Documents([{"_id": "other"}]),
            policy=SnapshotPolicy.STABLE,
        )
        with self.assertRaisesRegex(AssertionError, "owned, stable"):
            await _check_snapshot(
                _SnapshotEngine(wrong_documents),
                "db",
                "items",
            )

        with self.assertRaisesRegex(AssertionError, "owned, stable"):
            await _check_snapshot(
                _LiveSnapshotEngine(),
                "db",
                "items",
            )

        with self.assertRaisesRegex(AssertionError, "mutable engine-owned"):
            await _check_snapshot(
                _LiveSnapshotEngine(mutable_items=True),
                "db",
                "items",
            )

    async def test_optional_profiles_reject_divergent_claims_and_results(self):
        with self.assertRaisesRegex(AssertionError, "sequenced mode"):
            await _check_change_delivery(object(), "db", "items")

        engine = MemoryEngine()

        async def divergent_search(*_args, **_kwargs):
            return SearchExecutionOutcome.from_documents(
                [{"_id": "other"}],
                backend="test",
            )

        engine.execute_search = divergent_search
        with self.assertRaisesRegex(AssertionError, "matching document"):
            await _check_search(engine, "db", "items")

    async def test_change_delivery_check_rejects_each_delivery_violation(self):
        async def exercise(*, checkpoint, dispatch, outcome):
            engine = MemoryEngine()
            await engine.connect()
            try:
                with (
                    patch.object(
                        engine,
                        "register_change_consumer",
                        return_value=checkpoint,
                    ),
                    patch.object(engine, "insert_document", return_value=outcome),
                    patch.object(
                        engine, "dispatch_committed_changes", side_effect=dispatch
                    ),
                    patch.object(engine, "unregister_change_consumer"),
                ):
                    await _check_change_delivery(engine, "db", "items")
            finally:
                await engine.disconnect()

        missing_sequence = InsertOutcome(applied=True, document={"_id": "event"})
        with self.assertRaisesRegex(AssertionError, "commit_sequence"):
            await exercise(
                checkpoint=0,
                dispatch=lambda _consumer, _callback: None,
                outcome=missing_sequence,
            )

        sequenced = InsertOutcome(
            applied=True,
            document={"_id": "event"},
            commit_sequence=1,
        )
        with self.assertRaisesRegex(AssertionError, "monotonic and complete"):
            await exercise(
                checkpoint=0,
                dispatch=lambda _consumer, _callback: None,
                outcome=sequenced,
            )

        replay_calls = 0

        def replay(_consumer, callback):
            nonlocal replay_calls
            replay_calls += 1
            callback(SimpleNamespace(sequence=1))
            callback(SimpleNamespace(sequence=2))

        with self.assertRaisesRegex(AssertionError, "must not replay"):
            await exercise(checkpoint=0, dispatch=replay, outcome=sequenced)

        def duplicate(_consumer, callback):
            callback(SimpleNamespace(sequence=1))
            callback(SimpleNamespace(sequence=1))

        with self.assertRaisesRegex(AssertionError, "monotonic and complete"):
            await exercise(checkpoint=0, dispatch=duplicate, outcome=sequenced)

        checkpoint_calls = 0

        def stale_checkpoint(_consumer, callback):
            nonlocal checkpoint_calls
            checkpoint_calls += 1
            if checkpoint_calls == 1:
                callback(SimpleNamespace(sequence=1))
                callback(SimpleNamespace(sequence=2))

        with self.assertRaisesRegex(AssertionError, "checkpoint must precede"):
            await exercise(
                checkpoint=2,
                dispatch=stale_checkpoint,
                outcome=sequenced,
            )

    async def test_change_delivery_recovery_rejects_each_retry_violation(self):
        async def exercise(*, insert_outcome, dispatch):
            engine = MemoryEngine()
            await engine.connect()
            try:
                with (
                    patch.object(
                        engine,
                        "insert_document",
                        **(
                            {"side_effect": insert_outcome}
                            if isinstance(insert_outcome, list)
                            else {"return_value": insert_outcome}
                        ),
                    ),
                    patch.object(
                        engine,
                        "dispatch_committed_changes",
                        side_effect=dispatch,
                    ),
                ):
                    await _check_change_delivery_recovery(engine, "db", "items")
            finally:
                await engine.disconnect()

        missing_sequence = InsertOutcome(applied=True, document={"_id": "event"})
        with self.assertRaisesRegex(AssertionError, "commit_sequence"):
            await exercise(
                insert_outcome=missing_sequence,
                dispatch=lambda _consumer, _callback: None,
            )

        sequenced = InsertOutcome(
            applied=True,
            document={"_id": "event"},
            commit_sequence=1,
        )
        sequenced_pair = [
            sequenced,
            InsertOutcome(
                applied=True,
                document={"_id": "event-2"},
                commit_sequence=2,
            ),
        ]

        def swallow_failure(_consumer, callback):
            with suppress(RuntimeError):
                callback(CommittedChange(1, {"_id": "event"}))
                callback(CommittedChange(2, {"_id": "event-2"}))

        with self.assertRaisesRegex(AssertionError, "propagate"):
            await exercise(insert_outcome=sequenced_pair, dispatch=swallow_failure)

        dispatch_calls = 0

        def lose_failed_delivery(_consumer, callback):
            nonlocal dispatch_calls
            dispatch_calls += 1
            if dispatch_calls == 1:
                callback(CommittedChange(1, {"_id": "event"}))
                callback(CommittedChange(2, {"_id": "event-2"}))

        with self.assertRaisesRegex(AssertionError, "partial delivery"):
            await exercise(
                insert_outcome=sequenced_pair,
                dispatch=lose_failed_delivery,
            )

    async def test_search_check_rejects_each_declared_capability_violation(self):
        invalid_metadata = (
            SearchExecutionOutcome(),
            SearchExecutionOutcome(
                metadata=SearchMetadata(
                    count=SearchCountResult(mode="total", value=2, exact=True),
                ),
            ),
            SearchExecutionOutcome(
                metadata=SearchMetadata(
                    count=SearchCountResult(mode="total", value=1, exact=True),
                ),
            ),
        )
        expected_messages = (
            "must not return hits",
            "count must preserve",
            "facet collectors",
        )
        for outcome, expected in zip(
            invalid_metadata,
            expected_messages,
            strict=True,
        ):
            engine = MemoryEngine()

            async def execute_search(_db, _coll, request, *, invalid=outcome):
                if request.mode is SearchExecutionMode.METADATA:
                    return invalid
                return SearchExecutionOutcome.from_documents(
                    [{"_id": "search"}],
                    backend="test",
                )

            engine.execute_search = execute_search
            with (
                self.subTest(case=expected),
                self.assertRaisesRegex(
                    AssertionError,
                    expected,
                ),
            ):
                await _check_search(engine, "db", "items")

        engine = MemoryEngine()

        async def no_highlight(_db, _coll, request):
            if request.mode is SearchExecutionMode.METADATA:
                return await MemoryEngine.execute_search(engine, _db, _coll, request)
            return SearchExecutionOutcome.from_documents(
                [{"_id": "search"}],
                backend="test",
            )

        engine.execute_search = no_highlight
        with self.assertRaisesRegex(AssertionError, "highlight"):
            await _check_search(engine, "db", "items")

    async def test_search_check_rejects_invalid_explain_evidence(self):
        for invalid_verbosity, expected in (
            (SearchExplainVerbosity.QUERY_PLANNER, "queryPlanner"),
            (SearchExplainVerbosity.EXECUTION_STATS, "runtime evidence"),
        ):
            engine = MemoryEngine()
            original_explain = engine.explain_search

            async def invalid_explain(
                db,
                coll,
                request,
                verbosity,
                *,
                _original_explain=original_explain,
                _invalid_verbosity=invalid_verbosity,
            ):
                explanation = await _original_explain(db, coll, request, verbosity)
                details = dict(explanation.details or {})
                details["executionStats"] = (
                    {}
                    if _invalid_verbosity is SearchExplainVerbosity.QUERY_PLANNER
                    else None
                )
                if verbosity is _invalid_verbosity:
                    return replace(explanation, details=details)
                return explanation

            engine.explain_search = invalid_explain
            with (
                self.subTest(verbosity=invalid_verbosity),
                self.assertRaisesRegex(
                    AssertionError,
                    expected,
                ),
            ):
                await _check_search(engine, "db", "items")


if __name__ == "__main__":
    unittest.main()
