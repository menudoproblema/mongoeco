import unittest
from types import SimpleNamespace

import mongoeco
from mongoeco.driver._runtime_attempts import RuntimeAttemptLifecycle
from mongoeco.driver import LocalCommandTransport
from mongoeco.core.aggregation.cost import AggregationCostPolicy
from mongoeco.core.update_paths import CompiledUpdateInstruction, compile_update_path
from mongoeco.core.update_scalar_operators import apply_bit, apply_rename
from mongoeco.error_catalog import DUPLICATE_KEY_ERROR, build_error_metadata
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession


class MiscCoverageTests(unittest.TestCase):
    def test_package_getattr_resolves_driver_exports(self):
        self.assertIs(mongoeco.__getattr__("LocalCommandTransport"), LocalCommandTransport)

    def test_package_getattr_rejects_unknown_export(self):
        with self.assertRaises(AttributeError):
            mongoeco.__getattr__("totally_missing_symbol")

    def test_aggregation_cost_policy_covers_validation_and_non_spill_error(self):
        with self.assertRaises(ValueError):
            AggregationCostPolicy(max_materialized_documents=0)

        policy = AggregationCostPolicy(
            max_materialized_documents=1,
            require_spill_for_blocking_stages=False,
        )
        with self.assertRaisesRegex(OperationFailure, "configured materialization budget"):
            policy.enforce_budget(
                document_count=2,
                has_materializing_stage=True,
                spill_available=False,
            )

    def test_aggregation_cost_policy_returns_early_when_budget_or_stage_do_not_require_failure(self):
        policy = AggregationCostPolicy(max_materialized_documents=2)
        self.assertIsNone(
            policy.enforce_budget(
                document_count=3,
                has_materializing_stage=False,
                spill_available=False,
            )
        )
        self.assertIsNone(
            policy.enforce_budget(
                document_count=2,
                has_materializing_stage=True,
                spill_available=False,
            )
        )

    def test_error_catalog_and_session_helpers_cover_remaining_label_branches(self):
        self.assertEqual(
            build_error_metadata(
                DUPLICATE_KEY_ERROR,
                error_labels=("RetryableWriteError",),
            ),
            (
                11000,
                "DuplicateKey",
                {"codeName": "DuplicateKey", "errorLabels": ["RetryableWriteError"]},
            ),
        )
        session = ClientSession()
        self.assertEqual(
            session._error_labels(type("E", (), {"error_labels": "bad"})()),
            (),
        )

    def test_runtime_attempt_lifecycle_rejects_empty_candidate_server_lists(self):
        class StubConnections:
            async def checkout_async(self, server):
                raise AssertionError(server)

        class StubMonitor:
            def emit(self, event):
                raise AssertionError(event)

        lifecycle = RuntimeAttemptLifecycle(
            connections=StubConnections(),
            monitor=StubMonitor(),
            resolve_plan=lambda plan: plan,
        )
        request = type("Request", (), {"database": "db", "command_name": "ping", "read_only": True, "session_id": None})()
        plan = type("Plan", (), {"candidate_servers": (), "request": request})()

        with self.assertRaisesRegex(RuntimeError, "no candidate servers available"):
            import asyncio

            asyncio.run(lifecycle.prepare(plan, attempt_number=1))

    def test_update_scalar_helper_branches_cover_bitwise_and_rename_guards(self):
        bit_instruction = CompiledUpdateInstruction(
            operator="$bit",
            path=compile_update_path("count"),
            value="bad",
        )
        rename_instruction = CompiledUpdateInstruction(
            operator="$rename",
            path=compile_update_path("name"),
            value="alias",
            target_path=None,
        )
        target = SimpleNamespace(concrete_path="count")
        helpers = SimpleNamespace(
            _resolve_instruction_applications=lambda *_args, **_kwargs: (
                SimpleNamespace(instruction=bit_instruction, targets=(target,)),
            ),
            _is_integral=lambda value: isinstance(value, int) and not isinstance(value, bool),
        )

        with self.assertRaisesRegex(OperationFailure, "document of bitwise operations"):
            apply_bit({"count": 1}, (bit_instruction,), context=SimpleNamespace(), helpers=helpers)
        with self.assertRaisesRegex(OperationFailure, "string target paths"):
            apply_rename({"name": "Ada"}, (rename_instruction,), context=SimpleNamespace(), helpers=SimpleNamespace())
