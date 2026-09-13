from __future__ import annotations

import tempfile

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from mongoeco.core.aggregation.compiled_pipeline import compile_pipeline
from mongoeco.core.aggregation.grouping_stages import _apply_group
from mongoeco.core.aggregation.runtime import AggregationStageContext
from mongoeco.core.aggregation.spill import AggregationSpillPolicy
from mongoeco.core.aggregation.stages import _stage_densify
from mongoeco.core.sorting import sort_documents
from mongoeco.core.work_control import DEADLINE_CHECK_INTERVAL, iter_with_deadline
from mongoeco.errors import OperationFailure


_DEADLINE = 123.0
_MAX_EXPECTED_MERGE_FAN_IN = 32


def _fail_on_check(check_number: int):
    checks = 0

    def _enforce(deadline: float | None) -> None:
        nonlocal checks
        if deadline != _DEADLINE:
            message = f"unexpected deadline: {deadline!r}"
            raise AssertionError(message)
        checks += 1
        if checks == check_number:
            message = "operation exceeded time limit"
            raise OperationFailure(message)

    return _enforce


class AggregationWorkControlTests(TestCase):
    def test_checkpointed_iterator_bounds_work_between_checks(self):
        values = list(range(DEADLINE_CHECK_INTERVAL + 1))

        with (
            patch(
                "mongoeco.core.work_control.enforce_deadline",
                side_effect=_fail_on_check(2),
            ),
            self.assertRaisesRegex(OperationFailure, "time limit"),
        ):
            list(iter_with_deadline(values, _DEADLINE))

    def test_group_checks_deadline_inside_document_loop(self):
        documents = [
            {"_id": index, "group": index}
            for index in range(DEADLINE_CHECK_INTERVAL + 1)
        ]

        with (
            patch(
                "mongoeco.core.work_control.enforce_deadline",
                side_effect=_fail_on_check(2),
            ),
            self.assertRaisesRegex(OperationFailure, "time limit"),
        ):
            _apply_group(
                documents,
                {"_id": "$group", "count": {"$sum": 1}},
                deadline=_DEADLINE,
            )

    def test_sort_checks_deadline_during_native_comparisons(self):
        documents = [{"_id": index, "rank": -index} for index in range(10)]

        with (
            patch(
                "mongoeco.core.work_control.enforce_deadline",
                side_effect=_fail_on_check(2),
            ),
            self.assertRaisesRegex(OperationFailure, "time limit"),
        ):
            sort_documents(
                documents,
                [("rank", 1)],
                deadline=_DEADLINE,
            )

    def test_compiled_stream_block_checks_deadline_after_materialization(self):
        plan = compile_pipeline([{"$project": {"value": 1}}])
        self.assertIsNotNone(plan)

        with (
            patch(
                "mongoeco.core.work_control.enforce_deadline",
                side_effect=_fail_on_check(2),
            ),
            self.assertRaisesRegex(OperationFailure, "time limit"),
        ):
            plan.execute(
                [{"_id": 1, "value": 1}, {"_id": 2, "value": 2}],
                deadline=_DEADLINE,
            )

    def test_densify_checks_deadline_while_expanding_output(self):
        context = AggregationStageContext(stage_index=0, deadline=_DEADLINE)

        with (
            patch(
                "mongoeco.core.work_control.enforce_deadline",
                side_effect=_fail_on_check(2),
            ),
            self.assertRaisesRegex(OperationFailure, "time limit"),
        ):
            _stage_densify(
                [{"value": 0}],
                {
                    "field": "value",
                    "range": {"step": 1, "bounds": [0, 10_000]},
                },
                context,
            )

    def test_spill_cleans_temporary_file_when_deadline_expires(self):
        policy = AggregationSpillPolicy(threshold=1)
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-aggspill"))

        with (
            patch(
                "mongoeco.core.work_control.enforce_deadline",
                side_effect=_fail_on_check(2),
            ),
            self.assertRaisesRegex(OperationFailure, "time limit"),
        ):
            policy.maybe_spill(
                "$group",
                [{"_id": index} for index in range(DEADLINE_CHECK_INTERVAL + 1)],
                deadline=_DEADLINE,
            )

        self.assertEqual(
            set(temp_root.glob("*.mongoeco-aggspill")),
            before,
        )

    def test_external_sort_bounds_open_runs_and_cleans_every_temporary(self):
        policy = AggregationSpillPolicy(threshold=1)
        documents = [
            {"_id": index, "rank": -index}
            for index in range(_MAX_EXPECTED_MERGE_FAN_IN * 2 + 3)
        ]
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-aggsort"))
        real_open = Path.open
        active = 0
        peak = 0

        class TrackedStream:
            def __init__(self, path, *args, **kwargs):
                nonlocal active, peak
                self._stream = real_open(path, *args, **kwargs)
                self._closed = False
                active += 1
                peak = max(peak, active)

            def readline(self):
                return self._stream.readline()

            def close(self):
                nonlocal active
                if not self._closed:
                    self._closed = True
                    active -= 1
                self._stream.close()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                self.close()

        def tracked_open(path, *args, **kwargs):
            return TrackedStream(path, *args, **kwargs)

        with patch.object(Path, "open", tracked_open):
            result = policy.sort_with_spill(documents, [("rank", 1)])

        self.assertEqual(
            [document["rank"] for document in result],
            sorted(document["rank"] for document in documents),
        )
        self.assertLessEqual(peak, _MAX_EXPECTED_MERGE_FAN_IN)
        self.assertEqual(active, 0)
        self.assertEqual(set(temp_root.glob("*.mongoeco-aggsort")), before)

    def test_external_sort_cleans_original_and_intermediate_runs_on_failure(self):
        policy = AggregationSpillPolicy(threshold=1)
        documents = [
            {"_id": index, "rank": -index}
            for index in range(_MAX_EXPECTED_MERGE_FAN_IN * 2 + 3)
        ]
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-aggsort"))
        original_decode = policy.codec.decode
        decoded = 0

        def decode_then_fail(payload, **kwargs):
            nonlocal decoded
            decoded += 1
            if decoded == _MAX_EXPECTED_MERGE_FAN_IN + 2:
                message = "decode failure"
                raise RuntimeError(message)
            return original_decode(payload, **kwargs)

        with (
            patch.object(policy.codec, "decode", side_effect=decode_then_fail),
            self.assertRaisesRegex(RuntimeError, "decode failure"),
        ):
            policy.sort_with_spill(documents, [("rank", 1)])

        self.assertEqual(set(temp_root.glob("*.mongoeco-aggsort")), before)

    def test_external_sort_stream_closes_temporaries_after_partial_consumption(self):
        policy = AggregationSpillPolicy(threshold=2)
        documents = ({"_id": index, "rank": -index} for index in range(10))
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-aggsort"))

        stream = policy.iter_sort_with_spill(documents, [("rank", 1)])
        self.assertEqual(next(stream)["rank"], -9)
        self.assertGreater(len(set(temp_root.glob("*.mongoeco-aggsort"))), len(before))
        stream.close()

        self.assertEqual(set(temp_root.glob("*.mongoeco-aggsort")), before)

    def test_external_sort_never_builds_a_run_above_its_threshold(self):
        policy = AggregationSpillPolicy(threshold=3)
        documents = ({"_id": index, "rank": -index} for index in range(17))

        with patch(
            "mongoeco.core.aggregation.spill.sort_documents",
            wraps=sort_documents,
        ) as sorter:
            result = list(policy.iter_sort_with_spill(documents, [("rank", 1)]))

        self.assertEqual(
            [document["rank"] for document in result],
            sorted(document["rank"] for document in result),
        )
        self.assertLessEqual(
            max(len(call.args[0]) for call in sorter.call_args_list),
            policy.threshold,
        )
