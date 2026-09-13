from __future__ import annotations

import tempfile

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from mongoeco.core.aggregation.compiled_pipeline import compile_pipeline
from mongoeco.core.aggregation.grouping_stages import _apply_group, _IncrementalGroup
from mongoeco.core.aggregation.runtime import AggregationStageContext
from mongoeco.core.aggregation.spill import (
    AggregationSpillPolicy,
    _AggregationGroupStateSerializationError,
)
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

    def test_sort_spool_accepts_bounded_batches_and_owns_partial_output(self):
        policy = AggregationSpillPolicy(threshold=3)
        spool = policy.open_sort_spool([("rank", 1)])
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-aggsort"))

        for start in range(0, 12, 2):
            spool.add(
                {"_id": index, "rank": 11 - index} for index in range(start, start + 2)
            )
            self.assertLessEqual(spool.buffered_documents, policy.threshold)

        self.assertGreater(spool.run_count, 0)
        output = spool.finish()
        self.assertEqual(next(output)["rank"], 0)
        self.assertGreater(len(set(temp_root.glob("*.mongoeco-aggsort"))), len(before))

        output.close()

        self.assertEqual(set(temp_root.glob("*.mongoeco-aggsort")), before)

    def test_group_spool_recursively_bounds_partition_key_sets(self):
        policy = AggregationSpillPolicy(threshold=2)
        accumulator = _IncrementalGroup({"_id": "$group", "count": {"$sum": 1}})
        for position in range(67):
            accumulator.consume_document(
                {"_id": position, "group": position},
                position=position,
            )
        spool = policy.open_group_spool(
            group_id_for_document=accumulator.group_id,
            group_key_for_id=accumulator.group_key,
        )
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-agggroup"))
        spool.seed(accumulator.release_buckets(), next_sequence=67)
        spool.add({"_id": index, "group": index % 67} for index in range(67, 67 * 3))

        seen = []
        partitions = spool.iter_partitions()
        for partition in partitions:
            records = list(partition)
            keys = {
                record[1] if record[0] == "state" else accumulator.group_key(record[2])
                for record in records
            }
            self.assertLessEqual(
                len(keys),
                policy.threshold,
            )
            seen.extend(records)

        self.assertEqual(len(seen), 67 * 3)
        self.assertEqual(
            sorted(record[3] if record[0] == "state" else record[1] for record in seen),
            list(range(67 * 3)),
        )
        self.assertEqual(set(temp_root.glob("*.mongoeco-agggroup")), before)

    def test_group_spool_cleans_all_partitions_after_partial_consumption(self):
        policy = AggregationSpillPolicy(threshold=1)
        accumulator = _IncrementalGroup({"_id": "$group", "count": {"$sum": 1}})
        for position in range(40):
            accumulator.consume_document(
                {"_id": position, "group": position},
                position=position,
            )
        spool = policy.open_group_spool(
            group_id_for_document=accumulator.group_id,
            group_key_for_id=accumulator.group_key,
        )
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-agggroup"))
        spool.seed(accumulator.release_buckets(), next_sequence=40)
        partitions = spool.iter_partitions()

        first_partition = next(partitions)
        next(first_partition)
        self.assertGreater(
            len(set(temp_root.glob("*.mongoeco-agggroup"))),
            len(before),
        )
        partitions.close()

        self.assertEqual(set(temp_root.glob("*.mongoeco-agggroup")), before)

    def test_group_spool_reports_unserializable_private_state_without_leaking(self):
        policy = AggregationSpillPolicy(threshold=1)
        accumulator = _IncrementalGroup(
            {"_id": "$group", "first": {"$first": "$value"}}
        )
        accumulator.consume(
            [
                {"group": 1, "value": lambda: None},
                {"group": 2, "value": lambda: None},
            ]
        )
        spool = policy.open_group_spool(
            group_id_for_document=accumulator.group_id,
            group_key_for_id=accumulator.group_key,
        )
        temp_root = Path(tempfile.gettempdir())
        before = set(temp_root.glob("*.mongoeco-agggroup"))

        with self.assertRaises(_AggregationGroupStateSerializationError):
            spool.seed(accumulator.release_buckets(), next_sequence=2)

        self.assertEqual(set(temp_root.glob("*.mongoeco-agggroup")), before)
