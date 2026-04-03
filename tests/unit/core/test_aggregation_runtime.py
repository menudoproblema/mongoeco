import datetime
import unittest

from mongoeco.core.aggregation.runtime import (
    _MISSING,
    _aggregation_key,
    _bson_cstring_size,
    _evaluate_expression_with_missing,
    evaluate_expression,
)
from mongoeco.core.aggregation.stages import apply_pipeline
from mongoeco.errors import OperationFailure
from mongoeco.types import Timestamp


class AggregationRuntimeTests(unittest.TestCase):
    def test_aggregation_runtime_helpers_cover_runtime_key_bson_and_missing_expression_paths(self):
        self.assertEqual(_aggregation_key(1 << 40), ("long", 1 << 40))
        self.assertEqual(_aggregation_key(1.5), ("float", 1.5))
        self.assertEqual(_aggregation_key(Timestamp(1, 2)), ("timestamp", Timestamp(1, 2)))
        with self.assertRaisesRegex(OperationFailure, "NUL bytes"):
            _bson_cstring_size("a\x00b")
        self.assertEqual(evaluate_expression({"name": "Ada"}, {"present": "$name", "missing": "$unknown"}), {"present": "Ada", "missing": None})
        self.assertIs(
            _evaluate_expression_with_missing({"items": []}, "$items.0.name", None),
            _MISSING,
        )

    def test_densify_stage_handles_empty_input_and_datetime_units(self):
        self.assertEqual(
            apply_pipeline(
                [],
                [
                    {
                        "$densify": {
                            "field": "ts",
                            "range": {"step": 1, "unit": "millisecond", "bounds": "full"},
                        }
                    }
                ],
            ),
            [],
        )

        base = datetime.datetime(2026, 4, 1, 12, 0, 0)
        result = apply_pipeline(
            [{"ts": base}, {"ts": base + datetime.timedelta(seconds=2)}],
            [
                {
                    "$densify": {
                        "field": "ts",
                        "range": {"step": 1, "unit": "second", "bounds": "full"},
                    }
                }
            ],
        )
        self.assertEqual(
            [doc["ts"] for doc in result],
            [base, base + datetime.timedelta(seconds=1), base + datetime.timedelta(seconds=2)],
        )
