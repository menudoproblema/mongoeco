import datetime
import decimal
import unittest

from mongoeco.core.aggregation.runtime import (
    _MISSING,
    _aggregation_key,
    _bson_cstring_size,
    _evaluate_expression_with_missing,
    evaluate_expression,
)
from mongoeco.core.search import TEXT_SCORE_FIELD, VECTOR_SEARCH_SCORE_FIELD
from mongoeco.core.aggregation.stages import apply_pipeline
from mongoeco.errors import OperationFailure
from mongoeco.types import Timestamp


class AggregationRuntimeTests(unittest.TestCase):
    def test_aggregation_runtime_helpers_cover_runtime_key_bson_and_missing_expression_paths(self):
        self.assertEqual(_aggregation_key(1 << 40), ("long", 1 << 40))
        self.assertEqual(_aggregation_key(1.5), ("float", 1.5))
        self.assertEqual(_aggregation_key(decimal.Decimal("1.5")), ("decimal", decimal.Decimal("1.5")))
        self.assertEqual(_aggregation_key(Timestamp(1, 2)), ("timestamp", Timestamp(1, 2)))
        self.assertIs(_aggregation_key(object())[0], object)
        with self.assertRaisesRegex(OperationFailure, "NUL bytes"):
            _bson_cstring_size("a\x00b")
        self.assertEqual(evaluate_expression({TEXT_SCORE_FIELD: 1.5}, {"$meta": "textScore"}), 1.5)
        self.assertEqual(evaluate_expression({VECTOR_SEARCH_SCORE_FIELD: 2.5}, {"$meta": "vectorSearchScore"}), 2.5)
        self.assertEqual(evaluate_expression({"name": "Ada"}, {"present": "$name", "missing": "$unknown"}), {"present": "Ada", "missing": None})
        self.assertIs(
            _evaluate_expression_with_missing({"items": []}, "$items.0.name", None),
            _MISSING,
        )
        self.assertEqual(
            evaluate_expression(
                {"name": "Ada"},
                {"kept": "$name", "dropped": "$missing.value"},
            ),
            {"kept": "Ada", "dropped": None},
        )
        self.assertEqual(evaluate_expression({}, {"kept": 1, "dropped": _MISSING}), {"kept": 1})
        with self.assertRaisesRegex(OperationFailure, "only supports 'textScore' or 'vectorSearchScore'"):
            evaluate_expression({}, {"$meta": "bad"})

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

    def test_transform_stage_helpers_cover_missing_add_fields_and_replace_root_validation(self):
        self.assertEqual(
            apply_pipeline(
                [{"_id": 1, "name": "Ada"}],
                [{"$addFields": {"profile.city": _MISSING}}],
            ),
            [{"_id": 1, "name": "Ada"}],
        )
        with self.assertRaisesRegex(OperationFailure, "newRoot must evaluate to a document"):
            apply_pipeline(
                [{"_id": 1, "name": "Ada"}],
                [{"$replaceRoot": {"newRoot": "$missing"}}],
            )
