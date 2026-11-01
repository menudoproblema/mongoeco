import math
import unittest
from unittest.mock import patch

from mongoeco.core.aggregation.runtime import evaluate_expression
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary


class AggregationScalarExpressionTests(unittest.TestCase):
    def test_scalar_conversion_error_branches_cover_int_long_and_uuid_paths(self):
        with self.assertRaisesRegex(OperationFailure, "cannot convert the value"):
            evaluate_expression({"value": math.inf}, {"$toInt": "$value"})
        with self.assertRaisesRegex(OperationFailure, "cannot convert the value"):
            evaluate_expression({"value": math.inf}, {"$toLong": "$value"})
        with self.assertRaisesRegex(OperationFailure, "overflow"):
            evaluate_expression({"value": float(2**63)}, {"$toLong": "$value"})
        with self.assertRaisesRegex(OperationFailure, "overflow"):
            evaluate_expression({"value": str(1 << 80)}, {"$toLong": "$value"})

        class BrokenUUID:
            def __new__(cls, *args, **kwargs):
                raise ValueError("bad uuid")

        with patch("mongoeco.core.aggregation.scalar_expressions.uuid.UUID", BrokenUUID):
            with self.assertRaisesRegex(OperationFailure, "cannot convert the value"):
                evaluate_expression({"value": Binary(b"0123456789abcdef")}, {"$toUUID": "$value"})


class DatePartsBoundaryTests(unittest.TestCase):
    def test_null_date_parts_short_circuit_and_out_of_domain_dates_fail(self):
        for parts in (
            {"year": 2020, "timezone": None},
            {"year": 2020, "hour": None},
            {"year": None},
        ):
            with self.subTest(parts=parts):
                self.assertIsNone(
                    evaluate_expression(
                        {"parts": parts},
                        {"$dateFromParts": {key: "$parts." + key for key in parts}},
                    )
                )
        with self.assertRaisesRegex(OperationFailure, "invalid date"):
            evaluate_expression(
                {"day": 10**12}, {"$dateFromParts": {"year": 2020, "day": "$day"}}
            )
