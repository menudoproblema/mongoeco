import decimal
import math
import unittest

from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    BsonInt32,
    BsonInt64,
    BsonScalarOverflowError,
    bson_mod,
    bson_numeric_alias,
    compare_bson_numeric,
    is_bson_numeric,
    unwrap_bson_numeric,
    validate_bson_value,
    wrap_bson_numeric,
)


class BsonScalarTests(unittest.TestCase):
    def test_wrap_bson_numeric_distinguishes_numeric_kinds(self):
        self.assertEqual(wrap_bson_numeric(1), BsonInt32(1))
        self.assertEqual(wrap_bson_numeric(1 << 40), BsonInt64(1 << 40))
        self.assertEqual(wrap_bson_numeric(1.5), BsonDouble(1.5))
        self.assertEqual(
            wrap_bson_numeric(decimal.Decimal("10.25")),
            BsonDecimal128(decimal.Decimal("10.25")),
        )
        self.assertIsNone(wrap_bson_numeric(True))
        with self.assertRaises(BsonScalarOverflowError):
            wrap_bson_numeric(1 << 80)

    def test_bson_numeric_alias_and_unwrap_support_wrappers(self):
        wrapped = BsonInt64(1 << 40)
        self.assertEqual(bson_numeric_alias(wrapped), "long")
        self.assertEqual(unwrap_bson_numeric(wrapped), 1 << 40)
        self.assertTrue(is_bson_numeric(BsonDouble(1.5)))

    def test_compare_bson_numeric_handles_nan_and_infinities(self):
        self.assertEqual(compare_bson_numeric(float("nan"), float("nan")), 0)
        self.assertEqual(
            compare_bson_numeric(
                BsonDecimal128(decimal.Decimal("NaN")),
                BsonDecimal128(decimal.Decimal("NaN")),
            ),
            0,
        )
        self.assertLess(compare_bson_numeric(float("nan"), 1), 0)
        self.assertGreater(compare_bson_numeric(float("inf"), 1), 0)
        self.assertLess(compare_bson_numeric(float("-inf"), 1), 0)
        self.assertGreater(compare_bson_numeric(float("inf"), float("-inf")), 0)
        with self.assertRaises(TypeError):
            compare_bson_numeric("x", 1)

    def test_bson_mod_preserves_integer_precision_for_large_values(self):
        value = (1 << 62) + 5
        self.assertEqual(bson_mod(value, 3), value % 3)

    def test_validate_bson_value_walks_nested_documents_and_lists(self):
        validate_bson_value(
            {
                "items": [
                    {"value": BsonDecimal128(decimal.Decimal("1.25"))},
                    {"value": [BsonInt32(1), BsonInt64(2), BsonDouble(3.5)]},
                ]
            }
        )
