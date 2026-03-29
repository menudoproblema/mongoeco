import decimal
import math
import unittest

from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    BsonInt32,
    BsonInt64,
    BsonScalarOverflowError,
    bson_add,
    bson_bitwise,
    bson_mod,
    bson_multiply,
    bson_numeric_alias,
    bson_subtract,
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
        with self.assertRaises(BsonScalarOverflowError):
            bson_numeric_alias(1 << 80)

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
        self.assertEqual(bson_mod(-7, 3), -1)
        self.assertEqual(bson_mod(7, -3), 1)

    def test_compare_bson_numeric_handles_mixed_decimal_and_float_nan_infinities(self):
        self.assertEqual(compare_bson_numeric(BsonDecimal128(decimal.Decimal("NaN")), float("nan")), 0)
        self.assertGreater(compare_bson_numeric(BsonDecimal128(decimal.Decimal("Infinity")), 1.0), 0)
        self.assertLess(compare_bson_numeric(BsonDecimal128(decimal.Decimal("-Infinity")), 1.0), 0)

    def test_bson_float_arithmetic_preserves_double_promotion_and_wrappers(self):
        self.assertEqual(bson_add(1.5, 2), 3.5)
        self.assertEqual(bson_subtract(5.5, 2), 3.5)
        self.assertEqual(bson_multiply(1.5, 2), 3.0)
        self.assertEqual(bson_add(BsonInt32(2), 1.5), BsonDouble(3.5))

    def test_bson_bitwise_rejects_non_integral_operands(self):
        with self.assertRaises(TypeError):
            bson_bitwise("and", BsonInt32(1), 1.5)

    def test_validate_bson_value_walks_nested_documents_and_lists(self):
        validate_bson_value(
            {
                "items": [
                    {"value": BsonDecimal128(decimal.Decimal("1.25"))},
                    {"value": [BsonInt32(1), BsonInt64(2), BsonDouble(3.5)]},
                ]
            }
        )

    def test_validate_bson_value_handles_deep_nesting_iteratively(self):
        nested: object = BsonDecimal128(decimal.Decimal("2.5"))
        for _ in range(2000):
            nested = {"value": [nested]}

        validate_bson_value(nested)

    def test_decimal128_normalization_clamps_precision_to_34_significant_digits(self):
        value = decimal.Decimal("1." + "1" * 40)
        wrapped = wrap_bson_numeric(value)
        assert isinstance(wrapped, BsonDecimal128)
        self.assertEqual(len(wrapped.value.as_tuple().digits), 34)

    def test_decimal128_normalization_handles_extreme_exponents(self):
        very_large = decimal.Decimal("9.9E+6144")
        wrapped_large = wrap_bson_numeric(very_large)
        assert isinstance(wrapped_large, BsonDecimal128)
        self.assertGreater(wrapped_large.value, 0)

        very_small = decimal.Decimal("1E-6176")
        wrapped_small = wrap_bson_numeric(very_small)
        assert isinstance(wrapped_small, BsonDecimal128)
        self.assertGreater(wrapped_small.value, 0)

    def test_bson_mod_int_fast_path_matches_float_path_for_all_sign_combinations(self):
        cases = [(7, 3), (-7, 3), (7, -3), (-7, -3)]
        for left, right in cases:
            with self.subTest(left=left, right=right):
                int_result = bson_mod(left, right)
                float_result = bson_mod(float(left), float(right))
                self.assertEqual(int_result, float_result, msg=f"bson_mod({left}, {right})")
