import decimal
import math
import unittest
from unittest.mock import patch

from mongoeco.errors import OperationFailure
from mongoeco.types import Decimal128
from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    BsonInt32,
    BsonInt64,
    BsonScalarOverflowError,
    bson_add,
    bson_bitwise,
    bson_divide,
    bson_mod,
    bson_multiply,
    bson_numeric_alias,
    bson_rewrap_numeric,
    bson_subtract,
    compare_bson_numeric,
    is_bson_numeric,
    _coerce_integral,
    _numeric_template_metadata,
    _numeric_to_decimal,
    _try_fast_float_arithmetic,
    _wrap_from_templates,
    _wrap_numeric_result,
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

    def test_direct_bson_integer_wrappers_validate_range_and_type(self):
        with self.assertRaises(BsonScalarOverflowError):
            BsonInt32((1 << 31))
        with self.assertRaises(BsonScalarOverflowError):
            BsonInt64(1 << 80)
        with self.assertRaises(TypeError):
            BsonInt32(True)
        with self.assertRaises(TypeError):
            BsonInt64("bad")  # type: ignore[arg-type]

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

    def test_bson_scalars_cover_decimal128_float_and_wrapper_helpers(self):
        wrapped_decimal128 = wrap_bson_numeric(Decimal128("10.25"))
        self.assertEqual(wrapped_decimal128, BsonDecimal128(decimal.Decimal("10.25")))
        self.assertEqual(_numeric_to_decimal(Decimal128("3.5")), decimal.Decimal("3.5"))
        self.assertEqual(compare_bson_numeric(1.5, 1.5), 0)
        self.assertEqual(compare_bson_numeric(1.5, 2.5), -1)
        self.assertEqual(compare_bson_numeric(2.5, 1.5), 1)
        self.assertEqual(compare_bson_numeric(Decimal128("2.50"), decimal.Decimal("2.5")), 0)
        self.assertEqual(compare_bson_numeric(Decimal128("2.75"), decimal.Decimal("2.5")), 1)
        self.assertEqual(compare_bson_numeric(float("inf"), float("inf")), 0)
        self.assertEqual(compare_bson_numeric(1.0, float("inf")), -1)
        self.assertEqual(compare_bson_numeric(1.0, float("-inf")), 1)
        self.assertEqual(compare_bson_numeric(decimal.Decimal("2.5"), decimal.Decimal("2.5")), 0)

    def test_bson_arithmetic_overflow_divide_and_mod_edge_cases(self):
        with self.assertRaises(BsonScalarOverflowError):
            bson_add((1 << 63) - 1, 1)
        with self.assertRaises(BsonScalarOverflowError):
            bson_multiply(1 << 62, 4)
        with self.assertRaises(BsonScalarOverflowError):
            bson_subtract(-(1 << 63), 1)
        with self.assertRaises(TypeError):
            bson_divide("x", 2)
        with self.assertRaises(TypeError):
            bson_mod("x", 2)

        self.assertEqual(bson_divide(BsonDecimal128(decimal.Decimal("5")), 2), BsonDecimal128(decimal.Decimal("2.5")))
        self.assertEqual(bson_divide(BsonDouble(5.0), 2), BsonDouble(2.5))
        self.assertEqual(bson_mod(BsonDecimal128(decimal.Decimal("5.5")), 2), BsonDecimal128(decimal.Decimal("1.5")))
        self.assertEqual(bson_mod(BsonInt32(-7), BsonInt32(3)), BsonInt32(-1))
        self.assertTrue(math.isnan(bson_mod(float("inf"), 3.0)))
        self.assertTrue(math.isnan(bson_mod(3.0, float("inf"))))

    def test_bson_divide_and_mod_reject_zero_divisors(self):
        with self.assertRaisesRegex(OperationFailure, "divide by zero"):
            bson_divide(5, 0)
        with self.assertRaisesRegex(OperationFailure, "divide by zero"):
            bson_divide(BsonDecimal128(decimal.Decimal("5")), decimal.Decimal("0"))
        with self.assertRaisesRegex(OperationFailure, "divide by zero"):
            bson_mod(5, 0)
        with self.assertRaisesRegex(OperationFailure, "divide by zero"):
            bson_mod(BsonDecimal128(decimal.Decimal("5.5")), decimal.Decimal("0"))

    def test_bson_bitwise_and_integral_helpers_cover_wrapped_and_error_paths(self):
        self.assertEqual(bson_bitwise("and", 0b1100, 0b1010), 0b1000)
        self.assertEqual(bson_bitwise("or", BsonInt32(0b1100), 0b0011), BsonInt32(0b1111))
        self.assertEqual(bson_bitwise("xor", BsonInt64(0b1100), BsonInt32(0b1010)), BsonInt32(0b0110))
        self.assertEqual(bson_bitwise("xor", BsonInt64(1 << 40), 1), BsonInt64((1 << 40) ^ 1))
        with self.assertRaises(ValueError):
            bson_bitwise("nope", 1, 1)
        with self.assertRaises(BsonScalarOverflowError):
            _coerce_integral(1 << 80)

    def test_bson_internal_wrapping_helpers_cover_template_and_result_paths(self):
        self.assertEqual(_try_fast_float_arithmetic(1, 2, "add"), None)
        self.assertEqual(_try_fast_float_arithmetic(Decimal128("1.5"), 2.0, "add"), None)
        self.assertEqual(_try_fast_float_arithmetic(BsonDouble(1.5), 2, "add"), BsonDouble(3.5))
        self.assertEqual(_try_fast_float_arithmetic(1.5, 2, "multiply"), 3.0)
        with self.assertRaises(ValueError):
            _try_fast_float_arithmetic(1.5, 2, "divide")
        with self.assertRaises(TypeError):
            _try_fast_float_arithmetic("x", 2, "add")

        self.assertEqual(_numeric_template_metadata(BsonDecimal128(decimal.Decimal("1.5"))), ("decimal", True))
        self.assertEqual(_numeric_template_metadata(BsonDouble(1.5)), ("double", True))
        self.assertEqual(_numeric_template_metadata(BsonInt64(1 << 40)), ("long", True))
        self.assertEqual(_numeric_template_metadata(BsonInt32(1)), ("int", True))
        self.assertEqual(_numeric_template_metadata(Decimal128("1.5")), ("decimal", False))
        self.assertEqual(_numeric_template_metadata(1.5), ("double", False))
        self.assertEqual(_numeric_template_metadata(1), ("int", False))
        self.assertEqual(_numeric_template_metadata(1 << 40), ("long", False))
        self.assertEqual(_numeric_template_metadata(True), (None, False))
        with self.assertRaises(BsonScalarOverflowError):
            _numeric_template_metadata(1 << 80)

        self.assertEqual(_wrap_numeric_result(Decimal128("1.5"), 2, decimal.Decimal("3.5")), decimal.Decimal("3.5"))
        self.assertEqual(_wrap_numeric_result(BsonDouble(1.5), 2, decimal.Decimal("3.5")), BsonDouble(3.5))
        self.assertEqual(_wrap_numeric_result(BsonInt32(2), 2, decimal.Decimal("3")), BsonInt32(3))
        self.assertEqual(_wrap_numeric_result(BsonInt64(1 << 40), 2, decimal.Decimal(str(1 << 40))), BsonInt64(1 << 40))
        self.assertEqual(_wrap_numeric_result(1, 2, decimal.Decimal("3")), 3)
        self.assertEqual(_wrap_numeric_result(BsonInt32(2), 2, decimal.Decimal("3.5")), BsonDouble(3.5))
        with self.assertRaises(TypeError):
            _wrap_numeric_result("x", 1, decimal.Decimal("1"))
        with self.assertRaises(BsonScalarOverflowError):
            _wrap_numeric_result(BsonInt64(1 << 40), 2, decimal.Decimal(1 << 80))

        self.assertEqual(_wrap_from_templates(3, 1, 2), 3)
        self.assertEqual(_wrap_from_templates(2, BsonDecimal128(decimal.Decimal("1"))), BsonDecimal128(decimal.Decimal("2")))
        self.assertEqual(_wrap_from_templates(2, BsonDouble(1.0)), BsonDouble(2.0))
        self.assertEqual(_wrap_from_templates(decimal.Decimal("2.5"), BsonInt32(1)), BsonDouble(2.5))
        self.assertEqual(_wrap_from_templates(2.0, BsonInt64(1 << 40)), BsonInt64(2))
        self.assertEqual(bson_rewrap_numeric(1.5, BsonInt32(1)), BsonDouble(1.5))
        self.assertEqual(bson_rewrap_numeric(5, BsonInt64(1 << 40)), BsonInt64(5))
        with self.assertRaises(BsonScalarOverflowError):
            _wrap_from_templates(1 << 80, BsonInt64(1 << 40))
        with self.assertRaises(TypeError):
            with patch("mongoeco.core.bson_scalars.wrap_bson_numeric", return_value=BsonDouble(1.5)):
                bson_bitwise("and", BsonInt32(3), 1)

    def test_bson_multiply_decimal_path_uses_wrapped_result(self):
        result = bson_multiply(BsonInt32(2), Decimal128("1.5"))
        self.assertEqual(result, BsonDecimal128(decimal.Decimal("3.0")))
