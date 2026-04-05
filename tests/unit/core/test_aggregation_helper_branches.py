import datetime
import decimal
import math
import re
import unittest
import uuid

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.aggregation import evaluate_expression
from mongoeco.core.aggregation import accumulators as accumulators_module
from mongoeco.core.aggregation import array_string_expressions as array_module
from mongoeco.core.aggregation import compiled_aggregation as compiled_module
from mongoeco.core.aggregation import control_object_expressions as control_module
from mongoeco.core.aggregation import date_expressions as date_module
from mongoeco.core.aggregation import grouping_stages
from mongoeco.core.aggregation import join_stages
from mongoeco.core.aggregation import numeric_expressions as numeric_module
from mongoeco.core.aggregation import planning as planning_module
from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.errors import OperationFailure
from mongoeco.types import Decimal128, Regex, UNDEFINED


class ArrayStringHelperBranchTests(unittest.TestCase):
    def test_set_lookup_helpers_cover_hashable_and_residual_paths(self):
        values = [None, True, "name", b"bytes", uuid.UUID("12345678-1234-5678-1234-567812345678"), {"x": 1}]

        lookup, residual = array_module._build_set_lookup(values, dialect=MONGODB_DIALECT_70)

        self.assertIn(("none", None), lookup)
        self.assertIn(("bool", True), lookup)
        self.assertIn(("str", "name"), lookup)
        self.assertIn(("bytes", b"bytes"), lookup)
        self.assertEqual(len(residual), 1)
        self.assertTrue(array_module._set_contains("name", lookup=lookup, residual=residual, dialect=MONGODB_DIALECT_70))
        self.assertTrue(array_module._set_contains({"x": 1}, lookup=lookup, residual=residual, dialect=MONGODB_DIALECT_70))
        self.assertIsNone(array_module._hashable_set_lookup_key("name", dialect=MONGODB_DIALECT_80))

    def test_array_string_helpers_cover_slice_zip_regex_bounds_and_trim_paths(self):
        copied = array_module._copy_if_mutable({"nested": [1]})
        copied["nested"].append(2)
        self.assertEqual(copied, {"nested": [1, 2]})

        self.assertIsNone(evaluate_expression({"items": None}, {"$slice": ["$items", 2]}))
        self.assertEqual(evaluate_expression({"items": [1, 2, 3]}, {"$slice": ["$items", 0]}), [])
        self.assertEqual(evaluate_expression({"items": [1, 2, 3]}, {"$slice": ["$items", -2]}), [2, 3])
        with self.assertRaisesRegex(OperationFailure, "\\$slice count must be an integer"):
            evaluate_expression({"items": [1]}, {"$slice": ["$items", "x"]})
        with self.assertRaisesRegex(OperationFailure, "\\$slice position must be an integer"):
            evaluate_expression({"items": [1]}, {"$slice": ["$items", "x", 1]})
        with self.assertRaisesRegex(OperationFailure, "\\$slice count must be an integer"):
            evaluate_expression({"items": [1]}, {"$slice": ["$items", 0, "x"]})
        with self.assertRaisesRegex(OperationFailure, "\\$slice count must be a non-negative integer"):
            evaluate_expression({"items": [1]}, {"$slice": ["$items", 0, -1]})

        self.assertIsNone(evaluate_expression({"doc": None}, {"$objectToArray": "$doc"}))
        with self.assertRaisesRegex(OperationFailure, "\\$objectToArray requires a document input"):
            evaluate_expression({"doc": "x"}, {"$objectToArray": "$doc"})

        zipped = evaluate_expression(
            {"a": [1], "b": [2, 3]},
            {"$zip": {"inputs": ["$a", "$b"], "useLongestLength": True, "defaults": [0, 9]}},
        )
        self.assertEqual(zipped, [[1, 2], [0, 3]])
        self.assertEqual(
            evaluate_expression(
                {"a": [[1], [2, 3]]},
                {"$zip": {"inputs": "$a", "useLongestLength": True}},
            ),
            [[1, 2], [None, 3]],
        )
        self.assertIsNone(evaluate_expression({"a": [[1], None]}, {"$zip": {"inputs": "$a"}}))
        with self.assertRaisesRegex(OperationFailure, "\\$zip requires inputs"):
            evaluate_expression({}, {"$zip": {}})
        with self.assertRaisesRegex(OperationFailure, "\\$zip defaults must evaluate to an array"):
            evaluate_expression({"a": [[1], [2]]}, {"$zip": {"inputs": "$a", "defaults": "bad"}})
        with self.assertRaisesRegex(OperationFailure, "\\$zip defaults length must match inputs length"):
            evaluate_expression({"a": [[1], [2]]}, {"$zip": {"inputs": "$a", "defaults": [0]}})
        self.assertIsNone(evaluate_expression({"a": None}, {"$zip": {"inputs": "$a"}}))

        self.assertIsNone(evaluate_expression({"text": None}, {"$indexOfBytes": ["$text", "a"]}))
        self.assertEqual(evaluate_expression({"text": "abcd"}, {"$indexOfBytes": ["$text", "a", 5, 6]}), -1)
        self.assertEqual(evaluate_expression({"text": "abcd"}, {"$indexOfCP": ["$text", "a", 5, 6]}), -1)
        self.assertEqual(evaluate_expression({"text": "abcd"}, {"$indexOfBytes": ["$text", "a", 3, 1]}), -1)
        self.assertEqual(evaluate_expression({"text": "abcd"}, {"$indexOfCP": ["$text", "a", 3, 1]}), -1)
        with self.assertRaisesRegex(OperationFailure, "start must be an integer"):
            array_module._normalize_index_bounds("$indexOfBytes", "x", None, 5)
        with self.assertRaisesRegex(OperationFailure, "end must be an integer"):
            array_module._normalize_index_bounds("$indexOfBytes", 0, "x", 5)
        with self.assertRaisesRegex(OperationFailure, "start must be a non-negative integer"):
            array_module._normalize_index_bounds("$indexOfBytes", -1, None, 5)
        with self.assertRaisesRegex(OperationFailure, "end must be a non-negative integer"):
            array_module._normalize_index_bounds("$indexOfBytes", 0, -1, 5)

        self.assertFalse(evaluate_expression({"text": None}, {"$regexMatch": {"input": "$text", "regex": "a"}}))
        self.assertEqual(evaluate_expression({"text": None}, {"$regexFindAll": {"input": "$text", "regex": "a"}}), [])
        self.assertIsNone(evaluate_expression({"text": "Ada"}, {"$regexFind": {"input": "$text", "regex": "z"}}))
        with self.assertRaisesRegex(OperationFailure, "requires input and regex"):
            evaluate_expression({"text": "Ada"}, {"$regexFind": {"input": "$text"}})
        self.assertEqual(array_module._compile_aggregation_regex(Regex("^ada", "i"), None, operator="$regexMatch").flags & re.IGNORECASE, re.IGNORECASE)
        with self.assertRaisesRegex(OperationFailure, "cannot specify options in both regex and options"):
            array_module._compile_aggregation_regex(Regex("^ada", "i"), "m", operator="$regexMatch")
        with self.assertRaisesRegex(OperationFailure, "only support embedded i and m options"):
            array_module._compile_aggregation_regex(re.compile("a", re.DOTALL), None, operator="$regexMatch")
        compiled_pattern = array_module._compile_aggregation_regex(re.compile("a", re.IGNORECASE | re.MULTILINE), None, operator="$regexMatch")
        self.assertEqual(compiled_pattern.flags & re.IGNORECASE, re.IGNORECASE)
        self.assertEqual(compiled_pattern.flags & re.MULTILINE, re.MULTILINE)
        with self.assertRaisesRegex(OperationFailure, "Unsupported regex option"):
            array_module._compile_aggregation_regex("a", "q", operator="$regexMatch")

        self.assertEqual(array_module._substr_code_points("abcd", -1, 2), "")
        self.assertEqual(array_module._substr_code_points("abcd", 10, 2), "")
        self.assertEqual(array_module._trim_string("\x00  Ada \x00", None, mode="both"), "Ada")
        self.assertEqual(array_module._trim_string("--Ada--", "-", mode="left"), "Ada--")
        self.assertEqual(array_module._trim_string("--Ada--", "-", mode="right"), "--Ada")
        with self.assertRaisesRegex(OperationFailure, "Unsupported array/string expression operator"):
            array_module.evaluate_array_string_expression(
                "$unknown",
                {},
                [],
                None,
                dialect=MONGODB_DIALECT_70,
                evaluate_expression=lambda _doc, expr, _vars=None: expr,
                evaluate_expression_with_missing=lambda _doc, expr, _vars=None: expr,
                require_expression_args=lambda _op, spec, _mn, _mx: list(spec),
                missing_sentinel=object(),
            )


class DateHelperBranchTests(unittest.TestCase):
    def test_date_helper_branches_cover_formats_parts_timezones_and_units(self):
        self.assertEqual(
            date_module._format_timezone_offset(
                datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=-3, minutes=-30)))
            ),
            "-0330",
        )
        self.assertEqual(date_module._python_strptime_format("$dateFromString", "%Y-%m-%d %H:%M:%S.%L%z"), "%Y-%m-%d %H:%M:%S.%f%z")
        with self.assertRaisesRegex(OperationFailure, "format token is not supported"):
            date_module._python_strptime_format("$dateFromString", "%Q")

        eval_expr = lambda _doc, expr, _vars=None: expr
        with self.assertRaisesRegex(OperationFailure, "requires either year or isoWeekYear"):
            date_module._build_date_from_parts("$dateFromParts", {}, {}, None, evaluate_expression=eval_expr)
        with self.assertRaisesRegex(OperationFailure, "cannot mix calendar and iso week date parts"):
            date_module._build_date_from_parts("$dateFromParts", {"year": 2026, "isoWeekYear": 2026}, {}, None, evaluate_expression=eval_expr)
        with self.assertRaisesRegex(OperationFailure, "year must be specified"):
            date_module._build_date_from_parts("$dateFromParts", {"month": 1}, {}, None, evaluate_expression=eval_expr)
        with self.assertRaisesRegex(OperationFailure, "isoWeekYear must be specified"):
            date_module._build_date_from_parts("$dateFromParts", {"isoWeek": 1}, {}, None, evaluate_expression=eval_expr)
        with self.assertRaisesRegex(OperationFailure, "produced an invalid date"):
            date_module._build_date_from_parts("$dateFromParts", {"year": 2026, "month": 2, "day": 31}, {}, None, evaluate_expression=eval_expr)
        with self.assertRaisesRegex(OperationFailure, "hour must be in range"):
            date_module._build_date_from_parts("$dateFromParts", {"year": 2026, "hour": 24}, {}, None, evaluate_expression=eval_expr)

        self.assertEqual(date_module._resolve_timezone("$x", "-02:30").utcoffset(None), datetime.timedelta(hours=-2, minutes=-30))
        madrid = date_module._resolve_timezone("$x", "Europe/Madrid")
        self.assertIsNotNone(madrid)
        with self.assertRaisesRegex(OperationFailure, "timezone must evaluate to a string"):
            date_module._resolve_timezone("$x", 1)
        with self.assertRaisesRegex(OperationFailure, "timezone is invalid"):
            date_module._resolve_timezone("$x", "Mars/Olympus")

        aware = datetime.datetime(2026, 3, 25, 10, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        restored = date_module._restore_datetime_timezone(
            datetime.datetime(2026, 3, 25, 8, 0, tzinfo=datetime.UTC),
            aware,
        )
        self.assertEqual(restored.tzinfo, aware.tzinfo)

        eval_missing = lambda _doc, expr, _vars=None: expr
        missing = object()
        self.assertIsNone(
            date_module._evaluate_localized_date_operand(
                "$year",
                {},
                missing,
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        )
        with self.assertRaisesRegex(OperationFailure, "requires a date expression"):
            date_module._evaluate_localized_date_operand(
                "$year",
                {},
                {},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        with self.assertRaisesRegex(OperationFailure, "requires a date input"):
            date_module._evaluate_localized_date_operand(
                "$year",
                {},
                "not-a-date",
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )

        base = datetime.datetime(2026, 1, 31, 10, 0, 0)
        self.assertEqual(date_module._add_date_unit(base, "millisecond", 1), base + datetime.timedelta(milliseconds=1))
        self.assertEqual(date_module._add_date_unit(base, "second", 1), base + datetime.timedelta(seconds=1))
        self.assertEqual(date_module._add_date_unit(base, "minute", 1), base + datetime.timedelta(minutes=1))
        self.assertEqual(date_module._add_date_unit(base, "hour", 1), base + datetime.timedelta(hours=1))
        self.assertEqual(date_module._add_date_unit(base, "day", 1), base + datetime.timedelta(days=1))
        self.assertEqual(date_module._add_date_unit(base, "week", 1), base + datetime.timedelta(weeks=1))
        self.assertEqual(date_module._add_date_unit(base, "month", 1).month, 2)
        self.assertEqual(date_module._add_date_unit(base, "quarter", 1).month, 4)
        self.assertEqual(date_module._add_date_unit(base, "year", 1).year, 2027)
        with self.assertRaisesRegex(OperationFailure, "Unsupported date unit"):
            date_module._add_date_unit(base, "century", 1)
        with self.assertRaisesRegex(OperationFailure, "unit must evaluate to a string"):
            date_module._require_date_unit("$dateAdd", 1)
        with self.assertRaisesRegex(OperationFailure, "unit is invalid"):
            date_module._require_date_unit("$dateAdd", "century")

        start = datetime.datetime(2026, 3, 25, 10, 5, 6, 1000)
        end = datetime.datetime(2026, 3, 26, 11, 6, 7, 2000)
        self.assertEqual(date_module._date_diff_units(start, end, "millisecond", start_of_week="sunday"), 90061001)
        self.assertEqual(date_module._date_diff_units(start, end, "second", start_of_week="sunday"), 90061)
        self.assertEqual(date_module._date_diff_units(start, end, "minute", start_of_week="sunday"), 1501)
        self.assertEqual(date_module._date_diff_units(start, end, "year", start_of_week="sunday"), 0)
        with self.assertRaisesRegex(OperationFailure, "unit is invalid"):
            date_module._date_diff_units(start, end, "century", start_of_week="sunday")

        with self.assertRaisesRegex(OperationFailure, "Unsupported date expression operator"):
            date_module.evaluate_date_expression(
                "$unknown",
                {},
                {},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )

    def test_date_expression_private_branches_cover_null_invalid_and_on_error_paths(self):
        eval_expr = lambda _doc, expr, _vars=None: expr
        eval_missing = lambda _doc, expr, _vars=None: expr
        missing = object()
        now = datetime.datetime(2026, 3, 25, 10, 0, 0)

        self.assertIsNone(
            date_module.evaluate_date_expression(
                "$dateAdd",
                {},
                {"startDate": None, "unit": "day", "amount": 1},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        )
        with self.assertRaisesRegex(OperationFailure, "requires a datetime startDate"):
            date_module.evaluate_date_expression(
                "$dateSubtract",
                {},
                {"startDate": "bad", "unit": "day", "amount": 1},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        self.assertIsNone(
            date_module.evaluate_date_expression(
                "$dateSubtract",
                {},
                {"startDate": now, "unit": "day", "amount": None},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        )

        with self.assertRaisesRegex(OperationFailure, "requires date"):
            date_module.evaluate_date_expression(
                "$dateToString",
                {},
                {},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        self.assertEqual(
            date_module.evaluate_date_expression(
                "$dateToString",
                {},
                {"date": None, "onNull": "fallback"},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            ),
            "fallback",
        )
        with self.assertRaisesRegex(OperationFailure, "format must evaluate to a string"):
            date_module.evaluate_date_expression(
                "$dateToString",
                {},
                {"date": now, "format": 1},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        with self.assertRaisesRegex(OperationFailure, "requires date"):
            date_module.evaluate_date_expression(
                "$dateToParts",
                {},
                {},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        self.assertIsNone(
            date_module.evaluate_date_expression(
                "$dateToParts",
                {},
                {"date": None},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        )
        with self.assertRaisesRegex(OperationFailure, "requires a date input"):
            date_module.evaluate_date_expression(
                "$dateToParts",
                {},
                {"date": "bad"},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        with self.assertRaisesRegex(OperationFailure, "requires dateString"):
            date_module.evaluate_date_expression(
                "$dateFromString",
                {},
                {},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        with self.assertRaisesRegex(OperationFailure, "format must evaluate to a string"):
            date_module.evaluate_date_expression(
                "$dateFromString",
                {},
                {"dateString": "2026-03-25", "format": 1},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        self.assertEqual(
            date_module.evaluate_date_expression(
                "$dateFromString",
                {},
                {"dateString": "bad-date", "onError": "fallback"},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            ),
            "fallback",
        )
        with self.assertRaisesRegex(OperationFailure, "requires a document specification"):
            date_module.evaluate_date_expression(
                "$dateFromParts",
                {},
                "bad",
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        self.assertIsNone(
            date_module.evaluate_date_expression(
                "$year",
                {},
                None,
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=lambda *_args, **_kwargs: missing,
                missing_sentinel=missing,
            )
        )
        with self.assertRaisesRegex(OperationFailure, "requires startDate, endDate and unit"):
            date_module.evaluate_date_expression(
                "$dateDiff",
                {},
                {},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        self.assertIsNone(
            date_module.evaluate_date_expression(
                "$dateDiff",
                {},
                {"startDate": None, "endDate": now, "unit": "day"},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        )
        with self.assertRaisesRegex(OperationFailure, "could not parse dateString"):
            date_module.evaluate_date_expression(
                "$dateFromString",
                {},
                {"dateString": "bad-date"},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                missing_sentinel=missing,
            )
        self.assertIsNone(
            date_module.evaluate_date_expression(
                "$isoWeek",
                {},
                None,
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=lambda *_args, **_kwargs: missing,
                missing_sentinel=missing,
            )
        )


class NumericHelperBranchTests(unittest.TestCase):
    def test_numeric_helper_branches_cover_nan_percentiles_rounding_and_subtract(self):
        nan_result = evaluate_expression({"value": float("nan")}, {"$abs": "$value"})
        self.assertTrue(math.isnan(nan_result))
        sqrt_nan = evaluate_expression({"value": float("nan")}, {"$sqrt": "$value"})
        self.assertTrue(math.isnan(sqrt_nan))
        self.assertIsNone(evaluate_expression({"left": None, "right": 2}, {"$pow": ["$left", "$right"]}))
        decimal_pow = evaluate_expression({"left": BsonDecimal128(decimal.Decimal("2.0")), "right": 2}, {"$pow": ["$left", "$right"]})
        self.assertEqual(decimal_pow, BsonDecimal128(decimal.Decimal("4.0")))
        self.assertEqual(evaluate_expression({"left": BsonDouble(2.0), "right": 3}, {"$pow": ["$left", "$right"]}), BsonDouble(8.0))
        self.assertEqual(evaluate_expression({"left": BsonInt32(2), "right": BsonInt64(3)}, {"$pow": ["$left", "$right"]}), BsonDouble(8.0))
        self.assertTrue(math.isinf(evaluate_expression({"value": float("inf")}, {"$round": ["$value", 0]})))
        self.assertIsNone(evaluate_expression({}, {"$round": ["$missing", 0]}))
        self.assertTrue(math.isnan(numeric_module._mongo_mod(float("inf"), 2)))
        self.assertEqual(numeric_module._mongo_mod(7, 3), 1)

        self.assertIsNone(numeric_module._sum_accumulator_operand(True))
        self.assertIsNone(numeric_module._sum_accumulator_operand("bad"))
        self.assertEqual(numeric_module._sum_accumulator_operand(BsonInt32(3)), BsonInt32(3))
        self.assertEqual(numeric_module._stddev_accumulator_operand(UNDEFINED), None)
        self.assertEqual(numeric_module._stddev_accumulator_operand(decimal.Decimal("1.5")), 1.5)
        self.assertEqual(numeric_module._stddev_accumulator_operand(2), 2.0)
        self.assertEqual(numeric_module._require_numeric("$add", 1.5), 1.5)
        self.assertEqual(numeric_module._require_numeric("$add", 3), 3)
        self.assertTrue(math.isnan(evaluate_expression({"left": float("nan"), "right": 10}, {"$log": ["$left", "$right"]})))
        self.assertEqual(
            numeric_module._stddev_expression_values(
                {},
                [["x"], decimal.Decimal("1.5"), None, UNDEFINED],
                None,
                evaluate_expression=lambda _doc, expr, _vars=None: expr,
                evaluate_expression_with_missing=lambda _doc, expr, _vars=None: expr,
                missing_sentinel=object(),
            ),
            [1.5],
        )

        with self.assertRaisesRegex(OperationFailure, "p must evaluate to an array"):
            numeric_module._normalize_percentile_probabilities("$percentile", "bad")
        with self.assertRaisesRegex(OperationFailure, "must not be an empty array"):
            numeric_module._normalize_percentile_probabilities("$percentile", [])
        with self.assertRaisesRegex(OperationFailure, "must be numeric"):
            numeric_module._normalize_percentile_probabilities("$percentile", ["bad"])
        with self.assertRaisesRegex(OperationFailure, "must be in the range"):
            numeric_module._normalize_percentile_probabilities("$percentile", [2])

        self.assertEqual(numeric_module._compute_percentiles([1, 3, 2], [0.0, 1.0]), [1, 3])
        self.assertEqual(numeric_module._round_numeric(12, 2), 12)
        self.assertEqual(numeric_module._trunc_numeric(12, 2), 12)
        self.assertEqual(numeric_module._trunc_numeric(12.345, 2), 12.34)
        self.assertEqual(numeric_module._require_integral_numeric("$bitNot", 4.0), 4)
        with self.assertRaisesRegex(OperationFailure, "requires numeric arguments"):
            numeric_module._require_numeric("$add", True)
        with self.assertRaisesRegex(OperationFailure, "requires numeric arguments"):
            numeric_module._require_numeric("$add", "bad")
        with self.assertRaisesRegex(OperationFailure, "requires integral numeric arguments"):
            numeric_module._require_integral_numeric("$bitNot", True)
        with self.assertRaisesRegex(OperationFailure, "requires integral numeric arguments"):
            numeric_module._require_integral_numeric("$bitNot", 4.5)

        when = datetime.datetime(2026, 3, 25, 10, 0, 0)
        self.assertEqual(numeric_module._subtract_values(when, 1000), when - datetime.timedelta(milliseconds=1000))
        self.assertEqual(
            numeric_module._subtract_values(when, when - datetime.timedelta(seconds=2)),
            2000,
        )
        with self.assertRaisesRegex(OperationFailure, "date-number"):
            numeric_module._subtract_values(1, when)
        with self.assertRaisesRegex(OperationFailure, "Unsupported numeric expression operator"):
            numeric_module.evaluate_numeric_expression(
                "$unknown",
                {},
                [],
                None,
                evaluate_expression=lambda _doc, expr, _vars=None: expr,
                evaluate_expression_with_missing=lambda _doc, expr, _vars=None: expr,
                require_expression_args=lambda _op, spec, _mn, _mx: list(spec),
                missing_sentinel=object(),
            )


class AccumulatorHelperBranchTests(unittest.TestCase):
    def test_accumulator_helper_branches_cover_flags_validation_and_finalize_paths(self):
        bucket = {}
        created_flags = accumulators_module._accumulator_flags(bucket)
        self.assertIs(created_flags, bucket[accumulators_module._ACCUMULATOR_FLAGS_KEY])
        self.assertEqual(accumulators_module._accumulator_flags(accumulators_module._AccumulatorBucket("a", {})), {})
        self.assertEqual(
            accumulators_module._accumulator_flags({accumulators_module._ACCUMULATOR_FLAGS_KEY: {"seen": True}}),
            {"seen": True},
        )

        for operator, expression in (
            ("$count", {"x": 1}),
            ("$firstN", {}),
            ("$top", {}),
            ("$topN", {"sortBy": {"x": 1}, "output": "$x"}),
            ("$median", {}),
            ("$percentile", {"input": "$x", "method": "approximate"}),
        ):
            with self.subTest(operator=operator):
                with self.assertRaises(OperationFailure):
                    accumulators_module._validate_accumulator_expression(operator, expression)

        self.assertIsNone(accumulators_module._sum_accumulator_operand(["x"]))
        self.assertEqual(accumulators_module._sum_accumulator_operand([1, 2, "x"]), 3)
        self.assertTrue(math.isnan(accumulators_module._stddev_accumulator_operand(float("inf"))))
        self.assertIsNone(accumulators_module._stddev_accumulator_operand([]))
        with self.assertRaisesRegex(OperationFailure, "positive integer"):
            accumulators_module._normalize_pick_n_size("$firstN", 0)

        eval_expr = lambda _doc, expr, _vars=None: expr
        eval_missing = lambda _doc, expr, _vars=None: expr
        missing = object()
        value, size = accumulators_module._evaluate_pick_n_input(
            "$firstN",
            {},
            {"input": missing, "n": 2},
            None,
            evaluate_expression=eval_expr,
            evaluate_expression_with_missing=eval_missing,
            missing_sentinel=missing,
        )
        self.assertIsNone(value)
        self.assertEqual(size, 2)
        with self.assertRaisesRegex(OperationFailure, "requires input and n"):
            accumulators_module._evaluate_pick_n_size("$firstN", {}, {}, None, evaluate_expression=eval_expr)

        left = ([1], "a", 1)
        right = ([1], "b", 2)
        self.assertEqual(
            accumulators_module._compare_ordered_accumulator_items(
                left,
                right,
                [("x", 1)],
                reverse_tie_break=True,
            ),
            1,
        )
        self.assertEqual(
            accumulators_module._compare_ordered_accumulator_items(left, left, [("x", 1)]),
            0,
        )
        with self.assertRaisesRegex(OperationFailure, "requires sortBy and output"):
            accumulators_module._evaluate_ordered_accumulator_input(
                "$top",
                {},
                {},
                None,
                evaluate_expression=eval_expr,
                require_sort=lambda spec: spec,
                resolve_aggregation_field_path=lambda doc, field: doc.get(field),
                missing_sentinel=missing,
            )
        with self.assertRaisesRegex(OperationFailure, "requires n"):
            accumulators_module._evaluate_ordered_accumulator_input(
                "$topN",
                {"x": 1},
                {"sortBy": {"x": 1}, "output": "$x"},
                None,
                evaluate_expression=eval_expr,
                require_sort=lambda spec: list(spec.items()),
                resolve_aggregation_field_path=lambda doc, field: doc.get(field),
                missing_sentinel=missing,
            )

        state = accumulators_module._OrderedAccumulator(items=[([3], "c", 1), ([1], "a", 2), ([2], "b", 3)])
        accumulators_module._trim_ordered_accumulator(state, [("x", 1)], keep=1, bottom=True)
        self.assertEqual([item[1] for item in state.items], ["c"])

        self.assertEqual(accumulators_module._coerce_accumulator_specs(None), (("count", "$sum", 1),))
        prepared = (("a", "$sum", 1),)
        self.assertIs(accumulators_module._coerce_accumulator_specs(prepared), prepared)
        with self.assertRaisesRegex(OperationFailure, "single-key document"):
            accumulators_module._coerce_accumulator_specs({"a": {"$sum": 1, "$max": 2}})

        std_bucket = {"score": accumulators_module._StdDevAccumulator(population=True)}
        accumulators_module._apply_accumulators(
            std_bucket,
            {"score": {"$stdDevPop": "$score"}},
            {"score": float("inf")},
        )
        self.assertTrue(std_bucket["score"].invalid)
        with self.assertRaisesRegex(OperationFailure, "consistent positive integer"):
            accumulators_module._apply_accumulators(
                {"top": accumulators_module._OrderedAccumulator(n=2, sort_spec=[("rank", 1)])},
                {"top": {"$topN": {"sortBy": {"rank": 1}, "output": "$rank", "n": 3}}},
                {"rank": 1},
            )
        with self.assertRaisesRegex(OperationFailure, "consistent positive integer within the group"):
            accumulators_module._apply_accumulators(
                {"firstOne": accumulators_module._PickNAccumulator(items=[1], n=1)},
                {"firstOne": {"$firstN": {"input": "$value", "n": 2}}},
                {"value": 2},
            )
        with self.assertRaisesRegex(OperationFailure, "consistent array within the group"):
            accumulators_module._apply_accumulators(
                {"pct": accumulators_module._PercentileAccumulator(probabilities=[0.5])},
                {"pct": {"$percentile": {"input": "$value", "p": [0.25], "method": "approximate"}}},
                {"value": 1},
            )
        self.assertFalse(accumulators_module._window_sort_keys_equal([1], [1, 2]))

        finalized = accumulators_module._finalize_accumulators(
            {
                "avg": accumulators_module._AverageAccumulator(total=decimal.Decimal("9"), count=2),
                "std_invalid": accumulators_module._StdDevAccumulator(
                    population=True,
                    total=0.0,
                    sum_of_squares=0.0,
                    count=1,
                    invalid=True,
                ),
                "ordered_empty": accumulators_module._OrderedAccumulator(n=2),
                "ordered_scalar": accumulators_module._OrderedAccumulator(items=[([1], "x", 0)]),
                "ordered_list": accumulators_module._OrderedAccumulator(items=[([1], "x", 0), ([2], "y", 1)], n=2),
                "percentile_scalar": accumulators_module._PercentileAccumulator(values=[1, 5], probabilities=[0.5], scalar_output=True),
                "percentile_list": accumulators_module._PercentileAccumulator(values=[], probabilities=[0.5], scalar_output=False),
            }
        )
        self.assertEqual(finalized["avg"], decimal.Decimal("4.5"))
        self.assertIsNone(finalized["std_invalid"])
        self.assertEqual(finalized["ordered_empty"], [])
        self.assertEqual(finalized["ordered_scalar"], "x")
        self.assertEqual(finalized["ordered_list"], ["x", "y"])
        self.assertEqual(finalized["percentile_scalar"], 1)
        self.assertIsNone(finalized["percentile_list"])


class CompiledAggregationHelperBranchTests(unittest.TestCase):
    def test_compiled_group_private_helpers_cover_cache_freezing_and_simple_arithmetic(self):
        self.assertIsNone(compiled_module._compiled_multiply([None, 2]))
        self.assertIsNone(compiled_module._compiled_subtract(None, 2))
        with self.assertRaisesRegex(OperationFailure, "single date argument"):
            compiled_module._compiled_add(
                [
                    datetime.datetime(2026, 1, 1, 0, 0, 0),
                    datetime.datetime(2026, 1, 2, 0, 0, 0),
                ]
            )

        self.assertFalse(compiled_module.CompiledGroup.supports({"total": {"$sum": "$value"}}))
        self.assertEqual(
            compiled_module.CompiledGroup._freeze_cache_value((1, 2)),
            ("tuple", (1, 2)),
        )
        self.assertEqual(
            compiled_module.CompiledGroup._freeze_cache_value({2, 1}),
            ("set", (1, 2)),
        )
        frozen_regex = compiled_module.CompiledGroup._freeze_cache_value(re.compile("^ad", re.IGNORECASE))
        self.assertEqual(frozen_regex[:2], ("regex", "^ad"))

        class _Unhashable:
            __hash__ = None

            def __repr__(self) -> str:
                return "<unhashable>"

        self.assertEqual(
            compiled_module.CompiledGroup._freeze_cache_value(_Unhashable()),
            ("repr", "<unhashable>"),
        )

    def test_compiled_group_private_branches_cover_cache_eviction_and_expression_codegen(self):
        original_maxsize = compiled_module.CompiledGroup._COMPILED_FUNCTION_CACHE_MAXSIZE
        compiled_module.CompiledGroup._COMPILED_FUNCTION_CACHE.clear()
        compiled_module.CompiledGroup._COMPILED_FUNCTION_CACHE_MAXSIZE = 1
        try:
            compiled_module.CompiledGroup({"_id": "$kind", "total": {"$sum": "$value"}})
            compiled_module.CompiledGroup({"_id": "$other", "total": {"$sum": "$value"}})
            self.assertEqual(len(compiled_module.CompiledGroup._COMPILED_FUNCTION_CACHE), 1)
        finally:
            compiled_module.CompiledGroup._COMPILED_FUNCTION_CACHE.clear()
            compiled_module.CompiledGroup._COMPILED_FUNCTION_CACHE_MAXSIZE = original_maxsize

        compiled = compiled_module.CompiledGroup({"_id": "$kind", "total": {"$sum": "$value"}})
        self.assertEqual(compiled._compile_expression("plain", "x"), "'plain'")
        self.assertEqual(compiled._compile_expression({"$literal": "value"}, "x"), "'value'")
        self.assertEqual(compiled._compile_expression({"nested": True}, "x").startswith("_evaluate("), True)
        self.assertIn("state[0] += 1", "\n".join(compiled_module.CompiledGroup({"_id": "$kind", "count": {"$count": {}}})._compile_logic_only()))


class GroupingHelperBranchTests(unittest.TestCase):
    def test_grouping_helpers_cover_mutable_copy_and_bucket_bounds(self):
        copied = grouping_stages._copy_if_mutable({"tags": {1}})
        copied["tags"].add(2)
        self.assertEqual(copied, {"tags": {1, 2}})
        self.assertEqual(grouping_stages._copy_if_mutable(5), 5)

        self.assertIsNone(grouping_stages._find_bucket_index(-1, [0, 5, 10]))
        self.assertIsNone(grouping_stages._find_bucket_index(10, [0, 5, 10]))
        self.assertEqual(grouping_stages._find_bucket_index(7, [0, 5, 10]), 1)


class PlanningAndJoinHelperBranchTests(unittest.TestCase):
    def test_planning_and_join_helpers_cover_documents_stage_and_lookup_let_validation(self):
        with self.assertRaisesRegex(OperationFailure, "\\$documents requires an array of documents"):
            planning_module._require_documents_stage("bad")

        with self.assertRaisesRegex(OperationFailure, "must begin with a lowercase letter or non-ascii character"):
            join_stages._apply_lookup(
                [{"tenant": "eu"}],
                {
                    "from": "users",
                    "as": "matches",
                    "pipeline": [],
                    "let": {"ROOT": "$tenant"},
                },
                lambda _name: [{"tenant": "eu"}],
            )


class ControlObjectHelperBranchTests(unittest.TestCase):
    def test_control_object_private_branches_cover_invalid_specs_and_fallbacks(self):
        document = {"items": [1, 2], "obj": {"name": "Ada"}}
        eval_expr = lambda _doc, expr, _vars=None: expr
        eval_missing = lambda _doc, expr, _vars=None: expr
        require_args = lambda _op, spec, _mn, _mx: list(spec)

        self.assertIsNone(control_module._evaluate_field_bound_query_operator("$in", document, "bad"))
        self.assertIsNone(control_module._evaluate_field_bound_query_operator("$in", document, ["name", []]))
        with self.assertRaisesRegex(OperationFailure, "only supports the field-bound form"):
            control_module.evaluate_control_object_expression(
                "$nin",
                document,
                ["literal", [1]],
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                require_expression_args=require_args,
                compare_values=lambda left, right, operator: left == right,
                expression_truthy=bool,
                require_array=lambda _op, value: value,
                evaluate_pick_n_input=lambda *_args, **_kwargs: ([], 1),
                missing_sentinel=object(),
            )
        for operator, spec, expected in (
            ("$setField", {}, "field, input, and value"),
            ("$unsetField", {}, "field and input"),
            ("$switch", {}, "requires branches"),
        ):
            with self.subTest(operator=operator):
                with self.assertRaisesRegex(OperationFailure, expected):
                    control_module.evaluate_control_object_expression(
                        operator,
                        document,
                        spec,
                        None,
                        evaluate_expression=eval_expr,
                        evaluate_expression_with_missing=eval_missing,
                        require_expression_args=require_args,
                        compare_values=lambda left, right, operator: left == right,
                        expression_truthy=bool,
                        require_array=lambda _op, value: value,
                        evaluate_pick_n_input=lambda *_args, **_kwargs: ([], 1),
                        missing_sentinel=object(),
                    )
        with self.assertRaisesRegex(OperationFailure, "no default was specified"):
            control_module.evaluate_control_object_expression(
                "$switch",
                document,
                {"branches": [{"case": False, "then": 1}]},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                require_expression_args=require_args,
                compare_values=lambda left, right, operator: left == right,
                expression_truthy=bool,
                require_array=lambda _op, value: value,
                evaluate_pick_n_input=lambda *_args, **_kwargs: ([], 1),
                missing_sentinel=object(),
            )
        with self.assertRaisesRegex(OperationFailure, "requires at least 1 arguments"):
            control_module.evaluate_control_object_expression(
                "$mergeObjects",
                document,
                [],
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                require_expression_args=require_args,
                compare_values=lambda left, right, operator: left == right,
                expression_truthy=bool,
                require_array=lambda _op, value: value,
                evaluate_pick_n_input=lambda *_args, **_kwargs: ([], 1),
                missing_sentinel=object(),
            )
        self.assertIsNone(
            control_module.evaluate_control_object_expression(
                "$getField",
                document,
                {"field": None, "input": "$$CURRENT"},
                None,
                evaluate_expression=lambda _doc, expr, _vars=None: None if expr is None else document,
                evaluate_expression_with_missing=lambda _doc, expr, _vars=None: document,
                require_expression_args=require_args,
                compare_values=lambda left, right, operator: left == right,
                expression_truthy=bool,
                require_array=lambda _op, value: value,
                evaluate_pick_n_input=lambda *_args, **_kwargs: ([], 1),
                missing_sentinel=object(),
            )
        )
        self.assertIsNone(
            control_module.evaluate_control_object_expression(
                "$getField",
                document,
                {"field": "name", "input": "not-an-object"},
                None,
                evaluate_expression=lambda _doc, expr, _vars=None: expr,
                evaluate_expression_with_missing=lambda _doc, expr, _vars=None: expr,
                require_expression_args=require_args,
                compare_values=lambda left, right, operator: left == right,
                expression_truthy=bool,
                require_array=lambda _op, value: value,
                evaluate_pick_n_input=lambda *_args, **_kwargs: ([], 1),
                missing_sentinel=object(),
            )
        )
        with self.assertRaisesRegex(OperationFailure, "Unsupported control/object expression operator"):
            control_module.evaluate_control_object_expression(
                "$unknown",
                document,
                {},
                None,
                evaluate_expression=eval_expr,
                evaluate_expression_with_missing=eval_missing,
                require_expression_args=require_args,
                compare_values=lambda left, right, operator: left == right,
                expression_truthy=bool,
                require_array=lambda _op, value: value,
                evaluate_pick_n_input=lambda *_args, **_kwargs: ([], 1),
                missing_sentinel=object(),
            )
