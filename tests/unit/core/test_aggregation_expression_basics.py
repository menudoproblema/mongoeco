import datetime
import decimal
import math
import re
import unittest
import uuid
from copy import deepcopy
from unittest.mock import ANY, patch

from mongoeco.compat import MongoDialect
import mongoeco.core.aggregation.accumulators as accumulators_module
import mongoeco.core.aggregation.grouping_stages as grouping_stages
from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.core.aggregation.compiled_aggregation import CompiledGroup
from mongoeco.core.collation import CollationSpec
from mongoeco.core.aggregation.accumulators import _AccumulatorBucket, _OrderedAccumulator
from mongoeco.core.aggregation import (
    _ACCUMULATOR_FLAGS_KEY,
    _MISSING,
    _accumulator_flags,
    _aggregation_key,
    _apply_accumulators,
    _apply_group,
    _finalize_accumulators,
    _initialize_accumulators,
    _is_simple_projection,
    _match_spec_contains_expr,
    _require_projection,
    _resolve_aggregation_field_path,
    AggregationSpillPolicy,
    CompiledPipelinePlan,
    apply_pipeline,
    compile_pipeline,
    evaluate_expression,
    register_aggregation_expression_operator,
    register_aggregation_stage,
    split_pushdown_pipeline,
    unregister_aggregation_expression_operator,
    unregister_aggregation_stage,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, Decimal128, ObjectId, Regex, Timestamp, UNDEFINED




class AggregationExpressionBasicsTests(unittest.TestCase):
    def test_aggregation_spill_policy_skips_non_blocking_stages_and_invalid_thresholds(self):
        policy = AggregationSpillPolicy(threshold=10)
        documents = [{"_id": "1"}, {"_id": "2"}]

        self.assertIs(policy.maybe_spill("$project", documents), documents)
        self.assertFalse(policy.should_spill("$sort", documents))

        with self.assertRaises(ValueError):
            AggregationSpillPolicy(threshold=0)

    def test_evaluate_expression_rejects_currently_unsupported_operators_explicitly(self):
        document = {"value": "10", "tags": ["a", "b"]}

        unsupported_specs = []

        for spec in unsupported_specs:
            with self.subTest(spec=spec):
                with self.assertRaises(OperationFailure):
                    evaluate_expression(document, spec)

    def test_evaluate_expression_rejects_broader_unsupported_inventory_explicitly(self):
        document = {
            "value": 10,
            "text": "Ada",
            "items": [1, 2, 3],
            "created_at": datetime.datetime(2026, 3, 25, 10, 0, 0),
        }

        unsupported_specs = [
            {"$sampleRate": 0.5},
            {"$toHashedIndexKey": "$text"},
            {"$function": {"body": "function() { return 1; }", "args": [], "lang": "js"}},
            {"$accumulator": {"init": "function(){}", "accumulate": "function(){}", "accumulateArgs": [], "merge": "function(){}", "finalize": "function(x){return x;}", "lang": "js"}},
        ]

        for spec in unsupported_specs:
            with self.subTest(spec=spec):
                with self.assertRaises(OperationFailure):
                    evaluate_expression(document, spec)

    def test_evaluate_expression_supports_is_number_and_type(self):
        document = {
            "value": 10,
            "ratio": 1.5,
            "text": "Ada",
            "items": [1, "x"],
            "blob": b"abc",
            "created_at": datetime.datetime(2026, 3, 25, 10, 0, 0),
            "legacy": UNDEFINED,
        }

        self.assertTrue(evaluate_expression(document, {"$isNumber": "$value"}))
        self.assertTrue(evaluate_expression(document, {"$isNumber": "$ratio"}))
        self.assertFalse(evaluate_expression(document, {"$isNumber": "$text"}))
        self.assertFalse(evaluate_expression(document, {"$isNumber": "$missing"}))
        self.assertEqual(evaluate_expression(document, {"$type": "$value"}), "int")
        self.assertEqual(evaluate_expression(document, {"$type": "$ratio"}), "double")
        self.assertEqual(evaluate_expression(document, {"$type": "$items"}), "array")
        self.assertEqual(evaluate_expression(document, {"$type": "$blob"}), "binData")
        self.assertEqual(evaluate_expression(document, {"$type": "$created_at"}), "date")
        self.assertEqual(evaluate_expression(document, {"$type": "$legacy"}), "undefined")
        self.assertEqual(evaluate_expression(document, {"$type": "$missing"}), "missing")

    def test_evaluate_expression_supports_scalar_coercions(self):
        document = {
            "int_text": "42",
            "float_text": "3.5",
            "truthy_text": "false",
            "date": datetime.datetime(2026, 3, 25, 10, 0, 0),
            "zero": 0,
            "flag": True,
            "missing": None,
        }

        self.assertEqual(evaluate_expression(document, {"$toInt": "$int_text"}), 42)
        self.assertEqual(evaluate_expression(document, {"$toDouble": "$float_text"}), 3.5)
        self.assertEqual(evaluate_expression(document, {"$toLong": "$flag"}), 1)
        self.assertEqual(evaluate_expression(document, {"$toBool": "$zero"}), False)
        self.assertEqual(evaluate_expression(document, {"$toBool": "$truthy_text"}), True)
        self.assertEqual(evaluate_expression(document, {"$toLong": "$date"}), 1774432800000)
        self.assertEqual(evaluate_expression(document, {"$toDouble": "$date"}), 1774432800000.0)
        self.assertIsNone(evaluate_expression(document, {"$toInt": "$missing"}))
        self.assertIsNone(evaluate_expression(document, {"$toLong": "$unknown"}))

    def test_evaluate_expression_scalar_coercions_reject_invalid_values(self):
        document = {
            "bad_text": "4.2",
            "array_value": [1],
            "huge": 1 << 70,
            "fractional": 3.5,
        }

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toInt": "$bad_text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toInt": "$fractional"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toLong": "$huge"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDouble": "$array_value"})

    def test_evaluate_expression_supports_date_add_subtract_and_diff(self):
        document = {
            "start": datetime.datetime(2026, 3, 25, 10, 0, 0),
            "end": datetime.datetime(2026, 3, 27, 9, 0, 0),
            "month_end": datetime.datetime(2026, 1, 31, 12, 0, 0),
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateAdd": {"startDate": "$start", "unit": "day", "amount": 2}},
            ),
            datetime.datetime(2026, 3, 27, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateSubtract": {"startDate": "$start", "unit": "hour", "amount": 3}},
            ),
            datetime.datetime(2026, 3, 25, 7, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateAdd": {"startDate": "$month_end", "unit": "month", "amount": 1}},
            ),
            datetime.datetime(2026, 2, 28, 12, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "day"}},
            ),
            2,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "hour"}},
            ),
            47,
        )

    def test_evaluate_expression_date_math_supports_timezone_and_week_diff(self):
        document = {
            "start": datetime.datetime(2026, 3, 29, 0, 30, 0),
            "end": datetime.datetime(2026, 4, 5, 0, 30, 0),
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {
                    "$dateAdd": {
                        "startDate": "$start",
                        "unit": "day",
                        "amount": 1,
                        "timezone": "+02:00",
                    }
                },
            ),
            datetime.datetime(2026, 3, 30, 0, 30, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {
                    "$dateDiff": {
                        "startDate": "$start",
                        "endDate": "$end",
                        "unit": "week",
                        "startOfWeek": "sunday",
                    }
                },
            ),
            1,
        )

    def test_evaluate_expression_date_math_rejects_invalid_shapes(self):
        document = {"start": datetime.datetime(2026, 3, 25, 10, 0, 0)}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateAdd": {"startDate": "$start", "unit": "day"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateAdd": {"startDate": "$start", "unit": "day", "amount": 1.5}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateSubtract": {"startDate": "$start", "unit": "century", "amount": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateDiff": {"startDate": "$start", "endDate": "$start", "unit": "week", "startOfWeek": 1}})

    def test_evaluate_expression_supports_substr_and_strlen_byte_and_codepoint_variants(self):
        document = {"text": "é寿司A"}

        self.assertEqual(evaluate_expression(document, {"$substrBytes": ["$text", 0, 2]}), "é")
        self.assertEqual(evaluate_expression(document, {"$substrCP": ["$text", 1, 2]}), "寿司")
        self.assertEqual(evaluate_expression(document, {"$strLenBytes": "$text"}), len("é寿司A".encode("utf-8")))
        self.assertEqual(evaluate_expression(document, {"$strLenCP": "$text"}), 4)
        self.assertEqual(evaluate_expression(document, {"$substrBytes": ["$missing", 0, 2]}), "")
        self.assertEqual(evaluate_expression(document, {"$substrCP": ["$missing", 0, 2]}), "")

    def test_evaluate_expression_substr_and_strlen_variants_reject_invalid_values(self):
        document = {"text": "é寿司A", "items": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substrBytes": ["$text", 1, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substrCP": ["$items", 0, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$strLenBytes": "$missing"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$strLenCP": "$items"})

    def test_evaluate_expression_supports_string_index_and_binary_size_variants(self):
        document = {"text": "é寿司A", "blob": b"abcd", "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678")}

        self.assertEqual(evaluate_expression(document, {"$indexOfBytes": ["$text", "寿", 0, 9]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfCP": ["$text", "司A", 0, 4]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfCP": ["$text", "x"]}), -1)
        self.assertEqual(evaluate_expression(document, {"$binarySize": "$blob"}), 4)
        self.assertEqual(evaluate_expression(document, {"$binarySize": "$uuid"}), 16)
        self.assertIsNone(evaluate_expression(document, {"$binarySize": "$missing"}))
        self.assertEqual(
            evaluate_expression(document, {"$toUUID": "12345678-1234-5678-1234-567812345678"}),
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
        )

    def test_evaluate_expression_index_and_binary_size_variants_reject_invalid_values(self):
        document = {"text": "é寿司A", "blob": b"abcd", "items": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$indexOfBytes": ["$text", "寿", -1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$indexOfCP": ["$items", "1"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$binarySize": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toUUID": "not-a-uuid"})

    def test_evaluate_expression_supports_regex_match_find_and_find_all(self):
        document = {"text": "Ada and ada", "missing": None}

        self.assertTrue(
            evaluate_expression(document, {"$regexMatch": {"input": "$text", "regex": "^ada", "options": "i"}})
        )
        self.assertEqual(
            evaluate_expression(document, {"$regexFind": {"input": "$text", "regex": "(a)(d)a", "options": "i"}}),
            {"match": "Ada", "idx": 0, "captures": ["A", "d"]},
        )
        self.assertEqual(
            evaluate_expression(document, {"$regexFindAll": {"input": "$text", "regex": "a", "options": "i"}}),
            [
                {"match": "A", "idx": 0, "captures": []},
                {"match": "a", "idx": 2, "captures": []},
                {"match": "a", "idx": 4, "captures": []},
                {"match": "a", "idx": 8, "captures": []},
                {"match": "a", "idx": 10, "captures": []},
            ],
        )
        self.assertFalse(evaluate_expression(document, {"$regexMatch": {"input": "$missing", "regex": "a"}}))
        self.assertIsNone(evaluate_expression(document, {"$regexFind": {"input": "$missing", "regex": "a"}}))
        self.assertEqual(evaluate_expression(document, {"$regexFindAll": {"input": "$missing", "regex": "a"}}), [])

    def test_evaluate_expression_regex_variants_reject_invalid_values(self):
        document = {"text": "Ada", "items": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexMatch": {"input": "$items", "regex": "a"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexFind": {"input": "$text", "regex": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexFindAll": {"input": "$text", "regex": "a", "options": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexMatch": {"input": "$text", "regex": re.compile("a", re.DOTALL)}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexMatch": {"input": "$text", "regex": re.compile("a"), "options": "i"}})

    def test_evaluate_expression_supports_numeric_math_variants(self):
        document = {"value": 19.25, "base": 100, "power": 3}

        self.assertEqual(evaluate_expression(document, {"$abs": -4}), 4)
        self.assertAlmostEqual(evaluate_expression(document, {"$exp": 1}), math.e)
        self.assertAlmostEqual(evaluate_expression(document, {"$ln": math.e}), 1.0)
        self.assertAlmostEqual(evaluate_expression(document, {"$log": ["$base", 10]}), 2.0)
        self.assertAlmostEqual(evaluate_expression(document, {"$log10": "$base"}), 2.0)
        self.assertEqual(evaluate_expression(document, {"$pow": [2, "$power"]}), 8.0)
        self.assertEqual(evaluate_expression(document, {"$round": ["$value", 1]}), 19.2)
        self.assertEqual(evaluate_expression(document, {"$round": [25, 0]}), 25)
        self.assertEqual(evaluate_expression(document, {"$trunc": ["$value", 1]}), 19.2)
        self.assertEqual(evaluate_expression(document, {"$trunc": ["$value", -1]}), 10)
        self.assertEqual(evaluate_expression(document, {"$sqrt": 25}), 5.0)
        self.assertIsNone(evaluate_expression(document, {"$ln": "$missing"}))

    def test_evaluate_expression_numeric_math_variants_reject_invalid_values(self):
        document = {"value": -1}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sqrt": "$value"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$ln": 0})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$log": [8, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$round": [1.25, 101]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trunc": [1.25, True]})

    def test_evaluate_expression_supports_week_variants(self):
        document = {"created_at": datetime.datetime(2026, 1, 1, 23, 30, 0)}

        self.assertEqual(evaluate_expression(document, {"$week": "$created_at"}), 0)
        self.assertEqual(
            evaluate_expression(document, {"$week": {"date": "$created_at", "timezone": "+02:00"}}),
            0,
        )
        self.assertEqual(evaluate_expression(document, {"$isoWeek": "$created_at"}), 1)
        self.assertEqual(evaluate_expression(document, {"$isoWeekYear": "$created_at"}), 2026)

    def test_evaluate_expression_week_variants_reject_invalid_values(self):
        document = {"text": "Ada"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$week": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$isoWeek": {"timezone": "UTC"}})

    def test_evaluate_expression_supports_date_string_parts_and_parsing_variants(self):
        document = {"created_at": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000)}

        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateToString": {"date": "$created_at", "format": "%Y-%m-%d %H:%M:%S.%L", "timezone": "UTC"}},
            ),
            "2026-03-25 10:05:06.789",
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateToParts": {"date": "$created_at", "timezone": "UTC"}},
            ),
            {
                "year": 2026,
                "month": 3,
                "day": 25,
                "hour": 10,
                "minute": 5,
                "second": 6,
                "millisecond": 789,
            },
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateToParts": {"date": "$created_at", "timezone": "UTC", "iso8601": True}},
            ),
            {
                "isoWeekYear": 2026,
                "isoWeek": 13,
                "isoDayOfWeek": 3,
                "hour": 10,
                "minute": 5,
                "second": 6,
                "millisecond": 789,
            },
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromString": {"dateString": "2026-03-25T10:05:06.789Z"}},
            ),
            datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {
                    "$dateFromString": {
                        "dateString": "2026-03-25 12:05:06.789",
                        "format": "%Y-%m-%d %H:%M:%S.%L",
                        "timezone": "+02:00",
                    }
                },
            ),
            datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
        )

    def test_evaluate_expression_date_string_parts_and_parsing_variants_reject_invalid_values(self):
        document = {"text": "Ada", "created_at": datetime.datetime(2026, 3, 25, 10, 5, 6)}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateToString": {"date": "$text"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateToParts": {"date": "$created_at", "iso8601": "yes"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromString": {"dateString": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromString": {"dateString": "2026-03-25", "format": "%Q"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateToString": {"date": "$created_at", "timezone": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromString": {"dateString": "2026-03-25", "timezone": 1}})

    def test_evaluate_expression_supports_object_to_array_and_zip(self):
        document = {"doc": {"a": 1, "b": 2}, "left": ["a", "b"], "right": [1], "defaults": ["x", 0]}

        self.assertEqual(
            evaluate_expression(document, {"$objectToArray": "$doc"}),
            [{"k": "a", "v": 1}, {"k": "b", "v": 2}],
        )
        self.assertEqual(
            evaluate_expression(document, {"$zip": {"inputs": ["$left", "$right"]}}),
            [["a", 1]],
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$zip": {"inputs": ["$left", "$right"], "useLongestLength": True, "defaults": "$defaults"}},
            ),
            [["a", 1], ["b", 0]],
        )

    def test_evaluate_expression_object_to_array_and_zip_reject_invalid_values(self):
        document = {"text": "Ada", "left": ["a"], "right": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$objectToArray": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$zip": {"inputs": ["$left", "$right"], "useLongestLength": "yes"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$zip": {"inputs": ["$left", "$right"], "defaults": [0]}})

    def test_evaluate_expression_supports_to_date_and_date_from_parts(self):
        oid = ObjectId("65f0a1000000000000000000")
        document = {"millis": 1_711_361_506_789, "text": "2026-03-25T10:05:06.789Z", "oid": oid}

        self.assertEqual(
            evaluate_expression(document, {"$toDate": "$millis"}),
            datetime.datetime(2024, 3, 25, 10, 11, 46, 789000),
        )
        self.assertEqual(
            evaluate_expression(document, {"$toDate": "$text"}),
            datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
        )
        self.assertEqual(
            evaluate_expression(document, {"$toDate": "$oid"}),
            datetime.datetime.fromtimestamp(oid.generation_time, tz=datetime.UTC).replace(tzinfo=None),
        )
        self.assertEqual(
            evaluate_expression(document, {"$toObjectId": "65f0a1000000000000000000"}),
            oid,
        )
        self.assertEqual(
            evaluate_expression(document, {"$toDecimal": "10.25"}),
            decimal.Decimal("10.25"),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromParts": {"year": 2026, "month": 3, "day": 25, "hour": 12, "timezone": "+02:00"}},
            ),
            datetime.datetime(2026, 3, 25, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromParts": {"isoWeekYear": 2026, "isoWeek": 13, "isoDayOfWeek": 3, "hour": 10}},
            ),
            datetime.datetime(2026, 3, 25, 10, 0, 0),
        )

    def test_evaluate_expression_to_date_and_date_from_parts_reject_invalid_values(self):
        document = {"text": "Ada"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDate": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toObjectId": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDecimal": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromParts": {"year": 2026, "isoWeekYear": 2026}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromParts": {"month": 3}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromParts": {"year": 2026, "timezone": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromParts": {"year": 2026, "hour": "x"}})

    def test_evaluate_expression_supports_switch_and_bitwise_variants(self):
        document = {"value": 5}

        self.assertEqual(
            evaluate_expression(
                document,
                {"$switch": {"branches": [{"case": {"$gt": ["$value", 10]}, "then": "big"}], "default": "small"}},
            ),
            "small",
        )
        self.assertEqual(evaluate_expression(document, {"$bitAnd": [7, 3]}), 3)
        self.assertEqual(evaluate_expression(document, {"$bitOr": [4, 1]}), 5)
        self.assertEqual(evaluate_expression(document, {"$bitXor": [7, 3]}), 4)
        self.assertEqual(evaluate_expression(document, {"$bitNot": 7}), -8)

    def test_evaluate_expression_switch_and_bitwise_variants_reject_invalid_values(self):
        document = {"value": 5}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$switch": {"branches": []}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$switch": {"branches": [{"case": True}]}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$bitAnd": [7, 3.5]})

    def test_evaluate_expression_supports_convert_and_set_field(self):
        document = {"value": "10", "nested": {"a": 1}}

        self.assertEqual(
            evaluate_expression(document, {"$convert": {"input": "$value", "to": "int"}}),
            10,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$convert": {"input": "$missing", "to": "int", "onNull": 7}},
            ),
            7,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$setField": {"field": "name", "input": "$nested", "value": "Ada"}},
            ),
            {"a": 1, "name": "Ada"},
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$unsetField": {"field": "a", "input": "$nested"}},
            ),
            {},
        )
        self.assertIsNone(
            evaluate_expression(
                {"nested": None},
                {"$unsetField": {"field": "a", "input": "$nested"}},
            )
        )
        self.assertIsNone(
            evaluate_expression(
                {"nested": UNDEFINED},
                {"$setField": {"field": "name", "input": "$nested", "value": "Ada"}},
            )
        )

    def test_evaluate_expression_convert_and_set_field_reject_invalid_values(self):
        document = {"value": "Ada", "nested": {"a": 1}, "text": "not-an-object"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$convert": {"input": "$value", "to": "int"}})
        self.assertEqual(
            evaluate_expression(
                document,
                {"$convert": {"input": "$value", "to": "int", "onError": -1}},
            ),
            -1,
        )
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$setField": {"field": 1, "input": "$nested", "value": "Ada"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$setField": {"field": "name", "input": "$text", "value": "Ada"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$unsetField": {"field": 1, "input": "$nested"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$unsetField": {"field": "name", "input": "$text"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$convert": {"input": "$value", "to": 1}})

    def test_evaluate_expression_supports_bson_size_and_rand(self):
        document = {"doc": {"a": 1, "name": "Ada"}, "nested": {"flag": True}}

        self.assertEqual(evaluate_expression(document, {"$bsonSize": "$doc"}), 26)
        value = evaluate_expression(document, {"$rand": {}})
        self.assertGreaterEqual(value, 0.0)
        self.assertLess(value, 1.0)

    def test_evaluate_expression_bson_size_and_rand_reject_invalid_values(self):
        document = {"text": "Ada"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$bsonSize": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$rand": 1})

    def test_evaluate_expression_supports_first_n_and_last_n_array_forms(self):
        document = {"scores": [10, 20, 30, 40], "short": [1], "missing": None}

        self.assertEqual(
            evaluate_expression(document, {"$firstN": {"input": "$scores", "n": 3}}),
            [10, 20, 30],
        )
        self.assertEqual(
            evaluate_expression(document, {"$lastN": {"input": "$scores", "n": 2}}),
            [30, 40],
        )
        self.assertEqual(
            evaluate_expression(document, {"$firstN": {"input": "$short", "n": 3}}),
            [1],
        )
        self.assertIsNone(
            evaluate_expression(document, {"$lastN": {"input": "$missing", "n": 1}})
        )
        self.assertEqual(
            evaluate_expression(document, {"$maxN": {"input": [1, None, 3, 2], "n": 2}}),
            [3, 2],
        )
        self.assertEqual(
            evaluate_expression(document, {"$minN": {"input": [1, None, 3, 2], "n": 2}}),
            [1, 2],
        )

    def test_evaluate_expression_first_n_and_last_n_reject_invalid_values(self):
        document = {"scores": [10, 20, 30], "text": "Ada"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$firstN": {"input": "$scores", "n": 0}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$lastN": {"input": "$text", "n": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$firstN": {"n": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$maxN": {"input": "$scores", "n": 0}})

    def test_group_and_set_window_fields_reject_unsupported_accumulator_inventory(self):
        documents = [{"_id": "1", "group": "a", "value": 10}]

        unsupported_group_accumulators = []
        for operator in unsupported_group_accumulators:
            with self.subTest(group_operator=operator):
                with self.assertRaises(OperationFailure):
                    apply_pipeline(documents, [{"$group": {"_id": "$group", "result": {operator: "$value"}}}])

        unsupported_window_operators = [
            "$covariancePop", "$covarianceSamp",
            "$derivative", "$expMovingAvg", "$integral",
            "$linearFill", "$locf", "$shift",
        ]
        for operator in unsupported_window_operators:
            with self.subTest(window_operator=operator):
                with self.assertRaises(OperationFailure):
                    apply_pipeline(
                        documents,
                        [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {operator: "$value"}}}}],
                    )

    def test_set_window_fields_supports_rank_dense_rank_and_document_number(self):
        result = apply_pipeline(
            [
                {"_id": "1", "group": "a", "score": 10},
                {"_id": "2", "group": "a", "score": 20},
                {"_id": "3", "group": "a", "score": 20},
                {"_id": "4", "group": "b", "score": 5},
            ],
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"score": 1},
                        "output": {
                            "rank": {"$rank": {}},
                            "dense": {"$denseRank": {}},
                            "docnum": {"$documentNumber": {}},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            result,
            [
                {"_id": "1", "group": "a", "score": 10, "rank": 1, "dense": 1, "docnum": 1},
                {"_id": "2", "group": "a", "score": 20, "rank": 2, "dense": 2, "docnum": 2},
                {"_id": "3", "group": "a", "score": 20, "rank": 2, "dense": 2, "docnum": 3},
                {"_id": "4", "group": "b", "score": 5, "rank": 1, "dense": 1, "docnum": 1},
            ],
        )

    def test_set_window_fields_rank_family_rejects_invalid_payloads(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$rank": "$value"}}}}],
            )
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"output": {"result": {"$documentNumber": {}}}}}],
            )
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$denseRank": {}, "window": {"documents": ["unbounded", "current"]}}}}}],
            )

    def test_set_window_fields_rejects_shift_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$shift": {"output": "$value", "by": 1}}}}}],
            )

    def test_set_window_fields_rejects_locf_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$locf": "$value"}}}}],
            )

    def test_set_window_fields_rejects_linear_fill_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$linearFill": "$value"}}}}],
            )

    def test_evaluate_expression_supports_dotted_variable_paths(self):
        document = {"profile": {"city": "Sevilla"}}
        variables = {
            "item": {"name": "Ada", "profile": {"city": "Madrid"}},
            "ROOT": {"profile": {"city": "Bilbao"}},
        }

        self.assertEqual(evaluate_expression(document, "$$item.name", variables), "Ada")
        self.assertEqual(evaluate_expression(document, "$$item.profile.city", variables), "Madrid")
        self.assertEqual(evaluate_expression(document, "$$ROOT.profile.city", variables), "Sevilla")
        self.assertIsNone(evaluate_expression(document, "$$item.missing", variables))
        self.assertIsNone(evaluate_expression(document, "$$missing.name", variables))

    def test_evaluate_expression_preserves_array_traversal_for_field_and_variable_paths(self):
        document = {
            "items": [{"kind": "a", "tags": ["x", "y"]}, {"kind": "b", "tags": ["z"]}],
        }
        variables = {
            "item": {"children": [{"name": "Ada"}, {"name": "Grace"}]},
        }

        self.assertEqual(evaluate_expression(document, "$items.kind"), ["a", "b"])
        self.assertEqual(evaluate_expression(document, "$items.tags"), [["x", "y"], ["z"]])
        self.assertEqual(evaluate_expression(document, "$$item.children.name", variables), ["Ada", "Grace"])

    def test_pipeline_project_preserves_array_traversal(self):
        documents = [{"_id": "1", "items": [{"kind": "a"}, {"kind": "b"}]}]

        self.assertEqual(
            apply_pipeline(documents, [{"$project": {"_id": 0, "kinds": "$items.kind"}}]),
            [{"kinds": ["a", "b"]}],
        )

    def test_pipeline_supports_documents_stage_as_first_stage(self):
        self.assertEqual(
            apply_pipeline(
                [],
                [
                    {
                        "$documents": [
                            {"_id": "1", "score": 10},
                            {"_id": "2", "score": 20},
                        ]
                    },
                    {"$match": {"score": {"$gte": 15}}},
                ],
            ),
            [{"_id": "2", "score": 20}],
        )

    def test_pipeline_match_supports_nested_expr_inside_and_or(self):
        documents = [
            {"_id": "1", "a": 5, "b": 11},
            {"_id": "2", "a": 5, "b": 9},
            {"_id": "3", "a": 4, "b": 20},
        ]

        self.assertEqual(
            apply_pipeline(
                documents,
                [{"$match": {"$and": [{"a": 5}, {"$expr": {"$gt": ["$b", 10]}}]}}],
            ),
            [{"_id": "1", "a": 5, "b": 11}],
        )

    def test_pipeline_match_expr_can_compare_two_fields_from_same_document(self):
        documents = [
            {"_id": "1", "spent": 12, "budget": 10},
            {"_id": "2", "spent": 8, "budget": 10},
        ]

        self.assertEqual(
            apply_pipeline(documents, [{"$match": {"$expr": {"$gt": ["$spent", "$budget"]}}}]),
            [{"_id": "1", "spent": 12, "budget": 10}],
        )

    def test_pipeline_match_expr_uses_mongo_truthiness_rules_for_arrays(self):
        documents = [
            {"_id": "1", "flag": [], "kind": "keep"},
            {"_id": "2", "flag": 0, "kind": "drop"},
        ]

        self.assertEqual(
            apply_pipeline(documents, [{"$match": {"$expr": "$flag"}}]),
            [{"_id": "1", "flag": [], "kind": "keep"}],
        )

    def test_evaluate_expression_supports_convert_with_dedicated_test(self):
        self.assertEqual(
            evaluate_expression({"value": "10"}, {"$convert": {"input": "$value", "to": "int"}}),
            10,
        )

    def test_evaluate_expression_supports_date_part_extractors(self):
        document = {
            "created_at": datetime.datetime(2026, 3, 29, 22, 5, 6, 789000),
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {"$year": {"date": "$created_at", "timezone": "+02:00"}},
            ),
            2026,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$month": {"date": "$created_at", "timezone": "+02:00"}},
            ),
            3,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dayOfMonth": {"date": "$created_at", "timezone": "+02:00"}},
            ),
            30,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dayOfWeek": {"date": "$created_at", "timezone": "+02:00"}},
            ),
            2,
        )
        self.assertEqual(evaluate_expression(document, {"$dayOfYear": "$created_at"}), 88)
        self.assertEqual(
            evaluate_expression(
                document,
                {"$hour": {"date": "$created_at", "timezone": "+02:00"}},
            ),
            0,
        )
        self.assertEqual(evaluate_expression(document, {"$minute": "$created_at"}), 5)
        self.assertEqual(evaluate_expression(document, {"$second": "$created_at"}), 6)
        self.assertEqual(evaluate_expression(document, {"$millisecond": "$created_at"}), 789)
        self.assertEqual(evaluate_expression(document, {"$isoDayOfWeek": "$created_at"}), 7)

    def test_evaluate_expression_supports_slice_is_array_and_cmp(self):
        document = {
            "values": [1, 2, 3, 4],
            "nested": {"a": 1},
        }

        self.assertEqual(evaluate_expression(document, {"$slice": ["$values", 2]}), [1, 2])
        self.assertEqual(evaluate_expression(document, {"$slice": ["$values", -2]}), [3, 4])
        self.assertEqual(evaluate_expression(document, {"$slice": ["$values", 1, 2]}), [2, 3])
        self.assertTrue(evaluate_expression(document, {"$isArray": "$values"}))
        self.assertFalse(evaluate_expression(document, {"$isArray": "$nested"}))
        self.assertEqual(evaluate_expression(document, {"$cmp": [1, 2]}), -1)
        self.assertEqual(evaluate_expression(document, {"$cmp": [2, 2]}), 0)
        self.assertEqual(evaluate_expression(document, {"$cmp": [3, 2]}), 1)

    def test_evaluate_expression_supports_set_field_with_dedicated_test(self):
        self.assertEqual(
            evaluate_expression({"value": 1}, {"$setField": {"field": "name", "input": "$$ROOT", "value": "Ada"}}),
            {"value": 1, "name": "Ada"},
        )

    def test_evaluate_expression_supports_percentile_and_median_with_dedicated_test(self):
        self.assertEqual(
            evaluate_expression(
                {"value": [1, 2, 3, 4]},
                {"$percentile": {"input": "$value", "p": [0.25, 0.5, 1.0], "method": "approximate"}},
            ),
            [1, 2, 4],
        )
        self.assertEqual(
            evaluate_expression(
                {"value": [1, 2, 3, 4]},
                {"$median": {"input": "$value", "method": "approximate"}},
            ),
            2,
        )

    def test_evaluate_expression_percentile_and_median_reject_invalid_payloads(self):
        with self.assertRaises(OperationFailure):
            evaluate_expression({"value": 10}, {"$percentile": {"input": "$value", "p": [0.5], "method": "exact"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression({"value": 10}, {"$percentile": {"input": "$value", "p": [], "method": "approximate"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression({"value": 10}, {"$median": {"input": "$value"}})

    def test_pipeline_match_supports_nested_expr_inside_nor(self):
        documents = [
            {"_id": "1", "a": 6, "kind": "drop"},
            {"_id": "2", "a": 4, "kind": "keep"},
            {"_id": "3", "a": 8, "kind": "keep"},
        ]

        self.assertEqual(
            apply_pipeline(
                documents,
                [{"$match": {"$nor": [{"$expr": {"$gt": ["$a", 5]}}, {"kind": "drop"}]}}],
            ),
            [{"_id": "2", "a": 4, "kind": "keep"}],
        )

    def test_pipeline_match_rejects_non_list_and_or_payloads_and_filters_false_expr(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": True, "$and": {"a": 1}}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": True, "$or": {"a": 1}}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": True, "$nor": {"a": 1}}}])

        self.assertEqual(
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": {"$gt": ["$a", 5]}}}]),
            [],
        )
        self.assertEqual(
            apply_pipeline(
                [{"_id": "1", "a": 1}],
                [{"$match": {"$expr": True, "$or": [{"a": 2}, {"$expr": False}]}}],
            ),
            [],
        )

    def test_evaluate_expression_rejects_invalid_string_operator_arguments(self):
        document = {"text": "Ada", "number": 7}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$concat": ["$text", "$number"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toLower": "$number"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toUpper": "$number"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$split": ["$text", "$number"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$split": ["$text", ""]})

    def test_evaluate_expression_covers_string_array_and_set_edge_cases(self):
        document = {
            "text": "Ada",
            "tags": ["a", "b"],
            "none_chars": None,
            "number": 7,
        }

        self.assertIsNone(evaluate_expression(document, {"$reverseArray": "$missing"}))
        self.assertIsNone(evaluate_expression(document, {"$allElementsTrue": "$missing"}))
        self.assertIsNone(evaluate_expression(document, {"$setDifference": ["$tags", "$missing"]}))
        self.assertIsNone(evaluate_expression(document, {"$setEquals": ["$tags", "$missing"]}))
        self.assertFalse(evaluate_expression(document, {"$setEquals": [["a"], ["a", "b"]]}))
        self.assertFalse(evaluate_expression(document, {"$setEquals": [["a", "c"], ["a", "b"]]}))
        self.assertFalse(evaluate_expression(document, {"$setIsSubset": [["a", "z"], ["a", "b"]]}))
        self.assertIsNone(evaluate_expression(document, {"$trim": {"input": "$text", "chars": "$none_chars"}}))
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$setIsSubset": [["a"], ["a", "b"], ["a"]]})

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$range": ["$number", 5, 0]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$range": [0, 5, 1.5]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trim": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trim": {"input": "$number"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trim": {"input": "$text", "chars": "$number"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$replaceOne": {"input": "$text", "find": "a"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$replaceAll": {"input": "$text", "find": "$text", "replacement": "$number"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression({"text": "cafétéria"}, {"$substr": ["$text", 3, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substr": ["$text", True, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substr": ["$text", 0, True]})

    def test_set_expressions_preserve_custom_dialect_equality_when_fast_path_is_disabled(self):
        class _CaseInsensitiveDialect(MongoDialect):
            def values_equal(self, left: object, right: object) -> bool:
                if isinstance(left, str) and isinstance(right, str):
                    return left.casefold() == right.casefold()
                return super().values_equal(left, right)

        dialect = _CaseInsensitiveDialect(key="test", server_version="test", label="Case Insensitive")
        document = {"left": ["Ada", "Mongo"], "right": ["ada", "mongo"]}

        self.assertEqual(
            evaluate_expression(document, {"$setIntersection": ["$left", "$right"]}, dialect=dialect),
            ["Ada", "Mongo"],
        )
        self.assertTrue(evaluate_expression(document, {"$setEquals": ["$left", "$right"]}, dialect=dialect))
        self.assertTrue(evaluate_expression(document, {"$setIsSubset": [["ADA"], "$right"]}, dialect=dialect))

    def test_evaluate_expression_supports_cond_object_and_if_null_fallback_to_none(self):
        document = {"score": 2, "bonus": None}

        self.assertEqual(
            evaluate_expression(
                document,
                {"$cond": {"if": {"$gt": ["$score", 5]}, "then": "high", "else": "low"}},
            ),
            "low",
        )
        self.assertIsNone(evaluate_expression(document, {"$ifNull": ["$bonus", None]}))
        self.assertEqual(
            evaluate_expression({"legacy": UNDEFINED}, {"$ifNull": ["$legacy", "fallback"]}),
            "fallback",
        )
        self.assertIsNone(evaluate_expression(document, "$$missing"))
        self.assertIsNone(evaluate_expression(document, "$missing"))
        self.assertIsNone(evaluate_expression(document, {"$toString": None}))
        self.assertEqual(
            evaluate_expression(
                {"profile": {"name": "Ada"}, "fallback": {"city": "Sevilla"}},
                {"$mergeObjects": [None, "$fallback", {"name": {"$getField": {"field": "name", "input": "$profile"}}}]},
            ),
            {"city": "Sevilla", "name": "Ada"},
        )
        self.assertEqual(evaluate_expression({"profile": {"name": "Ada"}}, {"$getField": "profile"}), {"name": "Ada"})
        self.assertIsNone(evaluate_expression({"profile": None}, {"$getField": {"field": "name", "input": "$profile"}}))
        self.assertEqual(
            evaluate_expression(
                {"literal": "$score", "score": 10},
                {"$getField": {"field": "literal"}},
            ),
            "$score",
        )
        self.assertEqual(
            evaluate_expression(
                {"a.b.c": 1, "$price": 2, "x..y": 3},
                {"$getField": {"field": "a.b.c"}},
            ),
            1,
        )
        self.assertEqual(
            evaluate_expression(
                {"a.b.c": 1, "$price": 2, "x..y": 3},
                {"$getField": {"field": {"$literal": "$price"}}},
            ),
            2,
        )
        self.assertEqual(
            evaluate_expression(
                {"a.b.c": 1, "$price": 2, "x..y": 3},
                {"$getField": {"field": "x..y"}},
            ),
            3,
        )
        self.assertIsNone(
            evaluate_expression(
                {"profile": {"name": "Ada"}},
                {"$getField": {"field": "$missing", "input": "$$CURRENT"}},
            )
        )

    def test_pipeline_supports_remove_variable_in_add_fields_and_project(self):
        documents = [{"_id": "1", "keep": 1, "drop": 2}]

        self.assertEqual(
            apply_pipeline(documents, [{"$addFields": {"drop": "$$REMOVE"}}]),
            [{"_id": "1", "keep": 1}],
        )
        self.assertEqual(
            apply_pipeline(documents, [{"$project": {"_id": 0, "keep": 1, "drop": "$$REMOVE"}}]),
            [{"keep": 1}],
        )

    def test_evaluate_expression_supports_array_to_object_index_of_array_and_sort_array(self):
        document = {
            "pairs": [["a", 1], ["b", 2]],
            "kv_pairs": [{"k": "x", "v": 10}, {"k": "y", "v": 20}],
            "numbers": [4, 1, 3, 2],
            "items": [{"rank": 3, "name": "c"}, {"rank": 1, "name": "a"}, {"rank": 2, "name": "b"}],
        }

        self.assertEqual(
            evaluate_expression(document, {"$arrayToObject": "$pairs"}),
            {"a": 1, "b": 2},
        )
        self.assertEqual(
            evaluate_expression(document, {"$arrayToObject": "$kv_pairs"}),
            {"x": 10, "y": 20},
        )
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 3]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 3, 3]}), -1)
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 3, 0, 3]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 9]}), -1)
        self.assertIsNone(evaluate_expression(document, {"$indexOfArray": ["$missing", 9]}))
        self.assertEqual(
            evaluate_expression(document, {"$sortArray": {"input": "$numbers", "sortBy": 1}}),
            [1, 2, 3, 4],
        )
        self.assertEqual(
            evaluate_expression(document, {"$sortArray": {"input": "$numbers", "sortBy": -1}}),
            [4, 3, 2, 1],
        )
        self.assertEqual(
            evaluate_expression(document, {"$sortArray": {"input": "$items", "sortBy": {"rank": 1}}}),
            [{"rank": 1, "name": "a"}, {"rank": 2, "name": "b"}, {"rank": 3, "name": "c"}],
        )
        self.assertIsNone(evaluate_expression(document, {"$sortArray": {"input": "$missing", "sortBy": 1}}))
