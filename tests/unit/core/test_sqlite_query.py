import unittest
import datetime
import uuid
import json
from unittest import mock
from unittest.mock import patch

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.query_plan import (
    AndCondition,
    AllCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExistsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    JsonSchemaCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    NotEqualsCondition,
    NotCondition,
    NotInCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
)
from mongoeco.engines.sqlite_query import (
    _comparison_type_order,
    _normalize_comparable_value,
    _path_crosses_scalar_parent,
    _translate_all_condition,
    _translate_elem_match_condition,
    _translate_json_each_scalar_comparison,
    _translate_json_each_scalar_match,
    _translate_json_each_value_plan,
    _translate_scalar_or_array_same_type_comparison,
    _translate_same_type_comparison,
    _translate_array_contains_scalar,
    _translate_comparison,
    _translate_equals,
    _translate_not_equals,
    _translate_scalar_equals,
    parse_safe_literal_regex,
    index_expressions_sql,
    json_path_for_field,
    path_array_prefixes,
    sort_type_expression_sql,
    translate_query_plan,
    translate_compiled_update_plan,
    translate_sort_spec,
    translate_update_spec,
    type_expression_sql,
    value_expression_sql,
)
from mongoeco.types import ObjectId, UNDEFINED


class SQLiteQueryTranslationTests(unittest.TestCase):
    def test_regex_and_json_each_helpers_cover_safe_and_rejected_shapes(self):
        self.assertEqual(parse_safe_literal_regex("^Ada.*", ""), ("prefix", "Ada", False))
        self.assertEqual(parse_safe_literal_regex("Ada$", "i"), ("suffix", "Ada", True))
        self.assertEqual(parse_safe_literal_regex("Ada", ""), ("contains", "Ada", False))
        self.assertEqual(parse_safe_literal_regex("^Ada$", ""), ("exact", "Ada", False))
        self.assertEqual(parse_safe_literal_regex("Ada\\.", ""), ("contains", "Ada.", False))
        self.assertIsNone(parse_safe_literal_regex("^", ""))
        self.assertIsNone(parse_safe_literal_regex("Ada", "m"))
        self.assertIsNone(parse_safe_literal_regex("Ada\\", ""))
        self.assertIsNone(parse_safe_literal_regex("A.*da", ""))
        self.assertIsNone(parse_safe_literal_regex("^", "i"))

        self.assertEqual(
            _translate_json_each_scalar_match("$.items", None),
            ("EXISTS (SELECT 1 FROM json_each(document, '$.items') WHERE json_each.type = 'null')", []),
        )
        self.assertEqual(
            _translate_json_each_scalar_match("$.items", True),
            (
                "EXISTS (SELECT 1 FROM json_each(document, '$.items') WHERE json_each.type IN ('true', 'false') AND json_each.value = ?)",
                [1],
            ),
        )
        self.assertEqual(
            _translate_json_each_scalar_match("$.items", "Ada"),
            (
                "EXISTS (SELECT 1 FROM json_each(document, '$.items') WHERE json_each.type = 'text' AND json_each.value = ?)",
                ["Ada"],
            ),
        )
        self.assertEqual(
            _translate_json_each_scalar_match("$.items", 3.5),
            (
                "EXISTS (SELECT 1 FROM json_each(document, '$.items') WHERE json_each.type IN ('integer', 'real') AND json_each.value = ?)",
                [3.5],
            ),
        )
        self.assertIsNone(_translate_json_each_scalar_match("$.items", object()))
        with self.assertRaisesRegex(NotImplementedError, "Unsupported comparison value for SQL translation"):
            _translate_json_each_scalar_comparison(">", object())
        with self.assertRaisesRegex(NotImplementedError, "Unsupported comparison value for SQL translation"):
            _translate_json_each_scalar_comparison(">", {"bad": 1})
        with self.assertRaisesRegex(NotImplementedError, "Unsupported comparison value for SQL translation"):
            _translate_json_each_scalar_comparison(">", None)

    def test_all_elem_match_and_type_translation_cover_error_branches(self):
        with self.assertRaisesRegex(NotImplementedError, "Only top-level \\$all paths"):
            _translate_all_condition("items.name", ("Ada",))
        with self.assertRaisesRegex(NotImplementedError, "Empty \\$all values"):
            _translate_all_condition("items", ())
        with self.assertRaisesRegex(NotImplementedError, "operator values"):
            _translate_all_condition("items", ({"$gt": 1},))
        with self.assertRaisesRegex(NotImplementedError, "simple scalar values"):
            _translate_all_condition("items", (ObjectId("0123456789abcdef01234567"),))

        self.assertIn("json_extract(document, '$.items')", _translate_all_condition("items", ("Ada",))[0])
        with self.assertRaisesRegex(NotImplementedError, "Only top-level \\$elemMatch paths"):
            _translate_elem_match_condition("items.value", EqualsCondition("value", 1), wrap_value=True)
        with self.assertRaisesRegex(NotImplementedError, "Only scalar \\$elemMatch shapes"):
            _translate_elem_match_condition("items", None, wrap_value=True)
        with self.assertRaisesRegex(NotImplementedError, "Only scalar \\$elemMatch shapes"):
            _translate_elem_match_condition("items", EqualsCondition("value", 1), wrap_value=False)
        with self.assertRaisesRegex(NotImplementedError, "Unsupported \\$elemMatch scalar predicate"):
            _translate_elem_match_condition("items", MatchAll(), wrap_value=True)

        with self.assertRaises(NotImplementedError):
            translate_query_plan(TypeCondition("value", (19,)))
        with self.assertRaises(NotImplementedError):
            translate_query_plan(TypeCondition("value", ([],)))  # type: ignore[list-item]

    def test_json_each_value_plan_and_array_aware_comparison_cover_additional_paths(self):
        self.assertIsNone(_translate_json_each_value_plan(EqualsCondition("other", 1)))
        self.assertIsNone(_translate_json_each_value_plan(EqualsCondition("value", ObjectId("0123456789abcdef01234567"))))
        self.assertEqual(
            _translate_json_each_value_plan(EqualsCondition("value", None)),
            ("json_each.type = 'null'", []),
        )
        self.assertEqual(
            _translate_json_each_value_plan(RegexCondition("value", "^Ada$", "")),
            ("json_each.type = 'text' AND json_each.value = ?", ["Ada"]),
        )
        self.assertIsNone(_translate_json_each_value_plan(RegexCondition("value", "^Áda$", "i")))
        self.assertIsNone(_translate_json_each_value_plan(RegexCondition("value", "A.*", "")))
        self.assertEqual(
            _translate_json_each_value_plan(GreaterThanCondition("value", "Ada")),
            ("json_each.type = 'text' AND json_each.value > ?", ["Ada"]),
        )
        self.assertEqual(
            _translate_json_each_value_plan(LessThanOrEqualCondition("value", False)),
            ("json_each.type IN ('true', 'false') AND json_each.value <= ?", [0]),
        )
        self.assertEqual(
            _translate_json_each_value_plan(GreaterThanOrEqualCondition("value", 4)),
            ("json_each.type IN ('integer', 'real') AND json_each.value >= ?", [4]),
        )
        self.assertEqual(
            _translate_json_each_value_plan(LessThanCondition("value", 4)),
            ("json_each.type IN ('integer', 'real') AND json_each.value < ?", [4]),
        )
        self.assertEqual(
            _translate_json_each_value_plan(RegexCondition("value", "^Ada", "")),
            ("json_each.type = 'text' AND substr(json_each.value, 1, length(?)) = ?", ["Ada", "Ada"]),
        )
        self.assertEqual(
            _translate_json_each_value_plan(RegexCondition("value", "Ada$", "")),
            ("json_each.type = 'text' AND substr(json_each.value, -length(?)) = ?", ["Ada", "Ada"]),
        )
        self.assertEqual(
            _translate_json_each_value_plan(RegexCondition("value", "Ada", "")),
            ("json_each.type = 'text' AND instr(json_each.value, ?) > 0", ["Ada"]),
        )
        with self.assertRaisesRegex(NotImplementedError, "Unsupported comparison value for SQL translation"):
            _translate_scalar_or_array_same_type_comparison(">", "value", None)
        with self.assertRaisesRegex(NotImplementedError, "Unsupported comparison value for SQL translation"):
            _translate_scalar_or_array_same_type_comparison(">", "value", {"bad": 1})

    def test_sqlite_query_private_translation_helpers_cover_none_fallbacks_and_assert_never(self):
        sql, params = _translate_array_contains_scalar("items", ObjectId("0123456789abcdef01234567"))
        self.assertIn("json_each.value = ?", sql)
        self.assertEqual(params, [ObjectId("0123456789abcdef01234567")])
        self.assertIsNone(_translate_json_each_scalar_comparison(">", ObjectId("0123456789abcdef01234567")))
        self.assertIsNone(_translate_json_each_value_plan(NotCondition(EqualsCondition("value", 1))))
        self.assertIsNone(_translate_json_each_value_plan(RegexCondition("value", "^Ada$", "m")))

        class UnknownPlan(QueryNode):
            pass

        with self.assertRaises(TypeError):
            translate_query_plan(UnknownPlan())

    def test_sqlite_query_private_translation_helpers_cover_remaining_none_and_error_branches(self):
        self.assertIsNone(parse_safe_literal_regex("^$", ""))
        with patch("mongoeco.engines.sqlite_query._translate_json_each_scalar_match", return_value=("bad", [])):
            self.assertIsNone(_translate_json_each_value_plan(EqualsCondition("value", 1)))
        with patch("mongoeco.engines.sqlite_query.parse_safe_literal_regex", return_value=("mystery", "Ada", False)):
            self.assertIsNone(_translate_json_each_value_plan(RegexCondition("value", "Ada", "")))
        with self.assertRaisesRegex(NotImplementedError, "Unsupported array-aware comparison value"):
            _translate_scalar_or_array_same_type_comparison(">", "items", ObjectId("0123456789abcdef01234567"))
    def test_path_array_prefixes_keep_non_numeric_prefixes_before_index_segments(self):
        self.assertEqual(path_array_prefixes("a.0.b.c"), ("a", "a.0.b"))
        self.assertEqual(path_array_prefixes("items.0"), ("items",))

    def test_path_crosses_scalar_parent_detects_scalar_none_list_and_missing_cases(self):
        self.assertFalse(_path_crosses_scalar_parent({"profile": None}, "profile.name"))
        self.assertTrue(_path_crosses_scalar_parent({"profile": 1}, "profile.name"))
        self.assertTrue(_path_crosses_scalar_parent({"items": []}, "items.name.more"))
        self.assertFalse(_path_crosses_scalar_parent({"items": []}, "items.0.name"))
        self.assertFalse(_path_crosses_scalar_parent({"items": [None]}, "items.0.name"))
        self.assertFalse(_path_crosses_scalar_parent({"items": [{"profile": {}}]}, "items.0.profile.name"))
        self.assertTrue(_path_crosses_scalar_parent({"items": [1]}, "items.0.name"))
        self.assertTrue(_path_crosses_scalar_parent(1, "profile.name"))

    def test_json_path_for_field_supports_dot_and_numeric_segments(self):
        self.assertEqual(json_path_for_field("profile.name"), "$.profile.name")
        self.assertEqual(json_path_for_field("items.0.name"), "$.items[0].name")

    def test_translate_match_all(self):
        self.assertEqual(translate_query_plan(MatchAll()), ("1 = 1", []))

    def test_translate_query_plan_covers_type_alias_and_or_paths(self):
        sql, params = translate_query_plan(TypeCondition("value", ("int",)))
        self.assertIn("BETWEEN -2147483648 AND 2147483647", sql)
        self.assertEqual(params, [])

        sql, params = translate_query_plan(OrCondition((EqualsCondition("a", 1), EqualsCondition("b", 2))))
        self.assertIn(" OR ", sql)
        self.assertEqual(params, [1, 1, 2, 2])

    def test_translate_json_schema_condition_is_explicitly_deferred_to_python(self):
        with self.assertRaisesRegex(NotImplementedError, "not yet translated to SQL"):
            translate_query_plan(
                JsonSchemaCondition(
                    {
                        "required": ["name"],
                        "properties": {"name": {"bsonType": "string"}},
                    }
                )
            )

    def test_translate_equals_and_not_equals(self):
        self.assertEqual(
            translate_query_plan(EqualsCondition("name", "Ada")),
            (
                "((json_type(document, '$.name') = 'text' AND json_extract(document, '$.name') = ?) OR (json_type(document, '$.name') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.name') WHERE json_each.type = 'text' AND json_each.value = ?)))",
                ["Ada", "Ada"],
            ),
        )
        self.assertEqual(
            translate_query_plan(EqualsCondition("name", None)),
            (
                "((json_type(document, '$.name') IS NULL OR json_type(document, '$.name') = 'null') OR (json_type(document, '$.name') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.name') WHERE json_each.type = 'null')))",
                [],
            ),
        )
        self.assertEqual(
            translate_query_plan(EqualsCondition("name", None, null_matches_undefined=True)),
            (
                "((json_type(document, '$.name') IS NULL OR json_type(document, '$.name') = 'null' OR COALESCE(json_extract(document, '$.name.\"$mongoeco\".type'), '') = 'undefined') OR (json_type(document, '$.name') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.name') WHERE json_each.type = 'null' OR (json_each.type = 'object' AND COALESCE(json_extract(json_each.value, '$.\"$mongoeco\".type'), '') = 'undefined'))))",
                [],
            ),
        )
        self.assertEqual(
            translate_query_plan(NotEqualsCondition("name", "Ada")),
            (
                "(json_type(document, '$.name') IS NULL OR json_type(document, '$.name') = 'null' OR NOT ((json_type(document, '$.name') = 'text' AND json_extract(document, '$.name') = ?) OR (json_type(document, '$.name') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.name') WHERE json_each.type = 'text' AND json_each.value = ?))))",
                ["Ada", "Ada"],
            ),
        )
        self.assertEqual(
            translate_query_plan(NotEqualsCondition("name", None)),
            (
                "(json_type(document, '$.name') IS NOT NULL AND json_type(document, '$.name') != 'null' AND NOT (json_type(document, '$.name') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.name') WHERE json_each.type = 'null')))",
                [],
            ),
        )
        self.assertEqual(
            translate_query_plan(EqualsCondition("flag", True)),
            (
                "((json_type(document, '$.flag') IN ('true', 'false') AND json_extract(document, '$.flag') = ?) OR (json_type(document, '$.flag') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.flag') WHERE json_each.type IN ('true', 'false') AND json_each.value = ?)))",
                [1, 1],
            ),
        )
        self.assertEqual(
            translate_query_plan(EqualsCondition("count", 1)),
            (
                "((json_type(document, '$.count') IN ('integer', 'real') AND json_extract(document, '$.count') = ?) OR (json_type(document, '$.count') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.count') WHERE json_each.type IN ('integer', 'real') AND json_each.value = ?)))",
                [1, 1],
            ),
        )

    def test_translate_comparison_and_exists(self):
        self.assertEqual(
            translate_query_plan(GreaterThanCondition("age", 18)),
            (
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'binary' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'timestamp' THEN 10 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'regex' THEN 11 ELSE 100 END > ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') > ?))",
                [2, 18],
            ),
        )
        self.assertEqual(
            translate_query_plan(GreaterThanOrEqualCondition("age", 18)),
            (
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'binary' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'timestamp' THEN 10 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'regex' THEN 11 ELSE 100 END > ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') >= ?))",
                [2, 18],
            ),
        )
        self.assertEqual(
            translate_query_plan(LessThanCondition("age", 18)),
            (
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'binary' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'timestamp' THEN 10 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'regex' THEN 11 ELSE 100 END < ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') < ?))",
                [2, 18],
            ),
        )
        self.assertEqual(
            translate_query_plan(LessThanOrEqualCondition("age", 18)),
            (
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'binary' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'timestamp' THEN 10 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'regex' THEN 11 ELSE 100 END < ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') <= ?))",
                [2, 18],
            ),
        )
        self.assertEqual(
            translate_query_plan(ExistsCondition("profile.name", True)),
            ("json_type(document, '$.profile.name') IS NOT NULL", []),
        )
        self.assertEqual(
            translate_query_plan(ExistsCondition("profile.name", False)),
            ("json_type(document, '$.profile.name') IS NULL", []),
        )

    def test_translate_type_and_bitwise_conditions(self):
        sql, params = translate_query_plan(TypeCondition("value", ("number", "string")))
        self.assertIn("json_type(document, '$.value') IN ('integer', 'real')", sql)
        self.assertIn("json_type(document, '$.value') = 'text'", sql)
        self.assertEqual(params, [])

        sql, params = translate_query_plan(TypeCondition("value", ("array",)))
        self.assertEqual(sql, "(json_type(document, '$.value') = 'array')")
        self.assertEqual(params, [])

    def test_translate_type_condition_supports_numeric_codes_and_aliases(self):
        sql, _ = translate_query_plan(TypeCondition("value", (1, 3, 5, 7, 8, 9, 10, 16, 18, "undefined")))

        self.assertIn("json_type(document, '$.value') = 'real'", sql)
        self.assertIn("json_type(document, '$.value') = 'object'", sql)
        self.assertIn("IN ('bytes', 'uuid')", sql)
        self.assertIn("= 'objectid'", sql)
        self.assertIn("json_type(document, '$.value') IN ('true', 'false')", sql)
        self.assertIn("= 'datetime'", sql)
        self.assertIn("json_type(document, '$.value') = 'null'", sql)
        self.assertIn("BETWEEN -2147483648 AND 2147483647", sql)
        self.assertIn("> 2147483647", sql)
        self.assertIn("= 'undefined'", sql)

    def test_translate_type_condition_rejects_non_translatable_specs(self):
        with self.assertRaises(NotImplementedError):
            translate_query_plan(TypeCondition("value", (True,)))
        with self.assertRaises(NotImplementedError):
            translate_query_plan(TypeCondition("value", (11,)))
        with self.assertRaises(NotImplementedError):
            translate_query_plan(TypeCondition("value", ("regex",)))
        with self.assertRaises(NotImplementedError):
            translate_query_plan(TypeCondition("value", ("future",)))

    def test_internal_translation_helpers_cover_string_and_error_branches(self):
        self.assertEqual(_normalize_comparable_value("Ada"), ("string", "Ada"))
        with self.assertRaises(NotImplementedError):
            _normalize_comparable_value(object())

        self.assertEqual(
            _translate_array_contains_scalar("payload", object()),
            (
                "(json_type(document, '$.payload') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.payload') WHERE json_each.value = ?))",
                [mock.ANY],
            ),
        )

        self.assertEqual(
            _translate_scalar_equals("payload", object()),
            (
                "(COALESCE(json_extract(document, '$.payload.\"$mongoeco\".type'), '') = ? AND COALESCE(json_extract(document, '$.payload.\"$mongoeco\".value'), json_extract(document, '$.payload')) = ?)",
                ["", mock.ANY],
            ),
        )

        with self.assertRaises(NotImplementedError):
            _translate_equals("payload", {"a": 1})
        with self.assertRaises(NotImplementedError):
            _translate_not_equals("payload", {"a": 1})

        self.assertEqual(_comparison_type_order("string"), 3)
        with self.assertRaises(NotImplementedError):
            _comparison_type_order("unsupported")

        sql, params = _translate_comparison(">", "name", "Ada")
        self.assertIn("json_type(document, '$.name') = 'text'", sql)
        self.assertEqual(params, [3, "Ada"])

    def test_translate_comparison_rejects_non_comparable_tagged_values(self):
        self.assertEqual(_normalize_comparable_value(UNDEFINED), ("undefined", True))

        with self.assertRaises(NotImplementedError):
            _translate_comparison(">", "payload", {"nested": True})

    def test_sqlite_query_helper_branches_cover_same_type_comparison_type_codes_and_update_fallbacks(self):
        self.assertEqual(_comparison_type_order("uuid"), 6)
        self.assertEqual(_comparison_type_order("objectid"), 7)
        self.assertEqual(_comparison_type_order("datetime"), 9)
        self.assertEqual(_comparison_type_order("timestamp"), 10)
        self.assertEqual(_comparison_type_order("regex"), 11)

        sql, params = _translate_same_type_comparison(">", "name", "Ada")
        self.assertIn("json_type(document, '$.name') = 'text'", sql)
        self.assertEqual(params, ["Ada"])

        sql, params = _translate_same_type_comparison("<", "flag", True)
        self.assertIn("json_type(document, '$.flag') IN ('true', 'false')", sql)
        self.assertEqual(params, [1])

        sql, params = _translate_same_type_comparison(">=", "_id", ObjectId("0123456789abcdef01234567"))
        self.assertIn("type'), '') = ?", sql)
        self.assertEqual(params[0], "objectid")

        sql, _ = translate_query_plan(TypeCondition("value", (2, 4, "double", "object", "array", "binData", "objectId", "bool", "date", "null", "long")))
        self.assertIn("json_type(document, '$.value') = 'text'", sql)
        self.assertIn("json_type(document, '$.value') = 'array'", sql)
        self.assertIn("json_type(document, '$.value') = 'null'", sql)
        self.assertIn("json_type(document, '$.value') = 'real'", sql)

        with mock.patch.object(UpdateEngine, "compile_update_plan", return_value=object()):
            with self.assertRaisesRegex(NotImplementedError, "Aggregation pipeline updates require Python update fallback"):
                translate_update_spec({"$set": {"name": "Ada"}})

        with self.assertRaisesRegex(NotImplementedError, "Unsupported update operator"):
            translate_compiled_update_plan(
                UpdateEngine.compile_update_plan({"$inc": {"rank": 1}})
            )
        with self.assertRaisesRegex(NotImplementedError, "Array index paths require Python update fallback"):
            translate_compiled_update_plan(
                UpdateEngine.compile_update_plan({"$set": {"items.0.name": "Ada"}})
            )
        with self.assertRaisesRegex(NotImplementedError, "Scalar parent requires Python update fallback"):
            translate_compiled_update_plan(
                UpdateEngine.compile_update_plan({"$set": {"profile.name": "Ada"}}),
                current_document={"profile": 1},
            )
        with self.assertRaisesRegex(NotImplementedError, "Array index paths require Python update fallback"):
            translate_compiled_update_plan(
                UpdateEngine.compile_update_plan({"$unset": {"items.0.name": ""}})
            )

        with self.assertRaisesRegex(NotImplementedError, "DBRef special subfields require Python query fallback"):
            translate_query_plan(EqualsCondition("ref.$id", "1"))
        with self.assertRaisesRegex(TypeError, "Unsupported query plan node"):
            translate_query_plan("bad")  # type: ignore[arg-type]

    def test_sort_type_expression_sql_keeps_bytes_out_of_uuid_bucket(self):
        expression = sort_type_expression_sql("payload")

        self.assertNotIn("= 'bytes' THEN 6", expression)
        self.assertIn("= 'binary' THEN 6", expression)
        self.assertIn("= 'uuid' THEN 6", expression)

    def test_translate_membership(self):
        self.assertEqual(
            translate_query_plan(InCondition("role", ("admin", "staff"))),
            (
                "((json_type(document, '$.role') = 'text' AND json_extract(document, '$.role') = ?)) OR (json_type(document, '$.role') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.role') WHERE json_each.type = 'text' AND json_each.value = ?)) OR ((json_type(document, '$.role') = 'text' AND json_extract(document, '$.role') = ?)) OR (json_type(document, '$.role') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.role') WHERE json_each.type = 'text' AND json_each.value = ?))",
                ["admin", "admin", "staff", "staff"],
            ),
        )
        self.assertEqual(
            translate_query_plan(NotInCondition("role", ("admin", "staff"))),
            (
                "(json_type(document, '$.role') IS NULL OR json_type(document, '$.role') = 'null' OR NOT (((json_type(document, '$.role') = 'text' AND json_extract(document, '$.role') = ?)) OR (json_type(document, '$.role') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.role') WHERE json_each.type = 'text' AND json_each.value = ?)) OR ((json_type(document, '$.role') = 'text' AND json_extract(document, '$.role') = ?)) OR (json_type(document, '$.role') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.role') WHERE json_each.type = 'text' AND json_each.value = ?))))",
                ["admin", "admin", "staff", "staff"],
            ),
        )
        self.assertEqual(
            translate_query_plan(InCondition("role", (None,), null_matches_undefined=True)),
            (
                "((json_type(document, '$.role') IS NULL OR json_type(document, '$.role') = 'null' OR COALESCE(json_extract(document, '$.role.\"$mongoeco\".type'), '') = 'undefined')) OR (json_type(document, '$.role') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.role') WHERE json_each.type = 'null' OR (json_each.type = 'object' AND COALESCE(json_extract(json_each.value, '$.\"$mongoeco\".type'), '') = 'undefined')))",
                [],
            ),
        )

    def test_translate_codec_aware_equals(self):
        object_id = ObjectId("0123456789abcdef01234567")
        created_at = datetime.datetime(2025, 1, 2, 3, 4, 5)
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")

        sql, params = translate_query_plan(EqualsCondition("_id", object_id))
        self.assertIn("json_each.type = 'object'", sql)
        self.assertEqual(params, ["objectid", "0123456789abcdef01234567", "objectid", "0123456789abcdef01234567"])

        sql, params = translate_query_plan(EqualsCondition("created_at", created_at))
        self.assertIn("json_each.type = 'object'", sql)
        self.assertEqual(params, ["datetime", "2025-01-02T03:04:05", "datetime", "2025-01-02T03:04:05"])

        sql, params = translate_query_plan(EqualsCondition("session_id", session_id))
        self.assertIn("json_each.type = 'object'", sql)
        self.assertEqual(params, ["uuid", "12345678-1234-5678-1234-567812345678", "uuid", "12345678-1234-5678-1234-567812345678"])

        sql, params = translate_query_plan(EqualsCondition("legacy", UNDEFINED))
        self.assertIn("json_each.type = 'object'", sql)
        self.assertEqual(params, ["undefined", True, "undefined", True])

        sql, params = translate_query_plan(NotEqualsCondition("_id", object_id))
        self.assertIn("NOT (", sql)
        self.assertIn("json_each.type = 'object'", sql)
        self.assertEqual(params, ["objectid", "0123456789abcdef01234567", "objectid", "0123456789abcdef01234567"])



    def test_translate_codec_aware_membership(self):
        values = (
            ObjectId("0123456789abcdef01234567"),
            ObjectId("abcdef0123456789abcdef01"),
        )

        sql, params = translate_query_plan(InCondition("_id", values))
        self.assertEqual(sql.count("json_each.type = 'object'"), 2)
        self.assertEqual(
            params,
            [
                "objectid",
                "0123456789abcdef01234567",
                "objectid",
                "0123456789abcdef01234567",
                "objectid",
                "abcdef0123456789abcdef01",
                "objectid",
                "abcdef0123456789abcdef01",
            ],
        )

        sql, params = translate_query_plan(NotInCondition("_id", values))
        self.assertIn("NOT (", sql)
        self.assertEqual(sql.count("json_each.type = 'object'"), 2)
        self.assertEqual(
            params,
            [
                "objectid",
                "0123456789abcdef01234567",
                "objectid",
                "0123456789abcdef01234567",
                "objectid",
                "abcdef0123456789abcdef01",
                "objectid",
                "abcdef0123456789abcdef01",
            ],
        )



    def test_translate_logical_nodes(self):
        plan = AndCondition(
            (
                EqualsCondition("name", "Ada"),
                OrCondition((ExistsCondition("role", True), EqualsCondition("kind", "user"))),
            )
        )

        sql, params = translate_query_plan(plan)

        self.assertEqual(
            sql,
            "(((json_type(document, '$.name') = 'text' AND json_extract(document, '$.name') = ?) OR (json_type(document, '$.name') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.name') WHERE json_each.type = 'text' AND json_each.value = ?)))) AND ((json_type(document, '$.role') IS NOT NULL) OR (((json_type(document, '$.kind') = 'text' AND json_extract(document, '$.kind') = ?) OR (json_type(document, '$.kind') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.kind') WHERE json_each.type = 'text' AND json_each.value = ?)))))",
        )
        self.assertEqual(params, ["Ada", "Ada", "user", "user"])

    def test_translate_not_condition(self):
        self.assertEqual(
            translate_query_plan(NotCondition(EqualsCondition("name", "Ada"))),
            (
                "NOT COALESCE((((json_type(document, '$.name') = 'text' AND json_extract(document, '$.name') = ?) OR (json_type(document, '$.name') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.name') WHERE json_each.type = 'text' AND json_each.value = ?)))), 0)",
                ["Ada", "Ada"],
            ),
        )

    def test_index_expressions_match_normalized_type_and_value_sql(self):
        self.assertEqual(
            index_expressions_sql("session_id"),
            (
                "COALESCE(json_extract(document, '$.session_id.\"$mongoeco\".type'), '')",
                "COALESCE(json_extract(document, '$.session_id.\"$mongoeco\".value'), json_extract(document, '$.session_id'))",
            ),
        )
        self.assertEqual(type_expression_sql("session_id"), index_expressions_sql("session_id")[0])
        self.assertEqual(value_expression_sql("session_id"), index_expressions_sql("session_id")[1])

    def test_translate_sort_spec(self):
        self.assertEqual(
            translate_sort_spec([("rank", 1), ("name", -1)]),
            " ORDER BY CASE WHEN json_type(document, '$.rank') IS NULL OR json_type(document, '$.rank') = 'null' THEN 1 WHEN json_type(document, '$.rank') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.rank') = 'text' THEN 3 WHEN json_type(document, '$.rank') = 'object' AND json_extract(document, '$.rank.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.rank') = 'array' THEN 5 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'binary' THEN 6 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.rank') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'datetime' THEN 9 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'timestamp' THEN 10 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'regex' THEN 11 ELSE 100 END ASC, COALESCE(json_extract(document, '$.rank.\"$mongoeco\".value'), json_extract(document, '$.rank')) ASC, CASE WHEN json_type(document, '$.name') IS NULL OR json_type(document, '$.name') = 'null' THEN 1 WHEN json_type(document, '$.name') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.name') = 'text' THEN 3 WHEN json_type(document, '$.name') = 'object' AND json_extract(document, '$.name.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.name') = 'array' THEN 5 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'binary' THEN 6 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.name') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'datetime' THEN 9 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'timestamp' THEN 10 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'regex' THEN 11 ELSE 100 END DESC, COALESCE(json_extract(document, '$.name.\"$mongoeco\".value'), json_extract(document, '$.name')) DESC",
        )

    def test_sort_type_expression_sql_tracks_bson_scalar_brackets(self):
        self.assertIn("THEN 1", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 2", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 3", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 4", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 5", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 6", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 7", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 8", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 9", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 10", sort_type_expression_sql("mixed"))
        self.assertIn("THEN 11", sort_type_expression_sql("mixed"))

    def test_translate_codec_aware_comparison(self):
        created_at = datetime.datetime(2025, 1, 2, 3, 4, 5)
        object_id = ObjectId("0123456789abcdef01234567")

        sql, params = translate_query_plan(GreaterThanCondition("created_at", created_at))
        self.assertIn("CASE WHEN json_type(document, '$.created_at') IS NULL", sql)
        self.assertIn("'datetime'", sql)
        self.assertEqual(params, [9, "datetime", "2025-01-02T03:04:05"])

        sql, params = translate_query_plan(LessThanCondition("_id", object_id))
        self.assertIn("CASE WHEN json_type(document, '$._id') IS NULL", sql)
        self.assertIn("'objectid'", sql)
        self.assertEqual(params, [7, "objectid", "0123456789abcdef01234567"])



    def test_translate_update_spec(self):
        sql, params = translate_update_spec({"$set": {"name": "Ada"}, "$unset": {"rank": ""}})

        self.assertEqual(sql, "json_remove(json_set(document, '$.name', json(?)), '$.rank')")
        self.assertEqual(params, [json.dumps("Ada")])

        nested_sql, nested_params = translate_update_spec({"$set": {"profile.name": "Ada"}, "$unset": {"profile.age": ""}})
        self.assertEqual(
            nested_sql,
            "json_remove(json_set(document, '$.profile.name', json(?)), '$.profile.age')",
        )
        self.assertEqual(nested_params, [json.dumps("Ada")])

        ordered_sql, ordered_params = translate_update_spec(
            {"$set": {"profile": {"z": 1, "a": 2}}},
        )
        self.assertEqual(ordered_sql, "json_set(document, '$.profile', json(?))")
        self.assertEqual(
            ordered_params,
            [json.dumps(DocumentCodec.encode({"z": 1, "a": 2}), separators=(",", ":"), sort_keys=False)],
        )

        with self.assertRaises(NotImplementedError):
            translate_update_spec({"$set": {"profile.name": "Ada"}}, current_document={"profile": 1})

        compiled_sql, compiled_params = translate_compiled_update_plan(
            UpdateEngine.compile_update_plan({"$set": {"name": "Ada"}, "$unset": {"rank": ""}})
        )
        self.assertEqual(compiled_sql, sql)
        self.assertEqual(compiled_params, params)

    def test_translate_update_spec_rejects_unsupported_payloads_and_paths(self):
        with self.assertRaises(NotImplementedError):
            translate_update_spec({"$set": "bad"})

        with self.assertRaises(NotImplementedError):
            translate_update_spec({"$set": {1: "Ada"}})

        with self.assertRaises(NotImplementedError):
            translate_update_spec({"$unset": "bad"})

        with self.assertRaises(NotImplementedError):
            translate_update_spec({"$unset": {1: ""}})

        with self.assertRaises(NotImplementedError):
            translate_update_spec({"$set": {"tags.0": "Ada"}})

        with self.assertRaises(NotImplementedError):
            translate_update_spec({"$unset": {"tags.0": ""}})

    def test_translate_rejects_unsupported_values(self):
        with self.assertRaises(NotImplementedError):
            translate_query_plan(EqualsCondition("data", {"a": 1}))

        with self.assertRaises(NotImplementedError):
            translate_query_plan(NotEqualsCondition("data", {"a": 1}))

        sql, params = translate_query_plan(GreaterThanCondition("flag", True))
        self.assertIn("json_type(document, '$.flag') IN ('true', 'false')", sql)
        self.assertEqual(params, [8, 1])

        with self.assertRaises(NotImplementedError):
            translate_query_plan(GreaterThanCondition("data", {"$mongoeco": {"type": "x", "value": "y"}}))

        sql, params = translate_query_plan(InCondition("value", (None,)))
        self.assertIn("json_each.type = 'null'", sql)
        self.assertEqual(params, [])
        with self.assertRaises(NotImplementedError):
            translate_query_plan(InCondition("value", (object(),)))


    def test_translate_rejects_unknown_node(self):
        class UnknownPlan(QueryNode):
            pass

        with self.assertRaises(TypeError):
            translate_query_plan(UnknownPlan())
