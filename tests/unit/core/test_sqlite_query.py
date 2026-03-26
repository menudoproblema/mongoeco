import unittest
import datetime
import uuid
import json

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.query_plan import (
    AndCondition,
    AllCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExistsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
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
    _translate_array_contains_scalar,
    _translate_comparison,
    _translate_equals,
    _translate_not_equals,
    _translate_scalar_equals,
    index_expressions_sql,
    json_path_for_field,
    sort_type_expression_sql,
    translate_query_plan,
    translate_sort_spec,
    translate_update_spec,
    type_expression_sql,
    value_expression_sql,
)
from mongoeco.types import ObjectId, UNDEFINED


class SQLiteQueryTranslationTests(unittest.TestCase):
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
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 ELSE 100 END > ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') > ?))",
                [2, 18],
            ),
        )
        self.assertEqual(
            translate_query_plan(GreaterThanOrEqualCondition("age", 18)),
            (
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 ELSE 100 END > ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') >= ?))",
                [2, 18],
            ),
        )
        self.assertEqual(
            translate_query_plan(LessThanCondition("age", 18)),
            (
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 ELSE 100 END < ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') < ?))",
                [2, 18],
            ),
        )
        self.assertEqual(
            translate_query_plan(LessThanOrEqualCondition("age", 18)),
            (
                "((CASE WHEN json_type(document, '$.age') IS NULL THEN NULL WHEN json_type(document, '$.age') = 'null' THEN 1 WHEN json_type(document, '$.age') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.age') = 'text' THEN 3 WHEN json_type(document, '$.age') = 'object' AND json_extract(document, '$.age.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.age') = 'array' THEN 5 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.age') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.age.\"$mongoeco\".type') = 'datetime' THEN 9 ELSE 100 END < ?) OR (json_type(document, '$.age') IN ('integer', 'real') AND json_extract(document, '$.age') <= ?))",
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

    def test_internal_translation_helpers_cover_string_and_error_branches(self):
        self.assertEqual(_normalize_comparable_value("Ada"), ("string", "Ada"))
        with self.assertRaises(NotImplementedError):
            _normalize_comparable_value(object())

        self.assertEqual(
            _translate_array_contains_scalar("payload", object()),
            (
                "(json_type(document, '$.payload') = 'array' AND EXISTS (SELECT 1 FROM json_each(document, '$.payload') WHERE json_each.value = ?))",
                [unittest.mock.ANY],
            ),
        )

        self.assertEqual(
            _translate_scalar_equals("payload", object()),
            (
                "(COALESCE(json_extract(document, '$.payload.\"$mongoeco\".type'), '') = ? AND COALESCE(json_extract(document, '$.payload.\"$mongoeco\".value'), json_extract(document, '$.payload')) = ?)",
                ["", unittest.mock.ANY],
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

    def test_sort_type_expression_sql_keeps_bytes_out_of_uuid_bucket(self):
        expression = sort_type_expression_sql("payload")

        self.assertNotIn("= 'bytes' THEN 6", expression)
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
            " ORDER BY CASE WHEN json_type(document, '$.rank') IS NULL OR json_type(document, '$.rank') = 'null' THEN 1 WHEN json_type(document, '$.rank') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.rank') = 'text' THEN 3 WHEN json_type(document, '$.rank') = 'object' AND json_extract(document, '$.rank.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.rank') = 'array' THEN 5 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.rank') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.rank.\"$mongoeco\".type') = 'datetime' THEN 9 ELSE 100 END ASC, COALESCE(json_extract(document, '$.rank.\"$mongoeco\".value'), json_extract(document, '$.rank')) ASC, CASE WHEN json_type(document, '$.name') IS NULL OR json_type(document, '$.name') = 'null' THEN 1 WHEN json_type(document, '$.name') IN ('integer', 'real') THEN 2 WHEN json_type(document, '$.name') = 'text' THEN 3 WHEN json_type(document, '$.name') = 'object' AND json_extract(document, '$.name.\"$mongoeco\".type') IS NULL THEN 4 WHEN json_type(document, '$.name') = 'array' THEN 5 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'uuid' THEN 6 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'objectid' THEN 7 WHEN json_type(document, '$.name') IN ('true', 'false') THEN 8 WHEN json_extract(document, '$.name.\"$mongoeco\".type') = 'datetime' THEN 9 ELSE 100 END DESC, COALESCE(json_extract(document, '$.name.\"$mongoeco\".value'), json_extract(document, '$.name')) DESC",
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
