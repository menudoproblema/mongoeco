import unittest

from mongoeco.core.query_plan import compile_filter
from mongoeco.core.sql_translation import BaseSQLTranslator
from mongoeco.engines.sqlite_query import SQLiteQueryTranslator


class _MinimalTranslator(BaseSQLTranslator):
    def translate_query_plan(self, plan):
        del plan
        return "1 = 1", []

    def translate_sort_spec(self, sort):
        del sort
        return ""


class SqlTranslationTests(unittest.TestCase):
    def test_base_sql_translator_builds_select_statement(self):
        translator = _MinimalTranslator()

        statement = translator.build_select_statement(
            select_clause="document",
            from_clause="documents",
            namespace_sql="db_name = ? AND coll_name = ?",
            namespace_params=("db", "coll"),
            where_fragment=("1 = 1", []),
            skip=2,
            limit=5,
        )

        self.assertIn("SELECT document", statement.sql)
        self.assertIn("WHERE db_name = ? AND coll_name = ? AND (1 = 1)", statement.sql)
        self.assertEqual(statement.params, ("db", "coll", 5, 2))

    def test_sqlite_query_translator_translates_simple_query_plan(self):
        translator = SQLiteQueryTranslator()

        sql, params = translator.translate_query_plan(
            compile_filter({"name": "Ada"}),
        )

        self.assertIn("json_extract", sql)
        self.assertEqual(params[-1], "Ada")
