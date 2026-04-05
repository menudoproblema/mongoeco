import unittest

from mongoeco.core.query_plan import DeferredQueryNode, QueryNode, compile_filter
from mongoeco.core.sql_translation import BaseSQLTranslator
from mongoeco.engines.sqlite_query import HANDLED_SQL_QUERY_NODE_TYPES, SQLiteQueryTranslator
from mongoeco.types import PlanningIssue


class _MinimalTranslator(BaseSQLTranslator):
    def translate_query_plan(self, plan):
        del plan
        return "1 = 1", []

    def translate_sort_spec(self, sort):
        del sort
        return ""


class SqlTranslationTests(unittest.TestCase):
    def test_sqlite_query_translator_dispatch_covers_all_concrete_query_nodes(self):
        def _collect_concrete_subclasses(base: type[QueryNode]) -> set[type[QueryNode]]:
            concrete: set[type[QueryNode]] = set()
            for subclass in base.__subclasses__():
                if subclass.__module__ == "mongoeco.core.query_plan":
                    concrete.add(subclass)
                concrete.update(_collect_concrete_subclasses(subclass))
            return concrete

        self.assertEqual(
            set(HANDLED_SQL_QUERY_NODE_TYPES),
            _collect_concrete_subclasses(QueryNode),
        )

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

    def test_base_sql_translator_build_select_statement_requires_plan_or_fragment(self):
        translator = _MinimalTranslator()
        with self.assertRaisesRegex(ValueError, "requires plan or where_fragment"):
            translator.build_select_statement(
                select_clause="document",
                from_clause="documents",
                namespace_sql="db_name = ?",
                namespace_params=("db",),
            )

    def test_base_sql_translator_builds_offset_only_statement(self):
        translator = _MinimalTranslator()
        statement = translator.build_select_statement(
            select_clause="document",
            from_clause="documents",
            namespace_sql="db_name = ?",
            namespace_params=("db",),
            plan=compile_filter({"name": "Ada"}),
            skip=3,
        )
        self.assertIn("LIMIT -1", statement.sql)
        self.assertIn("OFFSET ?", statement.sql)
        self.assertEqual(statement.params[-1], 3)

    def test_sqlite_query_translator_translates_simple_query_plan(self):
        translator = SQLiteQueryTranslator()

        sql, params = translator.translate_query_plan(
            compile_filter({"name": "Ada"}),
        )

        self.assertIn("json_extract", sql)
        self.assertEqual(params[-1], "Ada")

    def test_sqlite_query_translator_rejects_deferred_query_nodes_explicitly(self):
        translator = SQLiteQueryTranslator()

        with self.assertRaises(NotImplementedError):
            translator.translate_query_plan(
                DeferredQueryNode(PlanningIssue(scope="query", message="deferred")),
            )
