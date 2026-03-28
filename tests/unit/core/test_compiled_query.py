import unittest

from mongoeco.core.compiled_query import CompiledQuery
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import compile_filter
from mongoeco.engines.semantic_core import compile_find_semantics, iter_filtered_documents


class CompiledQueryTests(unittest.TestCase):
    def test_compiled_query_matches_query_engine_for_top_level_array_equality_and_in(self):
        document = {"tags": ["python", "mongodb"], "scores": [1, 2]}

        equality_plan = compile_filter({"tags": "python"})
        in_plan = compile_filter({"tags": {"$in": ["python"]}})
        range_plan = compile_filter({"scores": {"$gte": 2}})

        self.assertEqual(
            QueryEngine.match_plan(document, equality_plan),
            CompiledQuery(equality_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, in_plan),
            CompiledQuery(in_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, range_plan),
            CompiledQuery(range_plan).match(document),
        )

    def test_compiled_query_matches_query_engine_for_missing_field_semantics(self):
        document = {}

        ne_plan = compile_filter({"value": {"$ne": None}})
        nin_plan = compile_filter({"value": {"$nin": [None]}})

        self.assertEqual(
            QueryEngine.match_plan(document, ne_plan),
            CompiledQuery(ne_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, nin_plan),
            CompiledQuery(nin_plan).match(document),
        )

    def test_iter_filtered_documents_uses_compiled_query_without_changing_results(self):
        semantics = compile_find_semantics({"tags": {"$in": ["python"]}})
        documents = [
            {"_id": 1, "tags": ["python", "mongo"]},
            {"_id": 2, "tags": ["sql"]},
        ]

        self.assertIsNotNone(semantics.compiled_query)
        self.assertEqual(
            list(iter_filtered_documents(documents, semantics)),
            [{"_id": 1, "tags": ["python", "mongo"]}],
        )
