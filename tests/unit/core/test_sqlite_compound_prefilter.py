import unittest

from mongoeco.core.search import compile_search_stage, search_query_operator_name
from mongoeco.engines._sqlite_compound_prefilter import (
    compound_clause_candidate_state,
    compound_entry_ranking_supported,
    describe_compound_prefilter,
    document_for_search_path,
    downstream_filter_implies_clause,
    sqlite_compound_candidate_plan,
    textual_search_field_types,
)
from mongoeco.types import SearchIndexDefinition


class SQLiteCompoundPrefilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "body": {"type": "autocomplete"},
                        "kind": {"type": "token"},
                        "meta": {
                            "type": "document",
                            "fields": {
                                "summary": {"type": "string"},
                                "ignored": [],
                            },
                        },
                    },
                }
            },
            name="by_text",
            index_type="search",
        )

    def test_sqlite_compound_candidate_plan_covers_empty_and_exclusion_paths(self) -> None:
        compound = compile_search_stage(
            "$search",
            {
                "index": "by_text",
                "compound": {
                    "must": [{"text": {"query": "ada", "path": "title"}}],
                    "should": [{"phrase": {"query": "Ada", "path": "title"}}],
                    "mustNot": [{"exists": {"path": "body"}}],
                    "minimumShouldMatch": 1,
                },
            },
        )

        def resolver(clause):
            operator = search_query_operator_name(clause)
            if operator == "text":
                return ["a", "b"], "fts5", True
            if operator == "phrase":
                return ["b"], "fts5", True
            if operator == "exists":
                return ["b"], "fts5", False
            return None, None, False

        plan = sqlite_compound_candidate_plan(
            self.definition,
            compound,
            candidate_resolver=resolver,
        )
        self.assertEqual(plan.candidates, ("b",))
        self.assertFalse(plan.exact)

        impossible = compile_search_stage(
            "$search",
            {
                "index": "by_text",
                "compound": {
                    "should": [{"text": {"query": "ada", "path": "title"}}],
                    "minimumShouldMatch": 1,
                },
            },
        )
        no_candidates = sqlite_compound_candidate_plan(
            self.definition,
            impossible,
            candidate_resolver=lambda _clause: (None, None, False),
        )
        self.assertIsNone(no_candidates.candidates)
        self.assertFalse(no_candidates.exact)

    def test_clause_state_and_description_cover_downstream_refinements(self) -> None:
        clause = compile_search_stage("$search", {"index": "by_text", "text": {"query": "ada", "path": "title"}})
        state = compound_clause_candidate_state(
            definition=self.definition,
            clause=clause,
            candidate_resolver=lambda _clause: (["a", "b"], "fts5", True),
            downstream_filter_storage_keys=["b"],
            downstream_filter_spec={"title": "Ada algorithms"},
            downstream_filter_exact=True,
        )
        storage_keys, backend, exact, refinement = state
        self.assertEqual(storage_keys, ["b"])
        self.assertEqual(backend, "fts5+downstream-filter")
        self.assertTrue(exact)
        self.assertIsNotNone(refinement)
        assert refinement is not None
        self.assertEqual(refinement.path, "title")

        query = compile_search_stage(
            "$search",
            {
                "index": "by_text",
                "compound": {
                    "must": [{"text": {"query": "ada", "path": "title"}}],
                    "should": [
                        {"exists": {"path": "title"}},
                        {"autocomplete": {"query": "alg", "path": "body"}},
                    ],
                },
            },
        )
        metadata = describe_compound_prefilter(
            self.definition,
            query,
            candidate_resolver=lambda clause: (
                ["a"],
                search_query_operator_name(clause),
                search_query_operator_name(clause) != "autocomplete",
            ),
            physical_name="fts_table",
            downstream_filter={"spec": {"title": "Ada algorithms"}, "exact": True},
            downstream_filter_storage_keys=["a"],
        )
        self.assertEqual(metadata["candidateableShouldCount"], 2)
        self.assertEqual(metadata["requiredCandidateableShould"], 0)
        self.assertEqual(metadata["partialRanking"]["strategy"], "fts-materialized-entries")
        self.assertEqual(metadata["clauseClasses"]["should"][0], "candidateable-exact")
        self.assertEqual(metadata["clauseClasses"]["should"][1], "candidateable-ranking")

    def test_textual_field_types_and_downstream_filter_implication_cover_invalid_shapes(self) -> None:
        self.assertEqual(
            textual_search_field_types(
                SearchIndexDefinition({"mappings": {"fields": []}}, name="bad", index_type="search")
            ),
            {},
        )
        self.assertEqual(
            textual_search_field_types(
                SearchIndexDefinition({"mappings": "bad"}, name="bad", index_type="search")
            ),
            {},
        )
        self.assertEqual(
            textual_search_field_types(self.definition),
            {
                "title": "string",
                "body": "autocomplete",
                "kind": "token",
                "meta.summary": "string",
            },
        )

        exists_clause = compile_search_stage("$search", {"index": "by_text", "exists": {"path": "title"}})
        implied_path, implied_values = downstream_filter_implies_clause(
            exists_clause,
            definition=self.definition,
            filter_spec={"title": {"$exists": True}},
        )
        self.assertEqual(implied_path, "title")
        self.assertEqual(len(implied_values or []), 1)

        near_clause = compile_search_stage("$search", {"index": "by_text", "near": {"path": "score", "origin": 5, "pivot": 2}})
        self.assertEqual(
            downstream_filter_implies_clause(
                near_clause,
                definition=self.definition,
                filter_spec={"title": "Ada algorithms"},
            ),
            (None, None),
        )
        self.assertEqual(
            downstream_filter_implies_clause(
                exists_clause,
                definition=self.definition,
                filter_spec={"title": {"$in": [1]}},
            ),
            (None, None),
        )

        self.assertEqual(document_for_search_path("meta.summary", "ada"), {"meta": {"summary": "ada"}})
        self.assertTrue(compound_entry_ranking_supported(
            compile_search_stage(
                "$search",
                {"index": "by_text", "compound": {"should": [{"exists": {"path": "title"}}]}},
            ),
            physical_name="fts_table",
        ))
        self.assertFalse(compound_entry_ranking_supported(
            compile_search_stage(
                "$search",
                {"index": "by_text", "compound": {"should": [{"near": {"path": "score", "origin": 5, "pivot": 1}}]}},
            ),
            physical_name="fts_table",
        ))
