import sqlite3
import unittest

from mongoeco.core.search import materialize_search_document, compile_search_stage
from mongoeco.engines._sqlite_compound_ranking import (
    CompoundTopKPrefilter,
    exact_candidateable_should_scores,
    load_materialized_search_documents_by_storage_key,
    load_search_entries_by_storage_key,
    materialized_search_document_from_entries,
    prefilter_candidate_storage_keys_by_matched_should,
    prune_candidate_storage_keys_for_topk,
    prune_candidate_storage_keys_with_candidateable_ranking,
    rank_compound_candidate_storage_keys_from_entries,
    sort_search_documents_for_query,
)
from mongoeco.types import SearchIndexDefinition


class _FakeRankingEngine:
    def __init__(self) -> None:
        self._materialized_search_entry_cache = {}

    def _quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def _search_backend_version(self, db_name: str, coll_name: str) -> int:
        return 1


class SQLiteCompoundRankingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = _FakeRankingEngine()
        self.conn = sqlite3.connect(":memory:")
        self.physical_name = "fts_entries"
        self.conn.execute(
            f'CREATE TABLE "{self.physical_name}" (storage_key TEXT, field_path TEXT, content TEXT)'
        )
        self.conn.executemany(
            f'INSERT INTO "{self.physical_name}" (storage_key, field_path, content) VALUES (?, ?, ?)',
            [
                ("a", "title", "Ada algorithms"),
                ("a", "body", "vector compiler"),
                ("b", "title", "Grace notes"),
                ("b", "body", "vector systems"),
                ("c", "title", "Misc"),
            ],
        )
        self.definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "body": {"type": "autocomplete"},
                    },
                }
            },
            name="by_text",
            index_type="search",
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_prefilter_helpers_cover_empty_and_candidateable_ranking_paths(self) -> None:
        self.assertEqual(
            CompoundTopKPrefilter(applied=True, before_count=4, after_count=2, strategy="stable-prefix").to_dict()["strategy"],
            "stable-prefix",
        )
        self.assertEqual(prefilter_candidate_storage_keys_by_matched_should(["a", "b"], [], result_limit_hint=2), (None, None))
        self.assertIsNone(prune_candidate_storage_keys_with_candidateable_ranking(["a", "b"], [], result_limit_hint=1))

        pruned, cutoff = prune_candidate_storage_keys_with_candidateable_ranking(
            ["a", "b", "c"],
            [["a", "b"], ["a"], ["b", "c"]],
            result_limit_hint=1,
        )
        self.assertEqual(pruned, ["a", "b"])
        self.assertEqual(cutoff, 2)

    def test_exact_scores_loading_and_sorting_cover_supported_and_fallback_paths(self) -> None:
        query = compile_search_stage(
            "$search",
            {
                "index": "by_text",
                "compound": {
                    "should": [
                        {"text": {"query": "ada", "path": "title"}},
                        {"autocomplete": {"query": "vec", "path": "body"}},
                    ]
                },
            },
        )
        self.assertIsNone(
            exact_candidateable_should_scores(
                self.engine,
                self.conn,
                db_name="db",
                coll_name="coll",
                physical_name=None,
                query=query,
                candidate_storage_keys=["a"],
            )
        )
        scores = exact_candidateable_should_scores(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            query=query,
            candidate_storage_keys=["a", "b", "missing"],
        )
        assert scores is not None
        self.assertGreaterEqual(scores["a"]["matchedShould"], 1.0)
        self.assertIn("missing", scores)
        self.conn.execute(f'DELETE FROM "{self.physical_name}"')
        cached_scores = exact_candidateable_should_scores(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            query=query,
            candidate_storage_keys=["a"],
        )
        assert cached_scores is not None
        self.assertEqual(cached_scores["a"], scores["a"])

        self.assertEqual(load_search_entries_by_storage_key(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            storage_keys=[],
        ), {})
        loaded = load_materialized_search_documents_by_storage_key(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            storage_keys=["a", "b"],
        )
        self.assertEqual(set(loaded), {"a", "b"})
        cached_again = load_search_entries_by_storage_key(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            storage_keys=["a"],
        )
        self.assertEqual(set(cached_again), {"a"})

        empty_materialized = materialized_search_document_from_entries(())
        self.assertEqual(empty_materialized.entries, ())

        near_query = compile_search_stage("$search", {"index": "by_text", "near": {"path": "score", "origin": 5, "pivot": 2}})
        docs = [
            ({"score": 7}, self.definition, None),
            ({"score": 5}, self.definition, None),
        ]
        self.assertEqual(sort_search_documents_for_query(docs, query=near_query)[0]["score"], 5)

        prepared_a = materialize_search_document({"title": "Ada algorithms", "body": "vector compiler"}, self.definition)
        prepared_b = materialize_search_document({"title": "Grace notes", "body": "vector systems"}, self.definition)
        ranked = sort_search_documents_for_query(
            [
                ({"_id": "a", "title": "Ada algorithms", "body": "vector compiler"}, self.definition, prepared_a),
                ({"_id": "b", "title": "Grace notes", "body": "vector systems"}, self.definition, prepared_b),
            ],
            query=query,
        )
        self.assertEqual(ranked[0]["_id"], "a")

    def test_topk_pruning_and_entry_ranking_cover_multiple_strategies(self) -> None:
        text_query = compile_search_stage("$search", {"index": "by_text", "text": {"query": "ada", "path": "title"}})
        stable_pruned, stable_meta = prune_candidate_storage_keys_for_topk(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            query=text_query,
            candidate_storage_keys=["a", "b", "c"],
            candidate_exact=True,
            result_limit_hint=1,
            candidate_resolver=lambda clause: (["a"], "fts5", True),
        )
        self.assertEqual(stable_pruned, ["a"])
        self.assertEqual(stable_meta["strategy"], "stable-prefix")

        self.assertEqual(
            prune_candidate_storage_keys_for_topk(
                self.engine,
                self.conn,
                db_name="db",
                coll_name="coll",
                physical_name=self.physical_name,
                query=text_query,
                candidate_storage_keys=["a", "b"],
                candidate_exact=False,
                result_limit_hint=1,
                candidate_resolver=lambda clause: (["a"], "fts5", True),
            ),
            (["a", "b"], None),
        )

        compound = compile_search_stage(
            "$search",
            {
                "index": "by_text",
                "compound": {
                    "should": [
                        {"text": {"query": "ada", "path": "title"}},
                        {"autocomplete": {"query": "vec", "path": "body"}},
                    ]
                },
            },
        )
        exact_pruned, exact_meta = prune_candidate_storage_keys_for_topk(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            query=compound,
            candidate_storage_keys=["a", "b", "c"],
            candidate_exact=True,
            result_limit_hint=1,
            should_candidates=[["a"], ["a", "b"]],
            candidate_resolver=lambda clause: (["a"], "fts5", True),
        )
        self.assertEqual(exact_pruned, ["a"])
        self.assertEqual(exact_meta["strategy"], "exact-should-score-tier")

        broad_compound = compile_search_stage(
            "$search",
            {
                "index": "by_text",
                "compound": {
                    "should": [
                        {"text": {"query": "vector", "path": "body"}},
                        {"autocomplete": {"query": "vec", "path": "body"}},
                        {"exists": {"path": "title"}},
                    ]
                },
            },
        )
        ranked_keys = rank_compound_candidate_storage_keys_from_entries(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            query=broad_compound,
            candidate_storage_keys=["a", "b", "c"],
            result_limit_hint=2,
        )
        self.assertEqual(ranked_keys[:1], ["a"])
        self.conn.execute(f'DELETE FROM "{self.physical_name}"')
        cached_ranked_keys = rank_compound_candidate_storage_keys_from_entries(
            self.engine,
            self.conn,
            db_name="db",
            coll_name="coll",
            physical_name=self.physical_name,
            query=broad_compound,
            candidate_storage_keys=["a", "b", "c"],
            result_limit_hint=2,
        )
        self.assertEqual(cached_ranked_keys[:1], ["a"])
        self.assertIn(
            (("a", "b", "c"), 2),
            getattr(self.engine, "_compound_rank_cache")[("db", "coll", self.physical_name, 1, repr(broad_compound))],
        )
        self.assertEqual(
            rank_compound_candidate_storage_keys_from_entries(
                self.engine,
                self.conn,
                db_name="db",
                coll_name="coll",
                physical_name=self.physical_name,
                query=compile_search_stage(
                    "$search",
                    {"index": "by_text", "compound": {"should": [{"near": {"path": "score", "origin": 5, "pivot": 2}}]}},
                ),
                candidate_storage_keys=["a"],
                result_limit_hint=1,
            ),
            None,
        )
