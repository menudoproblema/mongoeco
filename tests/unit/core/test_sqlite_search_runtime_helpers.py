import unittest
from unittest.mock import patch

from mongoeco.core.search import compile_search_stage
from mongoeco.engines import _sqlite_search_runtime as search_runtime_module
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import SearchIndexDefinition
from tests.unit.core._sqlite_helper_test_cases import SQLiteInternalHelperTests as _BaseSQLiteHelperTests

_BaseSQLiteHelperTests.__test__ = False


def _build_case(name: str, methods: list[str]) -> type[unittest.TestCase]:
    namespace = {"__module__": __name__, "_connection": _BaseSQLiteHelperTests._connection}
    for method_name in methods:
        namespace[method_name] = getattr(_BaseSQLiteHelperTests, method_name)
    return type(name, (unittest.TestCase,), namespace)


SQLiteSearchRuntimeHelperTests = _build_case(
    "SQLiteSearchRuntimeHelperTests",
    [
        "test_sqlite_engine_module_keeps_admin_runtime_boundary",
        "test_sqlite_runtime_diagnostics_handles_missing_search_index_table",
        "test_sqlite_explain_contract_helpers_cover_hint_and_fallback_shapes",
        "test_sqlite_search_runtime_exact_should_score_prefilter_and_vector_prefilter_helpers",
        "test_sqlite_search_runtime_helper_branches_and_sync_wrappers",
        "test_sqlite_search_runtime_additional_compound_and_downstream_helper_paths",
        "test_sqlite_search_backend_cache_helpers_prune_stale_entries",
        "test_sqlite_search_helpers_cover_listing_and_python_fallback",
        "test_sqlite_array_comparison_and_search_entry_helpers_cover_fallback_paths",
        "test_sqlite_entry_rebuild_and_snapshot_helpers_cover_none_and_skip_paths",
    ],
)


class SQLiteSearchRuntimeDirectCoverageTests(unittest.TestCase):
    def test_sqlite_search_runtime_direct_branches(self) -> None:
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                await engine.put_document(
                    "db",
                    "coll",
                    {
                        "_id": 1,
                        "title": "Ada algorithms",
                        "body": "vector ranking",
                        "kind": "note",
                        "score": 7,
                        "embedding": [1.0, 0.0],
                    },
                )
                await engine.put_document(
                    "db",
                    "coll",
                    {
                        "_id": 2,
                        "title": "Grace notes",
                        "body": "compiler vector",
                        "kind": "reference",
                        "score": 4,
                        "embedding": [0.0, 1.0],
                    },
                )
                text_definition = SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "title": {"type": "string"},
                                "body": {"type": "autocomplete"},
                                "kind": {"type": "token"},
                            },
                        }
                    },
                    name="by_text",
                    index_type="search",
                )
                vector_definition = SearchIndexDefinition(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 2,
                                "similarity": "cosine",
                            }
                        ]
                    },
                    name="by_vector",
                    index_type="vectorSearch",
                )
                search_runtime_module.create_search_index_sync(engine, "db", "coll", text_definition, None, None)
                search_runtime_module.create_search_index_sync(engine, "db", "coll", vector_definition, None, None)

                self.assertEqual(
                    search_runtime_module._vector_filter_residual_description({"kind": "note"}, None),
                    {
                        "required": True,
                        "reason": "filter-not-candidateable",
                        "candidateable": False,
                        "exact": False,
                        "spec": {"kind": "note"},
                    },
                )
                self.assertEqual(
                    search_runtime_module._intersect_storage_key_lists([["a", "b"], ["b", "c"]]),
                    ["b"],
                )
                self.assertEqual(
                    search_runtime_module._storage_keys_with_minimum_frequency([["a", "b"], ["b", "c"]], 1),
                    ["a", "b", "c"],
                )

                conn = engine._require_connection()
                with engine._bind_connection(conn):
                    text_physical = search_runtime_module.ensure_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        text_definition,
                        engine._load_search_index_rows("db", "coll", name="by_text")[0][1],
                    )
                    vector_physical = search_runtime_module.ensure_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        vector_definition,
                        engine._load_search_index_rows("db", "coll", name="by_vector")[0][1],
                    )
                    assert text_physical is not None
                    assert vector_physical is not None

                    clause_only = search_runtime_module._candidate_storage_keys_for_downstream_filter_node(
                        engine,
                        conn,
                        text_physical,
                        filter_spec={"$and": [{"title": "Ada algorithms"}]},
                        field_types={"title": "string"},
                        return_clauses_only=True,
                    )
                    self.assertEqual(clause_only["clauses"], [("title", "Ada algorithms")])
                    evaluated = search_runtime_module._candidate_storage_keys_for_downstream_filter_node(
                        engine,
                        conn,
                        text_physical,
                        filter_spec={"title": "Ada algorithms"},
                        field_types={"title": "string"},
                    )
                    self.assertEqual(evaluated["keys"], [engine._storage_key(1)])
                    self.assertEqual(
                        search_runtime_module._sqlite_candidate_storage_keys_for_downstream_filter(
                            engine,
                            conn,
                            physical_name=text_physical,
                            definition=text_definition,
                            filter_spec={"$and": "bad"},
                        ),
                        (
                            None,
                            {
                                "spec": {"$and": "bad"},
                                "candidateable": False,
                                "exact": False,
                                "backend": None,
                                "supportedPaths": [],
                                "supportedClauseCount": 0,
                                "unsupportedClauseCount": 1,
                                "supportedOperators": [],
                                "booleanShape": None,
                            },
                        ),
                    )
                    with patch.object(search_runtime_module, "evaluate_candidate_filter", return_value=None):
                        self.assertIsNone(
                            search_runtime_module._candidate_storage_keys_for_downstream_filter_node(
                                engine,
                                conn,
                                text_physical,
                                filter_spec={"title": "Ada algorithms"},
                                field_types={"title": "string"},
                            )
                        )
                    self.assertEqual(
                        search_runtime_module._candidate_storage_keys_for_downstream_clause(
                            engine,
                            conn,
                            text_physical,
                            path="title",
                            clause={"$in": [1]},
                            field_types={"title": "string"},
                        ),
                        (None, "$in"),
                    )
                    self.assertEqual(
                        search_runtime_module._candidate_storage_keys_for_downstream_clause(
                            engine,
                            conn,
                            text_physical,
                            path="title",
                            clause={"$exists": False},
                            field_types={"title": "string"},
                        ),
                        (None, "eq"),
                    )

                    compound_clause = compile_search_stage(
                        "$search",
                        {"index": "by_text", "text": {"query": "ada", "path": "title"}},
                    )
                    state_keys, state_backend, state_exact, refinement = (
                        search_runtime_module._sqlite_candidate_state_for_compound_clause(
                            engine,
                            conn,
                            physical_name=text_physical,
                            definition=text_definition,
                            clause=compound_clause,
                            downstream_filter_storage_keys=[engine._storage_key(1)],
                            downstream_filter_spec={"title": "Ada algorithms"},
                            downstream_filter_exact=True,
                        )
                    )
                    self.assertEqual(state_keys, [engine._storage_key(1)])
                    self.assertEqual(state_backend, "fts5+downstream-filter")
                    self.assertTrue(state_exact)
                    self.assertEqual(refinement["path"], "title")

                    self.assertIsNotNone(
                        search_runtime_module._materialized_search_entry_cache_bucket(
                            engine,
                            db_name="db",
                            coll_name="coll",
                            physical_name=text_physical,
                        )
                    )
                    self.assertIsInstance(
                        search_runtime_module._load_materialized_search_documents_by_storage_key(
                            engine,
                            conn,
                            db_name="db",
                            coll_name="coll",
                            physical_name=text_physical,
                            storage_keys=[engine._storage_key(1)],
                        ),
                        dict,
                    )
                    self.assertIsNotNone(
                        search_runtime_module._exact_candidateable_should_scores(
                            engine,
                            conn,
                            db_name="db",
                            coll_name="coll",
                            physical_name=text_physical,
                            query=compile_search_stage(
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
                            ),
                            candidate_storage_keys=[engine._storage_key(1)],
                        )
                    )

                    conn.execute(f'DROP TABLE "{text_physical}"')
                    search_runtime_module.delete_search_entries_for_storage_key(
                        engine,
                        conn,
                        "db",
                        "coll",
                        engine._storage_key(1),
                        search_indexes=[(text_definition, text_physical, None)],
                    )
                    search_runtime_module.replace_search_entries_for_document(
                        engine,
                        conn,
                        "db",
                        "coll",
                        engine._storage_key(2),
                        {"_id": 2, "misc": "skip"},
                        search_indexes=[(text_definition, text_physical, None)],
                    )
                    self.assertTrue(engine._sqlite_table_exists(conn, text_physical))

                    vector_state = search_runtime_module.ensure_vector_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        vector_definition,
                        vector_physical,
                        "embedding",
                    )
                    with patch.object(search_runtime_module, "score_vector_document", return_value=None):
                        hits, requested, evaluated_count, filtered_count, filtered_by_min_score, fallback_reason = (
                            search_runtime_module._sqlite_vector_candidate_documents(
                                engine,
                                "db",
                                "coll",
                                vector_definition,
                                compile_search_stage(
                                    "$vectorSearch",
                                    {
                                        "index": "by_vector",
                                        "path": "embedding",
                                        "queryVector": [1.0, 0.0],
                                        "limit": 1,
                                        "numCandidates": 2,
                                    },
                                ),
                                vector_state,
                                prefilter_storage_keys=None,
                                prefilter_exact=False,
                            )
                        )
                    self.assertEqual(hits, [])
                    self.assertGreaterEqual(requested, 1)
                    self.assertGreaterEqual(evaluated_count, 1)
                    self.assertEqual(filtered_count, 0)
                    self.assertEqual(filtered_by_min_score, 0)
                    self.assertIsNone(fallback_reason)

                    vector_query = compile_search_stage(
                        "$vectorSearch",
                        {"index": "by_vector", "path": "embedding", "queryVector": [1.0, 0.0], "limit": 2},
                    )
                    with patch.object(
                        search_runtime_module,
                        "_sqlite_vector_candidate_documents",
                        return_value=(
                            [
                                {"_id": 1, "kind": "note"},
                                {"_id": 2, "kind": "reference"},
                            ],
                            2,
                            2,
                            0,
                            0,
                            None,
                        ),
                    ):
                        filtered_vector_docs = search_runtime_module.execute_sqlite_search_query(
                            engine,
                            conn,
                            "db",
                            "coll",
                            vector_definition,
                            vector_query,
                            vector_physical,
                            None,
                            2,
                            {"kind": "note"},
                        )
                    self.assertEqual([doc["_id"] for doc in filtered_vector_docs], [1])

                    text_query = compile_search_stage(
                        "$search",
                        {"index": "by_text", "text": {"query": "vector", "path": ["title", "body"]}},
                    )
                    with (
                        patch.object(
                            search_runtime_module,
                            "_sqlite_candidate_storage_keys_for_query",
                            return_value=(None, None, False),
                        ),
                        patch.object(
                            search_runtime_module,
                            "_sqlite_candidate_storage_keys_for_downstream_filter",
                            return_value=([engine._storage_key(1)], {"exact": True}),
                        ),
                    ):
                        downstream_only_docs = search_runtime_module.execute_sqlite_search_query(
                            engine,
                            conn,
                            "db",
                            "coll",
                            text_definition,
                            text_query,
                            text_physical,
                            None,
                            1,
                            {"kind": "note"},
                        )
                    self.assertEqual([doc["_id"] for doc in downstream_only_docs], [1])

                    with patch.object(
                        search_runtime_module,
                        "_sqlite_candidate_storage_keys_for_query",
                        return_value=([], "fts5", True),
                    ):
                        self.assertEqual(
                            search_runtime_module.execute_sqlite_search_query(
                                engine,
                                conn,
                                "db",
                                "coll",
                                text_definition,
                                text_query,
                                text_physical,
                                None,
                                1,
                                None,
                            ),
                            [],
                        )

                    compound_query = compile_search_stage(
                        "$search",
                        {
                            "index": "by_text",
                            "compound": {
                                "should": [{"exists": {"path": "title"}}],
                                "minimumShouldMatch": 0,
                            },
                        },
                    )
                    with (
                        patch.object(
                            search_runtime_module,
                            "_sqlite_compound_candidate_state",
                            return_value=(
                                [engine._storage_key(1), engine._storage_key(2)],
                                "fts5-prefilter",
                                True,
                                [[engine._storage_key(1), engine._storage_key(2)]],
                                0,
                            ),
                        ),
                        patch.object(search_runtime_module, "_compound_entry_ranking_supported", return_value=True),
                        patch.object(search_runtime_module, "_rank_compound_candidate_storage_keys_from_entries", return_value=None),
                    ):
                        compound_docs = search_runtime_module.execute_sqlite_search_query(
                            engine,
                            conn,
                            "db",
                            "coll",
                            text_definition,
                            compound_query,
                            text_physical,
                            None,
                            1,
                            {"kind": "note"},
                        )
                    self.assertEqual([doc["_id"] for doc in compound_docs], [1])

                    with (
                        patch.object(
                            search_runtime_module,
                            "_sqlite_candidate_storage_keys_for_query",
                            return_value=([engine._storage_key(1), engine._storage_key(2)], "fts5", True),
                        ),
                        patch.object(
                            search_runtime_module,
                            "_sqlite_candidate_storage_keys_for_downstream_filter",
                            return_value=([engine._storage_key(1)], {"exact": True}),
                        ),
                    ):
                        explanation = search_runtime_module.explain_search_documents_sync(
                            engine,
                            "db",
                            "coll",
                            "$search",
                            {"index": "by_text", "text": {"query": "vector", "path": ["title", "body"]}},
                            None,
                            None,
                            1,
                            {"kind": "note"},
                        )
                    self.assertEqual(explanation.details["candidateCount"], 1)
                    self.assertTrue(explanation.details["candidateExact"])

                    with (
                        patch.object(
                            search_runtime_module,
                            "_sqlite_candidate_storage_keys_for_query",
                            return_value=(None, None, False),
                        ),
                        patch.object(
                            search_runtime_module,
                            "_sqlite_candidate_storage_keys_for_downstream_filter",
                            return_value=([engine._storage_key(1)], {"exact": True}),
                        ),
                    ):
                        downstream_only_explanation = search_runtime_module.explain_search_documents_sync(
                            engine,
                            "db",
                            "coll",
                            "$search",
                            {"index": "by_text", "text": {"query": "vector", "path": ["title", "body"]}},
                            None,
                            None,
                            1,
                            {"kind": "note"},
                        )
                    self.assertEqual(downstream_only_explanation.details["candidateCount"], 1)
                    self.assertFalse(downstream_only_explanation.details["candidateExact"])
            finally:
                await engine.disconnect()

        import asyncio

        asyncio.run(_run())
