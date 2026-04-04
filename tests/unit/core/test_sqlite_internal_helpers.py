import asyncio
import ast
import json
from pathlib import Path
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExprCondition,
    GeoIntersectsCondition,
    GeoWithinCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    JsonSchemaCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NearCondition,
    NotCondition,
    OrCondition,
    RegexCondition,
    SizeCondition,
    TypeCondition,
    compile_filter,
)
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.core.search import SearchWildcardQuery, compile_search_stage
from mongoeco.engines import _sqlite_admin_runtime as admin_runtime_module
from mongoeco.engines import _sqlite_explain_contract as explain_contract
from mongoeco.engines import _sqlite_index_admin as index_admin
from mongoeco.engines import _sqlite_modify_ops as modify_ops
from mongoeco.engines import _sqlite_namespace_admin as namespace_admin
from mongoeco.engines import _sqlite_read_execution as read_execution_module
from mongoeco.engines import _sqlite_search_admin as search_admin
from mongoeco.engines import _sqlite_search_runtime as search_runtime_module
from mongoeco.engines import _sqlite_vector_backend as vector_backend
from mongoeco.engines import _sqlite_write_ops as write_ops
from mongoeco.engines.sqlite_planner import SQLiteReadExecutionPlan
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.engines.profiling import EngineProfiler
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, OperationFailure
from mongoeco.types import EngineIndexRecord, SearchIndexDefinition
from mongoeco.compat import MONGODB_DIALECT_70


class SQLiteInternalHelperTests(unittest.TestCase):
    def test_sqlite_engine_module_keeps_admin_runtime_boundary(self):
        module_path = Path(__file__).resolve().parents[3] / "src" / "mongoeco" / "engines" / "sqlite.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        imported_modules = {
            node.module
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }

        self.assertIn("mongoeco.engines._sqlite_admin_runtime", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_explain_contract", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_index_runtime", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_read_fast_path_runtime", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_read_runtime", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_search_runtime", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_session_runtime", imported_modules)

    def test_sqlite_runtime_diagnostics_handles_missing_search_index_table(self):
        engine = SQLiteEngine()
        engine._connection = sqlite3.connect(":memory:")
        try:
            info = engine._runtime_diagnostics_info()
        finally:
            engine._connection.close()
            engine._connection = None

        self.assertEqual(info["planner"]["engine"], "sqlite")
        self.assertEqual(info["search"]["declaredIndexCount"], 0)
        self.assertEqual(info["search"]["pendingIndexCount"], 0)

    def test_sqlite_explain_contract_helpers_cover_hint_and_fallback_shapes(self):
        details = explain_contract.sqlite_pushdown_details(
            SQLiteReadExecutionPlan(
                semantics=None,  # type: ignore[arg-type]
                strategy="hybrid",
                execution_lineage=(),
                fallback_reason="Sort requires Python fallback",
                use_sql=True,
                apply_python_sort=True,
            )
        )
        self.assertEqual(details["mode"], "hybrid")
        self.assertTrue(details["usesSqlRuntime"])
        self.assertTrue(details["pythonSort"])
        self.assertEqual(
            explain_contract.sqlite_planning_issues("Sort requires Python fallback")[0].message,
            "Sort requires Python fallback",
        )
        self.assertEqual(explain_contract.sqlite_planning_issues(None), ())

        hints = explain_contract.sqlite_pushdown_followup_hints(
            AndCondition((RegexCondition("name", "^Ada", ""), GreaterThanCondition("score", 5))),
            fallback_reason="Geospatial operators require Python query fallback",
        )
        self.assertEqual([hint["operator"] for hint in hints], ["$regex", "range-comparison", "geo-runtime"])

        broad_hints = explain_contract.sqlite_pushdown_followup_hints(
            OrCondition(
                (
                    GeoWithinCondition("location", "polygon", {"shape": "a"}),  # type: ignore[arg-type]
                    GeoIntersectsCondition("location", "polygon", {"shape": "b"}),  # type: ignore[arg-type]
                    NearCondition("location", point=(0.0, 0.0), min_distance=None, max_distance=None, spherical=True),
                    ModCondition("score", 2, 0),
                    SizeCondition("items", 2),
                    AllCondition("items", ("Ada",)),
                    ElemMatchCondition("items", EqualsCondition("value", 1), wrap_value=True),
                    TypeCondition("value", ("number",)),
                    BitwiseCondition("flags", "$bitsAllSet", 1),
                    ExprCondition({"$gt": ["$score", 1]}, {}),
                    JsonSchemaCondition({"required": ["name"]}),
                    NotCondition(LessThanCondition("score", 0)),
                )
            ),
            fallback_reason="Tagged undefined requires Python fallback",
        )
        operators = {hint["operator"] for hint in broad_hints}
        self.assertTrue(
            {
                "$geoWithin",
                "$geoIntersects",
                "$nearSphere",
                "$mod",
                "$size",
                "$all",
                "$elemMatch",
                "$type",
                "$bitsAllSet",
                "$expr",
                "$jsonSchema",
                "range-comparison",
                "tagged-undefined",
            }.issubset(operators)
        )

        more_hints = explain_contract.sqlite_pushdown_followup_hints(
            OrCondition(
                (
                    NearCondition("location", point=(0.0, 0.0), min_distance=None, max_distance=None, spherical=False),
                    BitwiseCondition("flags", "$bitsAnySet", 1),
                    BitwiseCondition("flags", "$bitsAllClear", 1),
                    BitwiseCondition("flags", "$bitsAnyClear", 1),
                    GreaterThanOrEqualCondition("score", 1),
                    LessThanOrEqualCondition("score", 9),
                )
            ),
            fallback_reason="Array traversal requires Python fallback",
        )
        operator_map = {hint["operator"]: hint for hint in more_hints}
        self.assertIn("$near", operator_map)
        self.assertIn("$bitsAnySet", operator_map)
        self.assertIn("$bitsAllClear", operator_map)
        self.assertIn("$bitsAnyClear", operator_map)
        self.assertIn("array-traversal", operator_map)

    def test_sqlite_vector_backend_covers_build_search_and_error_paths(self):
        definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 2,
                        "similarity": "euclidean",
                        "connectivity": 8,
                        "expansionAdd": 16,
                        "expansionSearch": 32,
                    }
                ]
            },
            name="vec",
            index_type="vectorSearch",
        )
        state = vector_backend.build_sqlite_vector_backend(
            db_name="db",
            coll_name="coll",
            definition=definition,
            physical_name="search_idx_vec",
            path="embedding",
            collection_version=3,
            documents=[
                ("1", {"embedding": [1.0, 0.0], "kind": "keep"}),
                ("2", {"embedding": [0.0, 1.0], "kind": "drop"}),
                ("3", {"embedding": [1.0]}),
                ("4", {"embedding": ["bad", 1.0]}),
            ],
        )

        self.assertEqual(state.valid_vectors, 2)
        self.assertEqual(state.invalid_vectors, 2)
        self.assertEqual(vector_backend.vector_backend_stats_document(state)["backend"], "usearch")
        self.assertEqual(vector_backend._metric_kind("dotProduct").name, vector_backend.MetricKind.IP.name)
        self.assertEqual(vector_backend._metric_kind("euclidean").name, vector_backend.MetricKind.L2sq.name)
        self.assertEqual(vector_backend._optional_positive_int(None), None)
        self.assertEqual(vector_backend._optional_positive_int(2), 2)
        with self.assertRaisesRegex(OperationFailure, "positive integers"):
            vector_backend._optional_positive_int(0)
        with self.assertRaisesRegex(OperationFailure, "not defined on index"):
            vector_backend._require_vector_field_spec(definition, "other")

        hits = vector_backend.search_sqlite_vector_backend(state, query_vector=(1.0, 0.0), count=2)
        self.assertEqual(len(hits), 2)
        self.assertEqual(vector_backend.search_sqlite_vector_backend(state, query_vector=(1.0, 0.0), count=0), [])
        with self.assertRaisesRegex(OperationFailure, "dimensions do not match"):
            vector_backend.search_sqlite_vector_backend(state, query_vector=(1.0,), count=1)
        self.assertIsNone(vector_backend._extract_vector({"embedding": [1.0]}, path="embedding", num_dimensions=2))
        self.assertIsNone(vector_backend._extract_vector({"embedding": [True, 1.0]}, path="embedding", num_dimensions=2))

        filter_keys, filter_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"kind": "keep"},
        )
        self.assertEqual(filter_keys, ["1"])
        self.assertEqual(filter_description["backend"], "vector-filter-index")
        self.assertEqual(filter_description["supportedPaths"], ["kind"])
        self.assertTrue(filter_description["exact"])

    def test_sqlite_vector_backend_filter_prefilter_supports_in_exists_and_reports_unsupported(self):
        definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 2,
                    }
                ]
            },
            name="vec",
            index_type="vectorSearch",
        )
        state = vector_backend.build_sqlite_vector_backend(
            db_name="db",
            coll_name="coll",
            definition=definition,
            physical_name="search_idx_vec",
            path="embedding",
            collection_version=1,
            documents=[
                ("1", {"embedding": [1.0, 0.0], "kind": "keep", "tags": ["a", "b"], "meta": {"rank": 1}}),
                ("2", {"embedding": [0.0, 1.0], "kind": "drop", "tags": ["b"]}),
                ("3", {"embedding": [0.5, 0.5], "kind": "keep"}),
                ("4", {"embedding": ["bad", 1.0], "kind": "keep"}),
            ],
        )

        in_keys, in_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"kind": {"$in": ["keep"]}, "tags": "b"},
        )
        self.assertEqual(in_keys, ["1"])
        self.assertEqual(in_description["supportedOperators"], ["$in", "eq"])
        self.assertEqual(in_description["supportedClauseCount"], 2)

        exists_keys, exists_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"meta.rank": {"$exists": True}},
        )
        self.assertEqual(exists_keys, ["1"])
        self.assertTrue(exists_description["exact"])

        missing_keys, missing_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"meta.rank": {"$exists": False}},
        )
        self.assertEqual(missing_keys, ["2", "3"])
        self.assertTrue(missing_description["exact"])

        unsupported_keys, unsupported_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"kind": {"$gt": "keep"}},
        )
        self.assertIsNone(unsupported_keys)
        self.assertFalse(unsupported_description["candidateable"])
        self.assertEqual(unsupported_description["unsupportedClauseCount"], 1)

        or_keys, or_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"$or": [{"kind": "drop"}, {"meta.rank": {"$exists": True}}]},
        )
        self.assertEqual(or_keys, ["1", "2"])
        self.assertEqual(or_description["booleanShape"], "$or")
        self.assertTrue(or_description["exact"])

        partial_and_keys, partial_and_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"$and": [{"kind": "keep"}, {"kind": {"$gt": "drop"}}]},
        )
        self.assertEqual(partial_and_keys, ["1", "3"])
        self.assertEqual(partial_and_description["booleanShape"], "$and")
        self.assertFalse(partial_and_description["exact"])

        unsupported_or_keys, unsupported_or_description = vector_backend.vector_filter_candidate_storage_keys(
            state,
            filter_spec={"$or": [{"kind": "keep"}, {"kind": {"$gt": "drop"}}]},
        )
        self.assertIsNone(unsupported_or_keys)
        self.assertFalse(unsupported_or_description["candidateable"])
        self.assertEqual(unsupported_or_description["booleanShape"], "$or")

    def test_sqlite_search_runtime_exact_should_score_prefilter_and_vector_prefilter_helpers(self):
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                await engine.put_document("db", "coll", {"_id": 1, "title": "Ada algorithms", "body": "vector algorithm", "embedding": [1.0, 0.0], "kind": "keep"})
                await engine.put_document("db", "coll", {"_id": 2, "title": "Ada", "body": "vector vector", "embedding": [0.9, 0.1], "kind": "drop"})
                await engine.put_document("db", "coll", {"_id": 3, "title": "Grace", "body": "algorithm", "embedding": [0.0, 1.0], "kind": "keep"})
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition(
                        {
                            "mappings": {
                                "dynamic": False,
                                "fields": {
                                    "title": {"type": "string"},
                                    "body": {"type": "string"},
                                },
                            }
                        },
                        name="by_text",
                        index_type="search",
                    ),
                )
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition(
                        {
                            "fields": [
                                {
                                    "type": "vector",
                                    "path": "embedding",
                                    "numDimensions": 2,
                                }
                            ]
                        },
                        name="by_vector",
                        index_type="vectorSearch",
                    ),
                )

                text_rows = engine._load_search_index_rows("db", "coll", name="by_text")
                text_definition, physical_name, _ready_at = text_rows[0]
                conn = engine._require_connection()
                with engine._bind_connection(conn):
                    resolved_physical_name = search_runtime_module.ensure_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        text_definition,
                        physical_name,
                    )
                    query = search_runtime_module.compile_search_stage(
                        "$search",
                        {
                            "index": "by_text",
                            "compound": {
                                "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                "should": [
                                    {"exists": {"path": "title"}},
                                    {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                    {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                ],
                                "minimumShouldMatch": 1,
                            },
                        },
                    )
                    candidate_keys, _backend, candidate_exact, should_candidates, _non_candidateable = search_runtime_module._sqlite_compound_candidate_state(
                        engine,
                        conn,
                        physical_name=resolved_physical_name,
                        definition=text_definition,
                        query=query,
                    )
                    pruned_keys, topk_prefilter = search_runtime_module._prune_candidate_storage_keys_for_topk(
                        engine,
                        conn,
                        physical_name=resolved_physical_name,
                        query=query,
                        candidate_storage_keys=candidate_keys,
                        candidate_exact=candidate_exact,
                        result_limit_hint=1,
                        should_candidates=should_candidates,
                    )
                    self.assertEqual(pruned_keys, [engine._storage_key(1)])
                    self.assertEqual(topk_prefilter["strategy"], "exact-should-score-tier")

                    entries_by_key = search_runtime_module._load_search_entries_by_storage_key(
                        engine,
                        conn,
                        physical_name=resolved_physical_name,
                        storage_keys=[engine._storage_key(1), engine._storage_key(2)],
                    )
                    self.assertIn(engine._storage_key(1), entries_by_key)
                    prepared = search_runtime_module._materialized_search_document_from_entries(
                        entries_by_key[engine._storage_key(1)]
                    )
                    self.assertIn("title", prepared.searchable_paths)
                    self.assertIn("body", prepared.searchable_paths)

                vector_rows = engine._load_search_index_rows("db", "coll", name="by_vector")
                vector_definition, vector_physical_name, _ready = vector_rows[0]
                with engine._bind_connection(conn):
                    vector_backend_state = search_runtime_module.ensure_vector_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        vector_definition,
                        vector_physical_name,
                        "embedding",
                    )
                    filter_keys, filter_description = vector_backend.vector_filter_candidate_storage_keys(
                        vector_backend_state,
                        filter_spec={"kind": "keep"},
                    )
                    self.assertEqual(filter_keys, [engine._storage_key(1), engine._storage_key(3)])
                    self.assertTrue(filter_description["exact"])
                    vector_query = search_runtime_module.compile_search_stage(
                        "$vectorSearch",
                        {
                            "index": "by_vector",
                            "path": "embedding",
                            "queryVector": [1.0, 0.0],
                            "limit": 1,
                            "numCandidates": 1,
                            "filter": {"kind": "keep"},
                        },
                    )
                    documents, requested, evaluated, filtered, fallback_reason = search_runtime_module._sqlite_vector_candidate_documents(
                        engine,
                        "db",
                        "coll",
                        vector_query,
                        vector_backend_state,
                        prefilter_storage_keys=filter_keys,
                        prefilter_exact=True,
                    )
                    self.assertEqual([document["_id"] for document in documents], [1])
                    self.assertEqual(requested, 1)
                    self.assertEqual(filtered, 0)
                    self.assertEqual(fallback_reason, None)
                    self.assertGreaterEqual(evaluated, 1)

                    empty_documents, empty_requested, empty_evaluated, empty_filtered, empty_reason = search_runtime_module._sqlite_vector_candidate_documents(
                        engine,
                        "db",
                        "coll",
                        vector_query,
                        vector_backend_state,
                        prefilter_storage_keys=[],
                        prefilter_exact=True,
                    )
                    self.assertEqual(empty_documents, [])
                    self.assertEqual((empty_requested, empty_evaluated, empty_filtered), (0, 0, 0))
                    self.assertIsNone(empty_reason)

                    downstream_or_keys, downstream_or_description = search_runtime_module._sqlite_candidate_storage_keys_for_downstream_filter(
                        engine,
                        conn,
                        physical_name=resolved_physical_name,
                        definition=text_definition,
                        filter_spec={"$or": [{"title": "Ada algorithms"}, {"body": {"$exists": True}}]},
                    )
                    self.assertEqual(downstream_or_description["booleanShape"], "$or")
                    self.assertTrue(downstream_or_description["candidateable"])
                    self.assertTrue(downstream_or_description["exact"])
                    self.assertEqual(downstream_or_description["supportedClauseCount"], 2)
                    self.assertEqual(downstream_or_description["supportedOperators"], ["eq", "$exists"])
                    self.assertEqual(
                        downstream_or_keys,
                        [engine._storage_key(1), engine._storage_key(2), engine._storage_key(3)],
                    )

                    downstream_partial_and_keys, downstream_partial_and_description = search_runtime_module._sqlite_candidate_storage_keys_for_downstream_filter(
                        engine,
                        conn,
                        physical_name=resolved_physical_name,
                        definition=text_definition,
                        filter_spec={"$and": [{"title": "Ada algorithms"}, {"score": {"$gt": 5}}]},
                    )
                    self.assertEqual(downstream_partial_and_description["booleanShape"], "$and")
                    self.assertTrue(downstream_partial_and_description["candidateable"])
                    self.assertFalse(downstream_partial_and_description["exact"])
                    self.assertEqual(downstream_partial_and_keys, [engine._storage_key(1)])

                    downstream_unsupported_or_keys, downstream_unsupported_or_description = search_runtime_module._sqlite_candidate_storage_keys_for_downstream_filter(
                        engine,
                        conn,
                        physical_name=resolved_physical_name,
                        definition=text_definition,
                        filter_spec={"$or": [{"title": "Ada algorithms"}, {"score": {"$gt": 5}}]},
                    )
                    self.assertIsNone(downstream_unsupported_or_keys)
                    self.assertFalse(downstream_unsupported_or_description["candidateable"])
                    self.assertEqual(downstream_unsupported_or_description["booleanShape"], "$or")
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_sqlite_search_runtime_helper_branches_and_sync_wrappers(self):
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                await engine.put_document("db", "coll", {"_id": 1, "title": "Ada algorithms", "body": "vector algorithm", "score": 7, "kind": "note", "embedding": [1.0, 0.0]})
                await engine.put_document("db", "coll", {"_id": 2, "title": "Grace notes", "body": "compiler vector", "score": 9, "kind": "reference", "embedding": [0.5, 0.5]})
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
                    {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2}]},
                    name="by_vector",
                    index_type="vectorSearch",
                )
                search_runtime_module.create_search_index_sync(engine, "db", "coll", text_definition, None, None)
                search_runtime_module.create_search_index_sync(engine, "db", "coll", vector_definition, None, None)

                conn = engine._require_connection()
                with engine._bind_connection(conn):
                    text_rows = search_runtime_module.load_search_index_rows(engine, "db", "coll", name="by_text")
                    self.assertEqual(len(search_runtime_module.load_search_indexes(engine, "db", "coll")), 2)
                    self.assertIsNone(search_runtime_module.pending_search_index_ready_at(engine))
                    self.assertTrue(search_runtime_module.search_index_is_ready_sync(None))
                    text_definition_loaded, physical_name, _ = text_rows[0]
                    resolved_physical = search_runtime_module.ensure_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        text_definition_loaded,
                        physical_name,
                    )
                    self.assertEqual(search_runtime_module._load_candidate_documents(engine, "db", "coll", []), [])
                    self.assertEqual(search_runtime_module._intersect_storage_key_lists([]), [])
                    self.assertEqual(search_runtime_module._union_storage_key_lists([["a", "b"], ["b", "c"]]), ["a", "b", "c"])
                    self.assertEqual(
                        search_runtime_module._storage_keys_with_minimum_frequency([["a", "b"], ["b", "c"], ["b"]], 2),
                        ["b"],
                    )

                    text_query = compile_search_stage("$search", {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}})
                    phrase_query = compile_search_stage("$search", {"index": "by_text", "phrase": {"query": "Ada algorithms", "path": "title"}})
                    autocomplete_query = compile_search_stage("$search", {"index": "by_text", "autocomplete": {"query": "alg", "path": ["title", "body"]}})
                    wildcard_query = compile_search_stage("$search", {"index": "by_text", "wildcard": {"query": "*vector*", "path": "body"}})
                    exists_query = compile_search_stage("$search", {"index": "by_text", "exists": {"path": "title"}})
                    near_query = compile_search_stage("$search", {"index": "by_text", "near": {"path": "score", "origin": 8, "pivot": 2}})
                    compound_query = compile_search_stage(
                        "$search",
                        {
                            "index": "by_text",
                            "compound": {
                                "must": [{"text": {"query": "vector", "path": ["title", "body"]}}],
                                "should": [{"exists": {"path": "title"}}],
                                "minimumShouldMatch": 0,
                            },
                        },
                    )

                    text_keys, text_backend, text_exact = search_runtime_module._sqlite_leaf_candidate_storage_keys(
                        engine, conn, physical_name=resolved_physical, query=text_query
                    )
                    self.assertEqual(text_backend, "fts5")
                    self.assertTrue(text_exact)
                    self.assertIn(engine._storage_key(1), text_keys)

                    phrase_keys, phrase_backend, phrase_exact = search_runtime_module._sqlite_leaf_candidate_storage_keys(
                        engine, conn, physical_name=resolved_physical, query=phrase_query
                    )
                    self.assertEqual(phrase_backend, "fts5")
                    self.assertFalse(phrase_exact)
                    self.assertEqual(phrase_keys, [engine._storage_key(1)])

                    autocomplete_keys, _, _ = search_runtime_module._sqlite_leaf_candidate_storage_keys(
                        engine, conn, physical_name=resolved_physical, query=autocomplete_query
                    )
                    wildcard_keys, wildcard_backend, wildcard_exact = search_runtime_module._sqlite_leaf_candidate_storage_keys(
                        engine, conn, physical_name=resolved_physical, query=wildcard_query
                    )
                    exists_keys, exists_backend, exists_exact = search_runtime_module._sqlite_leaf_candidate_storage_keys(
                        engine, conn, physical_name=resolved_physical, query=exists_query
                    )
                    self.assertTrue(autocomplete_keys)
                    self.assertEqual(wildcard_backend, "fts5-glob")
                    self.assertTrue(wildcard_exact)
                    self.assertEqual(exists_backend, "fts5-path")
                    self.assertTrue(exists_exact)
                    self.assertEqual(
                        search_runtime_module._sqlite_candidate_storage_keys_for_query(
                            engine, conn, physical_name=None, query=text_query, definition=text_definition_loaded
                        ),
                        (None, None, False),
                    )
                    self.assertEqual(
                        search_runtime_module._sqlite_candidate_storage_keys_for_query(
                            engine, conn, physical_name=resolved_physical, query=near_query, definition=text_definition_loaded
                        ),
                        (None, None, False),
                    )
                    self.assertEqual(
                        search_runtime_module._sqlite_candidate_storage_keys_for_query(
                            engine, conn, physical_name=resolved_physical, query=compound_query
                        ),
                        (None, None, False),
                    )
                    compound_keys, compound_backend, _ = search_runtime_module._sqlite_candidate_storage_keys_for_query(
                        engine, conn, physical_name=resolved_physical, query=compound_query, definition=text_definition_loaded
                    )
                    self.assertEqual(compound_backend, "fts5-prefilter")
                    self.assertTrue(compound_keys)

                    self.assertEqual(search_runtime_module._textual_search_field_types(SearchIndexDefinition({"mappings": "bad"}, name="x", index_type="search")), {})
                    self.assertIsNone(search_runtime_module._flatten_downstream_filter_clauses({"$or": "bad"}))
                    self.assertEqual(search_runtime_module._document_for_search_path("a.b", 1), {"a": {"b": 1}})
                    self.assertEqual(search_runtime_module._clause_search_paths(near_query), None)
                    self.assertEqual(
                        search_runtime_module._downstream_filter_implies_clause(
                            exists_query,
                            definition=text_definition_loaded,
                            filter_spec={"title": {"$exists": True}},
                        )[0],
                        "title",
                    )
                    self.assertEqual(
                        search_runtime_module._downstream_filter_implies_clause(
                            wildcard_query,
                            definition=text_definition_loaded,
                            filter_spec={"title": {"$in": [1]}},
                        ),
                        (None, None),
                    )
                    self.assertEqual(
                        search_runtime_module._candidate_storage_keys_for_downstream_clause(
                            engine, conn, resolved_physical, path="score", clause=7, field_types={"score": "number"}
                        ),
                        (None, "eq"),
                    )
                    self.assertEqual(
                        search_runtime_module._candidate_storage_keys_for_downstream_clause(
                            None, None, None, path="title", clause="Ada algorithms", field_types={"title": "string"}
                        ),
                        (None, "eq"),
                    )

                    direct_docs = search_runtime_module.execute_sqlite_search_query(
                        engine,
                        conn,
                        "db",
                        "coll",
                        text_definition_loaded,
                        text_query,
                        resolved_physical,
                        None,
                        1,
                        {"kind": "note"},
                    )
                    self.assertEqual([document["_id"] for document in direct_docs], [1])

                    explained = search_runtime_module.explain_search_documents_sync(
                        engine,
                        "db",
                        "coll",
                        "$search",
                        {
                            "index": "by_text",
                            "compound": {
                                "must": [{"text": {"query": "vector", "path": ["title", "body"]}}],
                                "should": [{"exists": {"path": "title"}}],
                                "minimumShouldMatch": 0,
                            },
                        },
                        None,
                        None,
                        1,
                        {"kind": "note"},
                    )
                    self.assertEqual(explained.details["backend"], "fts5-prefilter")

                    search_runtime_module.replace_search_entries_for_document(
                        engine,
                        conn,
                        "db",
                        "coll",
                        engine._storage_key(1),
                        {"_id": 1, "title": "Ada refreshed", "body": "vector refreshed"},
                    )
                    search_runtime_module.delete_search_entries_for_storage_key(
                        engine,
                        conn,
                        "db",
                        "coll",
                        engine._storage_key(2),
                    )

                    vector_rows = search_runtime_module.load_search_index_rows(engine, "db", "coll", name="by_vector")
                    vector_definition_loaded, vector_physical_name, _ = vector_rows[0]
                    vector_state = search_runtime_module.ensure_vector_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        vector_definition_loaded,
                        vector_physical_name,
                        "embedding",
                    )
                    cached_vector_state = search_runtime_module.ensure_vector_search_backend_sync(
                        engine,
                        conn,
                        "db",
                        "coll",
                        vector_definition_loaded,
                        vector_physical_name,
                        "embedding",
                    )
                    self.assertIs(vector_state, cached_vector_state)
                    if conn.in_transaction:
                        conn.commit()

                search_runtime_module.update_search_index_sync(
                    engine,
                    "db",
                    "coll",
                    "by_text",
                    {"mappings": {"dynamic": True}},
                    None,
                    None,
                )
                listed = search_runtime_module.list_search_indexes_sync(engine, "db", "coll", None, None)
                self.assertEqual(len(listed), 2)
                search_runtime_module.drop_search_index_sync(engine, "db", "coll", "by_vector", None, None)
                listed_after_drop = search_runtime_module.list_search_indexes_sync(engine, "db", "coll", None, None)
                self.assertEqual(len(listed_after_drop), 1)

                rows_after_drop = engine._load_search_index_rows("db", "coll", name="by_vector")
                self.assertEqual(rows_after_drop, [])
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_sqlite_profile_namespace_paths_delegate_to_admin_runtime(self):
        engine = SQLiteEngine()
        with patch.object(
            engine._admin_runtime,
            "profile_namespace_document",
            return_value={"_id": "p1", "op": "query"},
        ) as doc_mock:
            document = engine._get_document_sync(
                "db",
                "system.profile",
                "p1",
                projection={"op": 1, "_id": 0},
            )
        self.assertEqual(document, {"_id": "p1", "op": "query"})
        doc_mock.assert_called_once()

        with patch.object(
            engine._admin_runtime,
            "profile_namespace_documents",
            return_value=[{"_id": "p1", "seq": 1}],
        ) as list_mock:
            self.assertEqual(engine._profile_documents("db"), [{"_id": "p1", "seq": 1}])
        list_mock.assert_called_once_with("db")

        engine._admin_runtime.profile_namespace_documents = None  # type: ignore[assignment]
        with patch.object(
            engine._admin_runtime,
            "profile_documents",
            return_value=[{"_id": "fallback"}],
            create=True,
        ) as fallback_mock:
            self.assertEqual(engine._profile_documents("db"), [{"_id": "fallback"}])
        fallback_mock.assert_called_once_with("db")

    def test_sqlite_search_backend_cache_helpers_prune_stale_entries(self):
        engine = SQLiteEngine()
        backend = SimpleNamespace(db_name="db", coll_name="coll", physical_name="phys")
        engine._vector_search_backends[("phys", "embedding")] = backend
        engine._search_backend_versions[("db", "coll")] = 3

        engine._mark_search_backend_changed("db", "coll")
        self.assertEqual(engine._search_backend_versions[("db", "coll")], 4)
        self.assertNotIn(("phys", "embedding"), engine._vector_search_backends)

        engine._vector_search_backends[("phys", "embedding")] = backend
        engine._search_backend_versions[("db", "other")] = 2
        engine._clear_search_backend_state_for_database("db")
        self.assertNotIn(("db", "coll"), engine._search_backend_versions)
        self.assertNotIn(("db", "other"), engine._search_backend_versions)
        self.assertEqual(engine._vector_search_backends, {})

        with patch.object(engine._admin_runtime, "clear_index_metadata_versions_for_database") as clear_mock:
            engine._clear_index_metadata_versions_for_database("db")
        clear_mock.assert_called_once_with("db")

    def test_sqlite_read_execution_helpers_cover_none_and_guard_paths(self):
        self.assertIsNone(
            read_execution_module.build_scalar_indexed_top_level_equals_sql(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                index_name="idx_kind",
                physical_name="scidx_kind",
                limit=1,
                null_matches_undefined=False,
                lookup_collection_id=lambda _db, _coll: None,
                multikey_signatures_for_query_value=lambda _value, _nulls: (("string", "view"),),
                multikey_type_score=lambda _kind: 1,
                quote_identifier=lambda name: name,
            )
        )
        self.assertIsNone(
            read_execution_module.build_scalar_indexed_top_level_equals_sql(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                index_name="idx_kind",
                physical_name="scidx_kind",
                limit=None,
                null_matches_undefined=False,
                lookup_collection_id=lambda _db, _coll: 1,
                multikey_signatures_for_query_value=lambda _value, _nulls: (),
                multikey_type_score=lambda _kind: 1,
                quote_identifier=lambda name: name,
            )
        )
        self.assertIsNone(
            read_execution_module.build_scalar_indexed_top_level_equals_sql(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                index_name="idx_kind",
                physical_name="scidx_kind",
                limit=None,
                null_matches_undefined=False,
                lookup_collection_id=lambda _db, _coll: 1,
                multikey_signatures_for_query_value=lambda _value, _nulls: (_ for _ in ()).throw(NotImplementedError()),
                multikey_type_score=lambda _kind: 1,
                quote_identifier=lambda name: name,
            )
        )
        self.assertIsNone(
            read_execution_module.build_scalar_indexed_top_level_range_sql(
                db_name="db",
                coll_name="coll",
                value=5,
                operator=">",
                index_name="idx_kind",
                physical_name="scidx_kind",
                limit=1,
                can_use_scalar_range_fast_path=lambda _db, _coll, _value: ("idx", 100, "5"),
                lookup_collection_id=lambda _db, _coll: None,
                quote_identifier=lambda name: name,
            )
        )

    def test_sqlite_read_execution_plan_helpers_cover_limit_branches(self):
        mod_plan = read_execution_module.plan_find_semantics_sync(
            db_name="db",
            coll_name="coll",
            semantics=compile_find_semantics({"value": {"$mod": [2, 0]}}, limit=2),
            storage_key_for_id=repr,
            find_scalar_fast_path_index=lambda _db, _coll, _field: None,
            field_is_top_level_array_in_collection=lambda _db, _coll, _field: False,
            field_contains_real_numeric_in_collection=lambda _db, _coll, _field: False,
            field_contains_non_ascii_text_in_collection=lambda _db, _coll, _field: False,
            build_equals_sql=lambda *args, **kwargs: None,
            build_range_sql=lambda *args, **kwargs: None,
            compile_read_plan=lambda semantics, hint: SQLiteReadExecutionPlan(semantics=semantics, strategy="python", execution_lineage=(), physical_plan=()),
        )
        self.assertEqual(mod_plan.strategy, "sql")
        self.assertIn("LIMIT 2", mod_plan.sql)

        regex_plan = read_execution_module.plan_find_semantics_sync(
            db_name="db",
            coll_name="coll",
            semantics=compile_find_semantics({"name": {"$regex": "Ada"}}, limit=3),
            storage_key_for_id=repr,
            find_scalar_fast_path_index=lambda _db, _coll, _field: None,
            field_is_top_level_array_in_collection=lambda _db, _coll, _field: False,
            field_contains_real_numeric_in_collection=lambda _db, _coll, _field: False,
            field_contains_non_ascii_text_in_collection=lambda _db, _coll, _field: False,
            build_equals_sql=lambda *args, **kwargs: None,
            build_range_sql=lambda *args, **kwargs: None,
            compile_read_plan=lambda semantics, hint: SQLiteReadExecutionPlan(semantics=semantics, strategy="python", execution_lineage=(), physical_plan=()),
        )
        self.assertEqual(regex_plan.strategy, "sql")
        self.assertIn("LIMIT 3", regex_plan.sql)

    def test_sqlite_scalar_and_multikey_helper_branches_cover_remaining_guard_paths(self):
        engine = SQLiteEngine()
        engine._connection = sqlite3.connect(":memory:")
        try:
            engine._connection.execute(
                """
                CREATE TABLE collections (
                    collection_id INTEGER,
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    options_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (db_name, coll_name)
                )
                """
            )
            engine._connection.execute(
                """
                CREATE TABLE documents (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    storage_key TEXT NOT NULL,
                    document TEXT NOT NULL,
                    PRIMARY KEY (db_name, coll_name, storage_key)
                )
                """
            )
            engine._connection.execute(
                "INSERT INTO collections (collection_id, db_name, coll_name, options_json) VALUES (?, ?, ?, ?)",
                (0, "db", "coll", "{}"),
            )
            self.assertIsNone(engine._can_use_scalar_range_fast_path("db", "coll", "a.b", 5))
            with patch.object(engine, "_find_scalar_fast_path_index", return_value=None), patch.object(
                engine,
                "_field_is_top_level_array_in_collection",
                return_value=False,
            ), patch.object(
                engine,
                "_field_has_comparison_type_mismatch_in_collection",
                return_value=False,
            ):
                self.assertIsNone(engine._can_use_scalar_range_fast_path("db", "coll", "value", 5))

            malformed = Mock()
            malformed.execute.return_value.fetchone.side_effect = [(None,), None]
            self.assertIsNone(engine._lookup_collection_id(malformed, "db", "missing"))
            self.assertEqual(
                engine._resolve_hint_index(
                    "db",
                    "coll",
                    [("_id", 1)],
                    indexes=[EngineIndexRecord(name="idx_builtin", fields=["_id"], key=[("_id", 1)], unique=True)],
                )["name"],
                "_id_",
            )

            index = EngineIndexRecord(
                name="idx_tags",
                fields=["tags"],
                key=[("tags", 1)],
                unique=False,
                multikey=True,
                multikey_physical_name="mkidx_tags",
            )
            sql, params = engine._translate_multikey_ordered_exists_clause(
                1,
                "db",
                "coll",
                index,
                element_type="number",
                element_key=SQLiteEngine._normalize_multikey_number(7),
                operator="<",
            )
            self.assertIn("type_score < ?", sql)
            numeric_score = SQLiteEngine._multikey_type_score("number")
            self.assertEqual(params[-3:], [numeric_score, numeric_score, SQLiteEngine._normalize_multikey_number(7)])

            engine._collection_features_cache[("db", "coll", "comparison_mismatch:value:number")] = True
            engine._connection.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "coll", "bool-doc", '{"value":["unexpected"]}'),
            )
            self.assertTrue(engine._field_has_comparison_type_mismatch_in_collection("db", "coll", "value", "number"))
            self.assertTrue(engine._field_has_scalar_or_array_element_type_mismatch_in_collection("db", "coll", "value", "object"))
            self.assertTrue(engine._field_has_scalar_or_array_element_type_mismatch_in_collection("db", "coll", "value", "bool"))

            translated_sql, _translated_params = engine._translate_range_with_multikey(
                1,
                "db",
                "coll",
                GreaterThanCondition("tags", "bad"),
                index,
                operator=">",
            )
            self.assertIn("json_extract", translated_sql)
        finally:
            engine._connection.close()
            engine._connection = None

    def test_sqlite_query_plan_multikey_translation_covers_match_all_fallbacks_and_range_variants(self):
        engine = SQLiteEngine()
        engine._connection = sqlite3.connect(":memory:")
        try:
            engine._connection.execute(
                """
                CREATE TABLE collections (
                    collection_id INTEGER,
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    options_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (db_name, coll_name)
                )
                """
            )
            engine._connection.execute(
                "INSERT INTO collections (collection_id, db_name, coll_name, options_json) VALUES (?, ?, ?, ?)",
                (1, "db", "coll", "{}"),
            )
            self.assertEqual(engine._translate_query_plan_with_multikey("db", "coll", compile_filter({})), ("1 = 1", []))

            with patch.object(engine, "_find_multikey_index", return_value=None), patch.object(
                engine,
                "_field_is_top_level_array_in_collection",
                return_value=False,
            ):
                sql, _ = engine._translate_query_plan_with_multikey("db", "coll", compile_filter({"kind": "view"}))
                self.assertIn("json_type", sql)

            with patch.object(engine, "_find_multikey_index", return_value=EngineIndexRecord(
                name="idx_tags",
                fields=["tags"],
                key=[("tags", 1)],
                unique=False,
                multikey=True,
                multikey_physical_name="mkidx_tags",
            )), patch.object(engine, "_lookup_collection_id", return_value=None):
                fallback_sql, _ = engine._translate_query_plan_with_multikey("db", "coll", compile_filter({"tags": "python"}))
                self.assertIn("json_each", fallback_sql)

            with patch("mongoeco.engines.sqlite._translate_all_condition", side_effect=NotImplementedError()), patch(
                "mongoeco.engines.sqlite.translate_query_plan",
                return_value=("fallback", []),
            ):
                fallback_sql, _ = engine._translate_query_plan_with_multikey("db", "coll", compile_filter({"tags": {"$all": [{"x": 1}]}}))
                self.assertEqual(fallback_sql, "fallback")
            with patch("mongoeco.engines.sqlite._translate_elem_match_condition", side_effect=NotImplementedError()), patch(
                "mongoeco.engines.sqlite.translate_query_plan",
                return_value=("fallback-elem", []),
            ):
                fallback_sql, _ = engine._translate_query_plan_with_multikey(
                    "db",
                    "coll",
                    compile_filter({"items": {"$elemMatch": {"score": {"$gt": 5}}}}),
                )
                self.assertEqual(fallback_sql, "fallback-elem")

            with patch.object(engine, "_find_multikey_index", return_value=None), patch.object(
                engine,
                "_field_is_top_level_array_in_collection",
                return_value=True,
            ), patch.object(
                engine,
                "_field_has_scalar_or_array_element_type_mismatch_in_collection",
                return_value=False,
            ):
                for filter_spec in (
                    {"value": {"$gt": 5}},
                    {"value": {"$gte": 5}},
                    {"value": {"$lt": 5}},
                    {"value": {"$lte": 5}},
                ):
                    sql, _ = engine._translate_query_plan_with_multikey("db", "coll", compile_filter(filter_spec))
                    self.assertIn("json_type(document", sql)
        finally:
            engine._connection.close()
            engine._connection = None

    def test_sqlite_array_comparison_and_search_entry_helpers_cover_fallback_paths(self):
        engine = SQLiteEngine()
        engine._connection = sqlite3.connect(":memory:")
        try:
            engine._connection.execute(
                """
                CREATE TABLE collections (
                    collection_id INTEGER,
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    options_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (db_name, coll_name)
                )
                """
            )
            engine._connection.execute(
                "INSERT INTO collections (collection_id, db_name, coll_name, options_json) VALUES (?, ?, ?, ?)",
                (1, "db", "coll", "{}"),
            )
            with patch.object(engine, "_field_is_top_level_array_in_collection", return_value=True):
                self.assertTrue(
                    engine._plan_requires_python_for_array_comparisons(
                        "db",
                        "coll",
                        compile_filter({"items.value": {"$gt": 5}}),
                    )
                )
            with patch.object(engine, "_field_is_top_level_array_in_collection", return_value=True):
                self.assertTrue(
                    engine._plan_requires_python_for_array_comparisons(
                        "db",
                        "coll",
                        GreaterThanCondition("value", {"x": 1}),
                    )
                )

            search_definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="text", index_type="search")
            vector_definition = SearchIndexDefinition(
                {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
                name="vec",
                index_type="vectorSearch",
            )
            engine._vector_search_backends[("phys", "embedding")] = SimpleNamespace(
                db_name="db",
                coll_name="coll",
                physical_name="phys",
            )
            engine._drop_search_backend_sync(engine._connection, "phys")
            self.assertEqual(engine._vector_search_backends, {})
            engine._replace_search_entries_for_document(
                engine._connection,
                "db",
                "coll",
                "1",
                {"_id": "1"},
                search_indexes=[(search_definition, None, None), (vector_definition, "vec_idx", None)],
            )
            self.assertIsNone(engine._ensure_search_backend_sync(engine._connection, "db", "coll", SearchIndexDefinition({}, name="hybrid", index_type="hybrid"), "x"))
        finally:
            engine._connection.close()
            engine._connection = None

    def test_sqlite_entry_rebuild_and_snapshot_helpers_cover_none_and_skip_paths(self):
        engine = SQLiteEngine()
        conn = sqlite3.connect(":memory:")
        try:
            engine._connection = conn
            conn.execute(
                """
                CREATE TABLE collections (
                    collection_id INTEGER,
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    options_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (db_name, coll_name)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE scalar_index_entries (
                    collection_id INTEGER NOT NULL,
                    index_name TEXT NOT NULL,
                    storage_key TEXT NOT NULL,
                    element_type TEXT NOT NULL,
                    type_score INTEGER NOT NULL DEFAULT 100,
                    element_key TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE indexes (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    physical_name TEXT,
                    fields TEXT NOT NULL,
                    keys TEXT,
                    unique_flag INTEGER NOT NULL,
                    sparse_flag INTEGER NOT NULL DEFAULT 0,
                    hidden_flag INTEGER NOT NULL DEFAULT 0,
                    partial_filter_json TEXT,
                    expire_after_seconds INTEGER,
                    multikey_flag INTEGER NOT NULL DEFAULT 0,
                    multikey_physical_name TEXT,
                    scalar_physical_name TEXT,
                    PRIMARY KEY (db_name, coll_name, name)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE multikey_entries (
                    collection_id INTEGER NOT NULL,
                    index_name TEXT NOT NULL,
                    storage_key TEXT NOT NULL,
                    element_type TEXT NOT NULL,
                    type_score INTEGER NOT NULL DEFAULT 100,
                    element_key TEXT NOT NULL
                )
                """
            )
            plain_index = EngineIndexRecord(name="idx_plain", fields=["name"], key=[("name", 1)], unique=False)
            self.assertEqual(engine._build_scalar_rows_for_document("1", {"_id": "1"}, [plain_index]), [])

            with patch.object(engine, "_lookup_collection_id", return_value=None):
                engine._rebuild_multikey_entries_for_document(conn, "db", "coll", "1", {"tags": ["python"]}, [plain_index])
                engine._rebuild_scalar_entries_for_document(conn, "db", "coll", "1", {"name": "Ada"}, [plain_index])
                engine._replace_multikey_entries_for_index_for_document(conn, "db", "coll", "1", {"tags": ["python"]}, plain_index)
                engine._replace_scalar_entries_for_index_for_document(conn, "db", "coll", "1", {"name": "Ada"}, plain_index)
                engine._backfill_scalar_indexes_sync(conn)

            with patch.object(engine, "_lookup_collection_id", return_value=1):
                engine._replace_multikey_entries_for_index_for_document(conn, "db", "coll", "1", {"tags": ["python"]}, plain_index)
                engine._replace_scalar_entries_for_index_for_document(conn, "db", "coll", "1", {"name": "Ada"}, plain_index)
        finally:
            conn.close()

    def test_sqlite_namespace_admin_and_modify_ops_cover_remaining_error_branches(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE collections (collection_id INTEGER, db_name TEXT NOT NULL, coll_name TEXT NOT NULL, options_json TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE documents (db_name TEXT NOT NULL, coll_name TEXT NOT NULL, storage_key TEXT NOT NULL, document TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE indexes (db_name TEXT NOT NULL, coll_name TEXT NOT NULL, name TEXT, physical_name TEXT, multikey_physical_name TEXT, scalar_physical_name TEXT)"
            )
            conn.execute(
                "CREATE TABLE search_indexes (db_name TEXT NOT NULL, coll_name TEXT NOT NULL, name TEXT, definition_json TEXT, physical_name TEXT, ready_at_epoch REAL)"
            )
            conn.execute("CREATE TABLE scalar_index_entries (collection_id INTEGER, storage_key TEXT, field_path TEXT, value_type TEXT, numeric_value REAL, text_value TEXT)")
            conn.execute("CREATE TABLE multikey_entries (collection_id INTEGER, storage_key TEXT, index_name TEXT, field_path TEXT, type_score INTEGER, text_value TEXT)")
            conn.execute(
                "INSERT INTO collections (collection_id, db_name, coll_name, options_json) VALUES (1, 'db', 'present', '{}')"
            )
            conn.execute(
                "INSERT INTO search_indexes (db_name, coll_name, name, definition_json, physical_name, ready_at_epoch) VALUES ('db', 'present', 'search_idx', '{}', 'fts_present_search_idx', 1.0)"
            )

            self.assertEqual(
                namespace_admin.collection_options(
                    conn=conn,
                    db_name="db",
                    coll_name="index_only",
                    profiler=None,
                    profile_collection_name="system.profile",
                    load_collection_options=lambda *_args: None,
                    collection_exists=lambda _conn, db, coll: db == "db" and coll == "index_only",
                ),
                {},
            )
            with self.assertRaises(CollectionInvalid):
                namespace_admin.rename_collection(
                    conn=conn,
                    db_name="db",
                    coll_name="missing",
                    new_name="renamed",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    collection_exists=lambda _conn, db, coll: coll == "present",
                    mark_index_metadata_changed=lambda *_args: None,
                    invalidate_collection_id_cache=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )

            with self.assertRaises(DuplicateKeyError):
                modify_ops.update_with_operation(
                    db_name="db",
                    coll_name="present",
                    operation=SimpleNamespace(array_filters=None),
                    upsert=False,
                    upsert_seed=None,
                    selector_filter=None,
                    dialect=None,
                    bypass_document_validation=True,
                    compile_update_semantics=lambda *_args, **_kwargs: SimpleNamespace(
                        dialect=MONGODB_DIALECT_70,
                        collation=None,
                        query_plan=MatchAll(),
                        compiled_update_plan=SimpleNamespace(apply=lambda _doc: True),
                        compiled_upsert_plan=SimpleNamespace(apply=lambda _doc: None),
                    ),
                    require_connection=lambda: conn,
                    purge_expired_documents=lambda *_args: None,
                    collection_options_or_empty=lambda *_args: {},
                    dialect_requires_python_fallback=lambda _dialect: False,
                    select_first_document_for_plan=lambda *_args: ("k1", {"_id": "1"}),
                    load_documents=lambda *_args: [],
                    match_plan=lambda *_args: False,
                    enforce_collection_document_validation=lambda *_args, **_kwargs: None,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    load_indexes=lambda *_args: [],
                    load_search_index_rows=lambda *_args: [],
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    translate_compiled_update_plan=lambda *_args: ("?", ()),
                    compiled_update_plan_type=type("Compiled", (), {}),
                    rebuild_multikey_entries_for_document=lambda *_args: None,
                    rebuild_scalar_entries_for_document=lambda *_args: (_ for _ in ()).throw(sqlite3.IntegrityError("dup")),
                    replace_search_entries_for_document=lambda *_args: None,
                    serialize_document=lambda doc: json.dumps(doc),
                    storage_key_for_id=lambda value: str(value),
                    new_object_id=lambda: "new-id",
                    invalidate_collection_features_cache=lambda *_args: None,
                )

            rollback = Mock()
            with self.assertRaises(RuntimeError):
                namespace_admin.drop_database(
                    conn=conn,
                    db_name="db",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
                    rollback_write=rollback,
                    quote_identifier=lambda name: f'"{name}"',
                    drop_search_backend=lambda *_args: None,
                    clear_index_metadata_versions=lambda *_args: None,
                    invalidate_index_cache=lambda: None,
                    invalidate_collection_id_cache=lambda: None,
                    invalidate_collection_features_cache=lambda: None,
                    clear_profiler=lambda *_args: None,
                )
            rollback.assert_called_once()
        finally:
            conn.close()

    def test_sqlite_runtime_metric_and_active_operation_helpers_delegate_cleanly(self):
        engine = SQLiteEngine()

        engine._record_runtime_opcounter("query", amount=2)
        engine._record_runtime_opcounter("unsupported", amount=4)
        snapshot = engine._runtime_opcounters_snapshot()
        self.assertEqual(snapshot["query"], 2)
        self.assertEqual(snapshot["command"], 0)

        record = engine._begin_active_operation(
            command_name="find",
            operation_type="read",
            namespace="db.coll",
            session_id="s1",
            cursor_id=5,
            connection_id=7,
            comment="trace",
            max_time_ms=10,
            metadata={"mode": "unit"},
        )
        operations = engine._snapshot_active_operations()
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0]["command"], "find")
        self.assertEqual(operations[0]["details"], {"mode": "unit"})

        self.assertFalse(engine._cancel_active_operation("missing"))
        self.assertTrue(engine._cancel_active_operation(record.opid))
        self.assertEqual(engine._snapshot_active_operations()[0]["state"], "cancelled")

        engine._complete_active_operation(record.opid)
        self.assertEqual(engine._snapshot_active_operations(), [])

    def test_sqlite_session_sync_helper_delegates_to_session_runtime(self):
        engine = SQLiteEngine()
        session = Mock()

        with patch.object(engine._session_runtime, "sync_session_state") as sync_mock:
            engine._sync_session_state(session, transaction_active=True)

        sync_mock.assert_called_once_with(session, transaction_active=True)

    def test_sqlite_physical_index_helpers_commit_only_when_needed(self):
        class _FakeConnection:
            def __init__(self):
                self.in_transaction = False
                self.executed: list[str] = []
                self.commits = 0

            def execute(self, sql: str):
                self.executed.append(sql)
                self.in_transaction = True
                return None

            def commit(self):
                self.commits += 1
                self.in_transaction = False

        engine = SQLiteEngine()
        multikey_index = EngineIndexRecord(
            name="mk_1",
            fields=["tags"],
            key=[("tags", 1)],
            unique=False,
            multikey=True,
            multikey_physical_name="mk_physical",
        )
        scalar_index = EngineIndexRecord(
            name="name_1",
            fields=["name"],
            key=[("name", 1)],
            unique=False,
            scalar_physical_name="scalar_physical",
        )

        multikey_conn = _FakeConnection()
        engine._ensure_multikey_physical_indexes_sync(multikey_conn, [multikey_index, multikey_index])
        self.assertEqual(multikey_conn.commits, 1)
        self.assertEqual(len(multikey_conn.executed), 1)
        engine._ensure_multikey_physical_indexes_sync(multikey_conn, [multikey_index])
        self.assertEqual(multikey_conn.commits, 1)

        scalar_conn = _FakeConnection()
        with patch.object(engine, "_supports_scalar_index", return_value=True):
            engine._ensure_scalar_physical_indexes_sync(scalar_conn, [scalar_index])
        self.assertEqual(scalar_conn.commits, 1)
        self.assertEqual(len(scalar_conn.executed), 1)

        scalar_conn = _FakeConnection()
        unsupported_index = EngineIndexRecord(
            name="unsupported_1",
            fields=["name"],
            key=[("name", 1)],
            unique=False,
            scalar_physical_name="unsupported_scalar",
        )
        missing_physical_name = EngineIndexRecord(
            name="missing_scalar",
            fields=["name"],
            key=[("name", 1)],
            unique=False,
        )
        with patch.object(engine, "_supports_scalar_index", side_effect=[False, True]):
            engine._ensure_scalar_physical_indexes_sync(
                scalar_conn,
                [unsupported_index, missing_physical_name],
            )
        self.assertEqual(scalar_conn.commits, 0)
        self.assertEqual(scalar_conn.executed, [])

    def test_sqlite_scalar_sort_helpers_cover_cache_lookup_and_sql_construction(self):
        engine = SQLiteEngine()
        feature_key = ("db", "coll", "uniform_scalar_sort_type:name")
        engine._collection_features_cache[feature_key] = "string"
        self.assertEqual(
            engine._field_has_uniform_scalar_sort_type_in_collection("db", "coll", "name"),
            "string",
        )
        engine._collection_features_cache[feature_key] = False
        self.assertIsNone(engine._field_has_uniform_scalar_sort_type_in_collection("db", "coll", "name"))

        sparse = EngineIndexRecord(
            name="name_sparse",
            fields=["name"],
            key=[("name", 1)],
            unique=False,
            sparse=True,
            scalar_physical_name="scalar_sparse",
        )
        partial = EngineIndexRecord(
            name="name_partial",
            fields=["name"],
            key=[("name", 1)],
            unique=False,
            partial_filter_expression={"active": True},
            scalar_physical_name="scalar_partial",
        )
        usable = EngineIndexRecord(
            name="name_usable",
            fields=["name"],
            key=[("name", 1)],
            unique=False,
            scalar_physical_name="scalar_name",
        )
        with patch.object(engine, "_load_indexes", return_value=[sparse, partial, usable]):
            self.assertIs(engine._find_scalar_sort_index("db", "coll", "name"), usable)
            self.assertIsNone(engine._find_scalar_sort_index("db", "coll", "other"))

        self.assertEqual(engine._resolve_select_clause_for_scalar_sort("document"), "documents.document")
        self.assertEqual(
            engine._resolve_select_clause_for_scalar_sort("storage_key, document"),
            "documents.storage_key, documents.document",
        )
        self.assertEqual(engine._resolve_select_clause_for_scalar_sort("documents.storage_key"), "documents.storage_key")

        sql, params = engine._build_select_statement_with_custom_order(
            select_clause="documents.document",
            from_clause="documents",
            namespace_sql="db_name = ? AND coll_name = ?",
            namespace_params=("db", "coll"),
            where_fragment=("json_extract(document, '$.name') IS NOT NULL", []),
            order_sql="ORDER BY documents.storage_key",
            skip=3,
            limit=None,
        )
        self.assertIn("LIMIT -1 OFFSET ?", sql)
        self.assertEqual(params, ["db", "coll", 3])

        sql, params = engine._build_select_statement_with_custom_order(
            select_clause="documents.document",
            from_clause="documents",
            namespace_sql="db_name = ? AND coll_name = ?",
            namespace_params=("db", "coll"),
            where_fragment=("1 = 1", []),
            order_sql="ORDER BY documents.storage_key",
            skip=0,
            limit=5,
        )
        self.assertIn("LIMIT ?", sql)
        self.assertEqual(params, ["db", "coll", 5])

    def test_sqlite_plan_and_dbref_helpers_cover_recursive_and_cached_paths(self):
        plan = AndCondition(
            (
                EqualsCondition("name", "Ada"),
                NotCondition(GreaterThanCondition("score", 5)),
                OrCondition((EqualsCondition("tier", "pro"), EqualsCondition("region", "es"))),
            )
        )
        self.assertEqual(SQLiteEngine._plan_fields(plan), {"name", "score", "tier", "region"})
        self.assertEqual(SQLiteEngine._comparison_fields(plan), {"score"})

        engine = SQLiteEngine()
        conn = self._connection()
        engine._connection = conn
        try:
            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                (
                    "db",
                    "coll",
                    "1",
                    json.dumps(
                        {
                            "_id": "1",
                            "ref": {
                                "$mongoeco": {
                                    "type": "dbref",
                                    "value": {"collection": "users", "id": "u1"},
                                }
                            },
                        }
                    ),
                ),
            )

            self.assertTrue(engine._field_traverses_dbref_in_collection("db", "coll", "ref.$id"))
            with patch.object(engine, "_require_connection", side_effect=AssertionError("cache should be used")):
                self.assertTrue(engine._field_traverses_dbref_in_collection("db", "coll", "ref.$id"))
        finally:
            conn.close()
            engine._connection = None

    def test_sqlite_search_helpers_cover_listing_and_python_fallback(self):
        engine = SQLiteEngine()
        conn = self._connection()
        engine._connection = conn
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="search_idx")
        try:
            with patch.object(
                engine,
                "_load_search_index_rows",
                return_value=[(definition, "fts_idx", 0.0)],
            ):
                documents = engine._list_search_indexes_sync("db", "coll", name=None, context=None)
            self.assertEqual(len(documents), 1)
            self.assertEqual(documents[0]["name"], "search_idx")

            query = SearchWildcardQuery(
                index_name="search_idx",
                raw_query="Ada*",
                normalized_pattern="ada*",
                paths=("name",),
            )
            with patch.object(engine, "_ensure_search_backend_sync", return_value="fts_idx"), patch.object(
                engine,
                "_supports_fts5",
                return_value=True,
            ), patch.object(
                engine,
                "_sqlite_table_exists",
                return_value=False,
            ), patch.object(
                engine,
                "_load_documents",
                return_value=[("1", {"name": "Ada"}), ("2", {"name": "Grace"})],
            ), patch(
                "mongoeco.engines._sqlite_search_runtime.matches_search_query",
                side_effect=lambda document, **_: document["name"] == "Ada",
            ):
                matched = engine._execute_sqlite_search_query(
                    conn,
                    "db",
                    "coll",
                    definition,
                    query,
                    None,
                    None,
                )
            self.assertEqual(matched, [{"name": "Ada"}])
        finally:
            conn.close()
            engine._connection = None

    def test_sqlite_admin_runtime_covers_collection_existence_and_options_helpers(self):
        conn = self._connection()
        try:
            class _EngineStub:
                _PROFILE_COLLECTION_NAME = "system.profile"

                def __init__(self, connection):
                    self._connection = connection
                    self._profiler = EngineProfiler("sqlite")

            runtime = admin_runtime_module.SQLiteAdminRuntime(_EngineStub(conn))

            self.assertFalse(runtime.collection_exists(conn, "db", "coll"))

            conn.execute(
                "INSERT INTO collections (db_name, coll_name, options_json) VALUES (?, ?, ?)",
                ("db", "coll", '{"capped": true}'),
            )
            self.assertTrue(runtime.collection_exists(conn, "db", "coll"))
            self.assertEqual(runtime.collection_options_or_empty(conn, "db", "coll"), {"capped": True})

            conn.execute("DELETE FROM collections WHERE db_name = ? AND coll_name = ?", ("db", "coll"))
            self.assertEqual(runtime.collection_options_or_empty(conn, "db", "coll"), {})

            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "docs_only", "1", '{"_id":"1"}'),
            )
            conn.execute(
                "INSERT INTO indexes (db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("db", "indexes_only", "ix", "ix_physical", '["name"]', '[["name", 1]]', 0, 0, None, None, 0, None, None),
            )
            conn.execute(
                "INSERT INTO search_indexes (db_name, coll_name, name, index_type, definition_json, physical_name, ready_at_epoch) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("db", "search_only", "search_idx", "search", '{"mappings":{"dynamic":true}}', "fts_idx", 10.0),
            )

            self.assertTrue(runtime.collection_exists(conn, "db", "docs_only"))
            self.assertTrue(runtime.collection_exists(conn, "db", "indexes_only"))
            self.assertTrue(runtime.collection_exists(conn, "db", "search_only"))
        finally:
            conn.close()

    def test_sqlite_admin_runtime_profile_helpers_and_cache_cleanup(self):
        class _EngineStub:
            _PROFILE_COLLECTION_NAME = "system.profile"

            def __init__(self):
                self._profiler = EngineProfiler("sqlite")
                self._index_metadata_versions = {
                    ("db", "coll"): 1,
                    ("db", "other"): 2,
                    ("other", "coll"): 3,
                }

        engine = _EngineStub()
        runtime = admin_runtime_module.SQLiteAdminRuntime(engine)

        self.assertTrue(runtime.is_profile_namespace("system.profile"))
        self.assertFalse(runtime.is_profile_namespace("users"))

        status = runtime.set_profiling_level("db", 2, slow_ms=25)
        self.assertEqual(status.to_document()["level"], 2)
        runtime.record_profile_event(
            "db",
            op="query",
            command={"find": "users"},
            duration_micros=2_500,
            fallback_reason="scan",
            ok=0.0,
            errmsg="boom",
        )
        documents = runtime.profile_namespace_documents("db")
        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["fallbackReason"], "scan")
        self.assertEqual(documents[0]["errmsg"], "boom")
        self.assertEqual(runtime.profile_namespace_document("db", 1), documents[0])
        self.assertEqual(
            runtime.profile_namespace_document(
                "db",
                1,
                projection={"op": 1, "_id": 0},
                dialect=MONGODB_DIALECT_70,
            ),
            {"op": "query"},
        )
        self.assertIsNone(runtime.profile_namespace_document("db", 99))

        runtime.clear_profile_namespace("db")
        self.assertEqual(runtime.profile_namespace_documents("db"), [])

        runtime.clear_index_metadata_versions_for_database("db")
        self.assertEqual(engine._index_metadata_versions, {("other", "coll"): 3})

    def test_sqlite_admin_runtime_delegates_namespace_entrypoints_with_engine_wiring(self):
        conn = self._connection()
        try:
            class _EngineStub:
                _PROFILE_COLLECTION_NAME = "system.profile"

                def __init__(self, connection):
                    self._connection = connection
                    self._profiler = EngineProfiler("sqlite")
                    self._index_metadata_versions = {}
                    self.calls = []

                def _require_connection(self, context):
                    self.calls.append(("require_connection", context))
                    return self._connection

                def _begin_write(self, current, context):
                    self.calls.append(("begin_write", current is self._connection, context))

                def _commit_write(self, current, context):
                    self.calls.append(("commit_write", current is self._connection, context))

                def _rollback_write(self, current, context):
                    self.calls.append(("rollback_write", current is self._connection, context))

                def _quote_identifier(self, value):
                    return f'"{value}"'

                def _drop_search_backend_sync(self, current, physical_name):
                    self.calls.append(("drop_search_backend", current is self._connection, physical_name))

                def _invalidate_index_cache(self):
                    self.calls.append(("invalidate_index_cache",))

                def _invalidate_collection_id_cache(self):
                    self.calls.append(("invalidate_collection_id_cache",))

                def _invalidate_collection_features_cache(self):
                    self.calls.append(("invalidate_collection_features_cache",))

            engine = _EngineStub(conn)
            runtime = admin_runtime_module.SQLiteAdminRuntime(engine)

            with patch.object(
                admin_runtime_module,
                "_sqlite_collection_options",
                return_value={"capped": True},
            ) as collection_options_mock:
                self.assertEqual(runtime.collection_options("db", "coll", context="ctx"), {"capped": True})
            collection_options_mock.assert_called_once()
            self.assertEqual(collection_options_mock.call_args.kwargs["conn"], conn)
            self.assertEqual(collection_options_mock.call_args.kwargs["profile_collection_name"], "system.profile")

            with patch.object(admin_runtime_module, "_sqlite_drop_database") as drop_database_mock:
                runtime.drop_database("db", context="ctx")
            drop_kwargs = drop_database_mock.call_args.kwargs
            self.assertEqual(drop_kwargs["conn"], conn)
            self.assertIs(drop_kwargs["clear_profiler"].__self__, engine._profiler)
            self.assertIs(drop_kwargs["clear_index_metadata_versions"].__self__, runtime)
            drop_kwargs["begin_write"](conn)
            drop_kwargs["commit_write"](conn)
            drop_kwargs["rollback_write"](conn)

            with patch.object(admin_runtime_module, "_sqlite_list_databases", return_value=["db"]) as list_databases_mock:
                self.assertEqual(runtime.list_databases(context="ctx"), ["db"])
            self.assertEqual(list_databases_mock.call_args.kwargs["conn"], conn)
            self.assertIs(list_databases_mock.call_args.kwargs["profiler"], engine._profiler)

            with patch.object(admin_runtime_module, "_sqlite_list_collections", return_value=["coll"]) as list_collections_mock:
                self.assertEqual(runtime.list_collections("db", context="ctx"), ["coll"])
            self.assertEqual(list_collections_mock.call_args.kwargs["conn"], conn)
            self.assertEqual(list_collections_mock.call_args.kwargs["db_name"], "db")
            self.assertEqual(
                list_collections_mock.call_args.kwargs["profile_collection_name"],
                "system.profile",
            )

            self.assertEqual(
                engine.calls,
                [
                    ("require_connection", "ctx"),
                    ("require_connection", "ctx"),
                    ("begin_write", True, "ctx"),
                    ("commit_write", True, "ctx"),
                    ("rollback_write", True, "ctx"),
                    ("require_connection", "ctx"),
                    ("require_connection", "ctx"),
                ],
            )
        finally:
            conn.close()

    def _connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE documents (
                db_name TEXT,
                coll_name TEXT,
                storage_key TEXT,
                document TEXT,
                PRIMARY KEY (db_name, coll_name, storage_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE indexes (
                db_name TEXT,
                coll_name TEXT,
                name TEXT,
                physical_name TEXT,
                fields TEXT,
                keys TEXT,
                unique_flag INTEGER,
                sparse_flag INTEGER,
                partial_filter_json TEXT,
                expire_after_seconds INTEGER,
                multikey_flag INTEGER,
                multikey_physical_name TEXT,
                scalar_physical_name TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE collections (
                collection_id INTEGER PRIMARY KEY AUTOINCREMENT,
                db_name TEXT,
                coll_name TEXT,
                options_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE search_indexes (
                db_name TEXT,
                coll_name TEXT,
                name TEXT,
                index_type TEXT,
                definition_json TEXT,
                physical_name TEXT,
                ready_at_epoch REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE scalar_index_entries (
                collection_id INTEGER,
                index_name TEXT,
                storage_key TEXT,
                element_type TEXT,
                type_score INTEGER,
                element_key TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE multikey_entries (
                collection_id INTEGER,
                index_name TEXT,
                storage_key TEXT,
                element_type TEXT,
                type_score INTEGER,
                element_key TEXT
            )
            """
        )
        return conn

    def test_sqlite_index_admin_helpers_cover_builtin_id_and_rollback_paths(self):
        self.assertEqual(index_admin.list_index_documents([])[0]["name"], "_id_")
        self.assertIn("_id_", index_admin.build_index_information([]))

        conn = self._connection()
        try:
            name = index_admin.create_index(
                conn,
                db_name="db",
                coll_name="coll",
                keys=[("_id", 1)],
                unique=True,
                name=None,
                sparse=False,
                hidden=False,
                partial_filter_expression=None,
                expire_after_seconds=None,
                deadline=None,
                enforce_deadline_fn=lambda _deadline: None,
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                purge_expired_documents=lambda *_args: None,
                mark_index_metadata_changed=lambda *_args: None,
                invalidate_collection_features_cache=lambda *_args: None,
                load_indexes=lambda *_args: [],
                field_traverses_array_in_collection=lambda *_args: False,
                supports_multikey_index=lambda *_args: False,
                physical_index_name=lambda *_args: "physical_idx",
                physical_multikey_index_name=lambda *_args: "multikey_idx",
                physical_scalar_index_name=lambda *_args: "scalar_idx",
                is_builtin_id_index=lambda keys: keys == [("_id", 1)],
                replace_multikey_entries_for_document=lambda *_args: None,
                replace_scalar_entries_for_document=lambda *_args: None,
                load_documents=lambda *_args: [],
                quote_identifier=lambda name: f'"{name}"',
            )
            self.assertEqual(name, "_id_")

            with self.assertRaisesRegex(OperationFailure, "index not found with name"):
                index_admin.drop_index(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    index_or_name="missing",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    load_indexes=lambda *_args: [],
                    lookup_collection_id=lambda *_args: 1,
                    quote_identifier=lambda value: f'"{value}"',
                    mark_index_metadata_changed=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                    is_builtin_id_index=lambda _keys: False,
                )

            with self.assertRaisesRegex(OperationFailure, "index not found with key pattern"):
                index_admin.drop_index(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    index_or_name=[("email", 1)],
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    load_indexes=lambda *_args: [],
                    lookup_collection_id=lambda *_args: 1,
                    quote_identifier=lambda value: f'"{value}"',
                    mark_index_metadata_changed=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                    is_builtin_id_index=lambda _keys: False,
                )

            target = EngineIndexRecord(
                name="email_1",
                fields=["email"],
                key=[("email", 1)],
                unique=False,
                physical_name="idx_email_1",
            )
            with self.assertRaises(RuntimeError):
                index_admin.drop_all_indexes(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
                    rollback_write=Mock(),
                    load_indexes=lambda *_args: [target],
                    lookup_collection_id=lambda *_args: 1,
                    quote_identifier=lambda value: f'"{value}"',
                    mark_index_metadata_changed=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
        finally:
            conn.close()

    def test_sqlite_search_admin_helpers_cover_documents_and_conflicts(self):
        conn = self._connection()
        try:
            definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="search_idx")
            backend_calls = []
            begin = Mock()
            commit = Mock()
            rollback = Mock()

            name = search_admin.create_search_index(
                conn,
                db_name="db",
                coll_name="coll",
                definition=definition,
                deadline=None,
                begin_write=lambda current: begin(current),
                ensure_collection_row=lambda current, db_name, coll_name: current.execute(
                    "INSERT INTO collections (db_name, coll_name, options_json) VALUES (?, ?, ?)",
                    (db_name, coll_name, "{}"),
                ),
                commit_write=lambda current: commit(current),
                rollback_write=lambda current: rollback(current),
                ensure_search_backend=lambda *_args: backend_calls.append("ensure"),
                physical_search_index_name=lambda *_args: "fts_idx",
                pending_ready_at=lambda: 42.0,
            )
            self.assertEqual(name, "search_idx")
            self.assertEqual(backend_calls, ["ensure"])

            rows = conn.execute(
                "SELECT index_type, definition_json, physical_name, ready_at_epoch FROM search_indexes"
            ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], "search")
            self.assertEqual(rows[0][2], "fts_idx")

            documents = search_admin.list_search_index_documents(
                [(definition, "fts_idx", 42.0)],
                is_ready=lambda ready_at: ready_at is not None and ready_at <= 42.0,
                name="search_idx",
            )
            self.assertEqual(len(documents), 1)
            self.assertEqual(documents[0]["name"], "search_idx")
            self.assertTrue(documents[0]["queryable"])

            same_name = search_admin.create_search_index(
                conn,
                db_name="db",
                coll_name="coll",
                definition=definition,
                deadline=None,
                begin_write=lambda _conn: None,
                ensure_collection_row=lambda *_args: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                ensure_search_backend=lambda *_args: (_ for _ in ()).throw(AssertionError("unexpected backend rebuild")),
                physical_search_index_name=lambda *_args: "fts_idx",
                pending_ready_at=lambda: 84.0,
            )
            self.assertEqual(same_name, "search_idx")

            with self.assertRaisesRegex(OperationFailure, "Conflicting search index definition"):
                search_admin.create_search_index(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    definition=SearchIndexDefinition({"mappings": {"dynamic": False}}, name="search_idx"),
                    deadline=None,
                    begin_write=lambda _conn: None,
                    ensure_collection_row=lambda *_args: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda current: rollback(current),
                    ensure_search_backend=lambda *_args: None,
                    physical_search_index_name=lambda *_args: "fts_idx",
                    pending_ready_at=lambda: 84.0,
                )
            self.assertTrue(rollback.called)
        finally:
            conn.close()

    def test_sqlite_namespace_admin_helpers_update_catalog_and_invalidate_runtime(self):
        conn = self._connection()
        try:
            def _collection_exists(current, db_name, coll_name):
                row = current.execute(
                    "SELECT 1 FROM collections WHERE db_name = ? AND coll_name = ?",
                    (db_name, coll_name),
                ).fetchone()
                return row is not None

            def _ensure_collection_row(current, db_name, coll_name, options=None):
                current.execute(
                    "INSERT INTO collections (db_name, coll_name, options_json) VALUES (?, ?, ?)",
                    (db_name, coll_name, "{}" if options is None else str(options)),
                )

            namespace_admin.create_collection(
                conn=conn,
                db_name="db",
                coll_name="coll",
                options={"capped": True},
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                collection_exists=_collection_exists,
                ensure_collection_row=_ensure_collection_row,
            )

            self.assertEqual(
                namespace_admin.list_databases(
                    conn=conn,
                    profiler=EngineProfiler("sqlite"),
                    list_database_names=lambda current: [
                        row[0] for row in current.execute("SELECT DISTINCT db_name FROM collections").fetchall()
                    ],
                ),
                ["db"],
            )

            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "coll", "1", '{"_id":"1"}'),
            )
            conn.execute(
                "INSERT INTO indexes (db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("db", "coll", "idx", "idx_physical", '["name"]', '[["name", 1]]', 0, 0, None, None, 0, None, None),
            )
            conn.execute(
                "INSERT INTO search_indexes (db_name, coll_name, name, index_type, definition_json, physical_name, ready_at_epoch) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("db", "coll", "search_idx", "search", '{"mappings":{"dynamic":true}}', "fts_idx", 10.0),
            )

            invalidate_collection_id_cache = Mock()
            invalidate_collection_features_cache = Mock()
            mark_index_metadata_changed = Mock()
            namespace_admin.rename_collection(
                conn=conn,
                db_name="db",
                coll_name="coll",
                new_name="renamed",
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                collection_exists=_collection_exists,
                mark_index_metadata_changed=mark_index_metadata_changed,
                invalidate_collection_id_cache=invalidate_collection_id_cache,
                invalidate_collection_features_cache=invalidate_collection_features_cache,
            )

            self.assertTrue(_collection_exists(conn, "db", "renamed"))
            self.assertFalse(_collection_exists(conn, "db", "coll"))
            self.assertEqual(mark_index_metadata_changed.call_count, 2)
            self.assertEqual(invalidate_collection_id_cache.call_count, 2)
            self.assertEqual(invalidate_collection_features_cache.call_count, 2)

            clear_versions = Mock()
            invalidate_index_cache = Mock()
            invalidate_collection_id_cache = Mock()
            invalidate_collection_features_cache = Mock()
            clear_profiler = Mock()
            dropped_backends = []

            namespace_admin.drop_database(
                conn=conn,
                db_name="db",
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                quote_identifier=lambda value: f'"{value}"',
                drop_search_backend=lambda _conn, physical_name: dropped_backends.append(physical_name),
                clear_index_metadata_versions=clear_versions,
                invalidate_index_cache=invalidate_index_cache,
                invalidate_collection_id_cache=invalidate_collection_id_cache,
                invalidate_collection_features_cache=invalidate_collection_features_cache,
                clear_profiler=clear_profiler,
            )

            self.assertEqual(dropped_backends, ["fts_idx"])
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM collections WHERE db_name = 'db'").fetchone()[0], 0)
            clear_versions.assert_called_once_with("db")
            invalidate_index_cache.assert_called_once_with()
            invalidate_collection_id_cache.assert_called_once_with()
            invalidate_collection_features_cache.assert_called_once_with()
            clear_profiler.assert_called_once_with("db")
        finally:
            conn.close()

    def test_sqlite_engine_delegates_namespace_admin_runtime_boundaries(self):
        engine = SQLiteEngine()
        engine._connection = sqlite3.connect(":memory:")
        try:
            class StubAdminRuntime:
                def __init__(self):
                    self.calls = []

                def collection_options(self, db_name, coll_name, *, context=None):
                    self.calls.append(("collection_options", db_name, coll_name, context))
                    return {"capped": True}

                def drop_database(self, db_name, *, context=None):
                    self.calls.append(("drop_database", db_name, context))

                def list_databases(self, *, context=None):
                    self.calls.append(("list_databases", context))
                    return ["db"]

                def list_collections(self, db_name, *, context=None):
                    self.calls.append(("list_collections", db_name, context))
                    return ["coll"]

                def record_profile_event(self, db_name, **kwargs):
                    self.calls.append(("record_profile_event", db_name, kwargs["op"]))

                def is_profile_namespace(self, coll_name):
                    self.calls.append(("is_profile_namespace", coll_name))
                    return coll_name == "system.profile"

                def profile_document(self, db_name, profile_id):
                    self.calls.append(("profile_document", db_name, profile_id))
                    return {"_id": profile_id, "ok": 1}

                def clear_profile_namespace(self, db_name):
                    self.calls.append(("clear_profile_namespace", db_name))

                def set_profiling_level(self, db_name, level, *, slow_ms=None):
                    self.calls.append(("set_profiling_level", db_name, level, slow_ms))
                    return {"was": 0, "slowms": slow_ms}

            stub = StubAdminRuntime()
            engine._admin_runtime = stub

            self.assertEqual(engine._collection_options_sync("db", "coll"), {"capped": True})
            engine._drop_database_sync("db")
            self.assertEqual(engine._list_databases_sync(), ["db"])
            self.assertEqual(engine._list_collections_sync("db"), ["coll"])
            engine._record_profile_event("db", op="find", command={"find": "coll"}, duration_micros=10)
            self.assertTrue(engine._is_profile_namespace("system.profile"))
            self.assertEqual(
                engine._get_document_sync("db", "system.profile", 7, projection=None),
                {"_id": 7, "ok": 1},
            )
            engine._drop_collection_sync("db", "system.profile")
            self.assertEqual(
                asyncio.run(engine.set_profiling_level("db", 1, slow_ms=25)),
                {"was": 0, "slowms": 25},
            )
            self.assertEqual(
                stub.calls,
                [
                    ("collection_options", "db", "coll", None),
                    ("drop_database", "db", None),
                    ("list_databases", None),
                    ("list_collections", "db", None),
                    ("record_profile_event", "db", "find"),
                    ("is_profile_namespace", "system.profile"),
                    ("is_profile_namespace", "system.profile"),
                    ("profile_document", "db", 7),
                    ("is_profile_namespace", "system.profile"),
                    ("clear_profile_namespace", "db"),
                    ("set_profiling_level", "db", 1, 25),
                ],
            )
        finally:
            engine._connection.close()

    def test_sqlite_write_helpers_cover_duplicate_snapshot_and_rollback_paths(self):
        conn = self._connection()
        try:
            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "coll", "1", '{"_id": "1"}'),
            )
            with self.assertRaises(DuplicateKeyError):
                write_ops.put_document(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    document={"_id": "1"},
                    overwrite=False,
                    bypass_document_validation=False,
                    storage_key="1",
                    serialized_document='{"_id":"1"}',
                    purge_expired_documents=lambda *_args: None,
                    begin_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    collection_options_or_empty=lambda *_args: {},
                    load_existing_document_for_storage_key=lambda *_args: None,
                    ensure_collection_row=lambda *_args, **_kwargs: None,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    load_indexes=lambda *_args: [],
                    rebuild_multikey_entries_for_document=lambda *_args: None,
                    supports_scalar_index=lambda _idx: False,
                    rebuild_scalar_entries_for_document=lambda *_args: None,
                    load_search_index_rows=lambda *_args: [],
                    replace_search_entries_for_document=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )

            with patch("mongoeco.engines._sqlite_write_ops.enforce_collection_document_validation") as validate_doc:
                results = write_ops.put_documents_bulk(
                    conn,
                    db_name="db",
                    coll_name="bulk",
                    documents=[{"_id": "1"}, {"_id": "1"}],
                    prepared_documents=[
                        ("1", '{"_id":"1"}', []),
                        ("1", '{"_id":"1"}', []),
                    ],
                    snapshot_indexes=[],
                    bypass_document_validation=False,
                    snapshot_options={"validator": {"x": 1}},
                    purge_expired_documents=lambda *_args: None,
                    collection_options_or_empty=lambda *_args: {},
                    load_indexes=lambda *_args: [],
                    load_search_index_rows=lambda *_args: [],
                    begin_write=lambda _conn: None,
                    ensure_collection_row=lambda *_args, **_kwargs: None,
                    lookup_collection_id=lambda *_args: 1,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    delete_multikey_entries_for_storage_key=lambda *_args: None,
                    delete_scalar_entries_for_storage_key=lambda *_args: None,
                    build_multikey_rows_for_document=lambda *_args: [],
                    ensure_multikey_physical_indexes=lambda *_args: None,
                    build_scalar_rows_for_document=lambda *_args: [],
                    ensure_scalar_physical_indexes=lambda *_args: None,
                    replace_search_entries_for_document=lambda *_args: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
                self.assertEqual(results, [True, False])
                self.assertEqual(validate_doc.call_count, 2)

            rollback = Mock()
            with self.assertRaises(RuntimeError):
                write_ops.put_documents_bulk(
                    conn,
                    db_name="db",
                    coll_name="bulk2",
                    documents=[{"_id": "2"}],
                    prepared_documents=[("2", '{"_id":"2"}', [("idx", "str", 1, "k")])],
                    snapshot_indexes=[],
                    bypass_document_validation=True,
                    snapshot_options=None,
                    purge_expired_documents=lambda *_args: None,
                    collection_options_or_empty=lambda *_args: {},
                    load_indexes=lambda *_args: [EngineIndexRecord(name="idx", fields=["name"], key=[("name", 1)], unique=False, multikey=True)],
                    load_search_index_rows=lambda *_args: [],
                    begin_write=lambda _conn: None,
                    ensure_collection_row=lambda *_args, **_kwargs: None,
                    lookup_collection_id=lambda *_args: 1,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    delete_multikey_entries_for_storage_key=lambda *_args: None,
                    delete_scalar_entries_for_storage_key=lambda *_args: None,
                    build_multikey_rows_for_document=lambda *_args: [("idx", "str", 1, "built")],
                    ensure_multikey_physical_indexes=lambda *_args: None,
                    build_scalar_rows_for_document=lambda *_args: [],
                    ensure_scalar_physical_indexes=lambda *_args: None,
                    replace_search_entries_for_document=lambda *_args: (_ for _ in ()).throw(RuntimeError("boom")),
                    commit_write=lambda _conn: None,
                    rollback_write=rollback,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
            rollback.assert_called_once()

            rollback = Mock()
            with self.assertRaises(RuntimeError):
                write_ops.delete_document(
                    conn,
                    db_name="db",
                    coll_name="bulk2",
                    storage_key="2",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
                    rollback_write=rollback,
                    delete_multikey_entries_for_storage_key=lambda *_args: None,
                    delete_scalar_entries_for_storage_key=lambda *_args: None,
                    delete_search_entries_for_storage_key=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
            rollback.assert_called_once()
        finally:
            conn.close()
