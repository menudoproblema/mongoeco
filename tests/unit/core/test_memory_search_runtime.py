import asyncio
import unittest
from unittest.mock import patch

import mongoeco.engines._memory_search_runtime as memory_search_runtime_module
from mongoeco.engines._memory_search_runtime import (
    _matches_vector_postfilter,
    _vector_filter_residual_description,
    execute_search_documents,
    explain_search_documents,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.types import SearchIndexDefinition


class MemorySearchRuntimeTests(unittest.TestCase):
    def test_vector_pruning_summary_returns_none_without_scan_signals(self) -> None:
        self.assertIsNone(
            memory_search_runtime_module._vector_pruning_summary(
                documents_scanned=None,
                prefilter_candidate_count=None,
                candidates_evaluated=None,
                post_candidate_filtered_count=0,
                min_score_filtered_count=0,
            )
        )

    def test_vector_filter_residual_description_covers_none_exact_and_non_exact_modes(self) -> None:
        self.assertIsNone(_vector_filter_residual_description(None, None))
        self.assertEqual(
            _vector_filter_residual_description({"kind": "note"}, None),
            {
                "required": True,
                "reason": "filter-not-candidateable",
                "candidateable": False,
                "exact": False,
                "spec": {"kind": "note"},
            },
        )
        self.assertEqual(
            _vector_filter_residual_description(
                {"kind": "note"},
                {
                    "candidateable": True,
                    "exact": True,
                    "supportedClauseCount": 1,
                    "unsupportedClauseCount": 0,
                },
            ),
            {
                "required": False,
                "reason": None,
                "candidateable": True,
                "exact": True,
                "supportedClauseCount": 1,
                "unsupportedClauseCount": 0,
                "spec": {"kind": "note"},
            },
        )
        self.assertEqual(
            _vector_filter_residual_description(
                {"kind": "note"},
                {
                    "candidateable": True,
                    "exact": False,
                    "supportedClauseCount": 1,
                    "unsupportedClauseCount": 0,
                },
            )["reason"],
            "non-exact-prefilter",
        )

    def test_matches_vector_postfilter_covers_all_candidateable_paths(self) -> None:
        self.assertTrue(_matches_vector_postfilter({"_id": 1}, filter_spec=None))

        with patch.object(
            memory_search_runtime_module,
            "matches_candidateable_filter",
            return_value=False,
        ):
            self.assertFalse(
                _matches_vector_postfilter({"_id": 1}, filter_spec={"kind": "note"})
            )

        with (
            patch.object(
                memory_search_runtime_module,
                "matches_candidateable_filter",
                return_value=None,
            ),
            patch.object(memory_search_runtime_module.QueryEngine, "match", return_value=True),
        ):
            self.assertTrue(
                _matches_vector_postfilter({"_id": 1}, filter_spec={"kind": "note"})
            )

    def test_execute_and_explain_cover_near_compound_and_vector_paths(self) -> None:
        async def _run() -> None:
            engine = MemoryEngine()
            await engine.connect()
            try:
                await engine.put_document(
                    "db",
                    "coll",
                    {"_id": 1, "title": "Ada algorithms", "body": "vector compiler", "kind": "note", "score": 7, "embedding": [1.0, 0.0]},
                )
                await engine.put_document(
                    "db",
                    "coll",
                    {"_id": 2, "title": "Grace notes", "body": "vector systems", "kind": "reference", "score": 10, "embedding": [0.5, 0.5]},
                )
                await engine.put_document(
                    "db",
                    "coll",
                    {"_id": 3, "title": "Other", "body": "misc text", "kind": "note"},
                )
                search_definition = SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "title": {"type": "string"},
                                "body": {"type": "autocomplete"},
                                "kind": {"type": "token"},
                                "score": {"type": "number"},
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
                await engine.create_search_index("db", "coll", search_definition)
                await engine.create_search_index("db", "coll", vector_definition)

                near_docs = await execute_search_documents(
                    engine,
                    "db",
                    "coll",
                    "$search",
                    {"index": "by_text", "near": {"path": "score", "origin": 8, "pivot": 2}},
                    downstream_filter_spec={"kind": {"$regex": "^n"}},
                )
                self.assertEqual([doc["_id"] for doc in near_docs], [1])

                compound_docs = await execute_search_documents(
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
                    result_limit_hint=1,
                    downstream_filter_spec={"kind": {"$regex": "^n"}},
                )
                self.assertEqual([doc["_id"] for doc in compound_docs], [1])

                text_docs = await execute_search_documents(
                    engine,
                    "db",
                    "coll",
                    "$search",
                    {"index": "by_text", "text": {"query": "vector", "path": ["title", "body"]}},
                    result_limit_hint=1,
                    downstream_filter_spec={"kind": "note"},
                )
                self.assertEqual([doc["_id"] for doc in text_docs], [1])

                vector_docs = await execute_search_documents(
                    engine,
                    "db",
                    "coll",
                    "$vectorSearch",
                    {
                        "index": "by_vector",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 2,
                        "filter": {"kind": {"$regex": "^n"}},
                    },
                    downstream_filter_spec={"kind": "note"},
                    result_limit_hint=1,
                )
                self.assertEqual([doc["_id"] for doc in vector_docs], [1])

                vector_explain = await explain_search_documents(
                    engine,
                    "db",
                    "coll",
                    "$vectorSearch",
                    {
                        "index": "by_vector",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 2,
                        "filter": {"kind": "note"},
                        "minScore": 0.95,
                    },
                    result_limit_hint=1,
                    downstream_filter_spec={"kind": {"$regex": "^n"}},
                )
                self.assertEqual(vector_explain.details["filterMode"], "candidate-prefilter")
                self.assertEqual(vector_explain.details["topKLimitHint"], 1)
                self.assertEqual(vector_explain.details["documentsScannedAfterPrefilter"], 1)
                self.assertEqual(vector_explain.details["minScore"], 0.95)
                self.assertEqual(vector_explain.details["documentsFilteredByMinScore"], 0)
                self.assertEqual(vector_explain.details["queryOperator"], "vectorSearch")
                self.assertEqual(vector_explain.details["mode"], "exact")
                self.assertEqual(
                    vector_explain.details["pathSummary"]["resolvedLeafPaths"],
                    ["embedding"],
                )
                self.assertEqual(
                    vector_explain.details["scoreBreakdown"]["scoreField"],
                    "vectorSearchScore",
                )
                self.assertEqual(
                    vector_explain.details["candidatePlan"]["candidateExpansionStrategy"],
                    "exact-baseline",
                )
                self.assertEqual(
                    vector_explain.details["hybridRetrieval"]["filterMode"],
                    "candidate-prefilter",
                )
                self.assertEqual(
                    vector_explain.details["vectorBackend"]["backend"],
                    "memory-exact-baseline",
                )
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_execute_search_documents_covers_guard_and_postfilter_branches(self) -> None:
        async def _run() -> None:
            engine = MemoryEngine()
            await engine.connect()
            try:
                await engine.put_document("db", "coll", {"_id": 1, "title": "Ada", "kind": "note", "embedding": [1.0, 0.0]})
                await engine.put_document("db", "coll", {"_id": 2, "title": "Grace", "kind": "reference", "embedding": [0.0, 1.0]})
                await engine.put_document("db", "coll", {"_id": 3, "title": "Linus", "kind": "note", "embedding": [0.5, 0.5]})
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition(
                        {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}, "kind": {"type": "token"}}}},
                        name="by_text",
                        index_type="search",
                    ),
                )
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition(
                        {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2}]},
                        name="by_vector",
                        index_type="vectorSearch",
                    ),
                )

                with (
                    patch.object(memory_search_runtime_module, "matches_search_query", return_value=True),
                    patch.object(memory_search_runtime_module, "search_near_distance", side_effect=[None, 2.0, 1.0]),
                ):
                    near_docs = await execute_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$search",
                        {"index": "by_text", "near": {"path": "score", "origin": 0, "pivot": 10}},
                        result_limit_hint=1,
                    )
                self.assertEqual([doc["_id"] for doc in near_docs], [3])

                def _text_prefilter(document, _spec):
                    if document["_id"] == 1:
                        return False
                    if document["_id"] == 2:
                        return None
                    return False

                with (
                    patch.object(memory_search_runtime_module, "matches_candidateable_filter", side_effect=_text_prefilter),
                    patch.object(memory_search_runtime_module.QueryEngine, "match", return_value=False),
                ):
                    text_docs = await execute_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$search",
                        {"index": "by_text", "text": {"query": "Ada", "path": "title"}},
                        downstream_filter_spec={"kind": {"$regex": "^x"}},
                    )
                self.assertEqual(text_docs, [])

                with patch.object(engine, "_materialized_vector_index", return_value=None):
                    no_vector_backend = await execute_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$vectorSearch",
                        {"index": "by_vector", "path": "embedding", "queryVector": [1.0, 0.0], "limit": 2},
                    )
                self.assertEqual(no_vector_backend, [])

                missing_path_hits = await execute_search_documents(
                    engine,
                    "db",
                    "coll",
                    "$vectorSearch",
                    {"index": "by_vector", "path": "missing", "queryVector": [1.0, 0.0], "limit": 2},
                )
                self.assertEqual(missing_path_hits, [])

                def _query_postfilter(document, _spec):
                    if document["_id"] == 1:
                        return False
                    if document["_id"] == 2:
                        return None
                    return False

                with (
                    patch.object(memory_search_runtime_module, "candidate_rows_for_vector_filter", side_effect=[(None, None), ([0, 1], {"exact": True})]),
                    patch.object(memory_search_runtime_module, "vector_scores_for_rows", return_value=[(1.0, 0), (0.9, 1)]),
                    patch.object(memory_search_runtime_module, "matches_candidateable_filter", side_effect=_query_postfilter),
                    patch.object(memory_search_runtime_module.QueryEngine, "match", return_value=False),
                ):
                    query_postfilter_hits = await execute_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$vectorSearch",
                        {
                            "index": "by_vector",
                            "path": "embedding",
                            "queryVector": [1.0, 0.0],
                            "limit": 2,
                            "filter": {"kind": {"$regex": "^n"}},
                        },
                    )
                self.assertEqual(query_postfilter_hits, [])

                def _downstream_postfilter(document, _spec):
                    if document["_id"] == 1:
                        return False
                    if document["_id"] == 2:
                        return None
                    return False

                with (
                    patch.object(memory_search_runtime_module, "candidate_rows_for_vector_filter", side_effect=[([0, 1], {"exact": True}), (None, None)]),
                    patch.object(memory_search_runtime_module, "vector_scores_for_rows", return_value=[(1.0, 0), (0.9, 1)]),
                    patch.object(memory_search_runtime_module, "matches_candidateable_filter", side_effect=_downstream_postfilter),
                    patch.object(memory_search_runtime_module.QueryEngine, "match", return_value=False),
                ):
                    downstream_postfilter_hits = await execute_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$vectorSearch",
                        {"index": "by_vector", "path": "embedding", "queryVector": [1.0, 0.0], "limit": 2},
                        downstream_filter_spec={"kind": {"$regex": "^n"}},
                    )
                self.assertEqual(downstream_postfilter_hits, [])
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_explain_search_documents_covers_vector_candidate_intersection_and_postfilters(self) -> None:
        async def _run() -> None:
            engine = MemoryEngine()
            await engine.connect()
            try:
                await engine.put_document("db", "coll", {"_id": 1, "kind": "note", "embedding": [1.0, 0.0]})
                await engine.put_document("db", "coll", {"_id": 2, "kind": "reference", "embedding": [0.5, 0.5]})
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition(
                        {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2}]},
                        name="by_vector",
                        index_type="vectorSearch",
                    ),
                )

                with (
                    patch.object(
                        memory_search_runtime_module,
                        "candidate_rows_for_vector_filter",
                        side_effect=[
                            ([0, 1], {"exact": True}),
                            ([1], {"exact": True}),
                        ],
                    ),
                    patch.object(
                        memory_search_runtime_module,
                        "vector_scores_for_rows",
                        side_effect=[[(0.5, 1)], [(0.5, 1)]],
                    ),
                ):
                    explanation = await explain_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$vectorSearch",
                        {
                            "index": "by_vector",
                            "path": "embedding",
                            "queryVector": [1.0, 0.0],
                            "limit": 2,
                            "filter": {"kind": "reference"},
                        },
                        downstream_filter_spec={"kind": "reference"},
                    )
                self.assertEqual(explanation.details["prefilterCandidateCount"], 1)
                self.assertEqual(explanation.details["documentsScannedAfterPrefilter"], 1)

                with (
                    patch.object(
                        memory_search_runtime_module,
                        "candidate_rows_for_vector_filter",
                        side_effect=[(None, None), (None, None)],
                    ),
                    patch.object(
                        memory_search_runtime_module,
                        "vector_scores_for_rows",
                        side_effect=[[(1.0, 0), (0.9, 1)], [(1.0, 0), (0.9, 1)]],
                    ),
                    patch.object(
                        memory_search_runtime_module,
                        "matches_candidateable_filter",
                        side_effect=[False, True, False],
                    ),
                ):
                    rejected = await explain_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$vectorSearch",
                        {
                            "index": "by_vector",
                            "path": "embedding",
                            "queryVector": [1.0, 0.0],
                            "limit": 2,
                            "filter": {"kind": "note"},
                        },
                        downstream_filter_spec={"kind": "reference"},
                    )
                self.assertEqual(rejected.details["documentsFiltered"], 2)
                self.assertEqual(
                    rejected.details["hybridRetrieval"]["documentsFilteredPostCandidate"],
                    2,
                )
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_execute_search_documents_covers_compound_heap_replacement(self) -> None:
        async def _run() -> None:
            engine = MemoryEngine()
            await engine.connect()
            try:
                for payload in (
                    {"_id": 1, "title": "Ada"},
                    {"_id": 2, "title": "Grace"},
                    {"_id": 3, "title": "Linus"},
                ):
                    await engine.put_document("db", "coll", payload)
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition(
                        {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}},
                        name="by_text",
                        index_type="search",
                    ),
                )

                with (
                    patch.object(memory_search_runtime_module, "matches_search_query", return_value=True),
                    patch.object(
                        memory_search_runtime_module,
                        "search_compound_ranking",
                        side_effect=[
                            (1, 1.0, float("inf")),
                            (2, 2.0, float("inf")),
                            (1, 0.5, float("inf")),
                        ],
                    ),
                ):
                    docs = await execute_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$search",
                        {
                            "index": "by_text",
                            "compound": {
                                "should": [{"exists": {"path": "title"}}],
                                "minimumShouldMatch": 0,
                            },
                        },
                        result_limit_hint=1,
                    )
                self.assertEqual([doc["_id"] for doc in docs], [2])
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_execute_search_documents_covers_compound_without_limit(self) -> None:
        async def _run() -> None:
            engine = MemoryEngine()
            await engine.connect()
            try:
                for payload in (
                    {"_id": 1, "title": "Ada"},
                    {"_id": 2, "title": "Grace"},
                ):
                    await engine.put_document("db", "coll", payload)
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition(
                        {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}},
                        name="by_text",
                        index_type="search",
                    ),
                )

                with (
                    patch.object(memory_search_runtime_module, "matches_search_query", return_value=True),
                    patch.object(
                        memory_search_runtime_module,
                        "search_compound_ranking",
                        side_effect=[(1, 1.0, float("inf")), (2, 2.0, float("inf"))],
                    ),
                ):
                    docs = await execute_search_documents(
                        engine,
                        "db",
                        "coll",
                        "$search",
                        {
                            "index": "by_text",
                            "compound": {
                                "should": [{"exists": {"path": "title"}}],
                                "minimumShouldMatch": 0,
                            },
                        },
                    )
                self.assertEqual([doc["_id"] for doc in docs], [2, 1])
            finally:
                await engine.disconnect()

        asyncio.run(_run())
