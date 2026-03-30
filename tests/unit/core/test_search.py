import unittest

from mongoeco.core.search import (
    SearchPhraseQuery,
    SearchTextQuery,
    SearchVectorQuery,
    build_search_index_document,
    compile_search_text_like_query,
    compile_search_phrase_query,
    compile_search_stage,
    compile_search_text_query,
    compile_vector_search_query,
    iter_searchable_text_entries,
    matches_search_phrase_query,
    matches_search_text_query,
    score_vector_document,
    sqlite_fts5_query,
    validate_search_index_definition,
    validate_search_stage_pipeline,
    vector_field_paths,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import SearchIndexDefinition


class SearchCoreTests(unittest.TestCase):
    def test_validate_search_index_definition_rejects_unsupported_field_type(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {
                    "mappings": {
                        "fields": {
                            "title": {"type": "number"},
                        }
                    }
                },
                index_type="search",
            )

    def test_build_search_index_document_marks_pending_queryable_vector_search(self) -> None:
        document = build_search_index_document(
            SearchIndexDefinition(
                {
                    "fields": [
                        {
                            "type": "vector",
                            "path": "embedding",
                            "numDimensions": 3,
                            "similarity": "cosine",
                        }
                    ]
                },
                name="vec",
                index_type="vectorSearch",
            ),
            ready=False,
        )
        self.assertTrue(document["queryable"])
        self.assertEqual(document["status"], "PENDING")
        self.assertEqual(document["statusDetail"], "pending-build")
        self.assertEqual(document["queryMode"], "vector")
        self.assertTrue(document["experimental"])
        self.assertEqual(document["capabilities"], ["vectorSearch"])
        self.assertIsNone(document["readyAtEpoch"])

    def test_search_index_definition_to_document_tracks_vector_queryability(self) -> None:
        vector = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                        "similarity": "cosine",
                    }
                ]
            },
            name="vec",
            index_type="vectorSearch",
        ).to_document()
        self.assertTrue(vector["queryable"])
        self.assertEqual(vector["status"], "READY")
        self.assertEqual(vector["queryMode"], "vector")
        self.assertTrue(vector["experimental"])

        unsupported = SearchIndexDefinition({"fields": []}, name="vec", index_type="vectorSearch").to_document()
        self.assertFalse(unsupported["queryable"])
        self.assertEqual(unsupported["status"], "UNSUPPORTED")
        self.assertEqual(unsupported["statusDetail"], "unsupported-definition")

    def test_compile_search_text_query_supports_paths(self) -> None:
        query = compile_search_text_query(
            {
                "index": "by_text",
                "text": {
                    "query": "ada lovelace",
                    "path": ["title", "body"],
                },
            }
        )
        self.assertEqual(
            query,
            SearchTextQuery(
                index_name="by_text",
                raw_query="ada lovelace",
                terms=("ada", "lovelace"),
                paths=("title", "body"),
            ),
        )
        self.assertEqual(sqlite_fts5_query(query), '"ada" AND "lovelace"')

    def test_compile_search_phrase_query_supports_paths(self) -> None:
        query = compile_search_phrase_query(
            {
                "index": "by_text",
                "phrase": {
                    "query": "ada lovelace",
                    "path": ["title", "body"],
                },
            }
        )
        self.assertEqual(
            query,
            SearchPhraseQuery(
                index_name="by_text",
                raw_query="ada lovelace",
                paths=("title", "body"),
            ),
        )
        self.assertEqual(sqlite_fts5_query(query), '"ada lovelace"')

    def test_iter_searchable_text_entries_respects_mapping(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "nested": {
                            "fields": {
                                "body": {"type": "string"},
                            }
                        },
                    },
                }
            },
            name="by_text",
        )
        entries = iter_searchable_text_entries(
            {
                "title": "Ada",
                "nested": {"body": "Notes"},
                "ignored": "skip me",
            },
            definition,
        )
        self.assertEqual(entries, [("title", "Ada"), ("nested.body", "Notes")])

    def test_iter_searchable_text_entries_supports_autocomplete_and_token_mappings(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "autocomplete"},
                        "slug": {"type": "token"},
                    },
                }
            },
            name="by_text",
        )
        entries = iter_searchable_text_entries(
            {
                "title": "Ada Lovelace",
                "slug": "ada-lovelace",
            },
            definition,
        )
        self.assertEqual(entries, [("title", "Ada Lovelace"), ("slug", "ada-lovelace")])

    def test_validate_search_stage_pipeline_requires_search_to_be_first(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_stage_pipeline(
                [
                    {"$match": {"x": 1}},
                    {"$search": {"text": {"query": "ada"}}},
                ]
            )

    def test_validate_search_stage_pipeline_rejects_multiple_search_operators(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_stage_pipeline(
                [
                    {"$search": {"index": "by_text", "text": {"query": "ada"}}},
                    {"$vectorSearch": {"index": "vec", "path": "embedding", "queryVector": [1], "limit": 1}},
                ]
            )

    def test_validate_search_stage_pipeline_ignores_non_list_and_non_search_stages(self) -> None:
        validate_search_stage_pipeline({"$search": {"text": {"query": "ada"}}})
        validate_search_stage_pipeline(
            [
                {"$project": {"title": 1}},
                ["not-a-stage"],
                {"$match": {"title": "Ada"}},
            ]
        )

    def test_compile_search_stage_supports_phrase_operator(self) -> None:
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "phrase": {"query": "ada", "path": "title"}},
            ),
            SearchPhraseQuery(
                index_name="by_text",
                raw_query="ada",
                paths=("title",),
            ),
        )

    def test_compile_search_stage_rejects_unsupported_search_operator_keys(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_stage(
                "$search",
                {"index": "by_text", "wildcard": {"query": "ada", "path": "title"}},
            )

    def test_compile_search_stage_rejects_missing_or_conflicting_text_clause(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_stage("$search", {"index": "by_text"})
        with self.assertRaises(OperationFailure):
            compile_search_stage(
                "$search",
                {
                    "index": "by_text",
                    "text": {"query": "ada"},
                    "phrase": {"query": "ada"},
                },
            )

    def test_compile_search_stage_rejects_invalid_operator_and_search_shapes(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_stage("$rankFusion", {})
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query([])
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"index": "", "text": {"query": "ada"}})
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"text": []})
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"text": {"query": "   "}})
        with self.assertRaises(OperationFailure):
            compile_search_text_query({"phrase": {"query": "ada"}})
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query({"text": {"query": "ada"}})

    def test_compile_search_phrase_query_rejects_unsupported_options(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query(
                {
                    "index": "by_text",
                    "phrase": {"query": "ada", "path": "title", "slop": 2},
                }
            )

    def test_compile_search_text_query_supports_wildcard_path(self) -> None:
        query = compile_search_text_query(
            {
                "index": "by_text",
                "text": {"query": "ada", "path": {"wildcard": "*"}},
            }
        )
        self.assertIsNone(query.paths)

    def test_compile_vector_search_query_supports_local_runtime_subset(self) -> None:
        query = compile_vector_search_query(
            {
                "index": "vec",
                "path": "embedding",
                "queryVector": [1, 0, 0],
                "limit": 2,
                "numCandidates": 4,
            }
        )
        self.assertEqual(
            query,
            SearchVectorQuery(
                index_name="vec",
                path="embedding",
                query_vector=(1.0, 0.0, 0.0),
                limit=2,
                num_candidates=4,
            ),
        )

    def test_compile_vector_search_query_rejects_invalid_payload_shapes(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_vector_search_query([])
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"path": "embedding", "queryVector": [1, True], "limit": 1}
            )
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"path": "embedding", "queryVector": [1], "limit": 0}
            )
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {
                    "path": "embedding",
                    "queryVector": [1],
                    "limit": 2,
                    "numCandidates": 1,
                }
            )

    def test_validate_vector_search_index_definition_requires_vector_fields(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"analyzer": "keyword"},
                index_type="vectorSearch",
            )

    def test_validate_search_index_definition_rejects_non_documents_and_unknown_types(self) -> None:
        with self.assertRaises(TypeError):
            validate_search_index_definition([], index_type="search")
        with self.assertRaises(OperationFailure):
            validate_search_index_definition({}, index_type="hybrid")

    def test_score_vector_document_uses_cosine_similarity(self) -> None:
        definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                        "similarity": "cosine",
                    }
                ]
            },
            name="vec",
            index_type="vectorSearch",
        )
        score = score_vector_document(
            {"embedding": [1.0, 0.0, 0.0]},
            definition=definition,
            query=SearchVectorQuery(
                index_name="vec",
                path="embedding",
                query_vector=(1.0, 0.0, 0.0),
                limit=1,
                num_candidates=1,
            ),
        )
        self.assertEqual(score, 1.0)

    def test_score_vector_document_returns_none_for_missing_invalid_or_zero_norm_candidates(self) -> None:
        definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                        "similarity": "cosine",
                    }
                ]
            },
            name="vec",
            index_type="vectorSearch",
        )
        query = SearchVectorQuery(
            index_name="vec",
            path="embedding",
            query_vector=(1.0, 0.0, 0.0),
            limit=1,
            num_candidates=1,
        )

        self.assertIsNone(score_vector_document({}, definition=definition, query=query))
        self.assertIsNone(
            score_vector_document({"embedding": "bad"}, definition=definition, query=query)
        )
        self.assertIsNone(
            score_vector_document({"embedding": [1.0, True, 0.0]}, definition=definition, query=query)
        )
        self.assertIsNone(
            score_vector_document({"embedding": [1.0, 0.0]}, definition=definition, query=query)
        )
        self.assertIsNone(
            score_vector_document({"embedding": [0.0, 0.0, 0.0]}, definition=definition, query=query)
        )

    def test_matches_search_text_and_phrase_queries_against_mapping(self) -> None:
        definition = SearchIndexDefinition(
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
        )
        document = {
            "title": "Ada Lovelace",
            "body": "Analytical engine pioneer",
        }
        self.assertTrue(
            matches_search_text_query(
                document,
                definition=definition,
                query=SearchTextQuery(
                    index_name="by_text",
                    raw_query="Ada pioneer",
                    terms=("Ada", "pioneer"),
                    paths=None,
                ),
            )
        )
        self.assertTrue(
            matches_search_phrase_query(
                document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="Ada Lovelace",
                    paths=("title",),
                ),
            )
        )
        self.assertFalse(
            matches_search_phrase_query(
                document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="engine pioneer",
                    paths=("title",),
                ),
            )
        )

    def test_iter_searchable_text_entries_supports_dynamic_mappings_and_path_filters(self) -> None:
        definition = SearchIndexDefinition({}, name="by_text")
        entries = iter_searchable_text_entries(
            {
                "title": "Ada Lovelace",
                "tags": ["math", "engine"],
                "nested": {"body": "Notes"},
            },
            definition,
        )

        self.assertEqual(
            entries,
            [
                ("title", "Ada Lovelace"),
                ("tags", "math"),
                ("tags", "engine"),
                ("nested.body", "Notes"),
            ],
        )
        self.assertFalse(
            matches_search_text_query(
                {"title": "Ada Lovelace"},
                definition=definition,
                query=SearchTextQuery(
                    index_name="by_text",
                    raw_query="Ada",
                    terms=("Ada",),
                    paths=("body",),
                ),
            )
        )

    def test_vector_field_paths_and_sqlite_query_handle_escaping(self) -> None:
        definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                    },
                    {
                        "type": "vector",
                        "path": "secondary",
                        "numDimensions": 2,
                    },
                ]
            },
            name="vec",
            index_type="vectorSearch",
        )

        self.assertEqual(vector_field_paths(definition), ("embedding", "secondary"))
        self.assertEqual(
            sqlite_fts5_query(
                SearchTextQuery(
                    index_name="by_text",
                    raw_query='ada "lovelace"',
                    terms=('ada', '"lovelace"'),
                    paths=None,
                )
            ),
            '"ada" AND """lovelace"""',
        )

    def test_build_search_index_document_marks_unqueryable_vector_as_unsupported(self) -> None:
        document = build_search_index_document(
            SearchIndexDefinition({"fields": []}, name="vec", index_type="vectorSearch"),
            ready=True,
        )
        self.assertFalse(document["queryable"])
        self.assertEqual(document["status"], "UNSUPPORTED")

    def test_validate_text_search_definition_rejects_invalid_mappings_payloads(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_index_definition({"mappings": {"dynamic": "yes"}}, index_type="search")
        with self.assertRaises(OperationFailure):
            validate_search_index_definition({"mappings": {"fields": []}}, index_type="search")
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"mappings": {"fields": {"title": {"type": "string", "tokenizer": "x"}}}},
                index_type="search",
            )

    def test_validate_text_search_definition_accepts_local_text_mapping_family(self) -> None:
        normalized = validate_search_index_definition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "slug": {"type": "token"},
                        "suggest": {"type": "autocomplete"},
                    },
                }
            },
            index_type="search",
        )
        self.assertEqual(
            normalized["mappings"]["fields"]["suggest"]["type"],
            "autocomplete",
        )

    def test_validate_vector_search_definition_rejects_invalid_similarity_and_path(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": [{"type": "vector", "path": "", "numDimensions": 3, "similarity": "cosine"}]},
                index_type="vectorSearch",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 3, "similarity": "dotProduct"}]},
                index_type="vectorSearch",
            )


if __name__ == "__main__":
    unittest.main()
