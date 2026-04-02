import unittest

import mongoeco.core.search as search_module
from mongoeco.core.search import (
    ClassicTextQuery,
    SearchPhraseQuery,
    SearchTextQuery,
    SearchVectorQuery,
    attach_text_score,
    build_search_index_document,
    classic_text_score,
    compile_classic_text_query,
    is_queryable_search_definition,
    compile_search_text_like_query,
    compile_search_phrase_query,
    compile_search_stage,
    compile_search_text_query,
    compile_vector_search_query,
    iter_searchable_text_entries,
    matches_search_phrase_query,
    matches_search_text_query,
    resolve_classic_text_index,
    split_classic_text_filter,
    score_vector_document,
    sqlite_fts5_query,
    tokenize_classic_text,
    validate_search_index_definition,
    validate_search_stage_pipeline,
    vector_field_paths,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import EngineIndexRecord, SearchIndexDefinition


class SearchCoreTests(unittest.TestCase):
    def test_compile_classic_text_query_and_filter_split(self) -> None:
        remaining, query = split_classic_text_filter(
            {
                "$text": {"$search": "Ada Lovelace", "$caseSensitive": False},
                "kind": "person",
            }
        )

        self.assertEqual(remaining, {"kind": "person"})
        self.assertEqual(
            query,
            ClassicTextQuery(raw_query="Ada Lovelace", terms=("ada", "lovelace")),
        )

    def test_compile_classic_text_query_rejects_sensitive_flags(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_classic_text_query({"$search": "Ada", "$caseSensitive": True})
        with self.assertRaises(OperationFailure):
            compile_classic_text_query({"$search": "Ada", "$diacriticSensitive": True})

    def test_tokenize_and_score_classic_text_query(self) -> None:
        query = compile_classic_text_query({"$search": "Ada algOrithm"})
        self.assertEqual(tokenize_classic_text("Áda wrote algorithms"), ("ada", "wrote", "algorithms"))
        self.assertEqual(
            classic_text_score(
                {"body": "Ada wrote the first algorithm. Ada again."},
                field="body",
                query=query,
            ),
            3.0,
        )
        self.assertIsNone(
            classic_text_score(
                {"body": "Grace Hopper"},
                field="body",
                query=query,
            )
        )
        self.assertEqual(attach_text_score({"_id": 1}, 2.0)["__mongoeco_textScore__"], 2.0)

    def test_resolve_classic_text_index_requires_single_unambiguous_text_index(self) -> None:
        indexes = [
            EngineIndexRecord(name="content_text", fields=["content"], key=[("content", "text")], unique=False),
        ]
        self.assertEqual(resolve_classic_text_index(indexes), ("content_text", "content"))
        with self.assertRaises(OperationFailure):
            resolve_classic_text_index(
                indexes
                + [
                    EngineIndexRecord(name="title_text", fields=["title"], key=[("title", "text")], unique=False),
                ]
            )

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

    def test_is_queryable_search_definition_returns_false_for_unknown_index_type(self) -> None:
        self.assertFalse(
            is_queryable_search_definition(
                SearchIndexDefinition({}, name="hybrid", index_type="hybrid")
            )
        )
        self.assertTrue(is_queryable_search_definition(SearchIndexDefinition({}, name="by_text")))

    def test_build_search_index_document_tracks_ready_text_indexes(self) -> None:
        document = build_search_index_document(
            SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text"),
            ready=True,
            ready_at_epoch=12.5,
        )

        self.assertEqual(document["status"], "READY")
        self.assertEqual(document["statusDetail"], "ready")
        self.assertEqual(document["queryMode"], "text")
        self.assertFalse(document["experimental"])
        self.assertEqual(document["readyAtEpoch"], 12.5)

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
        self.assertEqual(
            compile_search_stage(
                "$vectorSearch",
                {"index": "vec", "path": "embedding", "queryVector": [1], "limit": 1},
            ),
            SearchVectorQuery(
                index_name="vec",
                path="embedding",
                query_vector=(1.0,),
                limit=1,
                num_candidates=1,
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
            compile_search_text_like_query({"text": {"query": "ada", "path": ""}})
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"text": {"query": "ada", "path": []}})
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
                {"path": "", "queryVector": [1], "limit": 1}
            )
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
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"index": "", "path": "embedding", "queryVector": [1], "limit": 1}
            )
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"path": "embedding", "queryVector": [], "limit": 1}
            )
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"path": "embedding", "queryVector": [1], "limit": True}  # type: ignore[arg-type]
            )
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"path": "embedding", "queryVector": [1], "limit": 1, "numCandidates": True}  # type: ignore[arg-type]
            )
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"path": "embedding", "queryVector": [1], "limit": 1, "boost": 2}
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
        self.assertIsNone(
            score_vector_document({"other": [1.0, 0.0, 0.0]}, definition=definition, query=query)
        )
        self.assertIsNone(
            score_vector_document(
                {"embedding": [1.0, 0.0, 0.0]},
                definition=definition,
                query=SearchVectorQuery(
                    index_name="vec",
                    path="other",
                    query_vector=(1.0, 0.0, 0.0),
                    limit=1,
                    num_candidates=1,
                ),
            )
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
        self.assertFalse(
            matches_search_phrase_query(
                {"other": "Ada Lovelace"},
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="Ada",
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
        self.assertEqual(search_module._quote_fts_term('ada "lovelace"'), '"ada ""lovelace"""')

    def test_iter_searchable_text_entries_returns_empty_for_vector_indexes_and_non_document_mappings(self) -> None:
        self.assertEqual(
            iter_searchable_text_entries(
                {"title": "Ada"},
                SearchIndexDefinition(
                    {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 3}]},
                    name="vec",
                    index_type="vectorSearch",
                ),
            ),
            [],
        )
        self.assertEqual(
            iter_searchable_text_entries(
                "Ada",  # type: ignore[arg-type]
                SearchIndexDefinition(
                    {"mappings": {"dynamic": True, "fields": {"title": {"type": "string"}}}},
                    name="by_text",
                ),
            ),
            [],
        )
        self.assertEqual(search_module._collect_text_leaf_entries("Ada", ""), [])
        self.assertEqual(search_module._collect_text_leaf_entries(1, "title"), [])
        self.assertEqual(search_module._collect_text_leaf_entries(["Ada", 1], "title"), [("title", "Ada")])
        self.assertEqual(search_module._collect_dynamic_text_entries({"ok": "Ada", 1: "skip"}), [("ok", "Ada")])
        self.assertEqual(
            search_module._collect_entries_from_mapping(
                {"nested": {"title": "Ada"}},
                {"dynamic": False, "fields": {"nested": {"fields": {"title": {"type": "string"}}}}},
            ),
            [("nested.title", "Ada")],
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
            validate_search_index_definition({"unsupported": True}, index_type="search")
        with self.assertRaises(OperationFailure):
            validate_search_index_definition({"mappings": []}, index_type="search")
        with self.assertRaises(OperationFailure):
            validate_search_index_definition({"mappings": {"fields": {"": {"type": "string"}}}}, index_type="search")
        with self.assertRaises(OperationFailure):
            validate_search_index_definition({"mappings": {"fields": {"title": []}}}, index_type="search")
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"mappings": {"fields": {"title": {"type": "string", "tokenizer": "x"}}}},
                index_type="search",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"mappings": {"fields": {"title": {"type": "document", "extra": True}}}},
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
        nested = validate_search_index_definition(
            {"mappings": {"fields": {"nested": {"fields": {"title": {"type": "string"}}}}}},
            index_type="search",
        )
        self.assertEqual(
            nested["mappings"]["fields"]["nested"]["fields"]["title"]["type"],
            "string",
        )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"mappings": {"fields": {"nested": {"type": "document", "fields": {"title": {"type": "string"}}}}}},
                index_type="search",
            )

    def test_validate_vector_search_definition_rejects_invalid_similarity_and_path(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": [{"type": "vector", "path": "", "numDimensions": 3, "similarity": "cosine"}]},
                index_type="vectorSearch",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 3, "similarity": "manhattan"}]},
                index_type="vectorSearch",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": ["embedding"]},
                index_type="vectorSearch",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": [{"type": "text", "path": "embedding", "numDimensions": 3}]},
                index_type="vectorSearch",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": [{"type": "vector", "path": "embedding", "numDimensions": True}]},
                index_type="vectorSearch",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 3, "extra": 1}]},
                index_type="vectorSearch",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition({"fields": []}, index_type="vectorSearch")

    def test_validate_vector_search_definition_accepts_additional_local_similarities(self) -> None:
        for similarity in ("cosine", "dotProduct", "euclidean"):
            with self.subTest(similarity=similarity):
                normalized = validate_search_index_definition(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 3,
                                "similarity": similarity,
                            }
                        ]
                    },
                    index_type="vectorSearch",
                )
                self.assertEqual(normalized["fields"][0]["similarity"], similarity)

    def test_search_private_helpers_cover_empty_and_invalid_vector_specs(self) -> None:
        self.assertEqual(
            search_module._vector_field_specs(SearchIndexDefinition({}, name="by_text")),
            {},
        )
        self.assertEqual(
            search_module._vector_field_specs(
                SearchIndexDefinition({"fields": "bad"}, name="vec", index_type="vectorSearch")
            ),
            {},
        )
        self.assertEqual(
            search_module._vector_field_specs(
                SearchIndexDefinition(
                    {"fields": ["bad", {"type": "vector", "path": "", "numDimensions": 2}]},
                    name="vec",
                    index_type="vectorSearch",
                )
            ),
            {},
        )
        self.assertIsNone(search_module._cosine_similarity((0.0,), (1.0,)))
        self.assertAlmostEqual(search_module._cosine_similarity((1.0, 0.0), (1.0, 0.0)) or 0.0, 1.0)


if __name__ == "__main__":
    unittest.main()
