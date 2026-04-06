import unittest
from unittest.mock import patch
import datetime
from dataclasses import dataclass

import mongoeco.core.search as search_module
from mongoeco.core._search_contract import TEXT_SEARCH_INDEX_CAPABILITIES, TEXT_SEARCH_OPERATOR_NAMES
from mongoeco.core.search import (
    ClassicTextQuery,
    SearchAutocompleteQuery,
    SearchCompoundQuery,
    SearchEqualsQuery,
    SearchExistsQuery,
    SearchInQuery,
    SearchNearQuery,
    SearchPhraseQuery,
    SearchRegexQuery,
    SearchRangeQuery,
    SearchTextQuery,
    SearchVectorQuery,
    SearchWildcardQuery,
    attach_text_score,
    attach_vector_search_score,
    build_search_index_document,
    classic_text_score,
    compile_search_autocomplete_query,
    compile_classic_text_query,
    compile_search_compound_query,
    compile_search_equals_query,
    compile_search_exists_query,
    compile_search_in_query,
    compile_search_near_query,
    compile_search_regex_query,
    is_queryable_search_definition,
    compile_search_range_query,
    compile_search_text_like_query,
    compile_search_phrase_query,
    compile_search_stage,
    compile_search_text_query,
    compile_vector_search_query,
    compile_search_wildcard_query,
    iter_searchable_text_entries,
    materialize_search_document,
    search_compound_ranking,
    matches_search_autocomplete_query,
    matches_search_compound_query,
    matches_search_equals_query,
    matches_search_exists_query,
    matches_search_in_query,
    matches_search_near_query,
    matches_search_query,
    matches_search_phrase_query,
    matches_search_regex_query,
    matches_search_range_query,
    matches_search_text_query,
    matches_search_wildcard_query,
    resolve_classic_text_index,
    split_classic_text_filter,
    score_vector_document,
    search_near_distance,
    search_query_explain_details,
    search_query_operator_name,
    sqlite_fts5_query,
    strip_search_result_metadata,
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

    def test_classic_text_helpers_cover_invalid_shapes_and_ambiguous_hints(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_classic_text_query([])
        with self.assertRaises(OperationFailure):
            compile_classic_text_query({"$search": ""})
        with self.assertRaises(OperationFailure):
            compile_classic_text_query({"$search": "Ada", "$language": "en"})
        with self.assertRaises(OperationFailure):
            split_classic_text_filter({"$text": None})
        self.assertEqual(tokenize_classic_text(1), ())
        self.assertEqual(search_module.iter_classic_text_values({"tags": [1, "Ada"]}, "tags"), ("Ada",))
        self.assertEqual(search_module.iter_classic_text_values({}, "tags"), ())

        indexes = [
            EngineIndexRecord(name="body_text", fields=["body"], key=[("body", "text")], unique=False),
            EngineIndexRecord(name="title_text", fields=["title"], key=[("title", "text")], unique=False),
        ]
        with self.assertRaisesRegex(OperationFailure, "text index not found with name \\[missing\\]"):
            resolve_classic_text_index(indexes, hinted_name="missing")

        duplicate_named_indexes = [
            EngineIndexRecord(name="dup", fields=["body"], key=[("body", "text")], unique=False),
            EngineIndexRecord(name="dup", fields=["title"], key=[("title", "text")], unique=False),
        ]
        with self.assertRaisesRegex(OperationFailure, "text index not found with name \\[dup\\]"):
            resolve_classic_text_index(duplicate_named_indexes, hinted_name="dup")

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
        self.assertEqual(
            attach_vector_search_score({"_id": 1}, 0.9)["__mongoeco_vectorSearchScore__"],
            0.9,
        )
        self.assertEqual(
            strip_search_result_metadata(
                {
                    "_id": 1,
                    "__mongoeco_textScore__": 2.0,
                    "__mongoeco_vectorSearchScore__": 0.9,
                    "name": "Ada",
                }
            ),
            {"_id": 1, "name": "Ada"},
        )
        self.assertIsNone(
            classic_text_score(
                {"body": [1, 2, 3]},
                field="body",
                query=query,
            )
        )
        self.assertEqual(
            classic_text_score(
                {"title": "Ada", "body": "Algorithm Ada"},
                field=("title", "body"),
                query=query,
            ),
            3.0,
        )
        self.assertIsNone(
            classic_text_score(
                {"body": "Ada"},
                field=(),
                query=query,
            )
        )

    def test_resolve_classic_text_index_requires_single_unambiguous_text_index(self) -> None:
        indexes = [
            EngineIndexRecord(name="content_text", fields=["content"], key=[("content", "text")], unique=False),
        ]
        self.assertEqual(resolve_classic_text_index(indexes), ("content_text", ("content",)))
        self.assertEqual(
            resolve_classic_text_index(
                [
                    EngineIndexRecord(
                        name="content_title_text",
                        fields=["content", "title"],
                        key=[("content", "text"), ("title", "text")],
                        unique=False,
                    ),
                ]
            ),
            ("content_title_text", ("content", "title")),
        )
        with self.assertRaises(OperationFailure):
            resolve_classic_text_index(
                indexes
                + [
                    EngineIndexRecord(name="title_text", fields=["title"], key=[("title", "text")], unique=False),
                ]
            )

    def test_validate_search_index_definition_accepts_extended_scalar_mapping_types(self) -> None:
        normalized = validate_search_index_definition(
            {
                "mappings": {
                    "fields": {
                        "published": {"type": "boolean"},
                        "ownerId": {"type": "objectId"},
                        "externalUuid": {"type": "uuid"},
                    }
                }
            },
            index_type="search",
        )
        self.assertEqual(normalized["mappings"]["fields"]["published"]["type"], "boolean")
        self.assertEqual(normalized["mappings"]["fields"]["ownerId"]["type"], "objectId")
        self.assertEqual(normalized["mappings"]["fields"]["externalUuid"]["type"], "uuid")

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
        self.assertEqual(document["capabilities"], list(TEXT_SEARCH_INDEX_CAPABILITIES))

    def test_search_capabilities_and_operator_registry_share_the_same_contract(self) -> None:
        self.assertEqual(TEXT_SEARCH_INDEX_CAPABILITIES, TEXT_SEARCH_OPERATOR_NAMES)
        self.assertEqual(
            SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text").to_document()["capabilities"],
            list(TEXT_SEARCH_INDEX_CAPABILITIES),
        )

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

    def test_compile_search_phrase_query_supports_slop(self) -> None:
        query = compile_search_phrase_query(
            {
                "index": "by_text",
                "phrase": {
                    "query": "ada lovelace",
                    "path": ["title", "body"],
                    "slop": 2,
                },
            }
        )
        self.assertEqual(
            query,
            SearchPhraseQuery(
                index_name="by_text",
                raw_query="ada lovelace",
                paths=("title", "body"),
                slop=2,
            ),
        )
        self.assertEqual(sqlite_fts5_query(query), '"ada" AND "lovelace"')

    def test_search_compilers_cover_unsupported_keys_and_registry_gaps(self) -> None:
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            compile_search_text_like_query(
                {
                    "index": "by_text",
                    "text": {"query": "ada", "path": "title"},
                    "unsupported": True,
                }
            )
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            compile_search_text_query(
                {
                    "index": "by_text",
                    "text": {"query": "ada", "path": "title", "score": {"boost": 2}},
                }
            )
        autocomplete = compile_search_autocomplete_query(
            {
                "index": "by_text",
                "autocomplete": {
                    "query": "ada",
                    "path": "title",
                    "tokenOrder": "sequential",
                },
            }
        )
        self.assertEqual(autocomplete.token_order, "sequential")
        with self.assertRaisesRegex(OperationFailure, "at least one searchable token"):
            compile_search_autocomplete_query(
                {
                    "index": "by_text",
                    "autocomplete": {"query": "!!!", "path": "title"},
                }
            )
        wildcard = compile_search_wildcard_query(
            {
                "index": "by_text",
                "wildcard": {
                    "query": "ada*",
                    "path": "title",
                    "allowAnalyzedField": True,
                },
            }
        )
        self.assertTrue(wildcard.allow_analyzed_field)
        with patch.dict(search_module._SEARCH_CLAUSE_COMPILERS, {"text": None}, clear=False):
            with self.assertRaisesRegex(OperationFailure, "unsupported local \\$search operator: text"):
                compile_search_text_like_query(
                    {
                        "index": "by_text",
                        "text": {"query": "ada", "path": "title"},
                    }
                )
            with self.assertRaisesRegex(OperationFailure, "unsupported local \\$search operator: text"):
                search_module._compile_search_clause(
                    index_name="by_text",
                    clause_name="text",
                    clause_spec={"query": "ada", "path": "title"},
                )
        with patch.dict(search_module._SEARCH_CLAUSE_COMPILERS, {"text": None}, clear=False):
            with self.assertRaisesRegex(OperationFailure, "uses unsupported operator text"):
                compile_search_compound_query(
                    {
                        "index": "by_text",
                        "compound": {"must": [{"text": {"query": "ada", "path": "title"}}]},
                    }
                )
        self.assertIsInstance(
            search_module._compile_search_clause(
                index_name="by_text",
                clause_name="text",
                clause_spec={"query": "ada", "path": "title"},
            ),
            SearchTextQuery,
        )
        with self.assertRaisesRegex(OperationFailure, "must be a non-empty string"):
            compile_search_autocomplete_query(
                {
                    "index": "by_text",
                    "autocomplete": {"query": "   ", "path": "title"},
                }
            )

    def test_matches_search_autocomplete_query_skips_empty_token_candidates(self) -> None:
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text")
        query = SearchAutocompleteQuery(index_name="by_text", raw_query="ada", terms=("ada",), paths=("title",))

        self.assertFalse(
            matches_search_autocomplete_query(
                {"title": "!!!"},
                definition=definition,
                query=query,
            )
        )

    def test_matches_search_autocomplete_query_supports_sequential_token_order(self) -> None:
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text")
        query = SearchAutocompleteQuery(
            index_name="by_text",
            raw_query="ada alg",
            terms=("ada", "alg"),
            paths=("title",),
            token_order="sequential",
        )

        self.assertTrue(
            matches_search_autocomplete_query(
                {"title": "Ada algorithm handbook"},
                definition=definition,
                query=query,
            )
        )
        self.assertFalse(
            matches_search_autocomplete_query(
                {"title": "Algorithm notes by Ada"},
                definition=definition,
                query=query,
            )
        )

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

    def test_search_compilers_cover_more_invalid_shapes(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"index": "by_text"})
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"index": "by_text", "text": {"query": "ada"}, "wildcard": {"query": "*a*"}})
        with self.assertRaises(OperationFailure):
            compile_search_text_query({"index": "by_text", "text": []})
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query({"index": "by_text", "phrase": {"query": ""}})
        with self.assertRaises(OperationFailure):
            compile_search_autocomplete_query({"index": "by_text", "autocomplete": {"query": "!!!"}})
        with self.assertRaises(OperationFailure):
            compile_search_wildcard_query({"index": "by_text", "wildcard": {"query": ""}})
        with self.assertRaises(OperationFailure):
            compile_search_regex_query({"index": "by_text", "regex": {"query": ""}})
        with self.assertRaises(OperationFailure):
            compile_vector_search_query({"index": "vec", "path": "", "queryVector": [1], "limit": 1})
        with self.assertRaises(OperationFailure):
            compile_vector_search_query({"index": "vec", "path": "embedding", "queryVector": [1], "limit": 1, "filter": []})

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

    def test_wrapper_compilers_reject_wrong_operator_shapes(self) -> None:
        with self.assertRaisesRegex(OperationFailure, "exists specification is required"):
            compile_search_exists_query({"index": "by_text", "text": {"query": "ada"}})
        with self.assertRaisesRegex(OperationFailure, "near specification is required"):
            compile_search_near_query({"index": "by_text", "text": {"query": "ada"}})
        with self.assertRaisesRegex(OperationFailure, "regex specification is required"):
            compile_search_regex_query({"index": "by_text", "text": {"query": "ada"}})
        with self.assertRaisesRegex(OperationFailure, "compound specification is required"):
            compile_search_compound_query({"index": "by_text", "text": {"query": "ada"}})

    def test_compile_search_regex_query_accepts_valid_regex_specification(self) -> None:
        query = compile_search_regex_query(
            {
                "index": "by_text",
                "regex": {"query": "Ada.*", "path": "title"},
            }
        )

        self.assertEqual(query.index_name, "by_text")
        self.assertEqual(query.raw_query, "Ada.*")
        self.assertEqual(query.paths, ("title",))

    def test_compile_vector_search_query_and_search_stage_error_paths(self) -> None:
        with self.assertRaisesRegex(OperationFailure, "requires a document specification"):
            compile_vector_search_query([])
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            compile_vector_search_query(
                {
                    "index": "vec",
                    "path": "embedding",
                    "queryVector": [1.0],
                    "limit": 1,
                    "extra": True,
                }
            )
        with self.assertRaisesRegex(OperationFailure, "non-empty string"):
            compile_vector_search_query(
                {"index": "", "path": "embedding", "queryVector": [1.0], "limit": 1}
            )
        with self.assertRaisesRegex(OperationFailure, "path must be a non-empty string"):
            compile_vector_search_query(
                {"index": "vec", "path": "", "queryVector": [1.0], "limit": 1}
            )
        with self.assertRaisesRegex(OperationFailure, "queryVector must be a non-empty array"):
            compile_vector_search_query(
                {"index": "vec", "path": "embedding", "queryVector": [], "limit": 1}
            )
        with self.assertRaisesRegex(OperationFailure, "limit must be a positive integer"):
            compile_vector_search_query(
                {"index": "vec", "path": "embedding", "queryVector": [1.0], "limit": 0}
            )
        with self.assertRaisesRegex(OperationFailure, "numCandidates must be an integer >= limit"):
            compile_vector_search_query(
                {
                    "index": "vec",
                    "path": "embedding",
                    "queryVector": [1.0],
                    "limit": 1,
                    "numCandidates": 0,
                }
            )
        with self.assertRaisesRegex(OperationFailure, "requires a document specification"):
            compile_search_stage("$search", [])
        with self.assertRaisesRegex(OperationFailure, "unsupported search stage operator"):
            compile_search_stage("$other", {})

    def test_matches_and_ranking_cover_more_search_query_branches(self) -> None:
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text")
        document = {
            "title": "Ada algorithms",
            "body": "Ada wrote algorithm notes",
            "count": 7,
            "published": datetime.datetime(2024, 1, 1, 12, 0, 0),
            "tags": ["ada", "math"],
        }
        prepared = materialize_search_document(document, definition)

        exists_any = SearchExistsQuery(index_name="by_text", paths=None)
        self.assertTrue(matches_search_exists_query(document, definition=definition, query=exists_any, materialized=prepared))
        self.assertFalse(matches_search_exists_query({}, definition=definition, query=exists_any))

        text_query = SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("title", "body"))
        phrase_query = SearchPhraseQuery(index_name="by_text", raw_query="Ada algorithms", paths=("title",))
        autocomplete_query = SearchAutocompleteQuery(index_name="by_text", raw_query="alg", terms=("alg",), paths=("title", "body"))
        wildcard_query = SearchWildcardQuery(index_name="by_text", raw_query="*algorithm*", normalized_pattern="*algorithm*", paths=("body",))
        near_query = SearchNearQuery(index_name="by_text", path="count", origin=10, pivot=4.0, origin_kind="number")
        in_query = SearchInQuery(index_name="by_text", path="count", values=(7.0, 9.0), normalized_values=frozenset({("number", 7.0), ("number", 9.0)}))
        equals_query = SearchEqualsQuery(index_name="by_text", path="count", value=7.0, value_kind="number")
        range_query = SearchRangeQuery(index_name="by_text", path="count", gte=6.0, lt=8.0, bound_kind="number")
        compound_query = SearchCompoundQuery(
            index_name="by_text",
            must=(text_query,),
            should=(autocomplete_query, near_query, in_query, equals_query, range_query),
            filter=(),
            must_not=(),
            minimum_should_match=0,
        )

        self.assertTrue(matches_search_query(document, definition=definition, query=text_query, materialized=prepared))
        self.assertTrue(matches_search_phrase_query(document, definition=definition, query=phrase_query, materialized=prepared))
        self.assertTrue(matches_search_autocomplete_query(document, definition=definition, query=autocomplete_query, materialized=prepared))
        self.assertTrue(matches_search_wildcard_query(document, definition=definition, query=wildcard_query, materialized=prepared))
        self.assertTrue(matches_search_near_query(document, definition=definition, query=near_query, materialized=prepared))
        self.assertTrue(matches_search_in_query(document, definition=definition, query=in_query, materialized=prepared))
        self.assertTrue(matches_search_equals_query(document, definition=definition, query=equals_query, materialized=prepared))
        self.assertTrue(matches_search_range_query(document, definition=definition, query=range_query, materialized=prepared))
        self.assertTrue(matches_search_compound_query(document, definition=definition, query=compound_query, materialized=prepared))
        self.assertEqual(search_module.search_clause_ranking(document, definition=definition, query=text_query, materialized=prepared), (True, 2.0, None))
        self.assertEqual(search_module.search_clause_ranking(document, definition=definition, query=phrase_query, materialized=prepared), (True, 1.0, None))
        self.assertEqual(search_module.search_clause_ranking(document, definition=definition, query=wildcard_query, materialized=prepared), (True, 1.0, None))
        self.assertEqual(
            search_module.search_clause_ranking(
                document,
                definition=definition,
                query=exists_any,
                materialized=prepared,
            ),
            (True, float(len(prepared.searchable_paths)), None),
        )
        self.assertEqual(
            search_module.search_clause_ranking(
                document,
                definition=definition,
                query=in_query,
                materialized=prepared,
            ),
            (True, 1.0, None),
        )
        self.assertEqual(
            search_module.search_clause_ranking(
                document,
                definition=definition,
                query=equals_query,
                materialized=prepared,
            ),
            (True, 1.0, None),
        )
        self.assertEqual(
            search_module.search_clause_ranking(
                document,
                definition=definition,
                query=range_query,
                materialized=prepared,
            ),
            (True, 1.0, None),
        )
        compound_rank = search_module.search_clause_ranking(
            document,
            definition=definition,
            query=compound_query,
            materialized=prepared,
        )
        self.assertTrue(compound_rank[0])
        self.assertGreater(compound_rank[1], 1.0)
        matched_should, should_score, best_near_distance = search_compound_ranking(
            document,
            definition=definition,
            query=compound_query,
            materialized=prepared,
        )
        self.assertEqual(matched_should, 5)
        self.assertGreater(should_score, 0.0)
        self.assertLess(best_near_distance, float("inf"))

    def test_search_helper_functions_cover_materialized_and_near_paths(self) -> None:
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text")
        document = {
            "title": "Ada algorithms",
            "body": ["Compiler pioneer", "Algorithm notes"],
            "published": datetime.date(2024, 1, 1),
            "score": [7, 11],
        }
        prepared = materialize_search_document(document, definition)

        self.assertEqual(search_module._materialized_lowered_values(prepared, None), prepared.lowered_values)
        self.assertTrue(search_module._materialized_lowered_values(prepared, ("title",)))
        self.assertEqual(search_module._materialized_token_sets(prepared, None), tuple(prepared.token_sets_by_path.values()))
        self.assertTrue(search_module._materialized_token_sets(prepared, ("title",)))
        self.assertEqual(search_module._materialized_token_set(prepared, None), prepared.token_set)
        self.assertTrue(search_module._materialized_token_set(prepared, ("body",)))
        self.assertEqual(search_module._materialized_token_counter(prepared, None), prepared.token_counter)
        self.assertTrue(search_module._materialized_token_counter(prepared, ("title",)))
        self.assertEqual(search_module._materialized_token_counters(prepared, None), tuple(prepared.token_counter_by_path.values()))
        self.assertTrue(search_module._materialized_token_counters(prepared, ("title",)))
        self.assertEqual(vector_field_paths(SearchIndexDefinition({"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2}]}, name="vec", index_type="vectorSearch")), ("embedding",))
        self.assertEqual(iter_searchable_text_entries({"x": 1}, SearchIndexDefinition({}, name="plain", index_type="vectorSearch")), [])
        self.assertEqual(sqlite_fts5_query(SearchAutocompleteQuery(index_name="by_text", raw_query="Ada Al", terms=("ada", "al"), paths=("title",))), '"ada"* AND "al"*')
        self.assertEqual(search_module._normalize_search_paths({"wildcard": "*"}), None)
        with self.assertRaisesRegex(OperationFailure, "must be a non-empty string"):
            search_module._normalize_search_paths("")

        self.assertIsNone(search_module._search_near_origin_kind(True))
        self.assertEqual(search_module._search_near_origin_kind(datetime.date(2024, 1, 1)), "date")
        self.assertEqual(search_module._normalize_search_scalar_value(7), ("number", 7.0))
        self.assertEqual(search_module._normalize_search_scalar_value(None), ("null", None))
        self.assertIsNone(search_module._normalize_search_scalar_value(object()))
        self.assertEqual(
            search_module._search_path_scalar_values({"count": [7, "x", True]}, "count"),
            (("number", 7.0), ("string", "x"), ("bool", True)),
        )
        self.assertEqual(
            search_module._search_path_values({"items": [1, 2, 3]}, "items"),
            (1, 2, 3),
        )
        self.assertEqual(
            search_module._search_path_values({"items": [{"score": 7}, {"score": 9}]}, "items.1.score"),
            (9,),
        )
        self.assertEqual(
            search_module._search_path_values({"items": [{"score": 7}]}, "items.9.score"),
            (),
        )
        self.assertEqual(
            search_module._search_path_values({"items": [1, 2, 3]}, "items.score"),
            (),
        )
        self.assertEqual(
            search_module._search_path_values({"items": {"score": 7}}, "items.missing"),
            (),
        )
        self.assertEqual(
            search_module._search_path_values({"items": {"score": 7}}, "items.score.value"),
            (),
        )
        self.assertEqual(
            search_module._search_path_values({"items": {}}, "items.score.value"),
            (),
        )
        self.assertIsNone(search_module._search_near_scalar_distance(True, origin=1.0, origin_kind="number"))
        self.assertIsNone(search_module._search_near_scalar_distance(float("inf"), origin=1.0, origin_kind="number"))
        self.assertIsNone(search_module._search_near_scalar_distance("x", origin=datetime.date(2024, 1, 1), origin_kind="date"))
        self.assertGreater(search_module._datetime_to_sortable_number(datetime.datetime(2024, 1, 1, 1, 2, 3, 4)), 0.0)
        near_date_query = SearchNearQuery(
            index_name="by_text",
            path="published",
            origin=datetime.date(2024, 1, 2),
            pivot=100000.0,
            origin_kind="date",
        )
        near_list_query = SearchNearQuery(
            index_name="by_text",
            path="score",
            origin=10,
            pivot=5.0,
            origin_kind="number",
        )
        self.assertIsNotNone(search_near_distance(document, query=near_date_query))
        self.assertIsNotNone(search_near_distance(document, query=near_list_query))
        self.assertIsNone(search_near_distance({"score": ["bad"]}, query=near_list_query))

    def test_compile_clause_helpers_cover_invalid_subdocuments(self) -> None:
        with self.assertRaisesRegex(OperationFailure, "at least one searchable token"):
            search_module._compile_search_text_clause("by_text", {"query": "!!!"})
        with self.assertRaisesRegex(OperationFailure, "non-empty string"):
            search_module._compile_search_phrase_clause("by_text", {"query": ""})
        with self.assertRaisesRegex(OperationFailure, "at least one searchable token"):
            search_module._compile_search_autocomplete_clause("by_text", {"query": "!!!"})
        with self.assertRaisesRegex(OperationFailure, "non-empty string"):
            search_module._compile_search_wildcard_clause("by_text", {"query": ""})
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            search_module._compile_search_exists_clause("by_text", {"path": "title", "extra": 1})
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            search_module._compile_search_in_clause("by_text", {"path": "title", "value": ["Ada"], "extra": 1})
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            search_module._compile_search_equals_clause("by_text", {"path": "title", "value": "Ada", "extra": 1})
        with self.assertRaisesRegex(OperationFailure, "same value family"):
            search_module._compile_search_range_clause("by_text", {"path": "score", "gte": 1, "lt": datetime.date(2024, 1, 1)})
        with self.assertRaisesRegex(OperationFailure, "requires at least one"):
            search_module._compile_search_range_clause("by_text", {"path": "score"})
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            search_module._compile_search_near_clause("by_text", {"path": "score", "origin": 1, "pivot": 1, "extra": 1})
        with self.assertRaisesRegex(OperationFailure, "origin must be"):
            search_module._compile_search_near_clause("by_text", {"path": "score", "origin": object(), "pivot": 1})
        with self.assertRaisesRegex(OperationFailure, "pivot must be"):
            search_module._compile_search_near_clause("by_text", {"path": "score", "origin": 1, "pivot": 0})

    def test_search_in_equals_range_and_near_helpers_cover_remaining_scalar_branches(self) -> None:
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text")

        self.assertEqual(split_classic_text_filter({"kind": "note"}), ({"kind": "note"}, None))
        self.assertTrue(search_module.is_text_search_query(SearchTextQuery(index_name="by_text", raw_query="ada", terms=("ada",))))
        self.assertFalse(
            search_module.is_text_search_query(
                SearchVectorQuery(index_name="vec", path="embedding", query_vector=(1.0,), limit=1, num_candidates=1)
            )
        )

        with self.assertRaisesRegex(OperationFailure, "in specification is required"):
            compile_search_in_query({"index": "by_text", "text": {"query": "ada"}})
        with self.assertRaisesRegex(OperationFailure, "equals specification is required"):
            compile_search_equals_query({"index": "by_text", "text": {"query": "ada"}})
        with self.assertRaisesRegex(OperationFailure, "range specification is required"):
            compile_search_range_query({"index": "by_text", "text": {"query": "ada"}})
        with self.assertRaisesRegex(OperationFailure, "requires a document specification"):
            search_module._compile_search_in_clause("by_text", [])
        with self.assertRaisesRegex(OperationFailure, "path must be a non-empty string"):
            search_module._compile_search_in_clause("by_text", {"path": "", "value": ["x"]})
        with self.assertRaisesRegex(OperationFailure, "must be a non-empty array"):
            search_module._compile_search_in_clause("by_text", {"path": "kind", "value": []})
        with self.assertRaisesRegex(OperationFailure, "must be null, bool, finite number, string, date or datetime"):
            search_module._compile_search_in_clause("by_text", {"path": "kind", "value": [float("inf")]})
        with self.assertRaisesRegex(OperationFailure, "requires a document specification"):
            search_module._compile_search_equals_clause("by_text", [])
        with self.assertRaisesRegex(OperationFailure, "path must be a non-empty string"):
            search_module._compile_search_equals_clause("by_text", {"path": "", "value": "x"})
        with self.assertRaisesRegex(OperationFailure, "must be null, bool, finite number, string, date or datetime"):
            search_module._compile_search_equals_clause("by_text", {"path": "kind", "value": float("inf")})
        with self.assertRaisesRegex(OperationFailure, "requires a document specification"):
            search_module._compile_search_range_clause("by_text", [])
        with self.assertRaisesRegex(OperationFailure, "path must be a non-empty string"):
            search_module._compile_search_range_clause("by_text", {"path": "", "gte": 1})
        with self.assertRaisesRegex(OperationFailure, "must be a finite number, date or datetime"):
            search_module._compile_search_range_clause("by_text", {"path": "score", "gte": "bad"})
        with self.assertRaisesRegex(OperationFailure, "must be a finite number, date or datetime"):
            search_module._compile_search_range_clause("by_text", {"path": "score", "gte": True})

        self.assertEqual(
            search_module._compile_search_near_clause(
                "by_text",
                {"path": "metrics.score", "origin": 10, "pivot": 2},
            ),
            SearchNearQuery(
                index_name="by_text",
                path="metrics.score",
                origin=10,
                pivot=2.0,
                origin_kind="number",
            ),
        )

        self.assertIsNone(search_module._normalize_search_scalar_value(float("inf")))
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        self.assertEqual(search_module._normalize_search_scalar_value(dt), ("datetime", dt))
        self.assertEqual(search_module._search_path_scalar_values({}, "missing"), ())
        self.assertEqual(search_module._search_path_scalar_values({"meta": {"x": 1}}, "meta"), ())
        in_query = SearchInQuery(
            index_name="by_text",
            path="kind",
            values=("note", True, None),
            normalized_values=frozenset({("string", "note"), ("bool", True), ("null", None)}),
        )
        self.assertTrue(matches_search_in_query({"kind": ["note", "other"]}, definition=definition, query=in_query))
        self.assertFalse(matches_search_in_query({"kind": ["other"]}, definition=definition, query=in_query))

        number_range = SearchRangeQuery(index_name="by_text", path="score", gt=5.0, gte=6.0, lt=10.0, lte=9.0, bound_kind="number")
        self.assertFalse(search_module._search_range_matches_value(candidate_kind="string", candidate_value="7", query=number_range))
        self.assertFalse(search_module._search_range_matches_value(candidate_kind="number", candidate_value=5.0, query=number_range))
        self.assertFalse(search_module._search_range_matches_value(candidate_kind="number", candidate_value=5.5, query=number_range))
        self.assertFalse(search_module._search_range_matches_value(candidate_kind="number", candidate_value=10.0, query=number_range))
        self.assertFalse(search_module._search_range_matches_value(candidate_kind="number", candidate_value=9.5, query=number_range))
        self.assertTrue(search_module._search_range_matches_value(candidate_kind="number", candidate_value=7.0, query=number_range))

        must_query = SearchCompoundQuery(
            index_name="by_text",
            must=(SearchTextQuery(index_name="by_text", raw_query="missing", terms=("missing",), paths=("title",)),),
        )
        filter_query = SearchCompoundQuery(
            index_name="by_text",
            filter=(SearchEqualsQuery(index_name="by_text", path="kind", value="note", value_kind="string"),),
        )
        self.assertFalse(
            matches_search_compound_query(
                {"title": "Ada algorithms", "kind": "reference"},
                definition=definition,
                query=must_query,
            )
        )
        self.assertFalse(
            matches_search_compound_query(
                {"title": "Ada algorithms", "kind": "reference"},
                definition=definition,
                query=filter_query,
            )
        )

        no_near_match = SearchNearQuery(index_name="by_text", path="score", origin=10, pivot=1.0, origin_kind="number")
        self.assertEqual(
            search_module.search_clause_ranking(
                {"score": [1, 2]},
                definition=definition,
                query=no_near_match,
            ),
            (False, 0.0, None),
        )
        self.assertIsNone(search_module._search_near_scalar_distance(1, origin=1, origin_kind="unsupported"))

    def test_search_wrapper_success_and_fallback_ranking_paths_are_covered(self) -> None:
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text")
        exists_query = compile_search_exists_query({"index": "by_text", "exists": {"path": "title"}})
        self.assertEqual(exists_query, SearchExistsQuery(index_name="by_text", paths=("title",)))

        failing_compound = SearchCompoundQuery(
            index_name="by_text",
            must=(SearchTextQuery(index_name="by_text", raw_query="missing", terms=("missing",), paths=("title",)),),
        )
        self.assertEqual(
            search_module.search_clause_ranking(
                {"title": "Ada"},
                definition=definition,
                query=failing_compound,
            ),
            (False, 0.0, None),
        )

        @dataclass(frozen=True, slots=True)
        class _FallbackQuery:
            token: str = "ok"

        def _fallback_matcher(document, definition, query, materialized):
            del definition
            del query
            del materialized
            return bool(document.get("ok"))

        with patch.dict(search_module._SEARCH_QUERY_MATCHERS, {_FallbackQuery: _fallback_matcher}, clear=False):
            self.assertEqual(
                search_module.search_clause_ranking(
                    {"ok": True},
                    definition=definition,
                    query=_FallbackQuery(),
                ),
                (True, 1.0, None),
            )
            self.assertEqual(
                search_module.search_clause_ranking(
                    {"ok": False},
                    definition=definition,
                    query=_FallbackQuery(),
                ),
                (False, 0.0, None),
            )

        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            search_module._compile_search_range_clause("by_text", {"path": "score", "gte": 1, "boost": 2})
        with self.assertRaisesRegex(OperationFailure, "must be a finite number, date or datetime"):
            search_module._compile_search_range_clause("by_text", {"path": "score", "gte": float("inf")})
        self.assertIsNone(search_near_distance({}, query=SearchNearQuery(index_name="by_text", path="score", origin=10, pivot=1.0, origin_kind="number")))

    def test_search_explain_and_operator_helpers_cover_remaining_shapes(self) -> None:
        vector_query = SearchVectorQuery(
            index_name="vec",
            path="embedding",
            query_vector=(1.0, 0.0),
            limit=2,
            num_candidates=3,
            filter_spec={"kind": "keep"},
            min_score=0.75,
        )
        self.assertEqual(search_query_operator_name(vector_query), "vectorSearch")
        details = search_query_explain_details(vector_query)
        self.assertEqual(details["path"], "embedding")
        self.assertEqual(details["filter"], {"kind": "keep"})
        self.assertEqual(details["numCandidates"], 3)
        self.assertEqual(details["minScore"], 0.75)
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

    def test_compile_search_stage_supports_autocomplete_and_wildcard_operators(self) -> None:
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "autocomplete": {"query": "Ada Lov", "path": "title"}},
            ),
            SearchAutocompleteQuery(
                index_name="by_text",
                raw_query="Ada Lov",
                terms=("ada", "lov"),
                paths=("title",),
            ),
        )
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "exists": {"path": "title"}},
            ),
            SearchExistsQuery(
                index_name="by_text",
                paths=("title",),
            ),
        )
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "in": {"path": "kind", "value": ["note", "reference", "note"]}},
            ),
            SearchInQuery(
                index_name="by_text",
                path="kind",
                values=("note", "reference"),
                normalized_values=frozenset({("string", "note"), ("string", "reference")}),
            ),
        )
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "equals": {"path": "kind", "value": "note"}},
            ),
            SearchEqualsQuery(
                index_name="by_text",
                path="kind",
                value="note",
                value_kind="string",
            ),
        )
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "range": {"path": "score", "gte": 5, "lt": 10}},
            ),
            SearchRangeQuery(
                index_name="by_text",
                path="score",
                gte=5.0,
                lt=10.0,
                bound_kind="number",
            ),
        )
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "near": {"path": "score", "origin": 10, "pivot": 2}},
            ),
            SearchNearQuery(
                index_name="by_text",
                path="score",
                origin=10,
                pivot=2.0,
                origin_kind="number",
            ),
        )

    def test_compound_and_clause_compilers_cover_more_error_paths(self) -> None:
        self.assertEqual(
            search_module._compile_search_clause(
                index_name="by_text",
                clause_name="exists",
                clause_spec={"path": "title"},
            ),
            SearchExistsQuery(index_name="by_text", paths=("title",)),
        )
        self.assertEqual(
            compile_search_in_query(
                {"index": "by_text", "in": {"path": "kind", "value": ["note", "reference"]}}
            ),
            SearchInQuery(
                index_name="by_text",
                path="kind",
                values=("note", "reference"),
                normalized_values=frozenset({("string", "note"), ("string", "reference")}),
            ),
        )
        self.assertEqual(
            compile_search_equals_query(
                {"index": "by_text", "equals": {"path": "kind", "value": "note"}}
            ),
            SearchEqualsQuery(index_name="by_text", path="kind", value="note", value_kind="string"),
        )
        self.assertEqual(
            compile_search_range_query(
                {"index": "by_text", "range": {"path": "score", "gte": 5, "lt": 10}}
            ),
            SearchRangeQuery(index_name="by_text", path="score", gte=5.0, lt=10.0, bound_kind="number"),
        )
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"index": "by_text", "compound": []})
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"index": "by_text", "compound": {}})
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"index": "by_text", "compound": {"unsupported": []}})
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"index": "by_text", "compound": {"must": [1]}})
        with self.assertRaises(OperationFailure):
            compile_search_compound_query(
                {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "ada"}, "phrase": {"query": "ada"}}],
                    },
                }
            )

    def test_compile_search_stage_supports_compound_operator(self) -> None:
        self.assertEqual(
            compile_search_stage(
                "$search",
                {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "ada", "path": "title"}}],
                        "should": [{"wildcard": {"query": "*engine*", "path": "body"}}],
                    },
                },
            ),
            SearchCompoundQuery(
                index_name="by_text",
                must=(
                    SearchTextQuery(
                        index_name="by_text",
                        raw_query="ada",
                        terms=("ada",),
                        paths=("title",),
                    ),
                ),
                should=(
                    SearchWildcardQuery(
                        index_name="by_text",
                        raw_query="*engine*",
                        normalized_pattern="*engine*",
                        paths=("body",),
                    ),
                ),
                minimum_should_match=0,
            ),
        )

    def test_search_query_explain_details_reuse_operator_contract(self) -> None:
        phrase = compile_search_phrase_query(
            {
                "index": "by_text",
                "phrase": {"query": "Ada Lovelace", "path": "title", "slop": 1},
            }
        )
        wildcard = compile_search_wildcard_query(
            {
                "index": "by_text",
                "wildcard": {"query": "Ada*", "path": "title"},
            }
        )
        compound = compile_search_compound_query(
            {
                "index": "by_text",
                "compound": {
                    "must": [{"text": {"query": "Ada", "path": "title"}}],
                },
            }
        )
        vector = compile_vector_search_query(
            {
                "index": "vec",
                "path": "embedding",
                "queryVector": [1, 2, 3],
                "limit": 2,
            }
        )
        near = compile_search_near_query(
            {
                "index": "by_text",
                "near": {
                    "path": "score",
                    "origin": datetime.datetime(2024, 1, 1, 12, 0, 0),
                    "pivot": 60.0,
                },
            }
        )
        in_query = compile_search_in_query(
            {
                "index": "by_text",
                "in": {"path": "kind", "value": ["note", "reference"]},
            }
        )
        equals = compile_search_equals_query(
            {
                "index": "by_text",
                "equals": {"path": "kind", "value": "note"},
            }
        )
        range_query = compile_search_range_query(
            {
                "index": "by_text",
                "range": {"path": "score", "gte": 5, "lt": 10},
            }
        )

        self.assertEqual(search_query_operator_name(in_query), "in")
        self.assertEqual(search_query_operator_name(phrase), "phrase")
        self.assertEqual(search_query_explain_details(phrase)["slop"], 1)
        self.assertEqual(search_query_explain_details(in_query)["value"], ["note", "reference"])
        self.assertEqual(search_query_operator_name(wildcard), "wildcard")
        self.assertEqual(search_query_explain_details(wildcard)["paths"], ["title"])
        self.assertEqual(search_query_operator_name(equals), "equals")
        self.assertEqual(search_query_explain_details(equals)["value"], "note")
        self.assertEqual(
            search_query_explain_details(equals)["pathSummary"],
            {
                "all": ["kind"],
                "pathCount": 1,
                "multiPath": False,
                "usesEmbeddedPaths": False,
                "embeddedPaths": [],
                "parentPaths": ["kind"],
                "leafPaths": [],
                "sections": ["equals"],
            },
        )
        self.assertEqual(search_query_operator_name(range_query), "range")
        self.assertEqual(search_query_explain_details(range_query)["range"]["boundKind"], "number")
        self.assertEqual(
            search_query_explain_details(range_query)["pathSummary"],
            {
                "all": ["score"],
                "pathCount": 1,
                "multiPath": False,
                "usesEmbeddedPaths": False,
                "embeddedPaths": [],
                "parentPaths": ["score"],
                "leafPaths": [],
                "sections": ["range"],
            },
        )
        self.assertEqual(search_query_operator_name(near), "near")
        self.assertEqual(search_query_explain_details(near)["originKind"], "date")
        self.assertEqual(search_query_operator_name(compound), "compound")
        self.assertEqual(search_query_explain_details(compound)["compound"]["must"], 1)
        self.assertEqual(search_query_operator_name(vector), "vectorSearch")
        vector_details = search_query_explain_details(vector)
        self.assertEqual(vector_details["path"], "embedding")
        self.assertEqual(
            vector_details["querySemantics"],
            {
                "matchingMode": "nearest-neighbor",
                "scope": "local-vector-tier",
                "requiresLeadingStage": True,
                "supportsStructuredFilter": True,
            },
        )
        self.assertEqual(
            vector_details["scoreBreakdown"],
            {
                "similarity": "cosine",
                "scoreField": "vectorSearchScore",
                "scoreDirection": "higher-is-better",
                "scoreFormula": "dot(query, candidate) / (||query|| * ||candidate||)",
                "minScoreApplied": False,
            },
        )
        self.assertEqual(
            vector_details["candidatePlan"],
            {
                "requestedCandidates": 2,
                "resultLimit": 2,
                "structuredFilterPresent": False,
                "minScorePresent": False,
            },
        )
        self.assertEqual(
            vector_details["pathSummary"],
            {
                "all": ["embedding"],
                "pathCount": 1,
                "multiPath": False,
                "usesEmbeddedPaths": False,
                "embeddedPaths": [],
                "parentPaths": ["embedding"],
                "leafPaths": [],
                "sections": ["vectorSearch"],
            },
        )
        self.assertEqual(
            compile_search_stage(
                "$search",
                {"index": "by_text", "wildcard": {"query": "Ada*", "path": "title"}},
            ),
            SearchWildcardQuery(
                index_name="by_text",
                raw_query="Ada*",
                normalized_pattern="ada*",
                paths=("title",),
            ),
        )

    def test_search_matchers_cover_empty_entries_and_non_matching_paths(self) -> None:
        definition = SearchIndexDefinition(
            {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}},
            name="by_text",
        )
        self.assertFalse(
            matches_search_autocomplete_query(
                {"title": ""},
                definition=definition,
                query=SearchAutocompleteQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("title",)),
            )
        )
        self.assertFalse(
            matches_search_wildcard_query(
                {"title": ""},
                definition=definition,
                query=SearchWildcardQuery(index_name="by_text", raw_query="*ada*", normalized_pattern="*ada*", paths=("title",)),
            )
        )
        self.assertFalse(
            matches_search_query(
                {"title": "Ada"},
                definition=definition,
                query=SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("body",)),
            )
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
        with self.assertRaises(OperationFailure):
            compile_search_stage(
                "$search",
                {
                    "index": "by_text",
                    "text": {"query": "ada"},
                    "wildcard": {"query": "ad*"},
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
        with self.assertRaises(OperationFailure):
            compile_search_autocomplete_query({"text": {"query": "ada"}})
        with self.assertRaises(OperationFailure):
            compile_search_wildcard_query({"text": {"query": "ada"}})
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"wildcard": {"query": 1}})
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"text": {"query": "ada"}})
        with self.assertRaises(OperationFailure):
            compile_search_text_like_query({"compound": []})

    def test_compile_search_compound_query_validates_clause_structure(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"compound": {}})
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"compound": {"must": {}}})
        with self.assertRaises(OperationFailure):
            compile_search_compound_query(
                {"compound": {"must": [{"text": {"query": "ada"}}, {"phrase": {"query": "ada"}}], "minimumShouldMatch": 1}}
            )
        with self.assertRaises(OperationFailure):
            compile_search_compound_query({"compound": {"minimumShouldMatch": 1, "must": [{"text": {"query": "ada"}}]}})
        query = compile_search_compound_query(
            {
                "index": "by_text",
                "compound": {
                    "must": [{"text": {"query": "ada"}}],
                    "should": [{"phrase": {"query": "analytical engine"}}],
                    "minimumShouldMatch": 1,
                },
            }
        )
        self.assertEqual(query.minimum_should_match, 1)

    def test_compile_search_phrase_query_rejects_invalid_slop(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query(
                {
                    "index": "by_text",
                    "phrase": {"query": "ada", "path": "title", "slop": -1},
                }
            )
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query(
                {
                    "index": "by_text",
                    "phrase": {"query": "ada", "path": "title", "slop": True},
                }
            )
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query(
                {
                    "index": "by_text",
                    "phrase": {"query": "ada", "path": "title", "slop": 1.5},
                }
            )

    def test_compile_search_phrase_and_regex_queries_reject_unsupported_shapes(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query(
                {
                    "index": "by_text",
                    "phrase": {"query": "ada", "path": "title", "boost": 2},
                }
            )
        with self.assertRaises(OperationFailure):
            compile_search_phrase_query(
                {
                    "index": "by_text",
                    "phrase": {"query": "!!!", "path": "title"},
                }
            )
        query = compile_search_regex_query(
            {
                "index": "by_text",
                "regex": {"query": "Ada.*", "path": "title", "flags": "i"},
            }
        )
        self.assertEqual(query.flags, "i")
        with self.assertRaises(OperationFailure):
            compile_search_regex_query(
                {
                    "index": "by_text",
                    "regex": {"query": "Ada.*", "path": "title", "flags": "x"},
                }
            )

    def test_compile_search_autocomplete_and_wildcard_queries_support_paths(self) -> None:
        autocomplete = compile_search_autocomplete_query(
            {
                "index": "by_text",
                "autocomplete": {"query": "Ada Lov", "path": ["title", "body"]},
            }
        )
        wildcard = compile_search_wildcard_query(
            {
                "index": "by_text",
                "wildcard": {"query": "*algorithm*", "path": ["title", "body"]},
            }
        )
        self.assertEqual(
            autocomplete,
            SearchAutocompleteQuery(
                index_name="by_text",
                raw_query="Ada Lov",
                terms=("ada", "lov"),
                paths=("title", "body"),
            ),
        )
        self.assertEqual(sqlite_fts5_query(autocomplete), '"ada"* AND "lov"*')
        self.assertEqual(
            wildcard,
            SearchWildcardQuery(
                index_name="by_text",
                raw_query="*algorithm*",
                normalized_pattern="*algorithm*",
                paths=("title", "body"),
            ),
        )

    def test_compile_search_text_like_query_supports_stage_options(self) -> None:
        query = compile_search_text_like_query(
            {
                "index": "by_text",
                "text": {"query": "Ada", "path": "body"},
                "count": {"type": "total"},
                "highlight": {"path": ["body"], "maxChars": 32},
                "facet": {"path": "kind", "numBuckets": 4},
            }
        )
        self.assertEqual(query.stage_options.count.mode, "total")
        self.assertEqual(query.stage_options.highlight.paths, ("body",))
        self.assertEqual(query.stage_options.highlight.max_chars, 32)
        self.assertEqual(query.stage_options.facet.path, "kind")
        self.assertEqual(query.stage_options.facet.num_buckets, 4)

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
                "minScore": 0.75,
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
                min_score=0.75,
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
                {"path": "embedding", "queryVector": [1], "limit": 1, "minScore": "bad"}  # type: ignore[arg-type]
            )
        with self.assertRaises(OperationFailure):
            compile_vector_search_query(
                {"path": "embedding", "queryVector": [1], "limit": 1, "minScore": float("inf")}
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

    def test_matches_search_phrase_queries_with_slop_and_multiple_paths(self) -> None:
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
            "title": "Ada wrote the first truly published algorithm",
            "body": "The first practical algorithm was described by Ada.",
        }
        self.assertFalse(
            matches_search_phrase_query(
                document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="first algorithm",
                    paths=("title",),
                    slop=0,
                ),
            )
        )
        self.assertTrue(
            matches_search_phrase_query(
                document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="first algorithm",
                    paths=("title",),
                    slop=2,
                ),
            )
        )
        self.assertFalse(
            matches_search_phrase_query(
                document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="first algorithm",
                    paths=("title",),
                    slop=1,
                ),
            )
        )
        self.assertTrue(
            matches_search_phrase_query(
                document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="practical algorithm",
                    paths=("title", "body"),
                    slop=0,
                ),
            )
        )

    def test_matches_search_autocomplete_and_wildcard_queries_against_mapping(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "autocomplete"},
                        "body": {"type": "string"},
                    },
                }
            },
            name="by_text",
        )

    def test_matches_search_regex_query_rejects_invalid_patterns(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                    },
                }
            },
            name="by_text",
        )
        self.assertFalse(
            search_module.matches_search_regex_query(
                {"title": "Ada algorithms"},
                definition=definition,
                query=SearchRegexQuery(index_name="by_text", raw_query="(", paths=("title",)),
            )
        )

    def test_matches_search_regex_query_supports_flags(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "body": {"type": "string"},
                    },
                }
            },
            name="by_text",
        )
        self.assertTrue(
            matches_search_regex_query(
                {"body": "Ada\nalgorithm"},
                definition=definition,
                query=SearchRegexQuery(
                    index_name="by_text",
                    raw_query="ada.*algorithm",
                    paths=("body",),
                    flags="is",
                ),
            )
        )

    def test_search_phrase_ranking_handles_tokenless_queries_and_short_values(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                    },
                }
            },
            name="by_text",
        )
        short_document = {"title": "Ada"}
        self.assertEqual(
            search_module.search_clause_ranking(
                short_document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="Ada Lovelace",
                    paths=("title",),
                ),
            ),
            (False, 0.0, None),
        )
        self.assertEqual(
            search_module.search_clause_ranking(
                short_document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="!!!",
                    paths=("title",),
                ),
            ),
            (False, 0.0, None),
        )

    def test_matches_search_compound_queries_against_mapping(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "autocomplete"},
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
            matches_search_compound_query(
                document,
                definition=definition,
                query=SearchCompoundQuery(
                    index_name="by_text",
                    must=(
                        SearchAutocompleteQuery(
                            index_name="by_text",
                            raw_query="ada",
                            terms=("ada",),
                            paths=("title",),
                        ),
                    ),
                    filter=(
                        SearchWildcardQuery(
                            index_name="by_text",
                            raw_query="*engine*",
                            normalized_pattern="*engine*",
                            paths=("body",),
                        ),
                    ),
                ),
            )
        )
        self.assertFalse(
            matches_search_query(
                document,
                definition=definition,
                query=SearchCompoundQuery(
                    index_name="by_text",
                    must=(
                        SearchTextQuery(
                            index_name="by_text",
                            raw_query="ada",
                            terms=("ada",),
                            paths=("title",),
                        ),
                    ),
                    must_not=(
                        SearchPhraseQuery(
                            index_name="by_text",
                            raw_query="Analytical engine",
                            paths=("body",),
                        ),
                    ),
                ),
            )
        )
        self.assertTrue(
            matches_search_query(
                document,
                definition=definition,
                query=SearchCompoundQuery(
                    index_name="by_text",
                    should=(
                        SearchPhraseQuery(
                            index_name="by_text",
                            raw_query="Ada Lovelace",
                            paths=("title",),
                        ),
                        SearchWildcardQuery(
                            index_name="by_text",
                            raw_query="*compiler*",
                            normalized_pattern="*compiler*",
                            paths=("body",),
                        ),
                    ),
                    minimum_should_match=1,
                ),
            )
        )
        document = {
            "title": "Ada Lovelace",
            "body": "Analytical engine pioneer",
        }
        self.assertTrue(
            matches_search_autocomplete_query(
                document,
                definition=definition,
                query=SearchAutocompleteQuery(
                    index_name="by_text",
                    raw_query="ada lov",
                    terms=("ada", "lov"),
                    paths=("title",),
                ),
            )
        )
        self.assertFalse(
            matches_search_autocomplete_query(
                document,
                definition=definition,
                query=SearchAutocompleteQuery(
                    index_name="by_text",
                    raw_query="grace",
                    terms=("grace",),
                    paths=("title",),
                ),
            )
        )
        self.assertTrue(
            matches_search_wildcard_query(
                document,
                definition=definition,
                query=SearchWildcardQuery(
                    index_name="by_text",
                    raw_query="*engine*",
                    normalized_pattern="*engine*",
                    paths=("body",),
                ),
            )
        )
        self.assertFalse(
            matches_search_wildcard_query(
                document,
                definition=definition,
                query=SearchWildcardQuery(
                    index_name="by_text",
                    raw_query="grace*",
                    normalized_pattern="grace*",
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
        explicit_document = validate_search_index_definition(
            {
                "mappings": {
                    "fields": {
                        "body": {"type": "string"},
                        "score": {"type": "number"},
                        "metadata": {
                            "type": "document",
                            "fields": {
                                "topic": {"type": "string"},
                                "publishedAt": {"type": "date"},
                            },
                        }
                    }
                }
            },
            index_type="search",
        )
        self.assertEqual(
            explicit_document["mappings"]["fields"]["metadata"]["type"],
            "document",
        )
        self.assertEqual(
            explicit_document["mappings"]["fields"]["metadata"]["fields"][
                "publishedAt"
            ]["type"],
            "date",
        )
        embedded = validate_search_index_definition(
            {
                "mappings": {
                    "fields": {
                        "contributors": {
                            "type": "embeddedDocuments",
                            "fields": {
                                "name": {"type": "string"},
                                "role": {"type": "token"},
                                "verified": {"type": "boolean"},
                            },
                        }
                    }
                }
            },
            index_type="search",
        )
        self.assertEqual(
            embedded["mappings"]["fields"]["contributors"]["type"],
            "embeddedDocuments",
        )
        self.assertEqual(
            embedded["mappings"]["fields"]["contributors"]["fields"]["name"]["type"],
            "string",
        )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {
                    "mappings": {
                        "fields": {
                            "nested": {
                                "type": "document",
                                "fields": [],
                            }
                        }
                    }
                },
                index_type="search",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {"mappings": {"fields": {"flag": {"type": "decimal"}}}},
                index_type="search",
            )
        with self.assertRaises(OperationFailure):
            validate_search_index_definition(
                {
                    "mappings": {
                        "fields": {
                            "contributors": {
                                "type": "embeddedDocuments",
                                "fields": [],
                            }
                        }
                    }
                },
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

    def test_validate_vector_search_definition_accepts_local_ann_settings(self) -> None:
        normalized = validate_search_index_definition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                        "similarity": "cosine",
                        "connectivity": 16,
                        "expansionAdd": 32,
                        "expansionSearch": 64,
                    }
                ]
            },
            index_type="vectorSearch",
        )
        self.assertEqual(normalized["fields"][0]["connectivity"], 16)
        self.assertEqual(normalized["fields"][0]["expansionAdd"], 32)
        self.assertEqual(normalized["fields"][0]["expansionSearch"], 64)

    def test_validate_vector_search_definition_rejects_invalid_local_ann_settings(self) -> None:
        for option_name in ("connectivity", "expansionAdd", "expansionSearch"):
            with self.subTest(option=option_name):
                with self.assertRaises(OperationFailure):
                    validate_search_index_definition(
                        {
                            "fields": [
                                {
                                    "type": "vector",
                                    "path": "embedding",
                                    "numDimensions": 3,
                                    "similarity": "cosine",
                                    option_name: 0,
                                }
                            ]
                        },
                        index_type="vectorSearch",
                    )

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

    def test_search_contract_helpers_cover_additional_error_paths(self) -> None:
        with self.assertRaisesRegex(OperationFailure, "unsupported keys"):
            compile_classic_text_query({"$search": "Ada", "$language": "en"})
        with self.assertRaisesRegex(OperationFailure, "searchable token"):
            compile_classic_text_query({"$search": "!!!"})
        with self.assertRaisesRegex(
            OperationFailure,
            "classic \\$text requires a local text index on the collection",
        ):
            resolve_classic_text_index([])
        with self.assertRaisesRegex(OperationFailure, "text index not found with name \\[missing\\]"):
            resolve_classic_text_index(
                [EngineIndexRecord(name="content_text", fields=["content"], key=[("content", "text")], unique=False)],
                hinted_name="missing",
            )
        with self.assertRaisesRegex(OperationFailure, "must be the first stage"):
            validate_search_stage_pipeline(
                [{"$match": {"x": 1}}, {"$vectorSearch": {"index": "vec", "path": "embedding", "queryVector": [1], "limit": 1}}]
            )
        with self.assertRaisesRegex(OperationFailure, "\\$search.near.path must be a non-empty string"):
            search_module._compile_search_clause(index_name="by_text", clause_name="near", clause_spec={})

        self.assertEqual(search_module.iter_classic_text_values({"tags": ["Ada", 1, "Grace"]}, "tags"), ("Ada", "Grace"))
        self.assertEqual(search_module.iter_classic_text_values({"tags": 1}, "tags"), ())

    def test_search_clause_compilers_cover_invalid_shapes_and_matching_fallbacks(self) -> None:
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            compile_search_text_query({"text": "bad"})
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            compile_search_phrase_query({"phrase": "bad"})
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            compile_search_autocomplete_query({"autocomplete": "bad"})
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            compile_search_wildcard_query({"wildcard": "bad"})
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            compile_search_regex_query({"regex": "bad"})
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            compile_search_exists_query({"exists": "bad"})
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            compile_search_near_query({"near": "bad"})
        with self.assertRaisesRegex(OperationFailure, "valid regular expression"):
            compile_search_regex_query({"regex": {"query": "[", "path": "name"}})
        with self.assertRaisesRegex(OperationFailure, "positive finite number"):
            compile_search_near_query({"near": {"path": "score", "origin": 1, "pivot": 0}})
        with self.assertRaisesRegex(OperationFailure, "entries must be documents"):
            compile_search_compound_query({"compound": {"must": ["bad"]}})
        with self.assertRaisesRegex(OperationFailure, "require exactly one operator"):
            compile_search_compound_query(
                {"compound": {"must": [{"text": {"query": "ada"}, "phrase": {"query": "ada"}}]}}
            )
        with self.assertRaisesRegex(OperationFailure, "minimumShouldMatch must be a non-negative integer"):
            compile_search_compound_query(
                {"compound": {"should": [{"text": {"query": "ada"}}], "minimumShouldMatch": -1}}
            )

        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="default")
        self.assertFalse(
            matches_search_autocomplete_query(
                {"name": None},
                definition=definition,
                query=SearchAutocompleteQuery(index_name="default", raw_query="Ada", terms=("ada",), paths=("name",)),
            )
        )
        self.assertFalse(
            matches_search_wildcard_query(
                {"name": None},
                definition=definition,
                query=SearchWildcardQuery(index_name="default", raw_query="Ada*", normalized_pattern="ada*", paths=("name",)),
            )
        )
        self.assertFalse(
            matches_search_regex_query(
                {"name": None},
                definition=definition,
                query=SearchRegexQuery(index_name="default", raw_query="Ada.*", paths=("name",)),
            )
        )
        self.assertTrue(
            matches_search_exists_query(
                {"name": "Ada"},
                definition=definition,
                query=SearchExistsQuery(index_name="default", paths=("name",)),
            )
        )
        self.assertFalse(
            matches_search_exists_query(
                {"other": "Ada"},
                definition=definition,
                query=SearchExistsQuery(index_name="default", paths=("name",)),
            )
        )
        near_query = SearchNearQuery(
            index_name="default",
            path="score",
            origin=10,
            pivot=2.0,
            origin_kind="number",
        )
        self.assertTrue(
            matches_search_near_query(
                {"score": 11},
                definition=definition,
                query=near_query,
            )
        )
        self.assertFalse(
            matches_search_near_query(
                {"score": 14},
                definition=definition,
                query=near_query,
            )
        )
        self.assertEqual(
            search_near_distance(
                {"history": [15, 8, 3]},
                query=SearchNearQuery(
                    index_name="default",
                    path="history",
                    origin=10,
                    pivot=3.0,
                    origin_kind="number",
                ),
            ),
            2.0,
        )
        with self.assertRaisesRegex(OperationFailure, "unsupported local search query type"):
            matches_search_query(
                {"name": "Ada"},
                definition=definition,
                query=SearchVectorQuery(index_name="vec", path="embedding", query_vector=(1.0,), limit=1, num_candidates=1),
            )

    def test_vector_scoring_and_explain_helpers_cover_non_cosine_variants(self) -> None:
        dot_definition = SearchIndexDefinition(
            {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "dotProduct"}]},
            name="vec",
            index_type="vectorSearch",
        )
        self.assertEqual(
            score_vector_document(
                {"embedding": [1.0, 2.0]},
                definition=dot_definition,
                query=SearchVectorQuery(index_name="vec", path="embedding", query_vector=(2.0, 3.0), limit=1, num_candidates=1),
            ),
            8.0,
        )

        euclidean_definition = SearchIndexDefinition(
            {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "euclidean"}]},
            name="vec",
            index_type="vectorSearch",
        )
        self.assertEqual(
            score_vector_document(
                {"embedding": [2.0, 2.0]},
                definition=euclidean_definition,
                query=SearchVectorQuery(index_name="vec", path="embedding", query_vector=(1.0, 2.0), limit=1, num_candidates=1),
            ),
            -1.0,
        )
        compound = SearchCompoundQuery(
            index_name="by_text",
            must=(SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("Ada",), paths=("title",)),),
            should=(),
            filter=(),
            must_not=(),
            minimum_should_match=0,
        )
        details = search_query_explain_details(compound)
        self.assertEqual(details["queryOperator"], "compound")
        self.assertEqual(details["compound"]["must"], 1)
        self.assertEqual(details["compound"]["mustOperators"], ["text"])
        self.assertEqual(details["compound"]["shouldOperators"], [])
        self.assertEqual(
            details["ranking"],
            {
                "usesShouldRanking": False,
                "nearAware": False,
                "nearAwareShouldCount": 0,
                "shouldRankingMode": "no-should-ranking",
                "minimumShouldMatchApplied": False,
            },
        )
        exists_details = search_query_explain_details(
            SearchExistsQuery(index_name="by_text", paths=("title",))
        )
        self.assertEqual(exists_details["queryOperator"], "exists")
        self.assertEqual(exists_details["paths"], ["title"])
        self.assertEqual(
            exists_details["pathSummary"],
            {
                "all": ["title"],
                "pathCount": 1,
                "multiPath": False,
                "usesEmbeddedPaths": False,
                "embeddedPaths": [],
                "parentPaths": ["title"],
                "leafPaths": [],
            },
        )
        self.assertIsNone(
            search_query_explain_details(
                SearchExistsQuery(index_name="by_text", paths=None)
            )["pathSummary"]
        )
        regex_details = search_query_explain_details(
            SearchRegexQuery(
                index_name="by_text",
                raw_query="Ada.*algorithm",
                paths=("body",),
                flags="i",
                stage_options=search_module.SearchStageOptions(
                    count=search_module.SearchCountSpec(mode="total"),
                    highlight=search_module.SearchHighlightSpec(paths=("body",), max_chars=40),
                    facet=search_module.SearchFacetSpec(path="kind", num_buckets=3),
                ),
            )
        )
        self.assertEqual(regex_details["queryOperator"], "regex")
        self.assertEqual(regex_details["query"], "Ada.*algorithm")
        self.assertEqual(regex_details["paths"], ["body"])
        self.assertEqual(
            regex_details["querySemantics"],
            {
                "matchingMode": "python-regex-local",
                "flags": "i",
                "supportsFlags": True,
                "atlasParity": "subset",
                "scope": "local-text-tier",
            },
        )
        self.assertEqual(
            regex_details["stageOptions"],
            {
                "count": {"type": "total"},
                "highlight": {
                    "paths": ["body"],
                    "maxChars": 40,
                    "resultField": "searchHighlights",
                },
                "facet": {
                    "path": "kind",
                    "numBuckets": 3,
                    "previewOnly": True,
                },
            },
        )
        self.assertEqual(
            regex_details["pathSummary"],
            {
                "all": ["body"],
                "pathCount": 1,
                "multiPath": False,
                "usesEmbeddedPaths": False,
                "embeddedPaths": [],
                "parentPaths": ["body"],
                "leafPaths": [],
            },
        )
        near_details = search_query_explain_details(
            SearchNearQuery(
                index_name="by_text",
                path="score",
                origin=10,
                pivot=5.0,
                origin_kind="number",
            )
        )
        self.assertEqual(near_details["queryOperator"], "near")
        self.assertEqual(near_details["pivot"], 5.0)
        self.assertEqual(
            near_details["querySemantics"],
            {
                "matchingMode": "distance-ranking",
                "scope": "local-filter-tier",
            },
        )
        self.assertEqual(near_details["ranking"]["distanceMode"], "pivot-decay")
        self.assertEqual(
            near_details["ranking"]["scoreFormula"],
            "1 + 1 / (1 + distance / pivot)",
        )
        self.assertEqual(
            near_details["pathSummary"],
            {
                "all": ["score"],
                "pathCount": 1,
                "multiPath": False,
                "usesEmbeddedPaths": False,
                "embeddedPaths": [],
                "parentPaths": ["score"],
                "leafPaths": [],
                "sections": ["near"],
            },
        )
        self.assertEqual(
            near_details["supportedOriginKinds"],
            ["number", "date", "datetime"],
        )
        self.assertEqual(
            near_details["pivotDecay"],
            {
                "baselineScore": 1.0,
                "maxScore": 2.0,
                "pivot": 5.0,
            },
        )

    def test_search_query_explain_details_report_parent_and_embedded_paths_for_text_like_and_exists(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "body": {"type": "string"},
                        "score": {"type": "number"},
                        "metadata": {
                            "type": "document",
                            "fields": {
                                "topic": {"type": "string"},
                                "series": {"type": "token"},
                                "publishedAt": {"type": "date"},
                            },
                        },
                        "contributors": {
                            "type": "embeddedDocuments",
                            "fields": {
                                "name": {"type": "string"},
                                "role": {"type": "token"},
                                "verified": {"type": "boolean"},
                                "impact": {"type": "number"},
                            },
                        },
                    },
                }
            },
            name="by_text",
        )
        text_details = search_query_explain_details(
            SearchTextQuery(
                index_name="by_text",
                raw_query="Ada",
                terms=("ada",),
                paths=("contributors", "metadata.topic"),
            ),
            definition=definition,
        )
        self.assertEqual(
            text_details["pathSummary"],
            {
                "all": ["contributors", "metadata.topic"],
                "pathCount": 2,
                "multiPath": True,
                "usesEmbeddedPaths": True,
                "embeddedPaths": ["metadata.topic"],
                "parentPaths": ["contributors"],
                "leafPaths": ["metadata.topic"],
                "resolvedLeafPaths": [
                    "contributors.name",
                    "contributors.role",
                    "metadata.topic",
                ],
                "unresolvedPaths": [],
            },
        )
        self.assertEqual(
            text_details["querySemantics"],
            {
                "matchingMode": "tokenized-any-term",
                "scoringMode": "term-frequency-local",
                "scope": "local-text-tier",
            },
        )
        exists_details = search_query_explain_details(
            SearchExistsQuery(index_name="by_text", paths=("metadata", "contributors.verified")),
            definition=definition,
        )
        self.assertEqual(
            exists_details["pathSummary"],
            {
                "all": ["metadata", "contributors.verified"],
                "pathCount": 2,
                "multiPath": True,
                "usesEmbeddedPaths": True,
                "embeddedPaths": ["contributors.verified"],
                "parentPaths": ["metadata"],
                "leafPaths": ["contributors.verified"],
                "resolvedLeafPaths": [
                    "contributors.verified",
                    "metadata.publishedAt",
                    "metadata.series",
                    "metadata.topic",
                ],
                "unresolvedPaths": [],
            },
        )
        self.assertEqual(
            exists_details["querySemantics"],
            {
                "matchingMode": "field-presence",
                "scope": "local-text-tier",
            },
        )
        scalar_details = search_query_explain_details(
            SearchNearQuery(
                index_name="by_text",
                path="contributors.impact",
                origin=8,
                pivot=3.0,
                origin_kind="number",
            ),
            definition=definition,
        )
        self.assertEqual(
            scalar_details["pathSummary"],
            {
                "all": ["contributors.impact"],
                "pathCount": 1,
                "multiPath": False,
                "usesEmbeddedPaths": True,
                "embeddedPaths": ["contributors.impact"],
                "parentPaths": [],
                "leafPaths": ["contributors.impact"],
                "sections": ["near"],
                "resolvedLeafPaths": ["contributors.impact"],
                "unresolvedPaths": [],
            },
        )
        self.assertEqual(
            scalar_details["querySemantics"],
            {
                "matchingMode": "distance-ranking",
                "scope": "local-filter-tier",
            },
        )
        richer_compound = search_query_explain_details(
            SearchCompoundQuery(
                index_name="by_text",
                should=(
                    SearchNearQuery(
                        index_name="by_text",
                        path="score",
                        origin=10,
                        pivot=5.0,
                        origin_kind="number",
                    ),
                    SearchPhraseQuery(
                        index_name="by_text",
                        raw_query="Ada algorithm",
                        paths=("body",),
                        slop=1,
                    ),
                ),
                minimum_should_match=1,
            ),
            definition=definition,
        )
        self.assertEqual(richer_compound["ranking"]["nearAwareShouldCount"], 1)
        self.assertEqual(
            richer_compound["ranking"]["shouldRankingMode"],
            "matched-should-plus-clause-score",
        )
        self.assertTrue(richer_compound["ranking"]["minimumShouldMatchApplied"])
        self.assertEqual(
            richer_compound["pathSummary"],
            {
                "must": [],
                "should": ["body", "score"],
                "filter": [],
                "mustNot": [],
                "all": ["body", "score"],
                "parentPaths": ["body", "score"],
                "leafPaths": [],
                "textualPaths": ["body"],
                "textualParentPaths": ["body"],
                "textualLeafPaths": [],
                "scalarPaths": ["score"],
                "scalarParentPaths": ["score"],
                "scalarLeafPaths": [],
                "embeddedPaths": [],
                "embeddedTextualPaths": [],
                "embeddedScalarPaths": [],
                "embeddedPathSections": [],
                "usesEmbeddedPaths": False,
                "nestedCompoundCount": 0,
                "maxClauseDepth": 1,
                "resolvedLeafPaths": ["body", "score"],
                "resolvedTextualLeafPaths": ["body"],
                "resolvedScalarLeafPaths": ["score"],
                "unresolvedPaths": [],
            },
        )

    def test_search_query_explain_details_reports_nested_compound_structure(self) -> None:
        nested = SearchCompoundQuery(
            index_name="by_text",
            must=(
                SearchTextQuery(
                    index_name="by_text",
                    raw_query="Ada",
                    terms=("ada",),
                    paths=("contributors.name",),
                ),
            ),
            should=(
                SearchCompoundQuery(
                    index_name="by_text",
                    should=(
                        SearchNearQuery(
                            index_name="by_text",
                            path="metrics.score",
                            origin=10,
                            pivot=2.0,
                            origin_kind="number",
                        ),
                    ),
                    minimum_should_match=1,
                ),
            ),
        )

        details = search_query_explain_details(nested)

        self.assertEqual(
            details["pathSummary"],
            {
                "must": ["contributors.name"],
                "should": ["metrics.score"],
                "filter": [],
                "mustNot": [],
                "all": ["contributors.name", "metrics.score"],
                "parentPaths": [],
                "leafPaths": ["contributors.name", "metrics.score"],
                "textualPaths": ["contributors.name"],
                "textualParentPaths": [],
                "textualLeafPaths": ["contributors.name"],
                "scalarPaths": ["metrics.score"],
                "scalarParentPaths": [],
                "scalarLeafPaths": ["metrics.score"],
                "embeddedPaths": ["contributors.name", "metrics.score"],
                "embeddedTextualPaths": ["contributors.name"],
                "embeddedScalarPaths": ["metrics.score"],
                "embeddedPathSections": ["must", "should"],
                "usesEmbeddedPaths": True,
                "nestedCompoundCount": 1,
                "maxClauseDepth": 2,
            },
        )
        self.assertEqual(search_module._query_paths(object()), ())

    def test_search_highlights_and_stage_option_previews_cover_local_advanced_subset(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "autocomplete"},
                        "body": {"type": "string"},
                        "kind": {"type": "token"},
                    },
                }
            },
            name="by_text",
        )
        query = SearchTextQuery(
            index_name="by_text",
            raw_query="Ada",
            terms=("ada",),
            paths=("title", "body"),
            stage_options=search_module.SearchStageOptions(
                count=search_module.SearchCountSpec(mode="total"),
                highlight=search_module.SearchHighlightSpec(paths=("title", "body"), max_chars=20),
                facet=search_module.SearchFacetSpec(path="kind", num_buckets=3),
            ),
        )
        highlighted = search_module.attach_search_highlights(
            {
                "_id": 1,
                "title": "Ada Lovelace notes",
                "body": "Ada designed local search patterns.",
                "kind": "note",
            },
            definition=definition,
            query=query,
        )
        self.assertIn("searchHighlights", highlighted)
        previews = search_module.build_search_stage_option_previews(
            [highlighted],
            definition=definition,
            query=query,
        )
        self.assertEqual(
            previews["countPreview"],
            {"type": "total", "value": 1, "exact": True},
        )
        self.assertEqual(
            previews["facetPreview"],
            {"path": "kind", "numBuckets": 3, "buckets": [{"value": "note", "count": 1}]},
        )
        self.assertEqual(previews["highlightPreview"]["resultField"], "searchHighlights")
        self.assertTrue(previews["highlightPreview"]["sample"])

    def test_search_advanced_option_validators_cover_invalid_shapes(self) -> None:
        invalid_specs = (
            (
                search_module._compile_search_count_spec,
                [],
                "count requires a document specification",
            ),
            (
                search_module._compile_search_count_spec,
                {"type": "total", "extra": True},
                "count only supports type",
            ),
            (
                search_module._compile_search_count_spec,
                {"type": "approx"},
                "count.type must be 'total' or 'lowerBound'",
            ),
            (
                search_module._compile_search_highlight_spec,
                [],
                "highlight requires a document specification",
            ),
            (
                search_module._compile_search_highlight_spec,
                {"path": "title", "extra": True},
                "highlight only supports path and maxChars",
            ),
            (
                search_module._compile_search_highlight_spec,
                {"path": {"wildcard": "*"}},
                "highlight.path must be a string",
            ),
            (
                search_module._compile_search_highlight_spec,
                {"path": "title", "maxChars": 0},
                "highlight.maxChars must be a positive integer",
            ),
            (
                search_module._compile_search_facet_spec,
                [],
                "facet requires a document specification",
            ),
            (
                search_module._compile_search_facet_spec,
                {"path": "kind", "extra": True},
                "facet only supports path and numBuckets",
            ),
            (
                search_module._compile_search_facet_spec,
                {"path": ""},
                "facet.path must be a non-empty string",
            ),
            (
                search_module._compile_search_facet_spec,
                {"path": "kind", "numBuckets": 0},
                "facet.numBuckets must be a positive integer",
            ),
        )
        for compiler, spec, pattern in invalid_specs:
            with self.subTest(spec=spec), self.assertRaisesRegex(OperationFailure, pattern):
                compiler(spec)

        invalid_queries = (
            (
                compile_search_autocomplete_query,
                {
                    "index": "by_text",
                    "autocomplete": {"query": "Ada", "path": "title", "tokenOrder": "random"},
                },
                "tokenOrder",
            ),
            (
                compile_search_autocomplete_query,
                {
                    "index": "by_text",
                    "autocomplete": {"query": "Ada", "path": "title", "boost": 2},
                },
                "only supports query and path",
            ),
            (
                compile_search_wildcard_query,
                {
                    "index": "by_text",
                    "wildcard": {"query": "*ada*", "path": "title", "allowAnalyzedField": "yes"},
                },
                "allowAnalyzedField must be a boolean",
            ),
            (
                compile_search_wildcard_query,
                {
                    "index": "by_text",
                    "wildcard": {"query": "*ada*", "path": "title", "boost": 2},
                },
                "only supports query and path",
            ),
            (
                compile_search_regex_query,
                {
                    "index": "by_text",
                    "regex": {"query": "Ada.*", "path": "title", "flags": 1},
                },
                "flags must be a string",
            ),
            (
                compile_search_regex_query,
                {
                    "index": "by_text",
                    "regex": {"query": "Ada.*", "path": "title", "boost": 2},
                },
                "only supports query and path",
            ),
        )
        for compiler, spec, pattern in invalid_queries:
            with self.subTest(spec=spec), self.assertRaisesRegex(OperationFailure, pattern):
                compiler(spec)

    def test_search_advanced_text_helpers_cover_private_branches(self) -> None:
        self.assertFalse(search_module._token_sequence_matches(("ada",), ()))
        self.assertEqual(
            search_module._regex_compile_flags("ms"),
            search_module.re.MULTILINE | search_module.re.DOTALL,
        )

        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "autocomplete"},
                        "body": {"type": "string"},
                        "kind": {"type": "token"},
                        "count": {"type": "number"},
                    },
                }
            },
            name="by_text",
        )
        document = {
            "title": "Ada algorithms handbook",
            "body": "Ada algorithm notes",
            "kind": "note",
            "count": 2,
        }
        sequential = SearchAutocompleteQuery(
            index_name="by_text",
            raw_query="Ada alg",
            terms=("ada", "alg"),
            paths=("body",),
            token_order="sequential",
        )
        matched, score, _near = search_module.search_clause_ranking(
            document,
            definition=definition,
            query=sequential,
        )
        self.assertTrue(matched)
        self.assertEqual(score, 2.0)

        non_compound = search_module._highlightable_textual_clauses(
            SearchNearQuery(index_name="by_text", path="count", origin=2, pivot=1.0, origin_kind="number")
        )
        self.assertEqual(non_compound, ())

        recursive = SearchCompoundQuery(
            index_name="by_text",
            must=(SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("title",)),),
            should=(
                SearchCompoundQuery(
                    index_name="by_text",
                    should=(sequential,),
                    minimum_should_match=1,
                ),
            ),
        )
        recursive_clauses = search_module._highlightable_textual_clauses(recursive)
        self.assertEqual(len(recursive_clauses), 2)
        self.assertIn(sequential, recursive_clauses)
        self.assertIsNone(
            search_module._resolved_highlight_clause_paths(
                SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=None),
                available_leaf_paths=("title", "body"),
            )
        )

    def test_search_highlight_helpers_cover_dedup_empty_and_payload_variants(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "body": {"type": "string"},
                        "kind": {"type": "token"},
                        "score": {"type": "number"},
                    },
                }
            },
            name="by_text",
        )
        no_highlight_query = SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("title",))
        original = {"title": "Compiler reference"}
        self.assertIs(
            search_module.attach_search_highlights(original, definition=definition, query=no_highlight_query),
            original,
        )
        unmatched_query = SearchTextQuery(
            index_name="by_text",
            raw_query="Ada",
            terms=("ada",),
            paths=("title",),
            stage_options=search_module.SearchStageOptions(
                highlight=search_module.SearchHighlightSpec(paths=("title",), max_chars=10),
            ),
        )
        unmatched_document = {"title": "Compiler reference"}
        self.assertIs(
            search_module.attach_search_highlights(
                unmatched_document,
                definition=definition,
                query=unmatched_query,
            ),
            unmatched_document,
        )

        duplicate_query = SearchCompoundQuery(
            index_name="by_text",
            must=(
                SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("body",)),
                SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("title",)),
                SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("title",)),
            ),
        )
        highlights = search_module.build_search_highlights(
            {"title": "Ada title", "score": 7},
            definition=definition,
            query=duplicate_query,
            spec=search_module.SearchHighlightSpec(paths=("body", "title"), max_chars=20),
        )
        self.assertEqual(
            highlights,
            [{"path": "title", "operator": "text", "text": "Ada title", "matchedTerms": ["ada"]}],
        )

        preview_query = SearchTextQuery(
            index_name="by_text",
            raw_query="Ada",
            terms=("ada",),
            paths=("title",),
            stage_options=search_module.SearchStageOptions(
                count=search_module.SearchCountSpec(mode="lowerBound"),
                highlight=search_module.SearchHighlightSpec(paths=("title",), max_chars=10),
                facet=search_module.SearchFacetSpec(path="kind", num_buckets=2),
            ),
        )
        preview_documents = [
            {"kind": "note", "searchHighlights": [{"path": "title", "operator": "text", "text": f"Ada {index}"}]}
            for index in range(4)
        ]
        preview_documents[0]["kind"] = {"bad": True}
        preview_documents[1]["kind"] = ["note", "note"]
        previews = search_module.build_search_stage_option_previews(
            preview_documents,
            definition=definition,
            query=preview_query,
        )
        self.assertEqual(previews["countPreview"]["type"], "lowerBound")
        self.assertEqual(len(previews["highlightPreview"]["sample"]), 3)
        self.assertEqual(previews["facetPreview"]["buckets"], [{"value": "note", "count": 3}])

        phrase = SearchPhraseQuery(index_name="by_text", raw_query="Ada title", paths=("title",), slop=1)
        wildcard = SearchWildcardQuery(
            index_name="by_text",
            raw_query="Ada*",
            normalized_pattern="ada*",
            paths=("title",),
            allow_analyzed_field=True,
        )
        regex = SearchRegexQuery(
            index_name="by_text",
            raw_query="Ada.+",
            paths=("title",),
            flags="i",
        )
        self.assertTrue(search_module._text_value_matches_clause("Ada title", phrase))
        self.assertTrue(search_module._text_value_matches_clause("Ada title", sequential := SearchAutocompleteQuery(
            index_name="by_text",
            raw_query="Ada ti",
            terms=("ada", "ti"),
            paths=("title",),
            token_order="sequential",
        )))
        self.assertTrue(
            search_module._text_value_matches_clause(
                "Ada title",
                SearchAutocompleteQuery(
                    index_name="by_text",
                    raw_query="Ada ti",
                    terms=("ada", "ti"),
                    paths=("title",),
                ),
            )
        )
        self.assertTrue(search_module._text_value_matches_clause("Ada title", wildcard))
        self.assertTrue(search_module._text_value_matches_clause("Ada title", regex))
        self.assertEqual(
            search_module._highlight_payload_for_clause("title", "Ada title", clause=phrase, max_chars=50),
            {
                "path": "title",
                "operator": "phrase",
                "text": "Ada title",
                "matchedPhrase": "Ada title",
            },
        )
        self.assertEqual(
            search_module._highlight_payload_for_clause("title", "Ada title", clause=wildcard, max_chars=50),
            {
                "path": "title",
                "operator": "wildcard",
                "text": "Ada title",
                "pattern": "Ada*",
            },
        )
        self.assertEqual(
            search_module._highlight_payload_for_clause("title", "Ada title", clause=regex, max_chars=50),
            {
                "path": "title",
                "operator": "regex",
                "text": "Ada title",
                "pattern": "Ada.+",
                "flags": "i",
            },
        )

    def test_search_explain_resolution_helpers_cover_edge_cases(self) -> None:
        path_summary: dict[str, object | None] = {}
        search_module._attach_resolved_leaf_paths(
            path_summary,
            paths=None,
            available_leaf_paths=(),
        )
        self.assertEqual(path_summary, {})

        vector_definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                    }
                ]
            },
            name="by_vector",
            index_type="vectorSearch",
        )
        vector_text_details = search_query_explain_details(
            SearchTextQuery(
                index_name="by_vector",
                raw_query="Ada",
                terms=("ada",),
                paths=("title",),
            ),
            definition=vector_definition,
        )
        self.assertEqual(vector_text_details["pathSummary"]["resolvedLeafPaths"], [])
        self.assertEqual(
            vector_text_details["pathSummary"]["unresolvedPaths"],
            ["title"],
        )
        vector_query_details = search_query_explain_details(
            SearchVectorQuery(
                index_name="by_vector",
                path="embedding",
                query_vector=(1.0, 0.0, 0.0),
                limit=2,
                num_candidates=4,
                min_score=0.9,
            ),
            definition=vector_definition,
        )
        self.assertEqual(vector_query_details["queryOperator"], "vectorSearch")
        self.assertEqual(
            vector_query_details["pathSummary"]["resolvedLeafPaths"],
            ["embedding"],
        )
        self.assertEqual(vector_query_details["pathSummary"]["unresolvedPaths"], [])
        self.assertEqual(search_module._mapped_leaf_search_paths(vector_definition), ())

        no_mapping_definition = SearchIndexDefinition(
            {"mappings": []},
            name="by_text",
        )
        self.assertEqual(search_module._mapped_leaf_search_paths(no_mapping_definition), ())
        self.assertEqual(search_module._mapped_textual_search_paths(no_mapping_definition), ())
        self.assertEqual(search_module._mapped_scalar_search_paths(no_mapping_definition), ())
        self.assertEqual(search_module._mapped_scalar_search_paths(vector_definition), ())
        self.assertEqual(search_module._iter_mapped_leaf_search_paths({"fields": []}), ())
        self.assertEqual(
            search_module._iter_mapped_leaf_search_paths({"fields": {"": {}, "ok": "bad"}}),
            (),
        )
        self.assertEqual(
            search_module._resolve_requested_leaf_paths(
                ["metadata", "metadata.topic"],
                available_leaf_paths=("metadata.topic",),
            ),
            ["metadata.topic"],
        )
        self.assertEqual(
            search_module._vector_score_formula("dotProduct"),
            "sum(query[i] * candidate[i])",
        )
        self.assertEqual(
            search_module._vector_score_formula("euclidean"),
            "-sqrt(sum((query[i] - candidate[i])^2))",
        )
        missing_path_summary_details = {"pathSummary": None}
        search_module._enrich_search_explain_details(
            missing_path_summary_details,
            query=SearchVectorQuery(
                index_name="by_vector",
                path="embedding",
                query_vector=(1.0, 0.0, 0.0),
                limit=2,
                num_candidates=4,
            ),
            definition=vector_definition,
        )
        self.assertEqual(missing_path_summary_details, {"pathSummary": None})

    def test_materialized_search_document_reuses_entries_for_multiple_matchers(self) -> None:
        definition = SearchIndexDefinition(
            {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}, "body": {"type": "string"}}}},
            name="by_text",
        )
        document = {"title": "Ada Algorithms", "body": "Ada wrote vector algorithm notes"}
        prepared = materialize_search_document(document, definition)
        self.assertTrue(
            matches_search_text_query(
                document,
                definition=definition,
                query=SearchTextQuery(index_name="by_text", raw_query="Ada", terms=("ada",), paths=("title", "body")),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_wildcard_query(
                document,
                definition=definition,
                query=SearchWildcardQuery(
                    index_name="by_text",
                    raw_query="*vector*",
                    normalized_pattern="*vector*",
                    paths=("body",),
                ),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_exists_query(
                document,
                definition=definition,
                query=SearchExistsQuery(index_name="by_text", paths=("title",)),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_regex_query(
                document,
                definition=definition,
                query=SearchRegexQuery(
                    index_name="by_text",
                    raw_query="Ada.*vector",
                    paths=("body",),
                ),
                materialized=prepared,
            )
        )

    def test_search_scalar_operators_follow_embedded_document_paths(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "contributors": {
                            "type": "embeddedDocuments",
                            "fields": {
                                "verified": {"type": "boolean"},
                                "impact": {"type": "number"},
                            },
                        }
                    },
                }
            },
            name="by_text",
        )
        document = {
            "contributors": [
                {"verified": True, "impact": 10},
                {"verified": False, "impact": 2},
            ]
        }

        self.assertTrue(
            matches_search_equals_query(
                document,
                definition=definition,
                query=SearchEqualsQuery(
                    index_name="by_text",
                    path="contributors.verified",
                    value_kind="bool",
                    value=True,
                ),
            )
        )
        self.assertTrue(
            matches_search_range_query(
                document,
                definition=definition,
                query=SearchRangeQuery(
                    index_name="by_text",
                    path="contributors.impact",
                    bound_kind="number",
                    gt=None,
                    gte=9.0,
                    lt=None,
                    lte=None,
                ),
            )
        )
        self.assertEqual(
            search_near_distance(
                document,
                query=SearchNearQuery(
                    index_name="by_text",
                    path="contributors.impact",
                    origin=8.0,
                    origin_kind="number",
                    pivot=3.0,
                ),
            ),
            2.0,
        )

    def test_materialized_search_document_ignores_non_textual_mappings_for_text_entries(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "published": {"type": "boolean"},
                        "score": {"type": "number"},
                        "ownerId": {"type": "objectId"},
                        "externalUuid": {"type": "uuid"},
                    },
                }
            },
            name="by_text",
        )
        document = {
            "title": "Ada Algorithms",
            "published": True,
            "score": "not-a-number",
            "ownerId": "656565656565656565656561",
            "externalUuid": "11111111-1111-1111-1111-111111111111",
        }
        prepared = materialize_search_document(document, definition)
        self.assertEqual(prepared.entries, (("title", "Ada Algorithms"),))
        self.assertEqual(prepared.searchable_paths, frozenset({"title"}))

    def test_materialized_search_document_supports_embedded_documents_paths(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "contributors": {
                            "type": "embeddedDocuments",
                            "fields": {
                                "name": {"type": "string"},
                                "role": {"type": "token"},
                                "verified": {"type": "boolean"},
                            },
                        },
                    },
                }
            },
            name="by_text",
        )
        document = {
            "title": "Local search handbook",
            "contributors": [
                {"name": "Ada Lovelace", "role": "author", "verified": True},
                {"name": "Charles Babbage", "role": "editor", "verified": False},
                "ignored",
            ],
        }

        prepared = materialize_search_document(document, definition)

        self.assertEqual(
            prepared.entries,
            (
                ("title", "Local search handbook"),
                ("contributors.name", "Ada Lovelace"),
                ("contributors.role", "author"),
                ("contributors.name", "Charles Babbage"),
                ("contributors.role", "editor"),
            ),
        )
        self.assertEqual(
            prepared.searchable_paths,
            frozenset({"title", "contributors.name", "contributors.role"}),
        )
        self.assertTrue(
            matches_search_text_query(
                document,
                definition=definition,
                query=SearchTextQuery(
                    index_name="by_text",
                    raw_query="Ada",
                    terms=("ada",),
                    paths=("contributors.name",),
                ),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_text_query(
                document,
                definition=definition,
                query=SearchTextQuery(
                    index_name="by_text",
                    raw_query="Ada",
                    terms=("ada",),
                    paths=("contributors",),
                ),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_exists_query(
                document,
                definition=definition,
                query=SearchExistsQuery(index_name="by_text", paths=("contributors",)),
                materialized=prepared,
            )
        )

    def test_materialized_search_document_supports_explicit_document_paths(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "metadata": {
                            "type": "document",
                            "fields": {
                                "topic": {"type": "string"},
                                "series": {"type": "token"},
                                "publishedAt": {"type": "date"},
                            },
                        }
                    },
                }
            },
            name="by_text",
        )
        document = {
            "metadata": {
                "topic": "Local search",
                "series": "analysis",
                "publishedAt": datetime.date(2024, 1, 10),
            }
        }

        prepared = materialize_search_document(document, definition)

        self.assertEqual(
            prepared.entries,
            (
                ("metadata.topic", "Local search"),
                ("metadata.series", "analysis"),
            ),
        )
        self.assertEqual(
            prepared.searchable_paths,
            frozenset({"metadata.topic", "metadata.series"}),
        )
        self.assertTrue(
            matches_search_text_query(
                document,
                definition=definition,
                query=SearchTextQuery(
                    index_name="by_text",
                    raw_query="Local",
                    terms=("local",),
                    paths=("metadata.topic",),
                ),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_text_query(
                document,
                definition=definition,
                query=SearchTextQuery(
                    index_name="by_text",
                    raw_query="Local",
                    terms=("local",),
                    paths=("metadata",),
                ),
                materialized=prepared,
            )
        )

    def test_search_parent_paths_resolve_descendant_text_entries(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "metadata": {
                            "type": "document",
                            "fields": {
                                "topic": {"type": "string"},
                                "series": {"type": "token"},
                            },
                        },
                        "contributors": {
                            "type": "embeddedDocuments",
                            "fields": {
                                "name": {"type": "string"},
                                "role": {"type": "token"},
                            },
                        },
                    },
                }
            },
            name="by_text",
        )
        document = {
            "metadata": {"topic": "Local search", "series": "analysis"},
            "contributors": [
                {"name": "Ada Lovelace", "role": "author"},
                {"name": "Charles Babbage", "role": "editor"},
            ],
        }
        prepared = materialize_search_document(document, definition)

        self.assertTrue(
            matches_search_phrase_query(
                document,
                definition=definition,
                query=SearchPhraseQuery(
                    index_name="by_text",
                    raw_query="Local search",
                    paths=("metadata",),
                ),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_autocomplete_query(
                document,
                definition=definition,
                query=SearchAutocompleteQuery(
                    index_name="by_text",
                    raw_query="Ada",
                    terms=("ada",),
                    paths=("contributors",),
                ),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_wildcard_query(
                document,
                definition=definition,
                query=SearchWildcardQuery(
                    index_name="by_text",
                    raw_query="*search*",
                    normalized_pattern="*search*",
                    paths=("metadata",),
                ),
                materialized=prepared,
            )
        )
        self.assertTrue(
            matches_search_regex_query(
                document,
                definition=definition,
                query=SearchRegexQuery(
                    index_name="by_text",
                    raw_query="Charles.*",
                    paths=("contributors",),
                ),
                materialized=prepared,
            )
        )

    def test_search_exists_uses_field_presence_for_structured_and_scalar_paths(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "metadata": {
                            "type": "document",
                            "fields": {
                                "topic": {"type": "string"},
                            },
                        },
                        "contributors": {
                            "type": "embeddedDocuments",
                            "fields": {
                                "verified": {"type": "boolean"},
                            },
                        },
                    },
                }
            },
            name="by_text",
        )
        document = {
            "metadata": {"topic": "Local search"},
            "contributors": [],
        }

        self.assertTrue(
            matches_search_exists_query(
                document,
                definition=definition,
                query=SearchExistsQuery(index_name="by_text", paths=("metadata",)),
            )
        )
        self.assertTrue(
            matches_search_exists_query(
                document,
                definition=definition,
                query=SearchExistsQuery(index_name="by_text", paths=("contributors",)),
            )
        )
        self.assertFalse(
            matches_search_exists_query(
                document,
                definition=definition,
                query=SearchExistsQuery(index_name="by_text", paths=("contributors.verified",)),
            )
        )

    def test_search_exists_without_explicit_paths_uses_mapped_paths_or_materialized_entries(self) -> None:
        scalar_definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "published": {"type": "boolean"},
                    },
                }
            },
            name="by_text",
        )
        exists_any = SearchExistsQuery(index_name="by_text", paths=None)
        self.assertTrue(
            matches_search_exists_query(
                {"published": True},
                definition=scalar_definition,
                query=exists_any,
            )
        )
        matched, score, _near = search_module.search_clause_ranking(
            {"published": True},
            definition=scalar_definition,
            query=exists_any,
        )
        self.assertTrue(matched)
        self.assertEqual(score, 1.0)

        dynamic_definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text")
        self.assertTrue(
            matches_search_exists_query(
                {"title": "Ada"},
                definition=dynamic_definition,
                query=exists_any,
            )
        )
        matched, score, _near = search_module.search_clause_ranking(
            {"title": "Ada"},
            definition=dynamic_definition,
            query=exists_any,
        )
        self.assertTrue(matched)
        self.assertEqual(score, 1.0)

    def test_search_parent_path_helpers_cover_descendant_resolution_and_presence(self) -> None:
        definition = SearchIndexDefinition(
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "metadata": {
                            "type": "document",
                            "fields": {
                                "topic": {"type": "string"},
                                "series": {"type": "token"},
                            },
                        },
                        "contributors": {
                            "type": "embeddedDocuments",
                            "fields": {
                                "name": {"type": "string"},
                            },
                        },
                    },
                }
            },
            name="by_text",
        )
        prepared = materialize_search_document(
            {
                "metadata": {"topic": "Local search", "series": "analysis"},
                "contributors": [{"name": "Ada Lovelace"}],
            },
            definition,
        )

        self.assertEqual(
            search_module._resolved_materialized_paths(
                prepared,
                ("metadata", "metadata.topic", "contributors"),
            ),
            ("metadata.topic", "metadata.series", "contributors.name"),
        )
        self.assertEqual(
            search_module._mapped_search_paths(definition),
            (
                "metadata",
                "metadata.topic",
                "metadata.series",
                "contributors",
                "contributors.name",
            ),
        )
        self.assertEqual(search_module._mapped_search_paths(SearchIndexDefinition({}, name="vec", index_type="vectorSearch")), ())
        self.assertEqual(search_module._mapped_search_paths(SearchIndexDefinition({"mappings": []}, name="by_text")), ())
        self.assertEqual(search_module._iter_mapped_search_paths({"fields": []}), ())
        self.assertEqual(
            search_module._iter_mapped_search_paths(
                {
                    "fields": {
                        "": {"type": "string"},
                        "metadata": {"type": "document", "fields": {"topic": {"type": "string"}}},
                        "bad": "nope",
                    }
                }
            ),
            ("metadata", "metadata.topic"),
        )
        self.assertTrue(search_module._search_path_exists({"contributors": []}, "contributors"))
        self.assertFalse(search_module._search_path_exists({"contributors": []}, "contributors.name"))
        self.assertTrue(search_module._search_path_exists({"contributors": [{"name": "Ada"}]}, "contributors.name"))
        self.assertTrue(search_module._search_path_exists({"history": [1, 2]}, "history.1"))
        self.assertFalse(search_module._search_path_exists({"history": [1]}, "history.3"))
        self.assertFalse(search_module._search_path_exists(5, "history"))
        self.assertFalse(search_module._search_path_exists({"history": [1]}, "scores.0"))
        self.assertTrue(search_module._search_path_exists({"value": None}, ""))

    def test_search_compound_ranking_prefers_more_should_hits_and_near_closeness(self) -> None:
        definition = SearchIndexDefinition(
            {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}, "body": {"type": "string"}, "score": {"type": "number"}}}},
            name="by_text",
        )
        query = SearchCompoundQuery(
            index_name="by_text",
            must=(SearchTextQuery(index_name="by_text", raw_query="ada", terms=("ada",), paths=("body",)),),
            should=(
                SearchExistsQuery(index_name="by_text", paths=("title",)),
                SearchNearQuery(index_name="by_text", path="score", origin=10, pivot=2, origin_kind="number"),
            ),
            filter=(),
            must_not=(),
            minimum_should_match=0,
        )
        better = {"title": "Ada", "body": "ada algorithm", "score": 9}
        worse = {"title": "Ada", "body": "ada algorithm", "score": 12}
        better_rank = search_compound_ranking(
            better,
            definition=definition,
            query=query,
            materialized=materialize_search_document(better, definition),
        )
        worse_rank = search_compound_ranking(
            worse,
            definition=definition,
            query=query,
            materialized=materialize_search_document(worse, definition),
        )
        self.assertEqual(better_rank[0], 2)
        self.assertEqual(worse_rank[0], 2)
        self.assertGreater(better_rank[1], worse_rank[1])


if __name__ == "__main__":
    unittest.main()
