import unittest

from mongoeco.core.search import (
    SearchTextQuery,
    build_search_index_document,
    compile_search_stage,
    compile_search_text_query,
    iter_searchable_text_entries,
    sqlite_fts5_query,
    validate_search_index_definition,
    validate_search_stage_pipeline,
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

    def test_build_search_index_document_marks_vector_search_as_not_queryable(self) -> None:
        document = build_search_index_document(
            SearchIndexDefinition({"analyzer": "keyword"}, name="vec", index_type="vectorSearch")
        )
        self.assertFalse(document["queryable"])
        self.assertEqual(document["status"], "UNSUPPORTED")

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

    def test_validate_search_stage_pipeline_requires_search_to_be_first(self) -> None:
        with self.assertRaises(OperationFailure):
            validate_search_stage_pipeline(
                [
                    {"$match": {"x": 1}},
                    {"$search": {"text": {"query": "ada"}}},
                ]
            )

    def test_compile_search_stage_rejects_vector_search(self) -> None:
        with self.assertRaises(OperationFailure):
            compile_search_stage("$vectorSearch", {"index": "vec"})


if __name__ == "__main__":
    unittest.main()
