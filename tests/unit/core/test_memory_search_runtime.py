import asyncio
import unittest

from mongoeco.engines._memory_search_runtime import (
    execute_search_documents,
    explain_search_documents,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.types import SearchIndexDefinition


class MemorySearchRuntimeTests(unittest.TestCase):
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
                    },
                    result_limit_hint=1,
                    downstream_filter_spec={"kind": {"$regex": "^n"}},
                )
                self.assertEqual(vector_explain.details["filterMode"], "candidate-prefilter")
                self.assertEqual(vector_explain.details["topKLimitHint"], 1)
                self.assertEqual(vector_explain.details["documentsScannedAfterPrefilter"], 2)
            finally:
                await engine.disconnect()

        asyncio.run(_run())
