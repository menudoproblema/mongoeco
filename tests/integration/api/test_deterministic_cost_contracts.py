from __future__ import annotations

import unittest

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


MATCHING_DOCUMENT_COUNT = 3
DOCUMENTS = [
    {
        "_id": index,
        "kind": "keep" if index < MATCHING_DOCUMENT_COUNT else "drop",
        "score": index,
        "title": f"Ada {index}",
    }
    for index in range(6)
]
SEARCH_INDEX = SearchIndexModel(
    {
        "mappings": {
            "dynamic": False,
            "fields": {
                "title": {"type": "string"},
                "kind": {"type": "token"},
            },
        },
    },
    name="by_text",
)
SEARCH_PIPELINE = [
    {
        "$search": {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
        },
    },
    {"$match": {"kind": "keep"}},
    {"$limit": 2},
]


def _metric(explanation: dict[str, object], name: str) -> dict[str, object]:
    metrics = explanation["engine_plan"]["details"]["executionStats"]["metrics"]
    return next(item for item in metrics if item["name"] == name)


def _search_cost_fingerprint(explanation: dict[str, object]) -> dict[str, object]:
    plan = explanation["pushdown"]["searchPlan"]
    details = explanation["engine_plan"]["details"]
    return {
        "plan": plan,
        "enginePlan": details["pipelinePlan"],
        "queryMatched": _metric(explanation, "queryMatchedCount"),
        "returnedHits": _metric(explanation, "returnedHitCount"),
        "candidates": _metric(explanation, "candidateCount"),
        "collectorDocuments": _metric(explanation, "collectorDocumentCount"),
        "topK": details.get("topKPrefilter"),
    }


class DeterministicCostContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_sqlite_exact_find_owns_filter_sort_and_window(self) -> None:
        async with AsyncMongoClient(SQLiteEngine()) as client:
            collection = client.cost.finds
            await collection.insert_many(DOCUMENTS)
            await collection.create_index([("kind", 1)])
            explanation = (
                await collection.find({"kind": "keep"})
                .sort(
                    "score",
                    -1,
                )
                .limit(2)
                .explain()
            )

        pushdown = explanation["details"]["pushdown"]
        self.assertEqual(pushdown["contractMode"], "sql-exact")
        self.assertTrue(pushdown["sqlPredicateExact"])
        self.assertFalse(pushdown["residualRequired"])
        self.assertEqual(
            (
                pushdown["filterOwner"],
                pushdown["sortOwner"],
                pushdown["windowOwner"],
            ),
            ("sql", "sql", "sql"),
        )

    async def test_search_cost_contract_is_stable_across_engines_and_surfaces(
        self,
    ) -> None:
        async_fingerprints = {}
        for factory in (MemoryEngine, SQLiteEngine):
            async with AsyncMongoClient(factory()) as client:
                collection = client.cost.search
                await collection.insert_many(DOCUMENTS)
                await collection.create_search_index(SEARCH_INDEX)
                explanation = await collection.aggregate(SEARCH_PIPELINE).explain(
                    "executionStats",
                )
            async_fingerprints[factory.__name__] = _search_cost_fingerprint(explanation)

        sync_fingerprints = {}
        for factory in (MemoryEngine, SQLiteEngine):
            with MongoClient(factory()) as client:
                collection = client.cost_sync.search
                collection.insert_many(DOCUMENTS)
                collection.create_search_index(SEARCH_INDEX)
                explanation = collection.aggregate(SEARCH_PIPELINE).explain(
                    "executionStats",
                )
            sync_fingerprints[factory.__name__] = _search_cost_fingerprint(explanation)

        for engine_name, fingerprint in async_fingerprints.items():
            with self.subTest(engine=engine_name):
                sync = sync_fingerprints[engine_name]
                self.assertEqual(fingerprint["plan"], fingerprint["enginePlan"])
                self.assertEqual(fingerprint, sync)
                self.assertEqual(fingerprint["plan"]["strategy"], "prefix-iterative")
                self.assertEqual(
                    fingerprint["plan"]["appliedRules"],
                    ["search.prefix-monotonic", "search.downstream-filter"],
                )
                self.assertEqual(fingerprint["queryMatched"]["value"], 3)
                self.assertEqual(fingerprint["queryMatched"]["domain"], "query")
                self.assertEqual(fingerprint["returnedHits"]["value"], 2)
                self.assertEqual(fingerprint["returnedHits"]["domain"], "result")
                self.assertEqual(
                    fingerprint["collectorDocuments"]["availability"],
                    "unavailable",
                )
                if engine_name == "SQLiteEngine":
                    self.assertLessEqual(fingerprint["candidates"]["value"], 3)
                    self.assertEqual(
                        fingerprint["topK"],
                        {
                            "applied": True,
                            "strategy": "stable-prefix",
                            "beforeCount": 3,
                            "afterCount": 2,
                            "cutoffMatchedShould": None,
                        },
                    )
                else:
                    self.assertEqual(
                        fingerprint["candidates"]["availability"],
                        "unavailable",
                    )
                    self.assertIsNone(fingerprint["topK"])
