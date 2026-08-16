from __future__ import annotations

import unittest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


DOCUMENTS = (
    {"_id": 1, "kind": "keep", "score": 4},
    {"_id": 2, "kind": "keep", "score": 8},
    {"_id": 3, "kind": "keep", "score": 10},
    {"_id": 4, "kind": "drop", "score": 100},
)
FILTER = {"kind": "keep", "$expr": {"$gt": ["$score", 5]}}


def _configure_cursor(cursor):
    return cursor.sort([("score", -1)]).skip(1).limit(1)


class AsyncSQLitePushdownEquivalenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_unprofitable_conjunct_shape_stays_on_exact_python_fallback(
        self,
    ) -> None:
        async with AsyncMongoClient(SQLiteEngine()) as sqlite_client:
            indexed = sqlite_client.pushdown.indexed
            reference = sqlite_client.pushdown.reference
            await indexed.insert_many(DOCUMENTS)
            await reference.insert_many(DOCUMENTS)
            await indexed.create_index([("kind", 1)])

            optimized = await _configure_cursor(indexed.find(FILTER)).to_list()
            unoptimized = await _configure_cursor(reference.find(FILTER)).to_list()
            explanation = await _configure_cursor(indexed.find(FILTER)).explain()

        async with AsyncMongoClient(MemoryEngine()) as memory_client:
            memory = memory_client.pushdown.documents
            await memory.insert_many(DOCUMENTS)
            expected = await _configure_cursor(memory.find(FILTER)).to_list()

        self.assertEqual(optimized, [{"_id": 2, "kind": "keep", "score": 8}])
        self.assertEqual(optimized, unoptimized)
        self.assertEqual(optimized, expected)
        pushdown = explanation["details"]["pushdown"]
        self.assertEqual(pushdown["contractMode"], "python")
        self.assertFalse(pushdown["residualRequired"])
        self.assertEqual(pushdown["filterOwner"], "python")
        self.assertEqual(pushdown["sortOwner"], "python")
        self.assertEqual(pushdown["windowOwner"], "python")


class SyncSQLitePushdownEquivalenceTests(unittest.TestCase):
    def test_rejected_conjunct_pushdown_matches_python_reference(self) -> None:
        with MongoClient(SQLiteEngine()) as sqlite_client:
            indexed = sqlite_client.pushdown_sync.indexed
            reference = sqlite_client.pushdown_sync.reference
            indexed.insert_many(DOCUMENTS)
            reference.insert_many(DOCUMENTS)
            indexed.create_index([("kind", 1)])

            optimized = _configure_cursor(indexed.find(FILTER)).to_list()
            unoptimized = _configure_cursor(reference.find(FILTER)).to_list()

        with MongoClient(MemoryEngine()) as memory_client:
            memory = memory_client.pushdown_sync.documents
            memory.insert_many(DOCUMENTS)
            expected = _configure_cursor(memory.find(FILTER)).to_list()

        self.assertEqual(optimized, unoptimized)
        self.assertEqual(optimized, expected)
