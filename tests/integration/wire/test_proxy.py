import asyncio
import unittest

try:
    from pymongo import MongoClient as PyMongoClient
except Exception:  # pragma: no cover
    PyMongoClient = None

from mongoeco.engines.memory import MemoryEngine
from mongoeco.wire import AsyncMongoEcoProxyServer


@unittest.skipIf(PyMongoClient is None, "pymongo is required for wire proxy tests")
class WireProxyIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_proxy_supports_ping_insert_find_and_aggregate_through_pymongo(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
            uri = proxy.address.uri

            def _exercise() -> tuple[dict, dict, list[dict]]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    ping = client.admin.command("ping")
                    collection = client.alpha.events
                    insert_result = collection.insert_one({"kind": "view", "score": 2})
                    found = collection.find_one({"_id": insert_result.inserted_id})
                    aggregated = list(
                        collection.aggregate(
                            [
                                {"$match": {"kind": "view"}},
                                {"$project": {"_id": 0, "score": {"$add": ["$score", 1]}}},
                            ],
                            allowDiskUse=True,
                        )
                    )
                    return ping, found, aggregated
                finally:
                    client.close()

            ping, found, aggregated = await asyncio.to_thread(_exercise)

            self.assertEqual(ping["ok"], 1.0)
            self.assertEqual(found["kind"], "view")
            self.assertEqual(found["score"], 2)
            self.assertEqual(aggregated, [{"score": 3}])
