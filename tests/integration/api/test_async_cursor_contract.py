import unittest

from tests.support import ENGINE_FACTORIES, open_client


class AsyncCursorContractIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_find_cursor_supports_incremental_to_list_and_close(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.cursor_contract
                    await collection.insert_many(
                        [
                            {"_id": "second", "order": 2},
                            {"_id": "first", "order": 1},
                            {"_id": "third", "order": 3},
                        ]
                    )
                    cursor = collection.find({}).sort("order", 1)

                    self.assertEqual(
                        await cursor.to_list(length=1),
                        [{"_id": "first", "order": 1}],
                    )
                    self.assertEqual(
                        await cursor.to_list(length=1),
                        [{"_id": "second", "order": 2}],
                    )
                    await cursor.close()
                    self.assertEqual(await cursor.to_list(length=None), [])

                    with self.assertRaisesRegex(ValueError, "non-negative"):
                        await collection.find({}).to_list(length=-1)

    async def test_aggregation_cursor_is_incremental_and_single_use(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.aggregation_cursor_contract
                    await collection.insert_many(
                        [
                            {"_id": "first", "order": 1},
                            {"_id": "second", "order": 2},
                            {"_id": "third", "order": 3},
                        ]
                    )
                    cursor = collection.aggregate(
                        [{"$sort": {"order": 1}}]
                    )

                    self.assertEqual(
                        await cursor.to_list(length=1),
                        [{"_id": "first", "order": 1}],
                    )
                    self.assertEqual(
                        await cursor.to_list(length=1),
                        [{"_id": "second", "order": 2}],
                    )
                    await cursor.close()
                    self.assertEqual(await cursor.to_list(length=None), [])

                    with self.assertRaisesRegex(ValueError, "non-negative"):
                        await collection.aggregate([]).to_list(length=-1)


class SyncCursorContractIntegrationTests(unittest.TestCase):
    def test_find_and_aggregate_to_list_are_incremental(self):
        from mongoeco import MongoClient

        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(engine_factory()) as client:
                    collection = client.test.sync_cursor_contract
                    collection.insert_many(
                        [
                            {"_id": "second", "order": 2},
                            {"_id": "first", "order": 1},
                            {"_id": "third", "order": 3},
                        ]
                    )

                    cursor = collection.find({}).sort("order", 1)
                    self.assertEqual(
                        cursor.to_list(length=1),
                        [{"_id": "first", "order": 1}],
                    )
                    self.assertEqual(
                        cursor.to_list(length=1),
                        [{"_id": "second", "order": 2}],
                    )
                    self.assertEqual(
                        cursor.to_list(length=None),
                        [{"_id": "third", "order": 3}],
                    )

                    aggregate = collection.aggregate(
                        [{"$sort": {"order": 1}}]
                    )
                    self.assertEqual(
                        aggregate.to_list(length=2),
                        [
                            {"_id": "first", "order": 1},
                            {"_id": "second", "order": 2},
                        ],
                    )
                    self.assertEqual(
                        aggregate.to_list(length=None),
                        [{"_id": "third", "order": 3}],
                    )

                    with self.assertRaisesRegex(ValueError, "non-negative"):
                        collection.find({}).to_list(length=-1)


if __name__ == "__main__":
    unittest.main()
