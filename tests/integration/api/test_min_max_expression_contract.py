import unittest

from mongoeco import MongoClient
from tests.support import ENGINE_FACTORIES, open_client


_PIPELINE = [
    {
        "$project": {
            "maximum": {"$max": ["$low", "$high", None]},
            "minimum": {"$min": ["$low", "$high", None]},
            "array_maximum": {"$max": "$values"},
            "array_sum": {"$sum": "$values"},
            "array_average": {"$avg": "$values"},
            "listed_sum": {"$sum": ["$low", "$high", "$values"]},
        }
    }
]


class MinMaxExpressionAsyncContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_min_max_expression_matrix(self):
        for engine_name in ENGINE_FACTORIES:
            for dialect in ("7.0", "8.0"):
                for profile in ("4.9", "4.11", "4.13", "4.17"):
                    with self.subTest(
                        engine=engine_name,
                        dialect=dialect,
                        profile=profile,
                    ):
                        async with open_client(
                            engine_name,
                            mongodb_dialect=dialect,
                            pymongo_profile=profile,
                        ) as client:
                            collection = client.test.min_max
                            await collection.insert_one(
                                {
                                    "_id": "value",
                                    "low": 2,
                                    "high": 7,
                                    "values": [1, "ignored", 9],
                                }
                            )
                            projected = (await collection.aggregate(
                                _PIPELINE
                            ).to_list())[0]
                            self.assertEqual(
                                projected,
                                {
                                    "_id": "value",
                                    "maximum": 7,
                                    "minimum": 2,
                                    "array_maximum": 9,
                                    "array_sum": 10,
                                    "array_average": 5,
                                    "listed_sum": 9,
                                },
                            )
                            await collection.update_one(
                                {"_id": "value"},
                                [
                                    {
                                        "$set": {
                                            "selected": {
                                                "$max": ["$low", "$high"]
                                            }
                                        }
                                    }
                                ],
                            )
                            updated = await collection.find_one(
                                {"_id": "value"}
                            )
                            assert updated is not None
                            self.assertEqual(updated["selected"], 7)


class MinMaxExpressionSyncContractTests(unittest.TestCase):
    def test_min_max_expression_sync_parity(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(engine_factory()) as client:
                    collection = client.test.min_max
                    collection.insert_one(
                        {
                            "_id": "value",
                            "low": 2,
                            "high": 7,
                            "values": [1, "ignored", 9],
                        }
                    )
                    projected = collection.aggregate(_PIPELINE).to_list()[0]
                    self.assertEqual(projected["maximum"], 7)
                    self.assertEqual(projected["minimum"], 2)
                    self.assertEqual(projected["array_maximum"], 9)
                    self.assertEqual(projected["array_sum"], 10)
                    self.assertEqual(projected["array_average"], 5)
                    self.assertEqual(projected["listed_sum"], 9)


if __name__ == "__main__":
    unittest.main()
