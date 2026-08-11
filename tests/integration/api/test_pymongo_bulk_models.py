import unittest

from bson.objectid import ObjectId as BsonObjectId
from pymongo import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
from pymongo.collation import Collation

from mongoeco.errors import BulkWriteError
from tests.support import ENGINE_FACTORIES, open_client


class PyMongoBulkModelsIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_bulk_write_preserves_pymongo_model_options(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(
                    engine_name,
                    pymongo_profile="4.17",
                ) as client:
                    collection = client.test.pymongo_bulk_options
                    await collection.create_index([("name", 1)])
                    await collection.insert_many(
                        [
                            {
                                "_id": "low",
                                "name": "ALPHA",
                                "score": 1,
                                "items": [{"kind": "selected", "value": 0}],
                            },
                            {
                                "_id": "high",
                                "name": "alpha",
                                "score": 2,
                                "items": [{"kind": "selected", "value": 0}],
                            },
                        ]
                    )

                    result = await collection.bulk_write(
                        [
                            UpdateOne(
                                {"name": "alpha"},
                                {"$set": {"selected": True}},
                                collation=Collation("en", strength=2),
                                hint=[("name", 1)],
                                sort={"score": -1},
                            ),
                            UpdateMany(
                                {"name": "alpha"},
                                {"$inc": {"items.$[item].value": 1}},
                                collation=Collation("en", strength=2),
                                array_filters=[{"item.kind": "selected"}],
                                hint=[("name", 1)],
                            ),
                        ]
                    )

                    self.assertEqual(result.matched_count, 3)
                    self.assertEqual(result.modified_count, 3)
                    selected = await collection.find_one({"selected": True})
                    assert selected is not None
                    self.assertEqual(selected["_id"], "high")
                    self.assertEqual(
                        await collection.count_documents(
                            {"items.value": 1}
                        ),
                        2,
                    )

    async def test_bulk_write_enforces_update_sort_profile_gate(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(
                    engine_name,
                    pymongo_profile="4.9",
                ) as client:
                    collection = client.test.pymongo_bulk_profile
                    await collection.insert_one(
                        {"_id": "value", "score": 1}
                    )

                    with self.assertRaisesRegex(
                        TypeError,
                        "sort is not supported",
                    ):
                        await collection.bulk_write(
                            [
                                UpdateOne(
                                    {},
                                    {"$set": {"selected": True}},
                                    sort={"score": -1},
                                )
                            ]
                        )

    async def test_bulk_write_accepts_all_pymongo_write_models(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.pymongo_bulk
                    await collection.insert_many(
                        [
                            {"_id": "one", "value": 1},
                            {"_id": "many-a", "kind": "many", "value": 1},
                            {"_id": "many-b", "kind": "many", "value": 2},
                            {"_id": "replace", "value": 1},
                            {"_id": "delete-one", "kind": "delete-one"},
                            {"_id": "delete-many-a", "kind": "delete-many"},
                            {"_id": "delete-many-b", "kind": "delete-many"},
                        ]
                    )

                    result = await collection.bulk_write(
                        [
                            InsertOne({"_id": "inserted", "value": 1}),
                            UpdateOne(
                                {"_id": "one"},
                                {"$set": {"value": 2}},
                            ),
                            UpdateMany(
                                {"kind": "many"},
                                {"$inc": {"value": 1}},
                            ),
                            ReplaceOne(
                                {"_id": "replace"},
                                {"value": 9},
                            ),
                            DeleteOne({"kind": "delete-one"}),
                            DeleteMany({"kind": "delete-many"}),
                        ],
                        ordered=False,
                    )

                    self.assertEqual(result.inserted_count, 1)
                    self.assertEqual(result.matched_count, 4)
                    self.assertEqual(result.modified_count, 4)
                    self.assertEqual(result.deleted_count, 3)
                    self.assertEqual(
                        await collection.count_documents({}),
                        5,
                    )

    async def test_bulk_write_exposes_pymongo_object_id_for_upserts(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.pymongo_bulk_upsert
                    result = await collection.bulk_write(
                        [
                            UpdateOne(
                                {"kind": "generated"},
                                {"$set": {"value": 1}},
                                upsert=True,
                            )
                        ]
                    )

                    self.assertEqual(result.upserted_count, 1)
                    self.assertIs(type(result.upserted_ids[0]), BsonObjectId)

    async def test_bulk_write_mutates_insert_model_and_publicizes_partial_upserts(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.pymongo_bulk_public_ids
                    inserted = {"kind": "inserted"}
                    await collection.bulk_write([InsertOne(inserted)])

                    self.assertIs(type(inserted["_id"]), BsonObjectId)

                    with self.assertRaises(BulkWriteError) as error:
                        await collection.bulk_write(
                            [
                                UpdateOne(
                                    {"kind": "upserted"},
                                    {"$set": {"value": 1}},
                                    upsert=True,
                                ),
                                InsertOne({"_id": inserted["_id"]}),
                            ]
                        )

                    upserted = error.exception.details["upserted"]
                    self.assertEqual(len(upserted), 1)
                    self.assertIs(type(upserted[0]["_id"]), BsonObjectId)


if __name__ == "__main__":
    unittest.main()
