import unittest
from mongoeco.engines.memory import MemoryEngine
from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.errors import OperationFailure

class TestUpdateIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_update_one_set(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1", "name": "Val", "age": 30})
            
            result = await coll.update_one({"_id": "1"}, {"$set": {"age": 31, "city": "Madrid"}})
            
            self.assertEqual(result.matched_count, 1)
            self.assertEqual(result.modified_count, 1)
            
            doc = await coll.find_one({"_id": "1"})
            self.assertEqual(doc["age"], 31)
            self.assertEqual(doc["city"], "Madrid")

    async def test_update_one_nested_set(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1", "profile": {"name": "Val"}})
            
            await coll.update_one({"_id": "1"}, {"$set": {"profile.age": 30, "profile.details.city": "Madrid"}})
            
            doc = await coll.find_one({"_id": "1"})
            self.assertEqual(doc["profile"]["age"], 30)
            self.assertEqual(doc["profile"]["details"]["city"], "Madrid")

    async def test_update_one_upsert(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            
            result = await coll.update_one({"name": "New"}, {"$set": {"age": 20}}, upsert=True)
            
            self.assertEqual(result.matched_count, 0)
            self.assertEqual(result.modified_count, 0)
            self.assertIsNotNone(result.upserted_id)
            
            doc = await coll.find_one({"_id": result.upserted_id})
            self.assertEqual(doc["name"], "New")
            self.assertEqual(doc["age"], 20)

    async def test_update_one_upsert_seeds_nested_and_eq_filter_fields(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll

            result = await coll.update_one(
                {"profile.name": {"$eq": "Val"}, "kind": "user"},
                {"$set": {"profile.age": 30}},
                upsert=True,
            )

            self.assertEqual(result.modified_count, 0)
            doc = await coll.find_one({"_id": result.upserted_id})
            self.assertEqual(doc["kind"], "user")
            self.assertEqual(doc["profile"]["name"], "Val")
            self.assertEqual(doc["profile"]["age"], 30)

    async def test_update_one_set_none_creates_field(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1"})

            result = await coll.update_one({"_id": "1"}, {"$set": {"field": None}})
            doc = await coll.find_one({"_id": "1"})

            self.assertEqual(result.matched_count, 1)
            self.assertEqual(result.modified_count, 1)
            self.assertIn("field", doc)
            self.assertIsNone(doc["field"])

    async def test_update_one_unknown_operator_raises_operation_failure(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1", "arr": []})

            with self.assertRaises(OperationFailure):
                await coll.update_one({"_id": "1"}, {"$push": {"arr": 1}})

    async def test_update_one_rejects_modifying_id(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1", "name": "Val"})

            with self.assertRaises(OperationFailure):
                await coll.update_one({"_id": "1"}, {"$set": {"_id": "2"}})

    async def test_update_one_supports_setting_array_index(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1", "tags": ["old", "keep"]})

            result = await coll.update_one({"_id": "1"}, {"$set": {"tags.0": "new"}})
            doc = await coll.find_one({"_id": "1"})

            self.assertEqual(result.modified_count, 1)
            self.assertEqual(doc["tags"], ["new", "keep"])

    async def test_update_one_supports_unsetting_array_index_to_none(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1", "tags": ["old", "keep"]})

            result = await coll.update_one({"_id": "1"}, {"$unset": {"tags.0": ""}})
            doc = await coll.find_one({"_id": "1"})

            self.assertEqual(result.modified_count, 1)
            self.assertEqual(doc["tags"], [None, "keep"])

    async def test_update_one_supports_setting_nested_array_path_with_gap_expansion(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll
            await coll.insert_one({"_id": "1", "items": []})

            result = await coll.update_one({"_id": "1"}, {"$set": {"items.2.name": "Ada"}})
            doc = await coll.find_one({"_id": "1"})

            self.assertEqual(result.modified_count, 1)
            self.assertEqual(doc["items"], [None, None, {"name": "Ada"}])

    async def test_update_one_upsert_rejects_conflicting_seed_paths(self):
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.test_db.test_coll

            with self.assertRaises(OperationFailure):
                await coll.update_one(
                    {"a": 1, "a.b": 2},
                    {"$set": {"done": True}},
                    upsert=True,
                )
