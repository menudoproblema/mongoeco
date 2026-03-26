import unittest
from mongoeco.errors import OperationFailure
from tests.support import ENGINE_FACTORIES, open_client

class TestUpdateIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_update_one_set(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "name": "Val", "age": 30})

                    result = await coll.update_one({"_id": "1"}, {"$set": {"age": 31, "city": "Madrid"}})

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(result.modified_count, 1)

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc["age"], 31)
                    self.assertEqual(doc["city"], "Madrid")

    async def test_update_one_nested_set(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "profile": {"name": "Val"}})

                    await coll.update_one({"_id": "1"}, {"$set": {"profile.age": 30, "profile.details.city": "Madrid"}})

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc["profile"]["age"], 30)
                    self.assertEqual(doc["profile"]["details"]["city"], "Madrid")

    async def test_update_one_upsert(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll

                    result = await coll.update_one({"name": "New"}, {"$set": {"age": 20}}, upsert=True)

                    self.assertEqual(result.matched_count, 0)
                    self.assertEqual(result.modified_count, 0)
                    self.assertIsNotNone(result.upserted_id)

                    doc = await coll.find_one({"_id": result.upserted_id})
                    self.assertEqual(doc["name"], "New")
                    self.assertEqual(doc["age"], 20)

    async def test_update_one_upsert_seeds_nested_and_eq_filter_fields(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
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
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1"})

                    result = await coll.update_one({"_id": "1"}, {"$set": {"field": None}})
                    doc = await coll.find_one({"_id": "1"})

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(result.modified_count, 1)
                    self.assertIn("field", doc)
                    self.assertIsNone(doc["field"])

    async def test_update_one_unknown_operator_raises_operation_failure(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "arr": []})

                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$currentDate": {"updated_at": True}})

    async def test_update_one_rejects_modifying_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "name": "Val"})

                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$set": {"_id": "2"}})

    async def test_update_one_supports_setting_array_index(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "tags": ["old", "keep"]})

                    result = await coll.update_one({"_id": "1"}, {"$set": {"tags.0": "new"}})
                    doc = await coll.find_one({"_id": "1"})

                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(doc["tags"], ["new", "keep"])

    async def test_update_one_supports_unsetting_array_index_to_none(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "tags": ["old", "keep"]})

                    result = await coll.update_one({"_id": "1"}, {"$unset": {"tags.0": ""}})
                    doc = await coll.find_one({"_id": "1"})

                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(doc["tags"], [None, "keep"])

    async def test_update_one_supports_setting_nested_array_path_with_gap_expansion(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "items": []})

                    result = await coll.update_one({"_id": "1"}, {"$set": {"items.2.name": "Ada"}})
                    doc = await coll.find_one({"_id": "1"})

                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(doc["items"], [None, None, {"name": "Ada"}])

    async def test_update_one_rejects_crossing_scalar_parent(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "profile": 1})

                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$set": {"profile.name": "Ada"}})

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc, {"_id": "1", "profile": 1})

    async def test_update_one_rejects_excessive_array_index_expansion(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "items": []})

                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$set": {"items.10001.name": "Ada"}})

    async def test_update_one_upsert_rejects_conflicting_seed_paths(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll

                    with self.assertRaises(OperationFailure):
                        await coll.update_one(
                            {"a": 1, "a.b": 2},
                            {"$set": {"done": True}},
                            upsert=True,
                        )

    async def test_update_one_supports_push_and_add_to_set_each(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "tags": ["python"], "roles": ["admin"]})

                    await coll.update_one({"_id": "1"}, {"$push": {"tags": {"$each": ["mongodb", "sqlite"]}}})
                    await coll.update_one({"_id": "1"}, {"$addToSet": {"roles": {"$each": ["admin", "staff", "staff"]}}})

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc["tags"], ["python", "mongodb", "sqlite"])
                    self.assertEqual(doc["roles"], ["admin", "staff"])

    async def test_update_one_rejects_invalid_each_modifier_payloads(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "tags": []})

                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$push": {"tags": {"$each": "python"}}})
                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$addToSet": {"tags": {"$each": ["python"], "$slice": 1}}})

    async def test_update_one_supports_inc_push_add_to_set_and_pull(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "count": 1, "tags": ["python"]})

                    await coll.update_one({"_id": "1"}, {"$inc": {"count": 2}})
                    await coll.update_one({"_id": "1"}, {"$push": {"tags": "mongodb"}})
                    await coll.update_one({"_id": "1"}, {"$addToSet": {"tags": "python"}})
                    await coll.update_one({"_id": "1"}, {"$addToSet": {"tags": "sqlite"}})
                    await coll.update_one({"_id": "1"}, {"$pull": {"tags": "mongodb"}})

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc["count"], 3)
                    self.assertEqual(doc["tags"], ["python", "sqlite"])

    async def test_update_one_rejects_invalid_inc_and_array_operator_targets(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "count": "x", "tags": "python"})

                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$inc": {"count": 1}})
                    with self.assertRaises(OperationFailure):
                        await coll.update_one({"_id": "1"}, {"$push": {"tags": "mongodb"}})

    async def test_update_one_supports_pop(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one({"_id": "1", "tags": ["python", "mongodb", "sqlite"]})

                    await coll.update_one({"_id": "1"}, {"$pop": {"tags": -1}})
                    await coll.update_one({"_id": "1"}, {"$pop": {"tags": 1}})

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc["tags"], ["mongodb"])

    async def test_update_one_supports_nested_pop_and_embedded_document_array_mutation(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one(
                        {
                            "_id": "1",
                            "profile": {"tags": ["python"]},
                            "items": [{"kind": "a"}],
                        }
                    )

                    await coll.update_one({"_id": "1"}, {"$pop": {"profile.tags": 1}})
                    await coll.update_one({"_id": "1"}, {"$addToSet": {"items": {"kind": "a"}}})
                    await coll.update_one({"_id": "1"}, {"$addToSet": {"items": {"kind": "b"}}})
                    await coll.update_one({"_id": "1"}, {"$pull": {"items": {"kind": "a"}}})

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc["profile"]["tags"], [])
                    self.assertEqual(doc["items"], [{"kind": "b"}])

    async def test_update_one_supports_pull_with_predicate_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    coll = client.test_db.test_coll
                    await coll.insert_one(
                        {
                            "_id": "1",
                            "items": [
                                {"kind": "a", "qty": 1},
                                {"kind": "b", "qty": 3},
                                {"kind": "c", "qty": 5},
                            ],
                        }
                    )

                    await coll.update_one({"_id": "1"}, {"$pull": {"items": {"qty": {"$gte": 3}}}})

                    doc = await coll.find_one({"_id": "1"})
                    self.assertEqual(doc["items"], [{"kind": "a", "qty": 1}])
