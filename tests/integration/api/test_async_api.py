import datetime
import unittest
import uuid

from mongoeco import ClientSession
from mongoeco.errors import DuplicateKeyError
from tests.support import ENGINE_FACTORIES, open_client


class AsyncApiIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_supports_attribute_and_item_access(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client["alpha"]["users"].insert_one({"_id": "1", "name": "Ada"})

                    found = await client.alpha.users.find_one({"_id": "1"})
                    collections = await client["alpha"].list_collection_names()

                    self.assertEqual(found, {"_id": "1", "name": "Ada"})
                    self.assertEqual(collections, ["users"])

    async def test_find_one_without_filter_returns_first_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    found = await collection.find_one()

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_insert_one_generates_id_and_persists_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    payload = {
                        "name": "Ada",
                        "created_at": datetime.datetime(2026, 3, 23, 10, 0, 0),
                        "owner_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                    }

                    result = await collection.insert_one(payload)
                    found = await collection.find_one({"_id": result.inserted_id})

                    self.assertTrue(result.inserted_id)
                    self.assertEqual(found["_id"], result.inserted_id)
                    self.assertEqual(found["name"], "Ada")
                    self.assertEqual(found["created_at"], payload["created_at"])
                    self.assertEqual(found["owner_id"], payload["owner_id"])
                    self.assertNotIn("_id", payload)

    async def test_insert_one_duplicate_id_raises_duplicate_key_error(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "same", "name": "Ada"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "same", "name": "Grace"})

    async def test_find_one_projection_supports_inclusion_and_id_exclusion(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": "user-1"}, {"name": 1, "_id": 0})

                    self.assertEqual(found, {"name": "Ada"})

    async def test_find_one_projection_supports_exclusion_with_id_included(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": "user-1"}, {"role": 0, "_id": 1})

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_find_one_supports_id_operator_filter_without_direct_lookup_crash(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    found = await collection.find_one({"_id": {"$eq": "user-1"}})

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_update_and_delete_support_id_operator_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    update_result = await collection.update_one(
                        {"_id": {"$eq": "user-1"}},
                        {"$set": {"active": True}},
                    )
                    delete_result = await collection.delete_one({"_id": {"$eq": "user-1"}})

                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(delete_result.deleted_count, 1)

    async def test_insert_one_supports_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}

                    result = await collection.insert_one({"_id": doc_id, "name": "Ada"})
                    found = await collection.find_one({"_id": {"tenant": 1, "user": 2}})

                    self.assertEqual(result.inserted_id, doc_id)
                    self.assertEqual(found, {"_id": doc_id, "name": "Ada"})

    async def test_find_one_supports_list_id_via_direct_lookup(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = [1, 2, 3]
                    await collection.insert_one({"_id": doc_id, "name": "Ada"})

                    found = await collection.find_one({"_id": [1, 2, 3]})

                    self.assertEqual(found, {"_id": doc_id, "name": "Ada"})

    async def test_count_documents_uses_core_filtering_rules(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "payload": {"kind": "view"}})
                    await collection.insert_one({"_id": "2", "payload": {"kind": "click"}})
                    await collection.insert_one({"_id": "3", "payload": {"kind": "view"}})

                    count = await collection.count_documents({"payload.kind": "view"})

                    self.assertEqual(count, 2)

    async def test_find_supports_iteration_with_sort_skip_and_limit(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 3})
                    await collection.insert_one({"_id": "2", "rank": 1})
                    await collection.insert_one({"_id": "3", "rank": 2})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {},
                            sort=[("rank", 1)],
                            skip=1,
                            limit=1,
                        )
                    ]

                    self.assertEqual(documents, [{"_id": "3", "rank": 2}])

    async def test_find_supports_filter_sort_skip_limit_and_projection_together(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})
                    await collection.insert_one({"_id": "4", "kind": "view", "rank": 4, "payload": {"city": "Valencia"}})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {"kind": "view"},
                            {"payload.city": 1, "_id": 0},
                            sort=[("rank", 1)],
                            skip=1,
                            limit=1,
                        )
                    ]

                    self.assertEqual(documents, [{"payload": {"city": "Sevilla"}}])

    async def test_find_supports_array_sort_and_projection_together(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": [3, 8], "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "rank": [1, 9], "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "rank": [2, 4], "payload": {"city": "Bilbao"}})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {},
                            {"payload.city": 1, "_id": 0},
                            sort=[("rank", 1)],
                        )
                    ]

                    self.assertEqual(
                        documents,
                        [
                            {"payload": {"city": "Madrid"}},
                            {"payload": {"city": "Bilbao"}},
                            {"payload": {"city": "Sevilla"}},
                        ],
                    )

    async def test_async_client_exposes_session_placeholder_and_accepts_it(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    self.assertIsInstance(session, ClientSession)
                    async with session:
                        result = await client.test.users.insert_one({"name": "Ada"}, session=session)
                        found = await client.test.users.find_one({"_id": result.inserted_id}, session=session)

                    self.assertFalse(session.active)
                    self.assertEqual(found["name"], "Ada")

    async def test_async_session_state_reflects_connected_engine(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()

                    self.assertEqual(session.get_engine_state("memory"), {"connected": True})

    async def test_collection_can_manage_index_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    name = await collection.create_index(["payload.kind"], unique=False)
                    indexes = await collection.list_indexes()

                    self.assertEqual(name, "payload.kind_1")
                    self.assertEqual(
                        indexes,
                        [{"name": "payload.kind_1", "fields": ["payload.kind"], "unique": False}],
                    )

    async def test_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["email"], unique=True)
                    await collection.insert_one({"_id": "1", "email": "a@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "2", "email": "a@example.com"})

    async def test_compound_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["tenant", "email"], unique=True)
                    await collection.insert_one({"_id": "1", "tenant": "a", "email": "x@example.com"})
                    await collection.insert_one({"_id": "2", "tenant": "b", "email": "x@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "3", "tenant": "a", "email": "x@example.com"})

    async def test_nested_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["profile.email"], unique=True)
                    await collection.insert_one({"_id": "1", "profile": {"email": "a@example.com"}})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "2", "profile": {"email": "a@example.com"}})

    async def test_delete_one_removes_first_matching_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "role": "admin"})

                    result = await collection.delete_one({"role": "admin"})

                    self.assertEqual(result.deleted_count, 1)
                    self.assertIsNone(await collection.find_one({"_id": "user-1"}))

    async def test_delete_one_without_match_returns_zero(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    result = await client.test.users.delete_one({"role": "admin"})

                    self.assertEqual(result.deleted_count, 0)

    async def test_update_and_delete_support_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    await collection.insert_one({"_id": doc_id, "name": "Ada"})

                    update_result = await collection.update_one(
                        {"_id": {"tenant": 1, "user": 2}},
                        {"$set": {"active": True}},
                    )
                    delete_result = await collection.delete_one({"_id": {"tenant": 1, "user": 2}})

                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(delete_result.deleted_count, 1)

    async def test_find_one_projection_supports_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    await collection.insert_one({"_id": doc_id, "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": {"tenant": 1, "user": 2}}, {"name": 1, "_id": 0})

                    self.assertEqual(found, {"name": "Ada"})

    async def test_client_and_database_expose_collection_and_database_names(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.users.insert_one({"_id": "1"})
                    await client.beta.events.insert_one({"_id": "2"})

                    databases = await client.list_database_names()
                    collections = await client.alpha.list_collection_names()

                    self.assertEqual(set(databases), {"alpha", "beta"})
                    self.assertEqual(collections, ["users"])

    async def test_invalid_input_types_raise_type_error(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    with self.assertRaises(TypeError):
                        await collection.insert_one([])

                    with self.assertRaises(TypeError):
                        await collection.find_one([])

                    with self.assertRaises(TypeError):
                        await collection.find_one({}, [])

                    with self.assertRaises(TypeError):
                        await collection.update_one([], {})

                    with self.assertRaises(TypeError):
                        await collection.update_one({}, [])

                    with self.assertRaises(TypeError):
                        await collection.delete_one([])

                    with self.assertRaises(TypeError):
                        await collection.count_documents([])

    async def test_update_one_rejects_invalid_update_shapes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    with self.assertRaises(ValueError):
                        await collection.update_one({}, {})

                    with self.assertRaises(ValueError):
                        await collection.update_one({}, {"name": "Ada"})

                    with self.assertRaises(TypeError):
                        await collection.update_one({}, {"$set": []})
