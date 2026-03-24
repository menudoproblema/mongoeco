import unittest
import threading

from mongoeco import ClientSession, MongoClient, ObjectId
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import DuplicateKeyError, InvalidOperation


class SyncApiIntegrationTests(unittest.TestCase):
    def test_client_supports_attribute_and_item_access_with_lazy_connect(self):
        client = MongoClient()
        try:
            client["alpha"]["users"].insert_one({"_id": "1", "name": "Ada"})

            found = client.alpha.users.find_one({"_id": "1"})
            collections = client["alpha"].list_collection_names()

            self.assertEqual(found, {"_id": "1", "name": "Ada"})
            self.assertEqual(collections, ["users"])
        finally:
            client.close()

    def test_insert_find_and_list_names(self):
        with MongoClient() as client:
            collection = client.test.users

            result = collection.insert_one({"name": "Ada"})
            found = collection.find_one({"_id": result.inserted_id})

            self.assertIsInstance(result.inserted_id, ObjectId)
            self.assertEqual(found["name"], "Ada")
            self.assertEqual(set(client.list_database_names()), {"test"})
            self.assertEqual(client.test.list_collection_names(), ["users"])

    def test_duplicate_id_raises(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.insert_one({"_id": "same", "name": "Ada"})

            with self.assertRaises(DuplicateKeyError):
                collection.insert_one({"_id": "same", "name": "Grace"})

    def test_update_and_projection(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.insert_one({"_id": "user-1", "profile": {"name": "Ada"}})

            result = collection.update_one(
                {"_id": "user-1"},
                {"$set": {"profile.role": "admin"}},
            )
            found = collection.find_one({"_id": "user-1"}, {"profile.role": 1, "_id": 0})

            self.assertEqual(result.matched_count, 1)
            self.assertEqual(found, {"profile": {"role": "admin"}})

    def test_find_supports_sort_skip_and_limit(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "rank": 3})
            collection.insert_one({"_id": "2", "rank": 1})
            collection.insert_one({"_id": "3", "rank": 2})

            documents = collection.find({}, sort=[("rank", 1)], skip=1, limit=1)

            self.assertEqual(documents, [{"_id": "3", "rank": 2}])

    def test_find_supports_first_call_without_prior_connection(self):
        client = MongoClient()
        try:
            documents = client.test.users.find({})
        finally:
            client.close()

        self.assertEqual(documents, [])

    def test_find_supports_filter_sort_skip_limit_and_projection_together(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
            collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
            collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})
            collection.insert_one({"_id": "4", "kind": "view", "rank": 4, "payload": {"city": "Valencia"}})

            documents = collection.find(
                {"kind": "view"},
                {"payload.city": 1, "_id": 0},
                sort=[("rank", 1)],
                skip=1,
                limit=1,
            )

            self.assertEqual(documents, [{"payload": {"city": "Sevilla"}}])

    def test_find_supports_array_sort_and_projection_together(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "rank": [3, 8], "payload": {"city": "Sevilla"}})
            collection.insert_one({"_id": "2", "rank": [1, 9], "payload": {"city": "Madrid"}})
            collection.insert_one({"_id": "3", "rank": [2, 4], "payload": {"city": "Bilbao"}})

            documents = collection.find({}, {"payload.city": 1, "_id": 0}, sort=[("rank", 1)])

            self.assertEqual(
                documents,
                [
                    {"payload": {"city": "Madrid"}},
                    {"payload": {"city": "Bilbao"}},
                    {"payload": {"city": "Sevilla"}},
                ],
            )

    def test_index_metadata_round_trip(self):
        with MongoClient() as client:
            collection = client.test.users

            name = collection.create_index(["profile.name"], unique=False)
            indexes = collection.list_indexes()

            self.assertEqual(name, "profile.name_1")
            self.assertEqual(
                indexes,
                [{"name": "profile.name_1", "fields": ["profile.name"], "unique": False}],
            )

    def test_unique_index_is_enforced(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.create_index(["email"], unique=True)
            collection.insert_one({"_id": "1", "email": "a@example.com"})

            with self.assertRaises(DuplicateKeyError):
                collection.insert_one({"_id": "2", "email": "a@example.com"})

    def test_compound_unique_index_is_enforced(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.create_index(["tenant", "email"], unique=True)
            collection.insert_one({"_id": "1", "tenant": "a", "email": "x@example.com"})
            collection.insert_one({"_id": "2", "tenant": "b", "email": "x@example.com"})

            with self.assertRaises(DuplicateKeyError):
                collection.insert_one({"_id": "3", "tenant": "a", "email": "x@example.com"})

    def test_nested_unique_index_is_enforced(self):
        with MongoClient() as client:
            collection = client.test.users
            collection.create_index(["profile.email"], unique=True)
            collection.insert_one({"_id": "1", "profile": {"email": "a@example.com"}})

            with self.assertRaises(DuplicateKeyError):
                collection.insert_one({"_id": "2", "profile": {"email": "a@example.com"}})

    def test_sync_client_exposes_session_placeholder_and_accepts_it(self):
        with MongoClient() as client:
            session = client.start_session()
            self.assertIsInstance(session, ClientSession)

            with session:
                result = client.test.users.insert_one({"name": "Ada"}, session=session)
                found = client.test.users.find_one({"_id": result.inserted_id}, session=session)

            self.assertFalse(session.active)
            self.assertEqual(found["name"], "Ada")

    def test_sync_session_state_reflects_connected_engine(self):
        with MongoClient() as client:
            session = client.start_session()

            self.assertEqual(session.get_engine_state("memory"), {"connected": True})

    def test_sync_session_transaction_placeholder_round_trip(self):
        with MongoClient() as client:
            session = client.start_session()

            session.start_transaction()
            self.assertTrue(session.transaction_active)
            client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=session)
            session.end_transaction()

            self.assertFalse(session.transaction_active)

    def test_update_and_delete_support_embedded_document_id(self):
        with MongoClient() as client:
            collection = client.test.users
            doc_id = {"tenant": 1, "user": 2}
            collection.insert_one({"_id": doc_id, "name": "Ada"})

            update_result = collection.update_one(
                {"_id": {"tenant": 1, "user": 2}},
                {"$set": {"active": True}},
            )
            delete_result = collection.delete_one({"_id": {"tenant": 1, "user": 2}})

            self.assertEqual(update_result.matched_count, 1)
            self.assertEqual(delete_result.deleted_count, 1)

    def test_find_one_projection_supports_embedded_document_id(self):
        with MongoClient() as client:
            collection = client.test.users
            doc_id = {"tenant": 1, "user": 2}
            collection.insert_one({"_id": doc_id, "name": "Ada", "role": "admin"})

            found = collection.find_one({"_id": {"tenant": 1, "user": 2}}, {"name": 1, "_id": 0})

            self.assertEqual(found, {"name": "Ada"})

    def test_shared_memory_engine_is_safe_across_sync_threads(self):
        engine = MemoryEngine()
        errors: list[tuple[int, object]] = []

        def worker(i: int) -> None:
            try:
                with MongoClient(engine) as client:
                    client.test.users.insert_one({"_id": str(i), "n": i})
                    found = client.test.users.find_one({"_id": str(i)})
                    if found is None or found["n"] != i:
                        errors.append((i, found))
            except Exception as exc:  # pragma: no cover
                errors.append((i, exc))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])

    def test_invalid_input_types_raise_type_error(self):
        with MongoClient() as client:
            collection = client.test.users

            with self.assertRaises(TypeError):
                collection.insert_one([])

            with self.assertRaises(TypeError):
                collection.find_one([])

            with self.assertRaises(TypeError):
                collection.find_one({}, [])

            with self.assertRaises(TypeError):
                collection.update_one([], {})

            with self.assertRaises(TypeError):
                collection.update_one({}, [])

            with self.assertRaises(TypeError):
                collection.delete_one([])

            with self.assertRaises(TypeError):
                collection.count_documents([])

    def test_update_one_rejects_invalid_update_shapes(self):
        with MongoClient() as client:
            collection = client.test.users

            with self.assertRaises(ValueError):
                collection.update_one({}, {})

            with self.assertRaises(ValueError):
                collection.update_one({}, {"name": "Ada"})

            with self.assertRaises(TypeError):
                collection.update_one({}, {"$set": []})

    def test_operations_after_close_raise_invalid_operation(self):
        client = MongoClient()
        collection = client.test.users
        client.close()

        with self.assertRaises(InvalidOperation):
            client.list_database_names()

        with self.assertRaises(InvalidOperation):
            collection.find_one({})


class SyncApiLoopSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_client_rejects_usage_inside_active_event_loop(self):
        client = MongoClient()
        try:
            with self.assertRaises(InvalidOperation):
                client.list_database_names()
        finally:
            client.close()
