import unittest
import asyncio

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.core.query_plan import MatchAll
from mongoeco.core.upserts import _seed_filter_value, seed_upsert_document
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import OperationFailure


class AsyncCollectionHelperTests(unittest.TestCase):
    def setUp(self):
        self.collection = AsyncCollection(MemoryEngine(), "db", "coll")

    def test_seed_upsert_document_skips_top_level_operators_and_non_eq_filters(self):
        document = {}

        seed_upsert_document(
            document,
            {
                "$or": [{"name": "Ada"}],
                "name": {"$eq": "Ada"},
                "age": {"$gt": 18},
                "profile.city": "Madrid",
            },
        )

        self.assertEqual(document, {"name": "Ada", "profile": {"city": "Madrid"}})

    def test_direct_id_lookup_only_applies_to_literal_id_selector(self):
        self.assertTrue(self.collection._can_use_direct_id_lookup({"_id": "user-1"}))
        self.assertTrue(self.collection._can_use_direct_id_lookup({"_id": [1, 2, 3]}))
        self.assertFalse(self.collection._can_use_direct_id_lookup({"_id": {"$eq": "user-1"}}))
        self.assertFalse(self.collection._can_use_direct_id_lookup({"name": "Ada"}))

    def test_seed_upsert_document_rejects_conflicting_paths(self):
        with self.assertRaises(OperationFailure):
            seed_upsert_document({}, {"a": 1, "a.b": 2})

    def test_seed_filter_value_rejects_conflicting_existing_scalar(self):
        with self.assertRaises(OperationFailure):
            _seed_filter_value({"a": 1}, "a", 2)

    def test_seed_filter_value_allows_same_existing_scalar(self):
        document = {"a": 1}

        _seed_filter_value(document, "a", 1)

        self.assertEqual(document, {"a": 1})

    def test_seed_filter_value_rejects_bool_number_conflict(self):
        with self.assertRaises(OperationFailure):
            _seed_filter_value({"a": 1}, "a", True)

    def test_find_one_reapplies_projection_if_engine_ignores_it(self):
        class EngineStub:
            async def get_document(self, *args, **kwargs):
                return {"_id": "1", "name": "Ada", "role": "admin"}

            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    yield {"_id": "1", "name": "Ada", "role": "admin"}

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        direct = asyncio.run(collection.find_one({"_id": "1"}, {"name": 1, "_id": 0}))
        scanned = asyncio.run(collection.find_one({"name": "Ada"}, {"role": 0, "_id": 1}))

        self.assertEqual(direct, {"name": "Ada"})
        self.assertEqual(scanned, {"_id": "1", "name": "Ada"})

    def test_collection_compiles_plan_once_and_passes_it_to_engine(self):
        class EngineStub:
            def __init__(self):
                self.scan_plan = None
                self.update_plan = None
                self.delete_plan = None
                self.count_plan = None

            async def put_document(self, *args, **kwargs):
                return True

            async def get_document(self, *args, **kwargs):
                return None

            def scan_collection(self, *args, **kwargs):
                self.scan_plan = kwargs["plan"]

                async def _scan():
                    if False:
                        yield None

                return _scan()

            async def update_matching_document(self, *args, **kwargs):
                self.update_plan = kwargs["plan"]
                from mongoeco.types import UpdateResult

                return UpdateResult(matched_count=0, modified_count=0)

            async def delete_matching_document(self, *args, **kwargs):
                self.delete_plan = kwargs["plan"]
                from mongoeco.types import DeleteResult

                return DeleteResult(deleted_count=0)

            async def count_matching_documents(self, *args, **kwargs):
                self.count_plan = kwargs["plan"]
                return 0

            async def create_index(self, *args, **kwargs):
                return "idx"

            async def list_indexes(self, *args, **kwargs):
                return []

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        asyncio.run(collection.find({"name": "Ada"}).to_list())
        asyncio.run(collection.update_one({"name": "Ada"}, {"$set": {"active": True}}))
        asyncio.run(collection.delete_one({"name": "Ada"}))
        asyncio.run(collection.count_documents({"name": "Ada"}))

        self.assertEqual(type(engine.scan_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.update_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.delete_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.count_plan).__name__, "EqualsCondition")
        self.assertNotIsInstance(engine.scan_plan, MatchAll)
