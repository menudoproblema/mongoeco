import unittest
import asyncio

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.compat import MongoDialect70
from mongoeco.core.query_plan import MatchAll
from mongoeco.core.upserts import _seed_filter_value, seed_upsert_document
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
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

    def test_seed_upsert_document_keeps_literal_dict_values(self):
        document = {}

        seed_upsert_document(
            document,
            {
                "profile": {"nested": "x"},
                "empty": {},
            },
        )

        self.assertEqual(document, {"profile": {"nested": "x"}, "empty": {}})

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
                self.scan_dialect = None
                self.update_dialect = None
                self.delete_dialect = None
                self.count_dialect = None

            async def put_document(self, *args, **kwargs):
                return True

            async def get_document(self, *args, **kwargs):
                return None

            def scan_collection(self, *args, **kwargs):
                self.scan_plan = kwargs["plan"]
                self.scan_dialect = kwargs["dialect"]

                async def _scan():
                    if False:
                        yield None

                return _scan()

            async def update_matching_document(self, *args, **kwargs):
                self.update_plan = kwargs["plan"]
                self.update_dialect = kwargs["dialect"]
                from mongoeco.types import UpdateResult

                return UpdateResult(matched_count=0, modified_count=0)

            async def delete_matching_document(self, *args, **kwargs):
                self.delete_plan = kwargs["plan"]
                self.delete_dialect = kwargs["dialect"]
                from mongoeco.types import DeleteResult

                return DeleteResult(deleted_count=0)

            async def count_matching_documents(self, *args, **kwargs):
                self.count_plan = kwargs["plan"]
                self.count_dialect = kwargs["dialect"]
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
        self.assertIs(engine.scan_dialect, collection.mongodb_dialect)
        self.assertIs(engine.update_dialect, collection.mongodb_dialect)
        self.assertIs(engine.delete_dialect, collection.mongodb_dialect)
        self.assertIs(engine.count_dialect, collection.mongodb_dialect)

    def test_update_one_sort_is_rejected_by_older_pymongo_profile(self):
        collection = AsyncCollection(
            MemoryEngine(),
            "db",
            "coll",
            pymongo_profile="4.9",
        )

        with self.assertRaises(TypeError):
            asyncio.run(
                collection.update_one(
                    {"name": "Ada"},
                    {"$set": {"active": True}},
                    sort=[("rank", 1)],
                )
            )

    def test_update_one_sort_updates_first_sorted_match_when_profile_supports_it(self):
        async def _exercise() -> tuple[object, dict | None, dict | None]:
            collection = AsyncCollection(
                MemoryEngine(),
                "db",
                "coll",
                pymongo_profile="4.11",
            )
            await collection.insert_one({"_id": "first", "kind": "view", "rank": 2, "done": False})
            await collection.insert_one({"_id": "second", "kind": "view", "rank": 1, "done": False})

            result = await collection.update_one(
                {"kind": "view"},
                {"$set": {"done": True}},
                sort=[("rank", 1)],
            )
            first = await collection.find_one({"_id": "first"})
            second = await collection.find_one({"_id": "second"})
            return result, first, second

        result, first, second = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertFalse(first["done"])
        self.assertTrue(second["done"])

    def test_update_one_sort_returns_zero_counts_when_no_document_matches(self):
        async def _exercise():
            collection = AsyncCollection(
                MemoryEngine(),
                "db",
                "coll",
                pymongo_profile="4.11",
            )
            return await collection.update_one(
                {"kind": "view"},
                {"$set": {"done": True}},
                sort=[("rank", 1)],
            )

        result = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_update_one_sort_uses_custom_dialect_for_document_selection(self):
        class ReverseNumericDialect(MongoDialect70):
            def compare_values(self, left, right):
                if (
                    isinstance(left, (int, float))
                    and not isinstance(left, bool)
                    and isinstance(right, (int, float))
                    and not isinstance(right, bool)
                    and left != right
                ):
                    return -1 if left > right else 1
                return super().compare_values(left, right)

        async def _exercise(engine):
            await engine.connect()
            collection = AsyncCollection(
                engine,
                "db",
                "coll",
                mongodb_dialect=ReverseNumericDialect(),
                pymongo_profile="4.11",
            )
            try:
                await collection.insert_one({"_id": "low", "kind": "view", "rank": 1, "done": False})
                await collection.insert_one({"_id": "high", "kind": "view", "rank": 2, "done": False})

                result = await collection.update_one(
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                )
                low = await collection.find_one({"_id": "low"})
                high = await collection.find_one({"_id": "high"})
                return result, low, high
            finally:
                await engine.disconnect()

        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                result, low, high = asyncio.run(_exercise(engine))
                self.assertEqual(result.matched_count, 1)
                self.assertEqual(result.modified_count, 1)
                self.assertFalse(low["done"])
                self.assertTrue(high["done"])

    def test_update_one_applies_custom_dialect_validation_in_engines(self):
        class NoSetDialect(MongoDialect70):
            def supports_update_operator(self, name: str) -> bool:
                if name == "$set":
                    return False
                return super().supports_update_operator(name)

        async def _exercise(engine):
            await engine.connect()
            collection = AsyncCollection(
                engine,
                "db",
                "coll",
                mongodb_dialect=NoSetDialect(),
            )
            try:
                await collection.insert_one({"_id": "1", "done": False})
                await collection.update_one({"_id": "1"}, {"$set": {"done": True}})
            finally:
                await engine.disconnect()

        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                with self.assertRaises(OperationFailure):
                    asyncio.run(_exercise(engine))

    def test_custom_projection_dialect_is_honored_by_find_and_find_one(self):
        class ProjectionFlagTwoDialect(MongoDialect70):
            def projection_flag(self, value: object) -> int | None:
                if value == 2:
                    return 1
                return super().projection_flag(value)

        async def _exercise(engine):
            await engine.connect()
            collection = AsyncCollection(
                engine,
                "db",
                "coll",
                mongodb_dialect=ProjectionFlagTwoDialect(),
            )
            try:
                await collection.insert_one({"_id": "1", "name": "Ada", "role": "admin"})
                direct = await collection.find_one({"_id": "1"}, {"name": 2, "_id": 0})
                scanned = await collection.find({"name": "Ada"}, {"name": 2, "_id": 0}).to_list()
                return direct, scanned
            finally:
                await engine.disconnect()

        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                direct, scanned = asyncio.run(_exercise(engine))
                self.assertEqual(direct, {"name": "Ada"})
                self.assertEqual(scanned, [{"name": "Ada"}])
