import unittest
import asyncio

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.compat import MongoDialect70
from mongoeco.core.query_plan import MatchAll
from mongoeco.core.upserts import _seed_filter_value, seed_upsert_document
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.types import ReturnDocument


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

    def test_insert_many_requires_non_empty_document_list(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.insert_many({"name": "Ada"}))  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            asyncio.run(self.collection.insert_many([]))
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.insert_many([{"name": "Ada"}, []]))  # type: ignore[list-item]

    def test_insert_many_raises_duplicate_key_error_when_engine_rejects_document(self):
        class EngineStub:
            def __init__(self):
                self.calls = 0

            async def put_document(self, *args, **kwargs):
                self.calls += 1
                return self.calls == 1

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with self.assertRaisesRegex(DuplicateKeyError, "Duplicate key"):
            asyncio.run(collection.insert_many([{"_id": "1"}, {"_id": "1"}]))

    def test_update_many_returns_zero_counts_when_nothing_matches_and_upsert_is_false(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.update_many({"kind": "missing"}, {"$set": {"done": True}}))

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_replace_one_rejects_update_operator_document(self):
        with self.assertRaises(ValueError):
            asyncio.run(self.collection.replace_one({"name": "Ada"}, {"$set": {"name": "Grace"}}))

    def test_replace_one_rejects_non_document_replacement(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.replace_one({"name": "Ada"}, []))  # type: ignore[arg-type]

    def test_replace_one_sort_is_rejected_by_older_pymongo_profile(self):
        collection = AsyncCollection(
            MemoryEngine(),
            "db",
            "coll",
            pymongo_profile="4.9",
        )

        with self.assertRaises(TypeError):
            asyncio.run(
                collection.replace_one(
                    {"name": "Ada"},
                    {"name": "Grace"},
                    sort=[("rank", 1)],
                )
            )

    def test_replace_one_returns_zero_when_nothing_matches_and_upsert_is_false(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.replace_one({"name": "Ada"}, {"name": "Grace"}))

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_replace_one_upsert_builds_seeded_document(self):
        class EngineStub:
            def __init__(self):
                self.document = None

            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

            async def put_document(self, _db, _coll, document, **kwargs):
                self.document = document
                return True

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        result = asyncio.run(
            collection.replace_one(
                {"kind": "missing", "tenant": "a"},
                {"done": True},
                upsert=True,
            )
        )

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertTrue(result.upserted_id)
        self.assertEqual(
            engine.document,
            {"_id": result.upserted_id, "kind": "missing", "tenant": "a", "done": True},
        )

    def test_replace_one_upsert_duplicate_key_error_when_engine_rejects_document(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

            async def put_document(self, *args, **kwargs):
                return False

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with self.assertRaises(DuplicateKeyError):
            asyncio.run(collection.replace_one({"kind": "missing"}, {"done": True}, upsert=True))

    def test_replace_one_rejects_changing_id(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada"})
                await collection.replace_one({"_id": "1"}, {"_id": "2", "name": "Grace"})
            finally:
                await engine.disconnect()

        with self.assertRaises(OperationFailure):
            asyncio.run(_exercise())

    def test_find_one_and_update_requires_return_document_enum(self):
        with self.assertRaises(TypeError):
            asyncio.run(
                self.collection.find_one_and_update(
                    {"name": "Ada"},
                    {"$set": {"name": "Grace"}},
                    return_document="after",  # type: ignore[arg-type]
                )
            )

    def test_find_one_and_update_returns_none_when_nothing_matches_without_upsert(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(
            collection.find_one_and_update(
                {"name": "Ada"},
                {"$set": {"name": "Grace"}},
            )
        )

        self.assertIsNone(result)

    def test_find_one_and_update_returns_after_document_for_existing_match(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "done": False})
                return await collection.find_one_and_update(
                    {"_id": "1"},
                    {"$set": {"done": True}},
                    return_document=ReturnDocument.AFTER,
                    projection={"done": 1, "_id": 0},
                )
            finally:
                await engine.disconnect()

        result = asyncio.run(_exercise())

        self.assertEqual(result, {"done": True})

    def test_find_one_and_replace_covers_before_after_and_none_branches(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                none_result = await collection.find_one_and_replace(
                    {"name": "missing"},
                    {"name": "Grace"},
                )
                before_upsert = await collection.find_one_and_replace(
                    {"name": "upserted"},
                    {"done": True},
                    upsert=True,
                )
                after_upsert = await collection.find_one_and_replace(
                    {"name": "after-upsert"},
                    {"done": True},
                    upsert=True,
                    return_document=ReturnDocument.AFTER,
                    projection={"done": 1, "_id": 0},
                )
                await collection.insert_one({"_id": "1", "name": "Ada", "done": False})
                before_existing = await collection.find_one_and_replace(
                    {"_id": "1"},
                    {"name": "Ada", "done": True},
                    return_document=ReturnDocument.BEFORE,
                    projection={"done": 1, "_id": 0},
                )
                return none_result, before_upsert, after_upsert, before_existing
            finally:
                await engine.disconnect()

        none_result, before_upsert, after_upsert, before_existing = asyncio.run(_exercise())

        self.assertIsNone(none_result)
        self.assertIsNone(before_upsert)
        self.assertEqual(after_upsert, {"done": True})
        self.assertEqual(before_existing, {"done": False})

    def test_find_one_and_delete_returns_none_when_nothing_matches(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.find_one_and_delete({"name": "missing"}))

        self.assertIsNone(result)

    def test_distinct_rejects_non_string_key(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.distinct(1))  # type: ignore[arg-type]

    def test_distinct_honors_custom_dialect_equality(self):
        class CaseInsensitiveDialect(MongoDialect70):
            def values_equal(self, left, right) -> bool:
                if isinstance(left, str) and isinstance(right, str):
                    return left.lower() == right.lower()
                return super().values_equal(left, right)

        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(
                    engine,
                    "db",
                    "coll",
                    mongodb_dialect=CaseInsensitiveDialect(),
                )
                await collection.insert_one({"_id": "1", "tag": "Ada"})
                await collection.insert_one({"_id": "2", "tag": "ada"})
                return await collection.distinct("tag")
            finally:
                await engine.disconnect()

        result = asyncio.run(_exercise())

        self.assertEqual(result, ["Ada"])

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
