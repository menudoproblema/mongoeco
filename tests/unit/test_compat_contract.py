import asyncio
import unittest
from types import MappingProxyType

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.compat import MongoBehaviorPolicySpec, MongoDialect70, PyMongoProfile49
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import compile_filter
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import ReplaceOne, UNDEFINED, UpdateOne


class DialectAndProfileContractTests(unittest.TestCase):
    def test_query_planner_respects_declarative_operator_catalog(self):
        class _NoOrDialect(MongoDialect70):
            @property
            def query_top_level_operators(self) -> frozenset[str]:
                return frozenset({'$and', '$nor', '$expr'})

        compile_filter({'$or': [{'name': 'Ada'}]}, dialect=MongoDialect70())

        with self.assertRaises(OperationFailure):
            compile_filter({'$or': [{'name': 'Ada'}]}, dialect=_NoOrDialect())

    def test_query_engine_respects_null_query_matches_undefined_hook(self):
        class _ToggleNullDialect(MongoDialect70):
            __slots__ = ('_enabled',)

            def __init__(self, enabled: bool):
                super().__init__()
                object.__setattr__(self, '_enabled', enabled)

            def null_query_matches_undefined(self) -> bool:
                return self._enabled

        document = {'value': UNDEFINED}
        missing_document = {}

        true_dialect = _ToggleNullDialect(True)
        false_dialect = _ToggleNullDialect(False)

        self.assertTrue(QueryEngine.match(document, {'value': None}, dialect=true_dialect))
        self.assertFalse(QueryEngine.match(document, {'value': None}, dialect=false_dialect))
        self.assertFalse(QueryEngine.match(missing_document, {'value': {'$ne': None}}, dialect=true_dialect))
        self.assertTrue(QueryEngine.match(missing_document, {'value': {'$ne': None}}, dialect=false_dialect))
        self.assertFalse(QueryEngine.match(missing_document, {'value': {'$nin': [None]}}, dialect=true_dialect))
        self.assertTrue(QueryEngine.match(missing_document, {'value': {'$nin': [None]}}, dialect=false_dialect))

    def test_query_engine_respects_declarative_policy_spec_override(self):
        class _CatalogLikeDialect(MongoDialect70):
            @property
            def policy_spec(self) -> MongoBehaviorPolicySpec:
                return MongoBehaviorPolicySpec(null_query_matches_undefined=False)

        document = {'value': UNDEFINED}

        self.assertFalse(QueryEngine.match(document, {'value': None}, dialect=_CatalogLikeDialect()))

    def test_query_engine_respects_bson_type_order_hook(self):
        class _SwappedStringOrderDialect(MongoDialect70):
            @property
            def bson_type_order(self) -> MappingProxyType:
                overridden = dict(super().bson_type_order)
                overridden[str] = 2
                overridden[int] = 3
                overridden[float] = 3
                return MappingProxyType(overridden)

        document = {'value': 'Ada'}

        self.assertTrue(QueryEngine.match(document, {'value': {'$gt': 1}}, dialect=MongoDialect70()))
        self.assertFalse(
            QueryEngine.match(document, {'value': {'$gt': 1}}, dialect=_SwappedStringOrderDialect())
        )

    def test_async_collection_respects_supports_update_one_sort_profile_hook(self):
        class _ToggleSortProfile(PyMongoProfile49):
            __slots__ = ('_enabled',)

            def __init__(self, enabled: bool):
                super().__init__()
                object.__setattr__(self, '_enabled', enabled)

            def supports_update_one_sort(self) -> bool:
                return self._enabled

        async def _exercise(enabled: bool):
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(
                    engine,
                    'db',
                    'coll',
                    pymongo_profile=_ToggleSortProfile(enabled),
                )
                await collection.insert_one({'_id': '1', 'kind': 'view', 'rank': 2, 'done': False})
                await collection.insert_one({'_id': '2', 'kind': 'view', 'rank': 1, 'done': False})
                return await collection.update_one(
                    {'kind': 'view'},
                    {'$set': {'done': True}},
                    sort=[('rank', 1)],
                )
            finally:
                await engine.disconnect()

        with self.assertRaises(TypeError):
            asyncio.run(_exercise(False))

        result = asyncio.run(_exercise(True))
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)

    def test_async_collection_respects_supports_update_one_sort_profile_hook_in_bulk_write(self):
        class _ToggleSortProfile(PyMongoProfile49):
            __slots__ = ("_enabled",)

            def __init__(self, enabled: bool):
                super().__init__()
                object.__setattr__(self, "_enabled", enabled)

            def supports_update_one_sort(self) -> bool:
                return self._enabled

        async def _exercise(enabled: bool):
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(
                    engine,
                    "db",
                    "coll",
                    pymongo_profile=_ToggleSortProfile(enabled),
                )
                return await collection.bulk_write(
                    [
                        UpdateOne({"kind": "view"}, {"$set": {"done": True}}, sort=[("rank", 1)]),
                        ReplaceOne({"kind": "view"}, {"done": True}, sort=[("rank", 1)]),
                    ]
                )
            finally:
                await engine.disconnect()

        with self.assertRaises(TypeError):
            asyncio.run(_exercise(False))

        result = asyncio.run(_exercise(True))
        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
