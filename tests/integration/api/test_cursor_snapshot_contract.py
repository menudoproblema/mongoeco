import inspect
import unittest

from datetime import UTC, datetime, timedelta

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


ENGINE_FACTORIES = {'memory': MemoryEngine, 'sqlite': SQLiteEngine}


def _read_clock(clock: list[datetime]):
    return lambda: clock[0]


class SyncCursorOperationBoundaryTests(unittest.TestCase):
    def test_find_captures_time_and_detaches_arguments_at_creation(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                clock = [datetime(2026, 1, 1, tzinfo=UTC)]
                filter_spec = {'$expr': {'$lte': ['$ready', '$$NOW']}}
                projection = {'_id': 1}
                with MongoClient(
                    engine_factory(),
                    now_factory=_read_clock(clock),
                ) as client:
                    collection = client.test.sync_creation_boundary
                    collection.insert_many(
                        [
                            {'_id': 'early', 'ready': clock[0]},
                            {
                                '_id': 'late',
                                'ready': clock[0] + timedelta(seconds=1),
                            },
                        ],
                    )
                    cursor = collection.find(
                        filter_spec,
                        projection,
                        sort=[('_id', 1)],
                    )
                    filter_spec.clear()
                    projection.clear()
                    clock[0] += timedelta(seconds=1)

                    assert cursor.to_list() == [{'_id': 'early'}]


class CursorSnapshotContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_find_and_aggregate_batches_keep_one_stable_view(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            for surface in ('find', 'aggregate'):
                with self.subTest(engine=engine_name, surface=surface):
                    async with AsyncMongoClient(engine_factory()) as client:
                        collection = client.test.get_collection(
                            f'stable_batches_{surface}'
                        )
                        await collection.insert_many(
                            [
                                {'_id': 'one', 'rank': 1},
                                {'_id': 'two', 'rank': 2},
                                {'_id': 'three', 'rank': 3},
                            ]
                        )
                        cursor = (
                            collection.find(
                                {},
                                {'_id': 1},
                                sort=[('rank', 1)],
                                batch_size=1,
                            )
                            if surface == 'find'
                            else collection.aggregate(
                                [
                                    {'$sort': {'rank': 1}},
                                    {'$project': {'_id': 1}},
                                ],
                                batch_size=1,
                            )
                        )
                        iterator = cursor.__aiter__()
                        first = await iterator.__anext__()
                        await collection.insert_one({'_id': 'zero', 'rank': 0})
                        second = await iterator.__anext__()

                        self.assertEqual(first['_id'], 'one')
                        self.assertEqual(second['_id'], 'two')

    async def test_clone_captures_a_new_execution_time(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                clock = [datetime(2026, 1, 1, tzinfo=UTC)]
                async with AsyncMongoClient(
                    engine_factory(), now_factory=lambda: clock[0]
                ) as client:
                    collection = client.test.clone_now
                    await collection.insert_many(
                        [
                            {'_id': 'early', 'ready': clock[0]},
                            {
                                '_id': 'late',
                                'ready': clock[0] + timedelta(seconds=1),
                            },
                        ]
                    )
                    original = collection.find(
                        {'$expr': {'$lte': ['$ready', '$$NOW']}},
                        {'_id': 1},
                        sort=[('_id', 1)],
                    )
                    clock[0] += timedelta(seconds=1)
                    cloned = original.clone()

                    self.assertEqual(
                        [
                            document['_id']
                            for document in await original.to_list()
                        ],
                        ['early'],
                    )
                    self.assertEqual(
                        [
                            document['_id']
                            for document in await cloned.to_list()
                        ],
                        ['early', 'late'],
                    )

    async def test_async_cursor_factories_are_awaitable_without_breaking_direct_use(
        self,
    ):
        async with AsyncMongoClient(MemoryEngine()) as client:
            collection = client.test.awaitable_factories
            await collection.insert_one({'_id': 'one'})
            for cursor in (
                collection.aggregate([]),
                collection.list_indexes(),
                collection.watch(max_await_time_ms=1),
            ):
                with self.subTest(cursor=type(cursor).__name__):
                    self.assertTrue(inspect.isawaitable(cursor))
                    self.assertIs(await cursor, cursor)


if __name__ == '__main__':
    unittest.main()
