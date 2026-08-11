import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest

from mongoeco import AsyncMongoClient, MongoClient, ReturnDocument
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


class _SelectedMutationBarrierMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._selected_mutation_barrier = threading.Barrier(2)

    async def _wait_for_competitor(self) -> None:
        await asyncio.to_thread(self._selected_mutation_barrier.wait, 5)

    async def update_with_operation(
        self, *args, selector_filter=None, **kwargs
    ):
        if selector_filter is not None:
            await self._wait_for_competitor()
        return await super().update_with_operation(
            *args,
            selector_filter=selector_filter,
            **kwargs,
        )

    async def delete_with_operation(
        self, *args, selector_filter=None, **kwargs
    ):
        if selector_filter is not None:
            await self._wait_for_competitor()
        return await super().delete_with_operation(
            *args,
            selector_filter=selector_filter,
            **kwargs,
        )


class _BarrierMemoryEngine(_SelectedMutationBarrierMixin, MemoryEngine):
    pass


class _BarrierSQLiteEngine(_SelectedMutationBarrierMixin, SQLiteEngine):
    pass


_BARRIER_ENGINE_FACTORIES = {
    'memory': _BarrierMemoryEngine,
    'sqlite': _BarrierSQLiteEngine,
}


def _cas_document() -> dict[str, object]:
    return {
        '_id': 'mutation',
        'revision': 1,
        'fence': 0,
        'payload': [],
    }


def _cas_update(executor: str) -> dict[str, object]:
    return {
        '$inc': {'revision': 1, 'fence': 1},
        '$push': {'payload': executor},
    }


class AtomicSelectedMutationAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_find_one_and_update_rechecks_selector_atomically(self):
        for engine_name, engine_factory in _BARRIER_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.atomic_find_one_and_update
                    await collection.insert_one(_cas_document())
                    stream = collection.watch(
                        max_await_time_ms=5,
                        full_document='updateLookup',
                    )

                    async def acquire(executor: str):
                        return await collection.find_one_and_update(
                            {'_id': 'mutation', 'revision': 1},
                            _cas_update(executor),
                            return_document=ReturnDocument.AFTER,
                        )

                    results = await asyncio.gather(
                        acquire('first'),
                        acquire('second'),
                    )

                    winners = [
                        result for result in results if result is not None
                    ]
                    self.assertEqual(len(winners), 1)
                    stored = await collection.find_one({'_id': 'mutation'})
                    assert stored is not None
                    self.assertEqual(stored['revision'], 2)
                    self.assertEqual(stored['fence'], 1)
                    self.assertEqual(len(stored['payload']), 1)
                    self.assertEqual(winners[0], stored)

                    event = await stream.try_next()
                    self.assertIsNotNone(event)
                    self.assertEqual(event['operationType'], 'update')
                    self.assertEqual(event['fullDocument'], stored)
                    self.assertIsNone(await stream.try_next())

    async def test_update_one_sort_and_hint_recheck_selector_atomically(self):
        for selector_mode in ('sort', 'hint'):
            for (
                engine_name,
                engine_factory,
            ) in _BARRIER_ENGINE_FACTORIES.items():
                with self.subTest(engine=engine_name, selector=selector_mode):
                    async with AsyncMongoClient(
                        engine_factory(), pymongo_profile='4.11'
                    ) as client:
                        collection = client.test.get_collection(
                            f'atomic_update_one_{selector_mode}'
                        )
                        await collection.insert_one(_cas_document())
                        options: dict[str, object]
                        if selector_mode == 'sort':
                            options = {'sort': [('revision', 1)]}
                        else:
                            await collection.create_index([('revision', 1)])
                            options = {'hint': 'revision_1'}

                        async def acquire(executor: str):
                            return await collection.update_one(
                                {'_id': 'mutation', 'revision': 1},
                                _cas_update(executor),
                                **options,
                            )

                        results = await asyncio.gather(
                            acquire('first'),
                            acquire('second'),
                        )

                        self.assertEqual(
                            sorted(result.matched_count for result in results),
                            [0, 1],
                        )
                        stored = await collection.find_one({'_id': 'mutation'})
                        assert stored is not None
                        self.assertEqual(stored['revision'], 2)
                        self.assertEqual(stored['fence'], 1)
                        self.assertEqual(len(stored['payload']), 1)

    async def test_find_one_and_replace_rechecks_selector_atomically(self):
        for engine_name, engine_factory in _BARRIER_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.atomic_find_one_and_replace
                    await collection.insert_one(_cas_document())

                    async def acquire(executor: str):
                        return await collection.find_one_and_replace(
                            {'_id': 'mutation', 'revision': 1},
                            {
                                '_id': 'mutation',
                                'revision': 2,
                                'fence': 1,
                                'payload': [executor],
                            },
                            return_document=ReturnDocument.AFTER,
                        )

                    results = await asyncio.gather(
                        acquire('first'),
                        acquire('second'),
                    )

                    winners = [
                        result for result in results if result is not None
                    ]
                    self.assertEqual(len(winners), 1)
                    stored = await collection.find_one({'_id': 'mutation'})
                    self.assertEqual(winners[0], stored)

    async def test_find_one_and_delete_rechecks_selector_atomically(self):
        for engine_name, engine_factory in _BARRIER_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.atomic_find_one_and_delete
                    await collection.insert_one(_cas_document())

                    async def acquire():
                        return await collection.find_one_and_delete(
                            {'_id': 'mutation', 'revision': 1}
                        )

                    results = await asyncio.gather(acquire(), acquire())

                    self.assertEqual(
                        sum(result is not None for result in results),
                        1,
                    )
                    self.assertIsNone(
                        await collection.find_one({'_id': 'mutation'})
                    )


class AtomicSelectedMutationSyncTests(unittest.TestCase):
    def test_find_one_and_update_has_sync_engine_parity(self):
        for engine_name, engine_factory in {
            'memory': MemoryEngine,
            'sqlite': SQLiteEngine,
        }.items():
            with self.subTest(engine=engine_name):
                with MongoClient(engine_factory()) as client:
                    collection = client.test.atomic_find_one_and_update_sync
                    collection.insert_one(_cas_document())
                    stream = collection.watch(
                        max_await_time_ms=5,
                        full_document='updateLookup',
                    )

                    def acquire(executor: str):
                        return collection.find_one_and_update(
                            {'_id': 'mutation', 'revision': 1},
                            _cas_update(executor),
                            return_document=ReturnDocument.AFTER,
                        )

                    with ThreadPoolExecutor(max_workers=2) as executor:
                        results = list(
                            executor.map(acquire, ('first', 'second'))
                        )

                    winners = [
                        result for result in results if result is not None
                    ]
                    self.assertEqual(len(winners), 1)
                    stored = collection.find_one({'_id': 'mutation'})
                    assert stored is not None
                    self.assertEqual(stored['revision'], 2)
                    self.assertEqual(stored['fence'], 1)
                    self.assertEqual(len(stored['payload']), 1)
                    self.assertEqual(winners[0], stored)

                    event = stream.try_next()
                    self.assertIsNotNone(event)
                    self.assertEqual(event['operationType'], 'update')
                    self.assertEqual(event['fullDocument'], stored)
                    self.assertIsNone(stream.try_next())


if __name__ == '__main__':
    unittest.main()
