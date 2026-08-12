import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from mongoeco import AsyncMongoClient, ReturnDocument
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure


class _MutationGateMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.before_entered = asyncio.Event()
        self.before_release = asyncio.Event()
        self.after_entered = asyncio.Event()
        self.after_release = asyncio.Event()

    async def update_with_operation(self, *args, **kwargs):
        operation = args[2]
        if operation.comment == 'gate-before':
            self.before_entered.set()
            await self.before_release.wait()
        result = await super().update_with_operation(*args, **kwargs)
        if operation.comment == 'gate-after':
            self.after_entered.set()
            await self.after_release.wait()
        return result


class _GatedMemoryEngine(_MutationGateMixin, MemoryEngine):
    pass


class _GatedSQLiteEngine(_MutationGateMixin, SQLiteEngine):
    pass


class SelectedMutationSnapshotContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_find_and_modify_upserts_publish_insert_events_for_both_return_modes(
        self,
    ):
        for engine_name, engine_factory in {
            'memory': MemoryEngine,
            'sqlite': SQLiteEngine,
        }.items():
            for operation_name in ('update', 'replace'):
                for return_document in (
                    ReturnDocument.BEFORE,
                    ReturnDocument.AFTER,
                ):
                    with self.subTest(
                        engine=engine_name,
                        operation=operation_name,
                        return_document=return_document,
                    ):
                        async with AsyncMongoClient(
                            engine_factory()
                        ) as client:
                            collection = client.test.get_collection(
                                f'upsert_event_{operation_name}_{return_document.name}'
                            )
                            stream = collection.watch(max_await_time_ms=5)

                            if operation_name == 'update':
                                returned = (
                                    await collection.find_one_and_update(
                                        {'_id': 'item'},
                                        {'$set': {'value': 1}},
                                        upsert=True,
                                        return_document=return_document,
                                    )
                                )
                            else:
                                returned = (
                                    await collection.find_one_and_replace(
                                        {'_id': 'item'},
                                        {'_id': 'item', 'value': 1},
                                        upsert=True,
                                        return_document=return_document,
                                    )
                                )

                            event = await stream.try_next()
                            if return_document is ReturnDocument.BEFORE:
                                self.assertIsNone(returned)
                            else:
                                self.assertEqual(returned['value'], 1)
                            self.assertIsNotNone(event)
                            self.assertEqual(event['operationType'], 'insert')
                            self.assertEqual(
                                event['documentKey'], {'_id': 'item'}
                            )
                            self.assertEqual(event['fullDocument']['value'], 1)

    async def test_noop_updates_and_replacements_do_not_publish_change_events(
        self,
    ):
        for engine_name, engine_factory in {
            'memory': MemoryEngine,
            'sqlite': SQLiteEngine,
        }.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.noop_change_events
                    await collection.insert_one({'_id': 'item', 'value': 1})
                    stream = collection.watch(max_await_time_ms=5)

                    await collection.update_one(
                        {'_id': 'item'}, {'$set': {'value': 1}}
                    )
                    await collection.update_many(
                        {'_id': 'item'}, {'$set': {'value': 1}}
                    )
                    await collection.replace_one(
                        {'_id': 'item'}, {'_id': 'item', 'value': 1}
                    )
                    await collection.find_one_and_update(
                        {'_id': 'item'},
                        {'$set': {'value': 1}},
                        return_document=ReturnDocument.AFTER,
                    )
                    await collection.find_one_and_replace(
                        {'_id': 'item'},
                        {'_id': 'item', 'value': 1},
                        return_document=ReturnDocument.AFTER,
                    )

                    self.assertIsNone(await stream.try_next())

    async def test_single_mutations_validate_hints_inside_engine_boundary(
        self,
    ):
        for engine_name, engine_factory in {
            'memory': MemoryEngine,
            'sqlite': SQLiteEngine,
        }.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.atomic_hint_validation

                    with self.assertRaises(OperationFailure):
                        await collection.update_one(
                            {'_id': 'missing'},
                            {'$set': {'value': 1}},
                            hint='missing_index',
                        )
                    with self.assertRaises(OperationFailure):
                        await collection.replace_one(
                            {'_id': 'missing'},
                            {'value': 1},
                            hint='missing_index',
                        )
                    with self.assertRaises(OperationFailure):
                        await collection.delete_one(
                            {'_id': 'missing'},
                            hint='missing_index',
                        )

    async def test_before_and_after_images_come_from_the_atomic_mutation(self):
        for engine_name, engine_factory in {
            'memory': _GatedMemoryEngine,
            'sqlite': _GatedSQLiteEngine,
        }.items():
            with self.subTest(engine=engine_name):
                engine = engine_factory()
                async with AsyncMongoClient(engine) as client:
                    collection = client.test.atomic_images
                    await collection.insert_one(
                        {'_id': 'item', 'revision': 1, 'note': 'original'}
                    )

                    before_task = asyncio.create_task(
                        collection.find_one_and_update(
                            {'_id': 'item', 'revision': 1},
                            {'$inc': {'revision': 1}},
                            return_document=ReturnDocument.BEFORE,
                            comment='gate-before',
                        )
                    )
                    await engine.before_entered.wait()
                    await collection.update_one(
                        {'_id': 'item'}, {'$set': {'note': 'concurrent'}}
                    )
                    engine.before_release.set()
                    before = await before_task
                    self.assertEqual(before['note'], 'concurrent')

                    after_task = asyncio.create_task(
                        collection.find_one_and_update(
                            {'_id': 'item', 'revision': 2},
                            {'$inc': {'revision': 1}},
                            return_document=ReturnDocument.AFTER,
                            comment='gate-after',
                        )
                    )
                    await engine.after_entered.wait()
                    await collection.update_one(
                        {'_id': 'item'}, {'$set': {'note': 'later'}}
                    )
                    engine.after_release.set()
                    after = await after_task
                    self.assertEqual(after['revision'], 3)
                    self.assertEqual(after['note'], 'concurrent')

    async def test_sort_is_re_evaluated_inside_the_mutation_boundary(self):
        for engine_name, engine_factory in {
            'memory': _GatedMemoryEngine,
            'sqlite': _GatedSQLiteEngine,
        }.items():
            with self.subTest(engine=engine_name):
                engine = engine_factory()
                async with AsyncMongoClient(
                    engine, pymongo_profile='4.11'
                ) as client:
                    collection = client.test.atomic_sort
                    await collection.insert_many(
                        [
                            {'_id': 'first', 'score': 1},
                            {'_id': 'second', 'score': 2},
                        ]
                    )
                    task = asyncio.create_task(
                        collection.update_one(
                            {},
                            {'$set': {'selected': True}},
                            sort=[('score', 1)],
                            comment='gate-before',
                        )
                    )
                    await engine.before_entered.wait()
                    await collection.update_one(
                        {'_id': 'first'}, {'$set': {'score': 3}}
                    )
                    engine.before_release.set()
                    result = await task

                    self.assertEqual(result.matched_count, 1)
                    selected = await collection.find_one({'selected': True})
                    self.assertEqual(selected['_id'], 'second')

    async def test_sqlite_cas_is_atomic_across_engine_instances(self):
        with TemporaryDirectory() as directory:
            path = str(Path(directory) / 'shared.sqlite')
            first_client = AsyncMongoClient(SQLiteEngine(path))
            second_client = AsyncMongoClient(SQLiteEngine(path))
            async with first_client, second_client:
                first = first_client.test.shared_cas
                second = second_client.test.shared_cas
                await first.insert_one({'_id': 'item', 'revision': 1})
                start = asyncio.Event()

                async def acquire(collection, owner):
                    await start.wait()
                    return await collection.find_one_and_update(
                        {'_id': 'item', 'revision': 1},
                        {'$inc': {'revision': 1}, '$set': {'owner': owner}},
                        return_document=ReturnDocument.AFTER,
                    )

                tasks = [
                    asyncio.create_task(acquire(first, 'first')),
                    asyncio.create_task(acquire(second, 'second')),
                ]
                start.set()
                results = await asyncio.gather(*tasks)

                self.assertEqual(
                    sum(result is not None for result in results), 1
                )
                stored = await first.find_one({'_id': 'item'})
                self.assertEqual(stored['revision'], 2)


if __name__ == '__main__':
    unittest.main()
