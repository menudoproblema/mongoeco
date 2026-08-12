import asyncio
import copy
from datetime import UTC, datetime, timedelta, timezone
import unittest

from mongoeco import AsyncMongoClient, CodecOptions, InsertOne, UpdateOne
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, OperationFailure


ENGINE_FACTORIES = {'memory': MemoryEngine, 'sqlite': SQLiteEngine}


class _DelayedReturnMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.committed = asyncio.Event()
        self.release = asyncio.Event()

    async def update_with_operation(self, *args, **kwargs):
        result = await super().update_with_operation(*args, **kwargs)
        if args[2].comment == 'delay-after-commit':
            self.committed.set()
            await self.release.wait()
        return result


class _DelayedReturnMemoryEngine(_DelayedReturnMixin, MemoryEngine):
    pass


class _DelayedReturnSQLiteEngine(_DelayedReturnMixin, SQLiteEngine):
    pass


class BoundaryConsistencyContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_change_events_follow_commit_order_not_coroutine_return_order(
        self,
    ):
        for engine_factory in (
            _DelayedReturnMemoryEngine,
            _DelayedReturnSQLiteEngine,
        ):
            with self.subTest(engine=engine_factory.__name__):
                engine = engine_factory()
                async with AsyncMongoClient(engine) as client:
                    collection = client.test.commit_order
                    await collection.insert_one({'_id': 'one', 'revision': 0})
                    stream = collection.watch(
                        max_await_time_ms=5,
                        full_document='updateLookup',
                    )
                    first = asyncio.create_task(
                        collection.update_one(
                            {'_id': 'one'},
                            {'$set': {'revision': 1}},
                            comment='delay-after-commit',
                        )
                    )
                    await asyncio.wait_for(engine.committed.wait(), 1)
                    await collection.update_one(
                        {'_id': 'one'}, {'$set': {'revision': 2}}
                    )
                    engine.release.set()
                    await first

                    events = [await stream.try_next(), await stream.try_next()]
                    self.assertEqual(
                        [
                            event['fullDocument']['revision']
                            for event in events
                        ],
                        [1, 2],
                    )

    async def test_change_stream_failure_degrades_stream_without_reclassifying_write(
        self,
    ):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.change_event_failure
                    stream = collection.watch(max_await_time_ms=5)

                    def fail_publish(**_payload):
                        raise OSError('journal unavailable')

                    client._change_hub.publish = fail_publish
                    result = await collection.insert_one({'_id': 'committed'})

                    self.assertEqual(result.inserted_id, 'committed')
                    self.assertEqual(
                        await collection.find_one({'_id': 'committed'}),
                        {'_id': 'committed'},
                    )
                    state = collection.change_stream_state()
                    self.assertTrue(state['degraded'])
                    self.assertEqual(
                        state['lastPublishError'], 'journal unavailable'
                    )
                    with self.assertRaisesRegex(
                        OperationFailure,
                        'publication failure',
                    ):
                        await stream.try_next()

    async def test_change_events_preserve_mutation_order_across_profiling_awaits(
        self,
    ):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.change_event_order
                    await collection.insert_one({'_id': 'one', 'revision': 0})
                    stream = collection.watch(
                        max_await_time_ms=5,
                        full_document='updateLookup',
                    )
                    original_profile = collection._profile_operation
                    first_profile_started = asyncio.Event()
                    release_first_profile = asyncio.Event()
                    update_profiles = 0

                    async def blocking_profile(**kwargs):
                        nonlocal update_profiles
                        if kwargs.get('op') == 'update':
                            update_profiles += 1
                            if update_profiles == 1:
                                first_profile_started.set()
                                await release_first_profile.wait()
                        return await original_profile(**kwargs)

                    collection._profile_operation = blocking_profile
                    first = asyncio.create_task(
                        collection.update_one(
                            {'_id': 'one'}, {'$set': {'revision': 1}}
                        )
                    )
                    await asyncio.wait_for(first_profile_started.wait(), 1)
                    second = await collection.update_one(
                        {'_id': 'one'}, {'$set': {'revision': 2}}
                    )
                    release_first_profile.set()
                    await first

                    self.assertEqual(second.modified_count, 1)
                    events = [await stream.try_next(), await stream.try_next()]
                    self.assertEqual(
                        [
                            event['fullDocument']['revision']
                            for event in events
                        ],
                        [1, 2],
                    )

    async def test_write_let_uses_the_same_bson_boundary_as_persisted_documents(
        self,
    ):
        source = datetime(
            2024,
            9,
            20,
            16,
            43,
            45,
            123_456,
            tzinfo=UTC,
        )
        equivalent = source.astimezone(timezone(timedelta(hours=2)))
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.write_let_boundary
                    await collection.insert_many(
                        [
                            {'_id': 'direct', 'at': source},
                            {'_id': 'bulk', 'at': source},
                        ]
                    )
                    bindings = {'expected': equivalent}
                    snapshot = copy.deepcopy(bindings)
                    direct = await collection.update_one(
                        {
                            '_id': 'direct',
                            '$expr': {'$eq': ['$at', '$$expected']},
                        },
                        {'$set': {'matched': True}},
                        let=bindings,
                    )
                    bulk = await collection.bulk_write(
                        [
                            UpdateOne(
                                {
                                    '_id': 'bulk',
                                    '$expr': {'$eq': ['$at', '$$expected']},
                                },
                                {'$set': {'matched': True}},
                                let=bindings,
                            )
                        ]
                    )

                    self.assertEqual(direct.modified_count, 1)
                    self.assertEqual(bulk.modified_count, 1)
                    self.assertEqual(bindings, snapshot)

    async def test_bulk_insert_aliases_are_prepared_deterministically(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.bulk_aliases
                    shared = {'value': 1}
                    with self.assertRaises(BulkWriteError):
                        await collection.bulk_write(
                            [InsertOne(shared), InsertOne(shared)],
                            ordered=False,
                        )
                    self.assertIn('_id', shared)
                    self.assertEqual(await collection.count_documents({}), 1)

    async def test_codec_options_apply_to_change_streams_and_index_information(
        self,
    ):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.get_collection(
                        'codec_surfaces',
                        codec_options=CodecOptions(
                            dict, tz_aware=True, tzinfo=UTC
                        ),
                    )
                    value = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
                    stream = collection.watch(max_await_time_ms=5)
                    await collection.insert_one({'_id': 'one', 'at': value})
                    await collection.create_index(
                        'at',
                        name='at_partial',
                        partial_filter_expression={'at': {'$gte': value}},
                    )

                    event = await stream.try_next()
                    information = await collection.index_information()
                    event_value = event['fullDocument']['at']
                    index_value = information['at_partial'][
                        'partialFilterExpression'
                    ]['at']['$gte']
                    self.assertEqual(event_value.tzinfo, UTC)
                    self.assertEqual(index_value.tzinfo, UTC)

    async def test_create_ttl_index_uses_its_own_execution_time(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                clock = [datetime(2026, 1, 1, tzinfo=UTC)]
                async with AsyncMongoClient(
                    engine_factory(), now_factory=lambda: clock[0]
                ) as client:
                    collection = client.test.ttl_index_now
                    await collection.insert_one(
                        {'_id': 'expired', 'expires': clock[0]}
                    )
                    clock[0] += timedelta(seconds=10)
                    await collection.create_index(
                        'expires', expire_after_seconds=5
                    )
                    clock[0] -= timedelta(seconds=10)

                    self.assertIsNone(
                        await collection.find_one({'_id': 'expired'})
                    )


if __name__ == '__main__':
    unittest.main()
