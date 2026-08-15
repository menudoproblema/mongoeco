import asyncio
import copy
from datetime import UTC, datetime, timedelta, timezone
import unittest

from mongoeco import AsyncMongoClient, CodecOptions, InsertOne, UpdateOne
from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, OperationFailure


ENGINE_FACTORIES = {'memory': MemoryEngine, 'sqlite': SQLiteEngine}
_MULTI_DOCUMENT_COUNT = 3


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
    async def test_direct_engine_binding_recompiles_context_sensitive_plans(
        self,
    ):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                engine = engine_factory()
                await engine.connect()
                try:
                    await engine.insert_document(
                        'db',
                        'items',
                        {
                            '_id': 1,
                            'items': [{'name': 'A', 'hit': False}],
                        },
                        overwrite=False,
                        operation_context=OperationContext.create(
                            dialect=MONGODB_DIALECT_70,
                        ),
                    )
                    context = OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        collation={'locale': 'en', 'strength': 2},
                    )
                    operation = compile_update_operation(
                        {'_id': 1},
                        update_spec={
                            '$set': {'items.$[item].hit': True},
                        },
                        array_filters=[{'item.name': 'a'}],
                    )

                    outcome = await engine.update_with_operation(
                        'db',
                        'items',
                        operation,
                        operation_context=context,
                    )

                    assert outcome.modified_count == 1
                    assert await engine.get_document(
                        'db',
                        'items',
                        1,
                    ) == {
                        '_id': 1,
                        'items': [{'name': 'A', 'hit': True}],
                    }
                finally:
                    await engine.disconnect()
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

    async def test_admin_commands_share_the_collection_bson_boundary(self):
        source = datetime(
            2026,
            1,
            2,
            10,
            4,
            5,
            987_654,
            tzinfo=timezone(timedelta(hours=2)),
        )
        expected = source.astimezone(UTC).replace(
            microsecond=987_000,
            tzinfo=None,
        )
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.command_bson_boundary
                    await collection.insert_many(
                        [
                            {'_id': 'find', 'at': source},
                            {'_id': 'update', 'at': source},
                            {'_id': 'find-and-modify', 'at': source},
                        ],
                    )
                    bindings = {'expected': source}
                    snapshot = copy.deepcopy(bindings)

                    found = await client.test.command(
                        {
                            'find': 'command_bson_boundary',
                            'filter': {'_id': 'find', 'at': source},
                        },
                    )
                    aggregated = await client.test.command(
                        {
                            'aggregate': 'command_bson_boundary',
                            'pipeline': [
                                {
                                    '$match': {
                                        '_id': 'find',
                                        '$expr': {
                                            '$eq': ['$at', '$$expected'],
                                        },
                                    },
                                },
                                {
                                    '$project': {
                                        '_id': 0,
                                        'literal': {'$literal': source},
                                    },
                                },
                            ],
                            'let': bindings,
                            'cursor': {},
                        },
                    )
                    updated = await client.test.command(
                        {
                            'update': 'command_bson_boundary',
                            'let': bindings,
                            'updates': [
                                {
                                    'q': {
                                        '_id': 'update',
                                        '$expr': {
                                            '$eq': ['$at', '$$expected'],
                                        },
                                    },
                                    'u': {'$set': {'matched': True}},
                                },
                            ],
                        },
                    )
                    modified = await client.test.command(
                        {
                            'findAndModify': 'command_bson_boundary',
                            'query': {
                                '_id': 'find-and-modify',
                                '$expr': {'$eq': ['$at', '$$expected']},
                            },
                            'update': {'$set': {'matched': True}},
                            'let': bindings,
                            'new': True,
                        },
                    )

                    assert len(found['cursor']['firstBatch']) == 1
                    assert aggregated['cursor']['firstBatch'] == [
                        {'literal': expected},
                    ]
                    assert updated['nModified'] == 1
                    assert modified['value']['matched']
                    assert bindings == snapshot

    async def test_multi_document_writes_publish_one_event_per_document(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.multi_event_identity
                    await collection.insert_many(
                        [
                            {'_id': index, 'group': 'selected'}
                            for index in range(_MULTI_DOCUMENT_COUNT)
                        ],
                    )
                    stream = collection.watch(max_await_time_ms=5)

                    updated = await collection.update_many(
                        {'group': 'selected'},
                        {'$set': {'updated': True}},
                    )
                    update_events = [
                        await stream.try_next()
                        for _index in range(_MULTI_DOCUMENT_COUNT)
                    ]
                    deleted = await collection.delete_many(
                        {'group': 'selected'},
                    )
                    delete_events = [
                        await stream.try_next()
                        for _index in range(_MULTI_DOCUMENT_COUNT)
                    ]

                    assert updated.modified_count == _MULTI_DOCUMENT_COUNT
                    assert deleted.deleted_count == _MULTI_DOCUMENT_COUNT
                    assert [
                        event['operationType'] for event in update_events
                    ] == ['update', 'update', 'update']
                    assert [
                        event['operationType'] for event in delete_events
                    ] == ['delete', 'delete', 'delete']

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
