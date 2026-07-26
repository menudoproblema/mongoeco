import unittest
from datetime import UTC, datetime, timedelta

from mongoeco import AsyncMongoClient, InsertOne, Timestamp, UpdateOne
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import OperationFailure
from tests.support import ENGINE_FACTORIES, open_client


class NowExecutionContextIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_injected_clock_normalizes_and_survives_client_derivations(self):
        source = datetime(2026, 1, 2, 3, 4, 5, 987_654, tzinfo=UTC)
        clock = lambda: source
        async with AsyncMongoClient(MemoryEngine(), now_factory=clock) as client:
            database = client.get_database('clock')
            collection = database.get_collection('values').with_options()
            self.assertIs(client.now_factory, clock)
            self.assertIs(database.now_factory, clock)
            self.assertIs(collection.now_factory, clock)
            await collection.insert_one({'_id': 'value'})
            document = (await collection.aggregate([{'$project': {'now': '$$NOW'}}]).to_list())[0]
            self.assertEqual(document['now'], datetime(2026, 1, 2, 3, 4, 5, 987_000))
            self.assertIsNone(document['now'].tzinfo)

    async def test_injected_clock_drives_current_date_cache_and_ttl_in_all_local_engines(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                clock = [datetime(2026, 1, 2, 3, 4, 5, 123_789, tzinfo=UTC)]
                async with open_client(engine_name, now_factory=lambda: clock[0]) as client:
                    collection = client.test.clock
                    await collection.insert_one({'_id': 'current'})
                    update = {'$currentDate': {'date': True, 'first': {'$type': 'timestamp'}, 'second': {'$type': 'timestamp'}}}
                    await collection.update_one({'_id': 'current'}, update)
                    first = await collection.find_one({'_id': 'current'})
                    self.assertEqual(first['date'], datetime(2026, 1, 2, 3, 4, 5, 123_000))
                    self.assertEqual((first['first'].inc, first['second'].inc), (1, 2))
                    self.assertIsInstance(first['first'], Timestamp)

                    # The same compiled update template must not retain NOW.
                    clock[0] += timedelta(seconds=1)
                    await collection.update_one({'_id': 'current'}, update)
                    second = await collection.find_one({'_id': 'current'})
                    self.assertEqual(second['date'], datetime(2026, 1, 2, 3, 4, 6, 123_000))

                    await collection.create_index('expires', expire_after_seconds=10)
                    await collection.insert_one({'_id': 'ttl', 'expires': clock[0]})
                    self.assertIsNotNone(await collection.find_one({'_id': 'ttl'}))
                    clock[0] += timedelta(seconds=11)
                    self.assertIsNone(await collection.find_one({'_id': 'ttl'}))

    async def test_injected_clock_rejects_engines_without_the_capability(self):
        class ExternalEngine(MemoryEngine):
            supports_injected_clock = False

        with self.assertRaisesRegex(ValueError, 'supports injected clocks'):
            AsyncMongoClient(ExternalEngine(), now_factory=lambda: datetime.now(UTC))
        with self.assertRaisesRegex(TypeError, 'now_factory must return a datetime'):
            AsyncMongoClient(MemoryEngine(), now_factory=lambda: object())  # type: ignore[arg-type]

    async def test_bulk_batch_shares_injected_clock_and_next_batch_refreshes_it(self):
        clock = [datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)]
        async with AsyncMongoClient(MemoryEngine(), now_factory=lambda: clock[0]) as client:
            collection = client.test.clock
            await collection.insert_many([{'_id': 'a'}, {'_id': 'b'}])
            models = [
                UpdateOne({'_id': 'a'}, [{'$set': {'now': '$$NOW'}}]),
                UpdateOne({'_id': 'b'}, [{'$set': {'now': '$$NOW'}}]),
            ]
            await collection.bulk_write(models)
            first = await collection.find({}, sort=[('_id', 1)]).to_list()
            self.assertEqual(first[0]['now'], first[1]['now'])
            clock[0] += timedelta(seconds=1)
            await collection.bulk_write(models)
            second = await collection.find({}, sort=[('_id', 1)]).to_list()
            self.assertNotEqual(first[0]['now'], second[0]['now'])

    async def test_factory_is_called_once_per_command_or_logical_bulk_batch(self):
        fixed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        calls: list[datetime] = []

        def factory() -> datetime:
            calls.append(fixed)
            return fixed

        async with AsyncMongoClient(MemoryEngine(), now_factory=factory) as client:
            # Construction validates the return type; it is not an execution.
            calls.clear()
            collection = client.test.clock
            await collection.insert_many([{'_id': 'a'}, {'_id': 'b'}])
            self.assertEqual(len(calls), 1)

            calls.clear()
            await collection.update_many({}, [{'$set': {'now': '$$NOW'}}])
            self.assertEqual(len(calls), 1)

            calls.clear()
            await collection.find_one({'$expr': {'$ne': ['$$NOW', None]}})
            self.assertEqual(len(calls), 1)

            calls.clear()
            await collection.count_documents({'$expr': {'$ne': ['$$NOW', None]}})
            self.assertEqual(len(calls), 1)

            calls.clear()
            await collection.distinct('now', {'$expr': {'$ne': ['$$NOW', None]}})
            self.assertEqual(len(calls), 1)

            calls.clear()
            await collection.aggregate([{'$project': {'now': '$$NOW'}}]).to_list()
            self.assertEqual(len(calls), 1)

            calls.clear()
            await collection.bulk_write(
                [InsertOne({'_id': 'c'}), InsertOne({'_id': 'd'})]
            )
            self.assertEqual(len(calls), 1)

            calls.clear()
            with self.assertRaises(TypeError):
                await collection.bulk_write([UpdateOne([], {'$set': {'bad': True}})])
            self.assertEqual(calls, [])

            calls.clear()
            await client.test.command(
                {
                    'find': 'clock',
                    'filter': {'$expr': {'$ne': ['$$NOW', None]}},
                }
            )
            self.assertEqual(len(calls), 1)

            calls.clear()
            await client.test.command(
                {
                    'aggregate': 'clock',
                    'pipeline': [{'$project': {'command_now': '$$NOW'}}],
                    'cursor': {},
                }
            )
            self.assertEqual(len(calls), 1)

            calls.clear()
            await client.test.command(
                {
                    'update': 'clock',
                    'updates': [
                        {'q': {'_id': 'a'}, 'u': [{'$set': {'command_now': '$$NOW'}}]},
                        {'q': {'_id': 'b'}, 'u': [{'$set': {'command_now': '$$NOW'}}]},
                    ],
                }
            )
            self.assertEqual(len(calls), 1)

    async def test_now_is_stable_per_execution_across_dialects_and_profiles(self):
        for engine_name in ENGINE_FACTORIES:
            for dialect in ('7.0', '8.0'):
                for profile in ('4.9', '4.11', '4.13', '4.17'):
                    with self.subTest(
                        engine=engine_name,
                        dialect=dialect,
                        profile=profile,
                    ):
                        async with open_client(
                            engine_name,
                            mongodb_dialect=dialect,
                            pymongo_profile=profile,
                        ) as client:
                            collection = client.test.now
                            await collection.insert_many([{'_id': 'a'}, {'_id': 'b'}])
                            await collection.update_many(
                                {},
                                [{'$set': {'first': '$$NOW', 'second': '$$NOW'}}],
                            )
                            documents = await collection.find({}, sort=[('_id', 1)]).to_list()
                            self.assertEqual(documents[0]['first'], documents[0]['second'])
                            self.assertEqual(documents[0]['first'], documents[1]['first'])
                            self.assertIsNone(documents[0]['first'].tzinfo)
                            self.assertEqual(documents[0]['first'].microsecond % 1000, 0)

    async def test_now_is_shared_by_update_command_and_classic_bulk_batch(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.now
                    await collection.insert_many([{'_id': 'a'}, {'_id': 'b'}])
                    await client.test.command(
                        {
                            'update': 'now',
                            'updates': [
                                {'q': {'_id': 'a'}, 'u': [{'$set': {'command': '$$NOW'}}]},
                                {'q': {'_id': 'b'}, 'u': [{'$set': {'command': '$$NOW'}}]},
                            ],
                        }
                    )
                    await collection.bulk_write(
                        [
                            UpdateOne({'_id': 'a'}, [{'$set': {'bulk': '$$NOW'}}]),
                            UpdateOne({'_id': 'b'}, [{'$set': {'bulk': '$$NOW'}}]),
                        ]
                    )
                    documents = await collection.find({}, sort=[('_id', 1)]).to_list()
                    self.assertEqual(documents[0]['command'], documents[1]['command'])
                    self.assertEqual(documents[0]['bulk'], documents[1]['bulk'])

    async def test_find_count_and_distinct_capture_now_without_overdeclaring_distinct_let(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.now
                    await collection.insert_many([{'_id': 'a', 'due': None, 'kind': 'a'}])
                    self.assertEqual(
                        await collection.find(
                            {'$expr': {'$ne': ['$$NOW', None]}},
                            let={'limit': 1},
                        ).to_list(),
                        [{'_id': 'a', 'due': None, 'kind': 'a'}],
                    )
                    self.assertEqual(
                        await collection.count_documents(
                            {'$expr': {'$ne': ['$$NOW', None]}},
                            let={'limit': 1},
                        ),
                        1,
                    )
                    self.assertEqual(
                        await collection.distinct(
                            'kind', {'$expr': {'$ne': ['$$NOW', None]}}
                        ),
                        ['a'],
                    )
                    with self.assertRaises(TypeError):
                        await collection.distinct('kind', {}, let={'limit': 1})

    async def test_user_let_validates_types_and_variable_names(self):
        async with open_client('memory') as client:
            collection = client.test.now
            await collection.insert_one({'_id': 'a'})
            with self.assertRaisesRegex(TypeError, 'let must be a dict'):
                await collection.find({}, let=[]).to_list()
            with self.assertRaisesRegex(OperationFailure, r'\$lookup let variable names'):
                await collection.find({}, let={'Invalid': 1}).to_list()

    async def test_modify_selection_paths_preserve_user_let_and_now(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.now
                    await collection.insert_many(
                        [
                            {'_id': 'selected', 'kind': 'selected'},
                            {'_id': 'deleted', 'kind': 'deleted'},
                            {'_id': 'command', 'kind': 'command'},
                        ]
                    )
                    await collection.find_one_and_update(
                        {'$expr': {'$eq': ['$kind', '$$kind']}},
                        [{'$set': {'bound': '$$kind', 'at': '$$NOW'}}],
                        let={'kind': 'selected'},
                    )
                    selected = await collection.find_one({'_id': 'selected'})
                    self.assertEqual(selected['bound'], 'selected')
                    self.assertIsNotNone(selected['at'])

                    deleted = await collection.delete_many(
                        {
                            '$expr': {
                                '$and': [
                                    {'$eq': ['$kind', '$$kind']},
                                    {'$ne': ['$$NOW', None]},
                                ]
                            }
                        },
                        let={'kind': 'deleted'},
                    )
                    self.assertEqual(deleted.deleted_count, 1)

                    command_result = await client.test.command(
                        {
                            'delete': 'now',
                            'let': {'kind': 'command'},
                            'deletes': [
                                {
                                    'q': {
                                        '$expr': {
                                            '$and': [
                                                {'$eq': ['$kind', '$$kind']},
                                                {'$ne': ['$$NOW', None]},
                                            ]
                                        }
                                    },
                                    'limit': 1,
                                }
                            ],
                        }
                    )
                    self.assertEqual(command_result['n'], 1)

    async def test_aggregate_pushdown_and_subpipelines_inherit_one_now(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.now
                    foreign = client.test.foreign
                    await collection.insert_one({'_id': 'a'})
                    await foreign.insert_one({'_id': 'f'})
                    documents = await collection.aggregate(
                        [
                            {'$match': {'$expr': {'$ne': ['$$NOW', None]}}},
                            {'$project': {'top': '$$NOW', 'marker': '$$marker'}},
                            {
                                '$lookup': {
                                    'from': 'foreign',
                                    'pipeline': [
                                        {
                                            '$project': {
                                                'nested': '$$NOW',
                                                'marker': '$$marker',
                                            }
                                        }
                                    ],
                                    'as': 'joined',
                                }
                            },
                            {
                                '$facet': {
                                    'left': [
                                        {
                                            '$project': {
                                                'facet': '$$NOW',
                                                'top': 1,
                                                'marker': 1,
                                                'joined': 1,
                                            }
                                        }
                                    ],
                                    'right': [{'$project': {'facet': '$$NOW'}}],
                                }
                            },
                        ],
                        batch_size=1,
                        let={'marker': 'outer'},
                    ).to_list()
                    facets = documents[0]
                    values = [
                        facets['left'][0]['top'],
                        facets['left'][0]['joined'][0]['nested'],
                        facets['left'][0]['facet'],
                        facets['right'][0]['facet'],
                    ]
                    self.assertTrue(all(value == values[0] for value in values))
                    self.assertEqual(facets['left'][0]['marker'], 'outer')
                    self.assertEqual(facets['left'][0]['joined'][0]['marker'], 'outer')

    async def test_unknown_system_variables_use_the_server_undefined_variable_error(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.now
                    await collection.insert_one({'_id': 'a'})
                    for variable in ('$$CLUSTER_TIME', '$$SEARCH_META', '$$USER_ROLES', '$$IDX'):
                        with self.subTest(variable=variable):
                            with self.assertRaisesRegex(OperationFailure, 'Use of undefined variable') as raised:
                                await collection.find({'$expr': {'$eq': [variable, None]}}).to_list()
                            self.assertEqual(raised.exception.code, 17276)
