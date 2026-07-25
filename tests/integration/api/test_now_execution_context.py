import unittest

from mongoeco import UpdateOne
from mongoeco.errors import OperationFailure
from tests.support import ENGINE_FACTORIES, open_client


class NowExecutionContextIntegrationTests(unittest.IsolatedAsyncioTestCase):
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
