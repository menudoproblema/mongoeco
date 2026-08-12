import asyncio
import unittest

from mongoeco import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError, OperationFailure


ENGINE_FACTORIES = {'memory': MemoryEngine, 'sqlite': SQLiteEngine}


class MergeAtomicContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_keep_existing_does_not_validate_or_rewrite_legacy_document(
        self,
    ):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    await client.test.create_collection(
                        'target',
                        validator={'$jsonSchema': {'required': ['required']}},
                    )
                    target = client.test.target
                    source = client.test.source
                    await target.insert_one(
                        {'_id': 'legacy', 'value': 'stored'},
                        bypass_document_validation=True,
                    )
                    await source.insert_one(
                        {'_id': 'legacy', 'value': 'incoming'}
                    )

                    await source.aggregate(
                        [
                            {
                                '$merge': {
                                    'into': 'target',
                                    'whenMatched': 'keepExisting',
                                    'whenNotMatched': 'discard',
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        await target.find_one({'_id': 'legacy'}),
                        {'_id': 'legacy', 'value': 'stored'},
                    )

    async def test_secondary_unique_collision_is_not_reclassified_as_identity_match(
        self,
    ):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    target = client.test.target
                    source = client.test.source
                    await target.create_index('email', unique=True)
                    await target.insert_one({'_id': 'stored', 'email': 'same'})
                    await source.insert_one(
                        {'_id': 'incoming', 'email': 'same'}
                    )

                    with self.assertRaises(DuplicateKeyError):
                        await source.aggregate(
                            [
                                {
                                    '$merge': {
                                        'into': 'target',
                                        'whenMatched': 'fail',
                                        'whenNotMatched': 'insert',
                                    }
                                }
                            ]
                        ).to_list()

    async def test_concurrent_fail_on_match_allows_exactly_one_insert(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    await client.test.left.insert_one(
                        {'_id': 'same', 'side': 'left'}
                    )
                    await client.test.right.insert_one(
                        {'_id': 'same', 'side': 'right'}
                    )
                    stage = {
                        '$merge': {
                            'into': 'target',
                            'whenMatched': 'fail',
                            'whenNotMatched': 'insert',
                        }
                    }

                    results = await asyncio.gather(
                        client.test.left.aggregate([stage]).to_list(),
                        client.test.right.aggregate([stage]).to_list(),
                        return_exceptions=True,
                    )

                    self.assertEqual(
                        sum(
                            isinstance(result, OperationFailure)
                            for result in results
                        ),
                        1,
                    )
                    self.assertEqual(
                        await client.test.target.count_documents({}), 1
                    )


if __name__ == '__main__':
    unittest.main()
