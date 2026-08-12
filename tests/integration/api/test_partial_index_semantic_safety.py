import unittest
from decimal import Decimal

from bson.decimal128 import Decimal128

from mongoeco import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError
from mongoeco.errors import OperationFailure


ENGINE_FACTORIES = {
    'memory': MemoryEngine,
    'sqlite': SQLiteEngine,
}


class PartialIndexSemanticSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_index_hint_requires_matching_collation(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.partial_hint_collation
                    await collection.insert_one(
                        {'_id': 'one', 'value': 'target', 'state': 'active'}
                    )
                    index_name = await collection.create_index(
                        [('value', 1)],
                        collation={'locale': 'en', 'strength': 2},
                        partial_filter_expression={'state': 'ACTIVE'},
                    )

                    with self.assertRaises(OperationFailure):
                        await collection.find(
                            {'value': 'target', 'state': 'active'},
                            hint=index_name,
                        ).to_list()

                    documents = await collection.find(
                        {'value': 'target', 'state': 'active'},
                        hint=index_name,
                        collation={'locale': 'en', 'strength': 2},
                    ).to_list()
                    self.assertEqual(
                        documents,
                        [{'_id': 'one', 'value': 'target', 'state': 'active'}],
                    )

    async def test_index_never_changes_cross_type_numeric_equality(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.numeric_index_equality
                    await collection.insert_many(
                        [
                            {'_id': 'integer', 'value': 1},
                            {'_id': 'double', 'value': 2.0},
                            {'_id': 'decimal', 'value': Decimal128('3.0')},
                        ]
                    )
                    queries = [1.0, 2, Decimal('3'), Decimal128('1')]
                    before = [
                        await collection.distinct('_id', {'value': value})
                        for value in queries
                    ]

                    await collection.create_index([('value', 1)])
                    after = [
                        await collection.distinct('_id', {'value': value})
                        for value in queries
                    ]

                    self.assertEqual(
                        before,
                        [['integer'], ['double'], ['decimal'], ['integer']],
                    )
                    self.assertEqual(after, before)

    async def test_partial_unique_index_uses_its_collation_for_membership(
        self,
    ):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.partial_unique_collation
                    await collection.create_index(
                        [('email', 1)],
                        unique=True,
                        collation={'locale': 'en', 'strength': 2},
                        partial_filter_expression={'state': 'ACTIVE'},
                    )
                    await collection.insert_one(
                        {
                            '_id': 'first',
                            'email': 'same@example.com',
                            'state': 'active',
                        }
                    )

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one(
                            {
                                '_id': 'second',
                                'email': 'SAME@example.com',
                                'state': 'ACTIVE',
                            }
                        )

    async def test_partial_index_never_changes_bson_query_results(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.partial_index_bson_equality
                    await collection.insert_many(
                        [
                            {'_id': 'number', 'value': 'target', 'flag': 1},
                            {
                                '_id': 'boolean',
                                'value': 'target',
                                'flag': True,
                            },
                        ]
                    )

                    before = await collection.distinct(
                        '_id', {'value': 'target', 'flag': 1}
                    )
                    await collection.create_index(
                        [('value', 1)],
                        partial_filter_expression={'flag': True},
                    )
                    after = await collection.distinct(
                        '_id', {'value': 'target', 'flag': 1}
                    )

                    self.assertEqual(before, ['number'])
                    self.assertEqual(after, before)

    async def test_partial_in_implication_accepts_nested_documents(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(engine_factory()) as client:
                    collection = client.test.partial_index_document_values
                    await collection.insert_one(
                        {'_id': 'one', 'value': 'target', 'flag': {'a': 1}}
                    )
                    await collection.create_index(
                        [('value', 1)],
                        partial_filter_expression={
                            'flag': {'$in': [{'a': 1}, {'a': 2}]}
                        },
                    )

                    documents = await collection.find(
                        {'value': 'target', 'flag': {'$in': [{'a': 1}]}},
                        {'_id': 1},
                    ).to_list()

                    self.assertEqual(documents, [{'_id': 'one'}])


if __name__ == '__main__':
    unittest.main()
