import copy
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pymongo import UpdateOne

from mongoeco import MongoClient, ReturnDocument
from mongoeco.api import operations
from tests.support import ENGINE_FACTORIES, open_client


_SOURCE = datetime(
    2024,
    9,
    20,
    16,
    43,
    45,
    987_654,
    tzinfo=timezone(-timedelta(hours=7)),
)
_NORMALIZED = datetime(2024, 9, 20, 23, 43, 45, 987_000)


def _document(document_id: str) -> dict[str, object]:
    return {
        '_id': document_id,
        'items': [
            {
                'audit': {'windows': [{'at': _SOURCE}]},
                'value': 'before',
            }
        ],
    }


def _array_filters(value: datetime = _SOURCE) -> list[dict[str, object]]:
    return [{'item.audit.windows.at': value}]


class ArrayFilterDatetimeAsyncContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_datetime_array_filters_cover_all_async_update_surfaces(
        self,
    ):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.array_filter_datetime_async

                    direct_filters = _array_filters()
                    direct_snapshot = copy.deepcopy(direct_filters)
                    await collection.insert_one(_document('update-one'))
                    result = await collection.update_one(
                        {'_id': 'update-one'},
                        {'$set': {'items.$[item].value': 'update-one'}},
                        array_filters=direct_filters,
                    )
                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(direct_filters, direct_snapshot)

                    await collection.insert_many(
                        [_document('many-one'), _document('many-two')]
                    )
                    many_filters = _array_filters()
                    many_snapshot = copy.deepcopy(many_filters)
                    result = await collection.update_many(
                        {'_id': {'$in': ['many-one', 'many-two']}},
                        {'$set': {'items.$[item].value': 'update-many'}},
                        array_filters=many_filters,
                    )
                    self.assertEqual(result.matched_count, 2)
                    self.assertEqual(result.modified_count, 2)
                    self.assertEqual(many_filters, many_snapshot)

                    await collection.insert_one(_document('find-update'))
                    find_filters = _array_filters()
                    find_snapshot = copy.deepcopy(find_filters)
                    updated = await collection.find_one_and_update(
                        {'_id': 'find-update'},
                        {'$set': {'items.$[item].value': 'find-update'}},
                        array_filters=find_filters,
                        return_document=ReturnDocument.AFTER,
                    )
                    assert updated is not None
                    self.assertEqual(
                        updated['items'][0]['value'], 'find-update'
                    )
                    self.assertEqual(find_filters, find_snapshot)

                    await collection.insert_one(_document('bulk'))
                    bulk_filters = _array_filters()
                    bulk_snapshot = copy.deepcopy(bulk_filters)
                    result = await collection.bulk_write(
                        [
                            UpdateOne(
                                {'_id': 'bulk'},
                                {'$set': {'items.$[item].value': 'bulk'}},
                                array_filters=bulk_filters,
                            )
                        ]
                    )
                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(bulk_filters, bulk_snapshot)

                    stored = await collection.find_one({'_id': 'update-one'})
                    assert stored is not None
                    self.assertEqual(
                        stored['items'][0]['audit']['windows'][0]['at'],
                        _NORMALIZED,
                    )

    async def test_bulk_array_filters_cross_the_normalization_boundary_once(
        self,
    ):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.array_filter_normalization_count
                    await collection.insert_one(_document('bulk'))
                    normalize = operations._normalize_array_filters
                    with patch.object(
                        operations,
                        '_normalize_array_filters',
                        wraps=normalize,
                    ) as normalize_spy:
                        result = await collection.bulk_write(
                            [
                                UpdateOne(
                                    {'_id': 'bulk'},
                                    {'$set': {'items.$[item].value': 'bulk'}},
                                    array_filters=_array_filters(),
                                )
                            ]
                        )

                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(normalize_spy.call_count, 1)

    async def test_datetime_array_filter_non_match_remains_a_noop(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.array_filter_datetime_no_match
                    await collection.insert_one(_document('value'))
                    result = await collection.update_one(
                        {'_id': 'value'},
                        {'$set': {'items.$[item].value': 'after'}},
                        array_filters=_array_filters(
                            _SOURCE + timedelta(seconds=1)
                        ),
                    )
                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(result.modified_count, 0)
                    stored = await collection.find_one({'_id': 'value'})
                    assert stored is not None
                    self.assertEqual(stored['items'][0]['value'], 'before')


class ArrayFilterDatetimeSyncContractTests(unittest.TestCase):
    def test_datetime_array_filters_cover_all_sync_update_surfaces(self):
        for engine_name, engine_factory in ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(engine_factory()) as client:
                    collection = client.test.array_filter_datetime_sync
                    operations = (
                        ('update-one', 'update_one'),
                        ('find-update', 'find_one_and_update'),
                        ('bulk', 'bulk_write'),
                    )
                    for document_id, _operation in operations:
                        collection.insert_one(_document(document_id))
                    collection.insert_many(
                        [_document('many-one'), _document('many-two')]
                    )

                    direct_filters = _array_filters()
                    direct_snapshot = copy.deepcopy(direct_filters)
                    self.assertEqual(
                        collection.update_one(
                            {'_id': 'update-one'},
                            {'$set': {'items.$[item].value': 'update-one'}},
                            array_filters=direct_filters,
                        ).modified_count,
                        1,
                    )
                    self.assertEqual(direct_filters, direct_snapshot)

                    many_filters = _array_filters()
                    many_snapshot = copy.deepcopy(many_filters)
                    many_result = collection.update_many(
                        {'_id': {'$in': ['many-one', 'many-two']}},
                        {'$set': {'items.$[item].value': 'update-many'}},
                        array_filters=many_filters,
                    )
                    self.assertEqual(many_result.modified_count, 2)
                    self.assertEqual(many_filters, many_snapshot)

                    find_filters = _array_filters()
                    find_snapshot = copy.deepcopy(find_filters)
                    updated = collection.find_one_and_update(
                        {'_id': 'find-update'},
                        {'$set': {'items.$[item].value': 'find-update'}},
                        array_filters=find_filters,
                        return_document=ReturnDocument.AFTER,
                    )
                    assert updated is not None
                    self.assertEqual(
                        updated['items'][0]['value'], 'find-update'
                    )
                    self.assertEqual(find_filters, find_snapshot)

                    bulk_filters = _array_filters()
                    bulk_snapshot = copy.deepcopy(bulk_filters)
                    bulk_result = collection.bulk_write(
                        [
                            UpdateOne(
                                {'_id': 'bulk'},
                                {'$set': {'items.$[item].value': 'bulk'}},
                                array_filters=bulk_filters,
                            )
                        ]
                    )
                    self.assertEqual(bulk_result.modified_count, 1)
                    self.assertEqual(bulk_filters, bulk_snapshot)


if __name__ == '__main__':
    unittest.main()
