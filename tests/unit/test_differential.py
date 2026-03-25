import unittest
from unittest.mock import patch

from tests.differential._real_parity_base import MongoRealParityBase


class _MongoClientStub:
    seen_dialects: list[str | None] = []

    def __init__(self, _engine, *, mongodb_dialect=None, **_kwargs):
        type(self).seen_dialects.append(mongodb_dialect)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get_database(self, _name):
        return self

    def get_collection(self, _name):
        return self

    def insert_one(self, _document):
        return None


class MongoRealParityBaseUnitTests(unittest.TestCase):
    def test_assert_matches_real_uses_target_dialect_for_mongoeco_client(self):
        class Harness(MongoRealParityBase):
            TARGET_VERSION = (8, 0)

        harness = Harness(methodName='runTest')
        harness._real_client = type(
            'RealClientStub',
            (),
            {
                '__getitem__': lambda self, _name: self,
                'insert_one': lambda self, _document: None,
                'drop_database': lambda self, _name: None,
            },
        )()

        _MongoClientStub.seen_dialects = []

        with patch('tests.differential._real_parity_base.MongoClient', _MongoClientStub):
            harness._assert_matches_real(
                [{'_id': '1'}],
                lambda _collection: 'ok',
            )

        self.assertEqual(_MongoClientStub.seen_dialects, ['8.0', '8.0'])
