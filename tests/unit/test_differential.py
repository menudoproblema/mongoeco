import unittest
from unittest.mock import patch

from tests.differential._real_parity_base import MongoRealParityBase
from tests.differential.cases import REAL_PARITY_CASES, RealParityCase
from tests.differential.runner import available_case_names, build_suite


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
        case = RealParityCase(
            name="stub_case",
            seed_documents=[{"_id": "1"}],
            action=lambda _collection: "ok",
        )

        with patch('tests.differential._real_parity_base.MongoClient', _MongoClientStub):
            harness._assert_matches_real_case(case)

        self.assertEqual(_MongoClientStub.seen_dialects, ['8.0', '8.0'])

    def test_real_parity_cases_are_exposed_as_dynamic_tests(self):
        dynamic_names = {
            name
            for name in dir(MongoRealParityBase)
            if name.startswith("test_") and name.endswith("_matches_real_mongodb")
        }

        expected_names = {
            f"test_{case.name}_matches_real_mongodb"
            for case in REAL_PARITY_CASES
        }

        self.assertTrue(expected_names)
        self.assertEqual(dynamic_names, expected_names)

    def test_available_case_names_filters_by_target_version(self):
        all_cases = available_case_names()
        targeted_cases = available_case_names((7, 0))

        self.assertTrue(all_cases)
        self.assertEqual(all_cases, targeted_cases)

    def test_build_suite_can_filter_case_pattern(self):
        suite = build_suite("7.0", "find_expr_*")
        names = {
            getattr(test, "_testMethodName")
            for test in self._iter_tests(suite)
        }

        self.assertIn("test_find_expr_compare_fields_matches_real_mongodb", names)
        self.assertIn("test_find_expr_truthiness_array_matches_real_mongodb", names)
        self.assertNotIn("test_find_subdocument_order_sensitive_equality_matches_real_mongodb", names)

    def _iter_tests(self, suite: unittest.TestSuite):
        for test in suite:
            if isinstance(test, unittest.TestSuite):
                yield from self._iter_tests(test)
            else:
                yield test
