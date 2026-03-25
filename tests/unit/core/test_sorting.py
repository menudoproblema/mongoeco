import datetime
import unittest
import uuid

from mongoeco.compat import MongoDialect
from mongoeco.core.sorting import compare_documents, sort_documents, sort_value
from mongoeco.types import ObjectId


class SortingHelpersTests(unittest.TestCase):
    def test_sort_documents_can_use_custom_dialect_comparator(self):
        class _ReverseStringDialect(MongoDialect):
            def compare_values(self, left, right) -> int:
                if isinstance(left, str) and isinstance(right, str):
                    if left == right:
                        return 0
                    return -1 if left > right else 1
                return super().compare_values(left, right)

        documents = [{"_id": "1", "name": "Ada"}, {"_id": "2", "name": "Grace"}]

        self.assertEqual(
            sort_documents(
                documents,
                [("name", 1)],
                dialect=_ReverseStringDialect(
                    key="test",
                    server_version="test",
                    label="Reverse String Dialect",
                ),
            ),
            [{"_id": "2", "name": "Grace"}, {"_id": "1", "name": "Ada"}],
        )

    def test_sort_value_handles_missing_and_arrays(self):
        self.assertIsNone(sort_value({"name": "Ada"}, "rank", 1))
        self.assertEqual(sort_value({"rank": []}, "rank", 1), [])
        self.assertEqual(sort_value({"rank": [3, 1, 2]}, "rank", 1), 1)
        self.assertEqual(sort_value({"rank": [3, 1, 2]}, "rank", -1), 3)

    def test_sort_value_uses_min_or_max_for_paths_traversing_arrays_of_documents(self):
        document = {"scores": [{"v": 10}, {"v": 1}, {"v": 7}]}

        self.assertEqual(sort_value(document, "scores.v", 1), 1)
        self.assertEqual(sort_value(document, "scores.v", -1), 10)

    def test_compare_and_sort_documents(self):
        self.assertEqual(
            compare_documents(
                {"_id": "1", "rank": 1},
                {"_id": "2", "rank": 2},
                [("rank", -1)],
            ),
            1,
        )
        self.assertEqual(
            sort_documents(
                [{"_id": "1", "rank": 2}, {"_id": "2", "rank": 1}],
                [("rank", 1)],
            ),
            [{"_id": "2", "rank": 1}, {"_id": "1", "rank": 2}],
        )

    def test_compare_documents_supports_secondary_sort_key(self):
        self.assertEqual(
            compare_documents(
                {"_id": "1", "rank": 2, "name": "Grace"},
                {"_id": "2", "rank": 2, "name": "Ada"},
                [("rank", -1), ("name", 1)],
            ),
            1,
        )

    def test_sort_documents_returns_original_list_when_sort_is_none(self):
        documents = [{"_id": "1"}, {"_id": "2"}]

        self.assertIs(sort_documents(documents, None), documents)

    def test_sort_documents_supports_mixed_directions(self):
        documents = [
            {"_id": "1", "rank": 2, "name": "Grace"},
            {"_id": "2", "rank": 2, "name": "Ada"},
            {"_id": "3", "rank": 1, "name": "Bob"},
        ]

        self.assertEqual(
            sort_documents(documents, [("rank", -1), ("name", 1)]),
            [
                {"_id": "2", "rank": 2, "name": "Ada"},
                {"_id": "1", "rank": 2, "name": "Grace"},
                {"_id": "3", "rank": 1, "name": "Bob"},
            ],
        )

    def test_sort_documents_respects_bson_type_order_for_supported_mixed_scalars(self):
        documents = [
            {"_id": "bool", "mixed": True},
            {"_id": "datetime", "mixed": datetime.datetime(2025, 1, 2, 3, 4, 5)},
            {"_id": "null", "mixed": None},
            {"_id": "uuid", "mixed": uuid.UUID("12345678-1234-5678-1234-567812345678")},
            {"_id": "objectid", "mixed": ObjectId("0123456789abcdef01234567")},
            {"_id": "string", "mixed": "alpha"},
            {"_id": "number", "mixed": 7},
        ]

        self.assertEqual(
            [document["_id"] for document in sort_documents(documents, [("mixed", 1)])],
            ["null", "number", "string", "uuid", "objectid", "bool", "datetime"],
        )
