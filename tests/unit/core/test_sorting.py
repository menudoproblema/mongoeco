import datetime
import unittest
import uuid

from mongoeco.compat import MongoDialect
from mongoeco.core.sorting import compare_documents, sort_documents, sort_documents_limited, sort_documents_window, sort_value
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

    def test_sort_documents_window_keeps_only_requested_prefix(self):
        documents = [
            {"_id": "3", "rank": 3},
            {"_id": "1", "rank": 1},
            {"_id": "4", "rank": 4},
            {"_id": "2", "rank": 2},
        ]

        self.assertEqual(
            [document["_id"] for document in sort_documents_window(documents, [("rank", 1)], window=2)],
            ["1", "2"],
        )

    def test_sort_documents_window_matches_full_sort_prefix_for_array_paths(self):
        documents = [
            {"_id": "a", "scores": [{"value": 4}, {"value": 2}], "rank": 2},
            {"_id": "b", "scores": [{"value": 1}, {"value": 5}], "rank": 1},
            {"_id": "c", "scores": [{"value": 3}], "rank": 3},
        ]

        expected = [
            document["_id"]
            for document in sort_documents(documents, [("scores.value", 1), ("rank", -1)])[:2]
        ]

        self.assertEqual(
            [
                document["_id"]
                for document in sort_documents_window(
                    documents,
                    [("scores.value", 1), ("rank", -1)],
                    window=2,
                )
            ],
            expected,
        )

    def test_sort_documents_window_matches_full_sort_prefix_for_single_key_with_ties(self):
        documents = [
            {"_id": "0", "other": 4},
            {"_id": "1", "other": 3},
            {"_id": "2", "other": 1},
            {"_id": "3", "other": 4},
            {"_id": "4", "other": 4},
            {"_id": "5", "other": 0},
            {"_id": "6", "other": 4},
            {"_id": "7", "other": 4},
            {"_id": "8", "other": 3},
            {"_id": "9", "other": 4},
            {"_id": "10", "other": 5},
            {"_id": "11", "other": 0},
        ]

        expected = [
            document["_id"]
            for document in sort_documents(documents, [("other", 1)])[:8]
        ]

        self.assertEqual(
            [
                document["_id"]
                for document in sort_documents_window(
                    documents,
                    [("other", 1)],
                    window=8,
                )
            ],
            expected,
        )

    def test_sort_documents_limited_applies_skip_and_limit_without_full_tail(self):
        documents = [
            {"_id": "5", "rank": 5},
            {"_id": "1", "rank": 1},
            {"_id": "4", "rank": 4},
            {"_id": "2", "rank": 2},
            {"_id": "3", "rank": 3},
        ]

        self.assertEqual(
            [document["_id"] for document in sort_documents_limited(documents, [("rank", 1)], skip=1, limit=2)],
            ["2", "3"],
        )

    def test_sort_value_with_multiple_nested_arrays_picks_minimum_across_all_candidates(self):
        document = {"scores": [{"values": [10, 1, 5]}, {"values": [3, 8]}]}

        self.assertEqual(sort_value(document, "scores.values", 1), 1)

    def test_sort_value_returns_none_when_path_does_not_exist_at_any_array_element(self):
        document = {"items": [{"rank": 1}, {"rank": 2}]}

        self.assertIsNone(sort_value(document, "items.value", 1))
