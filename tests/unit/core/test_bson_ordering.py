from __future__ import annotations

import datetime
from decimal import Decimal
import unittest
import uuid

from mongoeco.core.bson_ordering import (
    SQLITE_SORT_BUCKET_WEIGHTS,
    bson_engine_key,
    bson_numeric_index_key,
)
from mongoeco.types import Binary, Regex, Timestamp


class BsonOrderingTests(unittest.TestCase):
    def test_sqlite_sort_bucket_weights_cover_special_bson_types(self):
        self.assertEqual(SQLITE_SORT_BUCKET_WEIGHTS["binary"], 6)
        self.assertEqual(SQLITE_SORT_BUCKET_WEIGHTS["timestamp"], 10)
        self.assertEqual(SQLITE_SORT_BUCKET_WEIGHTS["regex"], 11)

    def test_bson_engine_key_distinguishes_binary_subtype_and_raw_bytes(self):
        generic_bytes = bson_engine_key(b"abc")
        subtype_zero = bson_engine_key(Binary(b"abc", subtype=0))
        subtype_four = bson_engine_key(Binary(b"abc", subtype=4))

        self.assertNotEqual(generic_bytes, subtype_zero)
        self.assertNotEqual(subtype_zero, subtype_four)
        self.assertEqual(subtype_four, ("binary", 4, b"abc"))

    def test_bson_engine_key_normalizes_special_bson_value_families(self):
        now = datetime.datetime(2026, 4, 1, 12, 30, 0)
        token = uuid.UUID("12345678-1234-5678-1234-567812345678")

        self.assertEqual(
            bson_engine_key(Timestamp(10, 2)),
            ("timestamp", 10, 2),
        )
        self.assertEqual(
            bson_engine_key(Regex("^Ada$", "mi")),
            ("regex", "^Ada$", "im"),
        )
        self.assertEqual(
            bson_engine_key(
                {"payload": [Binary(b"x", subtype=4), Timestamp(2, 1), Regex("a", "im")]}
            ),
            (
                "dict",
                (
                    (
                        "payload",
                        (
                            "list",
                            (
                                ("binary", 4, b"x"),
                                ("timestamp", 2, 1),
                                ("regex", "a", "im"),
                            ),
                        ),
                    ),
                ),
            ),
        )
        self.assertEqual(bson_engine_key(now), ("datetime", now))
        self.assertEqual(bson_engine_key(token), ("uuid", token))

    def test_bson_numeric_index_key_orders_numbers_lexicographically(self):
        keys = [
            bson_numeric_index_key(-20),
            bson_numeric_index_key(-1),
            bson_numeric_index_key(0),
            bson_numeric_index_key(2),
            bson_numeric_index_key(10),
        ]

        self.assertEqual(keys, sorted(keys))
        self.assertEqual(bson_numeric_index_key(Decimal("0.00")), "1|0|!")

    def test_bson_numeric_index_key_rejects_non_finite_values(self):
        with self.assertRaises(NotImplementedError):
            bson_numeric_index_key(Decimal("NaN"))
        with self.assertRaises(NotImplementedError):
            bson_numeric_index_key(Decimal("Infinity"))


if __name__ == "__main__":
    unittest.main()
