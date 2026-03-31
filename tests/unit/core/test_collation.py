from __future__ import annotations

import unittest
from unittest.mock import patch

from mongoeco.core.collation import (
    CollationSpec,
    compare_with_collation,
    icu_collation_available,
    normalize_collation,
    values_equal_with_collation,
)


class CollationTests(unittest.TestCase):
    def test_normalize_collation_accepts_supported_document(self):
        spec = normalize_collation(
            {
                "locale": "en",
                "strength": 2,
                "caseLevel": True,
                "numericOrdering": True,
            }
        )

        self.assertEqual(
            spec,
            CollationSpec(
                locale="en",
                strength=2,
                case_level=True,
                numeric_ordering=True,
            ),
        )
        self.assertEqual(
            spec.to_document(),
            {
                "locale": "en",
                "strength": 2,
                "caseLevel": True,
                "numericOrdering": True,
            },
        )

    def test_normalize_collation_rejects_invalid_documents(self):
        with self.assertRaises(TypeError):
            normalize_collation("en")
        with self.assertRaises(TypeError):
            normalize_collation({"locale": ""})
        with self.assertRaises(ValueError):
            normalize_collation({"locale": "es"})
        with self.assertRaises(ValueError):
            normalize_collation({"strength": 4})
        with self.assertRaises(TypeError):
            normalize_collation({"caseLevel": 1})
        with self.assertRaises(TypeError):
            normalize_collation({"numericOrdering": 1})

    def test_compare_with_collation_honours_strength_and_case_level(self):
        strength_one = normalize_collation({"locale": "en", "strength": 1})
        strength_two = normalize_collation({"locale": "en", "strength": 2})
        strength_three = normalize_collation({"locale": "en", "strength": 3})
        case_level = normalize_collation({"locale": "en", "strength": 2, "caseLevel": True})

        self.assertEqual(compare_with_collation("Álvaro", "alvaro", collation=strength_one), 0)
        self.assertEqual(compare_with_collation("Alice", "alice", collation=strength_two), 0)
        self.assertNotEqual(compare_with_collation("Alice", "alice", collation=strength_three), 0)
        self.assertNotEqual(compare_with_collation("Alice", "alice", collation=case_level), 0)

    def test_compare_with_collation_supports_numeric_ordering(self):
        lexical = normalize_collation({"locale": "en", "strength": 2})
        numeric = normalize_collation({"locale": "en", "strength": 2, "numericOrdering": True})

        self.assertLess(compare_with_collation("file10", "file2", collation=lexical), 0)
        self.assertLess(compare_with_collation("file2", "file10", collation=numeric), 0)

    def test_values_equal_with_collation_uses_string_semantics_only_when_requested(self):
        self.assertFalse(values_equal_with_collation("Alice", "alice"))
        self.assertTrue(
            values_equal_with_collation(
                "Alice",
                "alice",
                collation=normalize_collation({"locale": "en", "strength": 2}),
            )
        )

    def test_compare_with_collation_prefers_icu_backend_when_available(self):
        spec = normalize_collation({"locale": "en", "strength": 2})

        with (
            patch("mongoeco.core.collation._can_use_icu_collation", return_value=True),
            patch("mongoeco.core.collation._compare_with_icu", return_value=-1) as compare_icu,
        ):
            self.assertEqual(compare_with_collation("b", "a", collation=spec), -1)

        compare_icu.assert_called_once_with("b", "a", spec)

    def test_icu_collation_available_exposes_backend_presence(self):
        self.assertIsInstance(icu_collation_available(), bool)
        self.assertTrue(values_equal_with_collation(3, 3))
        self.assertFalse(
            values_equal_with_collation(
                3,
                "3",
                collation=normalize_collation({"locale": "en", "strength": 2}),
            )
        )
