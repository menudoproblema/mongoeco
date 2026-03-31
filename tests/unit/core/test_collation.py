from __future__ import annotations

import unittest
from unittest.mock import patch

import mongoeco.core.collation as collation_module
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.collation import (
    collation_backend_info,
    CollationBackendInfo,
    CollationSpec,
    compare_with_collation,
    icu_collation_available,
    normalize_collation,
    unicode_collation_available,
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

    def test_normalize_collation_accepts_advanced_document_when_icu_is_available(self):
        with patch.object(collation_module, "_icu", object()):
            spec = normalize_collation(
                {
                    "locale": "en",
                    "strength": 2,
                    "caseLevel": True,
                    "numericOrdering": True,
                    "backwards": True,
                    "alternate": "shifted",
                    "maxVariable": "space",
                    "normalization": True,
                }
            )

        self.assertEqual(
            spec,
            CollationSpec(
                locale="en",
                strength=2,
                case_level=True,
                numeric_ordering=True,
                backwards=True,
                alternate="shifted",
                max_variable="space",
                normalization=True,
            ),
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
        with self.assertRaises(TypeError):
            normalize_collation({"backwards": 1})
        with self.assertRaises(ValueError):
            normalize_collation({"alternate": "ignored"})
        with self.assertRaises(ValueError):
            normalize_collation({"maxVariable": "symbol"})
        with self.assertRaises(TypeError):
            normalize_collation({"normalization": 1})

    def test_normalize_collation_rejects_icu_only_options_without_icu(self):
        with patch.object(collation_module, "_icu", None):
            with self.assertRaisesRegex(ValueError, "PyICU backend"):
                normalize_collation({"locale": "en", "strength": 2, "backwards": True})
            with self.assertRaisesRegex(ValueError, "PyICU backend"):
                normalize_collation({"locale": "en", "strength": 2, "alternate": "shifted"})
            with self.assertRaisesRegex(ValueError, "PyICU backend"):
                normalize_collation({"locale": "en", "strength": 2, "normalization": True})

    def test_simple_collation_rejects_unicode_tailoring_knobs(self):
        with self.assertRaisesRegex(ValueError, "simple collation does not support caseLevel"):
            normalize_collation({"locale": "simple", "caseLevel": True})
        with self.assertRaisesRegex(ValueError, "simple collation does not support numericOrdering"):
            normalize_collation({"locale": "simple", "numericOrdering": True})
        with self.assertRaisesRegex(ValueError, "simple collation does not support normalization"):
            normalize_collation({"locale": "simple", "normalization": True})

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

    def test_compare_with_collation_uses_pyuca_backend_when_icu_is_unavailable(self):
        spec = normalize_collation({"locale": "en", "strength": 2})

        with (
            patch("mongoeco.core.collation._can_use_icu_collation", return_value=False),
            patch("mongoeco.core.collation._can_use_pyuca_collation", return_value=True),
            patch("mongoeco.core.collation._compare_with_pyuca", return_value=1) as compare_pyuca,
        ):
            self.assertEqual(compare_with_collation("b", "a", collation=spec), 1)

        compare_pyuca.assert_called_once_with("b", "a", spec)

    def test_compare_with_collation_rejects_icu_only_options_without_icu(self):
        spec = CollationSpec(locale="en", strength=2, backwards=True)

        with (
            patch("mongoeco.core.collation._can_use_icu_collation", return_value=False),
            patch("mongoeco.core.collation._can_use_pyuca_collation", return_value=True),
        ):
            with self.assertRaisesRegex(ValueError, "require an ICU backend"):
                compare_with_collation("b", "a", collation=spec)

    def test_compare_with_simple_collation_skips_unicode_backends(self):
        spec = normalize_collation({"locale": "simple", "strength": 3})

        with (
            patch("mongoeco.core.collation._compare_with_icu") as compare_icu,
            patch("mongoeco.core.collation._compare_with_pyuca") as compare_pyuca,
        ):
            self.assertLess(compare_with_collation("A", "b", collation=spec), 0)

        compare_icu.assert_not_called()
        compare_pyuca.assert_not_called()

    def test_simple_collation_matches_baseline_string_semantics(self):
        spec = normalize_collation({"locale": "simple", "strength": 3})
        samples = [
            ("A", "a"),
            ("10", "2"),
            ("Á", "A"),
            ("hello", "hello"),
        ]

        for left, right in samples:
            self.assertEqual(
                compare_with_collation(left, right, collation=spec),
                MONGODB_DIALECT_70.policy.compare_values(left, right),
            )
            self.assertEqual(
                values_equal_with_collation(left, right, collation=spec),
                MONGODB_DIALECT_70.policy.values_equal(left, right),
            )

    def test_icu_collation_available_exposes_backend_presence(self):
        self.assertIsInstance(icu_collation_available(), bool)
        self.assertIsInstance(unicode_collation_available(), bool)
        info = collation_backend_info()
        self.assertIsInstance(info, CollationBackendInfo)
        self.assertIsInstance(info.to_document(), dict)
        self.assertTrue(values_equal_with_collation(3, 3))
        self.assertFalse(
            values_equal_with_collation(
                3,
                "3",
                collation=normalize_collation({"locale": "en", "strength": 2}),
            )
        )

    def test_collation_backend_info_prefers_icu_then_pyuca_then_none(self):
        with (
            patch.object(collation_module, "_icu", object()),
            patch.object(collation_module, "_pyuca", object()),
        ):
            info = collation_module.collation_backend_info()
            self.assertEqual(info.selected_backend, "icu")
            self.assertEqual(info.available_backends, ("icu", "pyuca"))
            self.assertTrue(info.unicode_available)
            self.assertTrue(info.advanced_options_available)

        with (
            patch.object(collation_module, "_icu", None),
            patch.object(collation_module, "_pyuca", object()),
        ):
            info = collation_module.collation_backend_info()
            self.assertEqual(info.selected_backend, "pyuca")
            self.assertEqual(info.available_backends, ("pyuca",))
            self.assertTrue(info.unicode_available)
            self.assertFalse(info.advanced_options_available)

        with (
            patch.object(collation_module, "_icu", None),
            patch.object(collation_module, "_pyuca", None),
        ):
            info = collation_module.collation_backend_info()
            self.assertEqual(info.selected_backend, "none")
            self.assertEqual(info.available_backends, ())
            self.assertFalse(info.unicode_available)
            self.assertFalse(info.advanced_options_available)

    def test_actual_unicode_backend_handles_accent_and_numeric_ordering(self):
        spec = normalize_collation({"locale": "en", "strength": 1, "numericOrdering": True})

        self.assertEqual(compare_with_collation("Álvaro", "alvaro", collation=spec), 0)
        self.assertLess(compare_with_collation("file2", "file10", collation=spec), 0)

    def test_actual_pyuca_backend_handles_accent_and_numeric_ordering(self):
        if collation_module._pyuca is None:
            self.skipTest("pyuca backend unavailable")

        spec = normalize_collation({"locale": "en", "strength": 1, "numericOrdering": True})

        self.assertEqual(collation_module._compare_with_pyuca("Álvaro", "alvaro", spec), 0)
        self.assertLess(collation_module._compare_with_pyuca("file2", "file10", spec), 0)

    def test_actual_icu_backend_handles_accent_and_numeric_ordering_when_available(self):
        if collation_module._icu is None:
            self.skipTest("ICU backend unavailable")

        spec = normalize_collation({"locale": "en", "strength": 1, "numericOrdering": True})

        self.assertEqual(collation_module._compare_with_icu("Álvaro", "alvaro", spec), 0)
        self.assertLess(collation_module._compare_with_icu("file2", "file10", spec), 0)
