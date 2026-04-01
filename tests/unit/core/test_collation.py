from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

import mongoeco.core.collation as collation_module
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.collation import (
    collation_backend_info,
    collation_capabilities_info,
    CollationBackendInfo,
    CollationCapabilitiesInfo,
    CollationSpec,
    compare_with_collation,
    icu_collation_available,
    normalize_collation,
    unicode_collation_available,
    values_equal_with_collation,
)


class CollationTests(unittest.TestCase):
    def tearDown(self):
        collation_module._get_icu_collator.cache_clear()
        collation_module._get_pyuca_collator.cache_clear()

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

    def test_collation_capabilities_info_reports_supported_contract(self):
        info = collation_capabilities_info()

        self.assertIsInstance(info, CollationCapabilitiesInfo)
        self.assertEqual(info.supported_locales, ("simple", "en"))
        self.assertEqual(info.supported_strengths, (1, 2, 3))
        self.assertTrue(info.supports_case_level)
        self.assertTrue(info.supports_numeric_ordering)
        self.assertTrue(info.optional_icu_backend)
        self.assertEqual(
            info.advanced_options_require_icu,
            ("backwards", "alternate", "maxVariable", "normalization"),
        )
        self.assertIn("supportedLocales", info.to_document())

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

    def test_collation_spec_to_document_includes_optional_fields(self):
        spec = CollationSpec(
            locale="en",
            strength=2,
            backwards=True,
            alternate="shifted",
            max_variable="space",
            normalization=True,
        )

        self.assertEqual(
            spec.to_document(),
            {
                "locale": "en",
                "strength": 2,
                "caseLevel": False,
                "numericOrdering": False,
                "backwards": True,
                "alternate": "shifted",
                "maxVariable": "space",
                "normalization": True,
            },
        )

    def test_internal_icu_helpers_cover_attributes_and_compare_paths(self):
        class _FakeCollator:
            PRIMARY = 1
            SECONDARY = 2
            TERTIARY = 3

            def __init__(self):
                self.strength = None
                self.attributes: list[tuple[object, object]] = []

            def setStrength(self, strength):
                self.strength = strength

            def setAttribute(self, attribute, value):
                self.attributes.append((attribute, value))

            def compare(self, left, right):
                if left < right:
                    return -2
                if left > right:
                    return 3
                return 0

        collator = _FakeCollator()
        fake_icu = SimpleNamespace(
            Collator=SimpleNamespace(
                PRIMARY=1,
                SECONDARY=2,
                TERTIARY=3,
                createInstance=lambda _locale: collator,
            ),
            Locale=lambda locale: locale,
            UCollAttribute=SimpleNamespace(
                NORMALIZATION_MODE="normalization",
                CASE_LEVEL="case",
                NUMERIC_COLLATION="numeric",
                FRENCH_COLLATION="french",
                ALTERNATE_HANDLING="alternate",
                MAX_VARIABLE="max-variable",
            ),
            UCollAttributeValue=SimpleNamespace(
                ON="on",
                OFF="off",
                SHIFTED="shifted",
                NON_IGNORABLE="non-ignorable",
                SPACE="space",
                PUNCT="punct",
            ),
        )

        with patch.object(collation_module, "_icu", fake_icu):
            resolved = collation_module._get_icu_collator(
                "en",
                2,
                True,
                True,
                True,
                "shifted",
                "space",
                True,
            )
            spec = CollationSpec(
                locale="en",
                strength=2,
                case_level=True,
                numeric_ordering=True,
                backwards=True,
                alternate="shifted",
                max_variable="space",
                normalization=True,
            )
            self.assertEqual(collation_module._compare_with_icu("a", "b", spec), -1)
            self.assertEqual(collation_module._compare_with_icu("b", "a", spec), 1)
            self.assertEqual(collation_module._compare_with_icu("a", "a", spec), 0)

        self.assertIs(resolved, collator)
        self.assertEqual(collator.strength, 2)
        self.assertIn(("normalization", "on"), collator.attributes)
        self.assertIn(("case", "on"), collator.attributes)
        self.assertIn(("numeric", "on"), collator.attributes)
        self.assertIn(("french", "on"), collator.attributes)
        self.assertIn(("alternate", "shifted"), collator.attributes)
        self.assertIn(("max-variable", "space"), collator.attributes)

    def test_pyuca_helpers_cover_runtime_guard_key_truncation_and_accent_stripping(self):
        with patch.object(collation_module, "_pyuca", None):
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                collation_module._get_pyuca_collator()

        class _FakePyucaCollator:
            def sort_key(self, value):
                if value == "Az":
                    return (1, 2, 0, 3, 0, 4)
                return tuple(ord(char) for char in value) + (0,)

        fake_pyuca = SimpleNamespace(Collator=lambda: _FakePyucaCollator())
        with patch.object(collation_module, "_pyuca", fake_pyuca):
            spec = CollationSpec(locale="en", strength=3, case_level=True)
            numeric_spec = CollationSpec(locale="en", strength=2, numeric_ordering=True)

            self.assertEqual(
                collation_module._truncate_uca_key((1, 2, 0, 3, 0, 4), spec),
                ((1, 2), (3,), (4,)),
            )
            self.assertEqual(
                collation_module._split_uca_levels((1, 2, 0, 3, 0, 4)),
                ((1, 2), (3,), (4,)),
            )
            self.assertEqual(
                collation_module._pyuca_collation_key("Az", spec),
                ((1, 2), (3,), (4,)),
            )
            self.assertEqual(
                collation_module._pyuca_collation_key("a10b", numeric_spec),
                ((1, ((97,),)), (0, 10), (1, ((98,),))),
            )

        self.assertEqual(collation_module._strip_accents("Árbol"), "Arbol")

    def test_fallback_primary_key_and_python_string_comparison_branches(self):
        with (
            patch("mongoeco.core.collation._can_use_icu_collation", return_value=False),
            patch("mongoeco.core.collation._can_use_pyuca_collation", return_value=False),
        ):
            strength_one = CollationSpec(locale="en", strength=1)
            strength_two_numeric = CollationSpec(locale="en", strength=2, numeric_ordering=True)
            case_level = CollationSpec(locale="en", strength=2, case_level=True)

            self.assertEqual(
                collation_module._collation_primary_key("Álvaro", strength_one),
                ("alvaro",),
            )
            self.assertEqual(
                collation_module._collation_primary_key("A10b", strength_two_numeric),
                ((1, "a"), (0, 10), (1, "b")),
            )
            self.assertLess(compare_with_collation("a", "b", collation=case_level), 0)
            self.assertGreater(compare_with_collation("b", "a", collation=case_level), 0)
