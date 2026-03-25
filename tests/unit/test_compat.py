from types import MappingProxyType
import unittest
from unittest.mock import patch

from mongoeco.compat import (
    AUTO_INSTALLED_PYMONGO_PROFILE,
    DEFAULT_MONGODB_DIALECT,
    DEFAULT_PYMONGO_PROFILE,
    detect_installed_pymongo_profile_resolution,
    MONGODB_DIALECT_HOOK_NAMES,
    MONGODB_DIALECT_70,
    MONGODB_DIALECT_80,
    MONGODB_DIALECT_ALIASES,
    MONGODB_DIALECT_BEHAVIOR_FLAGS,
    MONGODB_DIALECT_CAPABILITIES,
    MONGODB_DIALECTS,
    MongoDialect70,
    MongoDialect80,
    MongoDialectResolution,
    PYMONGO_PROFILE_49,
    PYMONGO_PROFILE_411,
    PYMONGO_PROFILE_413,
    PYMONGO_PROFILE_ALIASES,
    PYMONGO_PROFILE_BEHAVIOR_FLAGS,
    PYMONGO_PROFILE_CAPABILITIES,
    PYMONGO_PROFILE_HOOK_NAMES,
    PYMONGO_PROFILES,
    PyMongoProfile49,
    PyMongoProfile411,
    PyMongoProfile413,
    SUPPORTED_MONGODB_MAJORS,
    SUPPORTED_PYMONGO_MAJORS,
    detect_installed_pymongo_profile,
    PyMongoProfileResolution,
    resolve_mongodb_dialect,
    resolve_mongodb_dialect_resolution,
    resolve_pymongo_profile,
    resolve_pymongo_profile_resolution,
    STRICT_AUTO_INSTALLED_PYMONGO_PROFILE,
)


class CompatResolutionTests(unittest.TestCase):
    def test_catalog_is_exposed_as_immutable_global_data(self):
        self.assertIsInstance(MONGODB_DIALECTS, MappingProxyType)
        self.assertIsInstance(MONGODB_DIALECT_ALIASES, MappingProxyType)
        self.assertIsInstance(MONGODB_DIALECT_CAPABILITIES, MappingProxyType)
        self.assertIsInstance(MONGODB_DIALECT_BEHAVIOR_FLAGS, MappingProxyType)
        self.assertIsInstance(PYMONGO_PROFILES, MappingProxyType)
        self.assertIsInstance(PYMONGO_PROFILE_ALIASES, MappingProxyType)
        self.assertIsInstance(PYMONGO_PROFILE_CAPABILITIES, MappingProxyType)
        self.assertIsInstance(PYMONGO_PROFILE_BEHAVIOR_FLAGS, MappingProxyType)
        self.assertEqual(DEFAULT_MONGODB_DIALECT, '7.0')
        self.assertEqual(DEFAULT_PYMONGO_PROFILE, '4.9')
        self.assertEqual(AUTO_INSTALLED_PYMONGO_PROFILE, 'auto-installed')
        self.assertEqual(STRICT_AUTO_INSTALLED_PYMONGO_PROFILE, 'strict-auto-installed')
        self.assertEqual(MONGODB_DIALECT_HOOK_NAMES, ('null_query_matches_undefined',))
        self.assertEqual(PYMONGO_PROFILE_HOOK_NAMES, ('supports_update_one_sort',))
        self.assertEqual(SUPPORTED_MONGODB_MAJORS, frozenset({7, 8}))
        self.assertEqual(SUPPORTED_PYMONGO_MAJORS, frozenset({4}))

    def test_catalog_singletons_are_the_official_instances(self):
        self.assertIs(MONGODB_DIALECTS['7.0'], MONGODB_DIALECT_70)
        self.assertIs(MONGODB_DIALECTS['8.0'], MONGODB_DIALECT_80)
        self.assertIs(PYMONGO_PROFILES['4.9'], PYMONGO_PROFILE_49)
        self.assertIs(PYMONGO_PROFILES['4.11'], PYMONGO_PROFILE_411)
        self.assertIs(PYMONGO_PROFILES['4.13'], PYMONGO_PROFILE_413)
        self.assertEqual(MONGODB_DIALECT_CAPABILITIES['7.0'], MONGODB_DIALECT_70.capabilities)
        self.assertEqual(MONGODB_DIALECT_CAPABILITIES['8.0'], MONGODB_DIALECT_80.capabilities)
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.9'], PYMONGO_PROFILE_49.capabilities)
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.11'], PYMONGO_PROFILE_411.capabilities)
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.13'], PYMONGO_PROFILE_413.capabilities)

    def test_official_versioned_classes_provide_identity_defaults_without_manual_init(self):
        self.assertEqual(MongoDialect70().key, '7.0')
        self.assertEqual(MongoDialect80().key, '8.0')
        self.assertEqual(PyMongoProfile49().key, '4.9')
        self.assertEqual(PyMongoProfile411().key, '4.11')
        self.assertEqual(PyMongoProfile413().key, '4.13')
        self.assertEqual(PyMongoProfile413().label, 'PyMongo 4.13')

    def test_versioned_behavior_flags_capture_known_server_delta_between_7_and_8(self):
        self.assertTrue(MONGODB_DIALECT_70.null_query_matches_undefined())
        self.assertFalse(MONGODB_DIALECT_80.null_query_matches_undefined())
        self.assertTrue(MONGODB_DIALECT_BEHAVIOR_FLAGS['7.0']['null_query_matches_undefined'])
        self.assertFalse(MONGODB_DIALECT_BEHAVIOR_FLAGS['8.0']['null_query_matches_undefined'])
        self.assertEqual(MONGODB_DIALECT_BEHAVIOR_FLAGS['7.0'], MONGODB_DIALECT_70.behavior_flags())
        self.assertEqual(MONGODB_DIALECT_BEHAVIOR_FLAGS['8.0'], MONGODB_DIALECT_80.behavior_flags())

    def test_pymongo_profile_flags_capture_first_public_api_delta(self):
        self.assertFalse(PYMONGO_PROFILE_49.supports_update_one_sort())
        self.assertTrue(PYMONGO_PROFILE_411.supports_update_one_sort())
        self.assertTrue(PYMONGO_PROFILE_413.supports_update_one_sort())
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.9'], frozenset())
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.11'], frozenset({'update_one.sort'}))
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.13'], frozenset({'update_one.sort'}))
        self.assertFalse(PYMONGO_PROFILE_BEHAVIOR_FLAGS['4.9']['supports_update_one_sort'])
        self.assertTrue(PYMONGO_PROFILE_BEHAVIOR_FLAGS['4.11']['supports_update_one_sort'])
        self.assertTrue(PYMONGO_PROFILE_BEHAVIOR_FLAGS['4.13']['supports_update_one_sort'])
        self.assertEqual(PYMONGO_PROFILE_BEHAVIOR_FLAGS['4.9'], PYMONGO_PROFILE_49.behavior_flags())
        self.assertEqual(PYMONGO_PROFILE_BEHAVIOR_FLAGS['4.11'], PYMONGO_PROFILE_411.behavior_flags())
        self.assertEqual(PYMONGO_PROFILE_BEHAVIOR_FLAGS['4.13'], PYMONGO_PROFILE_413.behavior_flags())

    def test_dialect_support_helpers_are_derived_from_declarative_catalogs(self):
        self.assertIn('$eq', MongoDialect70().query_field_operators)
        self.assertTrue(MongoDialect70().supports_query_field_operator('$eq'))
        self.assertFalse(MongoDialect70().supports_query_field_operator('$unknown'))

    def test_resolve_mongodb_dialect_uses_baseline_by_default(self):
        self.assertIs(resolve_mongodb_dialect(), MONGODB_DIALECT_70)
        resolution = resolve_mongodb_dialect_resolution()
        self.assertIsInstance(resolution, MongoDialectResolution)
        self.assertIs(resolution.resolved_dialect, MONGODB_DIALECT_70)
        self.assertEqual(resolution.resolution_mode, 'default')

    def test_resolve_mongodb_dialect_supports_explicit_aliases_and_instances(self):
        explicit = MongoDialect80()

        self.assertIs(resolve_mongodb_dialect('8.0'), MONGODB_DIALECT_80)
        self.assertIs(resolve_mongodb_dialect('8'), MONGODB_DIALECT_80)
        self.assertIs(resolve_mongodb_dialect(explicit), explicit)
        alias_resolution = resolve_mongodb_dialect_resolution('8.0')
        self.assertIs(alias_resolution.resolved_dialect, MONGODB_DIALECT_80)
        self.assertEqual(alias_resolution.resolution_mode, 'explicit-alias')
        explicit_resolution = resolve_mongodb_dialect_resolution(explicit)
        self.assertIs(explicit_resolution.resolved_dialect, explicit)
        self.assertEqual(explicit_resolution.resolution_mode, 'explicit-instance')

    def test_resolve_mongodb_dialect_rejects_unsupported_values(self):
        with self.assertRaises(ValueError):
            resolve_mongodb_dialect('9.0')
        with self.assertRaises(ValueError):
            resolve_mongodb_dialect('auto-server')

    def test_resolve_pymongo_profile_uses_baseline_by_default(self):
        self.assertIs(resolve_pymongo_profile(), PYMONGO_PROFILE_49)
        resolution = resolve_pymongo_profile_resolution()
        self.assertIsInstance(resolution, PyMongoProfileResolution)
        self.assertIs(resolution.resolved_profile, PYMONGO_PROFILE_49)
        self.assertEqual(resolution.resolution_mode, 'default')

    def test_resolve_pymongo_profile_supports_explicit_aliases_and_instances(self):
        explicit = PyMongoProfile413()

        self.assertIs(resolve_pymongo_profile('4.11'), PYMONGO_PROFILE_411)
        self.assertIs(resolve_pymongo_profile('4.13'), PYMONGO_PROFILE_413)
        self.assertIs(resolve_pymongo_profile(explicit), explicit)
        alias_resolution = resolve_pymongo_profile_resolution('4.11')
        self.assertIs(alias_resolution.resolved_profile, PYMONGO_PROFILE_411)
        self.assertEqual(alias_resolution.resolution_mode, 'explicit-alias')
        explicit_resolution = resolve_pymongo_profile_resolution(explicit)
        self.assertIs(explicit_resolution.resolved_profile, explicit)
        self.assertEqual(explicit_resolution.resolution_mode, 'explicit-instance')

    def test_resolve_pymongo_profile_rejects_unsupported_values(self):
        with self.assertRaises(ValueError):
            resolve_pymongo_profile('6.0')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='4.10.2')
    def test_detect_installed_pymongo_profile_maps_unknown_minor_to_latest_compatible_profile(self, _version):
        self.assertIs(detect_installed_pymongo_profile(), PYMONGO_PROFILE_49)
        self.assertIs(resolve_pymongo_profile('auto-installed'), PYMONGO_PROFILE_49)
        resolution = detect_installed_pymongo_profile_resolution()
        self.assertIs(resolution.resolved_profile, PYMONGO_PROFILE_49)
        self.assertEqual(resolution.installed_version, '4.10.2')
        self.assertEqual(resolution.resolution_mode, 'auto-compatible-minor-fallback')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='4.8.4')
    def test_detect_installed_pymongo_profile_rejects_older_than_supported_baseline(self, _version):
        with self.assertRaises(ValueError):
            detect_installed_pymongo_profile()
        with self.assertRaises(ValueError):
            resolve_pymongo_profile('auto-installed')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='4.11.3')
    def test_detect_installed_pymongo_profile_maps_411_to_profile_411(self, _version):
        self.assertIs(detect_installed_pymongo_profile(), PYMONGO_PROFILE_411)
        resolution = detect_installed_pymongo_profile_resolution()
        self.assertIs(resolution.resolved_profile, PYMONGO_PROFILE_411)
        self.assertEqual(resolution.resolution_mode, 'auto-exact')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='4.13.1')
    def test_detect_installed_pymongo_profile_maps_413_plus_to_profile_413(self, _version):
        self.assertIs(detect_installed_pymongo_profile(), PYMONGO_PROFILE_413)
        resolution = detect_installed_pymongo_profile_resolution()
        self.assertIs(resolution.resolved_profile, PYMONGO_PROFILE_413)
        self.assertEqual(resolution.resolution_mode, 'auto-exact')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='4.13.0')
    def test_strict_auto_installed_accepts_exact_known_profile(self, _version):
        resolution = resolve_pymongo_profile_resolution('strict-auto-installed')

        self.assertIs(resolution.resolved_profile, PYMONGO_PROFILE_413)
        self.assertEqual(resolution.resolution_mode, 'auto-exact')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='4.14.0')
    def test_auto_installed_falls_back_to_latest_known_profile_in_same_major(self, _version):
        resolution = resolve_pymongo_profile_resolution('auto-installed')

        self.assertIs(resolution.resolved_profile, PYMONGO_PROFILE_413)
        self.assertEqual(resolution.resolution_mode, 'auto-compatible-minor-fallback')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='4.14.0')
    def test_strict_auto_installed_rejects_unknown_minor(self, _version):
        with self.assertRaises(ValueError):
            resolve_pymongo_profile_resolution('strict-auto-installed')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='5.1.0')
    def test_detect_installed_pymongo_profile_rejects_unknown_major_series(self, _version):
        with self.assertRaises(ValueError):
            detect_installed_pymongo_profile()
        with self.assertRaises(ValueError):
            resolve_pymongo_profile_resolution('auto-installed')

    @patch('mongoeco.compat.registry.importlib_metadata.version', return_value='dev-build')
    def test_detect_installed_pymongo_profile_rejects_unparseable_version(self, _version):
        with self.assertRaises(ValueError):
            detect_installed_pymongo_profile()

    @patch(
        'mongoeco.compat.registry.importlib_metadata.version',
        side_effect=__import__('importlib').metadata.PackageNotFoundError,
    )
    def test_auto_installed_profile_requires_pymongo(self, _version):
        with self.assertRaises(ValueError):
            detect_installed_pymongo_profile()
