import ast
from types import MappingProxyType
import math
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from mongoeco.compat import (
    AUTO_INSTALLED_PYMONGO_PROFILE,
    DATABASE_COMMAND_SUPPORT_CATALOG,
    DATABASE_COMMAND_OPTION_SUPPORT_CATALOG,
    DEFAULT_MONGODB_DIALECT,
    DEFAULT_PYMONGO_PROFILE,
    detect_installed_pymongo_profile_resolution,
    MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED,
    MONGODB_DIALECT_HOOK_NAMES,
    MONGODB_DIALECT_70,
    MONGODB_DIALECT_80,
    MONGODB_DIALECT_ALIASES,
    MONGODB_DIALECT_BEHAVIOR_FLAGS,
    MONGODB_DIALECT_CAPABILITIES,
    MONGODB_DIALECT_POLICY_SPECS,
    MONGODB_DIALECTS,
    MongoBehaviorPolicySpec,
    MongoDialect,
    MongoDialect70,
    MongoDialect80,
    MongoDialectResolution,
    PYMONGO_PROFILE_49,
    PYMONGO_PROFILE_411,
    PYMONGO_PROFILE_413,
    PYMONGO_PROFILE_ALIASES,
    PYMONGO_PROFILE_BEHAVIOR_FLAGS,
    PYMONGO_PROFILE_CAPABILITIES,
    PYMONGO_CAP_UPDATE_ONE_SORT,
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
    export_full_compat_catalog,
    export_full_compat_catalog_markdown,
    export_database_command_catalog,
    export_database_command_option_catalog,
    export_cxp_catalog,
    export_mongodb_dialect_catalog,
    export_local_runtime_subset_catalog,
    export_operation_option_catalog,
    export_pymongo_profile_catalog,
)
from mongoeco.compat import _catalog_data as catalog_data
from mongoeco.compat import _catalog_database_commands as catalog_database_commands
from mongoeco.compat import _catalog_dialects as catalog_dialects
from mongoeco.compat import _catalog_operation_options as catalog_operation_options
from mongoeco.compat import _catalog_profiles as catalog_profiles
from mongoeco.compat.base import (
    _compare_values_default,
    _expression_truthy_default,
    _projection_flag_default,
    _sort_update_path_items_default,
    _values_equal_default,
    build_mongo_behavior_policy,
)
from mongoeco.types import Binary, Timestamp


class _FlatComparable:
    def __init__(self, label: str) -> None:
        self.label = label

    def __lt__(self, other: object) -> bool:
        return False

    def __gt__(self, other: object) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        return False


class _TypeErrorComparable:
    def __init__(self, label: str) -> None:
        self.label = label

    def __lt__(self, other: object) -> bool:
        raise TypeError("not ordered")

    def __gt__(self, other: object) -> bool:
        raise TypeError("not ordered")

    def __str__(self) -> str:
        return self.label


class _CustomNaNDialect(MongoDialect):
    @property
    def bson_type_order(self) -> MappingProxyType:
        return MappingProxyType({float: 2, str: 2, _FlatComparable: 99, _TypeErrorComparable: 99})


class _OverrideDialect(MongoDialect):
    def null_query_matches_undefined(self) -> bool:
        return False


class CompatResolutionTests(unittest.TestCase):
    def test_default_behavior_helpers_cover_truthiness_projection_and_update_path_sort(self):
        self.assertFalse(_expression_truthy_default(None))
        self.assertFalse(_expression_truthy_default(False))
        self.assertFalse(_expression_truthy_default(0))
        self.assertTrue(_expression_truthy_default("0"))
        self.assertEqual(_projection_flag_default(True), 1)
        self.assertEqual(_projection_flag_default(False), 0)
        self.assertEqual(_projection_flag_default(1), 1)
        self.assertEqual(_projection_flag_default(0), 0)
        self.assertIsNone(_projection_flag_default(2))
        self.assertEqual(
            _sort_update_path_items_default({"10": "c", "2": "b", "a": "z"}),
            [("2", "b"), ("10", "c"), ("a", "z")],
        )
        with self.assertRaises(TypeError):
            _sort_update_path_items_default({1: "broken"})

    def test_exported_full_catalog_matches_versioned_snapshot_fixture(self):
        snapshot_path = Path("tests/fixtures/compat_catalog_snapshot.json")
        expected = json.loads(snapshot_path.read_text(encoding="utf-8"))

        self.assertEqual(export_full_compat_catalog(), expected)

    def test_catalog_module_is_thin_public_composer(self):
        module_path = Path("src/mongoeco/compat/catalog.py")
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        non_import_nodes = [
            node
            for node in tree.body
            if not isinstance(node, (ast.ImportFrom, ast.Import))
        ]

        self.assertEqual(non_import_nodes, [])

    def test_catalog_data_is_split_by_version_axis(self):
        self.assertIs(catalog_data.DATABASE_COMMAND_SUPPORT_CATALOG, catalog_database_commands.DATABASE_COMMAND_SUPPORT_CATALOG)
        self.assertIs(catalog_data.MONGODB_DIALECT_CATALOG, catalog_dialects.MONGODB_DIALECT_CATALOG)
        self.assertIs(catalog_data.MONGODB_DIALECT_ALIASES, catalog_dialects.MONGODB_DIALECT_ALIASES)
        self.assertIs(catalog_data.SUPPORTED_MONGODB_MAJORS, catalog_dialects.SUPPORTED_MONGODB_MAJORS)
        self.assertIs(catalog_data.PYMONGO_PROFILE_CATALOG, catalog_profiles.PYMONGO_PROFILE_CATALOG)
        self.assertIs(catalog_data.PYMONGO_PROFILE_ALIASES, catalog_profiles.PYMONGO_PROFILE_ALIASES)
        self.assertIs(catalog_data.SUPPORTED_PYMONGO_MAJORS, catalog_profiles.SUPPORTED_PYMONGO_MAJORS)
        self.assertIs(
            catalog_data.OPERATION_OPTION_SUPPORT_CATALOG,
            catalog_operation_options.OPERATION_OPTION_SUPPORT_CATALOG,
        )
        self.assertIs(
            catalog_data.DATABASE_COMMAND_OPTION_SUPPORT_CATALOG,
            catalog_operation_options.DATABASE_COMMAND_OPTION_SUPPORT_CATALOG,
        )

    def test_exported_markdown_catalog_matches_snapshot_fixture(self):
        snapshot_path = Path("tests/fixtures/compat_catalog_snapshot.md")
        expected = snapshot_path.read_text(encoding="utf-8")

        self.assertEqual(export_full_compat_catalog_markdown(), expected)

    def test_exported_cxp_catalog_includes_richer_operation_metadata_and_search_mappings(self):
        catalog = export_cxp_catalog()

        self.assertEqual(
            catalog["capabilities"]["read"]["metadata"]["operationMetadata"][
                "find"
            ]["resultType"],
            "cursor",
        )
        self.assertTrue(
            catalog["capabilities"]["write"]["metadata"]["operationMetadata"][
                "update_one"
            ]["supportsUpsert"]
        )
        self.assertTrue(
            catalog["capabilities"]["aggregation"]["metadata"][
                "operationMetadata"
            ]["aggregate"]["supportsCollectionScope"]
        )
        self.assertIn(
            "document",
            catalog["capabilities"]["search"]["metadata"]["fieldMappings"],
        )
        self.assertTrue(
            catalog["capabilities"]["read"]["metadata"]["operationMetadata"][
                "find"
            ]["acceptsSort"]
        )
        self.assertTrue(
            catalog["capabilities"]["write"]["metadata"]["operationMetadata"][
                "update_one"
            ]["acceptsArrayFilters"]
        )

    def test_catalog_is_exposed_as_immutable_global_data(self):
        self.assertIsInstance(MONGODB_DIALECTS, MappingProxyType)
        self.assertIsInstance(MONGODB_DIALECT_ALIASES, MappingProxyType)
        self.assertIsInstance(MONGODB_DIALECT_CAPABILITIES, MappingProxyType)
        self.assertIsInstance(MONGODB_DIALECT_BEHAVIOR_FLAGS, MappingProxyType)
        self.assertIsInstance(MONGODB_DIALECT_POLICY_SPECS, MappingProxyType)
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
        self.assertEqual(MONGODB_DIALECT_POLICY_SPECS['7.0'], MONGODB_DIALECT_70.policy_spec)
        self.assertEqual(MONGODB_DIALECT_POLICY_SPECS['8.0'], MONGODB_DIALECT_80.policy_spec)
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
        self.assertEqual(
            MONGODB_DIALECT_POLICY_SPECS["7.0"],
            MongoBehaviorPolicySpec(null_query_matches_undefined=True),
        )
        self.assertEqual(
            MONGODB_DIALECT_POLICY_SPECS["8.0"],
            MongoBehaviorPolicySpec(null_query_matches_undefined=False),
        )
        self.assertTrue(MONGODB_DIALECT_BEHAVIOR_FLAGS['7.0']['null_query_matches_undefined'])
        self.assertFalse(MONGODB_DIALECT_BEHAVIOR_FLAGS['8.0']['null_query_matches_undefined'])
        self.assertEqual(MONGODB_DIALECT_CAPABILITIES['7.0'], frozenset({MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED}))
        self.assertEqual(MONGODB_DIALECT_CAPABILITIES['8.0'], frozenset())
        self.assertEqual(MONGODB_DIALECT_BEHAVIOR_FLAGS['7.0'], MONGODB_DIALECT_70.behavior_flags())
        self.assertEqual(MONGODB_DIALECT_BEHAVIOR_FLAGS['8.0'], MONGODB_DIALECT_80.behavior_flags())

    def test_pymongo_profile_flags_capture_first_public_api_delta(self):
        self.assertFalse(PYMONGO_PROFILE_49.supports_update_one_sort())
        self.assertTrue(PYMONGO_PROFILE_411.supports_update_one_sort())
        self.assertTrue(PYMONGO_PROFILE_413.supports_update_one_sort())
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.9'], frozenset())
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.11'], frozenset({PYMONGO_CAP_UPDATE_ONE_SORT}))
        self.assertEqual(PYMONGO_PROFILE_CAPABILITIES['4.13'], frozenset({PYMONGO_CAP_UPDATE_ONE_SORT}))
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
        self.assertTrue(MongoDialect70().behavior_flag('null_query_matches_undefined'))
        self.assertFalse(MongoDialect80().behavior_flag('null_query_matches_undefined'))
        self.assertTrue(MongoDialect70().supports(MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED))
        self.assertFalse(MongoDialect80().supports(MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED))
        self.assertTrue(MongoDialect70().supports_query_top_level_operator("$and"))
        self.assertTrue(MongoDialect70().supports_aggregation_expression_operator("$eq"))
        self.assertTrue(MongoDialect70().supports_aggregation_stage("$match"))
        self.assertTrue(MongoDialect70().supports_group_accumulator("$sum"))
        self.assertTrue(MongoDialect70().supports_window_accumulator("$sum"))
        self.assertFalse(MongoDialect70().supports_query_top_level_operator("$nope"))
        self.assertFalse(MongoDialect70().supports_aggregation_expression_operator("$nope"))
        self.assertFalse(MongoDialect70().supports_aggregation_stage("$nope"))
        self.assertFalse(MongoDialect70().supports_group_accumulator("$nope"))
        self.assertFalse(MongoDialect70().supports_window_accumulator("$nope"))
        self.assertEqual(
            export_mongodb_dialect_catalog()["7.0"]["policy_spec"],
            {
                "null_query_matches_undefined": True,
                "expression_truthiness": "mongo-default",
                "projection_flag_mode": "bool-or-binary-int",
                "update_path_sort_mode": "numeric-then-lex",
                "equality_mode": "bson-structural",
                "comparison_mode": "bson-total-order",
            },
        )

    def test_base_dialect_can_override_operator_catalogs_declaratively(self):
        dialect = MongoDialect(
            key="test",
            server_version="test",
            label="Test",
            catalog_query_field_operators=frozenset({"$eq"}),
            catalog_update_operators=frozenset({"$set"}),
        )

        self.assertEqual(dialect.query_field_operators, frozenset({"$eq"}))
        self.assertEqual(dialect.update_operators, frozenset({"$set"}))
        self.assertTrue(dialect.supports_query_field_operator("$eq"))
        self.assertFalse(dialect.supports_query_field_operator("$in"))
        self.assertTrue(dialect.supports_update_operator("$set"))
        self.assertFalse(dialect.supports_update_operator("$inc"))

    def test_dialect_policy_caches_base_policy_and_uses_override_policy_when_needed(self):
        dialect = MongoDialect(key="test", server_version="test", label="Test")

        self.assertIs(dialect.base_policy, dialect.base_policy)
        self.assertIs(dialect.policy, dialect.policy)
        self.assertIs(dialect.policy, dialect.base_policy)

        override = _OverrideDialect(key="override", server_version="test", label="Override")
        self.assertIs(override.base_policy, override.base_policy)
        self.assertIs(override.policy, override.policy)
        self.assertIsNot(override.policy, override.base_policy)
        self.assertFalse(override.policy.null_query_matches_undefined())

    def test_dialect_behavior_flag_defaults_and_profile_support_helpers(self):
        dialect = MongoDialect(key="test", server_version="test", label="Test")
        profile = PyMongoProfile49()

        self.assertIsNone(dialect.behavior_flag("unknown"))
        self.assertTrue(dialect.behavior_flag("unknown", True))
        self.assertEqual(profile.behavior_flag("unknown", False), False)
        self.assertFalse(profile.supports_update_one_sort())
        self.assertFalse(profile.supports("nope"))
        self.assertEqual(profile.capabilities, frozenset())
        self.assertEqual(profile.behavior_flags(), PYMONGO_PROFILE_BEHAVIOR_FLAGS["4.9"])

    def test_exported_catalog_matches_public_runtime_catalogs(self):
        mongodb_catalog = export_mongodb_dialect_catalog()
        pymongo_catalog = export_pymongo_profile_catalog()
        database_command_catalog = export_database_command_catalog()
        operation_catalog = export_operation_option_catalog()
        database_command_option_catalog = export_database_command_option_catalog()
        runtime_subset_catalog = export_local_runtime_subset_catalog()
        cxp_catalog = export_cxp_catalog()
        full_catalog = export_full_compat_catalog()

        self.assertEqual(set(mongodb_catalog), set(MONGODB_DIALECTS))
        self.assertEqual(set(pymongo_catalog), set(PYMONGO_PROFILES))
        self.assertEqual(full_catalog['defaults']['mongodb_dialect'], DEFAULT_MONGODB_DIALECT)
        self.assertEqual(full_catalog['defaults']['pymongo_profile'], DEFAULT_PYMONGO_PROFILE)
        self.assertEqual(full_catalog['mongodb_dialects'], mongodb_catalog)
        self.assertEqual(full_catalog['pymongo_profiles'], pymongo_catalog)
        self.assertEqual(full_catalog['database_commands'], database_command_catalog)
        self.assertEqual(full_catalog['operation_options'], operation_catalog)
        self.assertEqual(full_catalog['database_command_options'], database_command_option_catalog)
        self.assertEqual(full_catalog['cxp'], cxp_catalog)
        self.assertEqual(full_catalog['local_runtime_subsets'], runtime_subset_catalog)
        self.assertEqual(full_catalog['cxp']['interface'], 'database/mongodb')
        self.assertIn('search', full_catalog['cxp']['capabilities'])
        self.assertEqual(
            full_catalog['cxp']['capabilities']['aggregation']['operations'][0]['name'],
            'aggregate',
        )
        self.assertIn(
            '$group',
            full_catalog['cxp']['capabilities']['aggregation']['metadata']['supportedStages'],
        )
        self.assertIn('query_field_operators', mongodb_catalog['7.0'])
        self.assertIn('behavior_flags', mongodb_catalog['7.0'])
        self.assertIn('behavior_flags', pymongo_catalog['4.9'])
        self.assertFalse(mongodb_catalog['8.0']['behavior_flags']['null_query_matches_undefined'])
        self.assertTrue(pymongo_catalog['4.11']['behavior_flags']['supports_update_one_sort'])
        self.assertEqual(
            operation_catalog['find']['hint']['status'],
            'effective',
        )
        self.assertEqual(
            database_command_option_catalog['find']['batchSize']['status'],
            'effective',
        )
        self.assertEqual(
            database_command_option_catalog['listCollections']['authorizedCollections']['status'],
            'effective',
        )
        self.assertTrue(PYMONGO_PROFILE_411.supports(PYMONGO_CAP_UPDATE_ONE_SORT))
        self.assertFalse(PYMONGO_PROFILE_49.supports(PYMONGO_CAP_UPDATE_ONE_SORT))

    def test_database_command_option_catalog_tracks_supported_command_surface(self):
        exported = export_database_command_option_catalog()

        self.assertEqual(set(exported), set(DATABASE_COMMAND_OPTION_SUPPORT_CATALOG))
        self.assertEqual(
            exported["findAndModify"]["arrayFilters"]["status"],
            "effective",
        )
        self.assertEqual(
            exported["validate"]["comment"]["status"],
            "effective",
        )

    def test_database_command_catalog_tracks_supported_command_families(self):
        exported = export_database_command_catalog()

        self.assertEqual(set(exported), set(DATABASE_COMMAND_SUPPORT_CATALOG))
        self.assertEqual(exported["find"]["family"], "admin_read")
        self.assertTrue(exported["find"]["supports_explain"])
        self.assertTrue(exported["find"]["supports_comment"])
        self.assertIn("comment", exported["find"]["supported_options"])
        self.assertEqual(exported["dbHash"]["family"], "admin_introspection")
        self.assertTrue(exported["find"]["supports_wire"])
        self.assertFalse(exported["listCommands"]["supports_explain"])
        self.assertFalse(exported["listCommands"]["supports_comment"])
        self.assertEqual(exported["profile"]["family"], "admin_control")

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

    def test_build_mongo_behavior_policy_rejects_unsupported_strategies(self):
        with self.assertRaisesRegex(ValueError, "expression truthiness"):
            build_mongo_behavior_policy(
                MongoBehaviorPolicySpec(expression_truthiness="custom")
            )
        with self.assertRaisesRegex(ValueError, "projection flag"):
            build_mongo_behavior_policy(
                MongoBehaviorPolicySpec(projection_flag_mode="custom")
            )
        with self.assertRaisesRegex(ValueError, "update path sort"):
            build_mongo_behavior_policy(
                MongoBehaviorPolicySpec(update_path_sort_mode="custom")
            )
        with self.assertRaisesRegex(ValueError, "equality strategy"):
            build_mongo_behavior_policy(
                MongoBehaviorPolicySpec(equality_mode="custom")
            )
        with self.assertRaisesRegex(ValueError, "comparison strategy"):
            build_mongo_behavior_policy(
                MongoBehaviorPolicySpec(comparison_mode="custom")
            )

    def test_compare_values_default_covers_binary_timestamp_and_nan_paths(self):
        bson_type_order = MappingProxyType({Binary: 5, Timestamp: 10, float: 2})

        self.assertEqual(
            _compare_values_default(
                Binary(b"a", subtype=0),
                Binary(b"a", subtype=0),
                bson_type_order,
            ),
            0,
        )
        self.assertLess(
            _compare_values_default(
                Binary(b"a", subtype=0),
                Binary(b"a", subtype=1),
                bson_type_order,
            ),
            0,
        )
        self.assertLess(
            _compare_values_default(
                Binary(b"a", subtype=0),
                Binary(b"b", subtype=0),
                bson_type_order,
            ),
            0,
        )
        self.assertEqual(
            _compare_values_default(
                Timestamp(1, 2),
                Timestamp(1, 2),
                bson_type_order,
            ),
            0,
        )
        self.assertLess(
            _compare_values_default(
                Timestamp(1, 2),
                Timestamp(2, 0),
                bson_type_order,
            ),
            0,
        )
        self.assertEqual(
            _compare_values_default(float("nan"), float("nan"), bson_type_order),
            0,
        )
        self.assertLess(
            _compare_values_default(float("nan"), 1.0, bson_type_order),
            0,
        )
        self.assertGreater(
            _compare_values_default(1.0, float("nan"), bson_type_order),
            0,
        )

    def test_compare_values_and_values_equal_cover_structural_paths_and_fallbacks(self):
        dialect = MongoDialect70()

        self.assertFalse(dialect.values_equal({"a": 1}, {"a": 1, "b": 2}))
        self.assertFalse(dialect.values_equal([1], [1, 2]))
        self.assertLess(dialect.compare_values({"a": 1}, {"b": 1}), 0)
        self.assertLess(dialect.compare_values({"a": 1}, {"a": 2}), 0)
        self.assertEqual(dialect.compare_values({"a": 1}, {"a": 1}), 0)
        self.assertLess(dialect.compare_values([1, 2], [1, 3]), 0)
        self.assertEqual(dialect.compare_values([1, 2], [1, 2]), 0)
        self.assertEqual(dialect.compare_values(float("nan"), float("nan")), 0)
        self.assertEqual(
            dialect.compare_values(complex(1, 2), complex(1, 2)),
            0,
        )

    def test_compare_values_and_values_equal_cover_remaining_length_nan_and_fallback_paths(self):
        bson_type_order = MappingProxyType({_FlatComparable: 99, _TypeErrorComparable: 99, float: 2})
        dialect = MongoDialect70()

        self.assertFalse(_values_equal_default({"a": 1}, {}, bson_type_order))
        self.assertEqual(_compare_values_default([1, 2], [1], bson_type_order), 1)
        self.assertEqual(_compare_values_default(_FlatComparable("x"), _FlatComparable("y"), bson_type_order), 0)
        self.assertEqual(
            _compare_values_default(
                _TypeErrorComparable("same"),
                _TypeErrorComparable("same"),
                bson_type_order,
            ),
            0,
        )
        nan_mixed_order = MappingProxyType({float: 2, str: 2})
        self.assertEqual(_compare_values_default(float("nan"), "x", nan_mixed_order), -1)
        self.assertEqual(_compare_values_default("x", float("nan"), nan_mixed_order), 1)

        self.assertTrue(dialect.values_equal([1, 2], [1, 2]))
        self.assertEqual(dialect.compare_values({"a": 1}, {"a": 1, "b": 2}), -1)
        self.assertEqual(dialect.compare_values([1, 2], [1]), 1)
        self.assertEqual(dialect.compare_values(1.0, float("nan")), 1)
        self.assertEqual(
            dialect.compare_values(
                _TypeErrorComparable("same"),
                _TypeErrorComparable("same"),
            ),
            0,
        )

        custom_dialect = _CustomNaNDialect(key="test", server_version="test", label="test")
        self.assertEqual(custom_dialect.compare_values(float("nan"), "x"), -1)
        self.assertEqual(custom_dialect.compare_values("x", float("nan")), 1)
        self.assertEqual(
            custom_dialect.compare_values(_FlatComparable("x"), _FlatComparable("y")),
            0,
        )

    def test_dialect_and_profile_behavior_flags_return_defaults_for_unknown_names(self):
        self.assertIsNone(MongoDialect70().behavior_flag("unknown"))
        self.assertEqual(MongoDialect70().behavior_flag("unknown", default=True), True)
        self.assertIsNone(PYMONGO_PROFILE_49.behavior_flag("unknown"))
        self.assertEqual(PYMONGO_PROFILE_49.behavior_flag("unknown", default=False), False)

    def test_dialect_respects_invalid_policy_modes_in_public_methods(self):
        dialect = MongoDialect(
            key="test",
            server_version="test",
            label="Test",
            catalog_policy_spec=MongoBehaviorPolicySpec(equality_mode="custom"),
        )
        with self.assertRaisesRegex(ValueError, "equality strategy"):
            dialect.values_equal(1, 1)

        dialect = MongoDialect(
            key="test",
            server_version="test",
            label="Test",
            catalog_policy_spec=MongoBehaviorPolicySpec(comparison_mode="custom"),
        )
        with self.assertRaisesRegex(ValueError, "comparison strategy"):
            dialect.compare_values(1, 1)

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
