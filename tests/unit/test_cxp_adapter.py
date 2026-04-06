import unittest
from types import SimpleNamespace

import mongoeco.cxp as mongoeco_cxp
import mongoeco.cxp.capabilities as cxp_capabilities_module
import mongoeco.cxp.integration as cxp_integration_module
from mongoeco.compat import export_cxp_catalog, export_full_compat_catalog
from mongoeco.cxp import (
    EXECUTION_ENGINE_CATALOG,
    EXECUTION_ENGINE_EXECUTION_STATUS,
    EXECUTION_ENGINE_EXECUTION_STREAM,
    EXECUTION_ENGINE_FAMILY_CATALOG,
    EXECUTION_ENGINE_FAMILY_INTERFACE,
    EXECUTION_ENGINE_INTERFACE,
    EXECUTION_ENGINE_INPUT_VALIDATION,
    EXECUTION_ENGINE_PLANNING,
    EXECUTION_ENGINE_RUN,
    MongoAggregationMetadata,
    MongoSearchMetadata,
    MongoVectorSearchMetadata,
    MONGODB_AGGREGATE,
    MONGODB_AGGREGATE_RICH_PROFILE,
    MONGODB_CATALOG,
    MONGODB_CORE_PROFILE,
    MONGODB_INTERFACE,
    MONGODB_PLATFORM_PROFILE,
    MONGODB_TEXT_SEARCH_PROFILE,
    MONGODB_SEARCH_PROFILE,
    PLAN_RUN_EXECUTION_CATALOG,
    PLAN_RUN_EXECUTION_EXECUTION_STATUS,
    PLAN_RUN_EXECUTION_EXECUTION_STREAM,
    PLAN_RUN_EXECUTION_INPUT_VALIDATION,
    PLAN_RUN_EXECUTION_INTERFACE,
    PLAN_RUN_EXECUTION_PLANNING,
    PLAN_RUN_EXECUTION_RUN,
)
from mongoeco.cxp.descriptors import (
    CapabilityDescriptor,
    CapabilityOperationBinding,
    ComponentCapabilitySnapshot,
)
from mongoeco.cxp.types import ComponentIdentity


class CxpAlignmentTests(unittest.TestCase):
    def test_mongoeco_cxp_exposes_canonical_database_mongodb_interface(
        self,
    ) -> None:
        self.assertEqual(MONGODB_CATALOG.interface, MONGODB_INTERFACE)
        self.assertEqual(MONGODB_INTERFACE, 'database/mongodb')
        self.assertEqual(
            MONGODB_CATALOG.capability_operation_names('aggregation'),
            (MONGODB_AGGREGATE,),
        )

    def test_profiles_are_reexported_from_canonical_cxp_catalog(self) -> None:
        self.assertEqual(MONGODB_CORE_PROFILE.interface, MONGODB_INTERFACE)
        self.assertEqual(MONGODB_TEXT_SEARCH_PROFILE.name, 'mongodb-text-search')
        self.assertEqual(MONGODB_SEARCH_PROFILE.name, 'mongodb-search')
        self.assertEqual(MONGODB_PLATFORM_PROFILE.name, 'mongodb-platform')
        self.assertEqual(
            MONGODB_AGGREGATE_RICH_PROFILE.name,
            'mongodb-aggregate-rich',
        )
        self.assertEqual(
            MongoAggregationMetadata.__name__,
            'MongoAggregationMetadata',
        )
        self.assertEqual(MongoSearchMetadata.__name__, 'MongoSearchMetadata')
        self.assertEqual(
            MongoVectorSearchMetadata.__name__,
            'MongoVectorSearchMetadata',
        )

    def test_execution_catalog_surface_is_aligned_with_cxp_2(self) -> None:
        self.assertEqual(EXECUTION_ENGINE_FAMILY_INTERFACE, 'execution/engine')
        self.assertEqual(
            EXECUTION_ENGINE_FAMILY_CATALOG.interface,
            'execution/engine',
        )
        self.assertEqual(
            EXECUTION_ENGINE_INTERFACE,
            'execution/plan-run',
        )
        self.assertEqual(
            EXECUTION_ENGINE_CATALOG.interface,
            'execution/plan-run',
        )
        self.assertEqual(
            PLAN_RUN_EXECUTION_INTERFACE,
            'execution/plan-run',
        )
        self.assertEqual(
            PLAN_RUN_EXECUTION_CATALOG.interface,
            'execution/plan-run',
        )
        self.assertEqual(
            PLAN_RUN_EXECUTION_CATALOG.capability_names(),
            (
                PLAN_RUN_EXECUTION_RUN,
                PLAN_RUN_EXECUTION_PLANNING,
                PLAN_RUN_EXECUTION_INPUT_VALIDATION,
                PLAN_RUN_EXECUTION_EXECUTION_STATUS,
                PLAN_RUN_EXECUTION_EXECUTION_STREAM,
            ),
        )
        self.assertEqual(EXECUTION_ENGINE_RUN, PLAN_RUN_EXECUTION_RUN)
        self.assertEqual(
            EXECUTION_ENGINE_PLANNING,
            PLAN_RUN_EXECUTION_PLANNING,
        )
        self.assertEqual(
            EXECUTION_ENGINE_INPUT_VALIDATION,
            PLAN_RUN_EXECUTION_INPUT_VALIDATION,
        )
        self.assertEqual(
            EXECUTION_ENGINE_EXECUTION_STATUS,
            PLAN_RUN_EXECUTION_EXECUTION_STATUS,
        )
        self.assertEqual(
            EXECUTION_ENGINE_EXECUTION_STREAM,
            PLAN_RUN_EXECUTION_EXECUTION_STREAM,
        )
        self.assertFalse(
            hasattr(mongoeco_cxp, 'EXECUTION_ENGINE_DRAFT_VALIDATION')
        )
        self.assertFalse(
            hasattr(mongoeco_cxp, 'EXECUTION_ENGINE_LIVE_EXECUTION_OBSERVABILITY')
        )

    def test_cxp_capability_exports_and_legacy_runtime_projection_share_a_canonical_source(
        self,
    ) -> None:
        metadata = (
            cxp_capabilities_module.mongoeco_public_cxp_capability_metadata()
        )
        exported = cxp_capabilities_module.export_cxp_capability_catalog()
        legacy = cxp_capabilities_module.export_legacy_runtime_subset_catalog()
        projection = cxp_capabilities_module.build_mongodb_explain_projection(
            capability='aggregation',
            additional_capabilities=('search',),
            metadata={'mode': 'local'},
        )
        platform_projection = (
            cxp_capabilities_module.build_mongodb_explain_projection(
                capability='transactions',
            )
        )
        write_projection = (
            cxp_capabilities_module.build_mongodb_explain_projection(
                capability='write',
            )
        )

        self.assertEqual(exported['interface'], 'database/mongodb')
        self.assertIn('profiles', exported)
        self.assertEqual(
            exported['profiles']['mongodb-text-search']['recommendedFor'],
            ['text-search-tests', 'search-without-vector-search'],
        )
        self.assertEqual(
            exported['profiles']['mongodb-text-search']['requirements'][-1],
            {
                'capabilityName': 'search',
                'requiredOperations': ['aggregate'],
                'requiredMetadataKeys': ['operators', 'aggregateStage'],
            },
        )
        self.assertEqual(
            exported['operations']['find'][0]['capabilityName'],
            'read',
        )
        self.assertEqual(
            exported['operations']['update_one'][0]['compatibleProfiles'],
            ['mongodb-core', 'mongodb-platform'],
        )
        self.assertEqual(
            exported['operations']['aggregate'][1]['capabilityName'],
            'search',
        )
        self.assertTrue(
            exported['profileSupport']['mongodb-text-search']['supported']
        )
        self.assertEqual(
            exported['profileSupport']['mongodb-text-search']['validation'],
            {
                'messages': [],
                'unknownProfileCapabilities': [],
                'missingCapabilities': [],
                'missingOperations': [],
                'missingMetadataKeys': [],
                'invalidMetadata': [],
                'interfaceMismatch': None,
                'expectedInterface': None,
            },
        )
        self.assertEqual(
            exported['capabilities']['aggregation']['operations'][0]['name'],
            MONGODB_AGGREGATE,
        )
        self.assertIn(
            '$group',
            exported['capabilities']['aggregation']['metadata'][
                'supportedStages'
            ],
        )
        self.assertIn(
            'operationMetadata',
            exported['capabilities']['read']['metadata'],
        )
        self.assertTrue(
            exported['capabilities']['read']['metadata']['operationMetadata'][
                'find'
            ]['supportsExplain']
        )
        self.assertEqual(
            exported['capabilities']['read']['metadata']['operationMetadata'][
                'find'
            ]['resultType'],
            'cursor',
        )
        self.assertTrue(
            exported['capabilities']['read']['metadata']['operationMetadata'][
                'find'
            ]['acceptsProjection']
        )
        self.assertTrue(
            exported['capabilities']['write']['metadata']['operationMetadata'][
                'update_one'
            ]['supportsPipelineUpdate']
        )
        self.assertTrue(
            exported['capabilities']['write']['metadata']['operationMetadata'][
                'update_one'
            ]['supportsUpsert']
        )
        self.assertTrue(
            exported['capabilities']['aggregation']['metadata'][
                'operationMetadata'
            ]['aggregate']['supportsDatabaseScope']
        )
        self.assertIn(
            'fieldMappings',
            exported['capabilities']['search']['metadata'],
        )
        self.assertIn(
            'embeddedDocuments',
            exported['capabilities']['search']['metadata']['fieldMappings'],
        )
        self.assertEqual(
            exported['capabilities']['search']['metadata'][
                'structuredFieldMappings'
            ],
            ['document', 'embeddedDocuments'],
        )
        self.assertTrue(
            exported['capabilities']['read']['metadata']['operationMetadata'][
                'find'
            ]['acceptsSort']
        )
        self.assertTrue(
            exported['capabilities']['write']['metadata']['operationMetadata'][
                'update_one'
            ]['acceptsArrayFilters']
        )
        self.assertTrue(
            exported['capabilities']['write']['metadata']['operationMetadata'][
                'bulk_write'
            ]['acceptsOrderedExecution']
        )
        self.assertTrue(
            exported['capabilities']['read']['metadata']['operationMetadata'][
                'find'
            ]['acceptsCollation']
        )
        self.assertTrue(
            exported['capabilities']['write']['metadata']['operationMetadata'][
                'update_one'
            ]['acceptsCollation']
        )
        self.assertEqual(
            exported['capabilities']['search']['metadata']['operationMetadata'][
                'aggregate'
            ]['aggregateStage'],
            '$search',
        )
        self.assertEqual(
            exported['capabilities']['search']['metadata']['operators'],
            metadata['search']['operators'],
        )
        self.assertEqual(
            legacy['vectorSearch']['similarities'],
            metadata['vector_search']['similarities'],
        )
        self.assertEqual(projection['provider'], 'mongoeco')
        self.assertEqual(projection['minimalProfile'], 'mongodb-text-search')
        self.assertEqual(
            projection['compatibleProfiles'],
            ['mongodb-text-search', 'mongodb-search'],
        )
        self.assertEqual(projection['operationName'], 'aggregate')
        self.assertEqual(
            projection['operationMetadata']['aggregateStage'],
            '$search',
        )
        self.assertTrue(
            projection['compatibleProfileSupport']['mongodb-text-search'][
                'supported'
            ]
        )
        self.assertEqual(
            projection['minimalProfileRequirements'][-1],
            {
                'capabilityName': 'search',
                'requiredOperations': ['aggregate'],
                'requiredMetadataKeys': ['operators', 'aggregateStage'],
            },
        )
        self.assertEqual(projection['additionalCapabilities'], ['search'])
        self.assertEqual(projection['metadata'], {'mode': 'local'})
        self.assertEqual(
            write_projection['compatibleProfiles'],
            ['mongodb-core', 'mongodb-platform'],
        )
        self.assertNotIn('operationName', write_projection)
        self.assertNotIn('operationMetadata', write_projection)
        self.assertNotIn('minimalProfile', platform_projection)
        self.assertNotIn('minimalProfileRequirements', platform_projection)
        self.assertNotIn('compatibleProfiles', platform_projection)

    def test_catalog_operation_result_types_reject_inconsistent_catalog_entries(
        self,
    ) -> None:
        conflicting_catalog = SimpleNamespace(
            capabilities=(
                SimpleNamespace(
                    operations=(
                        SimpleNamespace(name='aggregate', result_type='cursor'),
                    ),
                ),
                SimpleNamespace(
                    operations=(
                        SimpleNamespace(
                            name='aggregate',
                            result_type='document',
                        ),
                    ),
                ),
            ),
        )

        with self.assertRaisesRegex(
            ValueError,
            "catalog operation result types must stay consistent",
        ):
            cxp_capabilities_module._catalog_operation_result_types(
                conflicting_catalog,
            )

    def test_profiles_validate_snapshot_requirements_for_reusable_test_gates(
        self,
    ) -> None:
        snapshot = ComponentCapabilitySnapshot(
            component_name='provider',
            identity=ComponentIdentity(
                interface=MONGODB_INTERFACE,
                provider='test-provider',
                version='1.0.0',
            ),
            capabilities=(
                CapabilityDescriptor(
                    name='read',
                    level='supported',
                    operations=(
                        CapabilityOperationBinding('find'),
                        CapabilityOperationBinding('find_one'),
                        CapabilityOperationBinding('count_documents'),
                        CapabilityOperationBinding('estimated_document_count'),
                        CapabilityOperationBinding('distinct'),
                    ),
                ),
                CapabilityDescriptor(
                    name='write',
                    level='supported',
                    operations=(
                        CapabilityOperationBinding('insert_one'),
                        CapabilityOperationBinding('insert_many'),
                        CapabilityOperationBinding('update_one'),
                        CapabilityOperationBinding('update_many'),
                        CapabilityOperationBinding('replace_one'),
                        CapabilityOperationBinding('delete_one'),
                        CapabilityOperationBinding('delete_many'),
                        CapabilityOperationBinding('bulk_write'),
                    ),
                ),
                CapabilityDescriptor(
                    name='aggregation',
                    level='supported',
                    operations=(CapabilityOperationBinding('aggregate'),),
                    metadata={
                        'supportedStages': ['$match', '$group'],
                        'supportedExpressionOperators': ['$add'],
                        'supportedGroupAccumulators': ['$sum'],
                        'supportedWindowAccumulators': ['$sum'],
                    },
                ),
                CapabilityDescriptor(
                    name='search',
                    level='supported',
                    operations=(CapabilityOperationBinding('aggregate'),),
                    metadata={
                        'operators': ['text'],
                        'aggregateStage': '$search',
                    },
                ),
                CapabilityDescriptor(
                    name='vector_search',
                    level='supported',
                    operations=(CapabilityOperationBinding('aggregate'),),
                    metadata={
                        'similarities': ['cosine'],
                        'aggregateStage': '$vectorSearch',
                    },
                ),
                CapabilityDescriptor(
                    name='transactions',
                    level='supported',
                    operations=(
                        CapabilityOperationBinding('start_session'),
                        CapabilityOperationBinding('with_transaction'),
                    ),
                ),
                CapabilityDescriptor(
                    name='change_streams',
                    level='supported',
                    operations=(CapabilityOperationBinding('watch'),),
                ),
                CapabilityDescriptor(
                    name='collation',
                    level='supported',
                    metadata={'backend': {}, 'capabilities': {}},
                ),
                CapabilityDescriptor(
                    name='persistence',
                    level='supported',
                    metadata={'persistent': True, 'storageEngine': 'sqlite'},
                ),
                CapabilityDescriptor(
                    name='topology_discovery',
                    level='supported',
                    metadata={
                        'topologyType': 'single',
                        'serverCount': 1,
                        'sdam': {},
                    },
                ),
            ),
        )

        self.assertTrue(
            MONGODB_CATALOG.is_component_snapshot_profile_compliant(
                snapshot,
                MONGODB_CORE_PROFILE,
            )
        )
        self.assertTrue(
            MONGODB_CATALOG.is_component_snapshot_profile_compliant(
                snapshot,
                MONGODB_TEXT_SEARCH_PROFILE,
            )
        )
        self.assertTrue(
            MONGODB_CATALOG.is_component_snapshot_profile_compliant(
                snapshot,
                MONGODB_SEARCH_PROFILE,
            )
        )
        self.assertTrue(
            MONGODB_CATALOG.is_component_snapshot_profile_compliant(
                snapshot,
                MONGODB_PLATFORM_PROFILE,
            )
        )
        self.assertTrue(
            MONGODB_CATALOG.is_component_snapshot_profile_compliant(
                snapshot,
                MONGODB_AGGREGATE_RICH_PROFILE,
            )
        )

    def test_compat_export_surfaces_operations_and_aggregation_subset_metadata(
        self,
    ) -> None:
        full_catalog = export_full_compat_catalog()
        cxp_catalog = export_cxp_catalog()

        self.assertEqual(full_catalog['cxp'], cxp_catalog)
        self.assertEqual(
            cxp_catalog['capabilities']['aggregation']['operations'][0][
                'name'
            ],
            'aggregate',
        )
        self.assertIn('mongodb-text-search', cxp_catalog['profiles'])
        self.assertIn(
            'supportedStages',
            cxp_catalog['capabilities']['aggregation']['metadata'],
        )

    def test_cxp_module_stays_minimal_while_integration_helpers_remain_available_in_submodule(
        self,
    ) -> None:
        self.assertFalse(
            hasattr(mongoeco_cxp, 'collect_provider_capability_snapshot')
        )
        self.assertTrue(
            hasattr(
                cxp_integration_module, 'collect_provider_capability_snapshot'
            )
        )
        self.assertTrue(
            hasattr(cxp_integration_module, 'negotiate_with_provider')
        )
        self.assertFalse(hasattr(mongoeco_cxp, 'CxpProvider'))
        self.assertFalse(hasattr(mongoeco_cxp, 'AsyncCxpProvider'))
