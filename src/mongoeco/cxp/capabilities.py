from __future__ import annotations

from copy import deepcopy

import msgspec

from cxp.capabilities import Capability, CapabilityMatrix, CapabilityMetadata
from cxp.catalogs.base import CapabilityCatalog, CapabilityProfile
from cxp.descriptors import (
    CapabilityDescriptor,
    CapabilityOperationBinding,
    ComponentCapabilitySnapshot,
)
from cxp.types import ComponentIdentity
from mongoeco.compat._catalog_data import (
    OPERATION_OPTION_SUPPORT_CATALOG,
    SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
    SUPPORTED_AGGREGATION_STAGES,
    SUPPORTED_GROUP_ACCUMULATORS,
    SUPPORTED_QUERY_FIELD_OPERATORS,
    SUPPORTED_QUERY_TOP_LEVEL_OPERATORS,
    SUPPORTED_UPDATE_OPERATORS,
    SUPPORTED_WINDOW_ACCUMULATORS,
)
from mongoeco.compat._catalog_models import OptionSupportStatus
from mongoeco.cxp.catalogs.interfaces.database.mongodb import (
    MongoAggregationMetadata,
    MongoCollationMetadata,
    MongoPersistenceMetadata,
    MongoSearchMetadata,
    MongoTopologyDiscoveryMetadata,
    MongoVectorSearchMetadata,
    MONGODB_AGGREGATE_RICH_PROFILE,
    MONGODB_AGGREGATE_RICH_PROFILE_NAME,
    MONGODB_AGGREGATION,
    MONGODB_AGGREGATE,
    MONGODB_BULK_WRITE,
    MONGODB_CATALOG,
    MONGODB_CHANGE_STREAMS,
    MONGODB_COLLATION,
    MONGODB_COUNT_DOCUMENTS,
    MONGODB_CORE_PROFILE,
    MONGODB_CORE_PROFILE_NAME,
    MONGODB_DELETE_MANY,
    MONGODB_DELETE_ONE,
    MONGODB_DISTINCT,
    MONGODB_ESTIMATED_DOCUMENT_COUNT,
    MONGODB_FIND,
    MONGODB_FIND_ONE,
    MONGODB_INTERFACE,
    MONGODB_INSERT_MANY,
    MONGODB_INSERT_ONE,
    MONGODB_PERSISTENCE,
    MONGODB_PLATFORM_PROFILE,
    MONGODB_PLATFORM_PROFILE_NAME,
    MONGODB_READ,
    MONGODB_REPLACE_ONE,
    MONGODB_SEARCH,
    MONGODB_SEARCH_PROFILE,
    MONGODB_SEARCH_PROFILE_NAME,
    MONGODB_TEXT_SEARCH_PROFILE,
    MONGODB_TEXT_SEARCH_PROFILE_NAME,
    MONGODB_TOPOLOGY_DISCOVERY,
    MONGODB_TRANSACTIONS,
    MONGODB_UPDATE_MANY,
    MONGODB_UPDATE_ONE,
    MONGODB_VECTOR_SEARCH,
    MONGODB_WRITE,
)
from mongoeco.core.search import (
    EXACT_FILTER_SEARCH_FIELD_MAPPING_TYPES,
    STRUCTURED_SEARCH_FIELD_MAPPING_TYPES,
    SUPPORTED_SEARCH_FIELD_MAPPING_TYPES,
    TEXTUAL_SEARCH_FIELD_MAPPING_TYPES,
)

__all__ = (
    'Capability',
    'CapabilityMatrix',
    'CapabilityMetadata',
    'build_mongodb_explain_projection',
    'export_cxp_capability_catalog',
    'export_cxp_extension_catalog',
    'export_cxp_operation_catalog',
    'export_cxp_profile_catalog',
    'export_cxp_profile_support_catalog',
    'export_legacy_runtime_subset_catalog',
    'mongoeco_public_cxp_capability_metadata',
)


def _operation_option_metadata(
    operation_name: str,
    *,
    result_type: str | None = None,
    collection_scoped: bool | None = None,
    supports_collection_scope: bool | None = None,
    supports_database_scope: bool | None = None,
    supports_session: bool | None = None,
    supports_explain: bool = False,
    accepts_filter: bool | None = None,
    accepts_projection: bool | None = None,
    accepts_field_path: bool | None = None,
    accepts_document: bool | None = None,
    accepts_documents: bool | None = None,
    accepts_update_document: bool | None = None,
    accepts_replacement_document: bool | None = None,
    accepts_write_models: bool | None = None,
    accepts_pipeline: bool | None = None,
    supports_ordered_execution: bool | None = None,
    supports_upsert: bool | None = None,
    extra_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    supported_options: list[str] = []
    accepted_noop_options: list[str] = []
    unsupported_options: list[str] = []
    for option_name, support in OPERATION_OPTION_SUPPORT_CATALOG.get(operation_name, {}).items():
        if support.status is OptionSupportStatus.EFFECTIVE:
            supported_options.append(option_name)
        elif support.status is OptionSupportStatus.ACCEPTED_NOOP:
            accepted_noop_options.append(option_name)
        else:
            unsupported_options.append(option_name)
    metadata: dict[str, object] = {
        'supportedOptions': sorted(supported_options),
        'acceptedNoopOptions': sorted(accepted_noop_options),
        'unsupportedOptions': sorted(unsupported_options),
        'supportsExplain': supports_explain,
    }
    if result_type is not None:
        metadata['resultType'] = result_type
    if collection_scoped is not None:
        metadata['collectionScoped'] = collection_scoped
    if supports_collection_scope is not None:
        metadata['supportsCollectionScope'] = supports_collection_scope
    if supports_database_scope is not None:
        metadata['supportsDatabaseScope'] = supports_database_scope
    if supports_session is not None:
        metadata['supportsSession'] = supports_session
    if accepts_filter is not None:
        metadata['acceptsFilter'] = accepts_filter
    if accepts_projection is not None:
        metadata['acceptsProjection'] = accepts_projection
    if accepts_field_path is not None:
        metadata['acceptsFieldPath'] = accepts_field_path
    if accepts_document is not None:
        metadata['acceptsDocument'] = accepts_document
    if accepts_documents is not None:
        metadata['acceptsDocuments'] = accepts_documents
    if accepts_update_document is not None:
        metadata['acceptsUpdateDocument'] = accepts_update_document
    if accepts_replacement_document is not None:
        metadata['acceptsReplacementDocument'] = accepts_replacement_document
    if accepts_write_models is not None:
        metadata['acceptsWriteModels'] = accepts_write_models
    if accepts_pipeline is not None:
        metadata['acceptsPipeline'] = accepts_pipeline
    if supports_ordered_execution is not None:
        metadata['supportsOrderedExecution'] = supports_ordered_execution
    if supports_upsert is not None:
        metadata['supportsUpsert'] = supports_upsert
    if extra_metadata:
        metadata.update(extra_metadata)
    return metadata


def _input_shape_metadata(
    *,
    accepts_sort: bool | None = None,
    accepts_skip: bool | None = None,
    accepts_limit: bool | None = None,
    accepts_batch_size: bool | None = None,
    accepts_hint: bool | None = None,
    accepts_comment: bool | None = None,
    accepts_max_time_ms: bool | None = None,
    accepts_let: bool | None = None,
    accepts_collation: bool | None = None,
    accepts_array_filters: bool | None = None,
    accepts_ordered_execution: bool | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {}
    if accepts_sort is not None:
        metadata['acceptsSort'] = accepts_sort
    if accepts_skip is not None:
        metadata['acceptsSkip'] = accepts_skip
    if accepts_limit is not None:
        metadata['acceptsLimit'] = accepts_limit
    if accepts_batch_size is not None:
        metadata['acceptsBatchSize'] = accepts_batch_size
    if accepts_hint is not None:
        metadata['acceptsHint'] = accepts_hint
    if accepts_comment is not None:
        metadata['acceptsComment'] = accepts_comment
    if accepts_max_time_ms is not None:
        metadata['acceptsMaxTimeMs'] = accepts_max_time_ms
    if accepts_let is not None:
        metadata['acceptsLet'] = accepts_let
    if accepts_collation is not None:
        metadata['acceptsCollation'] = accepts_collation
    if accepts_array_filters is not None:
        metadata['acceptsArrayFilters'] = accepts_array_filters
    if accepts_ordered_execution is not None:
        metadata['acceptsOrderedExecution'] = accepts_ordered_execution
    return metadata


def _catalog_operation_result_types(catalog: CapabilityCatalog) -> dict[str, str]:
    operation_result_types: dict[str, str] = {}
    for catalog_capability in catalog.capabilities:
        for catalog_operation in catalog_capability.operations:
            existing = operation_result_types.get(catalog_operation.name)
            if existing is not None and existing != catalog_operation.result_type:
                message = (
                    'catalog operation result types must stay consistent '
                    f"across capabilities for {catalog_operation.name!r}"
                )
                raise ValueError(message)
            operation_result_types[catalog_operation.name] = (
                catalog_operation.result_type
            )
    return operation_result_types


_OPERATION_RESULT_TYPES = _catalog_operation_result_types(MONGODB_CATALOG)


def _mongoeco_collation_metadata() -> dict[str, object]:
    metadata = MongoCollationMetadata(
        backend={
            'selectedBackend': 'pyuca',
            'availableBackends': ['pyuca'],
            'unicodeAvailable': True,
            'advancedOptionsAvailable': False,
        },
        capabilities={
            'supportedLocales': ['simple', 'en'],
            'supportedStrengths': [1, 2, 3],
            'supportsCaseLevel': True,
            'supportsNumericOrdering': True,
            'optionalIcuBackend': True,
            'fallbackBackend': 'pyuca',
            'advancedOptionsRequireIcu': [
                'backwards',
                'alternate',
                'maxVariable',
                'normalization',
            ],
        },
    )
    return msgspec.to_builtins(metadata)


def _mongoeco_collation_operation_metadata() -> dict[str, dict[str, object]]:
    return {
        MONGODB_FIND: {
            'supportsCollation': True,
            'scope': 'collection-query',
            'behavior': 'filter-and-sort',
        },
        MONGODB_FIND_ONE: {
            'supportsCollation': True,
            'scope': 'collection-query',
            'behavior': 'filter',
        },
        MONGODB_COUNT_DOCUMENTS: {
            'supportsCollation': True,
            'scope': 'collection-query',
            'behavior': 'filter',
        },
        MONGODB_DISTINCT: {
            'supportsCollation': True,
            'scope': 'collection-query',
            'behavior': 'dedup',
        },
        MONGODB_UPDATE_ONE: {
            'supportsCollation': True,
            'scope': 'collection-write',
            'behavior': 'filter-match',
        },
        MONGODB_UPDATE_MANY: {
            'supportsCollation': True,
            'scope': 'collection-write',
            'behavior': 'filter-match',
        },
        MONGODB_REPLACE_ONE: {
            'supportsCollation': True,
            'scope': 'collection-write',
            'behavior': 'filter-match',
        },
        MONGODB_DELETE_ONE: {
            'supportsCollation': True,
            'scope': 'collection-write',
            'behavior': 'filter-match',
        },
        MONGODB_DELETE_MANY: {
            'supportsCollation': True,
            'scope': 'collection-write',
            'behavior': 'filter-match',
        },
        'serverStatus': {
            'supportsCapabilityInspection': True,
            'inspectionSurface': 'database.command',
            'metadataPath': 'mongoeco.collation',
        },
    }


def _mongoeco_persistence_metadata() -> dict[str, object]:
    metadata = MongoPersistenceMetadata(
        persistent=True,
        storageEngine='runtime-dependent',
    )
    return msgspec.to_builtins(metadata)


def _mongoeco_persistence_operation_metadata() -> dict[str, dict[str, object]]:
    return {
        'serverStatus': {
            'supportsCapabilityInspection': True,
            'inspectionSurface': 'database.command',
            'metadataPaths': [
                'storageEngine.name',
                'mongoeco.engineRuntime',
            ],
        },
        'listDatabases': {
            'supportsCapabilityInspection': True,
            'inspectionSurface': 'database.command',
            'behavior': 'list-visible-databases',
        },
    }


def _mongoeco_topology_discovery_metadata() -> dict[str, object]:
    metadata = MongoTopologyDiscoveryMetadata(
        topologyType='unknown',
        serverCount=1,
        sdam={
            'fullSdam': False,
            'topologyVersionAware': True,
            'helloMemberDiscovery': True,
            'serverHealthTracking': True,
            'electionMetadataAware': True,
            'longPollingHello': False,
            'distributedMonitoring': False,
        },
    )
    return msgspec.to_builtins(metadata)


def _mongoeco_topology_discovery_operation_metadata() -> dict[str, dict[str, object]]:
    return {
        'hello': {
            'supportsCapabilityInspection': True,
            'inspectionSurface': 'database.command',
            'metadataPaths': [
                'helloOk',
                'isWritablePrimary',
                'maxWireVersion',
            ],
        },
        'isMaster': {
            'supportsCapabilityInspection': True,
            'inspectionSurface': 'database.command',
            'legacyAliasOf': 'hello',
        },
        'serverStatus': {
            'supportsCapabilityInspection': True,
            'inspectionSurface': 'database.command',
            'metadataPath': 'mongoeco.sdam',
        },
        'sdam_capabilities': {
            'supportsCapabilityInspection': True,
            'inspectionSurface': 'mongoeco.client',
            'metadataPath': 'sdam_capabilities()',
        },
    }


def _serialize_profile(profile: CapabilityProfile, *, recommended_for: list[str]) -> dict[str, object]:
    return {
        'name': profile.name,
        'description': profile.description,
        'recommendedFor': list(recommended_for),
        'requirements': [
            {
                'capabilityName': requirement.capability_name,
                'requiredOperations': list(requirement.required_operations),
                'requiredMetadataKeys': list(requirement.required_metadata_keys),
            }
            for requirement in profile.requirements
        ],
    }


def _serialize_telemetry_field_requirement(requirement: object) -> dict[str, object]:
    return {
        'name': str(getattr(requirement, 'name')),
        'description': getattr(requirement, 'description'),
    }


def _serialize_capability_telemetry(catalog_capability: object | None) -> dict[str, object]:
    if catalog_capability is None:
        return {
            'spans': [],
            'metrics': [],
            'events': [],
        }
    telemetry = getattr(catalog_capability, 'telemetry', None)
    if telemetry is None:
        return {
            'spans': [],
            'metrics': [],
            'events': [],
        }
    return {
        'description': getattr(telemetry, 'description', None),
        'spans': [
            {
                'name': str(getattr(span, 'name')),
                'description': getattr(span, 'description'),
                'requiredAttributes': [
                    _serialize_telemetry_field_requirement(requirement)
                    for requirement in getattr(span, 'required_attributes', ())
                ],
            }
            for span in getattr(telemetry, 'spans', ())
        ],
        'metrics': [
            {
                'name': str(getattr(metric, 'name')),
                'unit': str(getattr(metric, 'unit')),
                'description': getattr(metric, 'description'),
                'requiredLabels': [
                    _serialize_telemetry_field_requirement(requirement)
                    for requirement in getattr(metric, 'required_labels', ())
                ],
            }
            for metric in getattr(telemetry, 'metrics', ())
        ],
        'events': [
            {
                'eventType': str(getattr(event, 'event_type')),
                'severity': getattr(event, 'severity'),
                'description': getattr(event, 'description'),
                'requiredPayloadKeys': [
                    _serialize_telemetry_field_requirement(requirement)
                    for requirement in getattr(event, 'required_payload_keys', ())
                ],
            }
            for event in getattr(telemetry, 'events', ())
        ],
    }


def _exported_profiles() -> dict[str, dict[str, object]]:
    return {
        MONGODB_CORE_PROFILE_NAME: _serialize_profile(
            MONGODB_CORE_PROFILE,
            recommended_for=['core-tests', 'general-resources'],
        ),
        MONGODB_TEXT_SEARCH_PROFILE_NAME: _serialize_profile(
            MONGODB_TEXT_SEARCH_PROFILE,
            recommended_for=[
                'text-search-tests',
                'search-without-vector-search',
            ],
        ),
        MONGODB_SEARCH_PROFILE_NAME: _serialize_profile(
            MONGODB_SEARCH_PROFILE,
            recommended_for=[
                'full-search-tests',
                'search-with-vector-search',
            ],
        ),
        MONGODB_PLATFORM_PROFILE_NAME: _serialize_profile(
            MONGODB_PLATFORM_PROFILE,
            recommended_for=['platform-tests', 'runtime-conformance'],
        ),
        MONGODB_AGGREGATE_RICH_PROFILE_NAME: _serialize_profile(
            MONGODB_AGGREGATE_RICH_PROFILE,
            recommended_for=[
                'aggregation-rich-tests',
                'subset-sensitive-tooling',
            ],
        ),
    }


def _build_public_cxp_component_snapshot() -> ComponentCapabilitySnapshot:
    descriptors: list[CapabilityDescriptor] = []
    for catalog_capability in MONGODB_CATALOG.capabilities:
        descriptors.append(
            CapabilityDescriptor(
                name=catalog_capability.name,
                level='supported',
                operations=tuple(
                    CapabilityOperationBinding(
                        operation.name,
                        result_type=operation.result_type,
                    )
                    for operation in catalog_capability.operations
                ),
                metadata=deepcopy(
                    _MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA.get(
                        catalog_capability.name,
                        {},
                    ),
                ),
            )
        )
    return ComponentCapabilitySnapshot(
        component_name='mongoeco',
        identity=ComponentIdentity(
            interface=MONGODB_INTERFACE,
            provider='mongoeco',
            version='public-catalog',
        ),
        capabilities=tuple(descriptors),
    )


def _serialize_profile_validation(validation: object) -> dict[str, object]:
    missing_operations = getattr(validation, 'missing_operations', ())
    return {
        'messages': list(validation.messages()),
        'unknownProfileCapabilities': list(
            getattr(validation, 'unknown_profile_capabilities', ())
        ),
        'missingCapabilities': list(
            getattr(validation, 'missing_capabilities', ())
        ),
        'missingOperations': [
            {
                'capabilityName': missing.capability_name,
                'operationNames': list(missing.operation_names),
            }
            for missing in missing_operations
        ],
        'missingMetadataKeys': list(
            getattr(validation, 'missing_metadata_keys', ())
        ),
        'invalidMetadata': list(getattr(validation, 'invalid_metadata', ())),
        'interfaceMismatch': getattr(validation, 'interface_mismatch', None),
        'expectedInterface': getattr(validation, 'expected_interface', None),
    }


def _exported_profile_support() -> dict[str, dict[str, object]]:
    snapshot = _build_public_cxp_component_snapshot()
    exported_profiles = _exported_profiles()
    profile_entries = (
        (MONGODB_CORE_PROFILE_NAME, MONGODB_CORE_PROFILE),
        (MONGODB_TEXT_SEARCH_PROFILE_NAME, MONGODB_TEXT_SEARCH_PROFILE),
        (MONGODB_SEARCH_PROFILE_NAME, MONGODB_SEARCH_PROFILE),
        (MONGODB_PLATFORM_PROFILE_NAME, MONGODB_PLATFORM_PROFILE),
        (MONGODB_AGGREGATE_RICH_PROFILE_NAME, MONGODB_AGGREGATE_RICH_PROFILE),
    )
    support: dict[str, dict[str, object]] = {}
    for profile_name, profile in profile_entries:
        validation = MONGODB_CATALOG.validate_component_snapshot_against_profile(
            snapshot,
            profile,
        )
        support[profile_name] = {
            **deepcopy(exported_profiles[profile_name]),
            'supported': validation.is_valid(),
            'validation': _serialize_profile_validation(validation),
        }
    return support


def _compatible_profile_names_for_capability(capability_name: str) -> list[str]:
    if capability_name == MONGODB_READ:
        return [MONGODB_CORE_PROFILE_NAME, MONGODB_PLATFORM_PROFILE_NAME]
    if capability_name == MONGODB_WRITE:
        return [MONGODB_CORE_PROFILE_NAME, MONGODB_PLATFORM_PROFILE_NAME]
    if capability_name == MONGODB_AGGREGATION:
        return [
            MONGODB_CORE_PROFILE_NAME,
            MONGODB_PLATFORM_PROFILE_NAME,
            MONGODB_AGGREGATE_RICH_PROFILE_NAME,
        ]
    if capability_name == MONGODB_SEARCH:
        return [MONGODB_TEXT_SEARCH_PROFILE_NAME, MONGODB_SEARCH_PROFILE_NAME]
    if capability_name == MONGODB_VECTOR_SEARCH:
        return [MONGODB_SEARCH_PROFILE_NAME]
    if capability_name == MONGODB_COLLATION:
        return [MONGODB_PLATFORM_PROFILE_NAME]
    if capability_name == MONGODB_PERSISTENCE:
        return [MONGODB_PLATFORM_PROFILE_NAME]
    if capability_name == MONGODB_TOPOLOGY_DISCOVERY:
        return [MONGODB_PLATFORM_PROFILE_NAME]
    return []


def _exported_operation_catalog() -> dict[str, list[dict[str, object]]]:
    operation_catalog: dict[str, list[dict[str, object]]] = {}
    profile_support = _exported_profile_support()
    catalog_capability_by_name = {
        capability.name: capability
        for capability in MONGODB_CATALOG.capabilities
    }
    for capability_name, metadata in _MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA.items():
        operation_metadata = metadata.get('operationMetadata', {})
        if not isinstance(operation_metadata, dict):
            continue
        serialized_telemetry = _serialize_capability_telemetry(
            catalog_capability_by_name.get(capability_name)
        )
        for operation_name, operation_entry in operation_metadata.items():
            if not isinstance(operation_entry, dict):
                continue
            compatible_profiles = _compatible_profile_names_for_capability(
                capability_name
            )
            operation_catalog.setdefault(operation_name, []).append(
                {
                    'capabilityName': capability_name,
                    'compatibleProfiles': compatible_profiles,
                    'compatibleProfileSupport': {
                        name: deepcopy(profile_support[name])
                        for name in compatible_profiles
                    },
                    'metadata': deepcopy(operation_entry),
                    'telemetry': deepcopy(serialized_telemetry),
                }
            )
    return operation_catalog


def _minimal_profile_name_for_projection(
    capability: str,
    additional_capabilities: tuple[str, ...],
) -> str | None:
    additional = frozenset(additional_capabilities)
    if capability in {MONGODB_READ, MONGODB_WRITE}:
        return MONGODB_CORE_PROFILE_NAME
    if capability == MONGODB_AGGREGATION:
        if MONGODB_VECTOR_SEARCH in additional:
            return MONGODB_SEARCH_PROFILE_NAME
        if MONGODB_SEARCH in additional:
            return MONGODB_TEXT_SEARCH_PROFILE_NAME
        return MONGODB_CORE_PROFILE_NAME
    return None


def _compatible_profile_names_for_projection(
    capability: str,
    additional_capabilities: tuple[str, ...],
) -> list[str]:
    if capability == MONGODB_READ:
        return [
            MONGODB_CORE_PROFILE_NAME,
            MONGODB_PLATFORM_PROFILE_NAME,
        ]
    if capability == MONGODB_WRITE:
        return [
            MONGODB_CORE_PROFILE_NAME,
            MONGODB_PLATFORM_PROFILE_NAME,
        ]
    if capability == MONGODB_AGGREGATION:
        if MONGODB_VECTOR_SEARCH in additional_capabilities:
            return [MONGODB_SEARCH_PROFILE_NAME]
        if MONGODB_SEARCH in additional_capabilities:
            return [
                MONGODB_TEXT_SEARCH_PROFILE_NAME,
                MONGODB_SEARCH_PROFILE_NAME,
            ]
        return [
            MONGODB_CORE_PROFILE_NAME,
            MONGODB_PLATFORM_PROFILE_NAME,
            MONGODB_AGGREGATE_RICH_PROFILE_NAME,
        ]
    return []


def _operation_name_for_projection(
    capability: str,
    additional_capabilities: tuple[str, ...],
) -> str | None:
    if capability == MONGODB_READ:
        return MONGODB_FIND
    if capability == MONGODB_AGGREGATION:
        return MONGODB_AGGREGATE
    return None


def _metadata_capability_for_projection(
    capability: str,
    additional_capabilities: tuple[str, ...],
) -> str:
    if capability == MONGODB_AGGREGATION:
        if MONGODB_VECTOR_SEARCH in additional_capabilities:
            return MONGODB_VECTOR_SEARCH
        if MONGODB_SEARCH in additional_capabilities:
            return MONGODB_SEARCH
    return capability


_MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA: dict[str, dict[str, object]] = {
    MONGODB_READ: {
        'embedded': True,
        'sync': True,
        'async': True,
        'queryFieldOperators': sorted(SUPPORTED_QUERY_FIELD_OPERATORS),
        'queryTopLevelOperators': sorted(SUPPORTED_QUERY_TOP_LEVEL_OPERATORS),
        'operationMetadata': {
            MONGODB_FIND: _operation_option_metadata(
                MONGODB_FIND,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_FIND],
                collection_scoped=True,
                supports_session=True,
                supports_explain=True,
                accepts_filter=True,
                accepts_projection=True,
                extra_metadata=_input_shape_metadata(
                    accepts_sort=True,
                    accepts_skip=True,
                    accepts_limit=True,
                    accepts_batch_size=True,
                    accepts_hint=True,
                    accepts_comment=True,
                    accepts_max_time_ms=True,
                    accepts_let=True,
                    accepts_collation=True,
                ),
            ),
            MONGODB_FIND_ONE: _operation_option_metadata(
                MONGODB_FIND_ONE,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_FIND_ONE],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                accepts_projection=True,
                extra_metadata=_input_shape_metadata(
                    accepts_sort=False,
                    accepts_skip=False,
                    accepts_limit=False,
                    accepts_batch_size=False,
                    accepts_hint=False,
                    accepts_comment=False,
                    accepts_max_time_ms=False,
                    accepts_let=False,
                    accepts_collation=True,
                ),
            ),
            MONGODB_COUNT_DOCUMENTS: _operation_option_metadata(
                MONGODB_COUNT_DOCUMENTS,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_COUNT_DOCUMENTS],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                extra_metadata=_input_shape_metadata(
                    accepts_hint=True,
                    accepts_comment=True,
                    accepts_max_time_ms=True,
                    accepts_collation=True,
                ),
            ),
            MONGODB_ESTIMATED_DOCUMENT_COUNT: _operation_option_metadata(
                MONGODB_ESTIMATED_DOCUMENT_COUNT,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_ESTIMATED_DOCUMENT_COUNT],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=False,
                extra_metadata=_input_shape_metadata(
                    accepts_comment=True,
                    accepts_max_time_ms=True,
                ),
            ),
            MONGODB_DISTINCT: _operation_option_metadata(
                MONGODB_DISTINCT,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_DISTINCT],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                accepts_field_path=True,
                extra_metadata=_input_shape_metadata(
                    accepts_hint=True,
                    accepts_comment=True,
                    accepts_max_time_ms=True,
                    accepts_collation=True,
                ),
            ),
        },
    },
    MONGODB_WRITE: {
        'embedded': True,
        'sync': True,
        'async': True,
        'updateOperators': sorted(SUPPORTED_UPDATE_OPERATORS),
        'supportsPipelineUpdate': True,
        'operationMetadata': {
            MONGODB_INSERT_ONE: _operation_option_metadata(
                MONGODB_INSERT_ONE,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_INSERT_ONE],
                collection_scoped=True,
                supports_session=True,
                accepts_document=True,
                extra_metadata=_input_shape_metadata(
                    accepts_comment=False,
                ),
            ),
            MONGODB_INSERT_MANY: _operation_option_metadata(
                MONGODB_INSERT_MANY,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_INSERT_MANY],
                collection_scoped=True,
                supports_session=True,
                accepts_documents=True,
                supports_ordered_execution=True,
                extra_metadata=_input_shape_metadata(
                    accepts_ordered_execution=True,
                    accepts_comment=False,
                ),
            ),
            MONGODB_UPDATE_ONE: _operation_option_metadata(
                MONGODB_UPDATE_ONE,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_UPDATE_ONE],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                accepts_update_document=True,
                supports_upsert=True,
                extra_metadata={
                    **_input_shape_metadata(
                        accepts_sort=True,
                        accepts_hint=True,
                        accepts_comment=True,
                        accepts_let=True,
                        accepts_collation=True,
                        accepts_array_filters=True,
                    ),
                    'supportsPipelineUpdate': True,
                    'supportedUpdateOperators': sorted(SUPPORTED_UPDATE_OPERATORS),
                },
            ),
            MONGODB_UPDATE_MANY: _operation_option_metadata(
                MONGODB_UPDATE_MANY,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_UPDATE_MANY],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                accepts_update_document=True,
                supports_upsert=True,
                extra_metadata={
                    **_input_shape_metadata(
                        accepts_sort=False,
                        accepts_hint=True,
                        accepts_comment=True,
                        accepts_let=True,
                        accepts_collation=True,
                        accepts_array_filters=True,
                    ),
                    'supportsPipelineUpdate': True,
                    'supportedUpdateOperators': sorted(SUPPORTED_UPDATE_OPERATORS),
                },
            ),
            MONGODB_REPLACE_ONE: _operation_option_metadata(
                MONGODB_REPLACE_ONE,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_REPLACE_ONE],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                accepts_replacement_document=True,
                supports_upsert=True,
                extra_metadata={
                    **_input_shape_metadata(
                        accepts_sort=True,
                        accepts_hint=True,
                        accepts_comment=True,
                        accepts_let=True,
                        accepts_collation=True,
                    ),
                    'supportsReplacementDocument': True,
                },
            ),
            MONGODB_DELETE_ONE: _operation_option_metadata(
                MONGODB_DELETE_ONE,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_DELETE_ONE],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                extra_metadata=_input_shape_metadata(
                    accepts_hint=True,
                    accepts_comment=True,
                    accepts_let=True,
                    accepts_collation=True,
                ),
            ),
            MONGODB_DELETE_MANY: _operation_option_metadata(
                MONGODB_DELETE_MANY,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_DELETE_MANY],
                collection_scoped=True,
                supports_session=True,
                accepts_filter=True,
                extra_metadata=_input_shape_metadata(
                    accepts_hint=True,
                    accepts_comment=True,
                    accepts_let=True,
                    accepts_collation=True,
                ),
            ),
            MONGODB_BULK_WRITE: _operation_option_metadata(
                MONGODB_BULK_WRITE,
                result_type=_OPERATION_RESULT_TYPES[MONGODB_BULK_WRITE],
                collection_scoped=True,
                supports_session=True,
                accepts_write_models=True,
                supports_ordered_execution=True,
                extra_metadata=_input_shape_metadata(
                    accepts_ordered_execution=True,
                    accepts_comment=True,
                    accepts_let=True,
                ),
            ),
        },
    },
    MONGODB_AGGREGATION: {},
    MONGODB_TRANSACTIONS: {
        'embedded': True,
        'sync': True,
        'async': True,
        'distributed': False,
        'mode': 'local',
    },
    MONGODB_CHANGE_STREAMS: {
        'implementation': 'local',
        'distributed': False,
        'persistent': False,
        'resumable': True,
        'resumableAcrossClientRestarts': False,
        'resumableAcrossProcesses': False,
        'resumableAcrossNodes': False,
        'boundedHistory': True,
    },
    MONGODB_SEARCH: {},
    MONGODB_VECTOR_SEARCH: {},
    MONGODB_COLLATION: {
        **_mongoeco_collation_metadata(),
        'operationMetadata': _mongoeco_collation_operation_metadata(),
    },
    MONGODB_PERSISTENCE: {
        **_mongoeco_persistence_metadata(),
        'operationMetadata': _mongoeco_persistence_operation_metadata(),
    },
    MONGODB_TOPOLOGY_DISCOVERY: {
        **_mongoeco_topology_discovery_metadata(),
        'operationMetadata': _mongoeco_topology_discovery_operation_metadata(),
    },
}

_MONGOECO_PUBLIC_CXP_EXTENSIONS: dict[str, dict[str, object]] = {
    'classicText': {
        'supportsTextScore': True,
        'supportsMetaProjection': True,
        'supportsSortByTextScore': True,
        'projectsFromCapability': MONGODB_SEARCH,
        'note': (
            'The classic $text subset remains local, token-based and '
            'intentionally lighter than MongoDB server full-text behavior.'
        ),
    },
    'geospatial': {
        'semantics': 'planar-local',
        'storedGeometries': [
            'Point',
            'LineString',
            'Polygon',
            'MultiPoint',
            'MultiLineString',
            'MultiPolygon',
            'GeometryCollection',
            'legacy [x, y]',
        ],
        'queryOperators': [
            '$geoWithin',
            '$geoIntersects',
            '$near',
            '$nearSphere',
        ],
        'aggregationStages': ['$geoNear'],
        'projectsFromCapability': MONGODB_READ,
        'note': (
            'The embedded runtime uses planar local geometry operations. '
            '$nearSphere and 2dsphere remain Mongo-like names over local '
            'planar distance.'
        ),
    },
}


def _metadata_to_document(value: msgspec.Struct) -> dict[str, object]:
    document = msgspec.to_builtins(value)
    if not isinstance(document, dict):
        message = 'msgspec metadata did not serialize to a mapping'
        raise TypeError(message)
    return _normalize_json_like(document)


def _normalize_json_like(value: object) -> object:
    if isinstance(value, tuple | list):
        return [_normalize_json_like(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _normalize_json_like(item)
            for key, item in value.items()
        }
    return value


_MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA[MONGODB_AGGREGATION] = {
    **_metadata_to_document(
        MongoAggregationMetadata(
            supportedStages=tuple(sorted(SUPPORTED_AGGREGATION_STAGES)),
            supportedExpressionOperators=tuple(
                sorted(SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS),
            ),
            supportedGroupAccumulators=tuple(
                sorted(SUPPORTED_GROUP_ACCUMULATORS),
            ),
            supportedWindowAccumulators=tuple(
                sorted(SUPPORTED_WINDOW_ACCUMULATORS),
            ),
        ),
    ),
    'embedded': True,
    'sync': True,
    'async': True,
    'explainable': True,
    'operationMetadata': {
        MONGODB_AGGREGATE: _operation_option_metadata(
            MONGODB_AGGREGATE,
            result_type=_OPERATION_RESULT_TYPES[MONGODB_AGGREGATE],
            supports_collection_scope=True,
            supports_database_scope=True,
            supports_session=True,
            supports_explain=True,
            accepts_pipeline=True,
            extra_metadata={
                'supportedStages': sorted(SUPPORTED_AGGREGATION_STAGES),
                'supportedExpressionOperators': sorted(
                    SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
                ),
                'supportedGroupAccumulators': sorted(
                    SUPPORTED_GROUP_ACCUMULATORS,
                ),
                'supportedWindowAccumulators': sorted(
                    SUPPORTED_WINDOW_ACCUMULATORS,
                ),
                'supportsLeadingSearchStage': True,
                'supportsLeadingVectorSearchStage': True,
            },
        ),
    },
}

_MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA[MONGODB_SEARCH] = {
    **_metadata_to_document(
        MongoSearchMetadata(
            operators=(
                'text',
                'phrase',
                'autocomplete',
                'wildcard',
                'exists',
                'in',
                'equals',
                'range',
                'near',
                'compound',
                'regex',
            ),
        ),
    ),
    'fieldMappings': sorted(SUPPORTED_SEARCH_FIELD_MAPPING_TYPES),
    'structuredFieldMappings': sorted(STRUCTURED_SEARCH_FIELD_MAPPING_TYPES),
    'textualFieldMappings': sorted(TEXTUAL_SEARCH_FIELD_MAPPING_TYPES),
    'exactFilterFieldMappings': sorted(EXACT_FILTER_SEARCH_FIELD_MAPPING_TYPES),
    'structuredParentPathOperators': [
        'text',
        'phrase',
        'autocomplete',
        'wildcard',
        'regex',
        'exists',
    ],
    'explainFeatures': [
        'pathSummary',
        'resolvedLeafPaths',
        'structuredParentPathResolution',
        'querySemantics',
        'stageOptions',
        'countPreview',
        'highlightPreview',
        'facetPreview',
    ],
    'operatorSemantics': {
        'text': {
            'matchingMode': 'tokenized-any-term',
            'scope': 'local-text-tier',
        },
        'phrase': {
            'matchingMode': 'ordered-token-window',
            'supportsSlop': True,
            'scope': 'local-text-tier',
        },
        'autocomplete': {
            'matchingMode': 'token-prefix',
            'tokenization': 'classic-text-local',
            'tokenOrder': ['any', 'sequential'],
            'supportsFuzzy': True,
            'fuzzy': {
                'maxEdits': [1, 2],
                'supportsPrefixLength': True,
                'supportsMaxExpansions': True,
            },
            'atlasParity': 'subset',
            'scope': 'local-text-tier',
        },
        'wildcard': {
            'matchingMode': 'glob-local',
            'patternSyntax': 'fnmatch-like',
            'allowAnalyzedField': True,
            'tokenFallbackOnAnalyzedField': True,
            'atlasParity': 'subset',
            'scope': 'local-text-tier',
        },
        'regex': {
            'matchingMode': 'python-regex-local',
            'supportsFlags': True,
            'supportsOptionsAlias': True,
            'supportedFlags': ['a', 'i', 'm', 's', 'u', 'x'],
            'allowAnalyzedField': True,
            'atlasParity': 'subset',
            'scope': 'local-text-tier',
        },
        'exists': {
            'matchingMode': 'field-presence',
            'scope': 'local-text-tier',
        },
        'in': {
            'matchingMode': 'exact-membership',
            'scope': 'local-filter-tier',
        },
        'equals': {
            'matchingMode': 'exact-equality',
            'scope': 'local-filter-tier',
        },
        'range': {
            'matchingMode': 'range-comparison',
            'scope': 'local-filter-tier',
        },
        'near': {
            'matchingMode': 'distance-ranking',
            'scope': 'local-filter-tier',
        },
    },
    'textSearchTier': 'closed-local-tier',
    'stageOptions': {
        'count': ['total', 'lowerBound'],
        'highlight': {
            'supportsPath': True,
            'supportsMaxChars': True,
            'resultField': 'searchHighlights',
        },
        'facet': {
            'supportsPath': True,
            'supportsNumBuckets': True,
            'supportsType': True,
            'supportedTypes': [
                'string',
                'number',
                'date',
                'boolean',
                'objectId',
                'uuid',
            ],
            'supportsCollectorOperator': True,
            'collectorOperatorSupports': [
                'text',
                'phrase',
                'autocomplete',
                'wildcard',
                'regex',
                'exists',
                'in',
                'equals',
                'range',
                'near',
                'compound',
            ],
            'previewOnly': True,
        },
    },
    'advancedAtlasLikeGaps': [
        'fullFacetCollectorParity',
        'fullHighlightParity',
        'countMetaParity',
        'advancedAutocompleteSemantics',
        'advancedWildcardSemantics',
        'advancedRegexOptions',
    ],
    'sqliteBackends': [
        'fts5',
        'fts5-glob',
        'fts5-path',
        'fts5-prefilter',
        'python',
    ],
    'operationMetadata': {
        MONGODB_AGGREGATE: _operation_option_metadata(
            MONGODB_AGGREGATE,
            result_type=_OPERATION_RESULT_TYPES[MONGODB_AGGREGATE],
            supports_collection_scope=True,
            supports_database_scope=False,
            supports_session=True,
            supports_explain=True,
            accepts_pipeline=True,
            extra_metadata={
                'aggregateStage': '$search',
                'operators': [
                    'text',
                    'phrase',
                    'autocomplete',
                    'wildcard',
                    'exists',
                    'in',
                    'equals',
                    'range',
                    'near',
                    'compound',
                    'regex',
                ],
                'stageOptions': {
                    'count': ['total', 'lowerBound'],
                    'highlight': {
                        'supportsPath': True,
                        'supportsMaxChars': True,
                        'resultField': 'searchHighlights',
                    },
                    'facet': {
                        'supportsPath': True,
                        'supportsNumBuckets': True,
                        'supportsType': True,
                        'supportedTypes': [
                            'string',
                            'number',
                            'date',
                            'boolean',
                            'objectId',
                            'uuid',
                        ],
                        'previewOnly': True,
                    },
                },
                'requiresLeadingStage': True,
            },
        ),
    },
    'note': (
        'The local textual $search tier is closed in its documented subset; '
        'remaining gaps are advanced Atlas-like features.'
    ),
}

_MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA[MONGODB_VECTOR_SEARCH] = {
    **_metadata_to_document(
        MongoVectorSearchMetadata(
            similarities=('cosine', 'dotProduct', 'euclidean'),
        ),
    ),
    'backend': 'usearch',
    'mode': 'local-ann-with-exact-baseline',
    'filterMode': 'post-candidate-with-adaptive-candidate-expansion',
    'fallback': 'exact',
    'hybridFilterModes': [
        'candidate-prefilter',
        'candidate-prefilter+post-candidate',
        'post-candidate',
    ],
    'explainFeatures': [
        'pathSummary',
        'resolvedLeafPaths',
        'querySemantics',
        'scoreBreakdown',
        'candidatePlan',
        'hybridRetrieval',
        'vectorBackend',
    ],
    'operationMetadata': {
        MONGODB_AGGREGATE: _operation_option_metadata(
            MONGODB_AGGREGATE,
            result_type=_OPERATION_RESULT_TYPES[MONGODB_AGGREGATE],
            supports_collection_scope=True,
            supports_database_scope=False,
            supports_session=True,
            supports_explain=True,
            accepts_pipeline=True,
            extra_metadata={
                'aggregateStage': '$vectorSearch',
                'similarities': ['cosine', 'dotProduct', 'euclidean'],
                'scoreField': 'vectorSearchScore',
                'hybridFilterModes': [
                    'candidate-prefilter',
                    'candidate-prefilter+post-candidate',
                    'post-candidate',
                ],
                'explainFeatures': [
                    'pathSummary',
                    'resolvedLeafPaths',
                    'querySemantics',
                    'scoreBreakdown',
                    'candidatePlan',
                    'hybridRetrieval',
                    'vectorBackend',
                ],
                'requiresLeadingStage': True,
            },
        ),
    },
    'note': (
        'SQLiteEngine uses a local usearch ANN backend when the vector '
        'index is materialized; MemoryEngine remains the exact semantic '
        'baseline.'
    ),
}


def mongoeco_public_cxp_capability_metadata() -> dict[str, dict[str, object]]:
    return deepcopy(_MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA)


def export_cxp_extension_catalog() -> dict[str, dict[str, object]]:
    return deepcopy(_MONGOECO_PUBLIC_CXP_EXTENSIONS)


def export_cxp_capability_catalog() -> dict[str, object]:
    tier_membership: dict[str, list[str]] = {}
    for tier in MONGODB_CATALOG.tiers:
        for capability_name in tier.required_capabilities:
            tier_membership.setdefault(capability_name, []).append(tier.name)

    capabilities: dict[str, dict[str, object]] = {}
    for catalog_capability in MONGODB_CATALOG.capabilities:
        metadata = deepcopy(
            _MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA.get(
                catalog_capability.name,
                {},
            ),
        )
        capabilities[catalog_capability.name] = {
            'description': catalog_capability.description,
            'tiers': list(tier_membership.get(catalog_capability.name, ())),
            'operations': [
                {
                    'name': operation.name,
                    'resultType': operation.result_type,
                    'description': operation.description,
                }
                for operation in catalog_capability.operations
            ],
            'metadata': metadata,
            'telemetry': _serialize_capability_telemetry(catalog_capability),
        }

    return {
        'interface': MONGODB_INTERFACE,
        'profiles': _exported_profiles(),
        'profileSupport': _exported_profile_support(),
        'operations': _exported_operation_catalog(),
        'capabilities': capabilities,
        'extensions': export_cxp_extension_catalog(),
    }


def export_cxp_operation_catalog() -> dict[str, list[dict[str, object]]]:
    return _exported_operation_catalog()


def export_cxp_profile_catalog() -> dict[str, dict[str, object]]:
    return _exported_profiles()


def export_cxp_profile_support_catalog() -> dict[str, dict[str, object]]:
    return _exported_profile_support()


def export_legacy_runtime_subset_catalog() -> dict[str, dict[str, object]]:
    capability_catalog = export_cxp_capability_catalog()['capabilities']
    if not isinstance(capability_catalog, dict):
        message = 'capability catalog must serialize to a mapping'
        raise TypeError(message)
    vector_search = capability_catalog[MONGODB_VECTOR_SEARCH]
    search = capability_catalog[MONGODB_SEARCH]
    extensions = export_cxp_extension_catalog()
    return {
        'vectorSearch': deepcopy(vector_search['metadata']),
        'search': deepcopy(search['metadata']),
        'classicText': deepcopy(extensions['classicText']),
        'geospatial': deepcopy(extensions['geospatial']),
    }


def build_mongodb_explain_projection(
    *,
    capability: str,
    additional_capabilities: tuple[str, ...] = (),
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    projection: dict[str, object] = {
        'interface': MONGODB_INTERFACE,
        'provider': 'mongoeco',
        'capability': capability,
    }
    metadata_capability = _metadata_capability_for_projection(
        capability,
        additional_capabilities,
    )
    capability_metadata = deepcopy(
        _MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA.get(metadata_capability, {})
    )
    if additional_capabilities:
        projection['additionalCapabilities'] = list(additional_capabilities)
    minimal_profile = _minimal_profile_name_for_projection(
        capability,
        additional_capabilities,
    )
    if minimal_profile is not None:
        projection['minimalProfile'] = minimal_profile
        projection['minimalProfileRequirements'] = deepcopy(
            _exported_profiles()[minimal_profile]['requirements']
        )
    compatible_profiles = _compatible_profile_names_for_projection(
        capability,
        additional_capabilities,
    )
    if compatible_profiles:
        projection['compatibleProfiles'] = compatible_profiles
        projection['compatibleProfileSupport'] = {
            name: deepcopy(_exported_profile_support()[name])
            for name in compatible_profiles
        }
    operation_name = _operation_name_for_projection(
        capability,
        additional_capabilities,
    )
    if operation_name is not None:
        operation_metadata = capability_metadata.get('operationMetadata', {})
        if isinstance(operation_metadata, dict):
            selected_operation_metadata = operation_metadata.get(operation_name)
            if isinstance(selected_operation_metadata, dict):
                projection['operationName'] = operation_name
                projection['operationMetadata'] = deepcopy(
                    selected_operation_metadata
                )
    if metadata:
        projection['metadata'] = deepcopy(metadata)
    return projection
