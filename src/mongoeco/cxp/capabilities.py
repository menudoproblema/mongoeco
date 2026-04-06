from __future__ import annotations

from copy import deepcopy

import msgspec

from cxp.capabilities import Capability, CapabilityMatrix, CapabilityMetadata
from cxp.catalogs.base import CapabilityCatalog, CapabilityProfile
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
    MongoSearchMetadata,
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
        'backendContract': 'local-collation-introspection',
        'metadataSources': [
            'collation_backend_info',
            'collation_capabilities_info',
        ],
    },
    MONGODB_PERSISTENCE: {
        'runtimeDependent': True,
        'embedded': True,
        'note': (
            'Persistence depends on the selected embedded backend and its '
            'storage mode.'
        ),
    },
    MONGODB_TOPOLOGY_DISCOVERY: {
        'metadataSources': ['topology_description', 'sdam_capabilities'],
        'distributed': False,
        'mode': 'local-sdam-subset',
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
                'requiresLeadingStage': True,
            },
        ),
    },
    'note': (
        'The local $search surface remains an explicit Atlas-like subset.'
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
        }

    return {
        'interface': MONGODB_INTERFACE,
        'profiles': {
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
        },
        'capabilities': capabilities,
        'extensions': export_cxp_extension_catalog(),
    }


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
    if additional_capabilities:
        projection['additionalCapabilities'] = list(additional_capabilities)
    minimal_profile = _minimal_profile_name_for_projection(
        capability,
        additional_capabilities,
    )
    if minimal_profile is not None:
        projection['minimalProfile'] = minimal_profile
    if metadata:
        projection['metadata'] = deepcopy(metadata)
    return projection
