from __future__ import annotations

from copy import deepcopy

import msgspec

from cxp.capabilities import Capability, CapabilityMatrix, CapabilityMetadata
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
    MONGODB_AGGREGATION,
    MONGODB_AGGREGATE,
    MONGODB_BULK_WRITE,
    MONGODB_CATALOG,
    MONGODB_CHANGE_STREAMS,
    MONGODB_COLLATION,
    MONGODB_COUNT_DOCUMENTS,
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
    MONGODB_READ,
    MONGODB_REPLACE_ONE,
    MONGODB_SEARCH,
    MONGODB_TOPOLOGY_DISCOVERY,
    MONGODB_TRANSACTIONS,
    MONGODB_UPDATE_MANY,
    MONGODB_UPDATE_ONE,
    MONGODB_VECTOR_SEARCH,
    MONGODB_WRITE,
)
from mongoeco.core.search import (
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
    supports_explain: bool = False,
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
    if extra_metadata:
        metadata.update(extra_metadata)
    return metadata


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
                supports_explain=True,
            ),
            MONGODB_FIND_ONE: _operation_option_metadata(MONGODB_FIND_ONE),
            MONGODB_COUNT_DOCUMENTS: _operation_option_metadata(
                MONGODB_COUNT_DOCUMENTS,
            ),
            MONGODB_ESTIMATED_DOCUMENT_COUNT: _operation_option_metadata(
                MONGODB_ESTIMATED_DOCUMENT_COUNT,
            ),
            MONGODB_DISTINCT: _operation_option_metadata(MONGODB_DISTINCT),
        },
    },
    MONGODB_WRITE: {
        'embedded': True,
        'sync': True,
        'async': True,
        'updateOperators': sorted(SUPPORTED_UPDATE_OPERATORS),
        'supportsPipelineUpdate': True,
        'operationMetadata': {
            MONGODB_INSERT_ONE: _operation_option_metadata(MONGODB_INSERT_ONE),
            MONGODB_INSERT_MANY: _operation_option_metadata(MONGODB_INSERT_MANY),
            MONGODB_UPDATE_ONE: _operation_option_metadata(
                MONGODB_UPDATE_ONE,
                extra_metadata={
                    'supportsPipelineUpdate': True,
                    'supportedUpdateOperators': sorted(SUPPORTED_UPDATE_OPERATORS),
                },
            ),
            MONGODB_UPDATE_MANY: _operation_option_metadata(
                MONGODB_UPDATE_MANY,
                extra_metadata={
                    'supportsPipelineUpdate': True,
                    'supportedUpdateOperators': sorted(SUPPORTED_UPDATE_OPERATORS),
                },
            ),
            MONGODB_REPLACE_ONE: _operation_option_metadata(
                MONGODB_REPLACE_ONE,
                extra_metadata={'supportsReplacementDocument': True},
            ),
            MONGODB_DELETE_ONE: _operation_option_metadata(MONGODB_DELETE_ONE),
            MONGODB_DELETE_MANY: _operation_option_metadata(MONGODB_DELETE_MANY),
            MONGODB_BULK_WRITE: _operation_option_metadata(MONGODB_BULK_WRITE),
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
            supports_explain=True,
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
    'textualFieldMappings': sorted(TEXTUAL_SEARCH_FIELD_MAPPING_TYPES),
    'exactFilterFieldMappings': sorted(
        SUPPORTED_SEARCH_FIELD_MAPPING_TYPES
        - TEXTUAL_SEARCH_FIELD_MAPPING_TYPES
    ),
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
            supports_explain=True,
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
            supports_explain=True,
            extra_metadata={
                'aggregateStage': '$vectorSearch',
                'similarities': ['cosine', 'dotProduct', 'euclidean'],
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
    if metadata:
        projection['metadata'] = deepcopy(metadata)
    return projection
