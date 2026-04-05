from __future__ import annotations

from copy import deepcopy

from cxp.capabilities import Capability, CapabilityMatrix, CapabilityMetadata
from mongoeco.compat._catalog_data import (
    SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
    SUPPORTED_AGGREGATION_STAGES,
    SUPPORTED_GROUP_ACCUMULATORS,
    SUPPORTED_QUERY_FIELD_OPERATORS,
    SUPPORTED_QUERY_TOP_LEVEL_OPERATORS,
    SUPPORTED_UPDATE_OPERATORS,
    SUPPORTED_WINDOW_ACCUMULATORS,
)
from mongoeco.cxp.catalogs.interfaces.database.mongodb import (
    MONGODB_AGGREGATION,
    MONGODB_CATALOG,
    MONGODB_CHANGE_STREAMS,
    MONGODB_COLLATION,
    MONGODB_INTERFACE,
    MONGODB_PERSISTENCE,
    MONGODB_READ,
    MONGODB_SEARCH,
    MONGODB_TOPOLOGY_DISCOVERY,
    MONGODB_TRANSACTIONS,
    MONGODB_VECTOR_SEARCH,
    MONGODB_WRITE,
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


_MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA: dict[str, dict[str, object]] = {
    MONGODB_READ: {
        'embedded': True,
        'sync': True,
        'async': True,
        'queryFieldOperators': sorted(SUPPORTED_QUERY_FIELD_OPERATORS),
        'queryTopLevelOperators': sorted(SUPPORTED_QUERY_TOP_LEVEL_OPERATORS),
    },
    MONGODB_WRITE: {
        'embedded': True,
        'sync': True,
        'async': True,
        'updateOperators': sorted(SUPPORTED_UPDATE_OPERATORS),
        'supportsPipelineUpdate': True,
    },
    MONGODB_AGGREGATION: {
        'embedded': True,
        'sync': True,
        'async': True,
        'explainable': True,
        'supportedStages': sorted(SUPPORTED_AGGREGATION_STAGES),
        'supportedExpressionOperators': sorted(
            SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
        ),
        'supportedGroupAccumulators': sorted(SUPPORTED_GROUP_ACCUMULATORS),
        'supportedWindowAccumulators': sorted(SUPPORTED_WINDOW_ACCUMULATORS),
    },
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
    MONGODB_SEARCH: {
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
        'aggregateStage': '$search',
        'sqliteBackends': [
            'fts5',
            'fts5-glob',
            'fts5-path',
            'fts5-prefilter',
            'python',
        ],
        'note': (
            'The local $search surface remains an explicit Atlas-like '
            'subset.'
        ),
    },
    MONGODB_VECTOR_SEARCH: {
        'backend': 'usearch',
        'aggregateStage': '$vectorSearch',
        'mode': 'local-ann-with-exact-baseline',
        'similarities': ['cosine', 'dotProduct', 'euclidean'],
        'filterMode': 'post-candidate-with-adaptive-candidate-expansion',
        'fallback': 'exact',
        'note': (
            'SQLiteEngine uses a local usearch ANN backend when the vector '
            'index is materialized; MemoryEngine remains the exact semantic '
            'baseline.'
        ),
    },
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
    assert isinstance(capability_catalog, dict)
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
