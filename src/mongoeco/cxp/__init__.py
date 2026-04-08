from mongoeco.cxp.capabilities import (
    build_mongodb_explain_projection,
    export_cxp_capability_catalog,
    export_cxp_operation_catalog,
    export_cxp_profile_catalog,
    export_cxp_profile_support_catalog,
)
from mongoeco.cxp.catalogs.interfaces.database import (
    MongoAggregationMetadata,
    MongoCollationMetadata,
    MongoPersistenceMetadata,
    MongoSearchMetadata,
    MongoTopologyDiscoveryMetadata,
    MongoVectorSearchMetadata,
    MONGODB_AGGREGATE_RICH_PROFILE,
    MONGODB_AGGREGATE_RICH_PROFILE_NAME,
    MONGODB_CATALOG,
    MONGODB_CORE_PROFILE,
    MONGODB_CORE_PROFILE_NAME,
    MONGODB_INTERFACE,
    MONGODB_PLATFORM_PROFILE,
    MONGODB_PLATFORM_PROFILE_NAME,
    MONGODB_SEARCH_PROFILE,
    MONGODB_SEARCH_PROFILE_NAME,
    MONGODB_TEXT_SEARCH_PROFILE,
    MONGODB_TEXT_SEARCH_PROFILE_NAME,
)

_FACADE_EXPORTS = (
    'build_mongodb_explain_projection',
    'export_cxp_capability_catalog',
    'export_cxp_operation_catalog',
    'export_cxp_profile_catalog',
    'export_cxp_profile_support_catalog',
)

_DATABASE_CONTRACT_EXPORTS = (
    'MONGODB_INTERFACE',
    'MONGODB_CATALOG',
    'MONGODB_CORE_PROFILE',
    'MONGODB_CORE_PROFILE_NAME',
    'MONGODB_TEXT_SEARCH_PROFILE',
    'MONGODB_TEXT_SEARCH_PROFILE_NAME',
    'MONGODB_SEARCH_PROFILE',
    'MONGODB_SEARCH_PROFILE_NAME',
    'MONGODB_PLATFORM_PROFILE',
    'MONGODB_PLATFORM_PROFILE_NAME',
    'MONGODB_AGGREGATE_RICH_PROFILE',
    'MONGODB_AGGREGATE_RICH_PROFILE_NAME',
)

_METADATA_SCHEMA_EXPORTS = (
    'MongoAggregationMetadata',
    'MongoCollationMetadata',
    'MongoPersistenceMetadata',
    'MongoSearchMetadata',
    'MongoTopologyDiscoveryMetadata',
    'MongoVectorSearchMetadata',
)

__all__ = tuple([
    *_FACADE_EXPORTS,
    *_DATABASE_CONTRACT_EXPORTS,
    *_METADATA_SCHEMA_EXPORTS,
])
