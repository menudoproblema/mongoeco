from importlib import import_module

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

_EXPORT_MODULES = {
    **dict.fromkeys(_FACADE_EXPORTS, 'mongoeco.cxp.capabilities'),
    **dict.fromkeys(_DATABASE_CONTRACT_EXPORTS, 'mongoeco.cxp.catalogs.interfaces.database'),
    **dict.fromkeys(_METADATA_SCHEMA_EXPORTS, 'mongoeco.cxp.catalogs.interfaces.database'),
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
