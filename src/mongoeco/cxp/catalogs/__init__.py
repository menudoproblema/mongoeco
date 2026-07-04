from importlib import import_module

_BASE_EXPORTS = (
    'CapabilityCatalog',
    'CapabilityProfile',
    'CapabilityProfileValidationResult',
    'CapabilityRequirement',
    'CapabilityMatrixValidationResult',
    'CapabilityMetadataSchema',
    'CatalogCapability',
    'CatalogOperation',
    'CatalogRegistry',
    'ConformanceTier',
    'DEFAULT_CATALOG_REGISTRY',
    'DescriptorValidationResult',
    'UnknownCapabilityOperations',
    'get_catalog',
    'register_catalog',
)

__all__ = tuple([*_BASE_EXPORTS])

_EXPORT_MODULES = {
    **dict.fromkeys(_BASE_EXPORTS, 'mongoeco.cxp.catalogs.base'),
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
