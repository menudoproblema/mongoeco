from importlib import import_module

_DIALECT_EXPORTS = (
    'MongoDialect',
    'MONGODB_DIALECT_HOOK_NAMES',
    'MONGODB_DIALECT_70',
    'MONGODB_DIALECT_80',
    'MONGODB_DIALECTS',
    'MONGODB_DIALECT_ALIASES',
    'MONGODB_DIALECT_CAPABILITIES',
    'MONGODB_DIALECT_BEHAVIOR_FLAGS',
    'MONGODB_DIALECT_POLICY_SPECS',
    'MongoDialect70',
    'MongoDialect80',
    'MongoBehaviorPolicySpec',
    'SUPPORTED_MONGODB_MAJORS',
    'DEFAULT_MONGODB_DIALECT',
    'MongoDialectResolution',
    'resolve_mongodb_dialect',
    'resolve_mongodb_dialect_resolution',
)

_PROFILE_EXPORTS = (
    'PyMongoProfile',
    'PYMONGO_PROFILE_HOOK_NAMES',
    'PYMONGO_PROFILE_49',
    'PYMONGO_PROFILE_411',
    'PYMONGO_PROFILE_413',
    'PYMONGO_PROFILE_417',
    'PYMONGO_PROFILES',
    'PYMONGO_PROFILE_ALIASES',
    'PYMONGO_PROFILE_CAPABILITIES',
    'PYMONGO_PROFILE_BEHAVIOR_FLAGS',
    'PyMongoProfile49',
    'PyMongoProfile411',
    'PyMongoProfile413',
    'PyMongoProfile417',
    'SUPPORTED_PYMONGO_MAJORS',
    'DEFAULT_PYMONGO_PROFILE',
    'AUTO_INSTALLED_PYMONGO_PROFILE',
    'STRICT_AUTO_INSTALLED_PYMONGO_PROFILE',
    'PyMongoProfileResolution',
    'resolve_pymongo_profile',
    'resolve_pymongo_profile_resolution',
    'detect_installed_pymongo_profile',
    'detect_installed_pymongo_profile_resolution',
)

_OPTION_EXPORTS = (
    'OptionSupportStatus',
    'OperationOptionSupport',
    'OPERATION_OPTION_SUPPORT',
    'UNSUPPORTED_OPERATION_OPTION',
    'get_operation_option_support',
    'is_operation_option_effective',
    'export_operation_option_catalog',
)

_CATALOG_EXPORTS = (
    'export_mongodb_dialect_catalog',
    'export_pymongo_profile_catalog',
    'export_database_command_catalog',
    'export_database_command_option_catalog',
    'export_cxp_catalog',
    'export_cxp_operation_catalog',
    'export_cxp_profile_catalog',
    'export_cxp_profile_support_catalog',
    'export_mock_safe_profile_catalog',
    'export_full_compat_catalog',
    'export_full_compat_catalog_markdown',
)

__all__ = [
    *_DIALECT_EXPORTS,
    *_PROFILE_EXPORTS,
    *_OPTION_EXPORTS,
    *_CATALOG_EXPORTS,
]

_BASE_EXPORTS = {
    'MongoDialect',
    'MONGODB_DIALECT_HOOK_NAMES',
    'MONGODB_DIALECT_70',
    'MONGODB_DIALECT_80',
    'MONGODB_DIALECTS',
    'MONGODB_DIALECT_ALIASES',
    'MONGODB_DIALECT_CAPABILITIES',
    'MONGODB_DIALECT_BEHAVIOR_FLAGS',
    'MONGODB_DIALECT_POLICY_SPECS',
    'MongoDialect70',
    'MongoDialect80',
    'SUPPORTED_MONGODB_MAJORS',
    'PyMongoProfile',
    'PYMONGO_PROFILE_HOOK_NAMES',
    'PYMONGO_PROFILE_49',
    'PYMONGO_PROFILE_411',
    'PYMONGO_PROFILE_413',
    'PYMONGO_PROFILE_417',
    'PYMONGO_PROFILES',
    'PYMONGO_PROFILE_ALIASES',
    'PYMONGO_PROFILE_CAPABILITIES',
    'PYMONGO_PROFILE_BEHAVIOR_FLAGS',
    'PyMongoProfile49',
    'PyMongoProfile411',
    'PyMongoProfile413',
    'PyMongoProfile417',
    'SUPPORTED_PYMONGO_MAJORS',
}
_REGISTRY_EXPORTS = {
    'DEFAULT_MONGODB_DIALECT',
    'MongoDialectResolution',
    'resolve_mongodb_dialect',
    'resolve_mongodb_dialect_resolution',
    'DEFAULT_PYMONGO_PROFILE',
    'AUTO_INSTALLED_PYMONGO_PROFILE',
    'STRICT_AUTO_INSTALLED_PYMONGO_PROFILE',
    'PyMongoProfileResolution',
    'resolve_pymongo_profile',
    'resolve_pymongo_profile_resolution',
    'detect_installed_pymongo_profile',
    'detect_installed_pymongo_profile_resolution',
}

_EXPORT_MODULES = {
    **dict.fromkeys(_BASE_EXPORTS, 'mongoeco.compat.base'),
    **dict.fromkeys(_REGISTRY_EXPORTS, 'mongoeco.compat.registry'),
    **dict.fromkeys(_OPTION_EXPORTS, 'mongoeco.compat.operation_support'),
    **dict.fromkeys(_CATALOG_EXPORTS, 'mongoeco.compat.catalog'),
    'MongoBehaviorPolicySpec': 'mongoeco.compat.catalog',
    'export_operation_option_catalog': 'mongoeco.compat.catalog',
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
