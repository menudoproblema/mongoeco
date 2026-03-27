from types import MappingProxyType

from mongoeco.compat.catalog import (
    OPERATION_OPTION_SUPPORT_CATALOG,
    OperationOptionSupport,
    OptionSupportStatus,
)


OPERATION_OPTION_SUPPORT = OPERATION_OPTION_SUPPORT_CATALOG
MANAGED_OPERATION_OPTION_NAMES = frozenset(
    option
    for options in OPERATION_OPTION_SUPPORT.values()
    for option in options
)
OPERATION_OPTION_SIGNATURE_EXCLUSIONS = MappingProxyType(
    {
        "find": frozenset({"sort"}),
    }
)
UNSUPPORTED_OPERATION_OPTION = OperationOptionSupport(
    OptionSupportStatus.UNSUPPORTED,
    "Option is not part of the public contract for this operation.",
)


def get_operation_option_support(
    operation: str,
    option: str,
) -> OperationOptionSupport:
    return OPERATION_OPTION_SUPPORT.get(operation, {}).get(
        option,
        UNSUPPORTED_OPERATION_OPTION,
    )


def is_operation_option_effective(operation: str, option: str) -> bool:
    support = get_operation_option_support(operation, option)
    return support is not None and support.status is OptionSupportStatus.EFFECTIVE
