from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType


class OptionSupportStatus(Enum):
    EFFECTIVE = "effective"
    ACCEPTED_NOOP = "accepted-noop"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True)
class OperationOptionSupport:
    status: OptionSupportStatus
    note: str | None = None


def _build_operation_option_support() -> MappingProxyType[str, MappingProxyType[str, OperationOptionSupport]]:
    accepted_noop = OptionSupportStatus.ACCEPTED_NOOP
    effective = OptionSupportStatus.EFFECTIVE
    matrix = {
        "find": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted by the API, but engines do not yet force plan selection."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only; no profiler or command envelope consumes it yet."),
            "max_time_ms": OperationOptionSupport(accepted_noop, "Validated, but no local timeout enforcement exists yet."),
            "batch_size": OperationOptionSupport(accepted_noop, "Cursor shape supports it, but local engines still materialize without real batching."),
        },
        "aggregate": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted and forwarded to pushdown find(), but no engine enforces index selection yet."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "max_time_ms": OperationOptionSupport(accepted_noop, "Validated, but no local timeout enforcement exists yet."),
            "batch_size": OperationOptionSupport(accepted_noop, "Cursor shape supports it, but aggregation remains materialized."),
            "let": OperationOptionSupport(effective, "Propagated into aggregate expression evaluation and subpipelines."),
        },
        "update_one": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but update paths do not yet consume command-level let variables."),
            "sort": OperationOptionSupport(effective, "Implemented with profile-aware validation since PyMongo 4.11."),
        },
        "update_many": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but update paths do not yet consume command-level let variables."),
        },
        "replace_one": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but replacement paths do not yet consume command-level let variables."),
            "sort": OperationOptionSupport(effective, "Implemented with profile-aware validation since PyMongo 4.11."),
        },
        "delete_one": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but delete paths do not yet consume command-level let variables."),
        },
        "delete_many": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but delete paths do not yet consume command-level let variables."),
        },
        "find_one_and_update": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "max_time_ms": OperationOptionSupport(accepted_noop, "Validated, but no local timeout enforcement exists yet."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but update paths do not yet consume command-level let variables."),
            "sort": OperationOptionSupport(effective, "Implemented through update_one()/find semantics with profile-aware validation."),
        },
        "find_one_and_replace": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "max_time_ms": OperationOptionSupport(accepted_noop, "Validated, but no local timeout enforcement exists yet."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but replacement paths do not yet consume command-level let variables."),
            "sort": OperationOptionSupport(effective, "Implemented through replace_one()/find semantics with profile-aware validation."),
        },
        "find_one_and_delete": {
            "hint": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but engines do not yet select indexes from hints."),
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "max_time_ms": OperationOptionSupport(accepted_noop, "Validated, but no local timeout enforcement exists yet."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but delete paths do not yet consume command-level let variables."),
        },
        "bulk_write": {
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "let": OperationOptionSupport(accepted_noop, "Accepted for compatibility, but command-level variables are not consumed by write execution yet."),
        },
        "list_indexes": {
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
        },
        "create_index": {
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "max_time_ms": OperationOptionSupport(accepted_noop, "Validated, but local index builds are still serialized inside the engine."),
        },
        "create_indexes": {
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
            "max_time_ms": OperationOptionSupport(accepted_noop, "Validated, but local index builds are still serialized inside the engine."),
        },
        "drop_index": {
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
        },
        "drop_indexes": {
            "comment": OperationOptionSupport(accepted_noop, "Accepted for compatibility only."),
        },
    }
    return MappingProxyType(
        {
            operation: MappingProxyType(options)
            for operation, options in matrix.items()
        }
    )


OPERATION_OPTION_SUPPORT = _build_operation_option_support()


def get_operation_option_support(
    operation: str,
    option: str,
) -> OperationOptionSupport | None:
    return OPERATION_OPTION_SUPPORT.get(operation, {}).get(option)


def is_operation_option_effective(operation: str, option: str) -> bool:
    support = get_operation_option_support(operation, option)
    return support is not None and support.status is OptionSupportStatus.EFFECTIVE
