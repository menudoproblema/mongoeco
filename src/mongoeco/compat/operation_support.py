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
            "hint": OperationOptionSupport(effective, "Validated against existing indexes and applied to read planning/explain where engines can honor it."),
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata and surfaced by explain()."),
            "max_time_ms": OperationOptionSupport(effective, "Enforced as a local deadline during read execution and explain()."),
            "batch_size": OperationOptionSupport(effective, "Async and sync find cursors now fetch local batches before yielding results, even though engines remain in-process."),
        },
        "count_documents": {
            "hint": OperationOptionSupport(effective, "Applied through the underlying find() path used to count matching documents."),
            "comment": OperationOptionSupport(effective, "Propagated through the underlying read path and session metadata."),
            "max_time_ms": OperationOptionSupport(effective, "Enforced through the underlying find() path used to count documents."),
        },
        "aggregate": {
            "hint": OperationOptionSupport(effective, "Applied through the pushdown find() path used by aggregate() and surfaced in explain()."),
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata and propagated through aggregate explain/materialization."),
            "max_time_ms": OperationOptionSupport(effective, "Applied to referenced collection loads, pushdown reads and final pipeline materialization."),
            "batch_size": OperationOptionSupport(effective, "Positive batch sizes trigger chunked execution for streamable aggregate pipelines; global stages still fall back to full materialization."),
            "let": OperationOptionSupport(effective, "Propagated into aggregate expression evaluation and subpipelines."),
        },
        "update_one": {
            "array_filters": OperationOptionSupport(effective, "Applied during update execution for supported filtered positional paths."),
            "hint": OperationOptionSupport(effective, "Applied through hinted document selection before single-document update execution."),
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for the write operation."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
            "sort": OperationOptionSupport(effective, "Implemented with profile-aware validation since PyMongo 4.11."),
        },
        "update_many": {
            "array_filters": OperationOptionSupport(effective, "Applied during per-document update execution for supported filtered positional paths."),
            "hint": OperationOptionSupport(effective, "Applied through hinted _id preselection before per-document updates."),
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for the write operation."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
        },
        "replace_one": {
            "hint": OperationOptionSupport(effective, "Applied through hinted document selection before replacement."),
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for the write operation."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
            "sort": OperationOptionSupport(effective, "Implemented with profile-aware validation since PyMongo 4.11."),
        },
        "delete_one": {
            "hint": OperationOptionSupport(effective, "Applied through hinted document selection before delete."),
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for the write operation."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
        },
        "delete_many": {
            "hint": OperationOptionSupport(effective, "Applied through hinted _id preselection before per-document deletes."),
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for the write operation."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
        },
        "find_one_and_update": {
            "array_filters": OperationOptionSupport(effective, "Propagated to the underlying update_one() execution for supported filtered positional paths."),
            "hint": OperationOptionSupport(effective, "Applied through hinted document selection and post-update fetch."),
            "comment": OperationOptionSupport(effective, "Propagated through the underlying read selection path and session metadata."),
            "max_time_ms": OperationOptionSupport(effective, "Propagated through the underlying read selection path and enforced there."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
            "sort": OperationOptionSupport(effective, "Implemented through update_one()/find semantics with profile-aware validation."),
        },
        "find_one_and_replace": {
            "hint": OperationOptionSupport(effective, "Applied through hinted document selection and post-replacement fetch."),
            "comment": OperationOptionSupport(effective, "Propagated through the underlying read selection path and session metadata."),
            "max_time_ms": OperationOptionSupport(effective, "Propagated through the underlying read selection path and enforced there."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
            "sort": OperationOptionSupport(effective, "Implemented through replace_one()/find semantics with profile-aware validation."),
        },
        "find_one_and_delete": {
            "sort": OperationOptionSupport(effective, "Implemented through find() selection semantics before delete."),
            "hint": OperationOptionSupport(effective, "Applied through hinted document selection before delete."),
            "comment": OperationOptionSupport(effective, "Propagated through the underlying read selection path and session metadata."),
            "max_time_ms": OperationOptionSupport(effective, "Propagated through the underlying read selection path and enforced there."),
            "let": OperationOptionSupport(effective, "Command-level let variables are available through $expr in write filters and selection paths."),
        },
        "bulk_write": {
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for the batch write operation."),
            "let": OperationOptionSupport(effective, "Command-level let variables flow into per-operation write filters through $expr when requests do not override them."),
        },
        "list_indexes": {
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for index administration."),
        },
        "create_index": {
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for index administration."),
            "max_time_ms": OperationOptionSupport(effective, "Enforced as a local deadline during index build and multikey backfill."),
        },
        "create_indexes": {
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for index administration."),
            "max_time_ms": OperationOptionSupport(effective, "Enforced as a local deadline across the whole index batch."),
        },
        "drop_index": {
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for index administration."),
        },
        "drop_indexes": {
            "comment": OperationOptionSupport(effective, "Recorded in engine session metadata for index administration."),
        },
    }
    return MappingProxyType(
        {
            operation: MappingProxyType(options)
            for operation, options in matrix.items()
        }
    )


OPERATION_OPTION_SUPPORT = _build_operation_option_support()
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
