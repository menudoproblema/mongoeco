from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MongoErrorDescriptor:
    name: str
    code: int | None = None
    code_name: str | None = None
    error_labels: tuple[str, ...] = ()


DUPLICATE_KEY_ERROR = MongoErrorDescriptor(
    name="DuplicateKeyError",
    code=11000,
    code_name="DuplicateKey",
)
DOCUMENT_VALIDATION_ERROR = MongoErrorDescriptor(
    name="DocumentValidationFailure",
    code=121,
    code_name="DocumentValidationFailure",
)
EXECUTION_TIMEOUT_ERROR = MongoErrorDescriptor(
    name="ExecutionTimeout",
    code=50,
    code_name="MaxTimeMSExpired",
)
SERVER_SELECTION_TIMEOUT_ERROR = MongoErrorDescriptor(
    name="ServerSelectionTimeoutError",
)
WRITE_ERROR = MongoErrorDescriptor(name="WriteError")
OPERATION_FAILURE = MongoErrorDescriptor(name="OperationFailure")
UNDEFINED_VARIABLE_ERROR = MongoErrorDescriptor(
    name="UndefinedVariable",
    code=17276,
)
EMPTY_GROUP_FIELD_ERROR = MongoErrorDescriptor(
    name="EmptyGroupField",
    code=12116300,
    code_name="Location12116300",
)
DENSIFY_FIELD_PREFIX_ERROR = MongoErrorDescriptor(
    name="DensifyFieldPrefix",
    code=8993000,
    code_name="Location8993000",
)
DENSIFY_PARTITION_PREFIX_ERROR = MongoErrorDescriptor(
    name="DensifyPartitionPrefix",
    code=9554500,
    code_name="Location9554500",
)

ERROR_DESCRIPTORS: dict[str, MongoErrorDescriptor] = {
    descriptor.name: descriptor
    for descriptor in (
        DUPLICATE_KEY_ERROR,
        DOCUMENT_VALIDATION_ERROR,
        EXECUTION_TIMEOUT_ERROR,
        SERVER_SELECTION_TIMEOUT_ERROR,
        WRITE_ERROR,
        OPERATION_FAILURE,
        UNDEFINED_VARIABLE_ERROR,
        EMPTY_GROUP_FIELD_ERROR,
        DENSIFY_FIELD_PREFIX_ERROR,
        DENSIFY_PARTITION_PREFIX_ERROR,
    )
}


def descriptor_for(name: str) -> MongoErrorDescriptor:
    return ERROR_DESCRIPTORS[name]


def build_error_metadata(
    descriptor: MongoErrorDescriptor,
    *,
    code: int | None = None,
    details: dict[str, Any] | None = None,
    error_labels: tuple[str, ...] = (),
) -> tuple[int | None, str | None, dict[str, Any] | None]:
    effective_code = descriptor.code if code is None else code
    effective_labels = _merge_labels(descriptor.error_labels, error_labels)
    merged = dict(details or {}) if details is not None else None
    if descriptor.code_name is not None:
        if merged is None and effective_labels:
            merged = {}
        if merged is not None:
            merged.setdefault("codeName", descriptor.code_name)
    if effective_labels:
        if merged is None:
            merged = {}
        merged.setdefault("errorLabels", list(effective_labels))
    return effective_code, descriptor.code_name, merged


def _merge_labels(
    base_labels: tuple[str, ...],
    error_labels: tuple[str, ...],
) -> tuple[str, ...]:
    merged: list[str] = []
    for label in (*base_labels, *error_labels):
        if label not in merged:
            merged.append(label)
    return tuple(merged)
