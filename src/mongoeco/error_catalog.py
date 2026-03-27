from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MongoErrorDescriptor:
    code: int | None = None
    error_labels: tuple[str, ...] = ()


DUPLICATE_KEY_ERROR = MongoErrorDescriptor(code=11000)
DOCUMENT_VALIDATION_ERROR = MongoErrorDescriptor(code=121)
EXECUTION_TIMEOUT_ERROR = MongoErrorDescriptor(code=50)
WRITE_ERROR = MongoErrorDescriptor()
OPERATION_FAILURE = MongoErrorDescriptor()
