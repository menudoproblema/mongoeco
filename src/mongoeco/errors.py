from typing import Any

from mongoeco.error_catalog import (
    DUPLICATE_KEY_ERROR,
    EXECUTION_TIMEOUT_ERROR,
    OPERATION_FAILURE,
    WRITE_ERROR,
)


class MongoEcoError(Exception):
    """Excepción base para todos los errores de mongoeco."""
    pass

class PyMongoError(MongoEcoError):
    """Excepción para errores compatibles con la API de PyMongo."""
    pass

class ConnectionFailure(PyMongoError):
    """Se produce cuando falla la conexión con el motor de almacenamiento."""
    pass

class InvalidOperation(PyMongoError):
    """Se produce cuando se intenta una operación no válida."""
    pass


class CollectionInvalid(PyMongoError):
    """Se produce cuando una colección no puede crearse o administrarse."""
    pass

class WriteError(PyMongoError):
    """Se produce cuando una operación de escritura falla."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        super().__init__(message)
        self.code = WRITE_ERROR.code if code is None else code
        self.error_labels = error_labels or WRITE_ERROR.error_labels
        self.details = _merge_error_details(details, self.error_labels)

class DuplicateKeyError(WriteError):
    """Se produce cuando se intenta insertar un documento con un _id que ya existe."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        super().__init__(
            message,
            code=DUPLICATE_KEY_ERROR.code if code is None else code,
            details=details,
            error_labels=error_labels or DUPLICATE_KEY_ERROR.error_labels,
        )

class BulkWriteError(WriteError):
    """Se produce cuando falla una operación de escritura múltiple."""
    pass

class OperationFailure(PyMongoError):
    """Se produce cuando una operación de base de datos falla."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        super().__init__(message)
        self.code = OPERATION_FAILURE.code if code is None else code
        self.error_labels = error_labels or OPERATION_FAILURE.error_labels
        self.details = _merge_error_details(details, self.error_labels)


class ExecutionTimeout(OperationFailure):
    """Se produce cuando una operación excede su max_time_ms local."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        super().__init__(
            message,
            code=EXECUTION_TIMEOUT_ERROR.code if code is None else code,
            details=details,
            error_labels=error_labels or EXECUTION_TIMEOUT_ERROR.error_labels,
        )


def _merge_error_details(
    details: dict[str, Any] | None,
    error_labels: tuple[str, ...],
) -> dict[str, Any] | None:
    if not error_labels:
        return details
    merged = dict(details or {})
    merged.setdefault("errorLabels", list(error_labels))
    return merged
