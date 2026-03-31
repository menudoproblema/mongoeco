from typing import Any

try:  # pragma: no cover - optional dependency bridge
    from pymongo import errors as _pymongo_errors
except Exception:  # pragma: no cover - pymongo is optional
    _PyMongoDuplicateKeyErrorBase = Exception
    _HAS_PYMONGO_DUPLICATE_KEY_ERROR = False
else:  # pragma: no cover - behavior exercised indirectly in environments with pymongo
    _PyMongoDuplicateKeyErrorBase = _pymongo_errors.DuplicateKeyError
    _HAS_PYMONGO_DUPLICATE_KEY_ERROR = True

from mongoeco.error_catalog import (
    DOCUMENT_VALIDATION_ERROR,
    DUPLICATE_KEY_ERROR,
    EXECUTION_TIMEOUT_ERROR,
    MongoErrorDescriptor,
    OPERATION_FAILURE,
    SERVER_SELECTION_TIMEOUT_ERROR,
    WRITE_ERROR,
    build_error_metadata,
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


class ServerSelectionTimeoutError(ConnectionFailure):
    """Se produce cuando no hay ningún servidor elegible antes del timeout de selección."""

    def __init__(self, message: str):
        super().__init__(message)
        self.code = SERVER_SELECTION_TIMEOUT_ERROR.code
        self.code_name = SERVER_SELECTION_TIMEOUT_ERROR.code_name
        self.details = None
        self.error_labels = SERVER_SELECTION_TIMEOUT_ERROR.error_labels

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
        *,
        descriptor: MongoErrorDescriptor = WRITE_ERROR,
    ):
        super().__init__(message)
        self.code, self.code_name, self.details = build_error_metadata(
            descriptor,
            code=code,
            details=details,
            error_labels=error_labels,
        )
        self.error_labels = tuple(self.details.get("errorLabels", ())) if self.details else ()

class DuplicateKeyError(WriteError, _PyMongoDuplicateKeyErrorBase):
    """Se produce cuando se intenta insertar un documento con un _id que ya existe."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        resolved_code, resolved_code_name, resolved_details = build_error_metadata(
            DUPLICATE_KEY_ERROR,
            code=DUPLICATE_KEY_ERROR.code if code is None else code,
            details=details,
            error_labels=error_labels or DUPLICATE_KEY_ERROR.error_labels,
        )
        if _HAS_PYMONGO_DUPLICATE_KEY_ERROR:
            _PyMongoDuplicateKeyErrorBase.__init__(
                self,
                message,
                code=resolved_code,
                details=resolved_details,
            )
            self.code_name = resolved_code_name
            labels = tuple(resolved_details.get("errorLabels", ())) if resolved_details else ()
            self.error_labels = labels
            if hasattr(self, "_error_labels"):
                self._error_labels = set(labels)
            return
        WriteError.__init__(
            self,
            message,
            code=resolved_code,
            details=resolved_details,
            error_labels=tuple(resolved_details.get("errorLabels", ())) if resolved_details else (),
            descriptor=DUPLICATE_KEY_ERROR,
        )

class DocumentValidationFailure(WriteError):
    """Se produce cuando una escritura viola el validador de la colección."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        super().__init__(
            message,
            code=DOCUMENT_VALIDATION_ERROR.code if code is None else code,
            details=details,
            error_labels=error_labels or DOCUMENT_VALIDATION_ERROR.error_labels,
            descriptor=DOCUMENT_VALIDATION_ERROR,
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
        *,
        descriptor: MongoErrorDescriptor = OPERATION_FAILURE,
    ):
        super().__init__(message)
        self.code, self.code_name, self.details = build_error_metadata(
            descriptor,
            code=code,
            details=details,
            error_labels=error_labels,
        )
        self.error_labels = tuple(self.details.get("errorLabels", ())) if self.details else ()


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
            descriptor=EXECUTION_TIMEOUT_ERROR,
        )
