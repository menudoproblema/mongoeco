from typing import Any

try:  # pragma: no cover - optional dependency bridge
    from pymongo import errors as _pymongo_errors
except Exception:  # pragma: no cover - pymongo is optional
    _HAS_PYMONGO = False
    _PyMongoErrorBase = Exception
    _ConnectionFailureBase = Exception
    _ServerSelectionTimeoutErrorBase = Exception
    _InvalidOperationBase = Exception
    _CollectionInvalidBase = Exception
    _OperationFailureBase = Exception
    _WriteErrorBase = Exception
    _DuplicateKeyErrorBase = Exception
    _BulkWriteErrorBase = Exception
    _ExecutionTimeoutBase = Exception
else:  # pragma: no cover - behavior exercised in environments with pymongo
    _HAS_PYMONGO = True
    _PyMongoErrorBase = _pymongo_errors.PyMongoError
    _ConnectionFailureBase = _pymongo_errors.ConnectionFailure
    _ServerSelectionTimeoutErrorBase = _pymongo_errors.ServerSelectionTimeoutError
    _InvalidOperationBase = _pymongo_errors.InvalidOperation
    _CollectionInvalidBase = _pymongo_errors.CollectionInvalid
    _OperationFailureBase = _pymongo_errors.OperationFailure
    _WriteErrorBase = _pymongo_errors.WriteError
    _DuplicateKeyErrorBase = _pymongo_errors.DuplicateKeyError
    _BulkWriteErrorBase = _pymongo_errors.BulkWriteError
    _ExecutionTimeoutBase = _pymongo_errors.ExecutionTimeout

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


def _labels_from_details(details: dict[str, Any] | None) -> tuple[str, ...]:
    if not details:
        return ()
    labels = details.get("errorLabels", ())
    return tuple(labels) if isinstance(labels, list | tuple) else ()


def _set_error_labels(exc: Exception, labels: tuple[str, ...]) -> None:
    setattr(exc, "error_labels", labels)
    if hasattr(exc, "_error_labels"):
        exc._error_labels = set(labels)  # type: ignore[attr-defined]


def _init_pymongo_error(
    exc: Exception,
    message: str,
    *,
    error_labels: tuple[str, ...] = (),
) -> None:
    if _HAS_PYMONGO:
        _PyMongoErrorBase.__init__(exc, message, error_labels=error_labels or None)
    else:
        Exception.__init__(exc, message)
    _set_error_labels(exc, error_labels)


def _init_operation_failure_like(
    exc: Exception,
    message: str,
    *,
    code: int | None,
    details: dict[str, Any] | None,
    code_name: str | None,
    pymongo_base: type[Exception],
) -> None:
    labels = _labels_from_details(details)
    if _HAS_PYMONGO:
        pymongo_base.__init__(exc, message, code=code, details=details)  # type: ignore[misc]
        if code_name is not None:
            setattr(exc, "code_name", code_name)
        _set_error_labels(exc, labels)
        return
    _init_pymongo_error(exc, message, error_labels=labels)
    setattr(exc, "code", code)
    setattr(exc, "code_name", code_name)
    setattr(exc, "details", details)


class MongoEcoError(Exception):
    """Excepción base para todos los errores de mongoeco."""


class PyMongoError(MongoEcoError, _PyMongoErrorBase):
    """Excepción para errores compatibles con la API de PyMongo."""

    def __init__(self, message: str = "", error_labels: tuple[str, ...] = ()) -> None:
        _init_pymongo_error(self, message, error_labels=error_labels)


class ConnectionFailure(PyMongoError, _ConnectionFailureBase):
    """Se produce cuando falla la conexión con el motor de almacenamiento."""

    def __init__(self, message: str = "", error_labels: tuple[str, ...] = ()) -> None:
        if _HAS_PYMONGO:
            _ConnectionFailureBase.__init__(self, message, error_labels=error_labels or None)  # type: ignore[misc]
            _set_error_labels(self, error_labels)
            return
        PyMongoError.__init__(self, message, error_labels=error_labels)


class ServerSelectionTimeoutError(ConnectionFailure, _ServerSelectionTimeoutErrorBase):
    """Se produce cuando no hay ningún servidor elegible antes del timeout de selección."""

    def __init__(self, message: str, errors: dict[str, Any] | None = None):
        labels = SERVER_SELECTION_TIMEOUT_ERROR.error_labels
        if _HAS_PYMONGO:
            _ServerSelectionTimeoutErrorBase.__init__(self, message, errors=errors)  # type: ignore[misc]
            _set_error_labels(self, labels)
        else:
            ConnectionFailure.__init__(self, message, error_labels=labels)
            self.details = errors
            self.errors = errors
        self.code = SERVER_SELECTION_TIMEOUT_ERROR.code
        self.code_name = SERVER_SELECTION_TIMEOUT_ERROR.code_name
        self.details = errors
        self.errors = errors


class InvalidOperation(PyMongoError, _InvalidOperationBase):
    """Se produce cuando se intenta una operación no válida."""

    def __init__(self, message: str = "", error_labels: tuple[str, ...] = ()) -> None:
        if _HAS_PYMONGO:
            _InvalidOperationBase.__init__(self, message, error_labels=error_labels or None)  # type: ignore[misc]
            _set_error_labels(self, error_labels)
            return
        PyMongoError.__init__(self, message, error_labels=error_labels)


class CollectionInvalid(PyMongoError, _CollectionInvalidBase):
    """Se produce cuando una colección no puede crearse o administrarse."""

    def __init__(self, message: str = "", error_labels: tuple[str, ...] = ()) -> None:
        if _HAS_PYMONGO:
            _CollectionInvalidBase.__init__(self, message, error_labels=error_labels or None)  # type: ignore[misc]
            _set_error_labels(self, error_labels)
            return
        PyMongoError.__init__(self, message, error_labels=error_labels)


class OperationFailure(PyMongoError, _OperationFailureBase):
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
        resolved_code, resolved_code_name, resolved_details = build_error_metadata(
            descriptor,
            code=code,
            details=details,
            error_labels=error_labels,
        )
        _init_operation_failure_like(
            self,
            message,
            code=resolved_code,
            details=resolved_details,
            code_name=resolved_code_name,
            pymongo_base=_OperationFailureBase,
        )


class WriteError(OperationFailure, _WriteErrorBase):
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
        resolved_code, resolved_code_name, resolved_details = build_error_metadata(
            descriptor,
            code=code,
            details=details,
            error_labels=error_labels,
        )
        _init_operation_failure_like(
            self,
            message,
            code=resolved_code,
            details=resolved_details,
            code_name=resolved_code_name,
            pymongo_base=_WriteErrorBase,
        )


class DuplicateKeyError(WriteError, _DuplicateKeyErrorBase):
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
        _init_operation_failure_like(
            self,
            message,
            code=resolved_code,
            details=resolved_details,
            code_name=resolved_code_name,
            pymongo_base=_DuplicateKeyErrorBase,
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


class BulkWriteError(WriteError, _BulkWriteErrorBase):
    """Se produce cuando falla una operación de escritura múltiple."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        results = {} if details is None else details
        if _HAS_PYMONGO:
            _BulkWriteErrorBase.__init__(self, results)  # type: ignore[misc]
            labels = _labels_from_details(results)
            _set_error_labels(self, labels or error_labels)
            if code is not None:
                setattr(self, "_OperationFailure__code", code)
            return
        WriteError.__init__(
            self,
            message,
            code=65 if code is None else code,
            details=results,
            error_labels=error_labels,
        )


class ExecutionTimeout(OperationFailure, _ExecutionTimeoutBase):
    """Se produce cuando una operación excede su max_time_ms local."""

    def __init__(
        self,
        message: str,
        code: int | None = None,
        details: dict[str, Any] | None = None,
        error_labels: tuple[str, ...] = (),
    ):
        resolved_code, resolved_code_name, resolved_details = build_error_metadata(
            EXECUTION_TIMEOUT_ERROR,
            code=EXECUTION_TIMEOUT_ERROR.code if code is None else code,
            details=details,
            error_labels=error_labels or EXECUTION_TIMEOUT_ERROR.error_labels,
        )
        _init_operation_failure_like(
            self,
            message,
            code=resolved_code,
            details=resolved_details,
            code_name=resolved_code_name,
            pymongo_base=_ExecutionTimeoutBase,
        )
