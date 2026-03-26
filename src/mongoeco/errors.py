from typing import Any

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

class WriteError(PyMongoError):
    """Se produce cuando una operación de escritura falla."""

    def __init__(self, message: str, code: int | None = None, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details

class DuplicateKeyError(WriteError):
    """Se produce cuando se intenta insertar un documento con un _id que ya existe."""
    pass

class BulkWriteError(WriteError):
    """Se produce cuando falla una operación de escritura múltiple."""
    pass

class OperationFailure(PyMongoError):
    """Se produce cuando una operación de base de datos falla."""

    def __init__(self, message: str, code: int | None = None, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details


class ExecutionTimeout(OperationFailure):
    """Se produce cuando una operación excede su max_time_ms local."""
    pass
