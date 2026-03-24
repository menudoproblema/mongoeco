from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.core.paths import delete_document_value, set_document_value


class UpdateEngine:
    """Motor central para aplicar operadores de actualización de MongoDB."""

    @staticmethod
    def apply_update(doc: dict[str, Any], update_spec: dict[str, Any]) -> bool:
        """
        Aplica las operaciones de actualización a un documento (in-place).
        Devuelve True si hubo cambios (parcialmente simplificado para v1).
        """
        modified = False

        # Si el update no empieza con $, se trata como un reemplazo completo (Mongo behavior)
        # Pero en update_one normalmente se requieren operadores. Aquí forzamos operadores.
        for op, params in update_spec.items():
            if op == "$set":
                if UpdateEngine._apply_set(doc, params):
                    modified = True
            elif op == "$unset":
                if UpdateEngine._apply_unset(doc, params):
                    modified = True
            else:
                raise OperationFailure(f"Unsupported update operator: {op}")

        return modified

    @staticmethod
    def _apply_set(doc: dict[str, Any], params: dict[str, Any]) -> bool:
        modified = False
        for path, value in params.items():
            if path == "_id" or path.startswith("_id."):
                raise OperationFailure("Modifying the immutable field '_id' is not allowed")
            if set_document_value(doc, path, value):
                modified = True
        return modified

    @staticmethod
    def _apply_unset(doc: dict[str, Any], params: dict[str, Any]) -> bool:
        modified = False
        for path in params.keys():
            if path == "_id" or path.startswith("_id."):
                raise OperationFailure("Modifying the immutable field '_id' is not allowed")
            if delete_document_value(doc, path):
                modified = True
        return modified
