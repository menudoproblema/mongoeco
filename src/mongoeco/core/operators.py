import re
from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.errors import OperationFailure
from mongoeco.core.paths import _same_value_for_update, delete_document_value, get_document_value, set_document_value


class UpdateEngine:
    """Motor central para aplicar operadores de actualización de MongoDB."""

    @staticmethod
    def apply_update(
        doc: dict[str, Any],
        update_spec: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        """
        Aplica las operaciones de actualización a un documento (in-place).
        Devuelve True si hubo cambios (parcialmente simplificado para v1).
        """
        if not isinstance(update_spec, dict) or not update_spec:
            raise OperationFailure("update specification must be a non-empty document")
        UpdateEngine._validate_update_paths(update_spec)
        modified = False

        # Si el update no empieza con $, se trata como un reemplazo completo (Mongo behavior)
        # Pero en update_one normalmente se requieren operadores. Aquí forzamos operadores.
        for op, params in update_spec.items():
            if not dialect.supports_update_operator(op):
                raise OperationFailure(f"Unsupported update operator: {op}")
            if not isinstance(params, dict):
                raise OperationFailure(f"{op} requires a document specification")
            if op == "$set":
                if UpdateEngine._apply_set(doc, params, dialect=dialect):
                    modified = True
            elif op == "$unset":
                if UpdateEngine._apply_unset(doc, params, dialect=dialect):
                    modified = True
            elif op == "$inc":
                if UpdateEngine._apply_inc(doc, params, dialect=dialect):
                    modified = True
            elif op == "$push":
                if UpdateEngine._apply_push(doc, params, dialect=dialect):
                    modified = True
            elif op == "$addToSet":
                if UpdateEngine._apply_add_to_set(doc, params, dialect=dialect):
                    modified = True
            elif op == "$pull":
                if UpdateEngine._apply_pull(doc, params, dialect=dialect):
                    modified = True
            elif op == "$pop":
                if UpdateEngine._apply_pop(doc, params, dialect=dialect):
                    modified = True
            else:
                raise OperationFailure(f"Unsupported update operator: {op}")

        return modified

    @staticmethod
    def _validate_update_paths(update_spec: dict[str, Any]) -> None:
        seen_paths: list[str] = []
        for params in update_spec.values():
            if not isinstance(params, dict):
                continue
            for path in params:
                if not isinstance(path, str):
                    raise OperationFailure("update field names must be strings")
                for seen in seen_paths:
                    if path == seen or path.startswith(f"{seen}.") or seen.startswith(f"{path}."):
                        raise OperationFailure("conflicting update paths are not allowed")
                seen_paths.append(path)

    @staticmethod
    def _assert_mutable_path(path: str) -> None:
        if path == "_id" or path.startswith("_id."):
            raise OperationFailure("Modifying the immutable field '_id' is not allowed")
        for segment in path.split("."):
            if segment == "$" or (segment.startswith("$[") and segment.endswith("]")):
                raise OperationFailure("Positional and array-filter update paths are not supported")

    @staticmethod
    def _is_numeric(value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    @staticmethod
    def _iter_ordered_update_items(
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> list[tuple[str, Any]]:
        try:
            return [(path, value) for path, value in dialect.sort_update_path_items(params)]
        except TypeError as exc:
            raise OperationFailure("update field names must be strings") from exc

    @staticmethod
    def _apply_set(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            UpdateEngine._assert_mutable_path(path)
            if set_document_value(doc, path, value):
                modified = True
        return modified

    @staticmethod
    def _apply_unset(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for path, _ in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            UpdateEngine._assert_mutable_path(path)
            if delete_document_value(doc, path):
                modified = True
        return modified

    @staticmethod
    def _apply_inc(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for path, increment in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            UpdateEngine._assert_mutable_path(path)
            if not UpdateEngine._is_numeric(increment):
                raise OperationFailure("$inc requires numeric values")
            found, current = get_document_value(doc, path)
            if not found:
                if set_document_value(doc, path, increment):
                    modified = True
                continue
            if not UpdateEngine._is_numeric(current):
                raise OperationFailure("$inc requires the target field to be numeric")
            if set_document_value(doc, path, current + increment):
                modified = True
        return modified

    @staticmethod
    def _expand_array_update_values(operator: str, value: Any) -> list[Any]:
        if not isinstance(value, dict) or "$each" not in value:
            return [deepcopy(value)]
        if set(value) != {"$each"}:
            raise OperationFailure(f"{operator} only supports the $each modifier")
        each = value["$each"]
        if not isinstance(each, list):
            raise OperationFailure(f"{operator} $each requires an array")
        return deepcopy(each)

    @staticmethod
    def _apply_push(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            UpdateEngine._assert_mutable_path(path)
            values = UpdateEngine._expand_array_update_values("$push", value)
            found, current = get_document_value(doc, path)
            if not found:
                if set_document_value(doc, path, list(values)):
                    modified = True
                continue
            if not isinstance(current, list):
                raise OperationFailure("$push requires the target field to be an array")
            if values:
                current.extend(values)
                modified = True
        return modified

    @staticmethod
    def _apply_add_to_set(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            UpdateEngine._assert_mutable_path(path)
            values = UpdateEngine._expand_array_update_values("$addToSet", value)
            found, current = get_document_value(doc, path)
            if not found:
                unique_values: list[Any] = []
                for candidate in values:
                    if any(
                        QueryEngine._values_equal(existing, candidate, dialect=dialect)
                        for existing in unique_values
                    ):
                        continue
                    unique_values.append(candidate)
                if set_document_value(doc, path, unique_values):
                    modified = True
                continue
            if not isinstance(current, list):
                raise OperationFailure("$addToSet requires the target field to be an array")
            for candidate in values:
                if any(
                    QueryEngine._values_equal(existing, candidate, dialect=dialect)
                    for existing in current
                ):
                    continue
                current.append(candidate)
                modified = True
        return modified

    @staticmethod
    def _apply_pull(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            UpdateEngine._assert_mutable_path(path)
            found, current = get_document_value(doc, path)
            if not found:
                continue
            if not isinstance(current, list):
                raise OperationFailure("$pull requires the target field to be an array")
            if isinstance(value, dict) or isinstance(value, re.Pattern):
                is_predicate = True if isinstance(value, re.Pattern) else (
                    any(isinstance(key, str) and key.startswith("$") for key in value) or any(
                        isinstance(candidate, dict) and any(isinstance(key, str) and key.startswith("$") for key in candidate)
                        for candidate in value.values()
                    )
                )
                filtered = [
                    candidate
                    for candidate in current
                    if not (
                        (
                            isinstance(candidate, str) and value.search(candidate) is not None
                            if isinstance(value, re.Pattern)
                            else QueryEngine._match_elem_match_candidate(candidate, value, dialect=dialect)
                        ) if is_predicate else QueryEngine._values_equal(candidate, value, dialect=dialect)
                    )
                ]
            else:
                filtered = [
                    candidate
                    for candidate in current
                    if not QueryEngine._values_equal(candidate, value, dialect=dialect)
                ]
            if not _same_value_for_update(filtered, current):
                if set_document_value(doc, path, filtered):
                    modified = True
        return modified

    @staticmethod
    def _apply_pop(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for path, direction in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            UpdateEngine._assert_mutable_path(path)
            if isinstance(direction, bool) or direction not in (-1, 1):
                raise OperationFailure("$pop requires 1 or -1 as direction")
            found, current = get_document_value(doc, path)
            if not found:
                continue
            if not isinstance(current, list):
                raise OperationFailure("$pop requires the target field to be an array")
            if not current:
                continue
            updated = current[1:] if direction == -1 else current[:-1]
            if set_document_value(doc, path, updated):
                modified = True
        return modified
