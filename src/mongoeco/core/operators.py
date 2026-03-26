import re
from datetime import UTC, datetime
from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.errors import OperationFailure
from mongoeco.core.paths import _same_value_for_update, delete_document_value, get_document_value, set_document_value
from mongoeco.core.update_paths import expand_positional_update_paths, is_valid_array_filter_identifier, parse_update_path
from mongoeco.types import ArrayFilters, Filter


class UpdateEngine:
    """Motor central para aplicar operadores de actualización de MongoDB."""

    @staticmethod
    def apply_update(
        doc: dict[str, Any],
        update_spec: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
        is_upsert_insert: bool = False,
    ) -> bool:
        """
        Aplica las operaciones de actualización a un documento (in-place).
        Devuelve True si hubo cambios (parcialmente simplificado para v1).
        """
        if not isinstance(update_spec, dict) or not update_spec:
            raise OperationFailure("update specification must be a non-empty document")
        compiled_array_filters = UpdateEngine._compile_array_filters(array_filters)
        UpdateEngine._validate_update_paths(update_spec, compiled_array_filters)
        modified = False

        # Si el update no empieza con $, se trata como un reemplazo completo (Mongo behavior)
        # Pero en update_one normalmente se requieren operadores. Aquí forzamos operadores.
        for op, params in update_spec.items():
            if not dialect.supports_update_operator(op):
                raise OperationFailure(f"Unsupported update operator: {op}")
            if not isinstance(params, dict):
                raise OperationFailure(f"{op} requires a document specification")
            if op == "$set":
                if UpdateEngine._apply_set(doc, params, dialect=dialect, selector_filter=selector_filter, array_filters=compiled_array_filters):
                    modified = True
            elif op == "$unset":
                if UpdateEngine._apply_unset(doc, params, dialect=dialect, selector_filter=selector_filter, array_filters=compiled_array_filters):
                    modified = True
            elif op == "$inc":
                if UpdateEngine._apply_inc(doc, params, dialect=dialect, selector_filter=selector_filter, array_filters=compiled_array_filters):
                    modified = True
            elif op == "$min":
                if UpdateEngine._apply_min(doc, params, dialect=dialect, selector_filter=selector_filter, array_filters=compiled_array_filters):
                    modified = True
            elif op == "$max":
                if UpdateEngine._apply_max(doc, params, dialect=dialect, selector_filter=selector_filter, array_filters=compiled_array_filters):
                    modified = True
            elif op == "$mul":
                if UpdateEngine._apply_mul(doc, params, dialect=dialect, selector_filter=selector_filter, array_filters=compiled_array_filters):
                    modified = True
            elif op == "$rename":
                if UpdateEngine._apply_rename(doc, params, dialect=dialect):
                    modified = True
            elif op == "$currentDate":
                if UpdateEngine._apply_current_date(doc, params, dialect=dialect, selector_filter=selector_filter, array_filters=compiled_array_filters):
                    modified = True
            elif op == "$setOnInsert":
                if UpdateEngine._apply_set_on_insert(
                    doc,
                    params,
                    dialect=dialect,
                    selector_filter=selector_filter,
                    array_filters=compiled_array_filters,
                    is_upsert_insert=is_upsert_insert,
                ):
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
    def _validate_update_paths(
        update_spec: dict[str, Any],
        array_filters: dict[str, dict[str, Any]],
    ) -> None:
        seen_paths: list[str] = []
        referenced_identifiers: set[str] = set()
        for operator, params in update_spec.items():
            if not isinstance(params, dict):
                continue
            for path, value in params.items():
                if not isinstance(path, str):
                    raise OperationFailure("update field names must be strings")
                for segment in parse_update_path(path):
                    if segment.kind == "filtered_positional":
                        if segment.identifier is None or segment.identifier not in array_filters:
                            raise OperationFailure("No array filter found for identifier used in update path")
                        referenced_identifiers.add(segment.identifier)
                UpdateEngine._register_update_path(seen_paths, path)
                if operator == "$rename":
                    if not isinstance(value, str):
                        raise OperationFailure("$rename requires string target paths")
                    UpdateEngine._register_update_path(seen_paths, value)
        unused_identifiers = set(array_filters) - referenced_identifiers
        if unused_identifiers:
            raise OperationFailure("array_filters contains identifiers that are not used in the update document")

    @staticmethod
    def _compile_array_filters(array_filters: ArrayFilters | None) -> dict[str, dict[str, Any]]:
        if array_filters is None:
            return {}
        compiled: dict[str, dict[str, Any]] = {}
        for item in array_filters:
            if not isinstance(item, dict) or not item:
                raise OperationFailure("array_filters entries must be non-empty documents")
            identifier: str | None = None
            normalized: dict[str, Any] = {}
            for key, value in item.items():
                if not isinstance(key, str):
                    raise OperationFailure("array_filters field names must be strings")
                if key.startswith("$"):
                    raise OperationFailure("Top-level operators in array_filters are not supported")
                root, dot, rest = key.partition(".")
                if not is_valid_array_filter_identifier(root):
                    raise OperationFailure("array filter identifiers must begin with a lowercase letter and contain only alphanumerics")
                if identifier is None:
                    identifier = root
                elif root != identifier:
                    raise OperationFailure("each array filter document must reference exactly one identifier")
                normalized[rest if dot else ""] = value
            assert identifier is not None
            if identifier in compiled:
                raise OperationFailure("duplicate array filter identifiers are not allowed")
            compiled[identifier] = normalized
        return compiled

    @staticmethod
    def _array_filter_matches(
        identifier: str,
        candidate: Any,
        *,
        array_filters: dict[str, dict[str, Any]],
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        filter_spec = array_filters.get(identifier)
        if filter_spec is None:
            return False
        for path, condition in filter_spec.items():
            if path == "":
                if not QueryEngine._match_elem_match_candidate(candidate, condition, dialect=dialect):
                    return False
                continue
            if not isinstance(candidate, dict):
                return False
            if not QueryEngine.match(candidate, {path: condition}, dialect=dialect):
                return False
        return True

    @staticmethod
    def _expand_update_targets(
        doc: dict[str, Any],
        path: str,
        *,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]],
        allow_positional: bool,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> list[str]:
        segments = parse_update_path(path)
        if segments[0].raw == "_id":
            raise OperationFailure("Modifying the immutable field '_id' is not allowed")
        if not allow_positional and any(
            segment.kind in {"positional", "all_positional", "filtered_positional"}
            for segment in segments
        ):
            raise OperationFailure("Positional and array-filter update paths are not supported")
        if any(segment.kind == "positional" for segment in segments):
            if selector_filter is None:
                raise OperationFailure("The positional operator did not find the match needed from the query")
            return [UpdateEngine._resolve_legacy_positional_path(doc, path, selector_filter, dialect=dialect)]
        concrete_paths = expand_positional_update_paths(
            doc,
            path,
            filtered_matcher=lambda identifier, candidate: UpdateEngine._array_filter_matches(
                identifier,
                candidate,
                array_filters=array_filters,
                dialect=dialect,
            ),
        )
        return list(dict.fromkeys(concrete_paths))

    @staticmethod
    def _resolve_legacy_positional_path(
        doc: dict[str, Any],
        path: str,
        selector_filter: Filter,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> str:
        segments = parse_update_path(path)
        positional_indexes = [index for index, segment in enumerate(segments) if segment.kind == "positional"]
        if len(positional_indexes) != 1:
            raise OperationFailure("Legacy positional '$' update paths support exactly one positional segment")
        positional_index = positional_indexes[0]
        array_prefix = ".".join(segment.raw for segment in segments[:positional_index])
        suffix = ".".join(segment.raw for segment in segments[positional_index + 1 :])
        found, array_value = get_document_value(doc, array_prefix)
        if not found or not isinstance(array_value, list):
            raise OperationFailure("The positional operator did not find the match needed from the query")

        matcher = UpdateEngine._build_legacy_positional_matcher(
            array_prefix,
            selector_filter,
            dialect=dialect,
        )
        for item_index, item in enumerate(array_value):
            if matcher(item):
                parts = [array_prefix, str(item_index)]
                if suffix:
                    parts.append(suffix)
                return ".".join(parts)
        raise OperationFailure("The positional operator did not find the match needed from the query")

    @staticmethod
    def _build_legacy_positional_matcher(
        array_prefix: str,
        selector_filter: Filter,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ):
        predicates: list[tuple[str, Any]] = []

        def _collect(filter_spec: Filter) -> None:
            for key, value in filter_spec.items():
                if key == "$and" and isinstance(value, list):
                    for clause in value:
                        if isinstance(clause, dict):
                            _collect(clause)
                    continue
                if not isinstance(key, str) or key.startswith("$"):
                    continue
                if key == array_prefix:
                    predicates.append(("", value))
                    continue
                prefix = f"{array_prefix}."
                if key.startswith(prefix):
                    predicates.append((key[len(prefix):], value))

        _collect(selector_filter)
        if not predicates:
            raise OperationFailure("The positional operator did not find the match needed from the query")

        def _matches(candidate: Any) -> bool:
            for subpath, condition in predicates:
                if subpath == "":
                    if isinstance(condition, dict) and set(condition) == {"$elemMatch"}:
                        if not QueryEngine._match_elem_match_candidate(candidate, condition["$elemMatch"], dialect=dialect):
                            return False
                        continue
                    if isinstance(condition, dict) and any(
                        operator in condition for operator in ("$ne", "$nin", "$not")
                    ):
                        raise OperationFailure("Negation queries are not supported with the positional operator")
                    if not QueryEngine._match_elem_match_candidate(candidate, condition, dialect=dialect):
                        return False
                    continue
                if not isinstance(candidate, dict):
                    return False
                if not QueryEngine.match(candidate, {subpath: condition}, dialect=dialect):
                    return False
            return True

        return _matches

    @staticmethod
    def _register_update_path(seen_paths: list[str], path: str) -> None:
        for seen in seen_paths:
            if path == seen or path.startswith(f"{seen}.") or seen.startswith(f"{path}."):
                raise OperationFailure("conflicting update paths are not allowed")
        seen_paths.append(path)

    @staticmethod
    def _assert_mutable_path(path: str) -> None:
        segments = parse_update_path(path)
        if segments[0].raw == "_id":
            raise OperationFailure("Modifying the immutable field '_id' is not allowed")
        for segment in segments:
            if segment.kind in {"positional", "all_positional", "filtered_positional"}:
                raise OperationFailure("Positional and array-filter update paths are not supported")

    @staticmethod
    def _assert_rename_path(path: str) -> None:
        segments = parse_update_path(path)
        if segments[0].raw == "_id":
            raise OperationFailure("Modifying the immutable field '_id' is not allowed")
        if any(
            segment.kind in {"positional", "all_positional", "filtered_positional"}
            for segment in segments
        ):
            raise OperationFailure("Positional and array-filter update paths are not supported")
        if any(segment.kind == "index" for segment in segments):
            raise OperationFailure("$rename does not support embedded documents in arrays")

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
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        modified = False
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            for concrete_path in UpdateEngine._expand_update_targets(
                doc,
                path,
                selector_filter=selector_filter,
                array_filters=array_filters or {},
                allow_positional=True,
                dialect=dialect,
            ):
                if set_document_value(doc, concrete_path, deepcopy(value)):
                    modified = True
        return modified

    @staticmethod
    def _apply_unset(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        modified = False
        for path, _ in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            for concrete_path in UpdateEngine._expand_update_targets(
                doc,
                path,
                selector_filter=selector_filter,
                array_filters=array_filters or {},
                allow_positional=True,
                dialect=dialect,
            ):
                if delete_document_value(doc, concrete_path):
                    modified = True
        return modified

    @staticmethod
    def _apply_inc(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        modified = False
        for path, increment in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            if not UpdateEngine._is_numeric(increment):
                raise OperationFailure("$inc requires numeric values")
            for concrete_path in UpdateEngine._expand_update_targets(
                doc,
                path,
                selector_filter=selector_filter,
                array_filters=array_filters or {},
                allow_positional=True,
                dialect=dialect,
            ):
                found, current = get_document_value(doc, concrete_path)
                if not found:
                    if set_document_value(doc, concrete_path, increment):
                        modified = True
                    continue
                if not UpdateEngine._is_numeric(current):
                    raise OperationFailure("$inc requires the target field to be numeric")
                if set_document_value(doc, concrete_path, current + increment):
                    modified = True
        return modified

    @staticmethod
    def _apply_min(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        modified = False
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            for concrete_path in UpdateEngine._expand_update_targets(
                doc,
                path,
                selector_filter=selector_filter,
                array_filters=array_filters or {},
                allow_positional=True,
                dialect=dialect,
            ):
                found, current = get_document_value(doc, concrete_path)
                if not found or dialect.compare_values(current, value) > 0:
                    if set_document_value(doc, concrete_path, deepcopy(value)):
                        modified = True
        return modified

    @staticmethod
    def _apply_max(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        modified = False
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            for concrete_path in UpdateEngine._expand_update_targets(
                doc,
                path,
                selector_filter=selector_filter,
                array_filters=array_filters or {},
                allow_positional=True,
                dialect=dialect,
            ):
                found, current = get_document_value(doc, concrete_path)
                if not found or dialect.compare_values(current, value) < 0:
                    if set_document_value(doc, concrete_path, deepcopy(value)):
                        modified = True
        return modified

    @staticmethod
    def _apply_mul(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        modified = False
        for path, factor in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            if not UpdateEngine._is_numeric(factor):
                raise OperationFailure("$mul requires numeric values")
            for concrete_path in UpdateEngine._expand_update_targets(
                doc,
                path,
                selector_filter=selector_filter,
                array_filters=array_filters or {},
                allow_positional=True,
                dialect=dialect,
            ):
                found, current = get_document_value(doc, concrete_path)
                if not found:
                    missing_value: int | float = 0.0 if isinstance(factor, float) else 0
                    if set_document_value(doc, concrete_path, missing_value):
                        modified = True
                    continue
                if not UpdateEngine._is_numeric(current):
                    raise OperationFailure("$mul requires the target field to be numeric")
                if set_document_value(doc, concrete_path, current * factor):
                    modified = True
        return modified

    @staticmethod
    def _apply_rename(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for source_path, target_path in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            if not isinstance(target_path, str):
                raise OperationFailure("$rename requires string target paths")
            UpdateEngine._assert_rename_path(source_path)
            UpdateEngine._assert_rename_path(target_path)
            found, current = get_document_value(doc, source_path)
            if not found:
                continue
            delete_document_value(doc, target_path)
            set_document_value(doc, target_path, deepcopy(current))
            delete_document_value(doc, source_path)
            modified = True
        return modified

    @staticmethod
    def _apply_current_date(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        modified = False
        now = datetime.now(UTC).replace(tzinfo=None)
        for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
            if value is True:
                replacement = now
            elif isinstance(value, dict) and set(value) == {"$type"} and value["$type"] == "date":
                replacement = now
            else:
                raise OperationFailure("$currentDate only supports True or {$type: 'date'}")
            for concrete_path in UpdateEngine._expand_update_targets(
                doc,
                path,
                selector_filter=selector_filter,
                array_filters=array_filters or {},
                allow_positional=True,
                dialect=dialect,
            ):
                if set_document_value(doc, concrete_path, replacement):
                    modified = True
        return modified

    @staticmethod
    def _apply_set_on_insert(
        doc: dict[str, Any],
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]] | None = None,
        is_upsert_insert: bool = False,
    ) -> bool:
        if not is_upsert_insert:
            return False
        return UpdateEngine._apply_set(
            doc,
            params,
            dialect=dialect,
            selector_filter=selector_filter,
            array_filters=array_filters,
        )

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
