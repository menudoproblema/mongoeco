import re
from functools import cmp_to_key
from datetime import UTC, datetime
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_scalars import bson_add, bson_bitwise, bson_multiply, is_bson_numeric, unwrap_bson_numeric
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.errors import OperationFailure
from mongoeco.core.paths import _same_value_for_update, delete_document_value, get_document_value, set_document_value
from mongoeco.core.sorting import sort_documents
from mongoeco.core.update_paths import (
    CompiledUpdateInstruction,
    CompiledUpdatePath,
    compile_update_path,
    is_valid_array_filter_identifier,
    ResolvedUpdatePath,
    resolve_positional_update_paths,
)
from mongoeco.types import ArrayFilters, Filter, Regex, SortSpec


@dataclass(frozen=True, slots=True)
class CompiledUpdateOperator:
    operator: str
    instructions: tuple[CompiledUpdateInstruction, ...]

    def apply(
        self,
        doc: dict[str, Any],
        *,
        context: "UpdateExecutionContext",
    ) -> bool:
        handler_name = UpdateEngine._OPERATOR_HANDLERS.get(self.operator)
        if handler_name is None:
            raise OperationFailure(f"Unsupported update operator: {self.operator}")
        handler = getattr(UpdateEngine, handler_name)
        if self.operator == "$rename":
            return handler(doc, self.instructions, dialect=context.dialect)
        return handler(doc, self.instructions, context=context)


@dataclass(frozen=True, slots=True)
class UpdateExecutionContext:
    dialect: MongoDialect = MONGODB_DIALECT_70
    selector_filter: Filter | None = None
    raw_array_filters: ArrayFilters | None = None
    compiled_array_filters: dict[str, dict[str, Any]] = field(default_factory=dict)
    is_upsert_insert: bool = False


@dataclass(frozen=True, slots=True)
class CompiledUpdatePlan:
    update_spec: dict[str, Any]
    compiled_operators: tuple[CompiledUpdateOperator, ...]
    context: UpdateExecutionContext

    def apply(self, doc: dict[str, Any]) -> bool:
        modified = False
        for compiled_operator in self.compiled_operators:
            if compiled_operator.apply(doc, context=self.context):
                modified = True
        return modified


@dataclass(frozen=True, slots=True)
class ResolvedInstructionApplication:
    instruction: CompiledUpdateInstruction
    targets: tuple[ResolvedUpdatePath, ...]


class UpdateEngine:
    """Motor central para aplicar operadores de actualización de MongoDB."""

    _OPERATOR_HANDLERS: dict[str, str] = {
        "$set": "_apply_set",
        "$unset": "_apply_unset",
        "$inc": "_apply_inc",
        "$min": "_apply_min",
        "$max": "_apply_max",
        "$mul": "_apply_mul",
        "$bit": "_apply_bit",
        "$rename": "_apply_rename",
        "$currentDate": "_apply_current_date",
        "$setOnInsert": "_apply_set_on_insert",
        "$push": "_apply_push",
        "$addToSet": "_apply_add_to_set",
        "$pull": "_apply_pull",
        "$pullAll": "_apply_pull_all",
        "$pop": "_apply_pop",
    }

    @staticmethod
    def apply_update(
        doc: dict[str, Any],
        update_spec: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
        is_upsert_insert: bool = False,
        context: UpdateExecutionContext | None = None,
    ) -> bool:
        """
        Aplica las operaciones de actualización a un documento (in-place).
        Devuelve True si hubo cambios (parcialmente simplificado para v1).
        """
        if not isinstance(update_spec, dict) or not update_spec:
            raise OperationFailure("update specification must be a non-empty document")
        return UpdateEngine.apply_compiled_update(
            doc,
            UpdateEngine.compile_update_plan(
                update_spec,
                dialect=dialect,
                selector_filter=selector_filter,
                array_filters=array_filters,
                is_upsert_insert=is_upsert_insert,
                context=context,
            ),
        )

    @staticmethod
    def compile_update_plan(
        update_spec: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
        is_upsert_insert: bool = False,
        context: UpdateExecutionContext | None = None,
    ) -> CompiledUpdatePlan:
        if not isinstance(update_spec, dict) or not update_spec:
            raise OperationFailure("update specification must be a non-empty document")
        execution = context or UpdateEngine.build_execution_context(
            dialect=dialect,
            selector_filter=selector_filter,
            array_filters=array_filters,
            is_upsert_insert=is_upsert_insert,
        )
        return CompiledUpdatePlan(
            update_spec=update_spec,
            compiled_operators=UpdateEngine._compile_update_spec(
                update_spec,
                execution.compiled_array_filters,
                dialect=execution.dialect,
            ),
            context=execution,
        )

    @staticmethod
    def apply_compiled_update(
        doc: dict[str, Any],
        plan: CompiledUpdatePlan,
    ) -> bool:
        return plan.apply(doc)

    @staticmethod
    def build_execution_context(
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
        is_upsert_insert: bool = False,
    ) -> UpdateExecutionContext:
        return UpdateExecutionContext(
            dialect=dialect,
            selector_filter=selector_filter,
            raw_array_filters=array_filters,
            compiled_array_filters=UpdateEngine._compile_array_filters(array_filters),
            is_upsert_insert=is_upsert_insert,
        )

    @staticmethod
    def _compile_update_spec(
        update_spec: dict[str, Any],
        array_filters: dict[str, dict[str, Any]],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> tuple[CompiledUpdateOperator, ...]:
        seen_paths: list[str] = []
        referenced_identifiers: set[str] = set()
        compiled_operators: list[CompiledUpdateOperator] = []
        for operator, params in update_spec.items():
            if not dialect.supports_update_operator(operator):
                raise OperationFailure(f"Unsupported update operator: {operator}")
            if not isinstance(params, dict):
                raise OperationFailure(f"{operator} requires a document specification")
            instructions: list[CompiledUpdateInstruction] = []
            for path, value in UpdateEngine._iter_ordered_update_items(params, dialect=dialect):
                compiled_path = compile_update_path(path)
                for segment in compiled_path.segments:
                    if segment.kind == "filtered_positional":
                        if segment.identifier is None or segment.identifier not in array_filters:
                            raise OperationFailure("No array filter found for identifier used in update path")
                        referenced_identifiers.add(segment.identifier)
                UpdateEngine._register_update_path(seen_paths, compiled_path.raw)
                target_path: CompiledUpdatePath | None = None
                if operator == "$rename":
                    if not isinstance(value, str):
                        raise OperationFailure("$rename requires string target paths")
                    target_path = compile_update_path(value)
                    UpdateEngine._register_update_path(seen_paths, target_path.raw)
                    UpdateEngine._assert_rename_path(compiled_path)
                    UpdateEngine._assert_rename_path(target_path)
                elif operator in {"$push", "$addToSet", "$pull", "$pullAll", "$pop"}:
                    UpdateEngine._assert_mutable_path(
                        compiled_path,
                        allow_positional=True,
                    )
                instructions.append(
                    CompiledUpdateInstruction(
                        operator=operator,
                        path=compiled_path,
                        value=value,
                        target_path=target_path,
                    )
                )
            compiled_operators.append(
                CompiledUpdateOperator(
                    operator=operator,
                    instructions=tuple(instructions),
                )
            )
        unused_identifiers = set(array_filters) - referenced_identifiers
        if unused_identifiers:
            raise OperationFailure("array_filters contains identifiers that are not used in the update document")
        return tuple(compiled_operators)

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
    def _resolve_update_targets(
        doc: dict[str, Any],
        path: str | CompiledUpdatePath,
        *,
        selector_filter: Filter | None = None,
        array_filters: dict[str, dict[str, Any]],
        allow_positional: bool,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> list[ResolvedUpdatePath]:
        compiled_path = path if isinstance(path, CompiledUpdatePath) else compile_update_path(path)
        segments = compiled_path.segments
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
            return [
                UpdateEngine._resolve_legacy_positional_path(
                    doc,
                    compiled_path,
                    selector_filter,
                    dialect=dialect,
                )
            ]
        concrete_paths = resolve_positional_update_paths(
            doc,
            compiled_path,
            filtered_matcher=lambda identifier, candidate: UpdateEngine._array_filter_matches(
                identifier,
                candidate,
                array_filters=array_filters,
                dialect=dialect,
            ),
        )
        deduplicated: list[ResolvedUpdatePath] = []
        seen_paths: set[str] = set()
        for target in concrete_paths:
            if target.concrete_path in seen_paths:
                continue
            deduplicated.append(target)
            seen_paths.add(target.concrete_path)
        return deduplicated

    @staticmethod
    def _resolve_instruction_applications(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
        allow_positional: bool,
    ) -> tuple[ResolvedInstructionApplication, ...]:
        return tuple(
            ResolvedInstructionApplication(
                instruction=instruction,
                targets=tuple(
                    UpdateEngine._resolve_update_targets(
                        doc,
                        instruction.path,
                        selector_filter=context.selector_filter,
                        array_filters=context.compiled_array_filters,
                        allow_positional=allow_positional,
                        dialect=context.dialect,
                    )
                ),
            )
            for instruction in instructions
        )

    @staticmethod
    def _resolve_legacy_positional_path(
        doc: dict[str, Any],
        path: str | CompiledUpdatePath,
        selector_filter: Filter,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> ResolvedUpdatePath:
        compiled_path = path if isinstance(path, CompiledUpdatePath) else compile_update_path(path)
        segments = compiled_path.segments
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
                return ResolvedUpdatePath(
                    requested=compiled_path,
                    concrete_path=".".join(parts),
                )
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
    def _assert_mutable_path(
        path: str | CompiledUpdatePath,
        *,
        allow_positional: bool = False,
    ) -> None:
        compiled = path if isinstance(path, CompiledUpdatePath) else compile_update_path(path)
        segments = compiled.segments
        if segments[0].raw == "_id":
            raise OperationFailure("Modifying the immutable field '_id' is not allowed")
        if not allow_positional:
            for segment in segments:
                if segment.kind in {"positional", "all_positional", "filtered_positional"}:
                    raise OperationFailure("Positional and array-filter update paths are not supported")

    @staticmethod
    def _assert_rename_path(path: str | CompiledUpdatePath) -> None:
        compiled = path if isinstance(path, CompiledUpdatePath) else compile_update_path(path)
        segments = compiled.segments
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
        return is_bson_numeric(value)

    @staticmethod
    def _is_integral(value: Any) -> bool:
        unwrapped = unwrap_bson_numeric(value)
        return isinstance(unwrapped, int) and not isinstance(unwrapped, bool)

    @staticmethod
    def _iter_ordered_update_items(
        params: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> list[tuple[str, Any]]:
        try:
            return [(path, value) for path, value in dialect.policy.sort_update_path_items(params)]
        except TypeError as exc:
            raise OperationFailure("update field names must be strings") from exc

    @staticmethod
    def _apply_set(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            for target in application.targets:
                if set_document_value(
                    doc,
                    target.concrete_path,
                    deepcopy(application.instruction.value),
                ):
                    modified = True
        return modified

    @staticmethod
    def _apply_unset(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            for target in application.targets:
                if delete_document_value(doc, target.concrete_path):
                    modified = True
        return modified

    @staticmethod
    def _apply_inc(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            increment = application.instruction.value
            if not UpdateEngine._is_numeric(increment):
                raise OperationFailure("$inc requires numeric values")
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found:
                    if set_document_value(doc, target.concrete_path, increment):
                        modified = True
                    continue
                if not UpdateEngine._is_numeric(current):
                    raise OperationFailure("$inc requires the target field to be numeric")
                if set_document_value(doc, target.concrete_path, bson_add(current, increment)):
                    modified = True
        return modified

    @staticmethod
    def _apply_min(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            value = application.instruction.value
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found or context.dialect.policy.compare_values(current, value) > 0:
                    if set_document_value(doc, target.concrete_path, deepcopy(value)):
                        modified = True
        return modified

    @staticmethod
    def _apply_max(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            value = application.instruction.value
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found or context.dialect.policy.compare_values(current, value) < 0:
                    if set_document_value(doc, target.concrete_path, deepcopy(value)):
                        modified = True
        return modified

    @staticmethod
    def _apply_mul(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            factor = application.instruction.value
            if not UpdateEngine._is_numeric(factor):
                raise OperationFailure("$mul requires numeric values")
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found:
                    missing_value = bson_multiply(
                        0.0 if isinstance(unwrap_bson_numeric(factor), float) else 0,
                        factor,
                    )
                    if set_document_value(doc, target.concrete_path, missing_value):
                        modified = True
                    continue
                if not UpdateEngine._is_numeric(current):
                    raise OperationFailure("$mul requires the target field to be numeric")
                if set_document_value(doc, target.concrete_path, bson_multiply(current, factor)):
                    modified = True
        return modified

    @staticmethod
    def _apply_bit(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            bit_spec = application.instruction.value
            if not isinstance(bit_spec, dict):
                raise OperationFailure("$bit requires a document of bitwise operations")
            if not bit_spec:
                continue
            if len(bit_spec) != 1:
                raise OperationFailure("$bit requires exactly one of and, or, or xor")
            operator, operand = next(iter(bit_spec.items()))
            if operator not in {"and", "or", "xor"}:
                raise OperationFailure("$bit only supports and, or, and xor")
            if not UpdateEngine._is_integral(operand):
                raise OperationFailure("$bit requires integer operands")
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found:
                    raise OperationFailure("$bit requires the target field to exist and be an integer")
                if not UpdateEngine._is_integral(current):
                    raise OperationFailure("$bit requires the target field to be an integer")
                replacement = bson_bitwise(operator, current, operand)
                if set_document_value(doc, target.concrete_path, replacement):
                    modified = True
        return modified

    @staticmethod
    def _apply_rename(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> bool:
        modified = False
        for instruction in instructions:
            if instruction.target_path is None:
                raise OperationFailure("$rename requires string target paths")
            found, current = get_document_value(doc, instruction.path.raw)
            if not found:
                continue
            delete_document_value(doc, instruction.target_path.raw)
            set_document_value(doc, instruction.target_path.raw, deepcopy(current))
            delete_document_value(doc, instruction.path.raw)
            modified = True
        return modified

    @staticmethod
    def _apply_current_date(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        now = datetime.now(UTC).replace(tzinfo=None)
        for application in UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        ):
            value = application.instruction.value
            if value is True:
                replacement = now
            elif isinstance(value, dict) and set(value) == {"$type"} and value["$type"] == "date":
                replacement = now
            else:
                raise OperationFailure("$currentDate only supports True or {$type: 'date'}")
            for target in application.targets:
                if set_document_value(doc, target.concrete_path, replacement):
                    modified = True
        return modified

    @staticmethod
    def _apply_set_on_insert(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        if not context.is_upsert_insert:
            return False
        return UpdateEngine._apply_set(
            doc,
            instructions,
            context=context,
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
    def _normalize_push_modifiers(
        value: Any,
    ) -> tuple[list[Any], int | None, int | None, SortSpec | int | None]:
        if not isinstance(value, dict) or "$each" not in value:
            return [deepcopy(value)], None, None, None

        unsupported = set(value) - {"$each", "$position", "$slice", "$sort"}
        if unsupported:
            unsupported_names = ", ".join(sorted(unsupported))
            raise OperationFailure(
                f"$push only supports the $each, $position, $slice and $sort modifiers: {unsupported_names}"
            )

        each = value["$each"]
        if not isinstance(each, list):
            raise OperationFailure("$push $each requires an array")

        position = value.get("$position")
        if position is not None and (
            not isinstance(position, int) or isinstance(position, bool)
        ):
            raise OperationFailure("$push $position must be an integer")

        slice_value = value.get("$slice")
        if slice_value is not None and (
            not isinstance(slice_value, int) or isinstance(slice_value, bool)
        ):
            raise OperationFailure("$push $slice must be an integer")

        sort_value = value.get("$sort")
        sort_spec: SortSpec | int | None = None
        if sort_value is not None:
            if sort_value in (1, -1) and not isinstance(sort_value, bool):
                sort_spec = sort_value
            elif isinstance(sort_value, dict):
                normalized: SortSpec = []
                for field, direction in sort_value.items():
                    if not isinstance(field, str):
                        raise OperationFailure("$push $sort fields must be strings")
                    if direction not in (1, -1) or isinstance(direction, bool):
                        raise OperationFailure("$push $sort directions must be 1 or -1")
                    normalized.append((field, direction))
                sort_spec = normalized
            else:
                raise OperationFailure("$push $sort must be 1, -1 or a document")

        return deepcopy(each), position, slice_value, sort_spec

    @staticmethod
    def _apply_push_values(
        current: list[Any],
        values: list[Any],
        *,
        position: int | None,
        slice_value: int | None,
        sort_spec: SortSpec | int | None,
        dialect: MongoDialect,
    ) -> bool:
        if not values and slice_value is None and sort_spec is None:
            return False

        if values:
            if position is None:
                current.extend(values)
            else:
                insertion_index = position if position >= 0 else len(current) + position
                insertion_index = min(max(insertion_index, 0), len(current))
                for offset, value in enumerate(values):
                    current.insert(insertion_index + offset, value)

        if sort_spec is not None:
            if isinstance(sort_spec, int):
                current.sort(
                    key=cmp_to_key(dialect.policy.compare_values),
                    reverse=sort_spec == -1,
                )
            else:
                if any(not isinstance(item, dict) for item in current):
                    raise OperationFailure(
                        "$push $sort with a document specification requires array elements to be documents"
                    )
                current[:] = sort_documents(current, sort_spec, dialect=dialect)

        if slice_value is not None:
            current[:] = current[:slice_value] if slice_value >= 0 else current[slice_value:]

        return bool(values) or sort_spec is not None or slice_value is not None

    @staticmethod
    def _resolve_array_instruction_applications(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> tuple[ResolvedInstructionApplication, ...]:
        return UpdateEngine._resolve_instruction_applications(
            doc,
            instructions,
            context=context,
            allow_positional=True,
        )

    @staticmethod
    def _get_or_create_array_target(
        doc: dict[str, Any],
        concrete_path: str,
        *,
        operator_name: str,
    ) -> tuple[list[Any], bool]:
        found, current = get_document_value(doc, concrete_path)
        if not found:
            current = []
            set_document_value(doc, concrete_path, current)
            return current, True
        if not isinstance(current, list):
            raise OperationFailure(f"{operator_name} requires the target field to be an array")
        return current, False

    @staticmethod
    def _apply_push(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_array_instruction_applications(
            doc,
            instructions,
            context=context,
        ):
            values, position, slice_value, sort_spec = UpdateEngine._normalize_push_modifiers(
                application.instruction.value
            )
            for target in application.targets:
                current, _created = UpdateEngine._get_or_create_array_target(
                    doc,
                    target.concrete_path,
                    operator_name="$push",
                )
                if UpdateEngine._apply_push_values(
                    current,
                    values,
                    position=position,
                    slice_value=slice_value,
                    sort_spec=sort_spec,
                    dialect=context.dialect,
                ):
                    modified = True
        return modified

    @staticmethod
    def _apply_add_to_set(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_array_instruction_applications(
            doc,
            instructions,
            context=context,
        ):
            values = UpdateEngine._expand_array_update_values(
                "$addToSet",
                application.instruction.value,
            )
            for target in application.targets:
                current, created = UpdateEngine._get_or_create_array_target(
                    doc,
                    target.concrete_path,
                    operator_name="$addToSet",
                )
                if created:
                    unique_values: list[Any] = []
                    for candidate in values:
                        if any(
                            QueryEngine._values_equal(
                                existing,
                                candidate,
                                dialect=context.dialect,
                            )
                            for existing in unique_values
                        ):
                            continue
                        unique_values.append(candidate)
                    if set_document_value(doc, target.concrete_path, unique_values):
                        modified = True
                    continue
                for candidate in values:
                    if any(
                        QueryEngine._values_equal(existing, candidate, dialect=context.dialect)
                        for existing in current
                    ):
                        continue
                    current.append(candidate)
                    modified = True
        return modified

    @staticmethod
    def _apply_pull(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_array_instruction_applications(
            doc,
            instructions,
            context=context,
        ):
            value = application.instruction.value
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found:
                    continue
                if not isinstance(current, list):
                    raise OperationFailure("$pull requires the target field to be an array")
                if isinstance(value, dict) or isinstance(value, (re.Pattern, Regex)):
                    compiled_regex = value.compile() if isinstance(value, Regex) else value
                    is_predicate = True if isinstance(value, (re.Pattern, Regex)) else (
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
                                isinstance(candidate, str) and compiled_regex.search(candidate) is not None
                                if isinstance(value, (re.Pattern, Regex))
                                else QueryEngine._match_elem_match_candidate(
                                    candidate,
                                    value,
                                    dialect=context.dialect,
                                )
                            ) if is_predicate else QueryEngine._values_equal(
                                candidate,
                                value,
                                dialect=context.dialect,
                            )
                        )
                    ]
                else:
                    filtered = [
                        candidate
                        for candidate in current
                        if not QueryEngine._values_equal(candidate, value, dialect=context.dialect)
                    ]
                if not _same_value_for_update(filtered, current):
                    if set_document_value(doc, target.concrete_path, filtered):
                        modified = True
        return modified

    @staticmethod
    def _apply_pull_all(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_array_instruction_applications(
            doc,
            instructions,
            context=context,
        ):
            value = application.instruction.value
            if not isinstance(value, list):
                raise OperationFailure("$pullAll requires an array of values")
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found:
                    continue
                if not isinstance(current, list):
                    raise OperationFailure("$pullAll requires the target field to be an array")
                filtered = [
                    candidate
                    for candidate in current
                    if not any(
                        QueryEngine._values_equal(candidate, removal, dialect=context.dialect)
                        for removal in value
                    )
                ]
                if not _same_value_for_update(filtered, current):
                    if set_document_value(doc, target.concrete_path, filtered):
                        modified = True
        return modified

    @staticmethod
    def _apply_pop(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        *,
        context: UpdateExecutionContext,
    ) -> bool:
        modified = False
        for application in UpdateEngine._resolve_array_instruction_applications(
            doc,
            instructions,
            context=context,
        ):
            direction = application.instruction.value
            if isinstance(direction, bool) or direction not in (-1, 1):
                raise OperationFailure("$pop requires 1 or -1 as direction")
            for target in application.targets:
                found, current = get_document_value(doc, target.concrete_path)
                if not found:
                    continue
                if not isinstance(current, list):
                    raise OperationFailure("$pop requires the target field to be an array")
                if not current:
                    continue
                updated = current[1:] if direction == -1 else current[:-1]
                if set_document_value(doc, target.concrete_path, updated):
                    modified = True
        return modified
