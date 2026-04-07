from __future__ import annotations

import re
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation import CompiledPipelinePlan, Pipeline, apply_pipeline, compile_pipeline
from mongoeco.core.collation import CollationSpec, normalize_collation
from mongoeco.core.bson_scalars import is_bson_numeric, unwrap_bson_numeric
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.errors import OperationFailure
from mongoeco.core.paths import get_document_value
from mongoeco.core.update_paths import (
    CompiledUpdateInstruction,
    CompiledUpdatePath,
    compile_update_path,
    is_valid_array_filter_identifier,
    ResolvedUpdatePath,
    resolve_positional_update_paths,
)
from mongoeco.types import ArrayFilters, Filter, Regex, SortSpec, Update
from mongoeco.core.update_array_operators import (
    apply_add_to_set,
    apply_pop,
    apply_pull,
    apply_pull_all,
    apply_push,
)
from mongoeco.core.update_scalar_operators import (
    apply_bit,
    apply_current_date,
    apply_inc,
    apply_max,
    apply_min,
    apply_mul,
    apply_rename,
    apply_set,
    apply_set_on_insert,
    apply_unset,
)


UpdateOperatorHandler = Callable[[dict[str, Any], tuple[CompiledUpdateInstruction, ...], "UpdateExecutionContext"], bool]


def _unsupported_update_operator_handler(operator: str) -> UpdateOperatorHandler:
    def _raise(
        doc: dict[str, Any],
        instructions: tuple[CompiledUpdateInstruction, ...],
        context: "UpdateExecutionContext",
    ) -> bool:
        del doc, instructions, context
        raise OperationFailure(f"Unsupported update operator: {operator}")

    return _raise


@dataclass(frozen=True, slots=True)
class CompiledUpdateOperator:
    operator: str
    instructions: tuple[CompiledUpdateInstruction, ...]
    handler: UpdateOperatorHandler

    def apply(
        self,
        doc: dict[str, Any],
        *,
        context: "UpdateExecutionContext",
    ) -> bool:
        return self.handler(doc, self.instructions, context)


@dataclass(frozen=True, slots=True)
class UpdateExecutionContext:
    dialect: MongoDialect = MONGODB_DIALECT_70
    collation: CollationSpec | None = None
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
class CompiledUpdatePipelinePlan:
    update_spec: Pipeline
    context: UpdateExecutionContext
    variables: dict[str, Any] | None = None
    compiled_pipeline: CompiledPipelinePlan | None = None

    def apply(self, doc: dict[str, Any]) -> bool:
        original = deepcopy(doc)
        working = deepcopy(doc)
        if self.compiled_pipeline is not None:
            result = self.compiled_pipeline.execute([working], variables=self.variables)
        else:
            result = apply_pipeline(
                [working],
                self.update_spec,
                variables=self.variables,
                dialect=self.context.dialect,
                collation=self.context.collation,
            )
        if len(result) != 1 or not isinstance(result[0], dict):
            raise OperationFailure("Update pipeline must produce a single document")
        replacement = result[0]
        modified = not self.context.dialect.values_equal(original, replacement)
        if modified:
            doc.clear()
            doc.update(replacement)
        return modified


type CompiledExecutableUpdatePlan = CompiledUpdatePlan | CompiledUpdatePipelinePlan


@dataclass(frozen=True, slots=True)
class ResolvedInstructionApplication:
    instruction: CompiledUpdateInstruction
    targets: tuple[ResolvedUpdatePath, ...]


class UpdateEngine:
    """Motor central para aplicar operadores de actualización de MongoDB."""

    _UPDATE_PIPELINE_STAGE_OPERATORS = frozenset(
        {"$addFields", "$set", "$project", "$unset", "$replaceRoot", "$replaceWith"}
    )

    _OPERATOR_HANDLERS: dict[str, UpdateOperatorHandler] = {
        "$set": lambda doc, instructions, context: apply_set(doc, instructions, context=context, helpers=UpdateEngine),
        "$unset": lambda doc, instructions, context: apply_unset(doc, instructions, context=context, helpers=UpdateEngine),
        "$inc": lambda doc, instructions, context: apply_inc(doc, instructions, context=context, helpers=UpdateEngine),
        "$min": lambda doc, instructions, context: apply_min(doc, instructions, context=context, helpers=UpdateEngine),
        "$max": lambda doc, instructions, context: apply_max(doc, instructions, context=context, helpers=UpdateEngine),
        "$mul": lambda doc, instructions, context: apply_mul(doc, instructions, context=context, helpers=UpdateEngine),
        "$bit": lambda doc, instructions, context: apply_bit(doc, instructions, context=context, helpers=UpdateEngine),
        "$rename": lambda doc, instructions, context: apply_rename(doc, instructions, context=context, helpers=UpdateEngine),
        "$currentDate": lambda doc, instructions, context: apply_current_date(doc, instructions, context=context, helpers=UpdateEngine),
        "$setOnInsert": lambda doc, instructions, context: apply_set_on_insert(doc, instructions, context=context, helpers=UpdateEngine),
        "$push": lambda doc, instructions, context: apply_push(doc, instructions, context=context, helpers=UpdateEngine),
        "$addToSet": lambda doc, instructions, context: apply_add_to_set(doc, instructions, context=context, helpers=UpdateEngine),
        "$pull": lambda doc, instructions, context: apply_pull(doc, instructions, context=context, helpers=UpdateEngine),
        "$pullAll": lambda doc, instructions, context: apply_pull_all(doc, instructions, context=context, helpers=UpdateEngine),
        "$pop": lambda doc, instructions, context: apply_pop(doc, instructions, context=context, helpers=UpdateEngine),
    }

    @staticmethod
    def apply_update(
        doc: dict[str, Any],
        update_spec: Update,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
        is_upsert_insert: bool = False,
        variables: dict[str, Any] | None = None,
        context: UpdateExecutionContext | None = None,
    ) -> bool:
        """
        Aplica las operaciones de actualización a un documento (in-place).
        Devuelve True si hubo cambios (parcialmente simplificado para v1).
        """
        return UpdateEngine.apply_compiled_update(
            doc,
            UpdateEngine.compile_update_plan(
                update_spec,
                dialect=dialect,
                collation=collation,
                selector_filter=selector_filter,
                array_filters=array_filters,
                is_upsert_insert=is_upsert_insert,
                variables=variables,
                context=context,
            ),
        )

    @staticmethod
    def compile_update_plan(
        update_spec: Update,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
        is_upsert_insert: bool = False,
        variables: dict[str, Any] | None = None,
        context: UpdateExecutionContext | None = None,
    ) -> CompiledExecutableUpdatePlan:
        execution = context or UpdateEngine.build_execution_context(
            dialect=dialect,
            collation=collation,
            selector_filter=selector_filter,
            array_filters=array_filters,
            is_upsert_insert=is_upsert_insert,
        )
        if isinstance(update_spec, list):
            return UpdateEngine.compile_update_pipeline_plan(
                update_spec,
                context=execution,
                variables=variables,
            )
        if not isinstance(update_spec, dict) or not update_spec:
            raise OperationFailure("update specification must be a non-empty document")
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
        plan: CompiledExecutableUpdatePlan,
    ) -> bool:
        return plan.apply(doc)

    @staticmethod
    def compile_update_pipeline_plan(
        update_spec: Pipeline,
        *,
        context: UpdateExecutionContext,
        variables: dict[str, Any] | None = None,
    ) -> CompiledUpdatePipelinePlan:
        UpdateEngine.validate_update_pipeline(update_spec, dialect=context.dialect)
        if context.raw_array_filters is not None:
            raise OperationFailure("arrayFilters may not be specified for pipeline-style updates")
        apply_pipeline(
            [],
            update_spec,
            variables=variables,
            dialect=context.dialect,
            collation=context.collation,
        )
        return CompiledUpdatePipelinePlan(
            update_spec=list(update_spec),
            context=context,
            variables=variables,
            compiled_pipeline=compile_pipeline(
                update_spec,
                dialect=context.dialect,
                collation=context.collation,
            ),
        )

    @staticmethod
    def build_execution_context(
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
        is_upsert_insert: bool = False,
    ) -> UpdateExecutionContext:
        return UpdateExecutionContext(
            dialect=dialect,
            collation=_normalize_update_collation(collation),
            selector_filter=selector_filter,
            raw_array_filters=array_filters,
            compiled_array_filters=UpdateEngine._compile_array_filters(array_filters),
            is_upsert_insert=is_upsert_insert,
        )

    @staticmethod
    def validate_update_pipeline(
        update_spec: object,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> None:
        if not isinstance(update_spec, list) or not update_spec:
            raise OperationFailure("update pipeline must be a non-empty list")
        for stage in update_spec:
            if not isinstance(stage, dict) or len(stage) != 1:
                raise OperationFailure("Each update pipeline stage must be a single-key document")
            operator = next(iter(stage))
            if not isinstance(operator, str) or not operator.startswith("$"):
                raise OperationFailure("Update pipeline stage operator must start with '$'")
            if operator not in UpdateEngine._UPDATE_PIPELINE_STAGE_OPERATORS:
                raise OperationFailure(f"Unsupported update pipeline stage: {operator}")
            if not dialect.supports_aggregation_stage(operator):
                raise OperationFailure(f"Unsupported update pipeline stage: {operator}")

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
                    handler=UpdateEngine._OPERATOR_HANDLERS.get(operator)
                    or _unsupported_update_operator_handler(operator),
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
        collation: CollationSpec | None = None,
    ) -> bool:
        filter_spec = array_filters.get(identifier)
        if filter_spec is None:
            return False
        for path, condition in filter_spec.items():
            if path == "":
                if not QueryEngine._match_elem_match_candidate(
                    candidate,
                    condition,
                    dialect=dialect,
                    collation=collation,
                ):
                    return False
                continue
            if not isinstance(candidate, dict):
                return False
            if not QueryEngine.match(
                candidate,
                {path: condition},
                dialect=dialect,
                collation=collation,
            ):
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
        collation: CollationSpec | None = None,
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
        concrete_paths = resolve_positional_update_paths(
            doc,
            compiled_path,
            filtered_matcher=lambda identifier, candidate: UpdateEngine._array_filter_matches(
                identifier,
                candidate,
                array_filters=array_filters,
                dialect=dialect,
                collation=collation,
            ),
            legacy_matcher=(
                None
                if selector_filter is None or not any(segment.kind == "positional" for segment in segments)
                else UpdateEngine._build_legacy_positional_matcher(
                    ".".join(segment.raw for segment in segments[: next(index for index, segment in enumerate(segments) if segment.kind == "positional")]),
                    selector_filter,
                    dialect=dialect,
                )
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
                        collation=context.collation,
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
        matcher = UpdateEngine._build_legacy_positional_matcher(
            array_prefix,
            selector_filter,
            dialect=dialect,
        )
        resolved = resolve_positional_update_paths(
            doc,
            compiled_path,
            filtered_matcher=lambda _identifier, _candidate: False,
            legacy_matcher=matcher,
        )
        if not resolved:
            raise OperationFailure("The positional operator did not find the match needed from the query")
        return resolved[0]

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


def _normalize_update_collation(collation: object | None) -> CollationSpec | None:
    if collation is None or isinstance(collation, CollationSpec):
        return collation
    return normalize_collation(collation)
