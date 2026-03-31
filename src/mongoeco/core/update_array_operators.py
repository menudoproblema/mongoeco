from __future__ import annotations

import re
from functools import cmp_to_key
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from mongoeco.core.filtering import QueryEngine
from mongoeco.core.paths import _same_value_for_update, get_document_value, set_document_value
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import OperationFailure
from mongoeco.types import Regex, SortSpec

if TYPE_CHECKING:
    from mongoeco.core.operators import ResolvedInstructionApplication, UpdateExecutionContext
    from mongoeco.core.update_paths import CompiledUpdateInstruction


def expand_array_update_values(operator: str, value: Any) -> list[Any]:
    if not isinstance(value, dict) or "$each" not in value:
        return [deepcopy(value)]
    if set(value) != {"$each"}:
        raise OperationFailure(f"{operator} only supports the $each modifier")
    each = value["$each"]
    if not isinstance(each, list):
        raise OperationFailure(f"{operator} $each requires an array")
    return deepcopy(each)


def normalize_push_modifiers(
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


def apply_push_values(
    current: list[Any],
    values: list[Any],
    *,
    position: int | None,
    slice_value: int | None,
    sort_spec: SortSpec | int | None,
    dialect,
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


def resolve_array_instruction_applications(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> tuple["ResolvedInstructionApplication", ...]:
    return helpers._resolve_instruction_applications(
        doc,
        instructions,
        context=context,
        allow_positional=True,
    )


def get_or_create_array_target(
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


def apply_push(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in resolve_array_instruction_applications(
        doc,
        instructions,
        context=context,
        helpers=helpers,
    ):
        values, position, slice_value, sort_spec = normalize_push_modifiers(
            application.instruction.value
        )
        for target in application.targets:
            current, _created = get_or_create_array_target(
                doc,
                target.concrete_path,
                operator_name="$push",
            )
            if apply_push_values(
                current,
                values,
                position=position,
                slice_value=slice_value,
                sort_spec=sort_spec,
                dialect=context.dialect,
            ):
                modified = True
    return modified


def apply_add_to_set(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in resolve_array_instruction_applications(
        doc,
        instructions,
        context=context,
        helpers=helpers,
    ):
        values = expand_array_update_values(
            "$addToSet",
            application.instruction.value,
        )
        for target in application.targets:
            current, created = get_or_create_array_target(
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
                            collation=context.collation,
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
                    QueryEngine._values_equal(
                        existing,
                        candidate,
                        dialect=context.dialect,
                        collation=context.collation,
                    )
                    for existing in current
                ):
                    continue
                current.append(candidate)
                modified = True
    return modified


def apply_pull(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in resolve_array_instruction_applications(
        doc,
        instructions,
        context=context,
        helpers=helpers,
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
                filtered = [
                    candidate
                    for candidate in current
                    if not (
                        isinstance(candidate, str) and compiled_regex.search(candidate) is not None
                        if isinstance(value, (re.Pattern, Regex))
                        else QueryEngine._match_elem_match_candidate(
                            candidate,
                            value,
                            dialect=context.dialect,
                            collation=context.collation,
                        )
                    )
                ]
            else:
                filtered = [
                    candidate
                    for candidate in current
                    if not QueryEngine._values_equal(
                        candidate,
                        value,
                        dialect=context.dialect,
                        collation=context.collation,
                    )
                ]
            if not _same_value_for_update(filtered, current):
                if set_document_value(doc, target.concrete_path, filtered):
                    modified = True
    return modified


def apply_pull_all(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in resolve_array_instruction_applications(
        doc,
        instructions,
        context=context,
        helpers=helpers,
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
                    QueryEngine._values_equal(
                        candidate,
                        removal,
                        dialect=context.dialect,
                        collation=context.collation,
                    )
                    for removal in value
                )
            ]
            if not _same_value_for_update(filtered, current):
                if set_document_value(doc, target.concrete_path, filtered):
                    modified = True
    return modified


def apply_pop(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in resolve_array_instruction_applications(
        doc,
        instructions,
        context=context,
        helpers=helpers,
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
