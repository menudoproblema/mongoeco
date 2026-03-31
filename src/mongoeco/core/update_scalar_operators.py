from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from mongoeco.core.bson_scalars import bson_add, bson_bitwise, bson_multiply, unwrap_bson_numeric
from mongoeco.core.paths import delete_document_value, get_document_value, set_document_value
from mongoeco.errors import OperationFailure
from mongoeco.types import Timestamp

if TYPE_CHECKING:
    from mongoeco.core.operators import UpdateExecutionContext
    from mongoeco.core.update_paths import CompiledUpdateInstruction


_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def _path_array_prefixes(path: str) -> tuple[str, ...]:
    parts = path.split(".")
    prefixes: list[str] = []
    current: list[str] = []
    for index, part in enumerate(parts[:-1]):
        current.append(part)
        next_part = parts[index + 1]
        if not part.isdigit() and not next_part.isdigit():
            prefixes.append(".".join(current))
    return tuple(prefixes)


def _document_traverses_array_on_field(document: dict[str, Any], field: str) -> bool:
    for prefix in _path_array_prefixes(field):
        found, value = get_document_value(document, prefix)
        if found and isinstance(value, list):
            return True
    return False


def _is_signed_int64(value: Any) -> bool:
    unwrapped = unwrap_bson_numeric(value)
    return isinstance(unwrapped, int) and not isinstance(unwrapped, bool) and _INT64_MIN <= unwrapped <= _INT64_MAX


def apply_set(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in helpers._resolve_instruction_applications(
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


def apply_unset(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in helpers._resolve_instruction_applications(
        doc,
        instructions,
        context=context,
        allow_positional=True,
    ):
        for target in application.targets:
            if delete_document_value(doc, target.concrete_path):
                modified = True
    return modified


def apply_inc(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in helpers._resolve_instruction_applications(
        doc,
        instructions,
        context=context,
        allow_positional=True,
    ):
        increment = application.instruction.value
        if not helpers._is_numeric(increment):
            raise OperationFailure("$inc requires numeric values")
        for target in application.targets:
            found, current = get_document_value(doc, target.concrete_path)
            if not found:
                if set_document_value(doc, target.concrete_path, increment):
                    modified = True
                continue
            if not helpers._is_numeric(current):
                raise OperationFailure("$inc requires the target field to be numeric")
            if set_document_value(doc, target.concrete_path, bson_add(current, increment)):
                modified = True
    return modified


def apply_min(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in helpers._resolve_instruction_applications(
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


def apply_max(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in helpers._resolve_instruction_applications(
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


def apply_mul(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in helpers._resolve_instruction_applications(
        doc,
        instructions,
        context=context,
        allow_positional=True,
    ):
        factor = application.instruction.value
        if not helpers._is_numeric(factor):
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
            if not helpers._is_numeric(current):
                raise OperationFailure("$mul requires the target field to be numeric")
            if set_document_value(doc, target.concrete_path, bson_multiply(current, factor)):
                modified = True
    return modified


def apply_bit(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    for application in helpers._resolve_instruction_applications(
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
        if not helpers._is_integral(operand) or not _is_signed_int64(operand):
            raise OperationFailure("$bit requires signed 64-bit integer operands")
        for target in application.targets:
            found, current = get_document_value(doc, target.concrete_path)
            if not found:
                raise OperationFailure("$bit requires the target field to exist and be an integer")
            if not helpers._is_integral(current) or not _is_signed_int64(current):
                raise OperationFailure("$bit requires the target field to be a signed 64-bit integer")
            replacement = bson_bitwise(operator, current, operand)
            if set_document_value(doc, target.concrete_path, replacement):
                modified = True
    return modified


def apply_rename(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    del context, helpers
    modified = False
    for instruction in instructions:
        if instruction.target_path is None:
            raise OperationFailure("$rename requires string target paths")
        if _document_traverses_array_on_field(doc, instruction.path.raw) or _document_traverses_array_on_field(
            doc,
            instruction.target_path.raw,
        ):
            raise OperationFailure("Cannot use $rename when source or target path crosses an array")
        found, current = get_document_value(doc, instruction.path.raw)
        if not found:
            continue
        delete_document_value(doc, instruction.target_path.raw)
        set_document_value(doc, instruction.target_path.raw, deepcopy(current))
        delete_document_value(doc, instruction.path.raw)
        modified = True
    return modified


def apply_current_date(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    modified = False
    now = datetime.now(UTC).replace(tzinfo=None)
    now_seconds = int(now.replace(tzinfo=UTC).timestamp())
    timestamp_inc = 0
    for application in helpers._resolve_instruction_applications(
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
        elif isinstance(value, dict) and set(value) == {"$type"} and value["$type"] == "timestamp":
            timestamp_inc += 1
            replacement = Timestamp(now_seconds, timestamp_inc)
        else:
            raise OperationFailure("$currentDate only supports True or {$type: 'date'/'timestamp'}")
        for target in application.targets:
            if set_document_value(doc, target.concrete_path, replacement):
                modified = True
    return modified


def apply_set_on_insert(
    doc: dict[str, Any],
    instructions: tuple["CompiledUpdateInstruction", ...],
    *,
    context: "UpdateExecutionContext",
    helpers: Any,
) -> bool:
    if not context.is_upsert_insert:
        return False
    return apply_set(
        doc,
        instructions,
        context=context,
        helpers=helpers,
    )
