from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from functools import cmp_to_key
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.compat._catalog_operators import CONTROL_EXPRESSION_ARGUMENTS
from mongoeco.core.aggregation.evaluation_environment import (
    scoped_environment,
)
from mongoeco.core.aggregation.expression_values import expression_values_equal
from mongoeco.core.aggregation.scalar_expressions import _aggregation_type_name
from mongoeco.core.bson_values import is_bson_string
from mongoeco.core.collation import compare_with_collation
from mongoeco.core.expression_context import validate_user_variable_names
from mongoeco.core.server_errors import server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, UndefinedType


type ExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]
type ExpressionArgValidator = Callable[[str, object, int, int | None], list[Any]]
type ComparisonEvaluator = Callable[[Any, Any, str], bool]
type TruthinessEvaluator = Callable[[Any], bool]
type ArrayValidator = Callable[[str, object], list[Any]]
type PickNInputEvaluator = Callable[
    [str, Document, object, dict[str, Any] | None], tuple[Any, int]
]


CONTROL_OBJECT_EXPRESSION_OPERATORS = frozenset(
    CONTROL_EXPRESSION_ARGUMENTS
) | frozenset(
    {
        "$cond",
        "$setField",
        "$unsetField",
        "$switch",
        "$let",
        "$firstN",
        "$lastN",
        "$maxN",
        "$minN",
        "$mergeObjects",
        "$getField",
    }
)


def parse_field_expression(operator, spec, *, parse_operand=None):
    """Own the syntax of field expressions without evaluating input documents."""
    getter = operator == "$getField"
    if getter and not isinstance(spec, dict):
        parsed = {"field": spec, "input": "$$CURRENT"}
        if parse_operand is not None:
            for operand in parsed.values():
                parse_operand(operand)
    else:
        if not isinstance(spec, dict):
            message = f"{operator} only supports an object as its argument"
            raise server_error(message, code=4161100, reason="expression_structure")
        allowed = {"field", "input"} | ({"value"} if operator == "$setField" else set())
        for key, operand in spec.items():
            if key not in allowed:
                message = f"{operator} found an unknown argument: {key}"
                raise server_error(
                    message,
                    code=3041701 if getter else 4161101,
                    reason="expression_structure",
                )
            if parse_operand is not None:
                parse_operand(operand)
        required = (
            ("field", 3041702 if getter else 4161102),
            ("value", 4161103),
            ("input", 3041703 if getter else 4161109),
        )
        for name, code in required:
            if name not in allowed:
                continue
            if name not in spec:
                message = f"{operator} requires '{name}' to be specified"
                raise server_error(message, code=code, reason="expression_structure")
        parsed = spec
    return parsed


def prepare_field_expression(operator, spec, *, dialect):
    parsed = parse_field_expression(operator, spec)
    getter = operator == "$getField"
    if not getter or not dialect.behavior_flag(
        "supports_dynamic_get_field", default=False
    ):
        _prepare_constant_field(parsed["field"], getter=getter)
    return parsed


def _prepare_constant_field(expression, *, getter):
    name = "$getField" if getter else "$setField"
    if is_bson_string(expression) and expression.startswith("$"):
        path = (
            "$" + expression[2:]
            if expression.startswith("$$")
            else "$CURRENT." + expression[1:]
        )
        message = (
            f"'{path}' is a field path reference which is not allowed in this context. "
            f"Did you mean {{$literal: '{path}'}}?"
        )
        raise server_error(
            message, code=5654600 if getter else 4161108, reason="expression_structure"
        )
    if (
        isinstance(expression, dict)
        and len(expression) == 1
        and next(iter(expression)) in {"$literal", "$const"}
    ):
        value = next(iter(expression.values()))
    elif isinstance(expression, (dict, list)):
        message = (
            f"{name} requires 'field' to evaluate to a constant, "
            "but got a non-constant argument"
        )
        raise server_error(
            message, code=5654601 if getter else 4161106, reason="expression_structure"
        )
    else:
        value = expression
    if not is_bson_string(value):
        kind = _aggregation_type_name(value, missing_sentinel=None)
        # null is a BSON value here, never the internal missing sentinel.
        kind = "null" if value is None else kind
        message = f"{name} requires 'field' to evaluate to type String, but got {kind}"
        raise server_error(
            message, code=5654602 if getter else 4161107, reason="expression_structure"
        )


def evaluate_control_object_expression(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
    require_expression_args: ExpressionArgValidator,
    compare_values: ComparisonEvaluator,
    expression_truthy: TruthinessEvaluator,
    require_array: ArrayValidator,
    evaluate_pick_n_input: PickNInputEvaluator,
    missing_sentinel: object,
) -> Any:
    if operator in {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}:
        args = require_expression_args(
            operator, spec, *CONTROL_EXPRESSION_ARGUMENTS[operator]
        )
        left = evaluate_expression(document, args[0], variables)
        right = evaluate_expression(document, args[1], variables)
        return compare_values(left, right, operator)

    if operator == "$and":
        args = require_expression_args(
            operator, spec, *CONTROL_EXPRESSION_ARGUMENTS[operator]
        )
        return all(
            expression_truthy(evaluate_expression(document, item, variables))
            for item in args
        )

    if operator == "$or":
        args = require_expression_args(
            operator, spec, *CONTROL_EXPRESSION_ARGUMENTS[operator]
        )
        return any(
            expression_truthy(evaluate_expression(document, item, variables))
            for item in args
        )

    if operator == "$in":
        args = require_expression_args(
            operator, spec, *CONTROL_EXPRESSION_ARGUMENTS[operator]
        )
        needle = evaluate_expression(document, args[0], variables)
        haystack = evaluate_expression(document, args[1], variables)
        if not isinstance(haystack, list):
            kind = _aggregation_type_name(haystack, missing_sentinel=missing_sentinel)
            message = f"$in requires an array as a second argument, found: {kind}"
            raise server_error(message, code=40081, reason="expression_type")
        return any(
            expression_values_equal(
                needle,
                item,
                dialect=dialect,
                collation=getattr(variables, "collation", None),
            )
            for item in haystack
        )

    if operator == "$not":
        args = require_expression_args(
            operator, spec, *CONTROL_EXPRESSION_ARGUMENTS[operator]
        )
        return not expression_truthy(evaluate_expression(document, args[0], variables))

    if operator == "$ifNull":
        args = require_expression_args(
            operator, spec, *CONTROL_EXPRESSION_ARGUMENTS[operator]
        )
        for item in args[:-1]:
            value = evaluate_expression(document, item, variables)
            if (
                value is not None
                and value is not missing_sentinel
                and not isinstance(value, UndefinedType)
            ):
                return value
        return evaluate_expression(document, args[-1], variables)

    if operator == "$cond":
        if isinstance(spec, list):
            args = require_expression_args(operator, spec, 3, 3)
            condition, when_true, when_false = args
        elif isinstance(spec, dict):
            if not {"if", "then", "else"} <= set(spec):
                raise OperationFailure("$cond object form requires if, then and else")
            condition = spec["if"]
            when_true = spec["then"]
            when_false = spec["else"]
        else:
            raise OperationFailure("$cond requires a list or document specification")
        condition_value = evaluate_expression(document, condition, variables)
        branch = when_true if expression_truthy(condition_value) else when_false
        return evaluate_expression(document, branch, variables)

    if operator in {"$setField", "$unsetField"}:
        parsed = prepare_field_expression(operator, spec, dialect=dialect)
        field_name = evaluate_expression(document, parsed["field"], variables)
        input_value = evaluate_expression_with_missing(
            document, parsed["input"], variables
        )
        if (
            input_value is missing_sentinel
            or input_value is None
            or isinstance(input_value, UndefinedType)
        ):
            return None
        if not isinstance(input_value, dict):
            message = "$setField requires 'input' to evaluate to type Object"
            raise server_error(message, code=4161105, reason="expression_type")
        result = deepcopy(input_value)
        value = (
            evaluate_expression(document, parsed["value"], variables)
            if operator == "$setField"
            else missing_sentinel
        )
        if value is missing_sentinel:
            result.pop(field_name, None)
        else:
            result[field_name] = value
        return result

    if operator == "$switch":
        if not isinstance(spec, dict) or "branches" not in spec:
            raise OperationFailure("$switch requires branches")
        branches = spec["branches"]
        if not isinstance(branches, list) or not branches:
            raise OperationFailure("$switch branches must be a non-empty array")
        for branch in branches:
            if (
                not isinstance(branch, dict)
                or "case" not in branch
                or "then" not in branch
            ):
                raise OperationFailure("$switch branches must contain case and then")
            condition_value = evaluate_expression(document, branch["case"], variables)
            if expression_truthy(condition_value):
                return evaluate_expression(document, branch["then"], variables)
        if "default" in spec:
            return evaluate_expression(document, spec["default"], variables)
        raise OperationFailure(
            "$switch could not find a matching branch for an input, and no default was specified"
        )

    if operator == "$let":
        if (
            not isinstance(spec, dict)
            or "vars" not in spec
            or "in" not in spec
            or not isinstance(spec["vars"], dict)
        ):
            raise OperationFailure("$let requires vars and in")
        validate_user_variable_names(spec["vars"])
        scoped = scoped_environment(variables)
        for name, value_expression in spec["vars"].items():
            scoped[name] = evaluate_expression(document, value_expression, variables)
        return evaluate_expression(document, spec["in"], scoped)

    if operator in {"$firstN", "$lastN", "$maxN", "$minN"}:
        value, size = evaluate_pick_n_input(operator, document, spec, variables)
        if value is None:
            return None
        values = require_array(operator, value)
        if operator in {"$maxN", "$minN"}:
            filtered = [
                deepcopy(item)
                for item in values
                if item is not None and not isinstance(item, UndefinedType)
            ]
            filtered.sort(
                key=cmp_to_key(
                    lambda left, right: compare_with_collation(
                        left,
                        right,
                        dialect=dialect,
                        collation=getattr(variables, "collation", None),
                    )
                ),
                reverse=operator == "$maxN",
            )
            return filtered[:size]
        if size >= len(values):
            return deepcopy(values)
        if operator == "$firstN":
            return deepcopy(values[:size])
        return deepcopy(values[-size:])

    if operator == "$mergeObjects":
        args = spec if isinstance(spec, list) else [spec]
        if not args:
            raise OperationFailure("$mergeObjects requires at least 1 arguments")
        merged: dict[str, Any] = {}
        allow_array_operand = not isinstance(spec, list)
        for item in args:
            value = evaluate_expression(document, item, variables)
            if value is None or value is missing_sentinel:
                continue
            if allow_array_operand and isinstance(value, list):
                for element in value:
                    if element is None:
                        continue
                    if not isinstance(element, dict):
                        raise OperationFailure(
                            "$mergeObjects requires document operands"
                        )
                    merged.update(deepcopy(element))
                continue
            if not isinstance(value, dict):
                raise OperationFailure("$mergeObjects requires document operands")
            merged.update(deepcopy(value))
        return merged

    if operator == "$getField":
        parsed = prepare_field_expression(operator, spec, dialect=dialect)
        field_name = evaluate_expression(document, parsed["field"], variables)
        if not is_bson_string(field_name):
            kind = _aggregation_type_name(field_name, missing_sentinel=missing_sentinel)
            message = (
                f"$getField requires 'field' to evaluate to type String, but got {kind}"
            )
            raise server_error(message, code=3041704, reason="expression_type")
        source = evaluate_expression_with_missing(document, parsed["input"], variables)
        if source is None or isinstance(source, UndefinedType):
            return None
        if not isinstance(source, dict) or field_name not in source:
            return missing_sentinel
        return deepcopy(source[field_name])

    raise OperationFailure(
        f"Unsupported control/object expression operator: {operator}"
    )
