from collections.abc import Callable
from copy import deepcopy
from functools import cmp_to_key
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, UndefinedType


type ExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]
type ExpressionArgValidator = Callable[[str, object, int, int | None], list[Any]]
type ComparisonEvaluator = Callable[[Any, Any, str], bool]
type TruthinessEvaluator = Callable[[Any], bool]
type ArrayValidator = Callable[[str, object], list[Any]]
type PickNInputEvaluator = Callable[[str, Document, object, dict[str, Any] | None], tuple[Any, int]]


CONTROL_OBJECT_EXPRESSION_OPERATORS = frozenset(
    {
        "$eq",
        "$ne",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$and",
        "$or",
        "$in",
        "$ifNull",
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


def _is_field_path_expression(value: object) -> bool:
    return isinstance(value, str) and value.startswith("$") and not value.startswith("$$") and value != "$"


def _evaluate_field_bound_match_expression(
    operator: str,
    document: Document,
    spec: object,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> bool | None:
    if not isinstance(spec, list) or len(spec) != 2:
        return None
    field_expression, raw_conditions = spec
    if not _is_field_path_expression(field_expression) or not isinstance(raw_conditions, list):
        return None
    field_path = field_expression[1:]
    clauses = [{field_path: deepcopy(condition)} for condition in raw_conditions]
    return QueryEngine.match(document, {operator: clauses}, dialect=dialect)


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
        args = require_expression_args(operator, spec, 2, 2)
        left = evaluate_expression(document, args[0], variables)
        right = evaluate_expression(document, args[1], variables)
        return compare_values(left, right, operator)

    if operator == "$and":
        field_bound = _evaluate_field_bound_match_expression(
            operator,
            document,
            spec,
            dialect=dialect,
        )
        if field_bound is not None:
            return field_bound
        args = require_expression_args(operator, spec, 0, None)
        return all(expression_truthy(evaluate_expression(document, item, variables)) for item in args)

    if operator == "$or":
        field_bound = _evaluate_field_bound_match_expression(
            operator,
            document,
            spec,
            dialect=dialect,
        )
        if field_bound is not None:
            return field_bound
        args = require_expression_args(operator, spec, 0, None)
        return any(expression_truthy(evaluate_expression(document, item, variables)) for item in args)

    if operator == "$in":
        args = require_expression_args(operator, spec, 2, 2)
        needle = evaluate_expression(document, args[0], variables)
        haystack = evaluate_expression(document, args[1], variables)
        if haystack is None:
            return None
        if not isinstance(haystack, list):
            raise OperationFailure("$in requires the second argument to evaluate to a list")
        return any(QueryEngine._values_equal(needle, item, dialect=dialect) for item in haystack)

    if operator == "$ifNull":
        args = require_expression_args(operator, spec, 2, None)
        for item in args:
            value = evaluate_expression(document, item, variables)
            if value is not None and not isinstance(value, UndefinedType):
                return value
        return None

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

    if operator == "$setField":
        if not isinstance(spec, dict) or not {"field", "input", "value"} <= set(spec):
            raise OperationFailure("$setField requires field, input, and value")
        field_name = evaluate_expression(document, spec["field"], variables)
        if not isinstance(field_name, str):
            raise OperationFailure("$setField field must resolve to a string")
        input_value = evaluate_expression_with_missing(document, spec["input"], variables)
        if input_value is missing_sentinel or input_value is None:
            return None
        if not isinstance(input_value, dict):
            raise OperationFailure("$setField input must resolve to an object")
        result = deepcopy(input_value)
        result[field_name] = evaluate_expression(document, spec["value"], variables)
        return result

    if operator == "$unsetField":
        if not isinstance(spec, dict) or not {"field", "input"} <= set(spec):
            raise OperationFailure("$unsetField requires field and input")
        field_name = evaluate_expression(document, spec["field"], variables)
        if not isinstance(field_name, str):
            raise OperationFailure("$unsetField field must resolve to a string")
        input_value = evaluate_expression_with_missing(document, spec["input"], variables)
        if input_value is missing_sentinel or input_value is None or isinstance(input_value, UndefinedType):
            return None
        if not isinstance(input_value, dict):
            raise OperationFailure("$unsetField input must resolve to an object")
        result = deepcopy(input_value)
        result.pop(field_name, None)
        return result

    if operator == "$switch":
        if not isinstance(spec, dict) or "branches" not in spec:
            raise OperationFailure("$switch requires branches")
        branches = spec["branches"]
        if not isinstance(branches, list) or not branches:
            raise OperationFailure("$switch branches must be a non-empty array")
        for branch in branches:
            if not isinstance(branch, dict) or set(branch) != {"case", "then"}:
                raise OperationFailure("$switch branches must contain case and then")
            condition_value = evaluate_expression(document, branch["case"], variables)
            if expression_truthy(condition_value):
                return evaluate_expression(document, branch["then"], variables)
        if "default" in spec:
            return evaluate_expression(document, spec["default"], variables)
        raise OperationFailure("$switch could not find a matching branch for an input, and no default was specified")

    if operator == "$let":
        if not isinstance(spec, dict) or "vars" not in spec or "in" not in spec or not isinstance(spec["vars"], dict):
            raise OperationFailure("$let requires vars and in")
        scoped = dict(variables or {})
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
                key=cmp_to_key(dialect.policy.compare_values),
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
            if value is None:
                continue
            if allow_array_operand and isinstance(value, list):
                for element in value:
                    if element is None:
                        continue
                    if not isinstance(element, dict):
                        raise OperationFailure("$mergeObjects requires document operands")
                    merged.update(deepcopy(element))
                continue
            if not isinstance(value, dict):
                raise OperationFailure("$mergeObjects requires document operands")
            merged.update(deepcopy(value))
        return merged

    if operator == "$getField":
        if isinstance(spec, str):
            field_name = spec
            source = document
        elif isinstance(spec, dict):
            if "field" not in spec:
                raise OperationFailure("$getField requires field")
            field_name = evaluate_expression(document, spec["field"], variables)
            source = evaluate_expression(document, spec.get("input", "$$CURRENT"), variables)
        else:
            raise OperationFailure("$getField requires a string or document specification")
        if field_name is None:
            return None
        if not isinstance(field_name, str):
            raise OperationFailure("$getField field must evaluate to a string")
        if not isinstance(source, dict):
            return None
        return deepcopy(source.get(field_name))

    raise OperationFailure(f"Unsupported control/object expression operator: {operator}")
