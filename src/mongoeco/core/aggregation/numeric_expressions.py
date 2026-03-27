import datetime
import decimal
import math
from collections.abc import Callable
from copy import deepcopy
from functools import cmp_to_key
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_scalars import (
    bson_add,
    bson_bitwise,
    bson_multiply,
    bson_subtract,
    is_bson_numeric,
    unwrap_bson_numeric,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, UndefinedType


type ExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]
type ExpressionArgValidator = Callable[[str, object, int, int | None], list[Any]]


NUMERIC_EXPRESSION_OPERATORS = frozenset(
    {
        "$add",
        "$multiply",
        "$subtract",
        "$divide",
        "$mod",
        "$bitAnd",
        "$bitOr",
        "$bitXor",
        "$bitNot",
        "$abs",
        "$exp",
        "$ln",
        "$log10",
        "$sqrt",
        "$stdDevPop",
        "$stdDevSamp",
        "$median",
        "$percentile",
        "$log",
        "$pow",
        "$round",
        "$trunc",
        "$floor",
        "$ceil",
        "$range",
    }
)


def evaluate_numeric_expression(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
    require_expression_args: ExpressionArgValidator,
    missing_sentinel: object,
) -> Any:
    if operator in {"$add", "$multiply"}:
        args = require_expression_args(operator, spec, 2, None)
        raw_values = [evaluate_expression(document, item, variables) for item in args]
        if any(value is None for value in raw_values):
            return None
        if operator == "$add":
            date_values = [value for value in raw_values if isinstance(value, datetime.datetime)]
            if date_values:
                if len(date_values) != 1:
                    raise OperationFailure("$add only supports a single date argument")
                result = date_values[0]
                for value in raw_values:
                    if isinstance(value, datetime.datetime):
                        continue
                    result = _add_milliseconds(result, unwrap_bson_numeric(_require_numeric(operator, value)))
                return result
        values = [_require_numeric(operator, value) for value in raw_values]
        result = values[0]
        for value in values[1:]:
            result = bson_add(result, value) if operator == "$add" else bson_multiply(result, value)
        return result

    if operator in {"$subtract", "$divide", "$mod"}:
        args = require_expression_args(operator, spec, 2, 2)
        left_raw = evaluate_expression(document, args[0], variables)
        right_raw = evaluate_expression(document, args[1], variables)
        if left_raw is None or right_raw is None:
            return None
        if operator == "$subtract":
            return _subtract_values(left_raw, right_raw)
        left = unwrap_bson_numeric(_require_numeric(operator, left_raw))
        right = unwrap_bson_numeric(_require_numeric(operator, right_raw))
        if operator == "$divide":
            if right == 0:
                raise OperationFailure("$divide cannot divide by zero")
            return left / right
        if right == 0:
            raise OperationFailure("$mod cannot divide by zero")
        return _mongo_mod(left, right)

    if operator in {"$bitAnd", "$bitOr", "$bitXor"}:
        args = require_expression_args(operator, spec, 2, None)
        values = [_require_integral_numeric(operator, evaluate_expression(document, item, variables)) for item in args]
        result = values[0]
        for value in values[1:]:
            if operator == "$bitAnd":
                result = bson_bitwise("and", result, value)
            elif operator == "$bitOr":
                result = bson_bitwise("or", result, value)
            else:
                result = bson_bitwise("xor", result, value)
        return result

    if operator == "$bitNot":
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        value = _require_integral_numeric(operator, evaluate_expression(document, args[0], variables))
        return ~int(unwrap_bson_numeric(value))

    if operator in {"$abs", "$exp", "$ln", "$log10", "$sqrt"}:
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        raw_value = evaluate_expression_with_missing(document, args[0], variables)
        if raw_value is missing_sentinel or raw_value is None:
            return None
        value = unwrap_bson_numeric(_require_numeric(operator, raw_value))
        if operator == "$abs":
            return abs(value)
        if math.isnan(value):
            return value
        if operator == "$exp":
            return math.exp(value)
        if operator == "$sqrt":
            if value < 0:
                raise OperationFailure("$sqrt cannot operate on negative numbers")
            return math.sqrt(value)
        if value <= 0:
            raise OperationFailure(f"{operator} requires a positive numeric argument")
        return math.log(value) if operator == "$ln" else math.log10(value)

    if operator in {"$stdDevPop", "$stdDevSamp"}:
        values = _stddev_expression_values(
            document,
            spec,
            variables,
            dialect=dialect,
            evaluate_expression=evaluate_expression,
            evaluate_expression_with_missing=evaluate_expression_with_missing,
            missing_sentinel=missing_sentinel,
        )
        return _compute_stddev(values, population=operator == "$stdDevPop")

    if operator in {"$median", "$percentile"}:
        values, probabilities = _parse_percentile_spec(
            operator,
            document,
            spec,
            variables,
            dialect=dialect,
            evaluate_expression=evaluate_expression,
            evaluate_expression_with_missing=evaluate_expression_with_missing,
        )
        percentiles = _compute_percentiles(values, probabilities, dialect=dialect)
        if operator == "$median":
            return None if percentiles is None else percentiles[0]
        return percentiles

    if operator in {"$log", "$pow"}:
        args = require_expression_args(operator, spec, 2, 2)
        left_raw = evaluate_expression_with_missing(document, args[0], variables)
        right_raw = evaluate_expression_with_missing(document, args[1], variables)
        if left_raw is missing_sentinel or right_raw is missing_sentinel or left_raw is None or right_raw is None:
            return None
        left = unwrap_bson_numeric(_require_numeric(operator, left_raw))
        right = unwrap_bson_numeric(_require_numeric(operator, right_raw))
        if operator == "$pow":
            return math.pow(left, right)
        if math.isnan(left) or math.isnan(right):
            return math.nan
        if left <= 0 or right <= 0 or right == 1:
            raise OperationFailure("$log requires a positive argument and base where base != 1")
        return math.log(left, right)

    if operator in {"$round", "$trunc"}:
        args = require_expression_args(operator, spec, 1, 2)
        raw_value = evaluate_expression_with_missing(document, args[0], variables)
        if raw_value is missing_sentinel or raw_value is None:
            return None
        value = unwrap_bson_numeric(_require_numeric(operator, raw_value))
        if math.isnan(value) or math.isinf(value):
            return value
        place_raw = evaluate_expression(document, args[1], variables) if len(args) == 2 else 0
        place = _normalize_numeric_place(operator, place_raw)
        return _round_numeric(value, place) if operator == "$round" else _trunc_numeric(value, place)

    if operator in {"$floor", "$ceil"}:
        args = require_expression_args(operator, spec, 1, 1)
        raw_value = evaluate_expression(document, args[0], variables)
        if raw_value is None:
            return None
        value = unwrap_bson_numeric(_require_numeric(operator, raw_value))
        if math.isnan(value) or math.isinf(value):
            return value
        integer = int(value)
        if operator == "$floor":
            return integer if integer <= value else integer - 1
        return integer if integer >= value else integer + 1

    if operator == "$range":
        args = require_expression_args(operator, spec, 2, 3)
        start = evaluate_expression(document, args[0], variables)
        end = evaluate_expression(document, args[1], variables)
        step = evaluate_expression(document, args[2], variables) if len(args) == 3 else 1
        if not all(isinstance(value, int) and not isinstance(value, bool) for value in (start, end, step)):
            raise OperationFailure("$range requires integer arguments")
        if step == 0:
            raise OperationFailure("$range step cannot be zero")
        return list(range(start, end, step))

    raise OperationFailure(f"Unsupported numeric expression operator: {operator}")


def _require_numeric(operator: str, value: object) -> object:
    if isinstance(value, bool):
        raise OperationFailure(f"{operator} requires numeric arguments")
    if is_bson_numeric(value):
        return value
    if not isinstance(value, (int, float, decimal.Decimal)):
        raise OperationFailure(f"{operator} requires numeric arguments")
    return value


def _is_numeric(value: object) -> bool:
    return is_bson_numeric(value) or (isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool))


def _sum_accumulator_operand(value: Any) -> object | None:
    if isinstance(value, bool):
        return None
    if is_bson_numeric(value) or isinstance(value, (int, float, decimal.Decimal)):
        return value
    return None


def _stddev_accumulator_operand(value: Any) -> float | None:
    if value is None or isinstance(value, UndefinedType) or isinstance(value, bool):
        return None
    if is_bson_numeric(value):
        return float(unwrap_bson_numeric(value))
    if isinstance(value, (int, float, decimal.Decimal)):
        return float(value)
    return None


def _stddev_expression_values(
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
    missing_sentinel: object,
) -> list[float]:
    args = [spec] if not isinstance(spec, list) else spec
    values: list[float] = []
    for item in args:
        resolved = evaluate_expression_with_missing(document, item, variables)
        if resolved is missing_sentinel or resolved is None or isinstance(resolved, UndefinedType):
            continue
        candidates = resolved if isinstance(resolved, list) else [resolved]
        for candidate in candidates:
            operand = _stddev_accumulator_operand(candidate)
            if operand is None:
                continue
            values.append(operand)
    return values


def _compute_stddev(values: list[float], *, population: bool) -> float | None:
    if not values:
        return None
    if population and len(values) == 1:
        return 0.0
    if not population and len(values) < 2:
        return None
    total = sum(values)
    sum_of_squares = sum(value * value for value in values)
    if population:
        mean = total / len(values)
        variance = max((sum_of_squares / len(values)) - (mean * mean), 0.0)
    else:
        variance = max((sum_of_squares - ((total * total) / len(values))) / (len(values) - 1), 0.0)
    return math.sqrt(variance)


def _normalize_percentile_probabilities(operator: str, probabilities_value: Any) -> list[float]:
    if not isinstance(probabilities_value, list):
        raise OperationFailure(f"{operator} p must evaluate to an array")
    if not probabilities_value:
        raise OperationFailure(f"{operator} p must not be an empty array")
    normalized: list[float] = []
    for item in probabilities_value:
        if not isinstance(item, (int, float, decimal.Decimal)) or isinstance(item, bool):
            raise OperationFailure(f"{operator} p values must be numeric")
        probability = float(item)
        if probability < 0.0 or probability > 1.0:
            raise OperationFailure(f"{operator} p values must be in the range [0.0, 1.0]")
        normalized.append(probability)
    return normalized


def _extract_percentile_candidates(
    _operator: str,
    input_value: Any,
) -> list[int | float | decimal.Decimal]:
    if input_value is None or isinstance(input_value, UndefinedType):
        return []
    candidates = input_value if isinstance(input_value, list) else [input_value]
    return [candidate for candidate in candidates if _is_numeric(candidate)]


def _parse_percentile_spec(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
) -> tuple[list[int | float | decimal.Decimal], list[float]]:
    if not isinstance(spec, dict) or "input" not in spec or "method" not in spec:
        raise OperationFailure(f"{operator} requires input and method")
    method = evaluate_expression(document, spec["method"], variables)
    if method != "approximate":
        raise OperationFailure(f"{operator} method must be 'approximate'")
    input_value = evaluate_expression_with_missing(document, spec["input"], variables)
    probabilities = (
        [0.5]
        if operator == "$median"
        else _normalize_percentile_probabilities(operator, evaluate_expression(document, spec.get("p"), variables))
    )
    return _extract_percentile_candidates(operator, input_value), probabilities


def _compute_percentiles(
    values: list[int | float | decimal.Decimal],
    probabilities: list[float],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[int | float | decimal.Decimal] | None:
    if not values:
        return None
    ordered = sorted(values, key=cmp_to_key(dialect.policy.compare_values))
    results: list[int | float | decimal.Decimal] = []
    length = len(ordered)
    for probability in probabilities:
        if probability <= 0.0:
            results.append(deepcopy(ordered[0]))
            continue
        if probability >= 1.0:
            results.append(deepcopy(ordered[-1]))
            continue
        index = math.ceil(probability * length) - 1
        index = min(max(index, 0), length - 1)
        results.append(deepcopy(ordered[index]))
    return results


def _mongo_mod(left: int | float, right: int | float) -> int | float:
    if not math.isfinite(left) or not math.isfinite(right):
        return math.nan
    quotient = int(left / right)
    return left - right * quotient


def _normalize_numeric_place(operator: str, place: Any) -> int:
    if not isinstance(place, int) or isinstance(place, bool):
        raise OperationFailure(f"{operator} place must be an integer")
    if place < -20 or place > 100:
        raise OperationFailure(f"{operator} place must be between -20 and 100")
    return place


def _round_numeric(value: int | float, place: int) -> int | float:
    unwrapped = unwrap_bson_numeric(value)
    if isinstance(unwrapped, int) and place >= 0:
        return unwrapped
    quantizer = decimal.Decimal(f"1e{-place}")
    rounded = decimal.Decimal(str(unwrapped)).quantize(quantizer, rounding=decimal.ROUND_HALF_EVEN)
    return int(rounded) if place <= 0 else float(rounded)


def _trunc_numeric(value: int | float, place: int) -> int | float:
    unwrapped = unwrap_bson_numeric(value)
    if isinstance(unwrapped, int) and place >= 0:
        return unwrapped
    factor = 10 ** place if place >= 0 else 10 ** (-place)
    if place >= 0:
        truncated = math.trunc(unwrapped * factor) / factor
        return int(truncated) if place == 0 else truncated
    truncated = math.trunc(unwrapped / factor) * factor
    return int(truncated)


def _require_integral_numeric(operator: str, value: Any) -> int:
    if isinstance(value, bool):
        raise OperationFailure(f"{operator} requires integral numeric arguments")
    unwrapped = unwrap_bson_numeric(value)
    if isinstance(unwrapped, int):
        return unwrapped
    if isinstance(unwrapped, float) and unwrapped.is_integer():
        return int(unwrapped)
    raise OperationFailure(f"{operator} requires integral numeric arguments")


def _add_milliseconds(value: datetime.datetime, milliseconds: int | float) -> datetime.datetime:
    return value + datetime.timedelta(milliseconds=milliseconds)


def _subtract_values(left: Any, right: Any) -> Any:
    if isinstance(left, datetime.datetime):
        if isinstance(right, datetime.datetime):
            delta = left - right
            return int(delta.total_seconds() * 1000)
        if isinstance(right, (int, float)) and not isinstance(right, bool):
            return _add_milliseconds(left, -right)
    if isinstance(right, datetime.datetime):
        raise OperationFailure("$subtract only supports number-number, date-date, or date-number")
    return bson_subtract(_require_numeric("$subtract", left), _require_numeric("$subtract", right))
