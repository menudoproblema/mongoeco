import decimal
import math
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from functools import cmp_to_key
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.paths import get_document_value
from mongoeco.core.bson_scalars import bson_add, is_bson_numeric, unwrap_bson_numeric
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, SortSpec, UndefinedType

from mongoeco.core.aggregation.numeric_expressions import (
    _compute_percentiles,
    _parse_percentile_spec,
    _require_integral_numeric,
)


type ExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]
type MissingExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]


_ACCUMULATOR_FLAGS_KEY = ("__mongoeco_internal__", "accumulator_flags")
_ACCUMULATORS_WITH_INTERNAL_EVALUATION = frozenset(
    {
        "$firstN",
        "$lastN",
        "$maxN",
        "$minN",
        "$top",
        "$bottom",
        "$topN",
        "$bottomN",
        "$median",
        "$percentile",
    }
)
type PreparedAccumulatorSpecs = tuple[tuple[str, str, object], ...]
type AccumulatorSpecsInput = dict[str, object] | PreparedAccumulatorSpecs | None


@dataclass(slots=True)
class _AverageAccumulator:
    total: object = 0
    count: int = 0


@dataclass(slots=True)
class _StdDevAccumulator:
    population: bool
    total: float = 0.0
    sum_of_squares: float = 0.0
    count: int = 0
    invalid: bool = False


@dataclass(slots=True)
class _PickNAccumulator:
    items: list[Any] = field(default_factory=list)
    n: int | None = None


@dataclass(slots=True)
class _OrderedAccumulator:
    items: list[tuple[list[Any], Any, int]] = field(default_factory=list)
    n: int | None = None
    sequence: int = 0
    sort_spec: SortSpec | None = None


@dataclass(slots=True)
class _PercentileAccumulator:
    values: list[int | float | decimal.Decimal] = field(default_factory=list)
    probabilities: list[float] | None = None
    scalar_output: bool = False


@dataclass(slots=True)
class _AccumulatorBucket:
    bucket_id: Any
    values: dict[str, Any]
    flags: dict[str, bool] = field(default_factory=dict)
    include_bucket_id: bool = True


def _accumulator_flags(bucket: dict[object, Any] | _AccumulatorBucket) -> dict[str, bool]:
    if isinstance(bucket, _AccumulatorBucket):
        return bucket.flags
    flags = bucket.get(_ACCUMULATOR_FLAGS_KEY)
    if isinstance(flags, dict):
        return flags
    new_flags: dict[str, bool] = {}
    bucket[_ACCUMULATOR_FLAGS_KEY] = new_flags
    return new_flags


def _validate_accumulator_expression(operator: str, expression: object) -> None:
    if operator == "$count":
        if not isinstance(expression, dict) or expression:
            raise OperationFailure("$count accumulator requires an empty document")
    if operator in {"$firstN", "$lastN", "$maxN", "$minN"}:
        if not isinstance(expression, dict) or not {"input", "n"} <= set(expression):
            raise OperationFailure(f"{operator} requires input and n")
    if operator in {"$top", "$bottom"}:
        if not isinstance(expression, dict) or not {"sortBy", "output"} <= set(expression):
            raise OperationFailure(f"{operator} requires sortBy and output")
    if operator in {"$topN", "$bottomN"}:
        if not isinstance(expression, dict) or not {"sortBy", "output", "n"} <= set(expression):
            raise OperationFailure(f"{operator} requires sortBy, output and n")
    if operator == "$median":
        if not isinstance(expression, dict) or not {"input", "method"} <= set(expression):
            raise OperationFailure("$median requires input and method")
    if operator == "$percentile":
        if not isinstance(expression, dict) or not {"input", "p", "method"} <= set(expression):
            raise OperationFailure("$percentile requires input, p and method")


def _is_numeric(value: object) -> bool:
    return is_bson_numeric(value) or (isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool))


def _sum_accumulator_operand(value: Any) -> object | None:
    if _is_numeric(value):
        return value
    if isinstance(value, list):
        numeric_items = [item for item in value if _is_numeric(item)]
        if not numeric_items:
            return None
        total: object = 0
        for item in numeric_items:
            total = bson_add(total, item)
        return total
    return None


def _stddev_accumulator_operand(value: Any) -> float | None:
    if isinstance(value, bool) or value is None or isinstance(value, list):
        return None
    if not _is_numeric(value):
        return None
    numeric_value = float(unwrap_bson_numeric(value))
    if not math.isfinite(numeric_value):
        return math.nan
    return numeric_value


def _normalize_pick_n_size(operator: str, value: Any) -> int:
    size = _require_integral_numeric(operator, value)
    if size < 1:
        raise OperationFailure(f"{operator} n must be a positive integer")
    return size


def _evaluate_pick_n_input(
    operator: str,
    document: Document,
    expression: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: MissingExpressionEvaluator,
    missing_sentinel: object,
) -> tuple[Any, int]:
    if not isinstance(expression, dict) or not {"input", "n"} <= set(expression):
        raise OperationFailure(f"{operator} requires input and n")
    value = evaluate_expression_with_missing(document, expression["input"], variables or {})
    if value is missing_sentinel or isinstance(value, UndefinedType):
        value = None
    size = _normalize_pick_n_size(
        operator,
        evaluate_expression(document, expression["n"], variables),
    )
    return value, size


def _evaluate_pick_n_size(
    operator: str,
    document: Document,
    expression: object,
    variables: dict[str, Any] | None,
    *,
    evaluate_expression: ExpressionEvaluator,
) -> int:
    if not isinstance(expression, dict) or "n" not in expression:
        raise OperationFailure(f"{operator} requires input and n")
    return _normalize_pick_n_size(
        operator,
        evaluate_expression(document, expression["n"], variables),
    )


def _evaluate_ordered_accumulator_input(
    operator: str,
    document: Document,
    expression: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    require_sort: Callable[[object], SortSpec],
    resolve_aggregation_field_path: Callable[[Any, str], Any],
    missing_sentinel: object,
) -> tuple[SortSpec, list[Any], Any, int | None]:
    if not isinstance(expression, dict) or "sortBy" not in expression or "output" not in expression:
        raise OperationFailure(f"{operator} requires sortBy and output")
    sort_spec = require_sort(expression["sortBy"])
    sort_values: list[Any] = []
    for field, _direction in sort_spec:
        resolved = resolve_aggregation_field_path(document, field)
        sort_values.append(None if resolved is missing_sentinel else resolved)
    output = evaluate_expression(document, expression["output"], variables)
    size: int | None = None
    if operator in {"$topN", "$bottomN"}:
        if "n" not in expression:
            raise OperationFailure(f"{operator} requires n")
        size = _normalize_pick_n_size(
            operator,
            evaluate_expression(document, expression["n"], variables),
        )
    return sort_spec, sort_values, output, size


def _compare_ordered_accumulator_items(
    left: tuple[list[Any], Any, int],
    right: tuple[list[Any], Any, int],
    sort_spec: SortSpec,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    reverse_tie_break: bool = False,
) -> int:
    left_sort_values, _left_output, left_sequence = left
    right_sort_values, _right_output, right_sequence = right
    for index, (_field, direction) in enumerate(sort_spec):
        comparison = dialect.policy.compare_values(left_sort_values[index], right_sort_values[index])
        if comparison == 0:
            continue
        return comparison if direction == 1 else -comparison
    if left_sequence == right_sequence:
        return 0
    if reverse_tie_break:
        return -1 if left_sequence > right_sequence else 1
    return -1 if left_sequence < right_sequence else 1


def _trim_ordered_accumulator(
    state: _OrderedAccumulator,
    sort_spec: SortSpec,
    *,
    keep: int,
    bottom: bool,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> None:
    if len(state.items) <= keep:
        state.items.sort(
            key=cmp_to_key(
                lambda left, right: _compare_ordered_accumulator_items(
                    left,
                    right,
                    sort_spec,
                    dialect=dialect,
                )
            )
        )
        return

    state.items.sort(
        key=cmp_to_key(
            lambda left, right: _compare_ordered_accumulator_items(
                left,
                right,
                sort_spec,
                dialect=dialect,
                reverse_tie_break=bottom,
            )
        )
    )
    if bottom:
        del state.items[: len(state.items) - keep]
        state.items.sort(
            key=cmp_to_key(
                lambda left, right: _compare_ordered_accumulator_items(
                    left,
                    right,
                    sort_spec,
                    dialect=dialect,
                )
            )
        )
    else:
        del state.items[keep:]


def _prepare_accumulator_specs(
    accumulator_specs: dict[str, object] | None,
    *,
    default_sum: bool = False,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    support_checker: Callable[[str], bool] | None = None,
    unsupported_message: str = "Unsupported accumulator",
) -> PreparedAccumulatorSpecs:
    specs = {"count": {"$sum": 1}} if accumulator_specs is None and default_sum else (accumulator_specs or {})
    if support_checker is None:
        support_checker = dialect.supports_group_accumulator
    prepared: list[tuple[str, str, object]] = []
    for field, accumulator in specs.items():
        if not isinstance(accumulator, dict) or len(accumulator) != 1:
            raise OperationFailure("Accumulator must be a single-key document")
        operator, expression = next(iter(accumulator.items()))
        if not support_checker(operator):
            raise OperationFailure(f"{unsupported_message}: {operator}")
        _validate_accumulator_expression(operator, expression)
        prepared.append((field, operator, expression))
    return tuple(prepared)


def _create_accumulator_state(
    prepared_specs: PreparedAccumulatorSpecs,
    *,
    unsupported_message: str = "Unsupported accumulator",
) -> dict[str, Any]:
    initialized: dict[str, Any] = {}
    for field, operator, _expression in prepared_specs:
        if operator in {"$sum", "$count"}:
            initialized[field] = 0
        elif operator in {"$min", "$max", "$first", "$last"}:
            initialized[field] = None
        elif operator in {"$firstN", "$lastN", "$maxN", "$minN"}:
            initialized[field] = _PickNAccumulator()
        elif operator in {"$top", "$bottom", "$topN", "$bottomN"}:
            initialized[field] = _OrderedAccumulator()
        elif operator in {"$median", "$percentile"}:
            initialized[field] = _PercentileAccumulator(scalar_output=operator == "$median")
        elif operator == "$avg":
            initialized[field] = _AverageAccumulator()
        elif operator == "$stdDevPop":
            initialized[field] = _StdDevAccumulator(population=True)
        elif operator == "$stdDevSamp":
            initialized[field] = _StdDevAccumulator(population=False)
        elif operator in {"$push", "$addToSet"}:
            initialized[field] = []
        elif operator == "$mergeObjects":
            initialized[field] = {}
        else:
            raise OperationFailure(f"{unsupported_message}: {operator}")
    return initialized


def _reset_accumulator_bucket(
    bucket: _AccumulatorBucket,
    prepared_specs: PreparedAccumulatorSpecs,
    *,
    unsupported_message: str = "Unsupported accumulator",
) -> None:
    bucket.values = _create_accumulator_state(
        prepared_specs,
        unsupported_message=unsupported_message,
    )
    bucket.flags.clear()


def _initialize_accumulators(
    accumulator_specs: dict[str, object] | None,
    *,
    default_sum: bool = False,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    support_checker: Callable[[str], bool] | None = None,
    unsupported_message: str = "Unsupported accumulator",
) -> dict[str, Any]:
    prepared_specs = _prepare_accumulator_specs(
        accumulator_specs,
        default_sum=default_sum,
        dialect=dialect,
        support_checker=support_checker,
        unsupported_message=unsupported_message,
    )
    return _create_accumulator_state(
        prepared_specs,
        unsupported_message=unsupported_message,
    )


def _coerce_accumulator_specs(
    accumulator_specs: AccumulatorSpecsInput,
) -> PreparedAccumulatorSpecs:
    if accumulator_specs is None:
        return (("count", "$sum", 1),)
    if isinstance(accumulator_specs, tuple):
        return accumulator_specs
    prepared: list[tuple[str, str, object]] = []
    for field, accumulator in accumulator_specs.items():
        if not isinstance(accumulator, dict) or len(accumulator) != 1:
            raise OperationFailure("Accumulator must be a single-key document")
        operator, expression = next(iter(accumulator.items()))
        prepared.append((field, operator, expression))
    return tuple(prepared)


def _apply_accumulators(
    bucket: _AccumulatorBucket | dict[str, Any],
    accumulator_specs: AccumulatorSpecsInput,
    document: Document,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator | None = None,
    evaluate_expression_with_missing: MissingExpressionEvaluator | None = None,
    append_unique_values: Callable[[list[Any], list[Any]], None] | None = None,
    require_sort: Callable[[object], SortSpec] | None = None,
    resolve_aggregation_field_path: Callable[[Any, str], Any] | None = None,
    missing_sentinel: object | None = None,
) -> None:
    if evaluate_expression is None or evaluate_expression_with_missing is None or append_unique_values is None or require_sort is None or resolve_aggregation_field_path is None or missing_sentinel is None:
        from mongoeco.core.aggregation.planning import _require_sort as _default_require_sort
        from mongoeco.core.aggregation.runtime import (
            _MISSING as _DEFAULT_MISSING,
            _append_unique_values as _default_append_unique_values,
            _evaluate_expression_with_missing as _default_evaluate_expression_with_missing,
            _resolve_aggregation_field_path as _default_resolve_aggregation_field_path,
            evaluate_expression as _default_evaluate_expression,
        )

        if evaluate_expression is None:
            evaluate_expression = lambda current_document, current_expression, current_variables=None: _default_evaluate_expression(
                current_document,
                current_expression,
                current_variables,
                dialect=dialect,
            )
        if evaluate_expression_with_missing is None:
            evaluate_expression_with_missing = (
                lambda current_document, current_expression, current_variables=None: _default_evaluate_expression_with_missing(
                    current_document,
                    current_expression,
                    current_variables or {},
                    dialect=dialect,
                )
            )
        if append_unique_values is None:
            append_unique_values = lambda target, values: _default_append_unique_values(
                target,
                values,
                dialect=dialect,
            )
        if require_sort is None:
            require_sort = _default_require_sort
        if resolve_aggregation_field_path is None:
            resolve_aggregation_field_path = _default_resolve_aggregation_field_path
        if missing_sentinel is None:
            missing_sentinel = _DEFAULT_MISSING

    prepared_specs = _coerce_accumulator_specs(accumulator_specs)
    values = bucket.values if isinstance(bucket, _AccumulatorBucket) else bucket
    flags = bucket.flags if isinstance(bucket, _AccumulatorBucket) else _accumulator_flags(bucket)
    for field, operator, expression in prepared_specs:
        value = None if operator == "$count" or operator in _ACCUMULATORS_WITH_INTERNAL_EVALUATION else evaluate_expression(document, expression, variables)
        if operator in {"$sum", "$count"}:
            if operator == "$count":
                values[field] += 1
                continue
            if value is None:
                continue
            numeric_value = _sum_accumulator_operand(value)
            if numeric_value is None:
                continue
            values[field] = bson_add(values[field], numeric_value)
        elif operator == "$min":
            if value is None:
                continue
            if not flags.get(field, False) or dialect.policy.compare_values(value, values[field]) < 0:
                values[field] = deepcopy(value)
                flags[field] = True
        elif operator == "$max":
            if value is None:
                continue
            if not flags.get(field, False) or dialect.policy.compare_values(value, values[field]) > 0:
                values[field] = deepcopy(value)
                flags[field] = True
        elif operator == "$avg":
            if value is None or isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal)):
                if not is_bson_numeric(value):
                    continue
            values[field].total = bson_add(values[field].total, value)
            values[field].count += 1
        elif operator in {"$stdDevPop", "$stdDevSamp"}:
            operand = _stddev_accumulator_operand(value)
            if operand is None:
                continue
            if not math.isfinite(operand):
                values[field].invalid = True
                continue
            values[field].total += operand
            values[field].sum_of_squares += operand * operand
            values[field].count += 1
        elif operator == "$push":
            values[field].append(deepcopy(value))
        elif operator == "$addToSet":
            append_unique_values(values[field], [value])
        elif operator == "$mergeObjects":
            if value is None:
                continue
            if not isinstance(value, dict):
                raise OperationFailure("$mergeObjects accumulator requires document operands")
            values[field].update(deepcopy(value))
        elif operator == "$first":
            if not flags.get(field, False):
                values[field] = deepcopy(value)
                flags[field] = True
        elif operator == "$last":
            values[field] = deepcopy(value)
            flags[field] = True
        elif operator in {"$firstN", "$lastN", "$maxN", "$minN"}:
            state = values[field]
            if operator == "$firstN" and state.n is not None and len(state.items) >= state.n:
                size = _evaluate_pick_n_size(
                    operator,
                    document,
                    expression,
                    variables,
                    evaluate_expression=evaluate_expression,
                )
                if state.n != size:
                    raise OperationFailure(f"{operator} n must evaluate to a consistent positive integer within the group")
                continue
            value, size = _evaluate_pick_n_input(
                operator,
                document,
                expression,
                variables,
                dialect=dialect,
                evaluate_expression=evaluate_expression,
                evaluate_expression_with_missing=evaluate_expression_with_missing,
                missing_sentinel=missing_sentinel,
            )
            if state.n is None:
                state.n = size
            elif state.n != size:
                raise OperationFailure(f"{operator} n must evaluate to a consistent positive integer within the group")
            if operator == "$firstN":
                if len(state.items) < state.n:
                    state.items.append(deepcopy(value))
            elif operator == "$lastN":
                state.items.append(deepcopy(value))
                if len(state.items) > state.n:
                    del state.items[:-state.n]
            else:
                if value is None or isinstance(value, UndefinedType):
                    continue
                state.items.append(deepcopy(value))
                state.items.sort(
                    key=cmp_to_key(dialect.policy.compare_values),
                    reverse=operator == "$maxN",
                )
                del state.items[state.n:]
        elif operator in {"$top", "$bottom", "$topN", "$bottomN"}:
            sort_spec, sort_values, output, size = _evaluate_ordered_accumulator_input(
                operator,
                document,
                expression,
                variables,
                dialect=dialect,
                evaluate_expression=evaluate_expression,
                require_sort=require_sort,
                resolve_aggregation_field_path=resolve_aggregation_field_path,
                missing_sentinel=missing_sentinel,
            )
            state = values[field]
            if state.sort_spec is None:
                state.sort_spec = sort_spec
            keep = size if operator in {"$topN", "$bottomN"} else 1
            if operator in {"$topN", "$bottomN"}:
                if state.n is None:
                    state.n = size
                elif state.n != size:
                    raise OperationFailure(f"{operator} n must evaluate to a consistent positive integer within the group")
            state.items.append((sort_values, deepcopy(output), state.sequence))
            state.sequence += 1
            _trim_ordered_accumulator(
                state,
                sort_spec,
                keep=keep,
                bottom=operator in {"$bottom", "$bottomN"},
                dialect=dialect,
            )
        elif operator in {"$median", "$percentile"}:
            candidates, probabilities = _parse_percentile_spec(
                operator,
                document,
                expression,
                variables,
                dialect=dialect,
                evaluate_expression=evaluate_expression,
                evaluate_expression_with_missing=evaluate_expression_with_missing,
            )
            state = values[field]
            if state.probabilities is None:
                state.probabilities = probabilities
            elif state.probabilities != probabilities:
                raise OperationFailure(f"{operator} p must evaluate to a consistent array within the group")
            state.values.extend(deepcopy(candidates))


def _finalize_accumulators(bucket: _AccumulatorBucket | dict[str, Any]) -> Document:
    document: Document = {}
    include_bucket_id = isinstance(bucket, _AccumulatorBucket) and bucket.include_bucket_id
    if include_bucket_id:
        document["_id"] = deepcopy(bucket.bucket_id)
    values = bucket.values if isinstance(bucket, _AccumulatorBucket) else bucket
    for field, value in values.items():
        if not isinstance(bucket, _AccumulatorBucket) and field == _ACCUMULATOR_FLAGS_KEY:
            continue
        if isinstance(value, _AverageAccumulator):
            if value.count == 0:
                document[field] = None
            else:
                total = unwrap_bson_numeric(value.total)
                if isinstance(total, decimal.Decimal):
                    document[field] = total / decimal.Decimal(value.count)
                else:
                    document[field] = total / value.count
        elif isinstance(value, _StdDevAccumulator):
            if value.invalid or value.count == 0:
                document[field] = None
            elif value.population:
                mean = value.total / value.count
                variance = max((value.sum_of_squares / value.count) - (mean * mean), 0.0)
                document[field] = math.sqrt(variance)
            elif value.count < 2:
                document[field] = None
            else:
                variance = max(
                    (value.sum_of_squares - ((value.total * value.total) / value.count))
                    / (value.count - 1),
                    0.0,
                )
                document[field] = math.sqrt(variance)
        elif isinstance(value, _PickNAccumulator):
            document[field] = list(value.items)
        elif isinstance(value, _OrderedAccumulator):
            if not value.items:
                document[field] = [] if value.n is not None else None
            elif value.n is None:
                document[field] = value.items[0][1]
            else:
                document[field] = [item[1] for item in value.items]
        elif isinstance(value, _PercentileAccumulator):
            probabilities = value.probabilities or [0.5]
            percentiles = _compute_percentiles(value.values, probabilities)
            if value.scalar_output:
                document[field] = None if percentiles is None else percentiles[0]
            else:
                document[field] = percentiles
        else:
            document[field] = value
    return document


def _require_window_output_spec(spec: object) -> tuple[str, object, dict[str, object] | None]:
    if not isinstance(spec, dict):
        raise OperationFailure("$setWindowFields output entries must be documents")
    window = spec.get("window")
    if window is not None and not isinstance(window, dict):
        raise OperationFailure("$setWindowFields window must be a document")
    operator_items = [(key, value) for key, value in spec.items() if key != "window"]
    if len(operator_items) != 1:
        raise OperationFailure("$setWindowFields output entries require exactly one accumulator")
    operator, expression = operator_items[0]
    return operator, expression, window


def _resolve_window_index(bound: object, current_index: int, last_index: int, *, lower: bool) -> int:
    if bound == "unbounded":
        return 0 if lower else last_index
    if bound == "current":
        return current_index
    if isinstance(bound, int) and not isinstance(bound, bool):
        return current_index + bound
    raise OperationFailure("$setWindowFields documents bounds must be integers, 'current' or 'unbounded'")


def _require_range_bound(bound: object) -> int | float | str:
    if bound in {"current", "unbounded"}:
        return bound
    if isinstance(bound, (int, float)) and not isinstance(bound, bool):
        return bound
    raise OperationFailure("$setWindowFields range bounds must be numeric, 'current' or 'unbounded'")


def _resolve_range_value(current_value: object, bound: int | float | str, *, lower: bool) -> float:
    if bound == "unbounded":
        return float("-inf") if lower else float("inf")
    if bound == "current":
        return float(current_value)  # type: ignore[arg-type]
    return float(current_value) + float(bound)  # type: ignore[arg-type]


def _window_sort_key_values(document: Document, sort_spec: SortSpec) -> list[Any]:
    values: list[Any] = []
    for field, _direction in sort_spec:
        found, value = get_document_value(document, field)
        values.append(value if found else None)
    return values


def _window_sort_keys_equal(
    left: list[Any],
    right: list[Any],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> bool:
    if len(left) != len(right):
        return False
    return all(dialect.policy.compare_values(left[index], right[index]) == 0 for index in range(len(left)))
