from functools import cmp_to_key
import math
from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.paths import get_document_value
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, UndefinedType

from mongoeco.core.aggregation.runtime import (
    _AccumulatorBucket,
    _AverageAccumulator,
    _OrderedAccumulator,
    _PercentileAccumulator,
    _PickNAccumulator,
    _StdDevAccumulator,
    _accumulator_flags,
    _aggregation_key,
    _append_unique_values,
    _compute_percentiles,
    _evaluate_ordered_accumulator_input,
    _evaluate_pick_n_input,
    _finalize_accumulators,
    _initialize_accumulators,
    _is_numeric,
    _parse_percentile_spec,
    _resolve_range_value,
    _resolve_window_index,
    _require_range_bound,
    _require_window_output_spec,
    _stddev_accumulator_operand,
    _sum_accumulator_operand,
    _trim_ordered_accumulator,
    _window_sort_key_values,
    _window_sort_keys_equal,
    evaluate_expression,
)
from mongoeco.core.aggregation.planning import _require_sort


def _apply_group(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "_id" not in spec:
        raise OperationFailure("$group requires a document specification with _id")

    accumulator_specs = {key: value for key, value in spec.items() if key != "_id"}
    _initialize_accumulators(
        accumulator_specs,
        dialect=dialect,
        support_checker=dialect.supports_group_accumulator,
        unsupported_message="Unsupported $group accumulator",
    )
    groups: dict[Any, _AccumulatorBucket] = {}

    for document in documents:
        group_id = evaluate_expression(document, spec["_id"], variables, dialect=dialect)
        group_key = _aggregation_key(group_id)
        if group_key not in groups:
            groups[group_key] = _AccumulatorBucket(
                bucket_id=deepcopy(group_id),
                values=_initialize_accumulators(
                    accumulator_specs,
                    dialect=dialect,
                    support_checker=dialect.supports_group_accumulator,
                    unsupported_message="Unsupported $group accumulator",
                ),
            )

        bucket = groups[group_key]
        _apply_accumulators(
            bucket,
            accumulator_specs,
            document,
            variables,
            dialect=dialect,
        )

    return [_finalize_accumulators(bucket) for bucket in groups.values()]


def _apply_accumulators(
    bucket: _AccumulatorBucket | dict[str, Any],
    accumulator_specs: dict[str, object] | None,
    document: Document,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> None:
    specs = {"count": {"$sum": 1}} if accumulator_specs is None else accumulator_specs
    values = bucket.values if isinstance(bucket, _AccumulatorBucket) else bucket
    flags = bucket.flags if isinstance(bucket, _AccumulatorBucket) else _accumulator_flags(bucket)
    for field, accumulator in specs.items():
        operator, expression = next(iter(accumulator.items()))
        value = None if operator == "$count" else evaluate_expression(document, expression, variables, dialect=dialect)
        if operator in {"$sum", "$count"}:
            if operator == "$count":
                values[field] += 1
                continue
            if value is None:
                continue
            numeric_value = _sum_accumulator_operand(value)
            if numeric_value is None:
                continue
            values[field] += numeric_value
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
            if value is None or not _is_numeric(value):
                continue
            values[field].total += value
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
            _append_unique_values(values[field], [value], dialect=dialect)
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
            value, size = _evaluate_pick_n_input(operator, document, expression, variables, dialect=dialect)
            state = values[field]
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
            )
            state = values[field]
            if state.probabilities is None:
                state.probabilities = probabilities
            elif state.probabilities != probabilities:
                raise OperationFailure(f"{operator} p must evaluate to a consistent array within the group")
            state.values.extend(deepcopy(candidates))


def _apply_bucket(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "groupBy" not in spec or "boundaries" not in spec:
        raise OperationFailure("$bucket requires groupBy and boundaries")
    boundaries = spec["boundaries"]
    if not isinstance(boundaries, list) or len(boundaries) < 2:
        raise OperationFailure("$bucket boundaries must be a list with at least two values")
    for index in range(len(boundaries) - 1):
        if dialect.policy.compare_values(boundaries[index], boundaries[index + 1]) >= 0:
            raise OperationFailure("$bucket boundaries must be strictly increasing")

    output = spec.get("output")
    if output is not None and not isinstance(output, dict):
        raise OperationFailure("$bucket output must be a document")

    default_bucket = spec.get("default")
    buckets: list[_AccumulatorBucket] = []
    for lower in boundaries[:-1]:
        buckets.append(
            _AccumulatorBucket(
                bucket_id=deepcopy(lower),
                values=_initialize_accumulators(output, default_sum=output is None, dialect=dialect),
            )
        )

    default_state: _AccumulatorBucket | None = None
    if "default" in spec:
        default_state = _AccumulatorBucket(
            bucket_id=deepcopy(default_bucket),
            values=_initialize_accumulators(output, default_sum=output is None, dialect=dialect),
        )

    for document in documents:
        value = evaluate_expression(document, spec["groupBy"], variables, dialect=dialect)
        matched = False
        for index, lower in enumerate(boundaries[:-1]):
            upper = boundaries[index + 1]
            if dialect.policy.compare_values(value, lower) >= 0 and dialect.policy.compare_values(value, upper) < 0:
                _apply_accumulators(buckets[index], output, document, variables, dialect=dialect)
                matched = True
                break
        if matched:
            continue
        if default_state is not None:
            _apply_accumulators(default_state, output, document, variables, dialect=dialect)
            continue
        raise OperationFailure("$bucket found a document outside of the specified boundaries")

    result = [_finalize_accumulators(bucket) for bucket in buckets]
    if default_state is not None:
        result.append(_finalize_accumulators(default_state))
    return result


def _apply_bucket_auto(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "groupBy" not in spec or "buckets" not in spec:
        raise OperationFailure("$bucketAuto requires groupBy and buckets")
    buckets = spec["buckets"]
    if not isinstance(buckets, int) or isinstance(buckets, bool) or buckets <= 0:
        raise OperationFailure("$bucketAuto buckets must be a positive integer")
    if "granularity" in spec:
        raise OperationFailure("$bucketAuto granularity is not supported")

    output = spec.get("output")
    if output is not None and not isinstance(output, dict):
        raise OperationFailure("$bucketAuto output must be a document")

    evaluated = [
        (evaluate_expression(document, spec["groupBy"], variables, dialect=dialect), document)
        for document in documents
    ]
    if not evaluated:
        return []

    evaluated.sort(key=lambda item: cmp_to_key(dialect.policy.compare_values)(item[0]))
    bucket_count = min(buckets, len(evaluated))
    base = len(evaluated) // bucket_count
    remainder = len(evaluated) % bucket_count
    sizes = [base + (1 if index < remainder else 0) for index in range(bucket_count)]

    result: list[Document] = []
    start = 0
    for size_index, size in enumerate(sizes):
        chunk = evaluated[start:start + size]
        start += size
        lower = deepcopy(chunk[0][0])
        upper = deepcopy(evaluated[start][0]) if size_index + 1 < len(sizes) else deepcopy(chunk[-1][0])
        bucket = _AccumulatorBucket(
            bucket_id={"min": lower, "max": upper},
            values=_initialize_accumulators(output, default_sum=output is None, dialect=dialect),
        )
        for _, document in chunk:
            _apply_accumulators(bucket, output, document, variables, dialect=dialect)
        result.append(_finalize_accumulators(bucket))
    return result


def _apply_set_window_fields(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "output" not in spec:
        raise OperationFailure("$setWindowFields requires output")
    output = spec["output"]
    if not isinstance(output, dict):
        raise OperationFailure("$setWindowFields output must be a document")

    sort_spec = _require_sort(spec["sortBy"]) if "sortBy" in spec else None
    partitions: dict[Any, list[Document]] = {}
    for document in documents:
        partition_key = (
            evaluate_expression(document, spec["partitionBy"], variables, dialect=dialect)
            if "partitionBy" in spec
            else None
        )
        partitions.setdefault(_aggregation_key(partition_key), []).append(deepcopy(document))

    result: list[Document] = []
    for partition_documents in partitions.values():
        ordered = sort_documents(partition_documents, sort_spec, dialect=dialect) if sort_spec is not None else partition_documents
        window_sort_keys = [_window_sort_key_values(document, sort_spec) for document in ordered] if sort_spec is not None else []
        last_index = len(ordered) - 1
        for current_index, document in enumerate(ordered):
            enriched = deepcopy(document)
            for field, field_spec in output.items():
                if not isinstance(field, str):
                    raise OperationFailure("$setWindowFields output field names must be strings")
                operator, expression, window = _require_window_output_spec(field_spec)
                if not dialect.supports_window_accumulator(operator):
                    raise OperationFailure(f"Unsupported $setWindowFields accumulator: {operator}")
                if operator in {"$rank", "$denseRank", "$documentNumber"}:
                    if sort_spec is None:
                        raise OperationFailure(f"{operator} requires sortBy")
                    if expression != {}:
                        raise OperationFailure(f"{operator} requires an empty document")
                    if window is not None:
                        raise OperationFailure(f"{operator} does not support an explicit window")
                    if operator == "$documentNumber":
                        set_document_value(enriched, field, current_index + 1)
                        continue
                    rank = 1
                    dense_rank = 1
                    previous_key = window_sort_keys[0]
                    for index in range(1, current_index + 1):
                        candidate_key = window_sort_keys[index]
                        if _window_sort_keys_equal(candidate_key, previous_key, dialect=dialect):
                            continue
                        dense_rank += 1
                        rank = index + 1
                        previous_key = candidate_key
                    set_document_value(enriched, field, rank if operator == "$rank" else dense_rank)
                    continue
                if window is None:
                    window_documents = ordered
                else:
                    has_documents = "documents" in window
                    has_range = "range" in window
                    if has_documents == has_range:
                        raise OperationFailure("$setWindowFields window must contain exactly one of documents or range")
                    if has_documents:
                        documents_window = window.get("documents")
                        if not isinstance(documents_window, list) or len(documents_window) != 2:
                            raise OperationFailure("$setWindowFields requires a two-item documents window")
                        start = max(0, _resolve_window_index(documents_window[0], current_index, last_index, lower=True))
                        end = min(last_index, _resolve_window_index(documents_window[1], current_index, last_index, lower=False))
                        window_documents = ordered[start:end + 1] if start <= end else []
                    else:
                        if sort_spec is None or len(sort_spec) != 1:
                            raise OperationFailure("$setWindowFields range windows require exactly one sort field")
                        range_window = window.get("range")
                        if not isinstance(range_window, list) or len(range_window) != 2:
                            raise OperationFailure("$setWindowFields requires a two-item range window")
                        lower_bound = _require_range_bound(range_window[0])
                        upper_bound = _require_range_bound(range_window[1])
                        sort_field = sort_spec[0][0]
                        found_current, current_value = get_document_value(document, sort_field)
                        if not found_current or not isinstance(current_value, (int, float)) or isinstance(current_value, bool):
                            raise OperationFailure("$setWindowFields numeric range windows require numeric sort values")
                        lower_value = _resolve_range_value(current_value, lower_bound, lower=True)
                        upper_value = _resolve_range_value(current_value, upper_bound, lower=False)
                        window_documents = []
                        for candidate in ordered:
                            found_candidate, candidate_value = get_document_value(candidate, sort_field)
                            if not found_candidate or not isinstance(candidate_value, (int, float)) or isinstance(candidate_value, bool):
                                raise OperationFailure("$setWindowFields numeric range windows require numeric sort values")
                            numeric_candidate = float(candidate_value)
                            if lower_value <= numeric_candidate <= upper_value:
                                window_documents.append(candidate)
                state = _AccumulatorBucket(
                    bucket_id=None,
                    values=_initialize_accumulators(
                        {field: {operator: expression}},
                        dialect=dialect,
                        support_checker=dialect.supports_window_accumulator,
                        unsupported_message="Unsupported $setWindowFields accumulator",
                    ),
                    include_bucket_id=False,
                )
                for window_document in window_documents:
                    _apply_accumulators(state, {field: {operator: expression}}, window_document, variables, dialect=dialect)
                set_document_value(enriched, field, _finalize_accumulators(state)[field])
            result.append(enriched)
    return result


def _apply_count(documents: list[Document], spec: object) -> list[Document]:
    if not isinstance(spec, str) or not spec or spec.startswith("$") or "." in spec:
        raise OperationFailure("$count requires a non-empty field name")
    return [{spec: len(documents)}]


def _apply_sort_by_count(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    grouped = _apply_group(
        documents,
        {"_id": spec, "count": {"$sum": 1}},
        variables,
        dialect=dialect,
    )
    return sort_documents(grouped, [("count", -1), ("_id", 1)], dialect=dialect)


from mongoeco.core.paths import set_document_value  # noqa: E402
