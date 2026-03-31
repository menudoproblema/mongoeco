from functools import cmp_to_key
from copy import deepcopy
import math
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
from mongoeco.core.paths import get_document_value, set_document_value
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import OperationFailure
from mongoeco.types import Document

from mongoeco.core.aggregation.accumulators import (
    _AccumulatorBucket,
    _apply_accumulators,
    _create_accumulator_state,
    _finalize_accumulators,
    _prepare_accumulator_specs,
    _reset_accumulator_bucket,
    _resolve_range_value,
    _resolve_window_index,
    _require_range_bound,
    _require_window_output_spec,
    _window_sort_key_values,
    _window_sort_keys_equal,
)
from mongoeco.core.aggregation.runtime import (
    _aggregation_key,
    _append_unique_values,
    _resolve_aggregation_field_path,
    _MISSING,
    evaluate_expression,
    _evaluate_expression_with_missing,
)
from mongoeco.core.aggregation.planning import _require_sort
from mongoeco.core.aggregation.compiled_aggregation import CompiledGroup


def _copy_if_mutable(value: Any) -> Any:
    if isinstance(value, (dict, list, set)):
        return deepcopy(value)
    return value


def _build_accumulator_runtime(
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> dict[str, Any]:
    def _evaluate(
        current_document: Document,
        current_expression: object,
        current_variables: dict[str, Any] | None = None,
    ) -> Any:
        return evaluate_expression(
            current_document,
            current_expression,
            current_variables,
            dialect=dialect,
        )

    def _evaluate_with_missing(
        current_document: Document,
        current_expression: object,
        current_variables: dict[str, Any] | None = None,
    ) -> Any:
        return _evaluate_expression_with_missing(
            current_document,
            current_expression,
            current_variables or {},
            dialect=dialect,
        )

    def _append_unique(target: list[Any], values: list[Any]) -> None:
        _append_unique_values(target, values, dialect=dialect, collation=collation)

    return {
        "evaluate_expression": _evaluate,
        "evaluate_expression_with_missing": _evaluate_with_missing,
        "append_unique_values": _append_unique,
        "require_sort": _require_sort,
        "resolve_aggregation_field_path": _resolve_aggregation_field_path,
        "missing_sentinel": _MISSING,
    }


def _find_bucket_index(
    value: Any,
    boundaries: list[Any],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> int | None:
    if dialect.policy.compare_values(value, boundaries[0]) < 0:
        return None
    if dialect.policy.compare_values(value, boundaries[-1]) >= 0:
        return None
    low = 0
    high = len(boundaries) - 1
    while low < high - 1:
        mid = (low + high) // 2
        if dialect.policy.compare_values(value, boundaries[mid]) < 0:
            high = mid
        else:
            low = mid
    return low


def _precompute_window_ranks(
    window_sort_keys: list[list[Any]],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> tuple[list[int], list[int]]:
    if not window_sort_keys:
        return [], []
    ranks = [1]
    dense_ranks = [1]
    previous_key = window_sort_keys[0]
    rank = 1
    dense_rank = 1
    for index, candidate_key in enumerate(window_sort_keys[1:], start=1):
        if not _window_sort_keys_equal(candidate_key, previous_key, dialect=dialect):
            dense_rank += 1
            rank = index + 1
            previous_key = candidate_key
        ranks.append(rank)
        dense_ranks.append(dense_rank)
    return ranks, dense_ranks


def _apply_group(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Document]:
    if not isinstance(spec, dict) or "_id" not in spec:
        raise OperationFailure("$group requires a document specification with _id")

    if CompiledGroup.supports(spec):
        try:
            compiled = CompiledGroup(spec, dialect=dialect)
            return compiled.apply(documents, variables, collation=collation)
        except (OperationFailure, SyntaxError, TypeError, ValueError):
            pass

    accumulator_specs = {key: value for key, value in spec.items() if key != "_id"}
    accumulator_runtime = _build_accumulator_runtime(dialect=dialect, collation=collation)
    prepared_accumulators = _prepare_accumulator_specs(
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
                bucket_id=_copy_if_mutable(group_id),
                values=_create_accumulator_state(
                    prepared_accumulators,
                    unsupported_message="Unsupported $group accumulator",
                ),
            )

        bucket = groups[group_key]
        _apply_accumulators(
            bucket,
            prepared_accumulators,
            document,
            variables,
            dialect=dialect,
            collation=collation,
            **accumulator_runtime,
        )

    return [_finalize_accumulators(bucket) for bucket in groups.values()]


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
    prepared_output = _prepare_accumulator_specs(
        output,
        default_sum=output is None,
        dialect=dialect,
        unsupported_message="Unsupported accumulator",
    )

    default_bucket = spec.get("default")
    accumulator_runtime = _build_accumulator_runtime(dialect=dialect, collation=None)
    buckets: list[_AccumulatorBucket] = []
    for lower in boundaries[:-1]:
        buckets.append(
            _AccumulatorBucket(
                bucket_id=_copy_if_mutable(lower),
                values=_create_accumulator_state(prepared_output),
            )
        )

    default_state: _AccumulatorBucket | None = None
    if "default" in spec:
        default_state = _AccumulatorBucket(
            bucket_id=_copy_if_mutable(default_bucket),
            values=_create_accumulator_state(prepared_output),
        )

    for document in documents:
        value = evaluate_expression(document, spec["groupBy"], variables, dialect=dialect)
        bucket_index = _find_bucket_index(value, boundaries, dialect=dialect)
        if bucket_index is not None:
            _apply_accumulators(
                buckets[bucket_index],
                prepared_output,
                document,
                variables,
                dialect=dialect,
                collation=None,
                **accumulator_runtime,
            )
            continue
        if default_state is not None:
            _apply_accumulators(
                default_state,
                prepared_output,
                document,
                variables,
                dialect=dialect,
                collation=None,
                **accumulator_runtime,
            )
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
    accumulator_runtime = _build_accumulator_runtime(dialect=dialect, collation=None)
    if output is not None and not isinstance(output, dict):
        raise OperationFailure("$bucketAuto output must be a document")
    prepared_output = _prepare_accumulator_specs(
        output,
        default_sum=output is None,
        dialect=dialect,
        unsupported_message="Unsupported accumulator",
    )

    evaluated = [
        (evaluate_expression(document, spec["groupBy"], variables, dialect=dialect), document)
        for document in documents
    ]
    if not evaluated:
        return []

    compare_key = cmp_to_key(dialect.policy.compare_values)
    evaluated.sort(key=lambda item: compare_key(item[0]))
    bucket_count = min(buckets, len(evaluated))
    base = len(evaluated) // bucket_count
    remainder = len(evaluated) % bucket_count
    sizes = [base + (1 if index < remainder else 0) for index in range(bucket_count)]

    result: list[Document] = []
    start = 0
    for size_index, size in enumerate(sizes):
        chunk = evaluated[start:start + size]
        start += size
        lower = _copy_if_mutable(chunk[0][0])
        upper = (
            _copy_if_mutable(evaluated[start][0])
            if size_index + 1 < len(sizes) and start < len(evaluated)
            else _copy_if_mutable(chunk[-1][0])
        )
        bucket = _AccumulatorBucket(
            bucket_id={"min": lower, "max": upper},
            values=_create_accumulator_state(prepared_output),
        )
        for _, document in chunk:
            _apply_accumulators(
                bucket,
                prepared_output,
                document,
                variables,
                dialect=dialect,
                collation=None,
                **accumulator_runtime,
            )
        result.append(_finalize_accumulators(bucket))
    return result


def _apply_set_window_fields(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Document]:
    if not isinstance(spec, dict) or "output" not in spec:
        raise OperationFailure("$setWindowFields requires output")
    output = spec["output"]
    if not isinstance(output, dict):
        raise OperationFailure("$setWindowFields output must be a document")

    sort_spec = _require_sort(spec["sortBy"]) if "sortBy" in spec else None
    accumulator_runtime = _build_accumulator_runtime(dialect=dialect, collation=collation)
    prepared_window_outputs: dict[str, tuple[str, object, dict[str, object] | None, tuple[tuple[str, str, object], ...]]] = {}
    reusable_window_states: dict[str, _AccumulatorBucket] = {}
    for field, field_spec in output.items():
        if not isinstance(field, str):
            raise OperationFailure("$setWindowFields output field names must be strings")
        operator, expression, window = _require_window_output_spec(field_spec)
        if not dialect.supports_window_accumulator(operator):
            raise OperationFailure(f"Unsupported $setWindowFields accumulator: {operator}")
        if operator in {"$rank", "$denseRank", "$documentNumber"}:
            prepared_window_outputs[field] = (operator, expression, window, ())
            continue
        prepared_specs = _prepare_accumulator_specs(
            {field: {operator: expression}},
            dialect=dialect,
            support_checker=dialect.supports_window_accumulator,
            unsupported_message="Unsupported $setWindowFields accumulator",
        )
        prepared_window_outputs[field] = (operator, expression, window, prepared_specs)
        reusable_window_states[field] = _AccumulatorBucket(
            bucket_id=None,
            values=_create_accumulator_state(
                prepared_specs,
                unsupported_message="Unsupported $setWindowFields accumulator",
            ),
            include_bucket_id=False,
        )
    partitions: dict[Any, list[Document]] = {}
    for document in documents:
        partition_key = (
            evaluate_expression(document, spec["partitionBy"], variables, dialect=dialect)
            if "partitionBy" in spec
            else None
        )
        partitions.setdefault(_aggregation_key(partition_key), []).append(document)

    result: list[Document] = []
    for partition_documents in partitions.values():
        ordered = sort_documents(partition_documents, sort_spec, dialect=dialect) if sort_spec is not None else partition_documents
        window_sort_keys = [_window_sort_key_values(document, sort_spec) for document in ordered] if sort_spec is not None else []
        ranks, dense_ranks = _precompute_window_ranks(window_sort_keys, dialect=dialect)
        last_index = len(ordered) - 1
        for current_index, document in enumerate(ordered):
            enriched = deepcopy(document)
            for field, (operator, expression, window, prepared_specs) in prepared_window_outputs.items():
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
                    set_document_value(enriched, field, ranks[current_index] if operator == "$rank" else dense_ranks[current_index])
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
                        if (
                            not found_current
                            or not isinstance(current_value, (int, float))
                            or isinstance(current_value, bool)
                            or not math.isfinite(float(current_value))
                        ):
                            raise OperationFailure("$setWindowFields numeric range windows require numeric sort values")
                        lower_value = _resolve_range_value(current_value, lower_bound, lower=True)
                        upper_value = _resolve_range_value(current_value, upper_bound, lower=False)
                        window_documents = []
                        for candidate in ordered:
                            found_candidate, candidate_value = get_document_value(candidate, sort_field)
                            if (
                                not found_candidate
                                or not isinstance(candidate_value, (int, float))
                                or isinstance(candidate_value, bool)
                                or not math.isfinite(float(candidate_value))
                            ):
                                raise OperationFailure("$setWindowFields numeric range windows require numeric sort values")
                            numeric_candidate = float(candidate_value)
                            if lower_value <= numeric_candidate <= upper_value:
                                window_documents.append(candidate)
                state = reusable_window_states[field]
                _reset_accumulator_bucket(
                    state,
                    prepared_specs,
                    unsupported_message="Unsupported $setWindowFields accumulator",
                )
                for window_document in window_documents:
                    _apply_accumulators(
                        state,
                        prepared_specs,
                        window_document,
                        variables,
                        dialect=dialect,
                        collation=collation,
                        **accumulator_runtime,
                    )
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
        collation=None,
    )
    return sort_documents(grouped, [("count", -1), ("_id", 1)], dialect=dialect)
