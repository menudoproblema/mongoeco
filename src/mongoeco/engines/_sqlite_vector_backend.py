from __future__ import annotations

from dataclasses import dataclass
import time

import numpy as np
from usearch.index import Index, MetricKind

from mongoeco.core.paths import get_document_value
from mongoeco.core.search_filter_prefilter import (
    collect_filterable_values,
    evaluate_candidate_filter,
    filter_value_key,
    value_key_matches_range,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, SearchIndexDefinition

_FilterValueKey = tuple[str, object]


@dataclass(slots=True)
class SQLiteVectorBackendState:
    db_name: str
    coll_name: str
    index_name: str
    physical_name: str
    path: str
    similarity: str
    num_dimensions: int
    connectivity: int | None
    expansion_add: int | None
    expansion_search: int | None
    built_at_epoch: float
    collection_version: int
    documents_scanned: int
    valid_vectors: int
    invalid_vectors: int
    index: Index
    storage_keys_by_slot: tuple[str, ...]
    scalar_filter_index: dict[str, dict[_FilterValueKey, tuple[str, ...]]]
    exists_filter_index: dict[str, tuple[str, ...]]


def build_sqlite_vector_backend(
    *,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    physical_name: str,
    path: str,
    collection_version: int,
    documents: list[tuple[str, Document]],
) -> SQLiteVectorBackendState:
    field_spec = _require_vector_field_spec(definition, path)
    num_dimensions = int(field_spec["numDimensions"])
    similarity = str(field_spec.get("similarity", "cosine"))
    connectivity = _optional_positive_int(field_spec.get("connectivity"))
    expansion_add = _optional_positive_int(field_spec.get("expansionAdd"))
    expansion_search = _optional_positive_int(field_spec.get("expansionSearch"))
    index = Index(
        ndim=num_dimensions,
        metric=_metric_kind(similarity),
        dtype=np.float32,
        connectivity=connectivity,
        expansion_add=expansion_add,
        expansion_search=expansion_search,
    )

    keys: list[int] = []
    vectors: list[tuple[float, ...]] = []
    storage_keys: list[str] = []
    invalid_vectors = 0
    scalar_filter_index: dict[str, dict[_FilterValueKey, list[str]]] = {}
    exists_filter_index: dict[str, list[str]] = {}
    for slot, (storage_key, document) in enumerate(documents):
        vector = _extract_vector(document, path=path, num_dimensions=num_dimensions)
        if vector is None:
            invalid_vectors += 1
            continue
        keys.append(slot)
        vectors.append(vector)
        storage_keys.append(storage_key)
        _index_filterable_document(
            document,
            storage_key=storage_key,
            scalar_filter_index=scalar_filter_index,
            exists_filter_index=exists_filter_index,
        )

    if keys:
        index.add(
            np.asarray(keys, dtype=np.uint64),
            np.asarray(vectors, dtype=np.float32),
        )

    return SQLiteVectorBackendState(
        db_name=db_name,
        coll_name=coll_name,
        index_name=definition.name,
        physical_name=physical_name,
        path=path,
        similarity=similarity,
        num_dimensions=num_dimensions,
        connectivity=connectivity,
        expansion_add=expansion_add,
        expansion_search=expansion_search,
        built_at_epoch=time.time(),
        collection_version=collection_version,
        documents_scanned=len(documents),
        valid_vectors=len(storage_keys),
        invalid_vectors=invalid_vectors,
        index=index,
        storage_keys_by_slot=tuple(storage_keys),
        scalar_filter_index={
            path: {
                value_key: tuple(storage_keys_for_value)
                for value_key, storage_keys_for_value in values.items()
            }
            for path, values in scalar_filter_index.items()
        },
        exists_filter_index={
            path: tuple(storage_keys_for_path)
            for path, storage_keys_for_path in exists_filter_index.items()
        },
    )


def search_sqlite_vector_backend(
    state: SQLiteVectorBackendState,
    *,
    query_vector: tuple[float, ...],
    count: int,
    exact: bool = False,
) -> list[tuple[str, float]]:
    if len(query_vector) != state.num_dimensions:
        raise OperationFailure("vector search query dimensions do not match index dimensions")
    if state.valid_vectors == 0 or count <= 0:
        return []
    matches = state.index.search(
        np.asarray(query_vector, dtype=np.float32),
        count=min(count, state.valid_vectors),
        exact=exact,
    )
    return [
        (
            state.storage_keys_by_slot[int(key)],
            float(distance),
        )
        for key, distance in zip(matches.keys, matches.distances, strict=False)
    ]


def vector_backend_stats_document(state: SQLiteVectorBackendState) -> dict[str, object]:
    return {
        "backend": "usearch",
        "physicalName": state.physical_name,
        "path": state.path,
        "similarity": state.similarity,
        "numDimensions": state.num_dimensions,
        "connectivity": state.connectivity,
        "expansionAdd": state.expansion_add,
        "expansionSearch": state.expansion_search,
        "collectionVersion": state.collection_version,
        "builtAtEpoch": state.built_at_epoch,
        "documentsScanned": state.documents_scanned,
        "validVectors": state.valid_vectors,
        "invalidVectors": state.invalid_vectors,
        "filterablePathCount": len(state.exists_filter_index),
    }


def vector_filter_candidate_storage_keys(
    state: SQLiteVectorBackendState,
    *,
    filter_spec: dict[str, object] | None,
) -> tuple[list[str] | None, dict[str, object] | None]:
    if filter_spec is None:
        return None, None
    candidate_result = evaluate_candidate_filter(
        filter_spec,
        all_candidates=state.storage_keys_by_slot,
        ordered_candidates=state.storage_keys_by_slot,
        clause_resolver=lambda path, clause: _candidate_storage_keys_for_filter_clause(
            state,
            path=path,
            clause=clause,
            all_storage_keys=set(state.storage_keys_by_slot),
        ),
    )
    if candidate_result is None or candidate_result.matches is None:
        return None, {
            "spec": filter_spec,
            "candidateable": False,
            "exact": False,
            "backend": None,
            "supportedPaths": [] if candidate_result is None else list(candidate_result.plan.supported_paths),
            "supportedClauseCount": 0 if candidate_result is None else candidate_result.plan.supported_clause_count,
            "unsupportedClauseCount": 1 if candidate_result is None else candidate_result.plan.unsupported_clause_count,
            "supportedOperators": [] if candidate_result is None else list(candidate_result.plan.supported_operators),
            "booleanShape": None if candidate_result is None else candidate_result.plan.shape,
        }
    return list(candidate_result.matches), candidate_result.to_metadata(backend="vector-filter-index")


def _extract_vector(
    document: Document,
    *,
    path: str,
    num_dimensions: int,
) -> tuple[float, ...] | None:
    found, value = get_document_value(document, path)
    if not found or not isinstance(value, list) or len(value) != num_dimensions:
        return None
    vector: list[float] = []
    for item in value:
        if not isinstance(item, (int, float)) or isinstance(item, bool):
            return None
        vector.append(float(item))
    return tuple(vector)


def _index_filterable_document(
    document: Document,
    *,
    storage_key: str,
    scalar_filter_index: dict[str, dict[_FilterValueKey, list[str]]],
    exists_filter_index: dict[str, list[str]],
) -> None:
    for path, values in collect_filterable_values(document):
        exists_filter_index.setdefault(path, []).append(storage_key)
        if not values:
            continue
        path_index = scalar_filter_index.setdefault(path, {})
        for value in values:
            path_index.setdefault(value, []).append(storage_key)


def _candidate_storage_keys_for_filter_clause(
    state: SQLiteVectorBackendState,
    *,
    path: str,
    clause: object,
    all_storage_keys: set[str],
) -> tuple[set[str] | None, str]:
    if isinstance(clause, dict) and set(clause) == {"$exists"} and isinstance(clause["$exists"], bool):
        existing = set(state.exists_filter_index.get(path, ()))
        return (existing if clause["$exists"] else all_storage_keys - existing), "$exists"
    if isinstance(clause, dict) and set(clause) == {"$in"} and isinstance(clause["$in"], list):
        matched_keys: set[str] = set()
        for item in clause["$in"]:
            value_key = filter_value_key(item)
            if value_key is None:
                return None, "$in"
            matched_keys.update(state.scalar_filter_index.get(path, {}).get(value_key, ()))
        return matched_keys, "$in"
    if isinstance(clause, dict):
        range_storage_keys = _candidate_storage_keys_for_range_clause(
            state,
            path=path,
            clause=clause,
        )
        if range_storage_keys is not None:
            return range_storage_keys, "range"
    value_key = filter_value_key(clause)
    if value_key is None:
        return None, "eq"
    return set(state.scalar_filter_index.get(path, {}).get(value_key, ())), "eq"


def _candidate_storage_keys_for_filter_node(
    state: SQLiteVectorBackendState,
    filter_spec: dict[str, object],
    *,
    all_storage_keys: set[str],
) -> dict[str, object] | None:
    result = evaluate_candidate_filter(
        filter_spec,
        all_candidates=tuple(all_storage_keys),
        clause_resolver=lambda path, clause: _candidate_storage_keys_for_filter_clause(
            state,
            path=path,
            clause=clause,
            all_storage_keys=all_storage_keys,
        ),
    )
    if result is None:
        return None
    return {
        "keys": set(result.matches) if result.matches is not None else None,
        "supported_paths": list(result.plan.supported_paths),
        "supported_operators": list(result.plan.supported_operators),
        "supported_clause_count": result.plan.supported_clause_count,
        "unsupported_clause_count": result.plan.unsupported_clause_count,
        "exact": result.plan.exact,
        "shape": result.plan.shape,
    }


def _candidate_storage_keys_for_range_clause(
    state: SQLiteVectorBackendState,
    *,
    path: str,
    clause: dict[str, object],
) -> set[str] | None:
    supported_operators = {"$gt", "$gte", "$lt", "$lte"}
    if not clause or any(operator not in supported_operators for operator in clause):
        return None

    bound_kind: str | None = None
    for operator, raw_value in clause.items():
        value_key = filter_value_key(raw_value)
        if value_key is None or value_key[0] not in {"number", "date", "datetime"}:
            return None
        if bound_kind is None:
            bound_kind = value_key[0]
        elif bound_kind != value_key[0]:
            return None

    path_index = state.scalar_filter_index.get(path)
    if not path_index:
        return set()

    matched: set[str] = set()
    for value_key, storage_keys in path_index.items():
        if value_key[0] != bound_kind:
            continue
        if value_key_matches_range(value_key, clause):
            matched.update(storage_keys)
    return matched


def _collect_filterable_values(
    value: object,
    *,
    prefix: str = "",
) -> list[tuple[str, set[_FilterValueKey]]]:
    return collect_filterable_values(value, prefix=prefix)


def _filter_value_key(value: object) -> _FilterValueKey | None:
    return filter_value_key(value)


def _value_key_matches_range(
    value_key: _FilterValueKey,
    bounds: dict[str, object],
) -> bool:
    if bounds and all(isinstance(value, tuple) and len(value) == 2 for value in bounds.values()):
        raw_bounds = {operator: value[1] for operator, value in bounds.items()}
        return value_key_matches_range(value_key, raw_bounds)
    return value_key_matches_range(value_key, bounds)


def _metric_kind(similarity: str) -> MetricKind:
    if similarity == "dotProduct":
        return MetricKind.IP
    if similarity == "euclidean":
        return MetricKind.L2sq
    return MetricKind.Cos


def _optional_positive_int(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise OperationFailure("local vectorSearch ANN settings must be positive integers")
    return value


def _require_vector_field_spec(definition: SearchIndexDefinition, path: str) -> dict[str, object]:
    raw_fields = definition.definition.get("fields")
    if not isinstance(raw_fields, list):
        raise OperationFailure("local vectorSearch definitions require a top-level fields array")
    for field in raw_fields:
        if (
            isinstance(field, dict)
            and field.get("type") == "vector"
            and field.get("path") == path
        ):
            return field
    raise OperationFailure(f"vectorSearch path [{path}] is not defined on index [{definition.name}]")
