from __future__ import annotations

from dataclasses import dataclass
import datetime
import time
from typing import TypeAlias

import numpy as np
from usearch.index import Index, MetricKind

from mongoeco.core.paths import get_document_value
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, SearchIndexDefinition


_FilterValueKey: TypeAlias = tuple[str, object]


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
    clauses = _flatten_simple_filter_clauses(filter_spec)
    if clauses is None:
        return None, {
            "spec": filter_spec,
            "candidateable": False,
            "exact": False,
            "backend": None,
            "supportedPaths": [],
            "supportedClauseCount": 0,
            "unsupportedClauseCount": 1,
            "supportedOperators": [],
        }

    all_storage_keys = set(state.storage_keys_by_slot)
    supported_paths: list[str] = []
    supported_operators: list[str] = []
    storage_key_sets: list[set[str]] = []
    unsupported_clause_count = 0

    for path, clause in clauses:
        matched_keys: set[str] | None = None
        operator_name = "eq"
        if isinstance(clause, dict) and set(clause) == {"$exists"} and isinstance(clause["$exists"], bool):
            operator_name = "$exists"
            existing = set(state.exists_filter_index.get(path, ()))
            matched_keys = existing if clause["$exists"] else all_storage_keys - existing
        elif isinstance(clause, dict) and set(clause) == {"$in"} and isinstance(clause["$in"], list):
            operator_name = "$in"
            matched_keys = set()
            for item in clause["$in"]:
                value_key = _filter_value_key(item)
                if value_key is None:
                    matched_keys = None
                    break
                matched_keys.update(state.scalar_filter_index.get(path, {}).get(value_key, ()))
        else:
            value_key = _filter_value_key(clause)
            if value_key is not None:
                matched_keys = set(state.scalar_filter_index.get(path, {}).get(value_key, ()))
            else:
                matched_keys = None

        if matched_keys is None:
            unsupported_clause_count += 1
            continue
        supported_paths.append(path)
        supported_operators.append(operator_name)
        storage_key_sets.append(matched_keys)

    if not storage_key_sets:
        return None, {
            "spec": filter_spec,
            "candidateable": False,
            "exact": False,
            "backend": None,
            "supportedPaths": [],
            "supportedClauseCount": 0,
            "unsupportedClauseCount": unsupported_clause_count,
            "supportedOperators": [],
        }

    matched = set.intersection(*storage_key_sets)
    ordered = [storage_key for storage_key in state.storage_keys_by_slot if storage_key in matched]
    return ordered, {
        "spec": filter_spec,
        "candidateable": True,
        "exact": unsupported_clause_count == 0,
        "backend": "vector-filter-index",
        "supportedPaths": supported_paths,
        "supportedClauseCount": len(storage_key_sets),
        "unsupportedClauseCount": unsupported_clause_count,
        "supportedOperators": supported_operators,
    }


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
    for path, values in _collect_filterable_values(document):
        exists_filter_index.setdefault(path, []).append(storage_key)
        if not values:
            continue
        path_index = scalar_filter_index.setdefault(path, {})
        for value in values:
            path_index.setdefault(value, []).append(storage_key)


def _collect_filterable_values(
    value: object,
    *,
    prefix: str = "",
) -> list[tuple[str, set[_FilterValueKey]]]:
    entries: list[tuple[str, set[_FilterValueKey]]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                continue
            path = f"{prefix}.{key}" if prefix else key
            entries.extend(_collect_filterable_values(item, prefix=path))
        return entries
    if not prefix:
        return entries
    if isinstance(value, list):
        scalar_values = {
            normalized
            for item in value
            if (normalized := _filter_value_key(item)) is not None
        }
        entries.append((prefix, scalar_values))
        return entries
    entries.append((prefix, {_filter_value_key(value)} if _filter_value_key(value) is not None else set()))
    return entries


def _filter_value_key(value: object) -> _FilterValueKey | None:
    if value is None:
        return ("null", None)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        number = float(value)
        if not np.isfinite(number):
            return None
        return ("number", number)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, datetime.datetime):
        return ("datetime", value)
    if isinstance(value, datetime.date):
        return ("date", value)
    return None


def _flatten_simple_filter_clauses(filter_spec: dict[str, object]) -> list[tuple[str, object]] | None:
    clauses: list[tuple[str, object]] = []
    for key, value in filter_spec.items():
        if key == "$and":
            if not isinstance(value, list):
                return None
            for item in value:
                if not isinstance(item, dict):
                    return None
                nested = _flatten_simple_filter_clauses(item)
                if nested is None:
                    return None
                clauses.extend(nested)
            continue
        if key.startswith("$"):
            return None
        clauses.append((key, value))
    return clauses


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
