from __future__ import annotations

import time

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from usearch.index import Index, MetricKind

from mongoeco.core.paths import get_document_value
from mongoeco.core.search import coerce_search_vector
from mongoeco.core.search_filter_prefilter import (
    collect_filterable_values,
    evaluate_candidate_filter,
    scalar_filter_candidates,
    unrepresented_filter_paths,
)
from mongoeco.errors import OperationFailure


if TYPE_CHECKING:
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
    unrepresented_filter_paths: frozenset[str]


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

    vectors: list[tuple[float, ...]] = []
    storage_keys: list[str] = []
    invalid_vectors = 0
    scalar_filter_index: dict[str, dict[_FilterValueKey, list[str]]] = {}
    exists_filter_index: dict[str, list[str]] = {}
    unrepresented_paths: set[str] = set()
    for storage_key, document in documents:
        vector = _extract_vector(document, path=path, num_dimensions=num_dimensions)
        if vector is None:
            invalid_vectors += 1
            continue
        unrepresented_paths.update(unrepresented_filter_paths(document))
        vectors.append(vector)
        storage_keys.append(storage_key)
        _index_filterable_document(
            document,
            storage_key=storage_key,
            scalar_filter_index=scalar_filter_index,
            exists_filter_index=exists_filter_index,
        )

    if storage_keys:
        index.add(
            np.arange(len(storage_keys), dtype=np.uint64),
            np.asarray(vectors, dtype=np.float32),
        )

    return SQLiteVectorBackendState(
        unrepresented_filter_paths=frozenset(unrepresented_paths),
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
    vector = coerce_search_vector(value) if found else None
    return vector if vector is not None and len(vector) == num_dimensions else None


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
) -> tuple[set[str] | None, str, bool]:
    return scalar_filter_candidates(
        path,
        clause,
        all_candidates=all_storage_keys,
        exists_index=state.exists_filter_index,
        value_index=state.scalar_filter_index,
        unrepresented_paths=state.unrepresented_filter_paths,
    )


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
