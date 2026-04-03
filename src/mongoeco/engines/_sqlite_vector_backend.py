from __future__ import annotations

from dataclasses import dataclass
import time

import numpy as np
from usearch.index import Index, MetricKind

from mongoeco.core.paths import get_document_value
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, SearchIndexDefinition


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
    for slot, (storage_key, document) in enumerate(documents):
        vector = _extract_vector(document, path=path, num_dimensions=num_dimensions)
        if vector is None:
            invalid_vectors += 1
            continue
        keys.append(slot)
        vectors.append(vector)
        storage_keys.append(storage_key)

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
