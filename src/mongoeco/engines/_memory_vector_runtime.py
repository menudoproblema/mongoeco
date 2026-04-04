from __future__ import annotations

from dataclasses import dataclass
import numpy as np

from mongoeco.core.paths import get_document_value
from mongoeco.core.search import SearchVectorQuery, vector_field_specs
from mongoeco.core.search_filter_prefilter import (
    collect_filterable_values,
    evaluate_candidate_filter,
    filter_value_key,
    value_key_matches_range,
)
from mongoeco.types import Document, SearchIndexDefinition


_FilterValueKey = tuple[str, object]


@dataclass(frozen=True, slots=True)
class MaterializedVectorDocument:
    document: Document
    vectors_by_path: dict[str, tuple[float, ...]]
    exists_paths: frozenset[str]
    scalar_values: dict[str, tuple[_FilterValueKey, ...]]


@dataclass(frozen=True, slots=True)
class MaterializedVectorIndex:
    documents: tuple[MaterializedVectorDocument, ...]
    vector_paths: tuple[str, ...]
    vector_specs: dict[str, dict[str, object]]
    valid_vector_counts: dict[str, int]
    vector_matrices: dict[str, np.ndarray]
    vector_row_positions: dict[str, tuple[int, ...]]
    vector_row_index_by_position: dict[str, dict[int, int]]
    exists_filter_index: dict[str, frozenset[int]]
    scalar_filter_index: dict[str, dict[_FilterValueKey, frozenset[int]]]
    scalar_values_by_path: dict[str, tuple[tuple[int, _FilterValueKey], ...]]
    vector_row_exists_filter_index: dict[str, dict[str, frozenset[int]]]
    vector_row_scalar_filter_index: dict[str, dict[str, dict[_FilterValueKey, frozenset[int]]]]
    vector_row_scalar_values_by_path: dict[str, dict[str, tuple[tuple[int, _FilterValueKey], ...]]]


def build_materialized_vector_index(
    documents: list[Document],
    definition: SearchIndexDefinition,
) -> MaterializedVectorIndex:
    specs = vector_field_specs(definition)
    exists_index: dict[str, set[int]] = {}
    scalar_index: dict[str, dict[_FilterValueKey, set[int]]] = {}
    scalar_values_by_path: dict[str, list[tuple[int, _FilterValueKey]]] = {}
    valid_vector_counts = {path: 0 for path in specs}
    vector_rows: dict[str, list[tuple[float, ...]]] = {path: [] for path in specs}
    vector_row_positions: dict[str, list[int]] = {path: [] for path in specs}
    materialized_documents: list[MaterializedVectorDocument] = []
    for position, document in enumerate(documents):
        vectors_by_path: dict[str, tuple[float, ...]] = {}
        for path, field_spec in specs.items():
            found, value = get_document_value(document, path)
            if not found or not isinstance(value, list):
                continue
            candidate = tuple(
                float(item)
                for item in value
                if isinstance(item, (int, float)) and not isinstance(item, bool)
            )
            if len(candidate) != len(value):
                continue
            if len(candidate) != int(field_spec["numDimensions"]):
                continue
            vectors_by_path[path] = candidate
            valid_vector_counts[path] += 1
            vector_rows[path].append(candidate)
            vector_row_positions[path].append(position)
        exists_paths: set[str] = set()
        scalar_values: dict[str, tuple[_FilterValueKey, ...]] = {}
        for path, values in collect_filterable_values(document):
            exists_paths.add(path)
            exists_index.setdefault(path, set()).add(position)
            normalized_values = tuple(sorted(values))
            scalar_values[path] = normalized_values
            if not normalized_values:
                continue
            path_index = scalar_index.setdefault(path, {})
            values_by_path = scalar_values_by_path.setdefault(path, [])
            for normalized in normalized_values:
                path_index.setdefault(normalized, set()).add(position)
                values_by_path.append((position, normalized))
        materialized_documents.append(
            MaterializedVectorDocument(
                document=document,
                vectors_by_path=vectors_by_path,
                exists_paths=frozenset(exists_paths),
                scalar_values=scalar_values,
            )
        )
    row_index_by_position = {
        path: {position: row_index for row_index, position in enumerate(values)}
        for path, values in vector_row_positions.items()
        if values
    }
    vector_row_exists_filter_index: dict[str, dict[str, frozenset[int]]] = {}
    vector_row_scalar_filter_index: dict[str, dict[str, dict[_FilterValueKey, frozenset[int]]]] = {}
    vector_row_scalar_values_by_path: dict[str, dict[str, tuple[tuple[int, _FilterValueKey], ...]]] = {}
    for vector_path, row_lookup in row_index_by_position.items():
        exists_by_path: dict[str, frozenset[int]] = {}
        for filter_path, positions in exists_index.items():
            matched_rows = frozenset(
                row_lookup[position]
                for position in positions
                if position in row_lookup
            )
            if matched_rows:
                exists_by_path[filter_path] = matched_rows
        scalar_by_path: dict[str, dict[_FilterValueKey, frozenset[int]]] = {}
        scalar_values_for_path: dict[str, tuple[tuple[int, _FilterValueKey], ...]] = {}
        for filter_path, values in scalar_index.items():
            row_values: dict[_FilterValueKey, frozenset[int]] = {}
            for value_key, positions in values.items():
                matched_rows = frozenset(
                    row_lookup[position]
                    for position in positions
                    if position in row_lookup
                )
                if matched_rows:
                    row_values[value_key] = matched_rows
            if row_values:
                scalar_by_path[filter_path] = row_values
        for filter_path, values in scalar_values_by_path.items():
            matched_entries = tuple(
                (row_lookup[position], value_key)
                for position, value_key in values
                if position in row_lookup
            )
            if matched_entries:
                scalar_values_for_path[filter_path] = matched_entries
        vector_row_exists_filter_index[vector_path] = exists_by_path
        vector_row_scalar_filter_index[vector_path] = scalar_by_path
        vector_row_scalar_values_by_path[vector_path] = scalar_values_for_path

    return MaterializedVectorIndex(
        documents=tuple(materialized_documents),
        vector_paths=tuple(specs),
        vector_specs=specs,
        valid_vector_counts=valid_vector_counts,
        vector_matrices={path: np.asarray(values, dtype=np.float32) for path, values in vector_rows.items() if values},
        vector_row_positions={path: tuple(values) for path, values in vector_row_positions.items() if values},
        vector_row_index_by_position=row_index_by_position,
        exists_filter_index={path: frozenset(values) for path, values in exists_index.items()},
        scalar_filter_index={
            path: {value_key: frozenset(values) for value_key, values in path_values.items()}
            for path, path_values in scalar_index.items()
        },
        scalar_values_by_path={path: tuple(values) for path, values in scalar_values_by_path.items()},
        vector_row_exists_filter_index=vector_row_exists_filter_index,
        vector_row_scalar_filter_index=vector_row_scalar_filter_index,
        vector_row_scalar_values_by_path=vector_row_scalar_values_by_path,
    )


def candidate_positions_for_vector_filter(
    vector_index: MaterializedVectorIndex,
    *,
    filter_spec: dict[str, object] | None,
    candidate_positions: tuple[int, ...] | list[int] | None = None,
) -> tuple[list[int] | None, dict[str, object] | None]:
    if filter_spec is None:
        return None, None
    ordered_positions = tuple(candidate_positions) if candidate_positions is not None else tuple(range(len(vector_index.documents)))
    all_positions = set(ordered_positions)
    result = evaluate_candidate_filter(
        filter_spec,
        all_candidates=ordered_positions,
        ordered_candidates=ordered_positions,
        clause_resolver=lambda path, clause: _candidate_positions_for_vector_filter_clause(
            vector_index,
            path=path,
            clause=clause,
            all_positions=all_positions,
        ),
    )
    if result is None or result.matches is None:
        return None, {
            "spec": filter_spec,
            "candidateable": False,
            "exact": False,
            "backend": None,
            "supportedPaths": [] if result is None else list(result.plan.supported_paths),
            "supportedClauseCount": 0 if result is None else result.plan.supported_clause_count,
            "unsupportedClauseCount": 1 if result is None else result.plan.unsupported_clause_count,
            "supportedOperators": [] if result is None else list(result.plan.supported_operators),
            "booleanShape": None if result is None else result.plan.shape,
        }
    return list(result.matches), result.to_metadata(backend="memory-vector-filter-index")


def candidate_rows_for_vector_filter(
    vector_index: MaterializedVectorIndex,
    *,
    query_path: str,
    filter_spec: dict[str, object] | None,
) -> tuple[list[int] | None, dict[str, object] | None]:
    if filter_spec is None:
        return None, None
    row_positions = tuple(range(len(vector_index.vector_row_positions.get(query_path, ()))))
    if not row_positions:
        return [], {
            "spec": filter_spec,
            "candidateable": True,
            "exact": True,
            "backend": "memory-vector-filter-index",
            "supportedPaths": [],
            "supportedClauseCount": 0,
            "unsupportedClauseCount": 0,
            "supportedOperators": [],
            "booleanShape": None,
        }
    all_rows = set(row_positions)
    result = evaluate_candidate_filter(
        filter_spec,
        all_candidates=row_positions,
        ordered_candidates=row_positions,
        clause_resolver=lambda path, clause: _candidate_rows_for_vector_filter_clause(
            vector_index,
            query_path=query_path,
            path=path,
            clause=clause,
            all_rows=all_rows,
        ),
    )
    if result is None or result.matches is None:
        return None, {
            "spec": filter_spec,
            "candidateable": False,
            "exact": False,
            "backend": None,
            "supportedPaths": [] if result is None else list(result.plan.supported_paths),
            "supportedClauseCount": 0 if result is None else result.plan.supported_clause_count,
            "unsupportedClauseCount": 1 if result is None else result.plan.unsupported_clause_count,
            "supportedOperators": [] if result is None else list(result.plan.supported_operators),
            "booleanShape": None if result is None else result.plan.shape,
        }
    return list(result.matches), result.to_metadata(backend="memory-vector-filter-index")


def vector_scores_for_positions(
    vector_index: MaterializedVectorIndex,
    *,
    query: SearchVectorQuery,
    candidate_positions: list[int],
    limit: int | None,
) -> list[tuple[float, int]]:
    matrix = vector_index.vector_matrices.get(query.path)
    if matrix is None or matrix.size == 0:
        return []
    selected_positions = list(candidate_positions)
    row_positions = vector_index.vector_row_positions.get(query.path)
    if not selected_positions or row_positions is None:
        return []
    if tuple(selected_positions) == row_positions:
        candidate_matrix = matrix
    else:
        row_index_by_position = vector_index.vector_row_index_by_position.get(query.path, {})
        selected_rows: list[int] = []
        filtered_positions: list[int] = []
        for position in selected_positions:
            row_index = row_index_by_position.get(position)
            if row_index is None:
                continue
            selected_rows.append(row_index)
            filtered_positions.append(position)
        if not selected_rows:
            return []
        selected_positions = filtered_positions
        candidate_matrix = matrix[np.asarray(selected_rows, dtype=np.int32)]
    query_vector = np.asarray(query.query_vector, dtype=np.float32)
    similarity = str(vector_index.vector_specs.get(query.path, {}).get("similarity", query.similarity))
    if similarity == "dotProduct":
        scores = candidate_matrix @ query_vector
    elif similarity == "euclidean":
        scores = -np.linalg.norm(candidate_matrix - query_vector, axis=1)
    else:
        candidate_norms = np.linalg.norm(candidate_matrix, axis=1)
        query_norm = float(np.linalg.norm(query_vector))
        denominator = candidate_norms * query_norm
        raw_scores = candidate_matrix @ query_vector
        scores = np.divide(raw_scores, denominator, out=np.zeros_like(raw_scores, dtype=np.float32), where=denominator > 0)
    if limit is not None and limit > 0 and len(scores) > limit:
        top_indexes = np.argpartition(scores, -limit)[-limit:]
        ordered_indexes = top_indexes[np.argsort(scores[top_indexes])[::-1]]
        return [(float(scores[int(index)]), selected_positions[int(index)]) for index in ordered_indexes]
    ordered_indexes = np.argsort(scores)[::-1]
    return [(float(scores[int(index)]), selected_positions[int(index)]) for index in ordered_indexes]


def vector_scores_for_rows(
    vector_index: MaterializedVectorIndex,
    *,
    query: SearchVectorQuery,
    candidate_rows: list[int],
    limit: int | None,
) -> list[tuple[float, int]]:
    matrix = vector_index.vector_matrices.get(query.path)
    if matrix is None or matrix.size == 0 or not candidate_rows:
        return []
    if len(candidate_rows) == matrix.shape[0] and candidate_rows == list(range(matrix.shape[0])):
        candidate_matrix = matrix
        selected_rows = candidate_rows
    else:
        selected_rows = list(candidate_rows)
        candidate_matrix = matrix[np.asarray(selected_rows, dtype=np.int32)]
    query_vector = np.asarray(query.query_vector, dtype=np.float32)
    similarity = str(vector_index.vector_specs.get(query.path, {}).get("similarity", query.similarity))
    if similarity == "dotProduct":
        scores = candidate_matrix @ query_vector
    elif similarity == "euclidean":
        scores = -np.linalg.norm(candidate_matrix - query_vector, axis=1)
    else:
        candidate_norms = np.linalg.norm(candidate_matrix, axis=1)
        query_norm = float(np.linalg.norm(query_vector))
        denominator = candidate_norms * query_norm
        raw_scores = candidate_matrix @ query_vector
        scores = np.divide(raw_scores, denominator, out=np.zeros_like(raw_scores, dtype=np.float32), where=denominator > 0)
    if limit is not None and limit > 0 and len(scores) > limit:
        top_indexes = np.argpartition(scores, -limit)[-limit:]
        ordered_indexes = top_indexes[np.argsort(scores[top_indexes])[::-1]]
        return [(float(scores[int(index)]), selected_rows[int(index)]) for index in ordered_indexes]
    ordered_indexes = np.argsort(scores)[::-1]
    return [(float(scores[int(index)]), selected_rows[int(index)]) for index in ordered_indexes]


def _candidate_positions_for_vector_filter_clause(
    vector_index: MaterializedVectorIndex,
    *,
    path: str,
    clause: object,
    all_positions: set[int],
) -> tuple[set[int] | None, str]:
    if isinstance(clause, dict) and set(clause) == {"$exists"} and isinstance(clause["$exists"], bool):
        existing = set(vector_index.exists_filter_index.get(path, ()))
        return (existing if clause["$exists"] else all_positions - existing), "$exists"
    if isinstance(clause, dict) and set(clause) == {"$in"} and isinstance(clause["$in"], list):
        matched_positions: set[int] = set()
        for item in clause["$in"]:
            value_key = filter_value_key(item)
            if value_key is None:
                return None, "$in"
            matched_positions.update(vector_index.scalar_filter_index.get(path, {}).get(value_key, ()))
        return matched_positions, "$in"
    if isinstance(clause, dict) and clause and set(clause).issubset({"$gt", "$gte", "$lt", "$lte"}):
        matched_positions = {
            position
            for position, value_key in vector_index.scalar_values_by_path.get(path, ())
            if value_key_matches_range(value_key, clause)
        }
        return matched_positions, "range"
    value_key = filter_value_key(clause)
    if value_key is None:
        return None, "eq"
    return set(vector_index.scalar_filter_index.get(path, {}).get(value_key, ())), "eq"


def _candidate_rows_for_vector_filter_clause(
    vector_index: MaterializedVectorIndex,
    *,
    query_path: str,
    path: str,
    clause: object,
    all_rows: set[int],
) -> tuple[set[int] | None, str]:
    row_exists_index = vector_index.vector_row_exists_filter_index.get(query_path, {})
    row_scalar_index = vector_index.vector_row_scalar_filter_index.get(query_path, {})
    row_scalar_values = vector_index.vector_row_scalar_values_by_path.get(query_path, {})
    if isinstance(clause, dict) and set(clause) == {"$exists"} and isinstance(clause["$exists"], bool):
        existing = set(row_exists_index.get(path, ()))
        return (existing if clause["$exists"] else all_rows - existing), "$exists"
    if isinstance(clause, dict) and set(clause) == {"$in"} and isinstance(clause["$in"], list):
        matched_rows: set[int] = set()
        for item in clause["$in"]:
            value_key = filter_value_key(item)
            if value_key is None:
                return None, "$in"
            matched_rows.update(row_scalar_index.get(path, {}).get(value_key, ()))
        return matched_rows, "$in"
    if isinstance(clause, dict) and clause and set(clause).issubset({"$gt", "$gte", "$lt", "$lte"}):
        matched_rows = {
            row_index
            for row_index, value_key in row_scalar_values.get(path, ())
            if value_key_matches_range(value_key, clause)
        }
        return matched_rows, "range"
    value_key = filter_value_key(clause)
    if value_key is None:
        return None, "eq"
    return set(row_scalar_index.get(path, {}).get(value_key, ())), "eq"
