"""Differential ranking oracle, including ties and exceptional IEEE values."""

import math

from dataclasses import replace

import numpy as np
import pytest

from mongoeco.core.search import SearchVectorQuery
from mongoeco.engines import _memory_vector_runtime as vector_runtime
from mongoeco.engines._memory_vector_runtime import (
    build_materialized_vector_index,
    vector_scores_for_rows,
)
from mongoeco.engines._vector_query_cache import VectorQueryCacheBudget
from mongoeco.engines._vector_ranking import top_score_indexes, top_scored_rows
from mongoeco.types import SearchIndexDefinition


@pytest.mark.parametrize("public_order", [False, True])
@pytest.mark.parametrize("limit", [None, 0, 1, 2, 5, 40, 100])
def test_partition_selection_equals_stable_complete_sort(public_order, limit):
    generator = np.random.default_rng(173)
    for size in (0, 1, 2, 7, 40, 100):
        scores = generator.integers(-3, 4, size=size).astype(np.float32)
        keys = tuple(str(number % 11) for number in reversed(range(size)))
        tie_key = keys.__getitem__ if public_order else None
        expected = list(map(int, np.argsort(-scores, kind="stable")))
        if public_order:
            expected.sort(key=lambda number: (-float(scores[number]), keys[number]))
        if limit is not None and limit > 0:
            expected = expected[:limit]
        assert top_score_indexes(scores, limit, tie_key=tie_key) == expected


@pytest.mark.parametrize(
    "values",
    [
        [float("nan"), 1.0, 1.0, -1.0],
        [float("inf"), float("-inf"), float("inf"), 0.0],
        [0.0, -0.0, 0.0, -0.0],
    ],
)
def test_exceptional_values_preserve_full_sort_order(values):
    scores = np.asarray(values, dtype=np.float32)
    keys = ("d", "c", "b", "a")
    expected = list(map(int, np.argsort(-scores, kind="stable")))
    expected.sort(key=lambda number: (-float(scores[number]), keys[number]))
    for limit in (1, 2, 4):
        assert (
            top_score_indexes(scores, limit, tie_key=keys.__getitem__)
            == expected[:limit]
        )
        rows = [
            (float(scores[number]), number)
            for number in np.argsort(-scores, kind="stable")
        ]
        assert [row for _score, row in top_scored_rows(rows, limit, keys)] == expected[
            :limit
        ]


def index_and_query(similarity, budget):
    documents = [
        {"_id": identifier, "embedding": vector}
        for identifier, vector in zip(
            ("d", "c", "b", "a"),
            ([1.0, 0.0], [0.5, 0.5], [0.0, 0.0], [1.0, 0.0]),
            strict=True,
        )
    ]
    index = build_materialized_vector_index(
        documents,
        SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 2,
                        "similarity": similarity,
                    }
                ]
            },
            name="vectors",
            index_type="vectorSearch",
        ),
        query_cache_budget=budget,
    )
    query = SearchVectorQuery(
        index_name="vectors",
        path="embedding",
        query_vector=(1.0, 0.0),
        similarity=similarity,
        num_candidates=10,
        limit=2,
    )
    return index, query


@pytest.mark.parametrize("similarity", ["dotProduct", "cosine", "euclidean"])
@pytest.mark.parametrize("capacity", [0, 2048, 1024 * 1024])
def test_score_cache_and_topk_equal_uncached_full_matrix_oracle(similarity, capacity):
    index, query = index_and_query(similarity, VectorQueryCacheBudget(capacity))
    matrix = index.vector_matrices["embedding"]
    vector = np.asarray(query.query_vector, dtype=np.float32)
    if similarity == "dotProduct":
        raw = matrix @ vector
    elif similarity == "euclidean":
        raw = -np.linalg.norm(matrix - vector, axis=1)
    else:
        dot = matrix @ vector
        divisor = np.linalg.norm(matrix, axis=1) * float(np.linalg.norm(vector))
        raw = np.divide(dot, divisor, out=np.zeros_like(dot), where=divisor > 0)
    assert not matrix.flags.writeable
    if similarity == "cosine":
        assert not index.vector_row_norms["embedding"].flags.writeable
    for _repeat in range(2):
        for public_order in (True, False):
            for rows in ([0, 1, 2, 3], [3, 0, 3], [1], [], [-1, 99]):
                for minimum in (None, 0.0, 0.75, 2.0):
                    bound_query = replace(query, min_score=minimum)
                    expected = [
                        (float(raw[row]), row)
                        for row in np.argsort(-raw, kind="stable")
                        if row in rows
                        and (minimum is None or float(raw[row]) >= minimum)
                    ]
                    if public_order:
                        expected.sort(
                            key=lambda item: (
                                -item[0],
                                index.vector_row_tie_keys["embedding"][item[1]],
                            )
                        )
                    assert (
                        vector_scores_for_rows(
                            index,
                            query=bound_query,
                            candidate_rows=rows,
                            limit=2,
                            public_order=public_order,
                        )
                        == expected[:2]
                    )


def test_min_score_does_not_round_the_threshold_to_float32():
    index, query = index_and_query("dotProduct", VectorQueryCacheBudget())
    above = math.nextafter(1.0, math.inf)
    assert (
        vector_scores_for_rows(
            index, query=replace(query, min_score=above), candidate_rows=[0, 3], limit=2
        )
        == []
    )


@pytest.mark.parametrize("minimum", [None, 0.0, 2.0])
def test_nan_subset_fallback_preserves_previous_score_walk(minimum):
    index, query = index_and_query("dotProduct", VectorQueryCacheBudget())
    # Pin an exceptional scoring artifact without generating overflow warnings.
    index.vector_score_cache[("embedding", (1.0, 0.0), "dotProduct")] = (
        float("nan"),
        1.0,
        1.0,
        float("nan"),
    )
    for rows in ([0, 1, 2, 3], [0, 1, 3]):
        result = vector_scores_for_rows(
            index,
            query=replace(query, min_score=minimum),
            candidate_rows=rows,
            limit=None,
        )
        if minimum is None:
            expected_rows = (
                [1, 2, 0, 3] if len(rows) == len(index.documents) else [1, 0, 3]
            )
        elif minimum == 0.0:
            expected_rows = [1, 2] if len(rows) == len(index.documents) else [1, 0, 3]
        else:
            expected_rows = []
        assert [row for _score, row in result] == expected_rows


@pytest.mark.parametrize("similarity", ["dotProduct", "cosine", "euclidean"])
def test_finite_topk_scores_large_candidate_sets_in_bounded_chunks(
    similarity,
    monkeypatch,
):
    budget = VectorQueryCacheBudget(0)
    documents = [{"_id": row, "embedding": [1.0, row / 20.0]} for row in range(20)]
    index = build_materialized_vector_index(
        documents,
        SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 2,
                        "similarity": similarity,
                    }
                ]
            },
            name="vectors",
            index_type="vectorSearch",
        ),
        query_cache_budget=budget,
    )
    query = SearchVectorQuery(
        index_name="vectors",
        path="embedding",
        query_vector=(1.0, 1.0),
        similarity=similarity,
        num_candidates=20,
        limit=3,
    )
    original = vector_runtime._score_vector_matrix
    chunk_sizes = []

    def observe(matrix, *args, **kwargs):
        chunk_sizes.append(len(matrix))
        return original(matrix, *args, **kwargs)

    monkeypatch.setattr(vector_runtime, "_VECTOR_SCORE_CHUNK_ROWS", 4)
    monkeypatch.setattr(vector_runtime, "_score_vector_matrix", observe)
    result = vector_scores_for_rows(
        index,
        query=query,
        candidate_rows=list(range(20)),
        limit=3,
        public_order=True,
    )

    matrix = index.vector_matrices["embedding"]
    vector = np.asarray(query.query_vector, dtype=np.float32)
    if similarity == "dotProduct":
        expected_scores = matrix @ vector
    elif similarity == "euclidean":
        expected_scores = -np.linalg.norm(matrix - vector, axis=1)
    else:
        denominator = np.linalg.norm(matrix, axis=1) * float(np.linalg.norm(vector))
        expected_scores = np.divide(
            matrix @ vector,
            denominator,
            out=np.zeros(len(matrix), dtype=np.float32),
            where=denominator > 0,
        )
    expected_rows = list(range(len(matrix)))
    expected_rows.sort(
        key=lambda row: (
            -float(expected_scores[row]),
            index.vector_row_tie_keys["embedding"][row],
        )
    )
    assert [row for _score, row in result] == expected_rows[:3]
    assert chunk_sizes == [4, 4, 4, 4, 4]
