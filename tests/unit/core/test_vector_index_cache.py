from dataclasses import dataclass

import numpy as np
import pytest

from mongoeco.engines._vector_index_cache import (
    VectorIndexCache,
    estimate_materialized_index_bytes,
)


_ENTRY_SIZE = 4


def test_vector_index_cache_evicts_lru_entries_by_shared_byte_capacity():
    cache = VectorIndexCache[str, int](
        capacity_bytes=10,
        entry_bytes=10,
        size_of=lambda value, _limit: value,
    )
    cache["first"] = _ENTRY_SIZE
    cache["second"] = _ENTRY_SIZE
    assert cache["first"] == _ENTRY_SIZE

    cache["third"] = _ENTRY_SIZE

    assert tuple(cache) == ("first", "third")
    assert cache.get("second") is None
    assert cache.stats() == {
        "capacityBytes": 10,
        "entryLimitBytes": 10,
        "estimatedBytes": 8,
        "entries": 2,
        "hits": 1,
        "misses": 1,
        "evictions": 1,
        "rejections": 0,
    }


def test_vector_index_cache_declines_oversized_entries_without_rejecting_value():
    cache = VectorIndexCache[str, int](
        capacity_bytes=10,
        entry_bytes=5,
        size_of=lambda value, limit: value if value <= limit else None,
    )

    cache["oversized"] = 6

    assert not cache
    assert cache.stats()["rejections"] == 1


def test_vector_index_cache_rejects_negative_budgets_and_replaces_entries():
    with pytest.raises(ValueError, match="budgets must be non-negative"):
        VectorIndexCache(capacity_bytes=-1)

    cache = VectorIndexCache[str, int](
        capacity_bytes=10,
        entry_bytes=10,
        size_of=lambda value, _limit: value,
    )
    cache["same"] = 3
    cache["same"] = 5

    assert cache["same"] == 5
    assert cache.stats()["estimatedBytes"] == 5


def test_materialized_index_estimator_counts_known_graphs_and_native_arrays():
    @dataclass(frozen=True, slots=True)
    class KnownIndex:
        documents: tuple[dict[str, object], ...]
        matrix: np.ndarray

    index = KnownIndex(({"_id": 1, "vector": [1.0, 2.0]},), np.ones((4, 2)))

    estimated = estimate_materialized_index_bytes(index, 1_000_000)

    assert estimated is not None
    assert estimated > index.matrix.nbytes
    assert estimate_materialized_index_bytes(index, 1) is None


def test_materialized_index_estimator_declines_unknown_or_borrowed_graphs():
    class Opaque:
        pass

    owner = np.ones((4, 2))

    assert estimate_materialized_index_bytes(Opaque(), 1_000_000) is None
    assert estimate_materialized_index_bytes(owner[:, :1], 1_000_000) is None
