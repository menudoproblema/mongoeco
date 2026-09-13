"""Capacity, ownership and reclamation of engine-shared vector query entries."""

import gc
import threading
import weakref

from concurrent.futures import ThreadPoolExecutor

import pytest

from mongoeco.engines._vector_query_cache import (
    VectorQueryCache,
    VectorQueryCacheBudget,
    _entry_size,
)


@pytest.mark.parametrize(["capacity", "entry"], [(-1, 0), (0, -1)])
def test_negative_budget_is_rejected(capacity, entry):
    with pytest.raises(ValueError, match="non-negative"):
        VectorQueryCacheBudget(capacity, entry)


def test_empty_budget_declines_without_raising():
    budget = VectorQueryCacheBudget(0)
    cache = VectorQueryCache(budget)
    cache["key"] = ((1.0, 1),)
    assert not cache
    assert cache.get("key") is None
    assert budget.stats()["rejections"] == 1
    assert budget.stats()["estimatedBytes"] == 0


def test_shared_lru_and_key_partitions():
    size = _entry_size("a", "value", 10000)
    budget = VectorQueryCacheBudget(2 * size)
    first = VectorQueryCache(budget)
    second = VectorQueryCache(budget)
    first["a"] = "value"
    second["a"] = "value"
    assert first["a"] == "value"  # Refresh only the first partition.
    first["b"] = "value"
    assert first.get("a") == "value"
    assert second.get("a") is None
    assert list(first) == ["a", "b"]
    stats = budget.stats()
    assert stats["entries"] == len(first)
    assert stats["estimatedBytes"] <= stats["capacityBytes"]
    assert stats["evictions"] == 1


def test_replacement_updates_accounting_and_oversized_value_removes_old_entry():
    budget = VectorQueryCacheBudget(4096, 2048)
    cache = VectorQueryCache(budget)
    cache["a"] = "small"
    first_bytes = budget.stats()["estimatedBytes"]
    cache["a"] = "larger" * 100
    assert budget.stats()["estimatedBytes"] > first_bytes
    assert len(cache) == 1
    cache["a"] = "too large" * 1000
    assert cache.get("a") is None
    assert budget.stats()["estimatedBytes"] == 0
    assert budget.stats()["rejections"] == 1


def test_clear_and_delete_only_release_own_partition():
    budget = VectorQueryCacheBudget()
    first = VectorQueryCache(budget)
    second = VectorQueryCache(budget)
    first["a"] = "one"
    first["b"] = "two"
    second["a"] = "three"
    del first["a"]
    assert first.get("a") is None
    with pytest.raises(KeyError):
        del first["a"]
    first.clear()
    first.clear()
    assert second["a"] == "three"
    assert budget.stats()["entries"] == 1
    second.clear()
    assert budget.stats()["estimatedBytes"] == 0


def test_budget_does_not_keep_discarded_query_cache_alive():
    budget = VectorQueryCacheBudget()
    cache = VectorQueryCache(budget)
    cache["key"] = tuple((float(index), index) for index in range(100))
    reference = weakref.ref(cache)
    assert budget.stats()["entries"] == 1
    del cache
    gc.collect()
    assert reference() is None
    assert budget.stats()["estimatedBytes"] == 0
    assert not budget._keys


def test_size_accounting_handles_aliases_cycles_and_declines_opaque_graphs():
    shared = ["value"]
    graph = [shared, shared]
    graph.append(graph)
    assert _entry_size("key", graph, 10000) is not None
    assert _entry_size("key", {"a": shared, "b": {1, 2}, "c": frozenset({3})}, 10000)
    assert _entry_size("key", object(), 10000) is None
    budget = VectorQueryCacheBudget()
    cache = VectorQueryCache(budget)
    cache["opaque"] = object()
    assert "opaque" not in cache


def test_multiple_threads_share_capacity_without_cross_partition_results():
    budget = VectorQueryCacheBudget(4096, 2048)
    caches = [VectorQueryCache(budget) for _ in range(4)]
    start = threading.Barrier(len(caches))

    def exercise(position):
        cache = caches[position]
        start.wait(timeout=5)
        for index in range(200):
            cache[index] = (position, index)
            observed = cache.get(index)
            assert observed is None or observed == (position, index)
            stats = budget.stats()
            assert 0 <= stats["estimatedBytes"] <= stats["capacityBytes"]

    with ThreadPoolExecutor(max_workers=len(caches)) as executor:
        list(executor.map(exercise, range(len(caches))))
    for cache in caches:
        cache.clear()
    assert budget.stats()["estimatedBytes"] == 0
