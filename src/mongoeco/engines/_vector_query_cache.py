"""Byte-admitted vector query caches sharing one engine-owned LRU budget."""

from __future__ import annotations

import sys
import threading
import weakref

from collections import OrderedDict
from collections.abc import Iterator, MutableMapping
from typing import cast


_VECTOR_QUERY_CACHE_BYTES = 8 * 1024 * 1024
_VECTOR_QUERY_CACHE_ENTRY_BYTES = 1024 * 1024
_ENTRY_OVERHEAD_BYTES = 256


def _entry_size(key: object, value: object, limit: int) -> int | None:
    """Count owned builtin graphs, stopping early or declining opaque values.

    Aliases within an entry count once; sharing across entries is conservatively
    counted again. The allowance includes bookkeeping, not native memory/RSS.
    An opaque value is never admitted with an unaccounted referent graph.
    """
    pending = [key, value]
    visited: set[int] = set()
    total = _ENTRY_OVERHEAD_BYTES
    while pending:
        item = pending.pop()
        if id(item) in visited:
            continue
        visited.add(id(item))
        total += sys.getsizeof(item)
        if total > limit:
            return None
        if type(item) is dict:
            pending.extend(item.keys())
            pending.extend(item.values())
        elif type(item) in (tuple, list, set, frozenset):
            pending.extend(item)
        elif type(item) not in (str, bytes, int, float, bool, type(None)):
            return None
    return total


class VectorQueryCacheBudget:
    """Own only query entries, never their materialized index or engine.

    Partition lifetimes are weak. Dropping the last index/operation reference
    releases its entries; a pinned old snapshot still shares the same budget.
    """

    def __init__(
        self,
        capacity_bytes: int = _VECTOR_QUERY_CACHE_BYTES,
        entry_bytes: int = _VECTOR_QUERY_CACHE_ENTRY_BYTES,
    ) -> None:
        if capacity_bytes < 0 or entry_bytes < 0:
            message = "vector cache budgets must be non-negative"
            raise ValueError(message)
        self.capacity_bytes = capacity_bytes
        self.entry_bytes = min(capacity_bytes, entry_bytes)
        self._entries: OrderedDict[tuple[int, object], tuple[object, int]] = (
            OrderedDict()
        )
        self._keys: dict[int, dict[object, None]] = {}
        self._next_partition = 0
        self._lock = threading.RLock()
        self._retained_bytes = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._rejections = 0

    def new_partition(self) -> int:
        with self._lock:
            self._next_partition += 1
            return self._next_partition

    def get(self, partition: int, key: object) -> object:
        with self._lock:
            identity = (partition, key)
            try:
                value, _size = self._entries[identity]
            except KeyError:
                self._misses += 1
                raise KeyError(key) from None
            self._entries.move_to_end(identity)
            self._hits += 1
            return value

    def put(self, partition: int, key: object, value: object) -> None:
        size = _entry_size(key, value, self.entry_bytes)
        with self._lock:
            identity = (partition, key)
            if identity in self._entries:
                self._remove(identity)
            if size is None:
                self._rejections += 1
                return
            while self._retained_bytes + size > self.capacity_bytes:
                self._remove(next(iter(self._entries)))
                self._evictions += 1
            self._entries[identity] = (value, size)
            self._keys.setdefault(partition, {})[key] = None
            self._retained_bytes += size

    def _remove(self, identity: tuple[int, object]) -> None:
        _value, size = self._entries.pop(identity)
        self._retained_bytes -= size
        partition, key = identity
        keys = self._keys[partition]
        del keys[key]
        if not keys:
            del self._keys[partition]
        if not self._entries:
            self._entries.clear()  # Also release the vacant hash-table capacity.

    def delete(self, partition: int, key: object) -> None:
        with self._lock:
            self._remove((partition, key))

    def clear_partition(self, partition: int) -> None:
        with self._lock:
            for key in tuple(self._keys.get(partition, ())):
                self._remove((partition, key))

    def keys(self, partition: int) -> tuple[object, ...]:
        with self._lock:
            return tuple(self._keys.get(partition, ()))

    def count(self, partition: int) -> int:
        with self._lock:
            return len(self._keys.get(partition, ()))

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "capacityBytes": self.capacity_bytes,
                "entryLimitBytes": self.entry_bytes,
                "estimatedBytes": self._retained_bytes,
                "entries": len(self._entries),
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "rejections": self._rejections,
            }


class VectorQueryCache[K, V](MutableMapping[K, V]):
    """Private mapping cache: writes may be declined by the shared budget.

    Values are owned, immutable query artifacts. Callers copy mutable metadata
    on entry/exit. No user operation fails when an entry cannot be admitted.
    """

    def __init__(self, budget: VectorQueryCacheBudget) -> None:
        self._budget = budget
        self._partition = budget.new_partition()
        self._finalizer = weakref.finalize(
            self, budget.clear_partition, self._partition
        )
        # Memory bookkeeping needs no process-exit callback. The registry keeps
        # the weak finalizer alive, but never keeps this cache/index alive.
        self._finalizer.atexit = False

    def __getitem__(self, key: K) -> V:
        return cast("V", self._budget.get(self._partition, key))

    def __setitem__(self, key: K, value: V) -> None:
        self._budget.put(self._partition, key, value)

    def __delitem__(self, key: K) -> None:
        self._budget.delete(self._partition, key)

    def __len__(self) -> int:
        return self._budget.count(self._partition)

    def __iter__(self) -> Iterator[K]:
        return iter(cast("tuple[K, ...]", self._budget.keys(self._partition)))

    def clear(self) -> None:
        self._budget.clear_partition(self._partition)
