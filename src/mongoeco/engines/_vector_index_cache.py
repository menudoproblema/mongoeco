"""Capacity-bounded ownership for materialized in-memory vector indexes."""

from __future__ import annotations

import datetime
import re
import sys
import threading
import uuid

from collections import OrderedDict
from collections.abc import Callable, Iterator, MutableMapping
from dataclasses import fields, is_dataclass
from decimal import Decimal
from typing import Any

import numpy as np

from mongoeco.engines._vector_query_cache import VectorQueryCache


_VECTOR_INDEX_CACHE_BYTES = 64 * 1024 * 1024
_VECTOR_INDEX_CACHE_ENTRY_BYTES = 32 * 1024 * 1024
_ENTRY_OVERHEAD_BYTES = 256
_LEAF_TYPES = (
    str,
    bytes,
    int,
    float,
    bool,
    type(None),
    datetime.date,
    datetime.time,
    datetime.timedelta,
    Decimal,
    uuid.UUID,
    re.Pattern,
)


def estimate_materialized_index_bytes(value: object, limit: int) -> int | None:
    """Estimate owned Python/native buffers, declining unknown object graphs.

    Shared objects inside one index count once. Sharing between cached indexes
    counts independently so admission never relies on undocumented aliases.
    Query-cache budgets are separate owners and are intentionally opaque here.
    """
    pending = [value]
    visited: set[int] = set()
    total = _ENTRY_OVERHEAD_BYTES
    while pending:
        item = pending.pop()
        identity = id(item)
        if identity in visited:
            continue
        visited.add(identity)
        total += sys.getsizeof(item)
        if total > limit:
            return None
        if type(item) is dict:
            pending.extend(item.keys())
            pending.extend(item.values())
        elif type(item) in (tuple, list, set, frozenset):
            pending.extend(item)
        elif isinstance(item, np.ndarray):
            # ``getsizeof`` includes an owned ndarray buffer but not a borrowed
            # base. Materialized indexes own their arrays; reject unexpected
            # views instead of under-accounting their owner graph.
            if item.base is not None:
                return None
        elif isinstance(item, (VectorQueryCache, *_LEAF_TYPES)):
            continue
        elif is_dataclass(item) and not isinstance(item, type):
            pending.extend(getattr(item, field.name) for field in fields(item))
        else:
            return None
    return total


class VectorIndexCache[K, V](MutableMapping[K, V]):
    """Engine-owned byte LRU; rejected entries remain valid uncached values."""

    def __init__(
        self,
        *,
        capacity_bytes: int = _VECTOR_INDEX_CACHE_BYTES,
        entry_bytes: int = _VECTOR_INDEX_CACHE_ENTRY_BYTES,
        size_of: Callable[[V, int], int | None] = estimate_materialized_index_bytes,
    ) -> None:
        if capacity_bytes < 0 or entry_bytes < 0:
            message = "vector index cache budgets must be non-negative"
            raise ValueError(message)
        self.capacity_bytes = capacity_bytes
        self.entry_bytes = min(capacity_bytes, entry_bytes)
        self._size_of = size_of
        self._entries: OrderedDict[K, tuple[V, int]] = OrderedDict()
        self._retained_bytes = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._rejections = 0
        self._lock = threading.RLock()

    def __getitem__(self, key: K) -> V:
        with self._lock:
            try:
                value, _size = self._entries[key]
            except KeyError:
                self._misses += 1
                raise
            self._entries.move_to_end(key)
            self._hits += 1
            return value

    def get(self, key: K, default: Any = None) -> V | Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key: K, value: V) -> None:
        size = self._size_of(value, self.entry_bytes)
        with self._lock:
            if key in self._entries:
                self._remove(key)
            if size is None:
                self._rejections += 1
                return
            while self._retained_bytes + size > self.capacity_bytes:
                self._remove(next(iter(self._entries)))
                self._evictions += 1
            self._entries[key] = (value, size)
            self._retained_bytes += size

    def _remove(self, key: K) -> None:
        _value, size = self._entries.pop(key)
        self._retained_bytes -= size
        if not self._entries:
            self._entries.clear()

    def __delitem__(self, key: K) -> None:
        with self._lock:
            self._remove(key)

    def pop(self, key: K, default: Any = None) -> V | Any:
        with self._lock:
            if key not in self._entries:
                return default
            value, _size = self._entries[key]
            self._remove(key)
            return value

    def __iter__(self) -> Iterator[K]:
        with self._lock:
            return iter(tuple(self._entries))

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._retained_bytes = 0

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
