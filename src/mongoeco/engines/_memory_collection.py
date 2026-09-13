"""Persistent collection and secondary-index roots for the memory engine."""

from __future__ import annotations

from collections.abc import ItemsView, KeysView, Set as AbstractSet, ValuesView
from copy import deepcopy
from dataclasses import dataclass
from heapq import nsmallest
from typing import TYPE_CHECKING, Any, Self, cast

from rpds import HashTrieMap, HashTrieSet


if TYPE_CHECKING:
    import datetime

    from collections.abc import Iterable, Iterator, Mapping


_ABSENT = object()
_DELETED = object()
_NO_CHUNK = -1
_ORDER_CHUNK_SIZE = 128


@dataclass(frozen=True, slots=True)
class _ExpiryNode[K]:
    key: tuple[datetime.datetime, int]
    storage_key: K
    priority: int
    left: _ExpiryNode[K] | None = None
    right: _ExpiryNode[K] | None = None


@dataclass(frozen=True, slots=True)
class _MembershipNode[K]:
    ordinal: int
    storage_key: K
    priority: int
    left: _MembershipNode[K] | None = None
    right: _MembershipNode[K] | None = None


type _IndexState[K] = tuple[
    HashTrieMap[tuple[Any, ...], HashTrieSet[K]],
    HashTrieMap[tuple[Any, ...], HashTrieMap[K, int]],
    HashTrieMap[tuple[Any, ...], _MembershipNode[K]],
    int,
    HashTrieMap[K, tuple[datetime.datetime, int]],
    _ExpiryNode[K] | None,
    int,
]


def _expiry_priority(token: int) -> int:
    """Return a well-distributed deterministic priority for the persistent treap."""
    value = (token + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return value ^ (value >> 31)


def _insert_membership[K](
    root: _MembershipNode[K] | None,
    node: _MembershipNode[K],
) -> _MembershipNode[K]:
    if root is None:
        return node
    if node.priority < root.priority:
        left, right = _split_membership(root, node.ordinal)
        return _MembershipNode(
            node.ordinal,
            node.storage_key,
            node.priority,
            left,
            right,
        )
    if node.ordinal < root.ordinal:
        return _MembershipNode(
            root.ordinal,
            root.storage_key,
            root.priority,
            _insert_membership(root.left, node),
            root.right,
        )
    return _MembershipNode(
        root.ordinal,
        root.storage_key,
        root.priority,
        root.left,
        _insert_membership(root.right, node),
    )


def _split_membership[K](
    root: _MembershipNode[K] | None,
    ordinal: int,
) -> tuple[_MembershipNode[K] | None, _MembershipNode[K] | None]:
    if root is None:
        return None, None
    if root.ordinal < ordinal:
        left, right = _split_membership(root.right, ordinal)
        return (
            _MembershipNode(
                root.ordinal,
                root.storage_key,
                root.priority,
                root.left,
                left,
            ),
            right,
        )
    left, right = _split_membership(root.left, ordinal)
    return (
        left,
        _MembershipNode(
            root.ordinal,
            root.storage_key,
            root.priority,
            right,
            root.right,
        ),
    )


def _merge_membership[K](
    left: _MembershipNode[K] | None,
    right: _MembershipNode[K] | None,
) -> _MembershipNode[K] | None:
    if left is None:
        return right
    if right is None:
        return left
    if left.priority < right.priority:
        return _MembershipNode(
            left.ordinal,
            left.storage_key,
            left.priority,
            left.left,
            _merge_membership(left.right, right),
        )
    return _MembershipNode(
        right.ordinal,
        right.storage_key,
        right.priority,
        _merge_membership(left, right.left),
        right.right,
    )


def _remove_membership[K](
    root: _MembershipNode[K] | None,
    ordinal: int,
) -> _MembershipNode[K] | None:
    if root is None:
        return None
    if ordinal == root.ordinal:
        return _merge_membership(root.left, root.right)
    if ordinal < root.ordinal:
        return _MembershipNode(
            root.ordinal,
            root.storage_key,
            root.priority,
            _remove_membership(root.left, ordinal),
            root.right,
        )
    return _MembershipNode(
        root.ordinal,
        root.storage_key,
        root.priority,
        root.left,
        _remove_membership(root.right, ordinal),
    )


def _iter_membership_keys[K](
    root: _MembershipNode[K] | None,
    limit: int | None,
) -> Iterator[K]:
    if limit is not None and limit <= 0:
        return
    stack: list[_MembershipNode[K]] = []
    current = root
    emitted = 0
    while current is not None or stack:
        while current is not None:
            stack.append(current)
            current = current.left
        current = stack.pop()
        yield current.storage_key
        emitted += 1
        if limit is not None and emitted >= limit:
            return
        current = current.right


def _split_expiry[K](
    root: _ExpiryNode[K] | None,
    key: tuple[datetime.datetime, int],
) -> tuple[_ExpiryNode[K] | None, _ExpiryNode[K] | None]:
    if root is None:
        return None, None
    if root.key < key:
        left_of_key, right_of_key = _split_expiry(root.right, key)
        return (
            _ExpiryNode(
                root.key,
                root.storage_key,
                root.priority,
                root.left,
                left_of_key,
            ),
            right_of_key,
        )
    left_of_key, right_of_key = _split_expiry(root.left, key)
    return (
        left_of_key,
        _ExpiryNode(
            root.key,
            root.storage_key,
            root.priority,
            right_of_key,
            root.right,
        ),
    )


def _insert_expiry[K](
    root: _ExpiryNode[K] | None,
    node: _ExpiryNode[K],
) -> _ExpiryNode[K]:
    if root is None:
        return node
    if node.priority < root.priority:
        left, right = _split_expiry(root, node.key)
        return _ExpiryNode(
            node.key,
            node.storage_key,
            node.priority,
            left,
            right,
        )
    if node.key < root.key:
        return _ExpiryNode(
            root.key,
            root.storage_key,
            root.priority,
            _insert_expiry(root.left, node),
            root.right,
        )
    return _ExpiryNode(
        root.key,
        root.storage_key,
        root.priority,
        root.left,
        _insert_expiry(root.right, node),
    )


def _merge_expiry[K](
    left: _ExpiryNode[K] | None,
    right: _ExpiryNode[K] | None,
) -> _ExpiryNode[K] | None:
    if left is None:
        return right
    if right is None:
        return left
    if left.priority < right.priority:
        return _ExpiryNode(
            left.key,
            left.storage_key,
            left.priority,
            left.left,
            _merge_expiry(left.right, right),
        )
    return _ExpiryNode(
        right.key,
        right.storage_key,
        right.priority,
        _merge_expiry(left, right.left),
        right.right,
    )


def _remove_expiry[K](
    root: _ExpiryNode[K] | None,
    key: tuple[datetime.datetime, int],
) -> _ExpiryNode[K] | None:
    if root is None:
        return None  # pragma: no cover - the storage map and tree share one root
    if key == root.key:
        return _merge_expiry(root.left, root.right)
    if key < root.key:
        return _ExpiryNode(
            root.key,
            root.storage_key,
            root.priority,
            _remove_expiry(root.left, key),
            root.right,
        )
    return _ExpiryNode(
        root.key,
        root.storage_key,
        root.priority,
        root.left,
        _remove_expiry(root.right, key),
    )


def _iter_expired[K](
    root: _ExpiryNode[K] | None,
    now: datetime.datetime,
) -> Iterator[K]:
    if root is None:
        return
    yield from _iter_expired(root.left, now)
    if root.key[0] > now:
        return
    yield root.storage_key
    yield from _iter_expired(root.right, now)


@dataclass(frozen=True, slots=True)
class _OrderChunk[K, V]:
    previous: int
    following: int
    items: tuple[object, ...]
    live_count: int


class _MemoryKeysView[K, V](KeysView[K]):
    __slots__ = ("_collection",)

    def __init__(self, collection: MemoryCollection[K, V]) -> None:
        super().__init__(collection)
        self._collection = collection

    def __iter__(self) -> Iterator[K]:
        yield from self._collection


class _MemoryItemsView[K, V](ItemsView[K, V]):
    __slots__ = ("_collection",)

    def __init__(self, collection: MemoryCollection[K, V]) -> None:
        super().__init__(collection)
        self._collection = collection

    def __iter__(self) -> Iterator[tuple[K, V]]:
        yield from self._collection._iter_items()


class _MemoryValuesView[K, V](ValuesView[V]):
    __slots__ = ("_collection",)

    def __init__(self, collection: MemoryCollection[K, V]) -> None:
        super().__init__(collection)
        self._collection = collection

    def __iter__(self) -> Iterator[V]:
        for _, value in self._collection._iter_items():
            yield value


class MemoryCollection[K, V](dict[K, V]):
    """A dict-compatible persistent root with collection-local insertion order.

    Copies share immutable HAMT roots. Writes replace only the affected HAMT
    paths and one bounded order chunk, so neither snapshot capture nor the first
    mutation copies every row. The chunk chain keeps full scans linear and has
    only bounded tombstones, compacted independently per chunk.
    """

    def __init__(self, values: Mapping[K, V] | Iterable[tuple[K, V]] = ()) -> None:
        super().__init__()
        self._values: HashTrieMap[K, V] = HashTrieMap()
        self._ordinals: HashTrieMap[K, int] = HashTrieMap()
        self._locations: HashTrieMap[K, tuple[int, int]] = HashTrieMap()
        self._chunks: HashTrieMap[int, _OrderChunk[K, V]] = HashTrieMap()
        self._head_chunk = _NO_CHUNK
        self._tail_chunk = _NO_CHUNK
        self._next_chunk = 0
        self._next_ordinal = 0
        self.update(values)

    def __contains__(self, key: object) -> bool:
        return key in self._values

    def __getitem__(self, key: K) -> V:
        return self._values[key]

    def __setitem__(self, key: K, value: V) -> None:
        location = self._locations.get(key)
        if location is not None:
            chunk_id, offset = location
            chunk = self._chunks[chunk_id]
            items = chunk.items
            replacement = (*items[:offset], (key, value), *items[offset + 1 :])
            self._chunks = self._chunks.insert(
                chunk_id,
                _OrderChunk(
                    chunk.previous,
                    chunk.following,
                    replacement,
                    chunk.live_count,
                ),
            )
            self._values = self._values.insert(key, value)
            return

        self._ordinals = self._ordinals.insert(key, self._next_ordinal)
        self._next_ordinal += 1
        if self._tail_chunk != _NO_CHUNK:
            tail = self._chunks[self._tail_chunk]
            if len(tail.items) < _ORDER_CHUNK_SIZE:
                offset = len(tail.items)
                self._chunks = self._chunks.insert(
                    self._tail_chunk,
                    _OrderChunk(
                        tail.previous,
                        tail.following,
                        (*tail.items, (key, value)),
                        tail.live_count + 1,
                    ),
                )
                self._locations = self._locations.insert(
                    key,
                    (self._tail_chunk, offset),
                )
                self._values = self._values.insert(key, value)
                return

        chunk_id = self._next_chunk
        self._next_chunk += 1
        previous = self._tail_chunk
        self._chunks = self._chunks.insert(
            chunk_id,
            _OrderChunk(previous, _NO_CHUNK, ((key, value),), 1),
        )
        if previous == _NO_CHUNK:
            self._head_chunk = chunk_id
        else:
            previous_chunk = self._chunks[previous]
            self._chunks = self._chunks.insert(
                previous,
                _OrderChunk(
                    previous_chunk.previous,
                    chunk_id,
                    previous_chunk.items,
                    previous_chunk.live_count,
                ),
            )
        self._tail_chunk = chunk_id
        self._locations = self._locations.insert(key, (chunk_id, 0))
        self._values = self._values.insert(key, value)

    def __delitem__(self, key: K) -> None:
        chunk_id, offset = self._locations[key]
        chunk = self._chunks[chunk_id]
        remaining_count = chunk.live_count - 1
        self._values = self._values.remove(key)
        self._ordinals = self._ordinals.remove(key)
        self._locations = self._locations.remove(key)

        if remaining_count:
            marked = (*chunk.items[:offset], _DELETED, *chunk.items[offset + 1 :])
            deleted_count = len(marked) - remaining_count
            if deleted_count * 2 < len(marked):
                self._chunks = self._chunks.insert(
                    chunk_id,
                    _OrderChunk(
                        chunk.previous,
                        chunk.following,
                        marked,
                        remaining_count,
                    ),
                )
                return
            compacted = tuple(item for item in marked if item is not _DELETED)
            self._chunks = self._chunks.insert(
                chunk_id,
                _OrderChunk(
                    chunk.previous,
                    chunk.following,
                    compacted,
                    remaining_count,
                ),
            )
            for position, item in enumerate(compacted):
                shifted_key, _ = cast("tuple[K, V]", item)
                self._locations = self._locations.insert(
                    shifted_key,
                    (chunk_id, position),
                )
            return

        self._chunks = self._chunks.remove(chunk_id)
        if chunk.previous == _NO_CHUNK:
            self._head_chunk = chunk.following
        else:
            previous = self._chunks[chunk.previous]
            self._chunks = self._chunks.insert(
                chunk.previous,
                _OrderChunk(
                    previous.previous,
                    chunk.following,
                    previous.items,
                    previous.live_count,
                ),
            )
        if chunk.following == _NO_CHUNK:
            self._tail_chunk = chunk.previous
        else:
            following = self._chunks[chunk.following]
            self._chunks = self._chunks.insert(
                chunk.following,
                _OrderChunk(
                    chunk.previous,
                    following.following,
                    following.items,
                    following.live_count,
                ),
            )

    def _iter_items(self) -> Iterator[tuple[K, V]]:
        chunk_id = self._head_chunk
        while chunk_id != _NO_CHUNK:
            chunk = self._chunks[chunk_id]
            for item in chunk.items:
                if item is not _DELETED:
                    yield cast("tuple[K, V]", item)
            chunk_id = chunk.following

    def __iter__(self) -> Iterator[K]:
        for key, _ in self._iter_items():
            yield key

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return repr(dict(self.items()))

    def __eq__(self, other: object) -> bool:
        if not hasattr(other, "items"):
            return False
        return dict(self.items()) == dict(other.items())

    __hash__ = None

    def keys(self) -> KeysView[K]:  # type: ignore[override]
        return _MemoryKeysView(self)

    def items(self) -> ItemsView[K, V]:  # type: ignore[override]
        return _MemoryItemsView(self)

    def values(self) -> ValuesView[V]:  # type: ignore[override]
        return _MemoryValuesView(self)

    def get(self, key: K, default: Any = None) -> V | Any:
        return self._values.get(key, default)

    def clear(self) -> None:
        self._values = HashTrieMap()
        self._ordinals = HashTrieMap()
        self._locations = HashTrieMap()
        self._chunks = HashTrieMap()
        self._head_chunk = _NO_CHUNK
        self._tail_chunk = _NO_CHUNK
        self._next_chunk = 0
        self._next_ordinal = 0

    def pop(self, key: K, *default: V) -> V:  # type: ignore[override]
        if len(default) > 1:
            message = "pop expected at most two arguments"
            raise TypeError(message)
        if key in self:
            value = self[key]
            del self[key]
            return value
        if default:
            return default[0]
        raise KeyError(key)

    def popitem(self) -> tuple[K, V]:
        if self._tail_chunk == _NO_CHUNK:
            message = "popitem(): dictionary is empty"
            raise KeyError(message)
        tail = self._chunks[self._tail_chunk]
        key, value = cast(
            "tuple[K, V]",
            next(item for item in reversed(tail.items) if item is not _DELETED),
        )
        del self[key]
        return key, value

    def setdefault(self, key: K, default: Any = None) -> V:
        if key not in self:
            self[key] = default
        return self[key]

    def update(self, values=(), **kwargs) -> None:
        items = values.items() if hasattr(values, "items") else values
        for key, value in items:
            self[key] = value
        for key, value in kwargs.items():
            self[key] = value  # type: ignore[index]

    def __ior__(self, values) -> Self:  # type: ignore[override, misc]
        self.update(values)
        return self

    def copy(self) -> Self:
        result = type(self)()
        result._values = self._values
        result._ordinals = self._ordinals
        result._locations = self._locations
        result._chunks = self._chunks
        result._head_chunk = self._head_chunk
        result._tail_chunk = self._tail_chunk
        result._next_chunk = self._next_chunk
        result._next_ordinal = self._next_ordinal
        return result

    def __copy__(self) -> Self:
        return self.copy()

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        result = type(self)()
        memo[id(self)] = result
        for key, value in self.items():
            result[deepcopy(key, memo)] = deepcopy(value, memo)
        return result

    def _snapshot_state(self) -> tuple[Any, ...]:
        return (
            self._values,
            self._ordinals,
            self._locations,
            self._chunks,
            self._head_chunk,
            self._tail_chunk,
            self._next_chunk,
            self._next_ordinal,
        )

    def _restore_state(self, state: tuple[Any, ...]) -> None:
        (
            self._values,
            self._ordinals,
            self._locations,
            self._chunks,
            self._head_chunk,
            self._tail_chunk,
            self._next_chunk,
            self._next_ordinal,
        ) = state

    def ordered_keys(
        self,
        candidates: Iterable[K],
        *,
        limit: int | None = None,
    ) -> list[K]:
        present = (key for key in candidates if key in self)
        if limit is None:
            return sorted(present, key=self._ordinals.__getitem__)
        if limit <= 0:
            return []
        return nsmallest(limit, present, key=self._ordinals.__getitem__)

    def ordinal_for(self, key: K) -> int:
        return self._ordinals[key]


class MemoryIndexMap[K](dict[tuple[Any, ...], AbstractSet[K]]):
    """Persistent secondary-index buckets and an optional ordered TTL schedule."""

    def __init__(
        self,
        values: Mapping[tuple[Any, ...], Iterable[K]]
        | Iterable[tuple[tuple[Any, ...], Iterable[K]]] = (),
    ) -> None:
        super().__init__()
        self._buckets: HashTrieMap[tuple[Any, ...], HashTrieSet[K]] = HashTrieMap()
        self._bucket_ordinals: HashTrieMap[tuple[Any, ...], HashTrieMap[K, int]] = (
            HashTrieMap()
        )
        self._bucket_order_roots: HashTrieMap[tuple[Any, ...], _MembershipNode[K]] = (
            HashTrieMap()
        )
        self._next_membership_ordinal = 0
        self._expiry_by_storage: HashTrieMap[K, tuple[datetime.datetime, int]] = (
            HashTrieMap()
        )
        self._expiry_root: _ExpiryNode[K] | None = None
        self._next_expiry_token = 0
        self.update(values)

    def __contains__(self, key: object) -> bool:
        return key in self._buckets

    def __getitem__(self, key: tuple[Any, ...]) -> HashTrieSet[K]:
        return self._buckets[key]

    def __setitem__(self, key: tuple[Any, ...], value: Iterable[K]) -> None:
        if key in self._buckets:
            self.__delitem__(key)
        for storage_key in value:
            self.add(key, storage_key)

    def __delitem__(self, key: tuple[Any, ...]) -> None:
        self._buckets = self._buckets.remove(key)
        self._bucket_ordinals = self._bucket_ordinals.remove(key)
        self._bucket_order_roots = self._bucket_order_roots.remove(key)

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self._buckets)

    def __len__(self) -> int:
        return len(self._buckets)

    def __repr__(self) -> str:
        return repr(dict(self.items()))

    def __eq__(self, other: object) -> bool:
        if not hasattr(other, "items"):
            return False
        return dict(self.items()) == dict(other.items())

    __hash__ = None

    def keys(self):
        return KeysView(self)

    def items(self):
        return ItemsView(self)

    def values(self):
        return ValuesView(self)

    def get(self, key: tuple[Any, ...], default: Any = None):
        return self._buckets.get(key, default)

    def clear(self) -> None:
        self._buckets = HashTrieMap()
        self._bucket_ordinals = HashTrieMap()
        self._bucket_order_roots = HashTrieMap()
        self._next_membership_ordinal = 0
        self._expiry_by_storage = HashTrieMap()
        self._expiry_root = None
        self._next_expiry_token = 0

    def pop(self, key: tuple[Any, ...], *default):
        if len(default) > 1:
            message = "pop expected at most two arguments"
            raise TypeError(message)
        value = self._buckets.get(key, _ABSENT)
        if value is _ABSENT:
            if default:
                return default[0]
            raise KeyError(key)
        self._buckets = self._buckets.remove(key)
        self._bucket_ordinals = self._bucket_ordinals.remove(key)
        self._bucket_order_roots = self._bucket_order_roots.remove(key)
        return value

    def setdefault(self, key: tuple[Any, ...], default=frozenset()):
        if key not in self:
            self[key] = default
        return self[key]

    def update(self, values=(), **kwargs) -> None:
        items = values.items() if hasattr(values, "items") else values
        for key, value in items:
            self[key] = value
        for key, value in kwargs.items():
            self[key] = value  # type: ignore[index]

    def copy(self) -> Self:
        result = type(self)()
        result._buckets = self._buckets
        result._bucket_ordinals = self._bucket_ordinals
        result._bucket_order_roots = self._bucket_order_roots
        result._next_membership_ordinal = self._next_membership_ordinal
        result._expiry_by_storage = self._expiry_by_storage
        result._expiry_root = self._expiry_root
        result._next_expiry_token = self._next_expiry_token
        return result

    def __copy__(self) -> Self:
        return self.copy()

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        result = type(self)()
        memo[id(self)] = result
        result._buckets = deepcopy(self._buckets, memo)
        result._bucket_ordinals = deepcopy(self._bucket_ordinals, memo)
        result._bucket_order_roots = deepcopy(self._bucket_order_roots, memo)
        result._next_membership_ordinal = self._next_membership_ordinal
        result._expiry_by_storage = deepcopy(self._expiry_by_storage, memo)
        result._expiry_root = deepcopy(self._expiry_root, memo)
        result._next_expiry_token = self._next_expiry_token
        return result

    def add(
        self,
        key: tuple[Any, ...],
        storage_key: K,
        *,
        ordinal: int | None = None,
    ) -> None:
        bucket = self._buckets.get(key, HashTrieSet())
        if storage_key in bucket:
            return
        effective_ordinal = (
            self._next_membership_ordinal if ordinal is None else ordinal
        )
        self._next_membership_ordinal = max(
            self._next_membership_ordinal,
            effective_ordinal + 1,
        )
        self._buckets = self._buckets.insert(key, bucket.insert(storage_key))
        ordinals = self._bucket_ordinals.get(key, HashTrieMap()).insert(
            storage_key,
            effective_ordinal,
        )
        self._bucket_ordinals = self._bucket_ordinals.insert(key, ordinals)
        root = self._bucket_order_roots.get(key)
        self._bucket_order_roots = self._bucket_order_roots.insert(
            key,
            _insert_membership(
                root,
                _MembershipNode(
                    effective_ordinal,
                    storage_key,
                    _expiry_priority(effective_ordinal),
                ),
            ),
        )

    def discard(self, key: tuple[Any, ...], storage_key: K) -> None:
        bucket = self._buckets.get(key)
        if bucket is None or storage_key not in bucket:
            return
        ordinals = self._bucket_ordinals[key]
        ordinal = ordinals[storage_key]
        remaining = bucket.remove(storage_key)
        if remaining:
            self._buckets = self._buckets.insert(key, remaining)
            self._bucket_ordinals = self._bucket_ordinals.insert(
                key,
                ordinals.remove(storage_key),
            )
            root = _remove_membership(
                self._bucket_order_roots.get(key),
                ordinal,
            )
            if root is None:
                message = "index membership order lost a non-empty bucket"
                raise RuntimeError(message)
            self._bucket_order_roots = self._bucket_order_roots.insert(
                key,
                root,
            )
        else:
            self._buckets = self._buckets.remove(key)
            self._bucket_ordinals = self._bucket_ordinals.remove(key)
            self._bucket_order_roots = self._bucket_order_roots.remove(key)

    def ordinal_for(self, key: tuple[Any, ...], storage_key: K) -> int | None:
        ordinals = self._bucket_ordinals.get(key)
        return None if ordinals is None else ordinals.get(storage_key)

    def ordered_storage_keys(
        self,
        key: tuple[Any, ...],
        *,
        limit: int | None = None,
    ) -> tuple[K, ...]:
        return tuple(_iter_membership_keys(self._bucket_order_roots.get(key), limit))

    def set_expiration(
        self,
        storage_key: K,
        expires_at: datetime.datetime | None,
    ) -> None:
        previous = self._expiry_by_storage.get(storage_key)
        if previous is not None:
            self._expiry_root = _remove_expiry(self._expiry_root, previous)
            self._expiry_by_storage = self._expiry_by_storage.remove(storage_key)
        if expires_at is None:
            return
        token = self._next_expiry_token
        self._next_expiry_token += 1
        key = (expires_at, token)
        self._expiry_root = _insert_expiry(
            self._expiry_root,
            _ExpiryNode(key, storage_key, _expiry_priority(token)),
        )
        self._expiry_by_storage = self._expiry_by_storage.insert(storage_key, key)

    def expired_storage_keys(self, now: datetime.datetime) -> tuple[K, ...]:
        return tuple(_iter_expired(self._expiry_root, now))

    @property
    def expiration_count(self) -> int:
        return len(self._expiry_by_storage)

    def _snapshot_state(self) -> _IndexState[K]:
        return (
            self._buckets,
            self._bucket_ordinals,
            self._bucket_order_roots,
            self._next_membership_ordinal,
            self._expiry_by_storage,
            self._expiry_root,
            self._next_expiry_token,
        )

    def _restore_state(self, state: _IndexState[K]) -> None:
        (
            self._buckets,
            self._bucket_ordinals,
            self._bucket_order_roots,
            self._next_membership_ordinal,
            self._expiry_by_storage,
            self._expiry_root,
            self._next_expiry_token,
        ) = state
