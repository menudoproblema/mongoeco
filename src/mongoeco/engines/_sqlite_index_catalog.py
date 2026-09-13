"""Owned, read-only catalog generations; mutable copies only at value borders."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from threading import RLock
from typing import TYPE_CHECKING, Any, Self


if TYPE_CHECKING:
    from collections.abc import Iterable

    from mongoeco.types import EngineIndexRecord


def _immutable(*_args: object, **_kwargs: object) -> None:
    message = "index catalog snapshots are immutable"
    raise TypeError(message)


class _CatalogList[T](list[T]):
    __slots__ = ()

    __setitem__ = __delitem__ = __iadd__ = __imul__ = _immutable
    append = clear = extend = insert = pop = remove = reverse = sort = _immutable

    def __deepcopy__(self, memo: dict[int, object]) -> list[T]:
        # IndexDefinition's public materialization explicitly asks for owned
        # mutable values. Only the complete internal catalog is borrowed.
        result: list[T] = []
        memo[id(self)] = result
        result.extend(deepcopy(value, memo) for value in self)
        return result


class _CatalogDocument(dict[str, Any]):
    __slots__ = ()

    __setitem__ = __delitem__ = __ior__ = _immutable
    clear = pop = popitem = setdefault = update = _immutable

    def __deepcopy__(self, memo: dict[int, object]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        memo[id(self)] = result
        result.update((key, deepcopy(value, memo)) for key, value in self.items())
        return result


class _UnshareableValueError(Exception):
    pass


def _freeze(value: Any) -> Any:
    if type(value) in (str, int, float, bool, type(None), datetime, Decimal):
        return value
    if isinstance(value, dict):
        return _CatalogDocument((key, _freeze(item)) for key, item in value.items())
    if isinstance(value, list):
        return _CatalogList(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    # In particular Binary carries mutable subtype metadata. Opaque BSON
    # leaves must retain the defensive-copy path until they have a proven
    # immutable representation; do not admit them based on a shallow freeze.
    raise _UnshareableValueError


class SQLiteIndexCatalog(_CatalogList["EngineIndexRecord"]):
    __slots__ = ("_ttl_indexes",)
    __setattr__ = _immutable

    def __init__(self, indexes: Iterable[EngineIndexRecord]) -> None:
        super().__init__(
            replace(
                index,
                fields=_freeze(index.fields),
                key=_freeze(index.key),
                collation=_freeze(index.collation),
                partial_filter_expression=_freeze(index.partial_filter_expression),
                weights=_freeze(index.weights),
            )
            for index in indexes
        )
        object.__setattr__(
            self,
            "_ttl_indexes",
            tuple(index for index in self if index.expire_after_seconds is not None),
        )

    @property
    def ttl_indexes(self) -> tuple[EngineIndexRecord, ...]:
        return self._ttl_indexes

    def __deepcopy__(self, _memo: dict[int, object]) -> Self:
        return self

    @classmethod
    def from_records(
        cls, indexes: Iterable[EngineIndexRecord]
    ) -> SQLiteIndexCatalog | None:
        try:
            return cls(indexes)
        except _UnshareableValueError:
            return None


class SQLiteCatalogPool:
    """Share representation only after comparing metadata from the actual view.

    A data_version from another connection is not an equivalence proof. The
    complete persisted records are; retain only the latest observed content per
    namespace, while readers independently own any older catalog they still use.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._entries: dict[
            tuple[str, str], tuple[tuple[tuple[object, ...], ...], SQLiteIndexCatalog]
        ] = {}

    def find(
        self, namespace: tuple[str, str], rows: tuple[tuple[object, ...], ...]
    ) -> SQLiteIndexCatalog | None:
        with self._lock:
            cached = self._entries.get(namespace)
            if cached is not None and cached[0] == rows:
                return cached[1]
            return None

    def remember(
        self,
        namespace: tuple[str, str],
        rows: tuple[tuple[object, ...], ...],
        catalog: SQLiteIndexCatalog,
    ) -> None:
        with self._lock:
            self._entries[namespace] = rows, catalog

    def clear(self, namespace: tuple[str, str] | None = None) -> None:
        with self._lock:
            if namespace is None:
                self._entries.clear()
            else:
                self._entries.pop(namespace, None)
