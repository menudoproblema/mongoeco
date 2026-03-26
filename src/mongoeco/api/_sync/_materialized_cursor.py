from typing import Generic, TypeVar

from mongoeco.errors import InvalidOperation


T = TypeVar("T")


class MaterializedCursor(Generic[T]):
    """Base interna para cursores sync respaldados por una lista materializada."""

    def __init__(self, client, async_cursor):
        self._client = client
        self._async_cursor = async_cursor
        self._cache: list[T] | None = None
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot use cursor after it has been closed")

    def _load(self) -> list[T]:
        self._ensure_open()
        if self._cache is None:
            self._cache = self._client._run(self._async_cursor.to_list())
        return list(self._cache)

    def __iter__(self):
        return iter(self._load())

    def to_list(self) -> list[T]:
        return self._load()

    def first(self) -> T | None:
        self._ensure_open()
        if self._cache is not None:
            return self._cache[0] if self._cache else None
        return self._client._run(self._async_cursor.first())

    def rewind(self):
        self._ensure_open()
        self._async_cursor.rewind()
        self._cache = None
        return self

    def close(self) -> None:
        self._closed = True
        self._async_cursor.close()
        self._cache = None

    @property
    def alive(self) -> bool:
        return not self._closed and self._async_cursor.alive
