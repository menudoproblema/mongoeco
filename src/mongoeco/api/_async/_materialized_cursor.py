from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Generic, TypeVar

from mongoeco.errors import InvalidOperation


T = TypeVar("T")


class AsyncMaterializedCursor(Generic[T]):
    """Base interna para cursores async respaldados por una lista materializada."""

    def __init__(self, loader: Callable[[], Awaitable[list[T]]]):
        self._loader = loader
        self._cache: list[T] | None = None
        self._started = False
        self._exhausted = False
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot use cursor after it has been closed")

    async def _materialize(self, *, exhaust: bool = True) -> list[T]:
        self._ensure_open()
        self._started = True
        if self._cache is None:
            self._cache = await self._loader()
        if exhaust:
            self._exhausted = True
        return list(self._cache)

    async def to_list(self) -> list[T]:
        return await self._materialize()

    async def first(self) -> T | None:
        documents = await self._materialize(exhaust=False)
        return documents[0] if documents else None

    def __aiter__(self) -> AsyncIterator[T]:
        async def _iterate() -> AsyncIterator[T]:
            for document in await self._materialize():
                yield document

        return _iterate()

    def rewind(self):
        self._ensure_open()
        self._started = False
        self._exhausted = False
        self._cache = None
        return self

    def close(self) -> None:
        self._closed = True
        self._cache = None
        self._exhausted = True

    @property
    def alive(self) -> bool:
        return not self._closed and not self._exhausted
