from collections.abc import AsyncIterator, Awaitable, Callable

from mongoeco.errors import InvalidOperation
from mongoeco.types import IndexDocument


class AsyncIndexCursor:
    """Cursor async mínimo para list_indexes()."""

    def __init__(self, loader: Callable[[], Awaitable[list[IndexDocument]]]):
        self._loader = loader
        self._cache: list[IndexDocument] | None = None
        self._started = False
        self._exhausted = False
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot use index cursor after it has been closed")

    async def _materialize(self, *, exhaust: bool = True) -> list[IndexDocument]:
        self._ensure_open()
        self._started = True
        if self._cache is None:
            self._cache = await self._loader()
        if exhaust:
            self._exhausted = True
        return list(self._cache)

    async def to_list(self) -> list[IndexDocument]:
        return await self._materialize()

    async def first(self) -> IndexDocument | None:
        documents = await self._materialize(exhaust=False)
        return documents[0] if documents else None

    def __aiter__(self) -> AsyncIterator[IndexDocument]:
        async def _iterate() -> AsyncIterator[IndexDocument]:
            for document in await self._materialize():
                yield document

        return _iterate()

    def rewind(self) -> "AsyncIndexCursor":
        self._ensure_open()
        self._started = False
        self._exhausted = False
        self._cache = None
        return self

    def clone(self) -> "AsyncIndexCursor":
        return type(self)(self._loader)

    def close(self) -> None:
        self._closed = True
        self._cache = None
        self._exhausted = True

    @property
    def alive(self) -> bool:
        return not self._closed and not self._exhausted
