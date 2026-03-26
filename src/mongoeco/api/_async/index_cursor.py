from collections.abc import AsyncIterator, Awaitable, Callable


class AsyncIndexCursor:
    """Cursor async mínimo para list_indexes()."""

    def __init__(self, loader: Callable[[], Awaitable[list[dict[str, object]]]]):
        self._loader = loader
        self._cache: list[dict[str, object]] | None = None
        self._started = False
        self._exhausted = False

    async def _materialize(self) -> list[dict[str, object]]:
        self._started = True
        if self._cache is None:
            self._cache = await self._loader()
            self._exhausted = True
        return list(self._cache)

    async def to_list(self) -> list[dict[str, object]]:
        return await self._materialize()

    async def first(self) -> dict[str, object] | None:
        documents = await self._materialize()
        return documents[0] if documents else None

    def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        async def _iterate() -> AsyncIterator[dict[str, object]]:
            for document in await self._materialize():
                yield document

        return _iterate()

