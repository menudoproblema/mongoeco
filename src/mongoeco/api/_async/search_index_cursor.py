from collections.abc import Awaitable, Callable

from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.types import SearchIndexDocument


class AsyncSearchIndexCursor(AsyncMaterializedCursor[SearchIndexDocument]):
    """Cursor async mínimo para list_search_indexes()."""

    def __init__(self, loader: Callable[[], Awaitable[list[SearchIndexDocument]]]):
        super().__init__(loader)

    def clone(self) -> "AsyncSearchIndexCursor":
        return type(self)(self._loader)
