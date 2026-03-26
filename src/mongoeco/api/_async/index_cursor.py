from collections.abc import Awaitable, Callable

from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.types import IndexDocument


class AsyncIndexCursor(AsyncMaterializedCursor[IndexDocument]):
    """Cursor async mínimo para list_indexes()."""

    def __init__(self, loader: Callable[[], Awaitable[list[IndexDocument]]]):
        super().__init__(loader)

    def clone(self) -> "AsyncIndexCursor":
        return type(self)(self._loader)
