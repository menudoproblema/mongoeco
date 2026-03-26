from collections.abc import Awaitable, Callable

from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.types import Document


class AsyncListingCursor(AsyncMaterializedCursor[Document]):
    """Cursor async mínimo para resultados administrativos materializados."""

    def __init__(self, loader: Callable[[], Awaitable[list[Document]]]):
        super().__init__(loader)

    def clone(self) -> "AsyncListingCursor":
        return type(self)(self._loader)
