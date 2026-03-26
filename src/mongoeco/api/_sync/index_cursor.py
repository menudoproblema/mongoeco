from mongoeco.api._sync._materialized_cursor import MaterializedCursor
from mongoeco.types import IndexDocument


class IndexCursor(MaterializedCursor[IndexDocument]):
    """Cursor sync mínimo para list_indexes()."""

    def __init__(self, client, async_index_cursor):
        super().__init__(client, async_index_cursor)

    def clone(self) -> "IndexCursor":
        return type(self)(self._client, self._async_cursor.clone())
