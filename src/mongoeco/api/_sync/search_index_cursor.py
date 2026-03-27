from mongoeco.api._sync._materialized_cursor import MaterializedCursor
from mongoeco.types import SearchIndexDocument


class SearchIndexCursor(MaterializedCursor[SearchIndexDocument]):
    """Cursor sync mínimo para list_search_indexes()."""

    def __init__(self, client, async_search_index_cursor):
        super().__init__(client, async_search_index_cursor)

    def clone(self) -> "SearchIndexCursor":
        return type(self)(self._client, self._async_cursor.clone())
