from mongoeco.api._sync._materialized_cursor import MaterializedCursor
from mongoeco.types import Document


class ListingCursor(MaterializedCursor[Document]):
    """Cursor sync mínimo para resultados administrativos materializados."""

    def __init__(self, client, async_listing_cursor):
        super().__init__(client, async_listing_cursor)

    def clone(self) -> "ListingCursor":
        return type(self)(self._client, self._async_cursor.clone())
