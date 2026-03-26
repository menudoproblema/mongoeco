from mongoeco.errors import InvalidOperation


class IndexCursor:
    """Cursor sync mínimo para list_indexes()."""

    def __init__(self, client, async_index_cursor):
        self._client = client
        self._async_index_cursor = async_index_cursor
        self._cache: list[dict[str, object]] | None = None
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot use index cursor after it has been closed")

    def _load(self) -> list[dict[str, object]]:
        self._ensure_open()
        if self._cache is None:
            self._cache = self._client._run(self._async_index_cursor.to_list())
        return list(self._cache)

    def __iter__(self):
        return iter(self._load())

    def to_list(self) -> list[dict[str, object]]:
        return self._load()

    def first(self) -> dict[str, object] | None:
        documents = self._load()
        return documents[0] if documents else None

    def rewind(self) -> "IndexCursor":
        self._ensure_open()
        self._async_index_cursor.rewind()
        self._cache = None
        return self

    def clone(self) -> "IndexCursor":
        return type(self)(self._client, self._async_index_cursor.clone())

    def close(self) -> None:
        self._closed = True
        self._async_index_cursor.close()
        self._cache = None

    @property
    def alive(self) -> bool:
        return not self._closed and self._async_index_cursor.alive
