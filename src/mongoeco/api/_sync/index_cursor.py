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

    def close(self) -> None:
        self._closed = True
        self._cache = None

