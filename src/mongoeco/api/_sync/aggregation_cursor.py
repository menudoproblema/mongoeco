from mongoeco.errors import InvalidOperation
from mongoeco.types import Document


class _AggregationCursorIterator:
    def __init__(self, cursor: "AggregationCursor", async_iterable):
        self._cursor = cursor
        self._async_iterable = async_iterable
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration
        if self._cursor._active_async_iterable is not self._async_iterable:
            self._closed = True
            raise StopIteration
        try:
            return self._cursor._client._run(self._async_iterable.__anext__())
        except StopAsyncIteration:
            self._cursor._exhausted = True
            self.close()
            raise StopIteration

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._cursor._close_active_iterator(self._async_iterable)

    def __del__(self):
        try:
            self.close()
        except Exception:
            self._closed = True


class AggregationCursor:
    """Cursor sync mínimo para resultados de aggregate()."""

    def __init__(self, client, async_aggregation_cursor):
        self._client = client
        self._async_aggregation_cursor = async_aggregation_cursor
        self._cache: list[Document] | None = None
        self._started = False
        self._closed = False
        self._active_async_iterable = None
        self._exhausted = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot use aggregation cursor after it has been closed")

    def _load(self) -> list[Document]:
        self._ensure_open()
        self._started = True
        if self._exhausted and self._cache is None:
            return []
        if self._cache is None:
            self._cache = self._client._run(self._async_aggregation_cursor.to_list())
        return self._cache

    def _close_active_iterator(self, async_iterable) -> None:
        if self._active_async_iterable is not async_iterable:
            return
        close = getattr(async_iterable, "aclose", None)
        try:
            if callable(close):
                self._client._run(close())
        finally:
            if self._active_async_iterable is async_iterable:
                self._active_async_iterable = None

    def __iter__(self):
        self._ensure_open()
        if self._cache is not None:
            return iter(self._cache)
        if self._exhausted:
            return iter(())

        self._started = True
        async_iterable = self._active_async_iterable
        if async_iterable is None:
            async_iterable = self._async_aggregation_cursor.__aiter__()
            self._active_async_iterable = async_iterable

        return _AggregationCursorIterator(self, async_iterable)

    def to_list(self) -> list[Document]:
        return list(self._load())

    def first(self) -> Document | None:
        self._ensure_open()
        if self._cache is not None:
            return self._cache[0] if self._cache else None
        if self._exhausted:
            return None
        active = self._active_async_iterable
        if active is not None:
            try:
                return self._client._run(active.__anext__())
            except StopAsyncIteration:
                self._exhausted = True
                self._close_active_iterator(active)
            return None
        return self._client._run(self._async_aggregation_cursor.first())

    def explain(self) -> dict[str, object]:
        self._ensure_open()
        return self._client._run(self._async_aggregation_cursor.explain())

    def close(self) -> None:
        if self._closed:
            return
        try:
            active = self._active_async_iterable
            if active is not None:
                self._close_active_iterator(active)
        finally:
            self._active_async_iterable = None
            self._cache = None
            self._closed = True

    def __del__(self):
        try:
            self.close()
        except Exception:
            self._closed = True
