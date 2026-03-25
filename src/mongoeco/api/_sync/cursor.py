from mongoeco.api._async.cursor import _validate_sort_spec
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.types import Document, Filter, Projection, SortSpec


class _CursorIterator:
    def __init__(self, cursor: "Cursor", async_iterable):
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


class Cursor:
    """Cursor sync mínimo sobre la API async."""

    def __init__(
        self,
        client,
        async_collection,
        filter_spec: Filter,
        projection: Projection | None,
        *,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        session: ClientSession | None = None,
    ):
        self._client = client
        self._async_collection = async_collection
        self._filter_spec = filter_spec
        self._projection = projection
        self._sort = sort
        self._skip = skip
        self._limit = limit
        self._session = session
        self._cache: list[Document] | None = None
        self._started = False
        self._closed = False
        self._active_async_iterable = None
        self._exhausted = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot use cursor after it has been closed")

    def _invalidate(self) -> None:
        self._cache = None

    def _ensure_mutable(self) -> None:
        self._ensure_open()
        if self._started:
            raise InvalidOperation("cannot modify cursor after iteration has started")

    def sort(self, sort: SortSpec) -> "Cursor":
        self._ensure_mutable()
        _validate_sort_spec(sort)
        self._sort = sort
        self._invalidate()
        return self

    def skip(self, skip: int) -> "Cursor":
        self._ensure_mutable()
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self._skip = skip
        self._invalidate()
        return self

    def limit(self, limit: int | None) -> "Cursor":
        self._ensure_mutable()
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        self._limit = limit
        self._invalidate()
        return self

    def _load(self) -> list[Document]:
        self._ensure_open()
        self._started = True
        if self._exhausted and self._cache is None:
            return []
        if self._cache is None:
            self._cache = self._client._run(
                self._async_collection.find(
                    self._filter_spec,
                    self._projection,
                    sort=self._sort,
                    skip=self._skip,
                    limit=self._limit,
                    session=self._session,
                ).to_list()
            )
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
            async_iterable = self._async_collection.find(
                self._filter_spec,
                self._projection,
                sort=self._sort,
                skip=self._skip,
                limit=self._limit,
                session=self._session,
            ).__aiter__()
            self._active_async_iterable = async_iterable

        return _CursorIterator(self, async_iterable)

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
        self._started = True
        return self._client._run(
            self._async_collection.find(
                self._filter_spec,
                self._projection,
                sort=self._sort,
                skip=self._skip,
                limit=self._limit,
                session=self._session,
            ).first()
        )

    def close(self) -> None:
        if self._closed:
            return
        try:
            active = self._active_async_iterable
            if active is not None:
                close = getattr(active, "aclose", None)
                if callable(close):
                    self._client._run(close())
        finally:
            self._active_async_iterable = None
            self._cache = None
            self._closed = True

    def __del__(self):
        try:
            self.close()
        except Exception:
            self._closed = True
