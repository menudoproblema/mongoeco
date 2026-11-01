from mongoeco.api._sync._finalization import finalize_best_effort
from mongoeco.errors import InvalidOperation
from mongoeco.types import Document


class _AggregationCursorIterator:
    def __init__(self, cursor: "AggregationCursor", async_iterable):
        self._cursor = cursor
        self._async_iterable = async_iterable
        self._closed = False

    def _pull_chunk(self) -> bool:
        if self._cursor._sync_buffer_index < len(self._cursor._sync_buffer):
            return True
        if self._cursor._exhausted:
            self.close()
            return False

        read_response = getattr(self._async_iterable, "_read_driver_response", None)
        if callable(read_response):
            self._cursor._sync_buffer = self._cursor._client._run(read_response())
            self._cursor._sync_buffer_index = 0
            if not self._cursor._sync_buffer:
                self._cursor._exhausted = True
                self.close()
                return False
            return True

        batch_size = self._cursor._batch_size or 101

        async def _pull() -> list[Document]:
            items: list[Document] = []
            while len(items) < batch_size:
                try:
                    items.append(await self._async_iterable.__anext__())
                except StopAsyncIteration:
                    self._cursor._exhausted = True
                    break
            return items

        self._cursor._sync_buffer = self._cursor._client._run(_pull())
        self._cursor._sync_buffer_index = 0
        if not self._cursor._sync_buffer:
            self.close()
            return False
        return True

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration
        if self._cursor._active_async_iterable is not self._async_iterable:
            self._closed = True
            raise StopIteration
        while True:
            try:
                has_chunk = self._pull_chunk()
            except Exception:
                try:
                    self.close()
                except Exception:
                    pass
                raise
            if not has_chunk:
                self._cursor._exhausted = True
                raise StopIteration
            value = self._cursor._sync_buffer[self._cursor._sync_buffer_index]
            self._cursor._sync_buffer_index += 1
            if self._cursor._sync_buffer_index >= len(
                self._cursor._sync_buffer,
            ):
                self._cursor._sync_buffer = []
                self._cursor._sync_buffer_index = 0
            if value is not None:
                return value

    def close(self) -> None:
        if self._closed:
            return
        self._cursor._close_active_iterator(self._async_iterable)
        self._closed = True


class AggregationCursor:
    """Cursor sync mínimo para resultados de aggregate()."""

    def __init__(self, client, async_aggregation_cursor):
        self._client = client
        self._async_aggregation_cursor = async_aggregation_cursor
        self._started = False
        self._closed = False
        self._active_async_iterable = None
        self._exhausted = False
        self._batch_size = getattr(
            async_aggregation_cursor,
            "_batch_size",
            None,
        )
        self._sync_buffer: list[Document] = []
        self._sync_buffer_index = 0

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation(
                "cannot use aggregation cursor after it has been closed",
            )

    def _close_active_iterator(self, async_iterable) -> None:
        if self._active_async_iterable is not async_iterable:
            return
        # The real command cursor is its own iterator and exposes close.
        # Its owner must also close an initial response before sync iteration.
        close = getattr(self._async_aggregation_cursor, "close", None)
        if not callable(close):
            fallback = (
                self._async_aggregation_cursor
                if async_iterable is None
                else async_iterable
            )
            close = getattr(fallback, "aclose", None)
        try:
            if callable(close):
                self._client._run(close())
        finally:
            if self._active_async_iterable is async_iterable:
                self._active_async_iterable = None
                self._sync_buffer = []
                self._sync_buffer_index = 0

    def __iter__(self):
        self._ensure_open()
        if self._active_async_iterable is not None and self._sync_buffer_index < len(
            self._sync_buffer,
        ):
            return _AggregationCursorIterator(
                self,
                self._active_async_iterable,
            )
        if self._exhausted:
            return iter(())

        self._started = True
        async_iterable = self._active_async_iterable
        if async_iterable is None:
            async_iterable = self._async_aggregation_cursor.__aiter__()
            self._active_async_iterable = async_iterable

        return _AggregationCursorIterator(self, async_iterable)

    def to_list(self, length: int | None = None) -> list[Document]:
        self._ensure_open()
        if length is not None and length < 0:
            raise ValueError("length must be non-negative or None")
        if length == 0:
            return []
        self._started = True
        documents: list[Document] = []
        available = self._sync_buffer[self._sync_buffer_index :]
        count = len(available) if length is None else min(length, len(available))
        documents.extend(available[:count])
        self._sync_buffer_index += count
        if self._sync_buffer_index == len(self._sync_buffer):
            self._sync_buffer = []
            self._sync_buffer_index = 0
        if not self._exhausted and (length is None or len(documents) < length):
            remaining = None if length is None else length - len(documents)
            documents.extend(
                self._client._run(self._async_aggregation_cursor.to_list(remaining))
            )
            self._exhausted = getattr(
                self._async_aggregation_cursor, "_closed", False
            ) and not getattr(self._async_aggregation_cursor, "_response_buffer", ())
        return documents

    def first(self) -> Document | None:
        self._ensure_open()
        if self._sync_buffer_index < len(self._sync_buffer):
            value = self._sync_buffer[self._sync_buffer_index]
            self._sync_buffer_index += 1
            if self._sync_buffer_index >= len(self._sync_buffer):
                self._sync_buffer = []
                self._sync_buffer_index = 0
            return value
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
            except Exception:
                try:
                    self._close_active_iterator(active)
                except Exception:
                    pass
                raise
        return self._client._run(self._async_aggregation_cursor.first())

    def explain(self, verbosity: str = "executionStats") -> dict[str, object]:
        self._ensure_open()
        if verbosity == "executionStats":
            return self._client._run(self._async_aggregation_cursor.explain())
        return self._client._run(
            self._async_aggregation_cursor.explain(verbosity),
        )

    def close(self) -> None:
        if self._closed:
            return
        self._close_active_iterator(self._active_async_iterable)
        self._closed = True

    def __del__(self):
        finalized = finalize_best_effort(
            getattr(self, "_client", None),
            self.close,
        )
        if not finalized:
            self._closed = True
