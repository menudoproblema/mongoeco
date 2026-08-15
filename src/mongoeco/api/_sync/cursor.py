from mongoeco.api._async.cursor import (
    _DEFAULT_LOCAL_PREFETCH_SIZE,
    _resolve_planning_mode,
)
from mongoeco.api._sync._finalization import finalize_best_effort
from mongoeco.api.argument_validation import HintSpec
from mongoeco.api.operations import FindOperation
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.types import (
    CollationDocument,
    Document,
    Filter,
    Projection,
    SortSpec,
)


_CURSOR_UNSET = object()


class _CursorIterator:
    def __init__(self, cursor: "Cursor", async_iterable):
        self._cursor = cursor
        self._async_iterable = async_iterable
        self._closed = False

    def _pull_chunk(self) -> bool:
        if self._cursor._sync_buffer_index < len(self._cursor._sync_buffer):
            return True
        pull_chunk = getattr(self._async_iterable, "pull_chunk", None)
        if not callable(pull_chunk):
            return False
        batch_size = self._cursor._batch_size or _DEFAULT_LOCAL_PREFETCH_SIZE
        self._cursor._sync_buffer = self._cursor._client._run(pull_chunk(batch_size))
        self._cursor._sync_buffer_index = 0
        return self._cursor._sync_buffer_index < len(self._cursor._sync_buffer)

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration
        if self._cursor._active_async_iterable is not self._async_iterable:
            self._closed = True
            raise StopIteration
        if self._pull_chunk():
            value = self._cursor._sync_buffer[self._cursor._sync_buffer_index]
            self._cursor._sync_buffer_index += 1
            if self._cursor._sync_buffer_index >= len(self._cursor._sync_buffer):
                self._cursor._sync_buffer = []
                self._cursor._sync_buffer_index = 0
            return value
        try:
            return self._cursor._client._run(self._async_iterable.__anext__())
        except StopAsyncIteration:
            self._cursor._exhausted = True
            self.close()
            raise StopIteration
        except Exception:
            try:
                self.close()
            except Exception:
                pass
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._cursor._close_active_iterator(self._async_iterable)

    def __del__(self):
        if not finalize_best_effort(
            getattr(self._cursor, "_client", None),
            self.close,
        ):
            self._closed = True


class Cursor:
    """Cursor sync mínimo sobre la API async."""

    def __init__(
        self,
        client,
        async_cursor_or_collection,
        filter_spec: Filter | object = _CURSOR_UNSET,
        projection: Projection | None = None,
        *,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ):
        self._client = client
        if filter_spec is _CURSOR_UNSET:
            self._async_cursor = async_cursor_or_collection
            self._async_collection = self._async_cursor.collection
            self._sync_state_from_async_cursor()
        else:
            self._async_collection = async_cursor_or_collection
            self._filter_spec = filter_spec
            self._projection = projection
            self._collation = collation
            self._sort = sort
            self._skip = skip
            self._limit = limit
            self._hint = hint
            self._comment = comment
            self._max_time_ms = max_time_ms
            self._batch_size = batch_size
            self._let = let
            self._session = session
            self._async_cursor = self._async_collection.find(
                filter_spec,
                projection,
                collation=collation,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                batch_size=batch_size,
                let=let,
                session=session,
            )
            self._sync_state_from_async_cursor(use_existing_defaults=True)
        self._cache: list[Document] | None = None
        self._started = False
        self._closed = False
        self._active_async_iterable = None
        self._exhausted = False
        self._sync_buffer: list[Document] = []
        self._sync_buffer_index = 0

    def _sync_state_from_async_cursor(
        self,
        *,
        use_existing_defaults: bool = False,
    ) -> None:
        for name in (
            'filter_spec',
            'projection',
            'collation',
            'sort',
            'skip',
            'limit',
            'hint',
            'comment',
            'max_time_ms',
            'batch_size',
            'let',
            'session',
        ):
            attribute = f'_{name}'
            if hasattr(self._async_cursor, attribute):
                value = getattr(self._async_cursor, attribute)
                setattr(self, attribute, value)
            elif not use_existing_defaults:
                message = (
                    'async cursor is missing required state: '
                    f'{attribute}'
                )
                raise TypeError(message)

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot use cursor after it has been closed")

    def _invalidate(self) -> None:
        self._cache = None
        self._exhausted = False
        self._sync_buffer = []
        self._sync_buffer_index = 0

    def _ensure_mutable(self) -> None:
        self._ensure_open()
        if self._started:
            raise InvalidOperation("cannot modify cursor after iteration has started")

    def sort(
        self,
        key_or_list: SortSpec | str,
        direction: int | None = None,
    ) -> "Cursor":
        self._ensure_mutable()
        self._async_cursor.sort(key_or_list, direction)
        self._sync_state_from_async_cursor()
        self._invalidate()
        return self

    def hint(self, hint: HintSpec) -> "Cursor":
        self._ensure_mutable()
        self._async_cursor.hint(hint)
        self._sync_state_from_async_cursor()
        self._invalidate()
        return self

    def comment(self, comment: object) -> "Cursor":
        self._ensure_mutable()
        self._async_cursor.comment(comment)
        self._sync_state_from_async_cursor()
        self._invalidate()
        return self

    def max_time_ms(self, max_time_ms: int) -> "Cursor":
        self._ensure_mutable()
        self._async_cursor.max_time_ms(max_time_ms)
        self._sync_state_from_async_cursor()
        self._invalidate()
        return self

    def batch_size(self, batch_size: int) -> "Cursor":
        self._ensure_mutable()
        self._async_cursor.batch_size(batch_size)
        self._sync_state_from_async_cursor()
        self._invalidate()
        return self

    def skip(self, skip: int) -> "Cursor":
        self._ensure_mutable()
        self._async_cursor.skip(skip)
        self._sync_state_from_async_cursor()
        self._invalidate()
        return self

    def limit(self, limit: int | None) -> "Cursor":
        self._ensure_mutable()
        self._async_cursor.limit(limit)
        self._sync_state_from_async_cursor()
        self._invalidate()
        return self

    def _load(self) -> list[Document]:
        self._ensure_open()
        self._started = True
        if self._exhausted and self._cache is None:
            return []
        if self._cache is None:
            if self._limit == 1:
                first = self.first()
                self._cache = [] if first is None else [first]
                self._exhausted = True
                return self._cache
            self._cache = self._client._run(self._async_cursor.to_list())
            self._exhausted = True
        return self._cache

    def _close_active_iterator(self, async_iterable) -> None:
        if self._active_async_iterable is not async_iterable:
            return
        close = getattr(async_iterable, "aclose", None)
        try:
            if callable(close):
                self._client._run(close())
        finally:
            self._sync_buffer = []
            self._sync_buffer_index = 0
            if self._active_async_iterable is async_iterable:
                self._active_async_iterable = None

    def __iter__(self):
        self._ensure_open()
        if self._cache is not None:
            return iter(self._cache)
        if self._exhausted:
            return iter(())
        if self._limit == 1 and self._active_async_iterable is None:
            return iter(self._load())

        self._started = True
        async_iterable = self._active_async_iterable
        if async_iterable is None:
            async_iterable = self._async_cursor.__aiter__()
            self._active_async_iterable = async_iterable

        return _CursorIterator(self, async_iterable)

    def to_list(self, length: int | None = None) -> list[Document]:
        self._ensure_open()
        if length is not None and length < 0:
            raise ValueError("length must be non-negative or None")
        if length == 0:
            return []
        if (
            length is None
            and not self._started
            and self._active_async_iterable is None
        ):
            return list(self._load())

        documents: list[Document] = []
        while length is None or len(documents) < length:
            document = self._next_for_to_list()
            if document is None:
                break
            documents.append(document)
        return documents

    def _next_for_to_list(self) -> Document | None:
        if self._exhausted:
            return None
        self._started = True
        active = self._active_async_iterable
        if active is None:
            active = self._async_cursor.__aiter__()
            self._active_async_iterable = active
        try:
            return self._client._run(active.__anext__())
        except StopAsyncIteration:
            self._exhausted = True
            self._close_active_iterator(active)
            return None

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
            except Exception:
                try:
                    self._close_active_iterator(active)
                except Exception:
                    pass
                raise
        self._started = True
        return self._client._run(self._async_cursor.first())

    def rewind(self) -> "Cursor":
        self._ensure_open()
        active = self._active_async_iterable
        if active is not None:
            self._close_active_iterator(active)
        rewind = getattr(self._async_cursor, 'rewind', None)
        if callable(rewind):
            rewind()
        self._started = False
        self._exhausted = False
        self._cache = None
        return self

    def clone(self) -> "Cursor":
        return type(self)(self._client, self._async_cursor.clone())

    @property
    def alive(self) -> bool:
        return not self._closed and not self._exhausted

    @property
    def collection(self):
        from mongoeco.api._sync.collection import Collection

        return Collection(
            self._client,
            self._async_collection._db_name,
            self._async_collection._collection_name,
            write_concern=self._async_collection.write_concern,
            read_concern=self._async_collection.read_concern,
            read_preference=self._async_collection.read_preference,
            codec_options=self._async_collection.codec_options,
            planning_mode=_resolve_planning_mode(self._async_collection),
        )

    def _as_operation(self) -> FindOperation:
        return self._async_cursor._as_operation()

    def explain(self) -> dict[str, object]:
        self._ensure_open()
        return self._client._run(self._async_cursor.explain())

    def close(self) -> None:
        if self._closed:
            return
        try:
            active = self._active_async_iterable
            if active is not None:
                close = getattr(active, "aclose", None)
                if callable(close):
                    self._client._run(close())
            close_cursor = getattr(self._async_cursor, 'close', None)
            if callable(close_cursor):
                awaitable = close_cursor()
                try:
                    self._client._run(awaitable)
                except BaseException:
                    close_awaitable = getattr(awaitable, 'close', None)
                    if callable(close_awaitable):
                        close_awaitable()
                    raise
        finally:
            self._active_async_iterable = None
            self._cache = None
            self._closed = True

    def __del__(self):
        finalized = finalize_best_effort(
            getattr(self, "_client", None),
            self.close,
        )
        if not finalized:
            self._closed = True
