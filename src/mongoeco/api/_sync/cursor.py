from mongoeco.api._async.cursor import (
    HintSpec,
    MONGODB_DIALECT_70,
    _resolve_planning_mode,
    _serialize_explanation,
    _validate_batch_size,
    _validate_hint_spec,
    _validate_max_time_ms,
    _validate_sort_spec,
)
from mongoeco.api.operations import FindOperation, compile_find_operation
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.types import CollationDocument, Document, Filter, Projection, QueryPlanExplanation, SortSpec


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
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        session: ClientSession | None = None,
    ):
        self._client = client
        self._async_collection = async_collection
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
        self._exhausted = False

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

    def hint(self, hint: HintSpec) -> "Cursor":
        self._ensure_mutable()
        _validate_hint_spec(hint)
        self._hint = hint
        self._invalidate()
        return self

    def comment(self, comment: object) -> "Cursor":
        self._ensure_mutable()
        self._comment = comment
        self._invalidate()
        return self

    def max_time_ms(self, max_time_ms: int) -> "Cursor":
        self._ensure_mutable()
        _validate_max_time_ms(max_time_ms)
        self._max_time_ms = max_time_ms
        self._invalidate()
        return self

    def batch_size(self, batch_size: int) -> "Cursor":
        self._ensure_mutable()
        _validate_batch_size(batch_size)
        self._batch_size = batch_size
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
                    collation=self._collation,
                    sort=self._sort,
                    skip=self._skip,
                    limit=self._limit,
                    hint=self._hint,
                    comment=self._comment,
                    max_time_ms=self._max_time_ms,
                    batch_size=self._batch_size,
                    session=self._session,
                ).to_list()
            )
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
                collation=self._collation,
                sort=self._sort,
                skip=self._skip,
                limit=self._limit,
                hint=self._hint,
                comment=self._comment,
                max_time_ms=self._max_time_ms,
                batch_size=self._batch_size,
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
                collation=self._collation,
                sort=self._sort,
                skip=self._skip,
                limit=self._limit,
                hint=self._hint,
                comment=self._comment,
                max_time_ms=self._max_time_ms,
                batch_size=self._batch_size,
                session=self._session,
            ).first()
        )

    def rewind(self) -> "Cursor":
        self._ensure_open()
        active = self._active_async_iterable
        if active is not None:
            self._close_active_iterator(active)
        self._started = False
        self._exhausted = False
        self._cache = None
        return self

    def clone(self) -> "Cursor":
        return type(self)(
            self._client,
            self._async_collection,
            self._filter_spec,
            self._projection,
            collation=self._collation,
            sort=self._sort,
            skip=self._skip,
            limit=self._limit,
            hint=self._hint,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            batch_size=self._batch_size,
            session=self._session,
        )

    @property
    def alive(self) -> bool:
        return not self._closed and not self._exhausted

    def _as_operation(self) -> FindOperation:
        return compile_find_operation(
            self._filter_spec,
            projection=self._projection,
            collation=self._collation,
            sort=self._sort,
            skip=self._skip,
            limit=self._limit,
            hint=self._hint,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            batch_size=self._batch_size,
            dialect=getattr(self._async_collection, "mongodb_dialect", MONGODB_DIALECT_70),
            planning_mode=_resolve_planning_mode(self._async_collection),
        )

    def explain(self) -> dict[str, object]:
        self._ensure_open()
        operation = self._as_operation()
        if operation.planning_issues:
            return QueryPlanExplanation(
                engine="planner",
                strategy="deferred",
                plan="planning-issues",
                sort=operation.sort,
                skip=operation.skip,
                limit=operation.limit,
                hint=operation.hint,
                hinted_index=None,
                comment=operation.comment,
                max_time_ms=operation.max_time_ms,
                details={"reason": "execution blocked by deferred planning issues"},
                planning_mode=operation.planning_mode,
                planning_issues=operation.planning_issues,
            ).to_document()
        dialect = getattr(self._async_collection, "mongodb_dialect", None)
        from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

        semantics = compile_find_semantics_from_operation(operation, dialect=dialect)
        return _serialize_explanation(
            self._client._run(
                self._async_collection._engine.explain_find_semantics(
                    self._async_collection._db_name,
                    self._async_collection._collection_name,
                    semantics,
                    context=self._session,
                )
            )
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
