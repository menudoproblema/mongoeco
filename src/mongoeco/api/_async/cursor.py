from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.query_plan import QueryNode
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import Document, Filter, PlanningMode, Projection, QueryPlanExplanation, SortSpec


type HintSpec = str | SortSpec


def _serialize_explanation(result: object) -> dict[str, object]:
    to_document = getattr(result, "to_document", None)
    if callable(to_document):
        return to_document()
    if isinstance(result, dict):
        return result
    raise TypeError(f"Unsupported explain result type: {type(result)!r}")


def _operation_issue_message(operation) -> str:
    messages = ", ".join(issue.message for issue in operation.planning_issues)
    return f"operation has deferred planning issues: {messages}"


def _ensure_operation_executable(collection, operation) -> None:
    checker = getattr(collection, "_ensure_operation_executable", None)
    if callable(checker):
        checker(operation)
        return
    if getattr(operation, "planning_issues", ()):
        raise OperationFailure(_operation_issue_message(operation))


class _AsyncCursorIterator:
    def __init__(self, cursor: "AsyncCursor", *, batch_size: int | None, enforce_ownership: bool):
        self._cursor = cursor
        self._batch_size = batch_size
        self._enforce_ownership = enforce_ownership
        self._buffer: list[Document] = []
        self._closed = False
        self._position = 0

    def __aiter__(self):
        return self

    async def __anext__(self) -> Document:
        if self._closed:
            raise StopAsyncIteration
        if self._enforce_ownership and self._cursor._active_async_iterable is not self:
            self._closed = True
            raise StopAsyncIteration
        if not self._buffer:
            await self._fill_buffer()
        if not self._buffer:
            await self.close()
            raise StopAsyncIteration
        return self._buffer.pop(0)

    async def _fill_buffer(self) -> None:
        target_size = self._batch_size if self._batch_size not in (None, 0) else 1
        page = await self._cursor._fetch_batch(self._position, target_size)
        if not page:
            self._cursor._exhausted = True
            return
        self._position += len(page)
        self._buffer.extend(page)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._cursor._active_async_iterable is self:
            self._cursor._active_async_iterable = None
        if self._cursor._active_async_iterable is None and self._cursor._exhausted:
            self._cursor._started = True

    async def aclose(self) -> None:
        await self.close()


def _resolve_planning_mode(collection) -> object:
    planning_mode = getattr(collection, "planning_mode", None)
    if planning_mode is not None:
        return planning_mode
    return getattr(collection, "_planning_mode", None) or PlanningMode.STRICT


def _validate_sort_spec(sort: SortSpec) -> None:
    if not isinstance(sort, list):
        raise TypeError("sort must be a list of (field, direction) tuples")
    for item in sort:
        if not isinstance(item, tuple) or len(item) != 2:
            raise TypeError("sort must be a list of (field, direction) tuples")
        field, direction = item
        if not isinstance(field, str):
            raise TypeError("sort fields must be strings")
        if direction not in (1, -1) or isinstance(direction, bool):
            raise ValueError("sort directions must be 1 or -1")


def _validate_hint_spec(hint: HintSpec) -> None:
    if isinstance(hint, str):
        if not hint:
            raise ValueError("hint string must not be empty")
        return
    _validate_sort_spec(hint)


def _validate_batch_size(batch_size: int) -> None:
    if not isinstance(batch_size, int) or isinstance(batch_size, bool):
        raise TypeError("batch_size must be an integer")
    if batch_size < 0:
        raise ValueError("batch_size must be >= 0")


def _validate_max_time_ms(max_time_ms: int) -> None:
    if not isinstance(max_time_ms, int) or isinstance(max_time_ms, bool):
        raise TypeError("max_time_ms must be an integer")
    if max_time_ms < 0:
        raise ValueError("max_time_ms must be >= 0")


class AsyncCursor:
    """Cursor async mínimo y explícito sobre una colección."""

    def __init__(
        self,
        collection,
        filter_spec: Filter,
        plan: QueryNode,
        projection: Projection | None,
        *,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        session: ClientSession | None = None,
    ):
        self._collection = collection
        self._filter_spec = filter_spec
        self._plan = plan
        self._projection = projection
        self._sort = sort
        self._skip = skip
        self._limit = limit
        self._hint = hint
        self._comment = comment
        self._max_time_ms = max_time_ms
        self._batch_size = batch_size
        self._session = session
        self._started = False
        self._exhausted = False
        self._active_async_iterable: _AsyncCursorIterator | None = None

    def _ensure_mutable(self) -> None:
        if self._started:
            raise InvalidOperation("cannot modify cursor after iteration has started")

    def _scan(self, *, limit: int | None = None):
        self._started = True
        engine = self._collection._engine
        operation = self.clone().limit(self._limit if limit is None else limit)._as_operation()
        _ensure_operation_executable(self._collection, operation)
        scan_find_operation = getattr(engine, "scan_find_operation", None)
        if callable(scan_find_operation):
            return scan_find_operation(
                self._collection._db_name,
                self._collection._collection_name,
                operation,
                dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
                context=self._session,
            )
        return engine.scan_collection(
            self._collection._db_name,
            self._collection._collection_name,
            operation.filter_spec,
            plan=operation.plan,
            projection=operation.projection,
            sort=operation.sort,
            skip=operation.skip,
            limit=operation.limit,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
            context=self._session,
        )

    async def _fetch_batch(self, offset: int, batch_size: int) -> list[Document]:
        effective_skip = self._skip + offset
        effective_limit = batch_size
        if self._limit is not None:
            remaining = self._limit - offset
            if remaining <= 0:
                return []
            effective_limit = min(batch_size, remaining)
        operation = self.clone().skip(effective_skip).limit(effective_limit)._as_operation()
        _ensure_operation_executable(self._collection, operation)
        engine = self._collection._engine
        scan_find_operation = getattr(engine, "scan_find_operation", None)
        if callable(scan_find_operation):
            iterable = scan_find_operation(
                self._collection._db_name,
                self._collection._collection_name,
                operation,
                dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
                context=self._session,
            )
        else:
            iterable = engine.scan_collection(
                self._collection._db_name,
                self._collection._collection_name,
                operation.filter_spec,
                plan=operation.plan,
                projection=operation.projection,
                sort=operation.sort,
                skip=operation.skip,
                limit=operation.limit,
                hint=operation.hint,
                comment=operation.comment,
                max_time_ms=operation.max_time_ms,
                dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
                context=self._session,
            )
        return [document async for document in iterable]

    def _iter(self, *, limit: int | None = None, enforce_ownership: bool = True) -> _AsyncCursorIterator:
        self._started = True
        batch_size = self._batch_size if limit is None else limit
        return _AsyncCursorIterator(self, batch_size=batch_size, enforce_ownership=enforce_ownership)

    def __aiter__(self):
        if self._exhausted and self._active_async_iterable is None:
            async def _empty():
                if False:
                    yield None

            return _empty()
        if self._active_async_iterable is None:
            self._active_async_iterable = self._iter()
        return self._active_async_iterable

    def sort(self, sort: SortSpec) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_sort_spec(sort)
        self._sort = sort
        return self

    def hint(self, hint: HintSpec) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_hint_spec(hint)
        self._hint = hint
        return self

    def comment(self, comment: object) -> "AsyncCursor":
        self._ensure_mutable()
        self._comment = comment
        return self

    def max_time_ms(self, max_time_ms: int) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_max_time_ms(max_time_ms)
        self._max_time_ms = max_time_ms
        return self

    def batch_size(self, batch_size: int) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_batch_size(batch_size)
        self._batch_size = batch_size
        return self

    def skip(self, skip: int) -> "AsyncCursor":
        self._ensure_mutable()
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self._skip = skip
        return self

    def limit(self, limit: int | None) -> "AsyncCursor":
        self._ensure_mutable()
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        self._limit = limit
        return self

    async def to_list(self) -> list[Document]:
        documents = [document async for document in self]
        return documents

    async def first(self) -> Document | None:
        if self._limit == 0:
            return None
        active = self._active_async_iterable
        if active is not None:
            try:
                return await active.__anext__()
            except StopAsyncIteration:
                return None
        if self._exhausted:
            return None
        async for document in self._iter(limit=1, enforce_ownership=False):
            return document
        return None

    def rewind(self) -> "AsyncCursor":
        active = self._active_async_iterable
        if active is not None:
            self._active_async_iterable = None
        self._started = False
        self._exhausted = False
        return self

    def clone(self) -> "AsyncCursor":
        return type(self)(
            self._collection,
            self._filter_spec,
            self._plan,
            self._projection,
            sort=self._sort,
            skip=self._skip,
            limit=self._limit,
            hint=self._hint,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            batch_size=self._batch_size,
            session=self._session,
        )

    def _as_operation(self):
        from mongoeco.api.operations import compile_find_operation

        return compile_find_operation(
            self._filter_spec,
            projection=self._projection,
            sort=self._sort,
            skip=self._skip,
            limit=self._limit,
            hint=self._hint,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            batch_size=self._batch_size,
            dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
            planning_mode=_resolve_planning_mode(self._collection),
            plan=self._plan,
        )

    @property
    def alive(self) -> bool:
        return not self._exhausted

    async def explain(self) -> dict[str, object]:
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
        engine = self._collection._engine
        explain_find_operation = getattr(engine, "explain_find_operation", None)
        if callable(explain_find_operation):
            result = await explain_find_operation(
                self._collection._db_name,
                self._collection._collection_name,
                operation,
                dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
                context=self._session,
            )
        else:
            result = await engine.explain_query_plan(
                self._collection._db_name,
                self._collection._collection_name,
                operation.filter_spec,
                plan=operation.plan,
                sort=operation.sort,
                skip=operation.skip,
                limit=operation.limit,
                hint=operation.hint,
                comment=operation.comment,
                max_time_ms=operation.max_time_ms,
                dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
                context=self._session,
            )
        return _serialize_explanation(
            result
        )
