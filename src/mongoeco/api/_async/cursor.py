import time
from dataclasses import replace
from collections.abc import Mapping, Sequence

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.query_plan import QueryNode
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import CollationDocument, Document, Filter, PlanningMode, Projection, QueryPlanExplanation, SortSpec


type HintSpec = str | SortSpec
_DEFAULT_LOCAL_PREFETCH_SIZE = 101


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
            if self._cursor._exhausted:
                await self.close()
                raise StopAsyncIteration
            await self._fill_buffer()
        if not self._buffer:
            await self.close()
            raise StopAsyncIteration
        return self._buffer.pop(0)

    async def _fill_buffer(self) -> None:
        target_size = self._batch_size if self._batch_size not in (None, 0) else _DEFAULT_LOCAL_PREFETCH_SIZE
        page = await self._cursor._fetch_batch(self._position, target_size)
        if not page:
            self._cursor._exhausted = True
            return
        if len(page) < target_size:
            self._cursor._exhausted = True
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


def _normalize_sort_spec(sort: object | None) -> SortSpec | None:
    if sort is None:
        return None
    if isinstance(sort, Mapping):
        normalized = list(sort.items())
        _validate_sort_spec(normalized)
        return normalized
    if (
        isinstance(sort, tuple)
        and len(sort) == 2
        and isinstance(sort[0], str)
        and sort[1] in (1, -1)
        and not isinstance(sort[1], bool)
    ):
        normalized = [sort]
        _validate_sort_spec(normalized)
        return normalized
    if isinstance(sort, Sequence) and not isinstance(sort, (str, bytes, bytearray, list)):
        normalized = list(sort)
        _validate_sort_spec(normalized)
        return normalized
    _validate_sort_spec(sort)
    return sort


def _validate_sort_spec(sort: object) -> None:
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
    _normalize_sort_spec(hint)


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
        self._collection = collection
        self._filter_spec = filter_spec
        self._plan = plan
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
        self._started = False
        self._exhausted = False
        self._active_async_iterable: _AsyncCursorIterator | None = None
        self._operation_cache = None
        self._semantics_cache = None

    def _ensure_mutable(self) -> None:
        if self._started:
            raise InvalidOperation("cannot modify cursor after iteration has started")

    def _invalidate_execution_cache(self) -> None:
        self._operation_cache = None
        self._semantics_cache = None

    def _current_dialect(self):
        return getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)

    def _base_operation(self):
        if self._operation_cache is None:
            self._operation_cache = self._as_operation()
        return self._operation_cache

    def _base_semantics(self):
        if self._semantics_cache is None:
            from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

            self._semantics_cache = compile_find_semantics_from_operation(
                self._base_operation(),
                dialect=self._current_dialect(),
            )
        return self._semantics_cache

    def _operation_with_overrides(self, **changes: object):
        return self._base_operation().with_overrides(**changes)

    def _semantics_with_overrides(self, **changes: object):
        return replace(self._base_semantics(), **changes)

    def _scan(self, *, limit: int | None = None):
        self._started = True
        engine = self._collection._engine
        operation = self._operation_with_overrides(limit=self._limit if limit is None else limit)
        _ensure_operation_executable(self._collection, operation)
        semantics = self._semantics_with_overrides(limit=operation.limit)
        return engine.scan_find_semantics(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
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
        operation = self._operation_with_overrides(skip=effective_skip, limit=effective_limit)
        _ensure_operation_executable(self._collection, operation)
        engine = self._collection._engine
        semantics = self._semantics_with_overrides(skip=effective_skip, limit=effective_limit)
        iterable = engine.scan_find_semantics(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
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
        self._sort = _normalize_sort_spec(sort)
        self._invalidate_execution_cache()
        return self

    def hint(self, hint: HintSpec) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_hint_spec(hint)
        self._hint = hint if isinstance(hint, str) else _normalize_sort_spec(hint)
        self._invalidate_execution_cache()
        return self

    def comment(self, comment: object) -> "AsyncCursor":
        self._ensure_mutable()
        self._comment = comment
        self._invalidate_execution_cache()
        return self

    def max_time_ms(self, max_time_ms: int) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_max_time_ms(max_time_ms)
        self._max_time_ms = max_time_ms
        self._invalidate_execution_cache()
        return self

    def batch_size(self, batch_size: int) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_batch_size(batch_size)
        self._batch_size = batch_size
        self._invalidate_execution_cache()
        return self

    def skip(self, skip: int) -> "AsyncCursor":
        self._ensure_mutable()
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self._skip = skip
        self._invalidate_execution_cache()
        return self

    def limit(self, limit: int | None) -> "AsyncCursor":
        self._ensure_mutable()
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        self._limit = limit
        self._invalidate_execution_cache()
        return self

    async def to_list(self) -> list[Document]:
        operation = self._as_operation()
        started_at = time.perf_counter_ns()
        try:
            documents = [document async for document in self]
        except Exception as exc:
            profiler = getattr(self._collection, "_profile_operation", None)
            if callable(profiler):
                await profiler(
                    op="query",
                    command={"find": self._collection._collection_name, "filter": operation.filter_spec},
                    duration_ns=time.perf_counter_ns() - started_at,
                    operation=operation,
                    errmsg=str(exc),
                )
            raise
        profiler = getattr(self._collection, "_profile_operation", None)
        if callable(profiler):
            await profiler(
                op="query",
                command={"find": self._collection._collection_name, "filter": operation.filter_spec},
                duration_ns=time.perf_counter_ns() - started_at,
                operation=operation,
            )
        return documents

    async def first(self) -> Document | None:
        operation = self._as_operation()
        started_at = time.perf_counter_ns()
        if self._limit == 0:
            return None
        active = self._active_async_iterable
        if active is not None:
            try:
                value = await active.__anext__()
            except StopAsyncIteration:
                value = None
            except Exception as exc:
                profiler = getattr(self._collection, "_profile_operation", None)
                if callable(profiler):
                    await profiler(
                        op="query",
                        command={"find": self._collection._collection_name, "filter": operation.filter_spec},
                        duration_ns=time.perf_counter_ns() - started_at,
                        operation=operation,
                        errmsg=str(exc),
                    )
                raise
            else:
                profiler = getattr(self._collection, "_profile_operation", None)
                if callable(profiler):
                    await profiler(
                        op="query",
                        command={"find": self._collection._collection_name, "filter": operation.filter_spec},
                        duration_ns=time.perf_counter_ns() - started_at,
                        operation=operation,
                    )
                return value
            return value
        if self._exhausted:
            return None
        async for document in self._iter(limit=1, enforce_ownership=False):
            profiler = getattr(self._collection, "_profile_operation", None)
            if callable(profiler):
                await profiler(
                    op="query",
                    command={"find": self._collection._collection_name, "filter": operation.filter_spec},
                    duration_ns=time.perf_counter_ns() - started_at,
                    operation=operation,
                )
            return document
        profiler = getattr(self._collection, "_profile_operation", None)
        if callable(profiler):
            await profiler(
                op="query",
                command={"find": self._collection._collection_name, "filter": operation.filter_spec},
                duration_ns=time.perf_counter_ns() - started_at,
                operation=operation,
            )
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

    def _as_operation(self):
        if self._operation_cache is None:
            from mongoeco.api.operations import compile_find_operation

            self._operation_cache = compile_find_operation(
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
                dialect=self._current_dialect(),
                planning_mode=_resolve_planning_mode(self._collection),
                plan=self._plan,
            )
        return self._operation_cache

    @property
    def alive(self) -> bool:
        return not self._exhausted

    @property
    def collection(self):
        return self._collection

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
        semantics = self._base_semantics()
        result = await engine.explain_find_semantics(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
            context=self._session,
        )
        return _serialize_explanation(result)
