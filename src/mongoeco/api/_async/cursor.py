import time
from dataclasses import replace
from collections import deque

from mongoeco.api.argument_validation import (
    HintSpec,
    normalize_sort_spec as _normalize_sort_spec,
    validate_batch_size as _validate_batch_size,
    validate_hint_spec as _validate_hint_spec,
    validate_max_time_ms as _validate_max_time_ms,
    validate_sort_spec as _validate_sort_spec,
)
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.cxp import build_mongodb_explain_projection
from mongoeco.core.query_plan import QueryNode
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import CollationDocument, Document, Filter, PlanningMode, Projection, QueryPlanExplanation, SortSpec

_DEFAULT_LOCAL_PREFETCH_SIZE = 101


def _serialize_explanation(result: object) -> dict[str, object]:
    to_document = getattr(result, "to_document", None)
    if callable(to_document):
        return to_document()
    if isinstance(result, dict):
        return result
    raise TypeError(f"Unsupported explain result type: {type(result)!r}")


def _find_explain_cxp_projection(filter_spec: Filter) -> dict[str, object]:
    metadata: dict[str, object] | None = None
    if isinstance(filter_spec, dict) and '$text' in filter_spec:
        metadata = {'nonCanonicalFeature': 'classicText'}
    return build_mongodb_explain_projection(
        capability='read',
        metadata=metadata,
    )


def _operation_issue_message(operation) -> str:
    mode = getattr(getattr(operation, "planning_mode", None), "value", None)
    prefix = "operation has deferred planning issues"
    if isinstance(mode, str) and mode:
        prefix = f"{prefix} ({mode})"
    issues = getattr(operation, "planning_issues", ())
    details: list[str] = []
    for issue in issues:
        scope = getattr(issue, "scope", None)
        message = getattr(issue, "message", None)
        if isinstance(scope, str) and scope:
            details.append(f"{scope}: {message}")
        else:
            details.append(str(message))
    if not details:
        return prefix
    return f"{prefix}: {', '.join(details)}"


def _ensure_operation_executable(collection, operation) -> None:
    checker = getattr(collection, "_ensure_operation_executable", None)
    if callable(checker):
        checker(operation)
        return
    if getattr(operation, "planning_issues", ()):
        raise OperationFailure(_operation_issue_message(operation))


class _AsyncCursorIterator:
    def __init__(
        self,
        cursor: "AsyncCursor",
        *,
        batch_size: int | None,
        enforce_ownership: bool,
        source=None,
    ):
        self._cursor = cursor
        self._batch_size = batch_size
        self._enforce_ownership = enforce_ownership
        self._buffer = deque()
        self._closed = False
        self._position = 0
        self._source = source

    def __aiter__(self):
        return self

    async def __anext__(self) -> Document:
        if self._closed:
            raise StopAsyncIteration
        if self._enforce_ownership and self._cursor._active_async_iterable is not self:
            self._closed = True
            raise StopAsyncIteration
        if self._source is not None:
            try:
                return await self._source.__anext__()
            except StopAsyncIteration:
                self._cursor._exhausted = True
                await self.close()
                raise
        if not self._buffer:
            if self._cursor._exhausted:
                await self.close()
                raise StopAsyncIteration
            await self._fill_buffer()
        if not self._buffer:
            await self.close()
            raise StopAsyncIteration
        return self._buffer.popleft()

    async def pull_chunk(self, max_items: int) -> list[Document]:
        if self._closed or max_items <= 0:
            return []
        if self._enforce_ownership and self._cursor._active_async_iterable is not self:
            self._closed = True
            return []

        items: list[Document] = []
        while len(items) < max_items:
            if self._source is not None:
                try:
                    items.append(await self._source.__anext__())
                    continue
                except StopAsyncIteration:
                    self._cursor._exhausted = True
                    await self.close()
                    break
            if not self._buffer:
                if self._cursor._exhausted:
                    await self.close()
                    break
                await self._fill_buffer()
            if not self._buffer:
                await self.close()
                break
            while self._buffer and len(items) < max_items:
                items.append(self._buffer.popleft())
        return items

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
        close = getattr(self._source, "aclose", None)
        if callable(close):
            await close()
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
        record_runtime_opcounter = getattr(engine, "_record_runtime_opcounter", None)
        if callable(record_runtime_opcounter):
            record_runtime_opcounter("query")
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
        record_runtime_opcounter = getattr(engine, "_record_runtime_opcounter", None)
        if callable(record_runtime_opcounter):
            record_runtime_opcounter("query" if offset == 0 else "getmore")
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
        if self._batch_size is None:
            return _AsyncCursorIterator(
                self,
                batch_size=None,
                enforce_ownership=enforce_ownership,
                source=self._scan(limit=limit),
            )
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
        if self._limit == 0:
            return []
        if self._limit == 1 and self._active_async_iterable is None and not self._started and not self._exhausted:
            first = await self.first()
            return [] if first is None else [first]
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
            explanation = QueryPlanExplanation(
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
                details={"reason": _operation_issue_message(operation)},
                planning_mode=operation.planning_mode,
                planning_issues=operation.planning_issues,
            ).to_document()
            explanation['cxp'] = _find_explain_cxp_projection(self._filter_spec)
            return explanation
        engine = self._collection._engine
        semantics = self._base_semantics()
        result = await engine.explain_find_semantics(
            self._collection._db_name,
            self._collection._collection_name,
            semantics,
            context=self._session,
        )
        explanation = _serialize_explanation(result)
        explanation['cxp'] = _find_explain_cxp_projection(self._filter_spec)
        return explanation
