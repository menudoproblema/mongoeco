import asyncio
import time

from collections import deque
from collections.abc import Mapping
from dataclasses import replace

from mongoeco.api.argument_validation import (
    HintSpec,
    normalize_sort_spec as _normalize_sort_spec,
    validate_batch_size as _validate_batch_size,
    validate_hint_spec as _validate_hint_spec,
    validate_max_time_ms as _validate_max_time_ms,
    validate_sort_spec as _validate_sort_spec,
)
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import (
    OperationContext,
    resolve_operation_session,
)
from mongoeco.core.query_plan import QueryNode
from mongoeco.cxp import build_mongodb_explain_projection
from mongoeco.engines.adapter import adapt_engine
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    CollationDocument,
    Document,
    Filter,
    PlanningMode,
    Projection,
    QueryPlanExplanation,
    SortSpec,
)


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

    async def _close_after_failure(self) -> None:
        try:
            await asyncio.shield(self.close())
        except BaseException:
            pass

    async def __anext__(self) -> Document:
        if self._closed:
            raise StopAsyncIteration
        await self._cursor._close_retired_sources()
        if self._enforce_ownership and self._cursor._active_async_iterable is not self:
            self._closed = True
            raise StopAsyncIteration
        if self._source is not None:
            try:
                document = await self._source.__anext__()
                if self._cursor._apply_codec_options:
                    return self._cursor._materialize_document(document)
                return document
            except StopAsyncIteration:
                self._cursor._exhausted = True
                await self.close()
                raise
            except BaseException:
                await self._close_after_failure()
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
        await self._cursor._close_retired_sources()
        if self._enforce_ownership and self._cursor._active_async_iterable is not self:
            self._closed = True
            return []

        items: list[Document] = []
        while len(items) < max_items:
            if self._source is not None:
                try:
                    document = await self._source.__anext__()
                    if self._cursor._apply_codec_options:
                        document = self._cursor._materialize_document(document)
                    items.append(document)
                    continue
                except StopAsyncIteration:
                    self._cursor._exhausted = True
                    self._source = None
                    if not items:
                        await self.close()
                    break
                except BaseException:
                    await self._close_after_failure()
                    raise
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
        try:
            page = await self._cursor._fetch_batch(self._position, target_size)
        except BaseException:
            await self._close_after_failure()
            raise
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
        self._source = None
        await self._cursor._close_batch_source()
        if self._cursor._active_async_iterable is self:
            self._cursor._active_async_iterable = None
        if self._cursor._active_async_iterable is None and self._cursor._exhausted:
            self._cursor._started = True

    async def aclose(self) -> None:
        await self.close()

    def retire(self) -> None:
        """Detach resources so synchronous rewind can close them on next I/O."""
        self._closed = True
        self._cursor._retire_source(self._source)
        self._source = None
        if self._cursor._active_async_iterable is self:
            self._cursor._active_async_iterable = None


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
        let: dict[str, object] | None = None,
        execution_variables: Mapping[str, object] | None = None,
        operation_context: OperationContext | None = None,
        session: ClientSession | None = None,
        apply_codec_options: bool = True,
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
        self._let = let
        self._execution_variables = execution_variables
        self._operation_context = operation_context
        self._session = resolve_operation_session(operation_context, session)
        self._apply_codec_options = apply_codec_options
        self._started = False
        self._exhausted = False
        self._closed = False
        self._active_async_iterable: _AsyncCursorIterator | None = None
        self._operation_cache = None
        self._semantics_cache = None
        self._batch_source = None
        self._retired_sources: list[object] = []

    def _ensure_mutable(self) -> None:
        if self._closed:
            raise InvalidOperation("cannot modify cursor after it has been closed")
        if self._started:
            raise InvalidOperation("cannot modify cursor after iteration has started")

    def _invalidate_execution_cache(self) -> None:
        self._operation_cache = None
        self._semantics_cache = None
        self._retire_batch_source()

    def _retire_source(self, source: object | None) -> None:
        if source is not None and all(
            candidate is not source for candidate in self._retired_sources
        ):
            self._retired_sources.append(source)

    def _retire_batch_source(self) -> None:
        self._retire_source(self._batch_source)
        self._batch_source = None

    async def _close_source(self, source: object | None) -> None:
        close = getattr(source, "aclose", None)
        if callable(close):
            await close()

    async def _close_batch_source(self) -> None:
        source = self._batch_source
        self._batch_source = None
        await self._close_source(source)

    async def _close_retired_sources(self) -> None:
        while self._retired_sources:
            await self._close_source(self._retired_sources.pop())

    def _current_dialect(self):
        return getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)

    def _base_operation(self):
        if self._operation_cache is None:
            self._operation_cache = self._as_operation()
        return self._operation_cache

    def _base_semantics(self):
        if self._semantics_cache is None:
            from mongoeco.engines.semantic_core import (  # noqa: PLC0415
                compile_find_semantics_from_operation,
            )

            self._semantics_cache = compile_find_semantics_from_operation(
                self._base_operation(),
                dialect=self._current_dialect(),
                variables=self._execution_variables,
            )
        return self._semantics_cache

    def _operation_with_overrides(self, **changes: object):
        return self._base_operation().with_overrides(**changes)

    def _semantics_with_overrides(self, **changes: object):
        return replace(self._base_semantics(), **changes)

    def _materialize_document(self, document: Document) -> Document:
        applier = getattr(self._collection, "_apply_codec_options_to_document", None)
        if callable(applier):
            return applier(document)
        return document

    def _scan(self, *, limit: int | None = None):
        self._started = True
        engine = self._collection._engine
        record_runtime_opcounter = getattr(engine, "_record_runtime_opcounter", None)
        if callable(record_runtime_opcounter):
            record_runtime_opcounter("query")
        operation = self._operation_with_overrides(limit=self._limit if limit is None else limit)
        _ensure_operation_executable(self._collection, operation)
        semantics = self._semantics_with_overrides(limit=operation.limit)
        open_snapshot = getattr(
            self._collection,
            '_engine_scan_with_operation',
            None,
        )
        stream = (
            open_snapshot(operation, session=self._session)
            if callable(open_snapshot)
            else adapt_engine(engine).open_read_snapshot(
                self._collection._db_name,
                self._collection._collection_name,
                semantics,
                operation_context=operation.context,
            )
        )
        return stream

    async def _fetch_batch(self, offset: int, batch_size: int) -> list[Document]:
        await self._close_retired_sources()
        if self._exhausted:
            return []
        if self._limit is not None:
            remaining = self._limit - offset
            if remaining <= 0:
                self._exhausted = True
                return []
            batch_size = min(batch_size, remaining)
        self._started = True
        engine = self._collection._engine
        record_runtime_opcounter = getattr(engine, "_record_runtime_opcounter", None)
        if callable(record_runtime_opcounter):
            record_runtime_opcounter("query" if offset == 0 else "getmore")
        if self._batch_source is None:
            operation = self._operation_with_overrides(
                skip=self._skip,
                limit=self._limit,
            )
            _ensure_operation_executable(self._collection, operation)
            semantics = self._semantics_with_overrides(
                skip=self._skip,
                limit=self._limit,
            )
            open_snapshot = getattr(
                self._collection,
                '_engine_scan_with_operation',
                None,
            )
            source = (
                open_snapshot(operation, session=self._session)
                if callable(open_snapshot)
                else adapt_engine(engine).open_read_snapshot(
                    self._collection._db_name,
                    self._collection._collection_name,
                    semantics,
                    operation_context=operation.context,
                )
            )
            self._batch_source = source.__aiter__()

        page: list[Document] = []
        while len(page) < batch_size:
            try:
                document = await self._batch_source.__anext__()
            except StopAsyncIteration:
                await self._close_batch_source()
                self._exhausted = True
                break
            except BaseException:
                self._exhausted = True
                await self._close_batch_source()
                raise
            page.append(
                self._materialize_document(document)
                if self._apply_codec_options
                else document
            )
        if self._limit is not None and offset + len(page) >= self._limit:
            self._exhausted = True
            await self._close_batch_source()
        return page

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
        if self._closed or (
            self._exhausted and self._active_async_iterable is None
        ):
            async def _empty():
                if False:
                    yield None

            return _empty()
        if self._active_async_iterable is None:
            self._active_async_iterable = self._iter()
        return self._active_async_iterable

    def sort(
        self,
        key_or_list: SortSpec | str,
        direction: int | None = None,
    ) -> "AsyncCursor":
        self._ensure_mutable()
        sort = (
            [(key_or_list, 1 if direction is None else direction)]
            if isinstance(key_or_list, str)
            else key_or_list
        )
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

    async def to_list(
        self,
        length: int | None = None,
    ) -> list[Document]:
        if length is not None and length < 0:
            raise ValueError("length must be non-negative or None")
        if length == 0 or self._closed:
            return []
        if self._limit == 0:
            return []
        if length is None and self._limit == 1 and self._active_async_iterable is None and not self._started and not self._exhausted:
            first = await self.first()
            self._exhausted = True
            self._started = True
            return [] if first is None else [first]
        operation = self._as_operation()
        started_at = time.perf_counter_ns()
        try:
            documents: list[Document] = []
            iterator = self.__aiter__()
            while length is None or len(documents) < length:
                try:
                    documents.append(await iterator.__anext__())
                except StopAsyncIteration:
                    break
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

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        active = self._active_async_iterable
        self._active_async_iterable = None
        self._exhausted = True
        if active is not None:
            await active.close()
        else:
            await self._close_batch_source()
        await self._close_retired_sources()

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
        iterator = _AsyncCursorIterator(
            self,
            batch_size=None,
            enforce_ownership=False,
            source=self._scan(limit=1),
        )
        try:
            document = await iterator.__anext__()
        except StopAsyncIteration:
            document = None
        finally:
            await iterator.close()
        if document is not None:
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
        if isinstance(active, _AsyncCursorIterator):
            active.retire()
        elif active is not None:
            self._active_async_iterable = None
        self._retire_batch_source()
        self._started = False
        self._exhausted = False
        self._semantics_cache = None
        return self

    def clone(self) -> "AsyncCursor":
        create_context = getattr(self._collection, '_new_operation_context', None)
        operation_context = (
            create_context(
                session=self._session,
                collation=self._collation,
                bindings=(
                    self._let.bindings
                    if isinstance(self._let, ExpressionExecutionContext)
                    else self._let
                ),
            )
            if callable(create_context)
            else None
        )
        execution_variables = (
            operation_context.expressions
            if operation_context is not None
            else None
        )
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
            let=self._let,
            execution_variables=execution_variables,
            operation_context=operation_context,
            session=self._session,
            apply_codec_options=self._apply_codec_options,
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
                variables=self._let,
                dialect=self._current_dialect(),
                planning_mode=_resolve_planning_mode(self._collection),
                plan=self._plan,
            )
            if self._operation_context is not None:
                self._operation_cache = self._operation_cache.bind(
                    self._operation_context,
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
