from __future__ import annotations

import asyncio
import base64
import json
import threading
import time
import weakref

from collections import deque
from collections.abc import (
    Awaitable,
    Callable,
    Generator,
    Mapping,
)
from copy import deepcopy
from typing import Self

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation import apply_pipeline, compile_pipeline
from mongoeco.core.bson_identity import same_bson_update_value
from mongoeco.core.bson_size import bson_document_size
from mongoeco.core.bson_values import matches_bson_type
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.expression_context import (
    ExpressionExecutionContext,
    ensure_expression_context,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.server_errors import server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import (
    ChangeEventDocument,
    ChangeEventSnapshot,
    encode_change_stream_token,
)

from .hub import ChangeStreamHub  # noqa: TC001 - introspectable signature
from .models import (
    ChangeStreamScope,  # noqa: TC001 - introspectable signature
)
from .pipeline import (
    apply_full_document_mode,
    compile_change_stream_pipeline,
    normalize_full_document_mode,
    pre_image_match,
    validate_change_stream_pipeline_container,
)


class _CompletedClose(Awaitable[None]):
    """Close immediately for existing callers and remain safely awaitable."""

    def __await__(self) -> Generator[None]:
        yield from ()


_COMPLETED_CLOSE = _CompletedClose()
_INITIAL_BATCH_COUNT = 101
_BATCH_BYTES = 16 * 1024 * 1024
# OP_MSG header and BSON body/cursor/array prefixes, before any batch entries.
# Namespace, cursor id and post-batch token are appended after the batch.
_INITIAL_BATCH_PREFIX_BYTES = 53
_NEXT_BATCH_PREFIX_BYTES = 52


class AsyncChangeStreamCursor:
    def __init__(
        self,
        hub: ChangeStreamHub,
        *,
        scope: ChangeStreamScope,
        pipeline: object | None = None,
        max_await_time_ms: int | None = None,
        resume_after: dict[str, object] | None = None,
        start_after: dict[str, object] | None = None,
        start_at_operation_time: int | None = None,
        full_document: str | None = "default",
        materialize_document: Callable[[dict[str, object]], dict[str, object]]
        | None = None,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        execution_context: ExpressionExecutionContext | None = None,
        lookup_full_document: Callable[[ChangeEventSnapshot], Awaitable[dict | None]]
        | None = None,
    ) -> None:
        self._lookup_full_document = lookup_full_document
        self._hub = hub
        self._scope = scope
        validate_change_stream_pipeline_container(pipeline, full_document)
        self._full_document = normalize_full_document_mode(full_document)
        token, after, operation_time = _prepare_resume_request(
            resume_after, start_after, start_at_operation_time,
        )
        if after is not None:
            hub.resolve_cursor_start(after=after, start_after=start_after is not None)
        self._pipeline = compile_change_stream_pipeline(
            pipeline,
            dialect=dialect,
            collection=scope.coll_name,
        )
        self._pre_image_match = pre_image_match(self._pipeline, dialect)
        self._dialect = dialect
        self._execution_context = ensure_expression_context(execution_context)
        self._compiled_pipeline = (
            compile_pipeline(self._pipeline, dialect=dialect)
            if self._pipeline is not None
            else None
        )
        self._max_await_time_ms = max_await_time_ms
        if self._full_document == "updateLookup" and lookup_full_document is None:
            message = "updateLookup requires a current-document lookup resource"
            raise NotImplementedError(message)
        self._materialize_document = materialize_document or DocumentCodec.to_pymongo
        self._closed = False
        self._stop_event = threading.Event()
        start = hub.resolve_cursor_start(
            after=after, operation_time=operation_time,
            start_after=start_after is not None, register=True,
        )
        self._offset, self._position = start.offset, start.position
        checkpoint, self._initial_end = start.checkpoint, start.available_end
        self._operation_time = operation_time
        self._resume_anchor = start.resume_event
        self._anchor_seen = True
        self._resume_token = (
            _checkpoint_token(checkpoint) if checkpoint is not None else token
        )
        self._watcher_finalizer = weakref.finalize(self, hub.unregister_watcher)
        self._initialized = False
        self._initialize_lock = asyncio.Lock()
        self._buffer = deque()
        self._pending = None
        self._batch_checkpoint = self._resume_token
        self._batch_invalidated = False

    def __await__(self) -> Generator[None, None, Self]:
        async def _resolve() -> Self:
            await self._initialize()
            return self

        return _resolve().__await__()

    async def _transform(self, event) -> ChangeEventDocument | None:
        if not self._scope.matches(event):
            return None
        document = event.to_document()
        if self._pre_image_match is not None and not QueryEngine.match_plan(
            document,
            self._pre_image_match,
            dialect=self._dialect,
            variables=self._execution_context,
        ):
            return None
        lookup_document = None
        if (
            event.operation_type == "update"
            and self._full_document == "updateLookup"
            and self._lookup_full_document is not None
        ):
            lookup_document = await self._lookup_full_document(event)
            if self._closed:
                return None
        document = apply_full_document_mode(
            document,
            self._full_document,
            post_image_available=event.post_image_available,
            lookup_document=lookup_document,
        )
        if self._pipeline is not None:
            token = document["_id"]
            transformed = (
                self._compiled_pipeline.execute(
                    [document],
                    variables=self._execution_context,
                )
                if self._compiled_pipeline is not None
                else apply_pipeline(
                    [document],
                    self._pipeline,
                    dialect=self._dialect,
                    variables=self._execution_context,
                )
            )
            if not transformed:
                return None
            document = transformed[0]
            if "_id" not in document or not same_bson_update_value(
                document["_id"], token
            ):
                message = (
                    "Change stream pipeline must preserve the event resume token"
                )
                raise server_error(message, code=280)
        return document

    async def _initialize(self) -> None:
        async with self._initialize_lock:
            if self._initialized or self._closed:
                return
            try:
                anchor, self._resume_anchor = self._resume_anchor, None
                if anchor is not None and anchor.operation_type != "invalidate":
                    self._anchor_seen = (
                        await self._transform(anchor)
                    ) is not None
                await self._fill_batch(initial=True)
                if self._max_await_time_ms is not None and not isinstance(
                    self._max_await_time_ms, int
                ):
                    message = "max_await_time_ms must be an integer or None"
                    raise TypeError(message)
                self._initialized = True
            except BaseException:
                self.close()
                raise

    def _validate_get_more(self) -> None:
        value = self._max_await_time_ms
        if isinstance(value, bool):
            message = "maxAwaitTimeMS must be a BSON number"
            raise server_error(message, code=14)
        if value is not None and not -(2**63) <= value < 2**63:
            message = "MongoDB can only handle up to 8-byte ints"
            raise OverflowError(message)
        if value is not None and value < 0:
            message = "maxAwaitTimeMS must not be negative"
            raise server_error(message, code=2)

    async def _prepare_event(self, next_offset, event):
        if self._operation_time is not None and event.token < self._operation_time:
            self._offset = next_offset
            return event, None, 0
        self._hub.validate_cursor_advance(self._position, event.token)
        document = await self._transform(event)
        if self._closed:
            return event, None, 0
        if document is not None and not self._anchor_seen:
            message = "cannot resume stream; the resume token was not found"
            raise server_error(message, code=280)
        self._hub.validate_cursor_advance(self._position, event.token)
        self._offset, self._position = next_offset, event.token
        size = 0 if document is None else bson_document_size(document)
        if size > _BATCH_BYTES:
            message = "change stream event exceeds the BSON document size limit"
            raise server_error(message, code=10334)
        return event, document, size

    async def _next_prepared_change(self, *, initial: bool, deadline, wait: bool):
        if self._pending is not None:
            prepared, self._pending = self._pending, None
            return prepared
        if initial and self._offset >= self._initial_end:
            return None
        next_offset, event = self._hub.retained_event(self._offset)
        while event is None:
            if initial or not wait or self._closed:
                return None
            timeout = (
                None if deadline is None else max(0.0, deadline - time.monotonic())
            )
            next_offset, event = await self._hub.wait_for_event_async(
                self._offset, timeout_seconds=timeout, stop_event=self._stop_event,
            )
            if self._closed or (event is None and deadline is not None):
                return None
        return await self._prepare_event(next_offset, event)

    async def _fill_batch(self, *, initial: bool) -> None:
        deadline = None
        if not initial:
            timeout = (
                1.0 if self._max_await_time_ms is None
                else self._max_await_time_ms / 1000.0
            )
            deadline = time.monotonic() + timeout
        rows = []
        total_bytes = (
            _INITIAL_BATCH_PREFIX_BYTES if initial else _NEXT_BATCH_PREFIX_BYTES
        )
        checkpoint = self._batch_checkpoint
        while not self._closed and (not initial or len(rows) < _INITIAL_BATCH_COUNT):
            try:
                prepared = await self._next_prepared_change(
                    initial=initial, deadline=deadline, wait=not rows,
                )
            except OperationFailure as error:
                diagnostic = getattr(error, "_server_diagnostic", None)
                if rows and getattr(diagnostic, "reason", None) == "change-stream-gap":
                    break
                raise
            if prepared is None or self._closed:
                break
            event, document, size = prepared
            # getMore also reserves the candidate's post-batch resume token.
            token_size = (
                bson_document_size(event.resume_token)
                if document is not None and not initial else 0
            )
            if (
                document is not None and rows
                and total_bytes + size + token_size > _BATCH_BYTES
            ):
                self._pending = event, document, size
                break
            invalidates = (
                event.operation_type == "invalidate" and self._scope.matches(event)
            )
            if document is not None:
                rows.append((document, event.resume_token))
                total_bytes += size + 2 + len(str(len(rows) - 1))
            if self._operation_time is None or event.token >= self._operation_time:
                checkpoint = (
                    event.resume_token if document is not None or invalidates
                    else _checkpoint_token(event.token)
                )
            if invalidates:
                self._batch_invalidated = True
                break
        self._cache_batch(rows, checkpoint)

    def _cache_batch(self, rows, checkpoint):
        if self._closed:
            return
        if self._pending is None and not self._batch_invalidated:
            try:
                position = self._hub.cursor_checkpoint(self._offset, self._position)
            except OperationFailure as error:
                diagnostic = getattr(error, "_server_diagnostic", None)
                if (
                    not rows
                    or getattr(diagnostic, "reason", None) != "change-stream-gap"
                ):
                    raise
                position = None
            if position is not None:
                self._position = position
                checkpoint = _checkpoint_token(position)
        # Decode only the admitted batch, as a command response would do.
        materialized = [
            (self._materialize_document(document), token) for document, token in rows
        ]
        if self._closed:
            return
        self._buffer.extend(materialized)
        self._batch_checkpoint = checkpoint
        if not rows:
            self._resume_token = deepcopy(checkpoint)

    def _pop_change(self):
        document, token = self._buffer.popleft()
        self._resume_token = deepcopy(token if self._buffer else self._batch_checkpoint)
        if not self._buffer and self._batch_invalidated:
            self.close()
        return document

    async def _read(self, *, blocking: bool) -> ChangeEventDocument | None:
        await self._initialize()
        if self._closed:
            return None
        while not self._closed:
            if self._buffer:
                return self._pop_change()
            if self._batch_invalidated:
                # PyMongo attempts resume before its read-error cleanup when an
                # aggregate response was invalidated with an empty first batch.
                message = "resumeAfter cannot use an invalidate event; use startAfter"
                raise server_error(message, code=260)
            try:
                self._validate_get_more()
                await self._fill_batch(initial=False)
                if self._closed:
                    return None
                if self._buffer:
                    return self._pop_change()
                if self._batch_invalidated:
                    self.close()
            except BaseException:
                self.close()
                raise
            if not blocking:
                return None
        return None

    async def try_next(self) -> ChangeEventDocument | None:
        return await self._read(blocking=False)

    async def next(self) -> ChangeEventDocument:
        document = await self._read(blocking=True)
        if document is None:
            raise StopAsyncIteration
        return document

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> ChangeEventDocument:
        return await self.next()

    async def __aenter__(self) -> Self:
        await self._initialize()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def close(self) -> Awaitable[None]:
        if not self._closed:
            self._closed = True
            self._buffer.clear()
            self._pending = None
            self._resume_anchor = None
            self._stop_event.set()
            self._hub.wake_waiters()
            self._watcher_finalizer()
        return _COMPLETED_CLOSE

    @property
    def alive(self) -> bool:
        return not self._closed

    @property
    def resume_token(self) -> dict[str, object] | None:
        return deepcopy(self._resume_token)


class ChangeStreamCursor:
    def __init__(self, client, async_cursor: AsyncChangeStreamCursor) -> None:
        self._client = client
        self._async_cursor = async_cursor
        self._client._run(async_cursor._initialize())

    def try_next(self) -> ChangeEventDocument | None:
        return self._client._run(self._async_cursor.try_next())

    def next(self) -> ChangeEventDocument:
        try:
            return self._client._run(self._async_cursor.next())
        except StopAsyncIteration:
            raise StopIteration from None

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> ChangeEventDocument:
        return self.next()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._async_cursor.close()

    @property
    def alive(self) -> bool:
        return self._async_cursor.alive

    @property
    def resume_token(self) -> dict[str, object] | None:
        return self._async_cursor.resume_token


def parse_resume_token(token: object) -> int:
    """Read legacy decimal and v1 local tokens without leaking Python errors."""
    return _decode_resume_token(token)[0]


def _decode_resume_token(token: object) -> tuple[int, bool | None]:
    if not isinstance(token, Mapping):
        message = "resume token must be a document"
        raise server_error(message, code=14)
    raw = token.get("_data")
    if not matches_bson_type(raw, "string"):
        message = "resume token must contain a string _data field"
        raise server_error(message, code=40647)
    if not raw:
        message = "resume token _data must not be empty"
        raise server_error(message, code=40649)
    invalid_data = "resume token must contain a valid _data field"
    try:
        if raw.isascii() and raw.isdecimal():
            return int(raw), None
        padding = "=" * (-len(raw) % 4)
        payload = base64.b64decode(
            (raw + padding).encode("ascii"), altchars=b"-_", validate=True
        )
        document = json.loads(payload.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        raise server_error(invalid_data, code=9) from exc
    token_value = document.get("t") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or not isinstance(document.get("v"), int)
        or isinstance(document.get("v"), bool)
        or document["v"] != 1
        or not isinstance(token_value, int)
        or isinstance(token_value, bool)
        or token_value < 0
        or ("i" in document and not isinstance(document["i"], bool))
    ):
        raise server_error(invalid_data, code=9)
    return token_value, document.get("i")


def _checkpoint_token(position: int) -> dict[str, object]:
    return {"_data": encode_change_stream_token(position, invalidate=False)}


def _prepare_resume_request(resume_after, start_after, operation_time):
    # PyMongo selects the token before building the command, including empty
    # start_after falling back to resume_after. Validate the selected request.
    token = deepcopy(start_after or resume_after)
    if token is not None:
        after = _decode_resume_token(token)
        return dict(token), after, None
    if operation_time is not None and (
        not isinstance(operation_time, int)
        or isinstance(operation_time, bool)
        or operation_time < 0
    ):
        message = "start_at_operation_time must be a non-negative integer"
        raise TypeError(message)
    return None, None, operation_time


def resolve_change_stream_offset(
    hub: ChangeStreamHub,
    *,
    resume_after: dict[str, object] | None,
    start_after: dict[str, object] | None,
    start_at_operation_time: int | None,
) -> int:
    _token, after, operation_time = _prepare_resume_request(
        resume_after, start_after, start_at_operation_time,
    )
    return hub.resolve_cursor_start(
        after=after, operation_time=operation_time, start_after=start_after is not None,
    )[0]
