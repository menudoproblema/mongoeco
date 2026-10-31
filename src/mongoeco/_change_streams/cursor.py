from __future__ import annotations

import base64
import json
import threading
import time
import weakref

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
from mongoeco.core.bson_values import matches_bson_type
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.expression_context import (
    ExpressionExecutionContext,
    ensure_expression_context,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.server_errors import server_error
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
)


class _CompletedClose(Awaitable[None]):
    """Close immediately for existing callers and remain safely awaitable."""

    def __await__(self) -> Generator[None]:
        yield from ()


_COMPLETED_CLOSE = _CompletedClose()


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
        full_document: str = "default",
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
        token, after, operation_time = _prepare_resume_request(
            resume_after, start_after, start_at_operation_time,
        )
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
        self._full_document = normalize_full_document_mode(full_document)
        if self._full_document == "updateLookup" and lookup_full_document is None:
            message = "updateLookup requires a current-document lookup resource"
            raise NotImplementedError(message)
        self._materialize_document = materialize_document or DocumentCodec.to_pymongo
        self._closed = False
        self._stop_event = threading.Event()
        self._offset, self._position, checkpoint = hub.resolve_cursor_start(
            after=after, operation_time=operation_time,
            start_after=start_after is not None, register=True,
        )
        self._operation_time = operation_time
        self._resume_token = (
            _checkpoint_token(checkpoint) if checkpoint is not None else token
        )
        self._watcher_finalizer = weakref.finalize(self, hub.unregister_watcher)

    def __await__(self) -> Generator[None, None, Self]:
        async def _resolve() -> Self:
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
        return self._materialize_document(document)

    async def _read(self, *, blocking: bool) -> ChangeEventDocument | None:
        deadline = None
        if not blocking:
            timeout = (
                1.0 if self._max_await_time_ms is None
                else self._max_await_time_ms / 1000.0
            )
            deadline = time.monotonic() + timeout
        try:
            while not self._closed:
                timeout_seconds = (
                    None if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                next_offset, event = await self._hub.wait_for_event_async(
                    self._offset,
                    timeout_seconds=timeout_seconds,
                    stop_event=self._stop_event,
                )
                if self._closed:
                    return None
                if event is None:
                    checkpoint = self._hub.cursor_checkpoint(
                        self._offset, self._position,
                    )
                    if checkpoint is not None:
                        self._position = checkpoint
                        self._resume_token = _checkpoint_token(checkpoint)
                    if not blocking:
                        return None
                    continue
                if (
                    self._operation_time is not None
                    and event.token < self._operation_time
                ):
                    self._offset = next_offset
                    continue
                self._hub.validate_cursor_advance(self._position, event.token)
                transformed = await self._transform(event)
                if self._closed:
                    return None
                self._hub.validate_cursor_advance(self._position, event.token)
                self._offset, self._position = next_offset, event.token
                self._resume_token = (
                    event.resume_token
                    if transformed is not None or (
                        event.operation_type == "invalidate"
                        and self._scope.matches(event)
                    ) else _checkpoint_token(event.token)
                )
                if event.operation_type == "invalidate" and self._scope.matches(event):
                    self.close()
                if transformed is not None:
                    return transformed
        except BaseException:
            self.close()
            raise
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
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def close(self) -> Awaitable[None]:
        if not self._closed:
            self._closed = True
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
