from __future__ import annotations

import base64
import json
import threading
import time
import weakref

from collections.abc import (
    Awaitable,
    Callable,
    Mapping,
)

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
from mongoeco.errors import OperationFailure
from mongoeco.types import (  # noqa: TC001 - introspectable signature
    ChangeEventDocument,
    ChangeEventSnapshot,
)

from .hub import ChangeStreamHub  # noqa: TC001 - introspectable cursor signature
from .models import (
    ChangeStreamScope,  # noqa: TC001 - cursor signature is introspectable
)
from .pipeline import (
    apply_full_document_mode,
    compile_change_stream_pipeline,
    normalize_full_document_mode,
    pre_image_match,
)


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
        self._offset = resolve_change_stream_offset(
            hub,
            resume_after=resume_after,
            start_after=start_after,
            start_at_operation_time=start_at_operation_time,
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
        hub.register_watcher()
        self._watcher_finalizer = weakref.finalize(self, hub.unregister_watcher)

    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationFailure("cannot use change stream after it has been closed")

    def __await__(self):
        async def _resolve():
            return self

        return _resolve().__await__()

    async def _transform(self, event) -> ChangeEventDocument | None:
        if not self._scope.matches(event):
            return None
        document = event.to_document()
        try:
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
                self._ensure_open()
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
        except OperationFailure:
            self.close()
            raise
        return self._materialize_document(document)

    async def try_next(self) -> ChangeEventDocument | None:
        self._ensure_open()
        deadline = None
        if self._max_await_time_ms is not None:
            deadline = time.monotonic() + (self._max_await_time_ms / 1000.0)
        while True:
            timeout_seconds = None
            if deadline is not None:
                timeout_seconds = max(0.0, deadline - time.monotonic())
            next_offset, event = await self._hub.wait_for_event_async(
                self._offset,
                timeout_seconds=timeout_seconds,
                stop_event=self._stop_event,
            )
            self._ensure_open()
            self._offset = next_offset
            if event is None:
                return None
            transformed = await self._transform(event)
            if event.operation_type == "invalidate" and self._scope.matches(event):
                self.close()
            if transformed is not None:
                return transformed

    async def next(self) -> ChangeEventDocument:
        self._ensure_open()
        while True:
            document = await self._hub.wait_for_event_async(
                self._offset,
                timeout_seconds=None,
                stop_event=self._stop_event,
            )
            self._ensure_open()
            self._offset, event = document
            if event is None:
                continue
            transformed = await self._transform(event)
            if event.operation_type == "invalidate" and self._scope.matches(event):
                self.close()
            if transformed is not None:
                return transformed

    def __aiter__(self):
        async def _iterate():
            while not self._closed:
                yield await self.next()

        return _iterate()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()
        self._hub.wake_waiters()
        self._watcher_finalizer()

    @property
    def alive(self) -> bool:
        return not self._closed


class ChangeStreamCursor:
    def __init__(self, client, async_cursor: AsyncChangeStreamCursor) -> None:
        self._client = client
        self._async_cursor = async_cursor

    def try_next(self) -> ChangeEventDocument | None:
        return self._client._run(self._async_cursor.try_next())

    def next(self) -> ChangeEventDocument:
        return self._client._run(self._async_cursor.next())

    def close(self) -> None:
        self._async_cursor.close()

    @property
    def alive(self) -> bool:
        return self._async_cursor.alive


def parse_resume_token(token: object) -> int:
    """Read legacy decimal and v1 local tokens without leaking Python errors."""
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
            return int(raw)
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
    ):
        raise server_error(invalid_data, code=9)
    return token_value


def resolve_change_stream_offset(
    hub: ChangeStreamHub,
    *,
    resume_after: dict[str, object] | None,
    start_after: dict[str, object] | None,
    start_at_operation_time: int | None,
) -> int:
    configured = sum(
        option is not None
        for option in (resume_after, start_after, start_at_operation_time)
    )
    if configured > 1:
        raise OperationFailure(
            "watch accepts at most one of resume_after, start_after, or start_at_operation_time"
        )
    if resume_after is not None:
        return hub.offset_after_token(parse_resume_token(resume_after))
    if start_after is not None:
        return hub.offset_after_token(parse_resume_token(start_after))
    if start_at_operation_time is not None:
        if (
            not isinstance(start_at_operation_time, int)
            or isinstance(start_at_operation_time, bool)
            or start_at_operation_time < 0
        ):
            raise TypeError("start_at_operation_time must be a non-negative integer")
        return hub.offset_at_or_after_cluster_time(start_at_operation_time)
    return hub.current_offset()
