from __future__ import annotations

import asyncio
import base64
import json
import time

from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventDocument

from .hub import ChangeStreamHub
from .models import ChangeStreamScope
from .pipeline import (
    apply_full_document_mode,
    compile_change_stream_pipeline,
    normalize_full_document_mode,
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
    ) -> None:
        self._hub = hub
        self._scope = scope
        self._offset = resolve_change_stream_offset(
            hub,
            resume_after=resume_after,
            start_after=start_after,
            start_at_operation_time=start_at_operation_time,
        )
        self._pipeline = compile_change_stream_pipeline(pipeline)
        self._max_await_time_ms = max_await_time_ms
        self._full_document = normalize_full_document_mode(full_document)
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationFailure("cannot use change stream after it has been closed")

    def _transform(self, event) -> ChangeEventDocument | None:
        if not self._scope.matches(event):
            return None
        document = event.to_document()
        document = apply_full_document_mode(document, self._full_document)
        if self._pipeline is not None:
            from mongoeco.core.aggregation import apply_pipeline

            transformed = apply_pipeline([document], self._pipeline)
            if not transformed:
                return None
            return transformed[0]
        return document

    async def try_next(self) -> ChangeEventDocument | None:
        self._ensure_open()
        deadline = None
        if self._max_await_time_ms is not None:
            deadline = time.monotonic() + (self._max_await_time_ms / 1000.0)
        while True:
            timeout_seconds = None
            if deadline is not None:
                timeout_seconds = max(0.0, deadline - time.monotonic())
            next_offset, event = await asyncio.to_thread(
                self._hub.wait_for_event,
                self._offset,
                timeout_seconds=timeout_seconds,
            )
            self._offset = next_offset
            if event is None:
                return None
            transformed = self._transform(event)
            if event.operation_type == "invalidate" and self._scope.matches(event):
                self.close()
            if transformed is not None:
                return transformed

    async def next(self) -> ChangeEventDocument:
        self._ensure_open()
        while True:
            document = await asyncio.to_thread(
                self._hub.wait_for_event,
                self._offset,
                timeout_seconds=None,
            )
            self._offset, event = document
            if event is None:
                continue
            transformed = self._transform(event)
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
        self._closed = True

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


def parse_resume_token(token: dict[str, object]) -> int:
    raw = token.get("_data")
    if not isinstance(raw, str):
        raise OperationFailure("resume token must contain a string _data field")
    if raw.isdigit():
        return int(raw)
    padding = "=" * (-len(raw) % 4)
    try:
        payload = base64.urlsafe_b64decode((raw + padding).encode("ascii"))
        document = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise OperationFailure("resume token must contain a valid _data field") from exc
    token_value = document.get("t")
    if document.get("v") != 1 or not isinstance(token_value, int) or isinstance(token_value, bool):
        raise OperationFailure("resume token must contain a valid _data field")
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
        if not isinstance(start_at_operation_time, int) or isinstance(start_at_operation_time, bool) or start_at_operation_time < 0:
            raise TypeError("start_at_operation_time must be a non-negative integer")
        return hub.offset_at_or_after_cluster_time(start_at_operation_time)
    return hub.current_offset()
