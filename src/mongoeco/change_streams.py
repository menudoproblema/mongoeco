from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass

from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventDocument, ChangeEventSnapshot, Document

_ALLOWED_CHANGE_STREAM_STAGES = frozenset(
    {"$match", "$project", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith"}
)


@dataclass(frozen=True, slots=True)
class ChangeStreamScope:
    db_name: str | None = None
    coll_name: str | None = None

    def matches(self, event: ChangeEventSnapshot) -> bool:
        if self.db_name is not None and event.db_name != self.db_name:
            return False
        if self.coll_name is not None and event.coll_name != self.coll_name:
            return False
        return True


class ChangeStreamHub:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._events: list[ChangeEventSnapshot] = []
        self._next_token = 1

    def current_offset(self) -> int:
        with self._condition:
            return len(self._events)

    def offset_after_token(self, token: int) -> int:
        with self._condition:
            for index, event in enumerate(self._events):
                if event.token > token:
                    return index
            return len(self._events)

    def offset_at_or_after_cluster_time(self, cluster_time: int) -> int:
        with self._condition:
            for index, event in enumerate(self._events):
                if event.token >= cluster_time:
                    return index
            return len(self._events)

    def publish(
        self,
        *,
        operation_type: str,
        db_name: str,
        coll_name: str,
        document_key: Document,
        full_document: Document | None = None,
        update_description: dict[str, object] | None = None,
    ) -> None:
        with self._condition:
            self._events.append(
                ChangeEventSnapshot(
                    token=self._next_token,
                    operation_type=operation_type,
                    db_name=db_name,
                    coll_name=coll_name,
                    document_key=document_key,
                    full_document=full_document,
                    update_description=update_description,
                )
            )
            self._next_token += 1
            self._condition.notify_all()

    def wait_for_event(
        self,
        offset: int,
        *,
        timeout_seconds: float | None,
    ) -> tuple[int, ChangeEventSnapshot | None]:
        with self._condition:
            if timeout_seconds is None:
                while len(self._events) <= offset:
                    self._condition.wait()
            else:
                deadline = time.monotonic() + timeout_seconds
                while len(self._events) <= offset:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return offset, None
                    self._condition.wait(remaining)
            if len(self._events) <= offset:
                return offset, None
            return offset + 1, self._events[offset]


def compile_change_stream_pipeline(
    pipeline: object | None,
) -> list[dict[str, object]] | None:
    if pipeline is None:
        return None
    if not isinstance(pipeline, list):
        raise TypeError("pipeline must be a list of stages")
    normalized: list[dict[str, object]] = []
    for stage in pipeline:
        if not isinstance(stage, dict) or len(stage) != 1:
            raise TypeError("pipeline stages must be single-key dicts")
        operator, spec = next(iter(stage.items()))
        if operator not in _ALLOWED_CHANGE_STREAM_STAGES:
            raise OperationFailure(
                "watch only supports $match, $project, $addFields, $set, $unset, $replaceRoot, and $replaceWith stages"
            )
        if operator in {"$match", "$project", "$addFields", "$set", "$replaceRoot", "$replaceWith"} and not isinstance(spec, dict):
            raise TypeError(f"{operator} stage must be a dict")
        normalized.append(stage)
    return normalized


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
        ) -> None:
        self._hub = hub
        self._scope = scope
        self._offset = _resolve_change_stream_offset(
            hub,
            resume_after=resume_after,
            start_after=start_after,
            start_at_operation_time=start_at_operation_time,
        )
        self._pipeline = compile_change_stream_pipeline(pipeline)
        self._max_await_time_ms = max_await_time_ms
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationFailure("cannot use change stream after it has been closed")

    def _transform(self, event: ChangeEventSnapshot) -> ChangeEventDocument | None:
        if not self._scope.matches(event):
            return None
        document = event.to_document()
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


def _resolve_change_stream_offset(
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
        return hub.offset_after_token(_parse_resume_token(resume_after))
    if start_after is not None:
        return hub.offset_after_token(_parse_resume_token(start_after))
    if start_at_operation_time is not None:
        if not isinstance(start_at_operation_time, int) or isinstance(start_at_operation_time, bool) or start_at_operation_time < 0:
            raise TypeError("start_at_operation_time must be a non-negative integer")
        return hub.offset_at_or_after_cluster_time(start_at_operation_time)
    return hub.current_offset()


def _parse_resume_token(token: dict[str, object]) -> int:
    raw = token.get("_data")
    if not isinstance(raw, str) or not raw.isdigit():
        raise OperationFailure("resume token must contain a numeric _data field")
    return int(raw)
