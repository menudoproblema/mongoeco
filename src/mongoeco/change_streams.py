from __future__ import annotations

import asyncio
import base64
import json
import os
import threading
import time
from dataclasses import dataclass

from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventDocument, ChangeEventSnapshot, Document

_ALLOWED_CHANGE_STREAM_STAGES = frozenset(
    {"$match", "$project", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith"}
)
_ALLOWED_FULL_DOCUMENT_MODES = frozenset({"default", "updateLookup", "whenAvailable", "required"})


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
    def __init__(
        self,
        *,
        max_retained_events: int | None = 10_000,
        journal_path: str | None = None,
    ) -> None:
        if (
            max_retained_events is not None
            and (
                not isinstance(max_retained_events, int)
                or isinstance(max_retained_events, bool)
                or max_retained_events <= 0
            )
        ):
            raise TypeError("max_retained_events must be a positive integer or None")
        if journal_path is not None and (not isinstance(journal_path, str) or not journal_path):
            raise TypeError("journal_path must be a non-empty string or None")
        self._condition = threading.Condition()
        self._events: list[ChangeEventSnapshot] = []
        self._base_offset = 0
        self._next_token = 1
        self._max_retained_events = max_retained_events
        self._journal_path = journal_path
        self._journal_event_log_path = (
            None if journal_path is None else f"{journal_path}.events"
        )
        self._journal_log_entries_since_compaction = 0
        self._load_journal()

    def _end_offset_locked(self) -> int:
        return self._base_offset + len(self._events)

    def _retained_start_token_locked(self) -> int | None:
        if not self._events:
            return None
        return self._events[0].token

    def _prune_locked(self) -> bool:
        if self._max_retained_events is None:
            return False
        excess = len(self._events) - self._max_retained_events
        if excess <= 0:
            return False
        del self._events[:excess]
        self._base_offset += excess
        return True

    def _journal_document_locked(self) -> dict[str, object]:
        return {
            "version": 1,
            "base_offset": self._base_offset,
            "next_token": self._next_token,
            "events": [_snapshot_to_document(event) for event in self._events],
        }

    def _persist_locked(self) -> None:
        if self._journal_path is None:
            return
        journal_dir = os.path.dirname(self._journal_path)
        if journal_dir:
            os.makedirs(journal_dir, exist_ok=True)
        payload = json.dumps(
            self._journal_document_locked(),
            separators=(",", ":"),
            ensure_ascii=True,
            sort_keys=True,
        )
        temp_path = f"{self._journal_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(temp_path, self._journal_path)

    def _append_journal_event_locked(self, event: ChangeEventSnapshot) -> None:
        if self._journal_event_log_path is None:
            return
        journal_dir = os.path.dirname(self._journal_event_log_path)
        if journal_dir:
            os.makedirs(journal_dir, exist_ok=True)
        with open(self._journal_event_log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(_snapshot_to_document(event), separators=(",", ":"), sort_keys=True))
            handle.write("\n")
        self._journal_log_entries_since_compaction += 1

    def _compaction_threshold(self) -> int:
        if self._max_retained_events is None:
            return 256
        return max(32, self._max_retained_events)

    def _compact_locked(self) -> None:
        if self._journal_path is None:
            return
        self._persist_locked()
        if self._journal_event_log_path is not None:
            try:
                os.remove(self._journal_event_log_path)
            except FileNotFoundError:
                pass
        self._journal_log_entries_since_compaction = 0

    def _load_journal(self) -> None:
        if self._journal_path is not None and os.path.exists(self._journal_path):
            try:
                with open(self._journal_path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except Exception as exc:  # noqa: BLE001
                raise OperationFailure("change stream journal could not be loaded") from exc
            if not isinstance(payload, dict) or payload.get("version") != 1:
                raise OperationFailure("change stream journal could not be loaded")
            base_offset = payload.get("base_offset")
            next_token = payload.get("next_token")
            raw_events = payload.get("events")
            if (
                not isinstance(base_offset, int)
                or isinstance(base_offset, bool)
                or base_offset < 0
                or not isinstance(next_token, int)
                or isinstance(next_token, bool)
                or next_token < 1
                or not isinstance(raw_events, list)
            ):
                raise OperationFailure("change stream journal could not be loaded")
            self._events = [_snapshot_from_document(item) for item in raw_events]
            self._base_offset = base_offset
            self._next_token = next_token
            self._prune_locked()
        if self._journal_event_log_path is None or not os.path.exists(self._journal_event_log_path):
            return
        try:
            with open(self._journal_event_log_path, "r", encoding="utf-8") as handle:
                lines = handle.readlines()
        except Exception as exc:  # noqa: BLE001
            raise OperationFailure("change stream journal could not be loaded") from exc
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = _snapshot_from_document(json.loads(line))
            except Exception as exc:  # noqa: BLE001
                raise OperationFailure("change stream journal could not be loaded") from exc
            if event.token < self._next_token:
                continue
            self._events.append(event)
            self._next_token = event.token + 1
        self._prune_locked()
        self._journal_log_entries_since_compaction = sum(1 for raw_line in lines if raw_line.strip())

    @property
    def journal_path(self) -> str | None:
        return self._journal_path

    def compact_journal(self) -> None:
        with self._condition:
            self._compact_locked()

    def current_offset(self) -> int:
        with self._condition:
            return self._end_offset_locked()

    def offset_after_token(self, token: int) -> int:
        with self._condition:
            retained_start = self._retained_start_token_locked()
            if retained_start is not None and token < retained_start and self._base_offset > 0:
                raise OperationFailure("resume token is no longer available in retained change stream history")
            for index, event in enumerate(self._events):
                if event.token > token:
                    return self._base_offset + index
            return self._end_offset_locked()

    def offset_at_or_after_cluster_time(self, cluster_time: int) -> int:
        with self._condition:
            retained_start = self._retained_start_token_locked()
            if retained_start is not None and cluster_time < retained_start and self._base_offset > 0:
                raise OperationFailure("start_at_operation_time is no longer available in retained change stream history")
            for index, event in enumerate(self._events):
                if event.token >= cluster_time:
                    return self._base_offset + index
            return self._end_offset_locked()

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
            event = ChangeEventSnapshot(
                token=self._next_token,
                operation_type=operation_type,
                db_name=db_name,
                coll_name=coll_name,
                document_key=document_key,
                full_document=full_document,
                update_description=update_description,
            )
            self._events.append(event)
            self._next_token += 1
            pruned = self._prune_locked()
            self._append_journal_event_locked(event)
            if pruned or self._journal_log_entries_since_compaction >= self._compaction_threshold():
                self._compact_locked()
            self._condition.notify_all()

    def wait_for_event(
        self,
        offset: int,
        *,
        timeout_seconds: float | None,
    ) -> tuple[int, ChangeEventSnapshot | None]:
        with self._condition:
            if offset < self._base_offset:
                raise OperationFailure("change stream history is no longer available")
            if timeout_seconds is None:
                while self._end_offset_locked() <= offset:
                    self._condition.wait()
            else:
                deadline = time.monotonic() + timeout_seconds
                while self._end_offset_locked() <= offset:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return offset, None
                    self._condition.wait(remaining)
            if offset < self._base_offset:
                raise OperationFailure("change stream history is no longer available")
            if self._end_offset_locked() <= offset:
                return offset, None
            return offset + 1, self._events[offset - self._base_offset]


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
        full_document: str = "default",
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
        self._full_document = _normalize_full_document_mode(full_document)
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationFailure("cannot use change stream after it has been closed")

    def _transform(self, event: ChangeEventSnapshot) -> ChangeEventDocument | None:
        if not self._scope.matches(event):
            return None
        document = event.to_document()
        document = _apply_full_document_mode(document, self._full_document)
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


def _normalize_full_document_mode(mode: str) -> str:
    if not isinstance(mode, str):
        raise TypeError("full_document must be a string")
    if mode not in _ALLOWED_FULL_DOCUMENT_MODES:
        raise OperationFailure("full_document must be one of default, updateLookup, whenAvailable, or required")
    return mode


def _apply_full_document_mode(document: ChangeEventDocument, mode: str) -> ChangeEventDocument:
    if document.get("operationType") != "update":
        return document
    normalized = dict(document)
    full_document = normalized.get("fullDocument")
    if mode == "default":
        normalized.pop("fullDocument", None)
        return normalized
    if mode in {"updateLookup", "whenAvailable"}:
        return normalized
    if full_document is None:
        raise OperationFailure("change stream fullDocument is required but was not available")
    return normalized


def _snapshot_to_document(snapshot: ChangeEventSnapshot) -> dict[str, object]:
    document: dict[str, object] = {
        "token": snapshot.token,
        "operation_type": snapshot.operation_type,
        "db_name": snapshot.db_name,
        "coll_name": snapshot.coll_name,
        "document_key": snapshot.document_key,
    }
    if snapshot.full_document is not None:
        document["full_document"] = snapshot.full_document
    if snapshot.update_description is not None:
        document["update_description"] = snapshot.update_description
    return document


def _snapshot_from_document(document: object) -> ChangeEventSnapshot:
    if not isinstance(document, dict):
        raise OperationFailure("change stream journal could not be loaded")
    token = document.get("token")
    operation_type = document.get("operation_type")
    db_name = document.get("db_name")
    coll_name = document.get("coll_name")
    document_key = document.get("document_key")
    update_description = document.get("update_description")
    if (
        not isinstance(token, int)
        or isinstance(token, bool)
        or token < 1
        or not isinstance(operation_type, str)
        or not isinstance(db_name, str)
        or not isinstance(coll_name, str)
        or not isinstance(document_key, dict)
        or (update_description is not None and not isinstance(update_description, dict))
    ):
        raise OperationFailure("change stream journal could not be loaded")
    full_document = document.get("full_document")
    if full_document is not None and not isinstance(full_document, dict):
        raise OperationFailure("change stream journal could not be loaded")
    return ChangeEventSnapshot(
        token=token,
        operation_type=operation_type,
        db_name=db_name,
        coll_name=coll_name,
        document_key=document_key,
        full_document=full_document,
        update_description=update_description,
    )
