from __future__ import annotations

import json
import os
import threading
import time

from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventSnapshot, Document

from .journal import (
    fsync_parent_directory,
    journal_event_entry,
    journal_payload,
    snapshot_from_document,
    snapshot_from_log_entry,
)
from .models import (
    ChangeStreamBackendInfo,
    ChangeStreamHubState,
    build_hub_state_document,
)


class ChangeStreamHub:
    def __init__(
        self,
        *,
        max_retained_events: int | None = 10_000,
        journal_path: str | None = None,
        journal_fsync: bool = False,
        journal_max_log_bytes: int | None = 1_048_576,
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
        if not isinstance(journal_fsync, bool):
            raise TypeError("journal_fsync must be a boolean")
        if (
            journal_max_log_bytes is not None
            and (
                not isinstance(journal_max_log_bytes, int)
                or isinstance(journal_max_log_bytes, bool)
                or journal_max_log_bytes <= 0
            )
        ):
            raise TypeError("journal_max_log_bytes must be a positive integer or None")
        self._condition = threading.Condition()
        self._events: list[ChangeEventSnapshot] = []
        self._base_offset = 0
        self._next_token = 1
        self._max_retained_events = max_retained_events
        self._journal_path = journal_path
        self._journal_fsync = journal_fsync
        self._journal_max_log_bytes = journal_max_log_bytes
        self._journal_event_log_path = None if journal_path is None else f"{journal_path}.events"
        self._journal_log_entries_since_compaction = 0
        self._journal_log_bytes_since_compaction = 0
        self._journal_compaction_count = 0
        self._last_compaction_time_monotonic: float | None = None
        self._load_journal()

    @property
    def backend_info(self) -> ChangeStreamBackendInfo:
        return ChangeStreamBackendInfo(
            implementation="local",
            distributed=False,
            persistent=self._journal_path is not None,
            resumable=True,
            resumable_across_client_restarts=self._journal_path is not None,
            resumable_across_processes=self._journal_path is not None,
            resumable_across_nodes=False,
            bounded_history=self._max_retained_events is not None,
            max_retained_events=self._max_retained_events,
            journal_enabled=self._journal_path is not None,
            journal_fsync=self._journal_fsync,
            journal_max_log_bytes=self._journal_max_log_bytes,
        )

    @property
    def journal_path(self) -> str | None:
        return self._journal_path

    @property
    def journal_fsync(self) -> bool:
        return self._journal_fsync

    @property
    def journal_max_log_bytes(self) -> int | None:
        return self._journal_max_log_bytes

    @property
    def state(self) -> ChangeStreamHubState:
        with self._condition:
            return build_hub_state_document(
                retained_events=len(self._events),
                base_offset=self._base_offset,
                current_offset=self._end_offset_locked(),
                next_token=self._next_token,
                retained_start_token=self._retained_start_token_locked(),
                retained_end_token=None if not self._events else self._events[-1].token,
                journal_enabled=self._journal_path is not None,
                journal_path=self._journal_path,
                journal_event_log_path=self._journal_event_log_path,
                journal_fsync=self._journal_fsync,
                journal_max_log_bytes=self._journal_max_log_bytes,
                journal_log_entries_since_compaction=self._journal_log_entries_since_compaction,
                journal_log_bytes_since_compaction=self._journal_log_bytes_since_compaction,
                journal_compaction_count=self._journal_compaction_count,
                last_compaction_time_monotonic=self._last_compaction_time_monotonic,
            )

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
            if (
                pruned
                or self._journal_log_entries_since_compaction >= self._compaction_threshold()
                or (
                    self._journal_max_log_bytes is not None
                    and self._journal_log_bytes_since_compaction >= self._journal_max_log_bytes
                )
            ):
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

    def _persist_locked(self) -> None:
        if self._journal_path is None:
            return
        journal_dir = os.path.dirname(self._journal_path)
        if journal_dir:
            os.makedirs(journal_dir, exist_ok=True)
        payload = journal_payload(
            base_offset=self._base_offset,
            next_token=self._next_token,
            events=self._events,
        )
        temp_path = f"{self._journal_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            handle.write(payload)
            if self._journal_fsync:
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(temp_path, self._journal_path)
        if self._journal_fsync:
            fsync_parent_directory(self._journal_path)

    def _append_journal_event_locked(self, event: ChangeEventSnapshot) -> None:
        if self._journal_event_log_path is None:
            return
        journal_dir = os.path.dirname(self._journal_event_log_path)
        if journal_dir:
            os.makedirs(journal_dir, exist_ok=True)
        payload = journal_event_entry(event)
        with open(self._journal_event_log_path, "a", encoding="utf-8") as handle:
            handle.write(payload)
            handle.write("\n")
            if self._journal_fsync:
                handle.flush()
                os.fsync(handle.fileno())
        if self._journal_fsync:
            fsync_parent_directory(self._journal_event_log_path)
        self._journal_log_entries_since_compaction += 1
        self._journal_log_bytes_since_compaction += len(payload.encode("utf-8")) + 1

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
            else:
                if self._journal_fsync:
                    fsync_parent_directory(self._journal_event_log_path)
        self._journal_log_entries_since_compaction = 0
        self._journal_log_bytes_since_compaction = 0
        self._journal_compaction_count += 1
        self._last_compaction_time_monotonic = time.monotonic()

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
            self._events = [snapshot_from_document(item) for item in raw_events]
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
        for index, raw_line in enumerate(lines):
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = snapshot_from_log_entry(json.loads(line))
            except Exception as exc:  # noqa: BLE001
                if index == len(lines) - 1 and not raw_line.endswith("\n"):
                    break
                raise OperationFailure("change stream journal could not be loaded") from exc
            if event.token < self._next_token:
                continue
            self._events.append(event)
            self._next_token = event.token + 1
        self._prune_locked()
        self._journal_log_entries_since_compaction = sum(1 for raw_line in lines if raw_line.strip())
        self._journal_log_bytes_since_compaction = os.path.getsize(self._journal_event_log_path)
