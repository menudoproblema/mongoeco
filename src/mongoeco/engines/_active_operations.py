from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ActiveOperationRecord:
    opid: str
    command_name: str
    namespace: str | None
    operation_type: str
    started_at_monotonic: float
    started_at_epoch: float
    session_id: str | None
    cursor_id: int | None
    connection_id: int | None
    comment: object | None
    max_time_ms: int | None
    metadata: dict[str, object]
    killable: bool
    task: asyncio.Task[object] | None
    cancelled: bool = False
    completed: bool = False

    def to_document(self) -> dict[str, object]:
        duration_millis = max(0.0, (time.monotonic() - self.started_at_monotonic) * 1000.0)
        document: dict[str, object] = {
            "opid": self.opid,
            "command": self.command_name,
            "type": self.operation_type,
            "secs_running": duration_millis / 1000.0,
            "millis_running": duration_millis,
            "startedAtEpoch": self.started_at_epoch,
            "killable": self.killable,
            "state": "cancelled" if self.cancelled else "completed" if self.completed else "running",
        }
        if self.namespace is not None:
            document["ns"] = self.namespace
        if self.session_id is not None:
            document["sessionId"] = self.session_id
        if self.cursor_id is not None:
            document["cursorId"] = self.cursor_id
        if self.connection_id is not None:
            document["connectionId"] = self.connection_id
        if self.comment is not None:
            document["comment"] = self.comment
        if self.max_time_ms is not None:
            document["maxTimeMS"] = self.max_time_ms
        if self.metadata:
            document["details"] = dict(self.metadata)
        return document


class LocalActiveOperationRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._operations: dict[str, ActiveOperationRecord] = {}

    def begin(
        self,
        *,
        command_name: str,
        operation_type: str,
        namespace: str | None,
        session_id: str | None = None,
        cursor_id: int | None = None,
        connection_id: int | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        killable: bool = True,
        metadata: dict[str, object] | None = None,
    ) -> ActiveOperationRecord:
        current_task: asyncio.Task[object] | None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        now_monotonic = time.monotonic()
        record = ActiveOperationRecord(
            opid=uuid.uuid4().hex,
            command_name=command_name,
            namespace=namespace,
            operation_type=operation_type,
            started_at_monotonic=now_monotonic,
            started_at_epoch=time.time(),
            session_id=session_id,
            cursor_id=cursor_id,
            connection_id=connection_id,
            comment=comment,
            max_time_ms=max_time_ms,
            metadata=dict(metadata or {}),
            killable=killable,
            task=current_task,
        )
        with self._lock:
            self._operations[record.opid] = record
        return record

    def complete(self, opid: str) -> None:
        with self._lock:
            record = self._operations.pop(opid, None)
        if record is not None:
            record.completed = True

    def snapshot(self) -> list[dict[str, object]]:
        with self._lock:
            records = list(self._operations.values())
        return [record.to_document() for record in records]

    def cancel(self, opid: str) -> bool:
        with self._lock:
            record = self._operations.get(opid)
        if record is None or not record.killable:
            return False
        record.cancelled = True
        if record.task is not None:
            record.task.cancel()
        return True

