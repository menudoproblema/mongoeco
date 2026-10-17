"""Bounded, thread-safe wire metadata; never retain BSON or exception messages."""

from __future__ import annotations

import threading
import time

from collections import deque
from collections.abc import Mapping  # noqa: TC003 - runtime annotation introspection


MAX_CAPACITY = 10_000


def classify_result(result: Mapping[str, object]) -> tuple[str, int | None]:
    code = result.get("code")
    write_errors = result.get("writeErrors")
    concern = result.get("writeConcernError")
    if code is None and isinstance(write_errors, list) and write_errors:
        first = write_errors[0]
        if isinstance(first, dict):
            code = first.get("code")
    if code is None and isinstance(concern, dict):
        code = concern.get("code")
    failed = result.get("ok") == 0 or bool(write_errors) or isinstance(concern, dict)
    return (
        "error" if failed or code is not None else "completed",
        code if type(code) is int else None,
    )


class WireDiagnostics:
    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or not 0 <= capacity <= MAX_CAPACITY:
            message = "diagnostic_capacity must be an integer between 0 and 10000"
            raise ValueError(message)
        self.capacity = capacity
        self._events: deque[dict[str, object]] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._started = time.monotonic()
        self._total = 0
        self._resources: dict[str, object] = {}
        self._first_error: dict[str, object] | None = None

    def record(self, event: str, **metadata: object) -> None:
        if not self.capacity:
            return
        with self._lock:
            self._total += 1
            entry = {
                "sequence": self._total,
                "elapsed_ms": round((time.monotonic() - self._started) * 1000, 3),
                "event": event,
                **metadata,
            }
            if self._first_error is None and (
                metadata.get("outcome") == "error" or event == "handler_failed"
            ):
                self._first_error = dict(entry)
            self._events.append(entry)

    def resources(self, **counts: object) -> None:
        if self.capacity:
            with self._lock:
                self._resources = counts

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            snapshot = {
                "enabled": bool(self.capacity),
                "capacity": self.capacity,
                "dropped_events": self._total - len(self._events),
                "events": [dict(event) for event in self._events],
                "resources": dict(self._resources),
            }
            if self.capacity:
                snapshot["first_error"] = (
                    dict(self._first_error) if self._first_error is not None else None
                )
            return snapshot
