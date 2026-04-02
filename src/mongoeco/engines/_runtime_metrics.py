from __future__ import annotations

import threading


_OPCOUNTER_KEYS = ("insert", "query", "update", "delete", "getmore", "command")
_PROFILE_TO_OPCOUNTER = {
    "insert": "insert",
    "update": "update",
    "remove": "delete",
    "delete": "delete",
    "command": "command",
    "query": "query",
    "getmore": "getmore",
}


class LocalRuntimeMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._opcounters = {key: 0 for key in _OPCOUNTER_KEYS}

    def record(self, operation: str, *, amount: int = 1) -> None:
        if amount <= 0:
            return
        opcounter = _PROFILE_TO_OPCOUNTER.get(operation)
        if opcounter is None:
            return
        with self._lock:
            self._opcounters[opcounter] += amount

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._opcounters)
