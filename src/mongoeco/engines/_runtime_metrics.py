from __future__ import annotations

import threading

from collections import Counter


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
        self._planner_modes: Counter[str] = Counter()
        self._planner_fallbacks: Counter[str] = Counter()

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

    def record_planner(
        self,
        mode: str,
        *,
        fallback_reason: str | None = None,
    ) -> None:
        if not isinstance(mode, str) or not mode:
            return
        with self._lock:
            self._planner_modes[mode] += 1
            if fallback_reason:
                self._planner_fallbacks[fallback_reason] += 1

    def planner_snapshot(self) -> dict[str, dict[str, int]]:
        with self._lock:
            return {
                "modes": dict(self._planner_modes),
                "fallbacks": dict(self._planner_fallbacks),
            }
