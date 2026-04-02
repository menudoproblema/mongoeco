from __future__ import annotations

from copy import deepcopy
import datetime
from typing import Iterable

from mongoeco.types import (
    ExecutionLineageStep,
    ProfileEntrySnapshot,
    ProfilingCommandResult,
    ProfilingSettingsSnapshot,
)


class EngineProfiler:
    def __init__(self, engine_name: str) -> None:
        self._engine_name = engine_name
        self._settings: dict[str, ProfilingSettingsSnapshot] = {}
        self._entries: dict[str, list[ProfileEntrySnapshot]] = {}
        self._next_id = 1

    def get_settings(self, db_name: str) -> ProfilingSettingsSnapshot:
        return self._settings.get(db_name, ProfilingSettingsSnapshot())

    def set_level(
        self,
        db_name: str,
        level: int,
        *,
        slow_ms: int | None = None,
    ) -> ProfilingCommandResult:
        if level not in {-1, 0, 1, 2}:
            raise ValueError("profile level must be -1, 0, 1 or 2")
        current = self.get_settings(db_name)
        snapshot = self.status_snapshot()
        namespace_visible = self.namespace_visible(db_name)
        if level == -1:
            return ProfilingCommandResult(
                previous_level=current.level,
                slow_ms=current.slow_ms,
                current_level=current.level,
                entry_count=self.count_entries(db_name),
                namespace_visible=namespace_visible,
                tracked_databases=snapshot["tracked_databases"],
                visible_namespaces=snapshot["visible_namespaces"],
            )
        resolved_slow_ms = current.slow_ms if slow_ms is None else slow_ms
        if resolved_slow_ms < 0:
            raise ValueError("slowms must be >= 0")
        self._settings[db_name] = ProfilingSettingsSnapshot(
            level=level,
            slow_ms=resolved_slow_ms,
        )
        updated_snapshot = self.status_snapshot()
        return ProfilingCommandResult(
            previous_level=current.level,
            slow_ms=resolved_slow_ms,
            current_level=level,
            entry_count=self.count_entries(db_name),
            namespace_visible=self.namespace_visible(db_name),
            tracked_databases=updated_snapshot["tracked_databases"],
            visible_namespaces=updated_snapshot["visible_namespaces"],
        )

    def record(
        self,
        db_name: str,
        *,
        op: str,
        namespace: str,
        command: dict[str, object],
        duration_micros: int,
        execution_lineage: Iterable[ExecutionLineageStep] = (),
        fallback_reason: str | None = None,
        ok: float = 1.0,
        errmsg: str | None = None,
    ) -> None:
        settings = self.get_settings(db_name)
        if settings.level == 0:
            return
        if settings.level == 1 and duration_micros < settings.slow_ms * 1000:
            return
        entry = ProfileEntrySnapshot(
            profile_id=self._next_id,
            op=op,
            namespace=namespace,
            command=deepcopy(command),
            millis=duration_micros / 1000.0,
            micros=duration_micros,
            ts=datetime.datetime.now(datetime.UTC).isoformat(),
            engine=self._engine_name,
            execution_lineage=tuple(execution_lineage),
            fallback_reason=fallback_reason,
            ok=ok,
            errmsg=errmsg,
        )
        self._next_id += 1
        self._entries.setdefault(db_name, []).append(entry)

    def list_entries(self, db_name: str) -> list[dict[str, object]]:
        return [entry.to_document() for entry in self._entries.get(db_name, [])]

    def get_entry(self, db_name: str, profile_id: object) -> dict[str, object] | None:
        for entry in self._entries.get(db_name, []):
            if entry.profile_id == profile_id:
                return entry.to_document()
        return None

    def count_entries(self, db_name: str) -> int:
        return len(self._entries.get(db_name, []))

    def status_snapshot(self) -> dict[str, int]:
        tracked_names = set(self._settings) | set(self._entries)
        return {
            "tracked_databases": len(tracked_names),
            "visible_namespaces": sum(1 for db_name in tracked_names if self.namespace_visible(db_name)),
            "entry_count": sum(len(entries) for entries in self._entries.values()),
        }

    def clear(self, db_name: str) -> None:
        self._entries.pop(db_name, None)

    def namespace_visible(self, db_name: str) -> bool:
        settings = self.get_settings(db_name)
        return settings.level > 0 or self.count_entries(db_name) > 0
