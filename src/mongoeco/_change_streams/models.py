from __future__ import annotations

import os
from dataclasses import dataclass

from mongoeco.types import ChangeEventSnapshot


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


@dataclass(frozen=True, slots=True)
class ChangeStreamHubState:
    retained_events: int
    base_offset: int
    current_offset: int
    next_token: int
    retained_start_token: int | None
    retained_end_token: int | None
    journal_enabled: bool
    journal_path: str | None
    journal_event_log_path: str | None
    journal_fsync: bool
    journal_max_log_bytes: int | None
    journal_log_entries_since_compaction: int
    journal_log_bytes_since_compaction: int
    journal_compaction_count: int
    last_compaction_time_monotonic: float | None
    snapshot_exists: bool
    event_log_exists: bool

    def to_document(self) -> dict[str, object]:
        return {
            "retainedEvents": self.retained_events,
            "baseOffset": self.base_offset,
            "currentOffset": self.current_offset,
            "nextToken": self.next_token,
            "retainedStartToken": self.retained_start_token,
            "retainedEndToken": self.retained_end_token,
            "journalEnabled": self.journal_enabled,
            "journalPath": self.journal_path,
            "journalEventLogPath": self.journal_event_log_path,
            "journalFsync": self.journal_fsync,
            "journalMaxLogBytes": self.journal_max_log_bytes,
            "journalLogEntriesSinceCompaction": self.journal_log_entries_since_compaction,
            "journalLogBytesSinceCompaction": self.journal_log_bytes_since_compaction,
            "journalCompactionCount": self.journal_compaction_count,
            "lastCompactionTimeMonotonic": self.last_compaction_time_monotonic,
            "snapshotExists": self.snapshot_exists,
            "eventLogExists": self.event_log_exists,
        }


@dataclass(frozen=True, slots=True)
class ChangeStreamBackendInfo:
    implementation: str
    distributed: bool
    persistent: bool
    resumable: bool
    resumable_across_client_restarts: bool
    resumable_across_processes: bool
    resumable_across_nodes: bool
    bounded_history: bool
    max_retained_events: int | None
    journal_enabled: bool
    journal_fsync: bool
    journal_max_log_bytes: int | None

    def to_document(self) -> dict[str, object]:
        return {
            "implementation": self.implementation,
            "distributed": self.distributed,
            "persistent": self.persistent,
            "resumable": self.resumable,
            "resumableAcrossClientRestarts": self.resumable_across_client_restarts,
            "resumableAcrossProcesses": self.resumable_across_processes,
            "resumableAcrossNodes": self.resumable_across_nodes,
            "boundedHistory": self.bounded_history,
            "maxRetainedEvents": self.max_retained_events,
            "journalEnabled": self.journal_enabled,
            "journalFsync": self.journal_fsync,
            "journalMaxLogBytes": self.journal_max_log_bytes,
        }


def build_hub_state_document(
    *,
    retained_events: int,
    base_offset: int,
    current_offset: int,
    next_token: int,
    retained_start_token: int | None,
    retained_end_token: int | None,
    journal_enabled: bool,
    journal_path: str | None,
    journal_event_log_path: str | None,
    journal_fsync: bool,
    journal_max_log_bytes: int | None,
    journal_log_entries_since_compaction: int,
    journal_log_bytes_since_compaction: int,
    journal_compaction_count: int,
    last_compaction_time_monotonic: float | None,
) -> ChangeStreamHubState:
    return ChangeStreamHubState(
        retained_events=retained_events,
        base_offset=base_offset,
        current_offset=current_offset,
        next_token=next_token,
        retained_start_token=retained_start_token,
        retained_end_token=retained_end_token,
        journal_enabled=journal_enabled,
        journal_path=journal_path,
        journal_event_log_path=journal_event_log_path,
        journal_fsync=journal_fsync,
        journal_max_log_bytes=journal_max_log_bytes,
        journal_log_entries_since_compaction=journal_log_entries_since_compaction,
        journal_log_bytes_since_compaction=journal_log_bytes_since_compaction,
        journal_compaction_count=journal_compaction_count,
        last_compaction_time_monotonic=last_compaction_time_monotonic,
        snapshot_exists=(journal_path is not None and os.path.exists(journal_path)),
        event_log_exists=(
            journal_event_log_path is not None and os.path.exists(journal_event_log_path)
        ),
    )
