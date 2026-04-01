from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from mongoeco.change_streams import AsyncChangeStreamCursor, ChangeStreamHub, ChangeStreamScope
from mongoeco.errors import InvalidOperation

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection


@dataclass(frozen=True, slots=True)
class CollectionChangeStreamConfig:
    history_size: int | None
    journal_path: str | None
    journal_fsync: bool
    journal_max_bytes: int | None


def create_change_stream_hub(
    *,
    change_hub: ChangeStreamHub | None,
    config: CollectionChangeStreamConfig,
) -> ChangeStreamHub:
    if change_hub is not None:
        return change_hub
    return ChangeStreamHub(
        max_retained_events=config.history_size,
        journal_path=config.journal_path,
        journal_fsync=config.journal_fsync,
        journal_max_log_bytes=config.journal_max_bytes,
    )


def open_collection_change_stream(
    collection: AsyncCollection,
    *,
    pipeline: object | None,
    max_await_time_ms: int | None,
    resume_after: dict[str, object] | None,
    start_after: dict[str, object] | None,
    start_at_operation_time: int | None,
    full_document: str,
    session: object | None,
) -> AsyncChangeStreamCursor:
    if session is not None:
        raise InvalidOperation("watch does not support explicit sessions")
    if max_await_time_ms is not None and (
        not isinstance(max_await_time_ms, int)
        or isinstance(max_await_time_ms, bool)
        or max_await_time_ms < 0
    ):
        raise TypeError("max_await_time_ms must be a non-negative integer")
    return AsyncChangeStreamCursor(
        collection._change_hub,
        scope=ChangeStreamScope(db_name=collection._db_name, coll_name=collection._collection_name),
        pipeline=pipeline,
        max_await_time_ms=max_await_time_ms,
        resume_after=resume_after,
        start_after=start_after,
        start_at_operation_time=start_at_operation_time,
        full_document=full_document,
    )
