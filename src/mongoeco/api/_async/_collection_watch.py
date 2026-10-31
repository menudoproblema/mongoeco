from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from mongoeco.api.operations import compile_find_operation
from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
)
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
    full_document: str | None,
    session: object | None,
) -> AsyncChangeStreamCursor:
    if session is not None:
        raise InvalidOperation("watch does not support explicit sessions")
    return AsyncChangeStreamCursor(
        collection._change_hub,
        scope=ChangeStreamScope(db_name=collection._db_name, coll_name=collection._collection_name),
        pipeline=pipeline,
        max_await_time_ms=max_await_time_ms,
        resume_after=resume_after,
        start_after=start_after,
        start_at_operation_time=start_at_operation_time,
        full_document=full_document,
        materialize_document=collection._apply_codec_options_to_document,
        dialect=collection._mongodb_dialect,
        execution_context=collection._new_execution_context(),
        lookup_full_document=lambda event: lookup_change_document(collection, event),
    )


async def lookup_change_document(collection, event):
    # A fresh read resource at consumption time; the stream's expression clock
    # remains fixed. Scan internally so BSON widths/undefined reach its pipeline
    # before codec_options and public materialization are applied once.
    operation = compile_find_operation(
        event.document_key,
        limit=1,
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    ).bind(collection._new_operation_context())
    async with collection._runtime.engine_scan_with_operation(operation) as snapshot:
        return await anext(snapshot, None)
