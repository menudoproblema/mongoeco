from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from mongoeco.session import ClientSession


@contextmanager
def track_active_operation(
    engine,
    *,
    command_name: str,
    operation_type: str,
    namespace: str | None,
    session: ClientSession | None = None,
    cursor_id: int | None = None,
    connection_id: int | None = None,
    comment: object | None = None,
    max_time_ms: int | None = None,
    killable: bool = True,
    metadata: dict[str, object] | None = None,
) -> Iterator[str | None]:
    begin = getattr(engine, "_begin_active_operation", None)
    if not callable(begin):
        yield None
        return
    record = begin(
        command_name=command_name,
        operation_type=operation_type,
        namespace=namespace,
        session_id=None if session is None else session.session_id,
        cursor_id=cursor_id,
        connection_id=connection_id,
        comment=comment,
        max_time_ms=max_time_ms,
        killable=killable,
        metadata=metadata,
    )
    try:
        yield record.opid
    finally:
        complete = getattr(engine, "_complete_active_operation", None)
        if callable(complete):
            complete(record.opid)

