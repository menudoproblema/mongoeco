from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.core.operation_context import (
    ChangeOperationType,
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.engines.results import CommittedChange
from mongoeco.errors import OperationFailure


if TYPE_CHECKING:
    import sqlite3

    from collections.abc import Callable

    from mongoeco.types import Document


def ensure_change_outbox_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS change_outbox (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('event', 'gap')),
            payload TEXT,
            UNIQUE (operation_id, event_index)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS change_outbox_state (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            pruned_through INTEGER NOT NULL DEFAULT 0
        )
        """,
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO change_outbox_state (
            singleton, pruned_through
        ) VALUES (1, 0)
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS change_outbox_consumers (
            consumer_id TEXT PRIMARY KEY,
            checkpoint INTEGER NOT NULL,
            durable INTEGER NOT NULL CHECK (durable IN (0, 1)),
            updated_at_epoch REAL NOT NULL
        )
        """,
    )


def _max_sequence(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        'SELECT COALESCE(MAX(sequence), 0) FROM change_outbox',
    ).fetchone()
    persisted = 0 if row is None else int(row[0])
    state_row = conn.execute(
        'SELECT pruned_through FROM change_outbox_state WHERE singleton = 1',
    ).fetchone()
    pruned = 0 if state_row is None else int(state_row[0])
    return max(persisted, pruned)


def _pruned_through(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT pruned_through
        FROM change_outbox_state
        WHERE singleton = 1
        """,
    ).fetchone()
    return 0 if row is None else int(row[0])


def register_consumer(
    conn: sqlite3.Connection,
    consumer_id: str,
    *,
    initial_checkpoint: int | None,
    durable: bool,
) -> int:
    newest_sequence = _max_sequence(conn)
    if (
        initial_checkpoint is not None
        and initial_checkpoint > newest_sequence
    ):
        message = (
            f'change consumer {consumer_id!r} checkpoint '
            f'{initial_checkpoint} is ahead of committed sequence '
            f'{newest_sequence}'
        )
        raise OperationFailure(message)
    row = conn.execute(
        """
        SELECT checkpoint, durable
        FROM change_outbox_consumers
        WHERE consumer_id = ?
        """,
        (consumer_id,),
    ).fetchone()
    if row is None:
        checkpoint = (
            newest_sequence
            if initial_checkpoint is None
            else initial_checkpoint
        )
        effective_durable = durable
    else:
        checkpoint = int(row[0])
        if initial_checkpoint is not None:
            checkpoint = max(checkpoint, initial_checkpoint)
        effective_durable = bool(row[1]) or durable
    floor = _pruned_through(conn)
    if checkpoint < floor:
        message = (
            f'change consumer {consumer_id!r} checkpoint {checkpoint} is '
            f'behind pruned sequence {floor}'
        )
        raise OperationFailure(message)
    conn.execute(
        """
        INSERT INTO change_outbox_consumers (
            consumer_id, checkpoint, durable, updated_at_epoch
        ) VALUES (?, ?, ?, CAST(strftime('%s', 'now') AS REAL))
        ON CONFLICT(consumer_id) DO UPDATE SET
            checkpoint = excluded.checkpoint,
            durable = excluded.durable,
            updated_at_epoch = excluded.updated_at_epoch
        """,
        (consumer_id, checkpoint, int(effective_durable)),
    )
    return checkpoint


def consumer_checkpoint(
    conn: sqlite3.Connection,
    consumer_id: str,
) -> int:
    row = conn.execute(
        """
        SELECT checkpoint
        FROM change_outbox_consumers
        WHERE consumer_id = ?
        """,
        (consumer_id,),
    ).fetchone()
    if row is None:
        message = 'change consumer must be registered before writes'
        raise RuntimeError(message)
    checkpoint = int(row[0])
    floor = _pruned_through(conn)
    if checkpoint < floor:
        message = (
            f'change history after sequence {checkpoint} is no longer '
            f'available; pruned through {floor}'
        )
        raise OperationFailure(message)
    return checkpoint


def checkpoint_consumer(
    conn: sqlite3.Connection,
    consumer_id: str,
    sequence: int,
) -> None:
    cursor = conn.execute(
        """
        UPDATE change_outbox_consumers
        SET checkpoint = MAX(checkpoint, ?),
            updated_at_epoch = CAST(strftime('%s', 'now') AS REAL)
        WHERE consumer_id = ?
        """,
        (sequence, consumer_id),
    )
    if cursor.rowcount == 0:
        message = 'change consumer must be registered before checkpointing'
        raise RuntimeError(message)


def unregister_consumer(
    conn: sqlite3.Connection,
    consumer_id: str,
    *,
    include_durable: bool,
) -> None:
    if include_durable:
        conn.execute(
            'DELETE FROM change_outbox_consumers WHERE consumer_id = ?',
            (consumer_id,),
        )
        return
    conn.execute(
        """
        DELETE FROM change_outbox_consumers
        WHERE consumer_id = ? AND durable = 0
        """,
        (consumer_id,),
    )


def compact_change_outbox(
    conn: sqlite3.Connection,
    *,
    max_entries: int,
) -> int:
    max_sequence = _max_sequence(conn)
    row = conn.execute(
        'SELECT MIN(checkpoint) FROM change_outbox_consumers',
    ).fetchone()
    acknowledged_floor = (
        max_sequence
        if row is None or row[0] is None
        else int(row[0])
    )
    capacity_floor = max(0, max_sequence - max_entries)
    target = max(_pruned_through(conn), acknowledged_floor, capacity_floor)
    if target <= 0:
        return 0
    cursor = conn.execute(
        'DELETE FROM change_outbox WHERE sequence <= ?',
        (target,),
    )
    conn.execute(
        """
        UPDATE change_outbox_state
        SET pruned_through = MAX(pruned_through, ?)
        WHERE singleton = 1
        """,
        (target,),
    )
    return max(0, cursor.rowcount)


def outbox_info(conn: sqlite3.Connection) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(MIN(sequence), 0),
            COALESCE(MAX(sequence), 0)
        FROM change_outbox
        """,
    ).fetchone()
    consumer_row = conn.execute(
        """
        SELECT COUNT(*), COALESCE(MIN(checkpoint), 0)
        FROM change_outbox_consumers
        """,
    ).fetchone()
    pruned_through = _pruned_through(conn)
    return {
        'pendingEntries': int(row[0]),
        'oldestSequence': int(row[1]),
        'newestSequence': max(int(row[2]), pruned_through),
        'prunedThrough': pruned_through,
        'consumerCount': int(consumer_row[0]),
        'minimumCheckpoint': int(consumer_row[1]),
    }


def append_change(  # noqa: PLR0913 - transactional outbox boundary
    conn: sqlite3.Connection,
    *,
    context: OperationContext | None,
    event_index: int,
    operation_type: ChangeOperationType,
    db_name: str,
    coll_name: str,
    document_key: Document,
    full_document: Document | None,
    serialize_document: Callable[[Document], str],
    max_entries: int = 10_000,
) -> int | None:
    if (
        context is None
        or context.publication is ChangePublicationPolicy.DISABLED
    ):
        return None
    kind = (
        'gap'
        if context.publication is ChangePublicationPolicy.RECORD_GAP
        else 'event'
    )
    payload = None
    if kind == 'event':
        payload = serialize_document(
            {
                'operation_type': operation_type,
                'db_name': db_name,
                'coll_name': coll_name,
                'document_key': document_key,
                'full_document': full_document,
                'update_description': None,
            },
        )
    existing = conn.execute(
        """
        SELECT sequence
        FROM change_outbox
        WHERE operation_id = ? AND event_index = ?
        """,
        (context.operation_id, event_index),
    ).fetchone()
    if existing is not None:
        return int(existing[0])
    cursor = conn.execute(
        """
        INSERT INTO change_outbox (
            operation_id, event_index, kind, payload
        ) VALUES (?, ?, ?, ?)
        """,
        (context.operation_id, event_index, kind, payload),
    )
    if cursor.lastrowid is None:
        message = 'change outbox row was not persisted'
        raise RuntimeError(message)
    compact_change_outbox(conn, max_entries=max_entries)
    return int(cursor.lastrowid)


def read_committed_changes(
    conn: sqlite3.Connection,
    *,
    after_sequence: int,
    deserialize_document: Callable[[str], Document],
    limit: int = 1_000,
) -> tuple[CommittedChange, ...]:
    floor = _pruned_through(conn)
    if after_sequence < floor:
        message = (
            f'change history after sequence {after_sequence} is no longer '
            f'available; pruned through {floor}'
        )
        raise OperationFailure(message)
    rows = conn.execute(
        """
        SELECT sequence, kind, payload
        FROM change_outbox
        WHERE sequence > ?
        ORDER BY sequence
        LIMIT ?
        """,
        (after_sequence, limit),
    ).fetchall()
    return tuple(
        CommittedChange(
            sequence=int(sequence),
            payload=(
                None
                if kind == 'gap'
                else deserialize_document(payload)
            ),
        )
        for sequence, kind, payload in rows
    )
