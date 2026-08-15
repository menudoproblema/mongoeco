from __future__ import annotations

import hashlib

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


_CHANGE_OUTBOX_SCHEMA_VERSION = 4
_SCHEMA_SAVEPOINT = 'mongoeco_change_outbox_schema'


def ensure_change_outbox_schema(conn: sqlite3.Connection) -> None:
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute('BEGIN IMMEDIATE')
    else:
        conn.execute(f'SAVEPOINT {_SCHEMA_SAVEPOINT}')
    try:
        _run_change_outbox_migrations(conn)
    except BaseException:
        if owns_transaction:
            conn.rollback()
        else:
            conn.execute(f'ROLLBACK TO {_SCHEMA_SAVEPOINT}')
            conn.execute(f'RELEASE {_SCHEMA_SAVEPOINT}')
        raise
    if owns_transaction:
        conn.commit()
    else:
        conn.execute(f'RELEASE {_SCHEMA_SAVEPOINT}')


def _run_change_outbox_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mongoeco_schema_migrations (
            component TEXT PRIMARY KEY,
            version INTEGER NOT NULL
        )
        """,
    )
    row = conn.execute(
        "SELECT version FROM mongoeco_schema_migrations "
        "WHERE component = 'change_outbox'",
    ).fetchone()
    if row is None:
        version = _infer_legacy_schema_version(conn)
        conn.execute(
            'INSERT INTO mongoeco_schema_migrations (component, version) '
            "VALUES ('change_outbox', ?)",
            (version,),
        )
    else:
        version = int(row[0])
    if version < 0:
        message = 'change_outbox schema version cannot be negative'
        raise OperationFailure(message)
    if version > _CHANGE_OUTBOX_SCHEMA_VERSION:
        message = (
            f'change_outbox schema version {version} is newer than supported '
            f'version {_CHANGE_OUTBOX_SCHEMA_VERSION}'
        )
        raise OperationFailure(message)

    migrations = {
        1: _migrate_change_outbox_v1,
        2: _migrate_change_outbox_v2,
        3: _migrate_change_outbox_v3,
        4: _migrate_change_outbox_v4,
    }
    while version < _CHANGE_OUTBOX_SCHEMA_VERSION:
        target = version + 1
        migrations[target](conn)
        conn.execute(
            'UPDATE mongoeco_schema_migrations SET version = ? '
            "WHERE component = 'change_outbox'",
            (target,),
        )
        version = target


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f'PRAGMA table_info({table})').fetchall()
    }


def _infer_legacy_schema_version(conn: sqlite3.Connection) -> int:
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'",
        ).fetchall()
    }
    if 'change_outbox' not in tables:
        return 0
    identity_columns = _table_columns(conn, 'change_outbox_identities')
    consumer_columns = _table_columns(conn, 'change_outbox_consumers')
    if {
        'owner_instance',
        'registration_expires_at_epoch',
        'lease_owner',
        'lease_generation',
        'lease_expires_at_epoch',
    } <= consumer_columns:
        return 4
    if {
        'lease_owner',
        'lease_generation',
        'lease_expires_at_epoch',
    } <= consumer_columns:
        return 3
    if {'event_kind', 'effect_hash'} <= identity_columns:
        return 2
    return 1


def _migrate_change_outbox_v1(conn: sqlite3.Connection) -> None:
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
        CREATE TABLE IF NOT EXISTS change_outbox_identities (
            operation_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            PRIMARY KEY (operation_id, event_index),
            UNIQUE (sequence)
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


def _migrate_change_outbox_v2(conn: sqlite3.Connection) -> None:
    _migrate_change_outbox_v1(conn)
    identity_columns = _table_columns(conn, 'change_outbox_identities')
    if 'event_kind' not in identity_columns:
        conn.execute(
            'ALTER TABLE change_outbox_identities ADD COLUMN event_kind TEXT',
        )
    if 'effect_hash' not in identity_columns:
        conn.execute(
            'ALTER TABLE change_outbox_identities '
            'ADD COLUMN effect_hash TEXT',
        )
    conn.execute(
        """
        INSERT OR IGNORE INTO change_outbox_identities (
            operation_id, event_index, sequence
        )
        SELECT operation_id, event_index, sequence
        FROM change_outbox
        """,
    )
    live_identities = conn.execute(
        """
        SELECT identities.operation_id,
               identities.event_index,
               outbox.kind,
               outbox.payload
        FROM change_outbox_identities AS identities
        JOIN change_outbox AS outbox
          ON outbox.sequence = identities.sequence
        WHERE identities.event_kind IS NULL
           OR identities.effect_hash IS NULL
        """,
    ).fetchall()
    for operation_id, event_index, event_kind, payload in live_identities:
        conn.execute(
            """
            UPDATE change_outbox_identities
            SET event_kind = ?, effect_hash = ?
            WHERE operation_id = ? AND event_index = ?
            """,
            (
                event_kind,
                (
                    _change_fingerprint(str(event_kind), payload)
                    if event_kind == 'event'
                    else None
                ),
                operation_id,
                event_index,
            ),
        )


def _migrate_change_outbox_v3(conn: sqlite3.Connection) -> None:
    _migrate_change_outbox_v2(conn)
    consumer_columns = _table_columns(conn, 'change_outbox_consumers')
    if 'lease_owner' not in consumer_columns:
        conn.execute(
            'ALTER TABLE change_outbox_consumers ADD COLUMN lease_owner TEXT',
        )
    if 'lease_generation' not in consumer_columns:
        conn.execute(
            'ALTER TABLE change_outbox_consumers ADD COLUMN '
            'lease_generation INTEGER NOT NULL DEFAULT 0',
        )
    if 'lease_expires_at_epoch' not in consumer_columns:
        conn.execute(
            'ALTER TABLE change_outbox_consumers ADD COLUMN '
            'lease_expires_at_epoch REAL',
        )


def _migrate_change_outbox_v4(conn: sqlite3.Connection) -> None:
    _migrate_change_outbox_v3(conn)
    consumer_columns = _table_columns(conn, 'change_outbox_consumers')
    if 'owner_instance' not in consumer_columns:
        conn.execute(
            'ALTER TABLE change_outbox_consumers '
            'ADD COLUMN owner_instance TEXT',
        )
    if 'registration_expires_at_epoch' not in consumer_columns:
        conn.execute(
            'ALTER TABLE change_outbox_consumers '
            'ADD COLUMN registration_expires_at_epoch REAL',
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


def _change_fingerprint(kind: str, effect: str | None) -> str:
    material = f'{kind}\0{effect or ""}'.encode()
    return hashlib.sha256(material).hexdigest()


def _pruned_through(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT pruned_through
        FROM change_outbox_state
        WHERE singleton = 1
        """,
    ).fetchone()
    return 0 if row is None else int(row[0])


def register_consumer(  # noqa: PLR0913 - persisted registration boundary
    conn: sqlite3.Connection,
    consumer_id: str,
    *,
    initial_checkpoint: int | None,
    durable: bool,
    owner_instance: str | None = None,
    ephemeral_ttl_seconds: float | None = None,
) -> int:
    if durable and (
        owner_instance is not None or ephemeral_ttl_seconds is not None
    ):
        message = 'durable change consumers cannot declare ephemeral ownership'
        raise ValueError(message)
    if (owner_instance is None) is not (ephemeral_ttl_seconds is None):
        message = (
            'ephemeral owner and registration TTL must be provided together'
        )
        raise TypeError(message)
    if ephemeral_ttl_seconds is not None and ephemeral_ttl_seconds <= 0:
        message = 'ephemeral consumer registration TTL must be positive'
        raise ValueError(message)
    expire_ephemeral_consumers(conn)
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
            consumer_id,
            checkpoint,
            durable,
            updated_at_epoch,
            owner_instance,
            registration_expires_at_epoch
        ) VALUES (
            ?,
            ?,
            ?,
            CAST(strftime('%s', 'now') AS REAL),
            ?,
            CASE
                WHEN ? IS NULL THEN NULL
                ELSE CAST(strftime('%s', 'now') AS REAL) + ?
            END
        )
        ON CONFLICT(consumer_id) DO UPDATE SET
            checkpoint = excluded.checkpoint,
            durable = excluded.durable,
            updated_at_epoch = excluded.updated_at_epoch,
            owner_instance = CASE
                WHEN excluded.durable = 1 THEN NULL
                ELSE excluded.owner_instance
            END,
            registration_expires_at_epoch = CASE
                WHEN excluded.durable = 1 THEN NULL
                ELSE excluded.registration_expires_at_epoch
            END
        """,
        (
            consumer_id,
            checkpoint,
            int(effective_durable),
            owner_instance,
            ephemeral_ttl_seconds,
            ephemeral_ttl_seconds,
        ),
    )
    return checkpoint


def expire_ephemeral_consumers(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        """
        DELETE FROM change_outbox_consumers
        WHERE durable = 0
          AND registration_expires_at_epoch IS NOT NULL
          AND registration_expires_at_epoch
              <= CAST(strftime('%s', 'now') AS REAL)
          AND (
              lease_owner IS NULL
              OR lease_expires_at_epoch IS NULL
              OR lease_expires_at_epoch
                  <= CAST(strftime('%s', 'now') AS REAL)
          )
        """,
    )
    return max(0, cursor.rowcount)


def renew_ephemeral_consumers(
    conn: sqlite3.Connection,
    *,
    owner_instance: str,
    now_epoch: float,
    ttl_seconds: float,
) -> int:
    if not owner_instance:
        message = 'ephemeral consumer owner must not be empty'
        raise ValueError(message)
    if ttl_seconds <= 0:
        message = 'ephemeral consumer registration TTL must be positive'
        raise ValueError(message)
    cursor = conn.execute(
        """
        UPDATE change_outbox_consumers
        SET registration_expires_at_epoch = ?,
            updated_at_epoch = ?
        WHERE durable = 0
          AND owner_instance = ?
        """,
        (now_epoch + ttl_seconds, now_epoch, owner_instance),
    )
    return max(0, cursor.rowcount)


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
    *,
    lease_owner: str | None = None,
    lease_generation: int | None = None,
) -> None:
    if lease_owner is not None or lease_generation is not None:
        if lease_owner is None or lease_generation is None:
            message = 'lease owner and generation must be provided together'
            raise TypeError(message)
        cursor = conn.execute(
            """
            UPDATE change_outbox_consumers
            SET checkpoint = MAX(checkpoint, ?),
                updated_at_epoch = CAST(strftime('%s', 'now') AS REAL)
            WHERE consumer_id = ?
              AND lease_owner = ?
              AND lease_generation = ?
            """,
            (sequence, consumer_id, lease_owner, lease_generation),
        )
    else:
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
        if lease_owner is not None:
            message = 'change consumer dispatch lease was lost'
            raise OperationFailure(message)
        message = 'change consumer must be registered before checkpointing'
        raise RuntimeError(message)


def acquire_consumer_lease(
    conn: sqlite3.Connection,
    consumer_id: str,
    *,
    owner: str,
    now_epoch: float,
    ttl_seconds: float,
) -> int | None:
    cursor = conn.execute(
        """
        UPDATE change_outbox_consumers
        SET lease_owner = ?,
            lease_generation = lease_generation + 1,
            lease_expires_at_epoch = ?
        WHERE consumer_id = ?
          AND (
              lease_owner IS NULL
              OR lease_expires_at_epoch IS NULL
              OR lease_expires_at_epoch <= ?
          )
        """,
        (owner, now_epoch + ttl_seconds, consumer_id, now_epoch),
    )
    if cursor.rowcount == 0:
        return None
    row = conn.execute(
        'SELECT lease_generation FROM change_outbox_consumers '
        'WHERE consumer_id = ? AND lease_owner = ?',
        (consumer_id, owner),
    ).fetchone()
    return None if row is None else int(row[0])


def renew_consumer_lease(
    conn: sqlite3.Connection,
    consumer_id: str,
    lease: tuple[str, int],
    *,
    now_epoch: float,
    ttl_seconds: float,
) -> bool:
    owner, generation = lease
    cursor = conn.execute(
        """
        UPDATE change_outbox_consumers
        SET lease_expires_at_epoch = ?
        WHERE consumer_id = ?
          AND lease_owner = ?
          AND lease_generation = ?
        """,
        (
            now_epoch + ttl_seconds,
            consumer_id,
            owner,
            generation,
        ),
    )
    return cursor.rowcount > 0


def release_consumer_lease(
    conn: sqlite3.Connection,
    consumer_id: str,
    *,
    owner: str,
    generation: int,
) -> None:
    conn.execute(
        """
        UPDATE change_outbox_consumers
        SET lease_owner = NULL,
            lease_expires_at_epoch = NULL
        WHERE consumer_id = ?
          AND lease_owner = ?
          AND lease_generation = ?
        """,
        (consumer_id, owner, generation),
    )


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
        'DELETE FROM change_outbox_identities WHERE sequence <= ?',
        (capacity_floor,),
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
    identity_row = conn.execute(
        'SELECT COUNT(*) FROM change_outbox_identities',
    ).fetchone()
    pruned_through = _pruned_through(conn)
    return {
        'pendingEntries': int(row[0]),
        'oldestSequence': int(row[1]),
        'newestSequence': max(int(row[2]), pruned_through),
        'prunedThrough': pruned_through,
        'consumerCount': int(consumer_row[0]),
        'minimumCheckpoint': int(consumer_row[1]),
        'retainedIdentities': int(identity_row[0]),
    }


def append_change(  # noqa: PLR0913 - transactional outbox boundary
    conn: sqlite3.Connection,
    *,
    context: OperationContext | None,
    event_index: int | None = None,
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
    effect = serialize_document(
        {
            'operation_type': operation_type,
            'db_name': db_name,
            'coll_name': coll_name,
            'document_key': document_key,
            'full_document': full_document,
            'update_description': None,
        },
    )
    payload = effect if kind == 'event' else None
    effective_event_index = (
        context.change_event_index if event_index is None else event_index
    )
    if (
        not isinstance(effective_event_index, int)
        or isinstance(effective_event_index, bool)
        or effective_event_index < 0
    ):
        message = 'event_index must be a non-negative integer'
        raise ValueError(message)
    existing = conn.execute(
        """
        SELECT sequence, event_kind, effect_hash
        FROM change_outbox_identities
        WHERE operation_id = ? AND event_index = ?
        """,
        (context.operation_id, effective_event_index),
    ).fetchone()
    if existing is not None:
        expected_hash = _change_fingerprint(kind, effect)
        if existing[1] is None or existing[2] is None:
            message = (
                'cannot verify replay for a legacy compacted outbox identity'
            )
            raise OperationFailure(message)
        if existing[1] != kind or existing[2] != expected_hash:
            message = (
                'outbox identity was reused with a different change payload'
            )
            raise OperationFailure(message)
        return int(existing[0])
    cursor = conn.execute(
        """
        INSERT INTO change_outbox (
            operation_id, event_index, kind, payload
        ) VALUES (?, ?, ?, ?)
        """,
        (context.operation_id, effective_event_index, kind, payload),
    )
    if cursor.lastrowid is None:  # pragma: no cover - sqlite insert invariant
        message = 'change outbox row was not persisted'
        raise RuntimeError(message)
    conn.execute(
        """
        INSERT INTO change_outbox_identities (
            operation_id, event_index, sequence, event_kind, effect_hash
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            context.operation_id,
            effective_event_index,
            cursor.lastrowid,
            kind,
            _change_fingerprint(kind, effect),
        ),
    )
    compact_change_outbox(conn, max_entries=max_entries)
    return int(cursor.lastrowid)


def read_committed_changes(
    conn: sqlite3.Connection,
    *,
    after_sequence: int,
    deserialize_document: Callable[[str], Document],
    limit: int = 1_000,
    through_sequence: int | None = None,
) -> tuple[CommittedChange, ...]:
    floor = _pruned_through(conn)
    if after_sequence < floor:
        message = (
            f'change history after sequence {after_sequence} is no longer '
            f'available; pruned through {floor}'
        )
        raise OperationFailure(message)
    upper_bound = (
        _max_sequence(conn)
        if through_sequence is None
        else through_sequence
    )
    rows = conn.execute(
        """
        SELECT sequence, kind, payload
        FROM change_outbox
        WHERE sequence > ? AND sequence <= ?
        ORDER BY sequence
        LIMIT ?
        """,
        (after_sequence, upper_bound, limit),
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
