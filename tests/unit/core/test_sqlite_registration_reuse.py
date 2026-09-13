"""Read-only reuse must consult persisted identity, liveness and checkpoint."""

from __future__ import annotations

import sqlite3

from contextlib import closing

import pytest

from mongoeco.engines._sqlite_outbox import (
    ensure_change_outbox_schema,
    register_consumer,
    reusable_consumer_checkpoint,
)
from mongoeco.errors import OperationFailure


@pytest.fixture
def connection():
    with closing(sqlite3.connect(":memory:")) as conn:
        ensure_change_outbox_schema(conn)
        register_consumer(
            conn,
            "consumer",
            initial_checkpoint=0,
            durable=False,
            owner_instance="owner",
            ephemeral_ttl_seconds=3600,
        )
        conn.commit()
        yield conn


@pytest.mark.parametrize("durable", [False, True])
def test_valid_registration_reuse_is_read_only(connection, durable):
    if durable:
        register_consumer(connection, "consumer", initial_checkpoint=0, durable=True)
        connection.commit()
    before = connection.total_changes
    assert (
        reusable_consumer_checkpoint(
            connection,
            "consumer",
            initial_checkpoint=0,
            durable=durable,
            owner_instance="owner",
        )
        == 0
    )
    assert connection.total_changes == before
    assert not connection.in_transaction


@pytest.mark.parametrize(
    ["consumer_id", "initial_checkpoint", "durable", "owner"],
    [
        ("absent", None, False, "owner"),
        ("consumer", 1, False, "owner"),
        ("consumer", None, True, "owner"),
        ("consumer", None, False, "other"),
    ],
)
def test_changed_registration_requires_the_write_path(
    connection, consumer_id, initial_checkpoint, durable, owner
):
    before = connection.total_changes
    assert (
        reusable_consumer_checkpoint(
            connection,
            consumer_id,
            initial_checkpoint=initial_checkpoint,
            durable=durable,
            owner_instance=owner,
        )
        is None
    )
    assert connection.total_changes == before
    assert not connection.in_transaction


@pytest.mark.parametrize("expiry", [None, 0])
def test_expired_or_unleased_ephemeral_registration_is_not_reused(connection, expiry):
    connection.execute(
        "UPDATE change_outbox_consumers SET registration_expires_at_epoch = ?",
        (expiry,),
    )
    connection.commit()
    assert (
        reusable_consumer_checkpoint(
            connection,
            "consumer",
            initial_checkpoint=None,
            durable=False,
            owner_instance="owner",
        )
        is None
    )


def test_durable_registration_is_not_demoted_by_ephemeral_preparation(connection):
    register_consumer(connection, "consumer", initial_checkpoint=0, durable=True)
    connection.commit()
    before = connection.total_changes
    assert (
        reusable_consumer_checkpoint(
            connection,
            "consumer",
            initial_checkpoint=None,
            durable=False,
            owner_instance="other",
        )
        == 0
    )
    assert connection.total_changes == before
    assert connection.execute(
        "SELECT durable, owner_instance, registration_expires_at_epoch "
        "FROM change_outbox_consumers WHERE consumer_id = 'consumer'"
    ).fetchone() == (1, None, None)


def test_reuse_rejects_a_checkpoint_behind_retained_history(connection):
    connection.execute("UPDATE change_outbox_state SET pruned_through = 1")
    connection.commit()
    with pytest.raises(OperationFailure, match="pruned through"):
        reusable_consumer_checkpoint(
            connection,
            "consumer",
            initial_checkpoint=None,
            durable=False,
            owner_instance="owner",
        )
