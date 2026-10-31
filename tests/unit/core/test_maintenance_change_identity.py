"""Maintenance records retain the request outbox's identity and replay rules."""

import json
import sqlite3

from contextlib import closing

import pytest

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import ChangePublicationPolicy, OperationContext
from mongoeco.engines._sqlite_outbox import (
    append_change,
    ensure_change_outbox_schema,
    read_committed_changes,
    register_consumer,
)
from mongoeco.errors import OperationFailure


ARGUMENTS = {
    "operation_type": "delete",
    "db_name": "test",
    "coll_name": "records",
    "document_key": {"_id": 1},
    "full_document": None,
    "serialize_document": json.dumps,
}


def test_maintenance_identity_replays_once_and_rejects_changed_effect():
    with closing(sqlite3.connect(":memory:")) as conn:
        ensure_change_outbox_schema(conn)
        register_consumer(conn, "observer", initial_checkpoint=0, durable=True)
        assert append_change(conn, context=None, **ARGUMENTS) is None
        sequence = append_change(
            conn, context=None, maintenance_operation_id="ttl-1", **ARGUMENTS
        )
        assert (
            append_change(
                conn, context=None, maintenance_operation_id="ttl-1", **ARGUMENTS
            )
            == sequence
        )
        with pytest.raises(OperationFailure, match="identity was reused"):
            append_change(
                conn,
                context=None,
                maintenance_operation_id="ttl-1",
                **{**ARGUMENTS, "document_key": {"_id": 2}},
            )
        changes = read_committed_changes(
            conn, after_sequence=0, deserialize_document=json.loads
        )
        assert len(changes) == 1
        assert changes[0].payload["document_key"] == {"_id": 1}
        assert changes[0].payload["operation_type"] == "delete"


@pytest.mark.parametrize("identity", ["", 17])
def test_maintenance_identity_must_be_a_nonempty_string(identity):
    with closing(sqlite3.connect(":memory:")) as conn:
        ensure_change_outbox_schema(conn)
        with pytest.raises(ValueError, match="non-empty string"):
            append_change(
                conn, context=None, maintenance_operation_id=identity, **ARGUMENTS
            )


def test_maintenance_cannot_override_request_publication_policy():
    with closing(sqlite3.connect(":memory:")) as conn:
        ensure_change_outbox_schema(conn)
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_70, publication=ChangePublicationPolicy.DISABLED
        )
        assert append_change(conn, context=context, **ARGUMENTS) is None
        with pytest.raises(ValueError, match="mutually exclusive"):
            append_change(
                conn, context=context, maintenance_operation_id="ttl-1", **ARGUMENTS
            )
