"""An idle fast path cannot cache away external work or failed deliveries."""

import asyncio
import json
import sqlite3

from contextlib import closing
from unittest.mock import patch

import pytest

from mongoeco.engines._sqlite_outbox import append_change
from mongoeco.engines.sqlite import SQLiteEngine


def test_idle_dispatch_uses_persisted_state_and_retries_unacknowledged_effects(
    tmp_path,
):
    async def exercise():
        path = str(tmp_path / "external.db")
        engine = SQLiteEngine(path)
        await engine.connect()
        try:
            engine.register_change_consumer(
                "observer", initial_checkpoint=0, durable=True
            )
            delivered = []

            def append_external(value):
                with closing(sqlite3.connect(path)) as conn, conn:
                    append_change(
                        conn,
                        context=None,
                        maintenance_operation_id=f"ttl-{value}",
                        operation_type="delete",
                        db_name="test",
                        coll_name="records",
                        document_key={"_id": value},
                        full_document=None,
                        serialize_document=json.dumps,
                    )

            with patch.object(
                engine,
                "_acquire_change_dispatch_lease",
                wraps=engine._acquire_change_dispatch_lease,
            ) as lease:
                engine.dispatch_committed_changes("observer", delivered.append)
                assert delivered == []
                assert lease.call_count == 0
                append_external(1)
                engine.dispatch_committed_changes("observer", delivered.append)
                assert [change.payload["document_key"] for change in delivered] == [
                    {"_id": 1}
                ]
                assert lease.call_count == 1
                engine.dispatch_committed_changes("observer", delivered.append)
                assert lease.call_count == 1
                append_external(2)

                def reject(_change):
                    message = "delivery rejected"
                    raise RuntimeError(message)

                with pytest.raises(RuntimeError, match="delivery rejected"):
                    engine.dispatch_committed_changes("observer", reject)
                engine.dispatch_committed_changes("observer", delivered.append)
                assert [change.payload["document_key"] for change in delivered] == [
                    {"_id": 1},
                    {"_id": 2},
                ]
                with pytest.raises(RuntimeError, match="must be registered"):
                    engine.dispatch_committed_changes("absent", delivered.append)
        finally:
            await engine.disconnect()

    asyncio.run(exercise())
