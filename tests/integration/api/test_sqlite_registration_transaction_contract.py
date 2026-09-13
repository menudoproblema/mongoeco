"""Administrative registration must not invalidate the caller's data snapshot."""

from __future__ import annotations

import asyncio

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.sqlite import SQLiteEngine


def _registration_clock(engine):
    connection = engine._change_delivery_connection
    epoch = [int(connection.execute("SELECT strftime('%s', 'now')").fetchone()[0])]

    def strftime(format_spec, value):
        assert (format_spec, value) == ("%s", "now")
        return str(epoch[0])

    connection.create_function("strftime", 2, strftime)
    return epoch


@pytest.mark.parametrize("python_fallback", [False, True])
@pytest.mark.parametrize("commit", [False, True])
def test_async_transaction_survives_registration_clock_tick(
    tmp_path, python_fallback, commit
):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "registration.sqlite"))
        async with AsyncMongoClient(engine) as client:
            epoch = _registration_clock(engine)
            collection = client.test.records
            documents = [
                {"_id": str(i), "value": i, "label": "keep"} for i in range(10)
            ]
            await collection.insert_many(documents)
            session = client.start_session()
            session.start_transaction()
            selector = {"label": {"$regex": "^keep"}} if python_fallback else {}
            cursor = collection.find(selector, session=session).batch_size(1)
            try:
                iterator = cursor.__aiter__()
                first = await anext(iterator)
                control_changes = engine._change_delivery_connection.total_changes
                epoch[0] += 2
                result = await collection.update_one(
                    {"_id": "9"}, {"$set": {"value": -1}}, session=session
                )
                assert result.modified_count == 1
                assert (
                    engine._change_delivery_connection.total_changes == control_changes
                )
                assert [first] + [doc async for doc in iterator] == documents
                await cursor.close()
                if commit:
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                assert (await collection.find_one({"_id": "9"}))["value"] == (
                    -1 if commit else 9
                )
            finally:
                await cursor.close()
                if session.in_transaction:
                    session.abort_transaction()
                session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("python_fallback", [False, True])
@pytest.mark.parametrize("commit", [False, True])
def test_sync_transaction_survives_registration_clock_tick(
    tmp_path, python_fallback, commit
):
    engine = SQLiteEngine(str(tmp_path / "registration.sqlite"))
    with MongoClient(engine) as client:
        epoch = _registration_clock(engine)
        collection = client.test.records
        documents = [{"_id": str(i), "value": i, "label": "keep"} for i in range(10)]
        collection.insert_many(documents)
        session = client.start_session()
        session.start_transaction()
        selector = {"label": {"$regex": "^keep"}} if python_fallback else {}
        cursor = collection.find(selector, session=session).batch_size(1)
        try:
            iterator = iter(cursor)
            first = next(iterator)
            control_changes = engine._change_delivery_connection.total_changes
            epoch[0] += 2
            result = collection.update_one(
                {"_id": "9"}, {"$set": {"value": -1}}, session=session
            )
            assert result.modified_count == 1
            assert engine._change_delivery_connection.total_changes == control_changes
            assert [first, *iterator] == documents
            cursor.close()
            if commit:
                session.commit_transaction()
            else:
                session.abort_transaction()
            assert collection.find_one({"_id": "9"})["value"] == (-1 if commit else 9)
        finally:
            cursor.close()
            if session.in_transaction:
                session.abort_transaction()
            session.close()


@pytest.mark.parametrize("change", ["removed", "expired", "owner", "durable"])
def test_registration_rechecks_persisted_state_before_reuse(tmp_path, change):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "registration.sqlite"))
        await engine.connect()
        try:
            assert (
                engine.register_change_consumer("consumer", initial_checkpoint=0) == 0
            )
            control = engine._change_delivery_connection
            sql = {
                "removed": "DELETE FROM change_outbox_consumers",
                "expired": (
                    "UPDATE change_outbox_consumers "
                    "SET registration_expires_at_epoch = 0"
                ),
                "owner": "UPDATE change_outbox_consumers SET owner_instance = 'other'",
                "durable": "SELECT 1",
            }[change]
            control.execute(sql)
            control.commit()
            durable = change == "durable"
            assert engine.register_change_consumer("consumer", durable=durable) == 0
            row = control.execute(
                "SELECT durable, owner_instance, registration_expires_at_epoch "
                "FROM change_outbox_consumers WHERE consumer_id = 'consumer'"
            ).fetchone()
            if durable:
                assert row == (1, None, None)
            else:
                assert row[:2] == (0, engine._change_dispatch_owner)
                assert row[2] > 0
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_memory_registration_can_be_recreated_after_transaction_rollback():
    async def exercise():
        engine = SQLiteEngine()
        await engine.connect()
        try:
            connection = engine._connection
            connection.execute("BEGIN")
            assert engine.register_change_consumer("consumer") == 0
            connection.rollback()
            assert engine.register_change_consumer("consumer") == 0
            assert connection.execute(
                "SELECT checkpoint FROM change_outbox_consumers "
                "WHERE consumer_id = 'consumer'"
            ).fetchone() == (0,)
        finally:
            await engine.disconnect()

    asyncio.run(exercise())
