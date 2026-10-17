"""The real wire boundary records outcomes without retaining application data."""

import asyncio
import json
import struct

from unittest.mock import AsyncMock

import pymongo
import pytest

from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.support_unacknowledged import packet, reply


TYPE_MISMATCH = 14
TIMEOUT = 50
DUPLICATE_KEY = 11000


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_capture_reply_suppression_errors_and_cleanup(backend, tmp_path):
    async def exercise():
        engine = (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(str(tmp_path / "data.sqlite"))
        )
        proxy = AsyncMongoEcoProxyServer(engine=engine, diagnostic_capacity=128)
        async with proxy:
            reader, writer = await asyncio.open_connection(
                proxy.address.host, proxy.address.port
            )
            try:
                for request_id, invalid in ((1, False), (2, True)):
                    writer.write(
                        packet(
                            {
                                "find": "private_collection",
                                "filter": []
                                if invalid
                                else {"secret": "private_value"},
                                "mirrored": True,
                                "$db": "private_database",
                            },
                            request_id,
                            more=True,
                        )
                    )
                writer.write(packet({"private_command": "secret", "$db": "admin"}, 3))
                writer.write(packet({"ping": 1, "$db": "admin"}, 4))
                await writer.drain()
                assert (await reply(reader, 3))["ok"] == 0
                assert (await reply(reader, 4))["ok"] == 1
            finally:
                writer.close()
                await writer.wait_closed()
        captured = proxy.diagnostics
        assert captured["resources"] == {
            "listener": False,
            "handlers": 0,
            "connections": 0,
            "cursors": 0,
            "sessions": 0,
            "engine_connected": False,
        }
        received = {
            e["request_id"]: e
            for e in captured["events"]
            if e["event"] == "request_received"
        }
        assert not received[1]["expects_reply"]
        assert received[2]["command"] == "find"
        assert received[2]["mirrored"] is True
        assert received[3]["command"] == "<unknown>"
        completed = {
            e["request_id"]: e
            for e in captured["events"]
            if e["event"] == "request_completed"
        }
        assert completed[1]["response"] == completed[2]["response"] == "suppressed"
        assert completed[2]["outcome"] == "error"
        assert completed[2]["code"] == TYPE_MISMATCH
        assert completed[4]["duration_ms"] >= 0
        assert [
            e["request_id"] for e in captured["events"] if e["event"] == "response_sent"
        ] == [3, 4]
        text = json.dumps(captured)
        assert "private_" not in text
        assert "secret" not in text
        assert "mongodb://" not in text

    asyncio.run(exercise())


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_fail_command_recipe_preserves_timeout_code_and_connection(
    target, surface, backend, tmp_path
):
    async def exercise():
        engine = (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(str(tmp_path / "data.sqlite"))
        )
        async with AsyncMongoEcoProxyServer(
            engine=engine, mongodb_dialect=target, diagnostic_capacity=128
        ) as proxy:
            failpoint = {
                "configureFailPoint": "failCommand",
                "mode": {"times": 1},
                "data": {"failCommands": ["findAndModify"], "errorCode": 50},
            }

            def sync_client():
                with pymongo.MongoClient(
                    proxy.address.uri,
                    retryReads=False,
                    retryWrites=False,
                    maxPoolSize=1,
                ) as client:
                    client.probe.items.insert_one({"_id": "a", "value": 0})
                    client.admin.command(failpoint)
                    with pytest.raises(pymongo.errors.ExecutionTimeout) as caught:
                        client.probe.items.find_one_and_update(
                            {"_id": "a"}, {"$inc": {"value": 1}}
                        )
                    assert caught.value.code == TIMEOUT
                    assert caught.value.details["codeName"] == "MaxTimeMSExpired"
                    assert client.probe.items.find_one({"_id": "a"})["value"] == 0
                    client.probe.items.find_one_and_update(
                        {"_id": "a"}, {"$inc": {"value": 1}}
                    )
                    assert client.probe.items.find_one({"_id": "a"})["value"] == 1

            async def async_client():
                async with pymongo.AsyncMongoClient(
                    proxy.address.uri,
                    retryReads=False,
                    retryWrites=False,
                    maxPoolSize=1,
                ) as client:
                    await client.probe.items.insert_one({"_id": "a", "value": 0})
                    await client.admin.command(failpoint)
                    with pytest.raises(pymongo.errors.ExecutionTimeout) as caught:
                        await client.probe.items.find_one_and_update(
                            {"_id": "a"}, {"$inc": {"value": 1}}
                        )
                    assert caught.value.code == TIMEOUT
                    assert caught.value.details["codeName"] == "MaxTimeMSExpired"
                    assert (await client.probe.items.find_one({"_id": "a"}))[
                        "value"
                    ] == 0
                    await client.probe.items.find_one_and_update(
                        {"_id": "a"}, {"$inc": {"value": 1}}
                    )
                    assert (await client.probe.items.find_one({"_id": "a"}))[
                        "value"
                    ] == 1

            if surface == "sync":
                await asyncio.to_thread(sync_client)
            else:
                await asyncio.to_thread(asyncio.run, async_client())
            events = proxy.diagnostics["events"]
            failures = [e for e in events if e.get("code") == TIMEOUT]
            assert len(failures) == 1
            assert failures[0]["outcome"] == "error"
            requests = [
                e
                for e in events
                if e["event"] == "request_received"
                and e["command"] in ("insert", "findAndModify", "find")
            ]
            assert len({e["connection_id"] for e in requests}) == 1

    asyncio.run(exercise())


def test_shutdown_records_pending_cursors_and_releases_them():
    async def exercise():
        proxy = AsyncMongoEcoProxyServer(diagnostic_capacity=128)
        async with proxy:
            reader, writer = await asyncio.open_connection(
                proxy.address.host, proxy.address.port
            )
            writer.write(
                packet(
                    {
                        "insert": "items",
                        "documents": [{"_id": 1}, {"_id": 2}],
                        "$db": "probe",
                    },
                    1,
                )
            )
            await writer.drain()
            await reply(reader, 1)
            writer.write(packet({"find": "items", "batchSize": 1, "$db": "probe"}, 2))
            await writer.drain()
            assert (await reply(reader, 2))["cursor"]["id"] != 0
            assert proxy.diagnostics["resources"]["cursors"] == 1
        writer.close()
        await writer.wait_closed()
        assert proxy.diagnostics["resources"]["cursors"] == 0
        assert proxy.diagnostics["resources"]["connections"] == 0
        assert any(
            e.get("reason") == "server_shutdown" for e in proxy.diagnostics["events"]
        )

    asyncio.run(exercise())


def test_malformed_frame_is_visible_without_exception_payload():
    async def exercise():
        proxy = AsyncMongoEcoProxyServer(diagnostic_capacity=128)
        async with proxy:
            reader, writer = await asyncio.open_connection(
                proxy.address.host, proxy.address.port
            )
            writer.write(struct.pack("<iiii", 0, 1, 0, 2013))
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), timeout=3) == b""
            await asyncio.sleep(0)
            writer.close()
            await writer.wait_closed()
        assert any(
            e.get("reason") == "protocol_error" for e in proxy.diagnostics["events"]
        )
        assert any(e["event"] == "handler_failed" for e in proxy.diagnostics["events"])

    asyncio.run(exercise())


def test_shutdown_records_cancelled_inflight_command():
    async def exercise():
        proxy = AsyncMongoEcoProxyServer(diagnostic_capacity=128)
        entered = asyncio.Event()

        async def block(*args, **kwargs):
            entered.set()
            await asyncio.Future()

        proxy._executor.execute_prepared = AsyncMock(side_effect=block)
        async with proxy:
            _, writer = await asyncio.open_connection(
                proxy.address.host, proxy.address.port
            )
            writer.write(packet({"ping": 1, "$db": "admin"}, 1))
            await writer.drain()
            await asyncio.wait_for(entered.wait(), timeout=3)
        writer.close()
        await writer.wait_closed()
        completed = [
            e for e in proxy.diagnostics["events"] if e["event"] == "request_completed"
        ]
        assert completed[0]["outcome"] == "cancelled"
        assert completed[0]["response"] == "not_produced"

    asyncio.run(exercise())


def test_duplicate_write_retains_code_without_error_message():
    async def exercise():
        proxy = AsyncMongoEcoProxyServer(diagnostic_capacity=128)
        async with proxy:
            reader, writer = await asyncio.open_connection(
                proxy.address.host, proxy.address.port
            )
            command = {
                "insert": "items",
                "documents": [{"_id": "PRIVATE_DUPLICATE"}],
                "$db": "probe",
            }
            try:
                for request_id in (1, 2):
                    writer.write(packet(command, request_id))
                    await writer.drain()
                    response = await reply(reader, request_id)
                assert response["writeErrors"][0]["code"] == DUPLICATE_KEY
            finally:
                writer.close()
                await writer.wait_closed()
        failures = [
            e for e in proxy.diagnostics["events"] if e.get("outcome") == "error"
        ]
        assert [e["code"] for e in failures] == [11000]
        assert "PRIVATE_DUPLICATE" not in json.dumps(proxy.diagnostics)

    asyncio.run(exercise())
