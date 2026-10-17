"""Diagnostic phases describe actual execution without retaining user BSON."""

import asyncio
import struct

from unittest.mock import patch

import pymongo
import pytest

from bson import BSON

from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer
from mongoeco.wire.protocol import parse_message_header

from tests.support_wire_envelopes import packet


CONCERN_CODE = 64


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("legacy", [False, True])
def test_diagnostics_use_executed_normalization_once(backend, legacy, tmp_path):
    async def exercise():
        engine = (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(str(tmp_path / "data.sqlite"))
        )
        async with AsyncMongoEcoProxyServer(
            engine=engine, diagnostic_capacity=128
        ) as proxy:
            reader, writer = await asyncio.open_connection(
                proxy.address.host, proxy.address.port
            )
            try:
                if legacy:
                    body = BSON.encode({"$query": {"ping": 1}})
                    payload = (
                        struct.pack("<i", 0)
                        + b"admin.$cmd\x00"
                        + struct.pack("<ii", 0, -1)
                        + body
                    )
                    raw = struct.pack("<iiii", 16 + len(payload), 42, 0, 2004) + payload
                else:
                    raw = packet({"$db": "admin", "ping": 1, "mirrored": True}, 42)
                with patch.object(
                    proxy._executor,
                    "_build_request_context",
                    wraps=proxy._executor._build_request_context,
                ) as prepare:
                    writer.write(raw)
                    await writer.drain()
                    header = await reader.readexactly(16)
                    response = await reader.readexactly(
                        struct.unpack("<i", header[:4])[0] - 16
                    )
                    assert response
                    assert prepare.call_count == 1
                received = [
                    e
                    for e in proxy.diagnostics["events"]
                    if e["event"] == "request_received"
                ]
                assert received[0]["command"] == "ping"
                assert received[0]["mirrored"] is (not legacy)
            finally:
                writer.close()
                await writer.wait_closed()

    asyncio.run(exercise())


def test_failed_encoding_is_not_reported_as_prepared(monkeypatch):
    async def exercise():
        proxy = AsyncMongoEcoProxyServer(diagnostic_capacity=128)
        async with proxy:
            raw = packet({"ping": 1, "$db": "admin"}, 42)

            def fail(*args, **kwargs):
                message = "PRIVATE_ENCODER"
                raise RuntimeError(message)

            monkeypatch.setattr("mongoeco.wire.proxy.encode_op_msg_response", fail)
            connection = proxy._connections.create(None)
            try:
                with pytest.raises(RuntimeError, match="PRIVATE_ENCODER"):
                    await proxy._dispatch_wire_request(
                        parse_message_header(raw[:16]), raw[16:], connection=connection
                    )
            finally:
                proxy._connections.close(connection)
            entry = proxy.diagnostics["first_error"]
            assert entry["response"] == "not_produced"
            assert entry["phase"] == "encode"
            assert "PRIVATE_ENCODER" not in str(proxy.diagnostics)

    asyncio.run(exercise())


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_concern_error_classification_and_client_effects(
    target, backend, surface, tmp_path
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
            execute = proxy._executor.execute_prepared

            async def with_concern(context):
                result = await execute(context)
                if context.command_name == "insert":
                    result["writeConcernError"] = {
                        "code": 64,
                        "codeName": "WriteConcernFailed",
                        "errmsg": "controlled concern failure",
                    }
                return result

            proxy._executor.execute_prepared = with_concern

            def sync_client():
                with pymongo.MongoClient(
                    proxy.address.uri, retryWrites=False, maxPoolSize=1
                ) as client:
                    with pytest.raises(pymongo.errors.WriteConcernError) as caught:
                        client.probe.items.insert_one({"_id": "a"})
                    assert caught.value.code == CONCERN_CODE
                    assert client.probe.items.find_one({"_id": "a"}) == {"_id": "a"}
                    assert client.admin.command("ping")["ok"] == 1

            async def async_client():
                async with pymongo.AsyncMongoClient(
                    proxy.address.uri, retryWrites=False, maxPoolSize=1
                ) as client:
                    with pytest.raises(pymongo.errors.WriteConcernError) as caught:
                        await client.probe.items.insert_one({"_id": "a"})
                    assert caught.value.code == CONCERN_CODE
                    assert await client.probe.items.find_one({"_id": "a"}) == {
                        "_id": "a"
                    }
                    assert (await client.admin.command("ping"))["ok"] == 1

            if surface == "sync":
                await asyncio.to_thread(sync_client)
            else:
                await async_client()
            event = proxy.diagnostics["first_error"]
            assert event["outcome"] == "error"
            assert event["code"] == CONCERN_CODE
            assert event["response"] == "ready"
            assert event["phase"] == "encode"

    asyncio.run(exercise())


@pytest.mark.parametrize("internal", [False, True])
def test_send_failure_has_connection_phase_without_claiming_delivery(internal):
    async def exercise():
        proxy = AsyncMongoEcoProxyServer(diagnostic_capacity=128)
        reader = asyncio.StreamReader()
        reader.feed_data(packet({"ping": 1, "$db": "admin"}, 42))

        class FailedWriter:
            def get_extra_info(self, key):
                return None

            def write(self, data):
                pass

            async def drain(self):
                message = "PRIVATE_SEND"
                raise (
                    RuntimeError(message) if internal else ConnectionResetError(message)
                )

            def close(self):
                pass

            async def wait_closed(self):
                pass

        async with proxy:
            if internal:
                with pytest.raises(RuntimeError, match="PRIVATE_SEND"):
                    await proxy._handle_connection(reader, FailedWriter())
            else:
                await proxy._handle_connection(reader, FailedWriter())
            events = proxy.diagnostics["events"]
            assert not any(e["event"] == "response_sent" for e in events)
            failed = proxy.diagnostics["first_error"]
            assert failed["phase"] == "send"
            assert type(failed["connection_id"]) is int
            assert failed["error_family"] == ("internal" if internal else "connection")
            assert "PRIVATE_SEND" not in str(proxy.diagnostics)

    asyncio.run(exercise())
