"""Resource ownership through startup and shutdown failures."""

import asyncio
import socket
import sqlite3
import threading

from contextlib import closing
from functools import partial
from unittest.mock import AsyncMock, Mock

import pytest

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from mongoeco import _pytest_runtime as runtime
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_owned_server_thread_and_directory_are_released(tmp_path, backend):
    server = runtime.ProxyThread(backend, tmp_path, "8.0", "4.19")
    uri = server.start()
    with MongoClient(uri) as client:
        client.same.items.insert_one({"_id": "same"})
    assert not server._thread.daemon
    server.close()
    server.close()
    assert not server._thread.is_alive()
    assert not list(tmp_path.glob("mongoeco-*"))


def test_start_failure_closes_owned_thread_and_directory(tmp_path, monkeypatch):
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        factory = partial(AsyncMongoEcoProxyServer, port=occupied.getsockname()[1])
        monkeypatch.setattr(runtime, "AsyncMongoEcoProxyServer", factory)
        server = runtime.ProxyThread("sqlite", tmp_path, "8.0", "4.19")
        with pytest.raises(OSError, match="address already in use"):
            server.start()
        assert not server._thread.is_alive()
        assert not list(tmp_path.glob("mongoeco-*"))
        assert occupied.fileno() >= 0


def test_sqlite_is_reopenable_after_proxy_shutdown(tmp_path):
    path = tmp_path / "persist.sqlite"
    existing_threads = set(threading.enumerate())

    async def exercise():
        engine = SQLiteEngine(str(path), executor_workers=1)
        proxy = AsyncMongoEcoProxyServer(engine=engine)
        await proxy.start()
        await proxy.close()
        assert not proxy._handlers

    asyncio.run(exercise())
    with closing(sqlite3.connect(path, timeout=0)) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone() == ("ok",)
    assert not any(
        t.name.startswith("mongoeco-sqlite")
        for t in set(threading.enumerate()) - existing_threads
    )


def test_close_error_does_not_leave_server_or_thread(tmp_path, monkeypatch):
    original = runtime.SQLiteEngine

    def engine(*args, **kwargs):
        instance = original(*args, **kwargs)
        disconnect = instance.disconnect

        async def failing_disconnect():
            await disconnect()
            message = "deliberate disconnect error"
            raise RuntimeError(message)

        instance.disconnect = failing_disconnect
        return instance

    monkeypatch.setattr(runtime, "SQLiteEngine", engine)
    server = runtime.ProxyThread("sqlite", tmp_path, "8.0", "4.19")
    uri = server.start()
    with pytest.raises(ExceptionGroup):
        server.close()
    assert not server._thread.is_alive()
    assert not list(tmp_path.glob("mongoeco-*"))
    with (
        MongoClient(uri, serverSelectionTimeoutMS=30) as client,
        pytest.raises(ServerSelectionTimeoutError),
    ):
        client.admin.command("ping")


def test_proxy_cleanup_continues_after_session_close_failure():
    async def exercise():
        proxy = AsyncMongoEcoProxyServer()
        proxy._session_store.clear = lambda: (_ for _ in ()).throw(
            ValueError("session")
        )
        await proxy.start()
        engine = proxy._client._engine
        engine.disconnect = AsyncMock(wraps=engine.disconnect)
        with pytest.raises(ExceptionGroup):
            await proxy.close()
        engine.disconnect.assert_awaited_once()
        assert not proxy._engine_connected

    asyncio.run(exercise())


def test_thread_timeout_is_reported_instead_of_hidden(tmp_path):
    server = runtime.ProxyThread("memory", tmp_path, "8.0", "4.19")
    server._thread = Mock(ident=1)
    server._thread.is_alive.return_value = True
    with pytest.raises(TimeoutError, match="did not stop within 10 seconds"):
        server.close()
    server._thread.join.assert_called_once_with(timeout=10)
