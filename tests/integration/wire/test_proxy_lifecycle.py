"""Socket and engine ownership regressions for the public proxy lifecycle."""

import asyncio
import socket
import unittest

from contextlib import suppress
from unittest.mock import AsyncMock, Mock, patch

from mongoeco.api import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.wire import AsyncMongoEcoProxyServer
from mongoeco.wire.protocol import encode_op_msg_response


class ProxyLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_start_after_bind_closes_unpublished_listener(self):
        proxy = AsyncMongoEcoProxyServer(engine=MemoryEngine())
        original = asyncio.Server._start_serving
        servers = []

        def cancel_after_bind(server):
            original(server)
            servers.append(server)
            asyncio.current_task().cancel()

        try:
            # Cancel at asyncio's first yield after binding the real socket.
            with (
                patch.object(asyncio.Server, "_start_serving", cancel_after_bind),
                self.assertRaises(asyncio.CancelledError),
            ):
                await proxy.start()
            self.assertEqual(len(servers), 1)
            self.assertFalse(servers[0].sockets, "cancelled start leaked its listener")
            self.assertFalse(proxy._engine_connected)
            self.assertIsNone(proxy._server)
        finally:
            for server in servers:
                server.close()
                server.abort_clients()
                await server.wait_closed()
            await proxy.close()

    async def test_cancelled_close_waiting_for_start_still_releases_resources(self):
        entered, release = asyncio.Event(), asyncio.Event()
        engine = MemoryEngine()
        connect = engine.connect

        async def delayed_connect():
            entered.set()
            await release.wait()
            await connect()

        engine.connect = delayed_connect
        proxy = AsyncMongoEcoProxyServer(engine=engine)
        starting = asyncio.create_task(proxy.start())
        await entered.wait()
        closing = asyncio.create_task(proxy.close())
        try:
            await asyncio.sleep(0)
            closing.cancel()
            await asyncio.sleep(0)
            self.assertFalse(closing.done(), "cancelled close abandoned pending start")
            closing.cancel()
            release.set()
            await starting
            with self.assertRaises(asyncio.CancelledError):
                await closing
            self.assertIsNone(proxy._server)
            self.assertFalse(proxy._engine_connected)
        finally:
            release.set()
            await starting
            with suppress(asyncio.CancelledError):
                await closing
            await proxy.close()

    async def test_completed_handler_with_queued_callback_cannot_spin_shutdown(self):
        proxy = AsyncMongoEcoProxyServer(engine=MemoryEngine())
        await proxy.start()
        loop = asyncio.get_running_loop()
        original_factory = loop.get_task_factory()
        original_gather = asyncio.gather
        calls = 0

        def bounded_gather(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                message = "shutdown spun on a completed handler"
                raise AssertionError(message)
            return original_gather(*args, **kwargs)

        try:
            # Eager completion places the done callback behind the close call.
            loop.set_task_factory(asyncio.eager_task_factory)
            reader, writer = Mock(), Mock()
            reader.at_eof.return_value = True
            writer.wait_closed = AsyncMock()
            proxy._accept_connection(reader, writer)
            self.assertTrue(all(task.done() for task in proxy._handlers))
            with patch("asyncio.gather", side_effect=bounded_gather):
                await proxy.close()
            self.assertFalse(proxy._handlers)
            writer.close.assert_called_once()
        finally:
            loop.set_task_factory(original_factory)
            proxy._handlers.clear()
            await proxy.close()

    async def test_shutdown_does_not_wait_for_a_peer_to_drain_a_large_response(self):
        proxy = AsyncMongoEcoProxyServer(engine=MemoryEngine())
        await proxy.start()
        seen = asyncio.Event()

        async def reply(*args, **kwargs):
            seen.set()
            return {"ok": 1, "data": "x" * (8 * 1024 * 1024)}

        proxy._executor.execute_command = reply
        _, peer = await asyncio.open_connection(proxy.address.host, proxy.address.port)
        server = proxy._server
        try:
            peer.write(
                encode_op_msg_response(
                    {"ping": 1, "$db": "admin"}, request_id=1, response_to=0
                )
            )
            await peer.drain()
            await seen.wait()
            closing = asyncio.create_task(proxy.close())
            done, _ = await asyncio.wait({closing}, timeout=2)
            if not done:
                # Let a broken implementation finish so the regression never leaks.
                server.abort_clients()
            await closing
            self.assertTrue(done, "shutdown waited for the unread response")
        finally:
            server.abort_clients()
            await proxy.close()
            peer.close()
            with suppress(ConnectionError):
                await peer.wait_closed()

    async def test_accept_finishing_after_shutdown_closes_peer_without_handler(self):
        proxy = AsyncMongoEcoProxyServer(engine=MemoryEngine())
        await proxy.start()
        await proxy.close()
        writer = Mock()
        proxy._accept_connection(Mock(), writer)
        writer.close.assert_called_once()
        self.assertFalse(proxy._handlers)

    async def test_accept_racing_shutdown_cannot_leave_close_waiting(self):
        for _ in range(50):
            proxy = AsyncMongoEcoProxyServer(engine=MemoryEngine())
            await proxy.start()
            address = (proxy.address.host, proxy.address.port)
            with socket.socket() as peer:
                peer.setblocking(False)  # noqa: FBT003 - socket API is positional-only
                opening = asyncio.create_task(
                    asyncio.get_running_loop().sock_connect(peer, address)
                )
                await asyncio.sleep(0)
                await asyncio.wait_for(proxy.close(), 2)
                # A shutdown racing a connect can be refused/reset by the OS.
                # Owning the raw socket avoids a client transport being created
                # around an already reset socket on CPython 3.13/macOS.
                with suppress(OSError):
                    await opening
            self.assertFalse(proxy._handlers)

    async def test_close_disconnects_open_peers_and_all_handlers(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
            reader, writer = await asyncio.open_connection(
                proxy.address.host, proxy.address.port
            )
            await asyncio.sleep(0)
            await asyncio.wait_for(proxy.close(), 2)
            self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
            self.assertFalse(proxy._handlers)
            writer.close()
            await writer.wait_closed()
        await proxy.close()

    async def test_failed_bind_rolls_back_connected_engine(self):
        occupied = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
        engine = MemoryEngine()
        engine.connect = AsyncMock(wraps=engine.connect)
        engine.disconnect = AsyncMock(wraps=engine.disconnect)
        proxy = AsyncMongoEcoProxyServer(
            engine=engine, port=occupied.sockets[0].getsockname()[1]
        )
        try:
            with self.assertRaises(OSError):
                await proxy.start()
            engine.connect.assert_awaited_once()
            engine.disconnect.assert_awaited_once()
            self.assertTrue(occupied.is_serving())
            await proxy.close()
            engine.disconnect.assert_awaited_once()
        finally:
            occupied.close()
            await occupied.wait_closed()

    async def test_partial_connect_failure_attempts_disconnect(self):
        engine = MemoryEngine()
        engine.connect = AsyncMock(side_effect=RuntimeError("connect failure"))
        engine.disconnect = AsyncMock()
        proxy = AsyncMongoEcoProxyServer(engine=engine)
        with self.assertRaisesRegex(RuntimeError, "connect failure"):
            await proxy.start()
        engine.disconnect.assert_awaited_once()

    async def test_failed_cleanup_preserves_startup_error(self):
        engine = MemoryEngine()
        engine.connect = AsyncMock(side_effect=ValueError("startup"))
        engine.disconnect = AsyncMock(side_effect=RuntimeError("disconnect"))
        proxy = AsyncMongoEcoProxyServer(engine=engine)
        with self.assertRaisesRegex(ValueError, "startup") as caught:
            await proxy.start()
        self.assertIn("cleanup failed", caught.exception.__notes__[0])
        engine.disconnect.side_effect = None
        await proxy.close()

    async def test_borrowed_client_is_neither_connected_nor_closed(self):
        engine = MemoryEngine()
        await engine.connect()
        engine.connect = AsyncMock(wraps=engine.connect)
        engine.disconnect = AsyncMock(wraps=engine.disconnect)
        client = AsyncMongoClient(engine)
        async with AsyncMongoEcoProxyServer(client=client) as proxy:
            self.assertEqual(await proxy.start(), proxy.address)
        await proxy.close()
        engine.connect.assert_not_awaited()
        engine.disconnect.assert_not_awaited()
        await engine.disconnect()

    async def test_repeated_cancellation_waits_for_engine_disconnect(self):
        entered, release = asyncio.Event(), asyncio.Event()
        engine = MemoryEngine()
        original = engine.disconnect

        async def disconnect():
            entered.set()
            await release.wait()
            await original()

        engine.disconnect = disconnect
        proxy = AsyncMongoEcoProxyServer(engine=engine)
        await proxy.start()
        closing = asyncio.create_task(proxy.close())
        await entered.wait()
        closing.cancel()
        await asyncio.sleep(0)
        closing.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertFalse(proxy._engine_connected)
        await proxy.close()

    async def test_start_can_be_retried_after_cancelled_bind(self):
        engine = MemoryEngine()
        proxy = AsyncMongoEcoProxyServer(engine=engine)
        with (
            patch("asyncio.start_server", side_effect=asyncio.CancelledError),
            self.assertRaises(asyncio.CancelledError),
        ):
            await proxy.start()
        self.assertFalse(proxy._engine_connected)
        await proxy.start()
        await proxy.close()
