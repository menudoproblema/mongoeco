"""Owned loopback runtime shared by the optional pytest adapters."""

from __future__ import annotations

import asyncio
import concurrent.futures
import tempfile
import threading

from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.wire._lifecycle import finish_cleanup
from mongoeco.wire.proxy import AsyncMongoEcoProxyServer


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pymongo import AsyncMongoClient


class ProxyThread:
    """Own one server, loop and engine; never borrow the test's event loop."""

    def __init__(
        self,
        backend: str,
        directory: Path,
        dialect: str,
        profile: str,
        *,
        diagnostic_capacity: int = 0,
    ):
        self._backend = backend
        self._directory = directory
        self._dialect = dialect
        self._profile = profile
        self._diagnostic_capacity = diagnostic_capacity
        self._proxy: AsyncMongoEcoProxyServer | None = None
        self._ready: concurrent.futures.Future[str] = concurrent.futures.Future()
        self._error: BaseException | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop: asyncio.Event | None = None
        self._stop_requested = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="mongoeco-pytest-server", daemon=False
        )

    @property
    def diagnostics(self) -> dict[str, object]:
        """Read already captured metadata, including after the server stops."""
        if self._proxy is None:
            return {"status": "not_started", "events": [], "resources": {}}
        return self._proxy.diagnostics

    def start(self) -> str:
        self._thread.start()
        try:
            return self._ready.result(timeout=10)
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup_error:
                error.add_note(f"test server cleanup failed: {cleanup_error}")
            raise

    def close(self) -> None:
        self._stop_requested.set()
        if self._loop is not None and self._stop is not None:
            with suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._stop.set)
        if self._thread.ident is not None:
            self._thread.join(timeout=10)
        if self._thread.is_alive():
            message = "mongoeco test server did not stop within 10 seconds"
            raise TimeoutError(message)
        if self._error is not None:
            raise self._error

    def _run(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as error:
            self._error = error
            if not self._ready.done():
                self._ready.set_exception(error)

    async def _serve(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        with tempfile.TemporaryDirectory(
            prefix="mongoeco-", dir=self._directory
        ) as name:
            engine = (
                MemoryEngine()
                if self._backend == "memory"
                else SQLiteEngine(
                    str(Path(name) / "database.sqlite"), executor_workers=1
                )
            )
            proxy = AsyncMongoEcoProxyServer(
                engine=engine,
                mongodb_dialect=self._dialect,
                pymongo_profile=self._profile,
                diagnostic_capacity=self._diagnostic_capacity,
            )
            self._proxy = proxy
            try:
                await asyncio.wait_for(proxy.start(), timeout=10)
                self._ready.set_result(proxy.address.uri)
                if not self._stop_requested.is_set():
                    await self._stop.wait()
            finally:
                await finish_cleanup(proxy.close())


@asynccontextmanager
async def async_client(
    uri: str, options: dict[str, Any]
) -> AsyncIterator[AsyncMongoClient[dict[str, Any]]]:
    from pymongo import AsyncMongoClient  # noqa: PLC0415 - optional dependency

    client = AsyncMongoClient(uri, **options)
    close = client.close
    try:
        yield client
    finally:
        await finish_cleanup(close())
