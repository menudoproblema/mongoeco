"""Native asynchronous client fixture for AnyIO's asyncio backend."""

from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING, Any

import pytest

from mongoeco.wire._lifecycle import finish_cleanup


if TYPE_CHECKING:
    from collections.abc import Iterator

    from pymongo import AsyncMongoClient


pytest_plugins = ("mongoeco.pytest_plugin",)
__all__ = ["mongoeco_async_client"]


def pytest_configure(config: pytest.Config) -> None:
    if config.pluginmanager.hasplugin("mongoeco.pytest_asyncio"):
        message = "load only one Mongoeco async adapter"
        raise pytest.UsageError(message)
    mode = config.getoption("asyncio_mode", default=None)
    if mode is None:
        try:
            mode = config.getini("asyncio_mode")
        except ValueError:
            mode = None
    if mode == "auto":
        message = "Mongoeco AnyIO adapter requires pytest-asyncio strict mode"
        raise pytest.UsageError(message)


@pytest.fixture
def _mongoeco_anyio_backend(anyio_backend_name: str) -> None:
    if anyio_backend_name != "asyncio":
        message = "Mongoeco native async clients require the AnyIO asyncio backend"
        raise pytest.UsageError(message)


@pytest.fixture
async def _mongoeco_anyio_loop(
    _mongoeco_anyio_backend: None,
) -> asyncio.AbstractEventLoop:
    return asyncio.get_running_loop()


@pytest.fixture
def mongoeco_async_client(
    _mongoeco_anyio_loop: asyncio.AbstractEventLoop,
    mongoeco_uri: str,
    _mongoeco_options: dict[str, Any],
) -> Iterator[AsyncMongoClient[dict[str, Any]]]:
    """Yield a native client and close it even if cancellation kills the runner task."""
    from pymongo import AsyncMongoClient  # noqa: PLC0415 - optional dependency

    client = AsyncMongoClient(mongoeco_uri, **_mongoeco_options)
    close = client.close
    try:
        yield client
    finally:
        # The coroutine loop fixture keeps the AnyIO loop alive. A synchronous
        # finalizer runs while that loop is idle, independently of a cancelled
        # AnyIO runner task. Never send teardown back through that dead task.
        _mongoeco_anyio_loop.run_until_complete(finish_cleanup(close()))
