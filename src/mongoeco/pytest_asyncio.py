"""Native asynchronous client fixture for pytest-asyncio function loops."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio

from mongoeco._pytest_runtime import async_client


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pymongo import AsyncMongoClient


pytest_plugins = ("mongoeco.pytest_plugin",)
__all__ = ["mongoeco_async_client"]


def pytest_configure(config: pytest.Config) -> None:
    if config.pluginmanager.hasplugin("mongoeco.pytest_anyio"):
        message = "load only one Mongoeco async adapter"
        raise pytest.UsageError(message)


@pytest.fixture
def _mongoeco_asyncio_scope(request: pytest.FixtureRequest) -> None:
    marker = request.node.get_closest_marker("asyncio")
    scope = (
        marker.kwargs.get("loop_scope") or marker.kwargs.get("scope")
        if marker is not None
        else None
    )
    if scope is None:
        scope = request.config.getini("asyncio_default_test_loop_scope")
    if scope != "function":
        message = "Mongoeco async clients require a function-scoped test loop"
        raise pytest.UsageError(message)


@pytest_asyncio.fixture(loop_scope="function")
async def mongoeco_async_client(
    _mongoeco_asyncio_scope: None,
    mongoeco_uri: str,
    _mongoeco_options: dict[str, Any],
) -> AsyncIterator[AsyncMongoClient[dict[str, Any]]]:
    """Yield a real client bound to the test's function-scoped asyncio loop."""
    async with async_client(mongoeco_uri, _mongoeco_options) as client:
        yield client
