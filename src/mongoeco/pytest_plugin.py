"""Opt-in pytest fixtures for native PyMongo clients over a local wire server."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

import pytest

from mongoeco._pytest_runtime import ProxyThread
from mongoeco._version import __version__
from mongoeco.compat import resolve_mongodb_dialect, resolve_pymongo_profile


if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from _pytest.terminal import TerminalReporter
    from pymongo import MongoClient


__all__ = ["mongoeco_client", "mongoeco_client_options", "mongoeco_uri"]


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("mongoeco")
    group.addoption(
        "--mongoeco-backend", choices=("memory", "sqlite", "both"), default="memory"
    )
    group.addoption(
        "--mongoeco-mongodb-dialect", choices=("7.0", "8.0", "9.0"), default="7.0"
    )
    group.addoption("--mongoeco-pymongo-profile", default="strict-auto-installed")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "mongoeco_uri" in metafunc.fixturenames:
        backend = metafunc.config.getoption("--mongoeco-backend")
        engines = ["memory", "sqlite"] if backend == "both" else [backend]
        metafunc.parametrize("mongoeco_uri", engines, indirect=True, ids=engines)


def pytest_terminal_summary(
    terminalreporter: TerminalReporter, config: pytest.Config
) -> None:
    """Report selected configuration without constructing test resources."""
    terminalreporter.section("Mongoeco configuration")
    for line in _configuration_report(config):
        terminalreporter.write_line(line)


def _configuration_report(config: pytest.Config) -> list[str]:
    try:
        sdk = version("pymongo")
    except PackageNotFoundError:
        sdk = "not installed"
    backend = config.getoption("--mongoeco-backend")
    engines = "memory, sqlite" if backend == "both" else backend
    requested = config.getoption("--mongoeco-pymongo-profile")
    try:
        profile = resolve_pymongo_profile(requested).key
    except ValueError:
        profile = "invalid"
    dialect = config.getoption("--mongoeco-mongodb-dialect")
    adapter = "none"
    for name in ("pytest_asyncio", "pytest_anyio"):
        if config.pluginmanager.hasplugin(f"mongoeco.{name}"):
            adapter = name
    return [
        f"Mongoeco {__version__}; PyMongo {sdk}",
        f"Selected backends: {engines}; MongoDB dialect: {dialect}",
        f"PyMongo profile: {requested!r} -> {profile}",
        f"Mongoeco async adapter: {adapter}",
    ]


@pytest.fixture
def mongoeco_client_options() -> dict[str, Any]:
    """Override native constructor options without changing the managed endpoint."""
    return {}


@pytest.fixture
def _mongoeco_options(mongoeco_client_options: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(mongoeco_client_options, dict):
        message = "mongoeco_client_options must be a dictionary"
        raise pytest.UsageError(message)
    if {"host", "port"} & mongoeco_client_options.keys():
        message = "mongoeco_client_options cannot override host or port"
        raise pytest.UsageError(message)
    return {"serverSelectionTimeoutMS": 3000, **mongoeco_client_options}


@pytest.fixture
def mongoeco_uri(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    _mongoeco_options: dict[str, Any],
) -> Iterator[str]:
    """Yield an isolated URI; close clients constructed from it yourself."""
    # Parametrize this public boundary: pytest 8.4 cannot see the full dependency
    # closure through a fixture that wraps an overridden fixture of the same name.
    dialect = resolve_mongodb_dialect(
        request.config.getoption("--mongoeco-mongodb-dialect")
    )
    profile = resolve_pymongo_profile(
        request.config.getoption("--mongoeco-pymongo-profile")
    )
    server = ProxyThread(request.param, tmp_path, dialect.key, profile.key)
    try:
        yield server.start()
    finally:
        server.close()


@pytest.fixture
def mongoeco_client(
    mongoeco_uri: str, _mongoeco_options: dict[str, Any]
) -> Iterator[MongoClient[dict[str, Any]]]:
    """Yield a real synchronous PyMongo client and always close it afterwards."""
    from pymongo import MongoClient  # noqa: PLC0415 - optional dependency

    client = MongoClient(mongoeco_uri, **_mongoeco_options)
    close = client.close
    try:
        yield client
    finally:
        close()
