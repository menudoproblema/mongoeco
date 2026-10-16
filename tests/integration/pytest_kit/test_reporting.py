"""Configuration reporting is observable without constructing runtime resources."""

import json

import pytest

from mongoeco._version import __version__

from tests.integration.pytest_kit.test_fixtures import ASYNC, SYNC, prepare, run_session


REPORT_GUARD = """
import asyncio
import socket
import sqlite3
import tempfile
import threading
from pathlib import Path
import pymongo
from mongoeco import _pytest_runtime as runtime
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_terminal_summary():
    calls = []
    def forbidden(*args, **kwargs):
        calls.append('resource')
        raise AssertionError('report must not construct resources')
    with pytest.MonkeyPatch.context() as patch:
        for obj, name in (
            (socket, 'socket'),
            (sqlite3, 'connect'),
            (tempfile, 'TemporaryDirectory'),
            (threading.Thread, 'start'),
            (asyncio, 'start_server'),
            (runtime, 'ProxyThread'),
            (MemoryEngine, '__init__'),
            (SQLiteEngine, '__init__'),
            (AsyncMongoEcoProxyServer, '__init__'),
            (pymongo, 'MongoClient'),
            (pymongo, 'AsyncMongoClient'),
        ):
            patch.setattr(obj, name, forbidden)
        result = yield
    assert calls == []
    Path('report-resources.json').write_text(json.dumps({'resource_calls': 0}))
    return result
"""


def assert_report(result, pytester, *, backends, adapter, profile):
    result.stdout.fnmatch_lines(
        [
            "*Mongoeco configuration*",
            f"Mongoeco {__version__}; PyMongo *",
            f"Selected backends: {backends}; MongoDB dialect: 8.0",
            f"PyMongo profile: * -> {profile}",
            f"Mongoeco async adapter: {adapter}",
        ]
    )
    assert result.stdout.str().count("Mongoeco configuration") == 1
    receipt = json.loads((pytester.path / "report-resources.json").read_text())
    assert receipt == {"resource_calls": 0}


@pytest.mark.parametrize("backend", ["memory", "sqlite", "both"])
@pytest.mark.parametrize("adapter", ["pytest_plugin", "pytest_asyncio", "pytest_anyio"])
def test_report_covers_native_clients_without_opening_resources(
    pytester, monkeypatch, backend, adapter
):
    body = SYNC
    if adapter != "pytest_plugin":
        body += ASYNC.replace(
            "REPLACE_MARK", "anyio" if adapter == "pytest_anyio" else "asyncio"
        )
    args = prepare(pytester, monkeypatch, adapter, body)
    with (pytester.path / "conftest.py").open("a") as source:
        source.write(REPORT_GUARD)
    result = run_session(
        pytester,
        *args,
        "--mongoeco-backend=" + backend,
        "--mongoeco-mongodb-dialect=8.0",
        "--mongoeco-pymongo-profile=4.9",
    )
    runs = 2 if backend == "both" else 1
    result.assert_outcomes(passed=(2 if adapter == "pytest_plugin" else 4) * runs + 1)
    assert_report(
        result,
        pytester,
        backends="memory, sqlite" if backend == "both" else backend,
        adapter="none" if adapter == "pytest_plugin" else adapter,
        profile="4.9",
    )
    section = result.stdout.str().split("Mongoeco configuration", 1)[1]
    assert "mongodb://" not in section
    assert "serverSelectionTimeoutMS" not in section


@pytest.mark.parametrize("workers", [2, 4])
@pytest.mark.parametrize("adapter", ["pytest_plugin", "pytest_asyncio", "pytest_anyio"])
def test_parallel_report_is_once_on_controller(pytester, monkeypatch, workers, adapter):
    body = SYNC
    if adapter != "pytest_plugin":
        body += ASYNC.replace(
            "REPLACE_MARK", "anyio" if adapter == "pytest_anyio" else "asyncio"
        )
    args = prepare(pytester, monkeypatch, adapter, body)
    with (pytester.path / "conftest.py").open("a") as source:
        source.write(REPORT_GUARD)
    result = run_session(
        pytester,
        *args,
        "-p",
        "xdist.plugin",
        "-n",
        str(workers),
        "--mongoeco-backend=both",
        "--mongoeco-mongodb-dialect=8.0",
        "--mongoeco-pymongo-profile=4.9",
    )
    result.assert_outcomes(passed=5 if adapter == "pytest_plugin" else 9)
    assert_report(
        result,
        pytester,
        backends="memory, sqlite",
        adapter="none" if adapter == "pytest_plugin" else adapter,
        profile="4.9",
    )


@pytest.mark.parametrize("quiet", [False, True])
def test_report_resolves_installed_profile_without_using_fixtures(
    pytester, monkeypatch, quiet
):
    args = prepare(pytester, monkeypatch, body="def test_unrelated(): pass")
    with (pytester.path / "conftest.py").open("a") as source:
        source.write(REPORT_GUARD)
    if not quiet:
        args = args[:-1]
    result = run_session(
        pytester, *args, "--mongoeco-backend=both", "--mongoeco-mongodb-dialect=8.0"
    )
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["PyMongo profile: 'strict-auto-installed' -> 4.*"])
    assert_report(
        result, pytester, backends="memory, sqlite", adapter="none", profile="4.*"
    )
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0


@pytest.mark.parametrize("uses_client", [False, True])
@pytest.mark.parametrize("sdk", ["missing", "4.99.0", "installed"])
@pytest.mark.parametrize("adapter", ["pytest_plugin", "pytest_asyncio", "pytest_anyio"])
def test_invalid_configuration_keeps_original_test_outcome(
    pytester, monkeypatch, sdk, uses_client, adapter
):
    body = "def test_unrelated(): pass"
    if uses_client:
        body = "def test_configuration(mongoeco_client): pass"
        if adapter != "pytest_plugin":
            mark = "anyio" if adapter == "pytest_anyio" else "asyncio"
            body = (
                f"import pytest\n@pytest.mark.{mark}\n"
                "async def test_configuration(mongoeco_async_client): pass"
            )
    args = prepare(pytester, monkeypatch, adapter, body)
    with (pytester.path / "conftest.py").open("a") as source:
        source.write(REPORT_GUARD)
        if sdk != "installed":
            source.write(
                f"""
from importlib import metadata
import mongoeco.pytest_plugin as plugin
original_version = metadata.version

def fake_version(name):
    if name != 'pymongo':
        return original_version(name)
    if {sdk!r} == 'missing':
        raise metadata.PackageNotFoundError(name)
    return {sdk!r}

plugin.version = fake_version
metadata.version = fake_version
"""
            )
    extra = ["--mongoeco-pymongo-profile=invalid"] if sdk == "installed" else []
    result = run_session(
        pytester,
        *args,
        *extra,
        "--mongoeco-backend=both",
        "--mongoeco-mongodb-dialect=8.0",
    )
    if uses_client:
        result.assert_outcomes(errors=2)
    else:
        result.assert_outcomes(passed=1)
    assert_report(
        result,
        pytester,
        backends="memory, sqlite",
        adapter="none" if adapter == "pytest_plugin" else adapter,
        profile="invalid",
    )
    if sdk == "missing":
        result.stdout.fnmatch_lines([f"Mongoeco {__version__}; PyMongo not installed"])
    elif sdk == "4.99.0":
        result.stdout.fnmatch_lines([f"Mongoeco {__version__}; PyMongo 4.99.0"])
    if uses_client:
        expected = {
            "missing": "*requiere tener pymongo instalado*",
            "4.99.0": "*Unsupported installed PyMongo*strict-auto-installed*",
            "installed": "*Unsupported PyMongo profile: invalid*",
        }
        result.stdout.fnmatch_lines([expected[sdk]])
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0


@pytest.mark.parametrize(
    "option", ["--mongoeco-backend=invalid", "--mongoeco-mongodb-dialect=invalid"]
)
def test_invalid_cli_choices_remain_usage_errors(pytester, monkeypatch, option):
    args = prepare(pytester, monkeypatch, body="def test_unrelated(): pass")
    result = run_session(pytester, *args, option)
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*invalid choice: 'invalid'*"])
    assert "INTERNALERROR" not in result.stdout.str() + result.stderr.str()
