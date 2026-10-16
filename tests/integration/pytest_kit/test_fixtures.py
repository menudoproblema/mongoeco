"""Exercise the installed or source plugin with genuine pytest sessions."""

import json
import sys
import textwrap

from pathlib import Path

import pytest


GUARD = """
import json
import socket
import threading
from urllib.parse import urlsplit
import pytest

endpoints = []
@pytest.fixture
def mongoeco_uri(mongoeco_uri):
    endpoints.append(mongoeco_uri)
    return mongoeco_uri

@pytest.fixture(autouse=True)
def cleanup_guard(tmp_path):
    yield
    for uri in endpoints:
        address = urlsplit(uri)
        with socket.socket() as sock:
            assert sock.connect_ex((address.hostname, address.port)) != 0
    assert not any(t.name == 'mongoeco-pytest-server' for t in threading.enumerate())
    assert not list(tmp_path.glob('mongoeco-*'))
    with open('cleanup.json', 'w') as f:
        json.dump({'endpoints':len(endpoints),'server_threads':0,'temporary_directories':0},f)

@pytest.fixture
def anyio_backend():
    return 'asyncio'
"""

SYNC = """
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pytest

@pytest.mark.parametrize('repeat', [1, 2])
def test_native(mongoeco_client, mongoeco_uri, repeat):
    assert type(mongoeco_client) is MongoClient
    assert mongoeco_client.options.server_selection_timeout == 3
    coll = mongoeco_client.same.items
    assert coll.count_documents({}) == 0
    coll.create_index('key', unique=True)
    result = coll.insert_one({'_id':'same', 'key':'unique'})
    assert result.inserted_id == 'same'
    with pytest.raises(DuplicateKeyError) as caught:
        coll.insert_one({'_id':'other','key':'unique'})
    assert caught.value.code == 11000
    with MongoClient(mongoeco_uri, tz_aware=True) as own:
        assert own.same.items.find_one({'_id':'same'})['key'] == 'unique'
    def spy():
        raise AssertionError('spy must not leak')
    mongoeco_client.close = spy

def test_unrelated():
    assert True
"""

ASYNC = """
import asyncio
import pytest
from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

@pytest.mark.REPLACE_MARK
@pytest.mark.parametrize('repeat', [1, 2])
async def test_native_async(mongoeco_async_client, mongoeco_uri, repeat):
    assert type(mongoeco_async_client) is AsyncMongoClient
    coll = mongoeco_async_client.same.items
    assert await coll.count_documents({}) == 0
    await coll.create_index('key', unique=True)
    await coll.insert_one({'_id':'same', 'key':'unique'})
    with pytest.raises(DuplicateKeyError) as caught:
        await coll.insert_one({'_id':'other','key':'unique'})
    assert caught.value.code == 11000
    await coll.update_one({'_id':'same'},{'$inc':{'counter':1}})
    assert (await coll.find_one({'_id':'same'}))['counter']==1
    async with AsyncMongoClient(mongoeco_uri, tz_aware=True) as own:
        assert await own.same.items.count_documents({}) == 1
    async def spy():
        raise AssertionError('spy must not leak')
    mongoeco_async_client.close = spy
"""


def run_session(pytester, *args):
    """Use separate consumer processes; include their runtime in active coverage."""
    coverage = sys.modules.get("coverage")
    active = coverage.Coverage.current() if coverage is not None else None
    if active is not None:
        data = str(
            Path(active.get_data().data_filename()).parent
            / Path(active.config.data_file).name
        )
        return pytester.run(
            sys.executable,
            "-m",
            "coverage",
            "run",
            "--parallel-mode",
            "--data-file=" + data,
            "--source=mongoeco",
            "-m",
            "pytest",
            *args,
        )
    return pytester.runpytest_subprocess(*args)


def prepare(pytester, monkeypatch, adapter="pytest_plugin", body=SYNC):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope=function")
    pytester.makeconftest(f'pytest_plugins = ("mongoeco.{adapter}",)\n' + GUARD)
    pytester.makepyfile(test_consumer=textwrap.dedent(body))
    return ["-p", "pytest_asyncio.plugin", "-p", "anyio.pytest_plugin", "-q"]


@pytest.mark.parametrize("adapter", ["pytest_plugin", "pytest_asyncio", "pytest_anyio"])
def test_native_clients_isolation_and_both_only_parametrizes_consumers(
    pytester, monkeypatch, adapter
):
    body = SYNC
    if adapter != "pytest_plugin":
        body += ASYNC.replace(
            "REPLACE_MARK", "anyio" if adapter == "pytest_anyio" else "asyncio"
        )
    args = prepare(pytester, monkeypatch, adapter, body)
    result = run_session(pytester, *args, "--mongoeco-backend=both")
    result.assert_outcomes(passed=5 if adapter == "pytest_plugin" else 9)
    receipt = json.loads((pytester.path / "cleanup.json").read_text())
    assert receipt["endpoints"] == (4 if adapter == "pytest_plugin" else 8)


@pytest.mark.parametrize("workers", [2, 4])
def test_parallel_consumers(pytester, monkeypatch, workers):
    args = prepare(pytester, monkeypatch)
    result = run_session(
        pytester,
        *args,
        "-p",
        "xdist.plugin",
        "-n",
        str(workers),
        "--mongoeco-backend=both",
    )
    result.assert_outcomes(passed=5)


@pytest.mark.parametrize("adapter", ["pytest_asyncio", "pytest_anyio"])
def test_failed_assertion_and_async_cancellation_still_finalize(
    pytester, monkeypatch, adapter
):
    mark = "anyio" if adapter == "pytest_anyio" else "asyncio"
    body = f"""
import asyncio
import pytest
@pytest.mark.{mark}
async def test_fail(mongoeco_async_client):
    await mongoeco_async_client.same.items.insert_one({{'_id':'same'}})
    raise AssertionError('deliberate assertion')
@pytest.mark.{mark}
async def test_cancel(mongoeco_async_client):
    await mongoeco_async_client.admin.command('ping')
    raise asyncio.CancelledError
"""
    args = prepare(pytester, monkeypatch, adapter, body)
    result = run_session(pytester, *args, "--mongoeco-backend=both")
    result.assert_outcomes(failed=4)
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == len(
        [1, 2]
    ) * len(["memory", "sqlite"])


def test_setup_failure_after_client_creation_finalizes(pytester, monkeypatch):
    args = prepare(
        pytester,
        monkeypatch,
        body="""
import pytest
@pytest.fixture
def broken(mongoeco_client):
    mongoeco_client.same.items.insert_one({'_id':'same'})
    raise RuntimeError('deliberate setup')
def test_setup(broken):
    pass
""",
    )
    result = run_session(pytester, *args, "--mongoeco-backend=both")
    result.assert_outcomes(errors=2)
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == len(
        ["memory", "sqlite"]
    )


@pytest.mark.parametrize("options", ["{'host':'invalid'}", "[]"])
def test_invalid_kit_options_create_no_server(pytester, monkeypatch, options):
    args = prepare(
        pytester, monkeypatch, body="def test_options(mongoeco_client): pass"
    )
    pytester.makeconftest(
        'pytest_plugins=("mongoeco.pytest_plugin",)\n'
        + GUARD
        + f"\n@pytest.fixture\ndef mongoeco_client_options(): return {options}\n"
    )
    result = run_session(pytester, *args)
    result.assert_outcomes(errors=1)
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0


def test_both_async_adapters_rejected(pytester, monkeypatch):
    prepare(pytester, monkeypatch)
    pytester.makeconftest(
        'pytest_plugins=("mongoeco.pytest_asyncio","mongoeco.pytest_anyio")'
    )
    result = run_session(pytester, "-p", "pytest_asyncio.plugin")
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*load only one Mongoeco async adapter*"])


def test_anyio_rejects_asyncio_auto(pytester, monkeypatch):
    args = prepare(pytester, monkeypatch, "pytest_anyio")
    result = run_session(pytester, *args, "--asyncio-mode=auto")
    assert result.ret == pytest.ExitCode.USAGE_ERROR


def test_asyncio_auto_mode(pytester, monkeypatch):
    args = prepare(
        pytester,
        monkeypatch,
        "pytest_asyncio",
        ASYNC.replace("REPLACE_MARK", "asyncio"),
    )
    result = run_session(
        pytester, *args, "--asyncio-mode=auto", "--mongoeco-backend=both"
    )
    result.assert_outcomes(passed=4)


def test_options_preserved(pytester, monkeypatch):
    body = """
from bson.son import SON
def test_options(mongoeco_client):
    assert mongoeco_client.codec_options.document_class is SON
    assert mongoeco_client.codec_options.tz_aware
    assert mongoeco_client.options.server_selection_timeout == 1.5
"""
    args = prepare(pytester, monkeypatch, body=body)
    pytester.makeconftest(
        'pytest_plugins=("mongoeco.pytest_plugin",)\n'
        + GUARD
        + """
from bson.son import SON
@pytest.fixture
def mongoeco_client_options():
    return {'document_class':SON,'tz_aware':True,'serverSelectionTimeoutMS':1500}
"""
    )
    run_session(pytester, *args, "--mongoeco-backend=both").assert_outcomes(passed=2)


def test_trio_backend_rejected_before_start(pytester, monkeypatch):
    args = prepare(
        pytester, monkeypatch, "pytest_anyio", ASYNC.replace("REPLACE_MARK", "anyio")
    )
    pytester.makeconftest(
        'pytest_plugins=("mongoeco.pytest_anyio",)\n'
        + GUARD
        + """
@pytest.fixture
def anyio_backend(): return 'trio'
"""
    )
    result = run_session(pytester, *args)
    result.assert_outcomes(errors=2)
    result.stdout.fnmatch_lines(["*require the AnyIO asyncio backend*"])
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0


def test_bad_profile_and_sdk_options_cleanup(pytester, monkeypatch):
    args = prepare(pytester, monkeypatch, body="def test_bad(mongoeco_client): pass")
    result = run_session(pytester, *args, "--mongoeco-pymongo-profile=invalid")
    result.assert_outcomes(errors=1)
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0
    pytester.makeconftest(
        'pytest_plugins=("mongoeco.pytest_plugin",)\n'
        + GUARD
        + """
@pytest.fixture
def mongoeco_client_options(): return {'unknownSDKOption':True}
"""
    )
    result = run_session(pytester, *args)
    result.assert_outcomes(errors=1)
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 1


def test_setup_failure_before_server_creation(pytester, monkeypatch):
    args = prepare(
        pytester,
        monkeypatch,
        body="""
import pytest
@pytest.fixture
def broken(): raise RuntimeError('before server')
def test_before(broken,mongoeco_uri): pass
""",
    )
    run_session(pytester, *args).assert_outcomes(errors=1)
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0


def test_anyio_adapter_without_asyncio_plugin(pytester, monkeypatch):
    args = prepare(
        pytester, monkeypatch, "pytest_anyio", ASYNC.replace("REPLACE_MARK", "anyio")
    )
    args = args[2:]
    pytester.makeini("[pytest]")
    run_session(pytester, *args).assert_outcomes(passed=2)


def test_asyncio_adapter_rejects_anyio_loaded_first(pytester, monkeypatch):
    prepare(pytester, monkeypatch)
    pytester.makeconftest(
        'pytest_plugins=("mongoeco.pytest_anyio","mongoeco.pytest_asyncio")'
    )
    result = run_session(pytester, "-p", "pytest_asyncio.plugin")
    assert result.ret == pytest.ExitCode.USAGE_ERROR


def test_asyncio_rejects_broader_test_loop(pytester, monkeypatch):
    body = """
import pytest
@pytest.mark.asyncio(loop_scope='module')
async def test_bad_loop(mongoeco_async_client): pass
"""
    args = prepare(pytester, monkeypatch, "pytest_asyncio", body)
    run_session(pytester, *args).assert_outcomes(errors=1)


@pytest.mark.parametrize(
    ["marker", "mode", "default_scope"],
    [
        ("@pytest.mark.asyncio", "strict", "module"),
        ("", "auto", "module"),
        ("@pytest.mark.asyncio(loop_scope=None)", "strict", "module"),
        ("@pytest.mark.asyncio(scope='module')", "strict", "function"),
    ],
)
def test_asyncio_effective_broader_loop_rejected_before_start(
    pytester, monkeypatch, marker, mode, default_scope
):
    args = prepare(
        pytester,
        monkeypatch,
        "pytest_asyncio",
        f"import pytest\n{marker}\nasync def test_loop(mongoeco_async_client): pass",
    )
    pytester.makeini(
        "[pytest]\nasyncio_default_fixture_loop_scope=function\n"
        f"asyncio_default_test_loop_scope={default_scope}\nasyncio_mode={mode}"
    )
    result = run_session(pytester, *args, "--mongoeco-backend=both")
    result.assert_outcomes(errors=2)
    result.stdout.fnmatch_lines(["*require a function-scoped test loop*"])
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0


def test_asyncio_function_marker_overrides_broader_default(pytester, monkeypatch):
    args = prepare(
        pytester,
        monkeypatch,
        "pytest_asyncio",
        ASYNC.replace("REPLACE_MARK", "asyncio(loop_scope='function')"),
    )
    pytester.makeini(
        "[pytest]\nasyncio_default_fixture_loop_scope=function\n"
        "asyncio_default_test_loop_scope=module"
    )
    run_session(pytester, *args, "--mongoeco-backend=both").assert_outcomes(passed=4)
