"""Failed tests report bounded server metadata after cleanup, including xdist."""

import json

import pytest

from mongoeco._pytest_runtime import ProxyThread

from tests.integration.pytest_kit.test_fixtures import prepare, run_session
from tests.integration.pytest_kit.test_reporting import REPORT_GUARD


BACKEND_COUNT = 2

RELEASE_GUARD = """
from mongoeco._pytest_diagnostics import SERVER, FAILED

def pytest_sessionfinish(session):
    assert all(SERVER not in item.stash for item in session.items)
    assert all(FAILED not in item.stash for item in session.items)
"""


def body(adapter, phase):
    declaration = "def test_operation(mongoeco_client, failure):"
    operations = """
    mongoeco_client.probe.items.insert_one({'_id': 'PRIVATE_BSON'})
    for _ in range(45):
        mongoeco_client.admin.command('ping')
"""
    prefix = ""
    if adapter != "pytest_plugin":
        mark = "anyio" if adapter == "pytest_anyio" else "asyncio"
        prefix = f"@pytest.mark.{mark}\n"
        declaration = "async def test_operation(mongoeco_async_client, failure):"
        operations = (
            operations.replace("mongoeco_client", "mongoeco_async_client")
            .replace("    mongoeco_", "    await mongoeco_")
            .replace("        mongoeco_", "        await mongoeco_")
        )
    return f"""
import pytest

@pytest.fixture
def failure(mongoeco_uri):
    if {phase!r} == 'setup':
        raise RuntimeError('deliberate setup failure')
    yield
    if {phase!r} == 'teardown':
        raise RuntimeError('deliberate teardown failure')

{prefix}{declaration}
{operations}
    if {phase!r} == 'call':
        pytest.fail('deliberate test failure')
"""


@pytest.mark.parametrize("adapter", ["pytest_plugin", "pytest_asyncio", "pytest_anyio"])
@pytest.mark.parametrize("phase", ["setup", "call", "teardown", "pass"])
def test_failure_metadata_is_reported_after_cleanup(
    pytester, monkeypatch, adapter, phase
):
    args = prepare(pytester, monkeypatch, adapter, body(adapter, phase))
    with (pytester.path / "conftest.py").open("a") as source:
        source.write(REPORT_GUARD)
        source.write(RELEASE_GUARD)
    result = run_session(
        pytester, *args, "--mongoeco-backend=both", "--mongoeco-wire-diagnostics"
    )
    if phase == "call":
        result.assert_outcomes(failed=2)
    elif phase == "setup":
        result.assert_outcomes(errors=2)
    elif phase == "teardown":
        result.assert_outcomes(passed=2, errors=2)
    else:
        result.assert_outcomes(passed=2)
    output = result.stdout.str()
    if phase == "pass":
        assert "Mongoeco wire diagnostics (after teardown)" not in output
    else:
        assert output.count("Mongoeco wire diagnostics (after teardown)") == 1
        section = output.split("Mongoeco wire diagnostics (after teardown)", 1)[1]
        assert section.count('"engine_connected": false') == BACKEND_COUNT
        assert section.count('"handlers": 0') == BACKEND_COUNT
        assert section.count('"connections": 0') == BACKEND_COUNT
        assert section.count('"cursors": 0') == BACKEND_COUNT
        assert section.count('"sessions": 0') == BACKEND_COUNT
        assert "PRIVATE_BSON" not in section
        assert "mongodb://" not in section
        if phase != "setup":
            assert '"command": "ping"' in section
            assert '"dropped_events": 0' not in section
    assert json.loads((pytester.path / "report-resources.json").read_text()) == {
        "resource_calls": 0
    }


@pytest.mark.parametrize("adapter", ["pytest_plugin", "pytest_asyncio", "pytest_anyio"])
def test_parallel_failures_are_reported_once_on_controller(
    pytester, monkeypatch, adapter
):
    args = prepare(pytester, monkeypatch, adapter, body(adapter, "call"))
    result = run_session(
        pytester,
        *args,
        "--mongoeco-backend=both",
        "--mongoeco-wire-diagnostics",
        "-p",
        "xdist.plugin",
        "-n",
        "2",
    )
    result.assert_outcomes(failed=2)
    output = result.stdout.str()
    assert output.count("Mongoeco wire diagnostics (after teardown)") == 1
    section = output.split("Mongoeco wire diagnostics (after teardown)", 1)[1]
    assert section.count('"engine_connected": false') == BACKEND_COUNT
    assert section.count('"handlers": 0') == BACKEND_COUNT
    assert "PRIVATE_BSON" not in section


def test_capture_is_off_by_default_even_for_failures(pytester, monkeypatch):
    args = prepare(pytester, monkeypatch, body=body("pytest_plugin", "call"))
    with (pytester.path / "conftest.py").open("a") as source:
        source.write(RELEASE_GUARD)
    result = run_session(pytester, *args)
    result.assert_outcomes(failed=1)
    assert "Mongoeco wire diagnostics (after teardown)" not in result.stdout.str()


def test_invalid_profile_reports_not_started_without_creating_resources(
    pytester, monkeypatch
):
    args = prepare(
        pytester, monkeypatch, body="def test_configuration(mongoeco_client): pass"
    )
    with (pytester.path / "conftest.py").open("a") as source:
        source.write(REPORT_GUARD)
    result = run_session(
        pytester,
        *args,
        "--mongoeco-pymongo-profile=invalid",
        "--mongoeco-wire-diagnostics",
    )
    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(['*"status": "not_started"*'])
    assert json.loads((pytester.path / "cleanup.json").read_text())["endpoints"] == 0


def test_unrelated_failures_do_not_create_wire_reports(pytester, monkeypatch):
    args = prepare(pytester, monkeypatch, body="def test_unrelated(): assert False")
    result = run_session(pytester, *args, "--mongoeco-wire-diagnostics")
    result.assert_outcomes(failed=1)
    assert "Mongoeco wire diagnostics (after teardown)" not in result.stdout.str()


def test_snapshot_before_start_does_not_create_runtime(tmp_path, monkeypatch):
    server = ProxyThread("sqlite", tmp_path, "8.0", "4.19", diagnostic_capacity=128)

    def forbidden(*args, **kwargs):
        pytest.fail("snapshot must not construct runtime resources")

    monkeypatch.setattr("mongoeco._pytest_runtime.SQLiteEngine", forbidden)
    monkeypatch.setattr("mongoeco._pytest_runtime.AsyncMongoEcoProxyServer", forbidden)
    assert server.diagnostics == {
        "status": "not_started",
        "events": [],
        "resources": {},
    }
    assert server._thread.ident is None
    assert not list(tmp_path.iterdir())
