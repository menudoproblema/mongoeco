"""Report captured wire metadata after teardown, including across xdist workers."""

from __future__ import annotations

import json

from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from collections.abc import Generator

    from _pytest.terminal import TerminalReporter

    from mongoeco._pytest_runtime import ProxyThread


SERVER: pytest.StashKey[ProxyThread] = pytest.StashKey()
FAILED = pytest.StashKey[bool]()
PROPERTY = "mongoeco_wire_diagnostics"


class WireDiagnosticsReporter:
    def __init__(self) -> None:
        self._failures: list[tuple[str, str]] = []

    @pytest.hookimpl(wrapper=True, tryfirst=True)
    def pytest_runtest_makereport(
        self, item: pytest.Item
    ) -> Generator[None, pytest.TestReport, pytest.TestReport]:
        report = yield
        if report.failed:
            item.stash[FAILED] = True
        if report.when != "teardown":
            return report
        failed = item.stash.get(FAILED, False)
        if FAILED in item.stash:
            del item.stash[FAILED]
        server = item.stash.get(SERVER, None)
        if SERVER in item.stash:
            del item.stash[SERVER]
        if (
            failed
            and isinstance(item, pytest.Function)
            and "mongoeco_uri" in item.fixturenames
        ):
            snapshot = (
                server.diagnostics
                if server is not None
                else {"status": "not_started", "events": [], "resources": {}}
            )
            report.user_properties.append(
                (PROPERTY, json.dumps(snapshot, sort_keys=True))
            )
        return report

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when == "teardown":
            for name, value in report.user_properties:
                if name == PROPERTY:
                    self._failures.append((report.nodeid, str(value)))

    def pytest_terminal_summary(self, terminalreporter: TerminalReporter) -> None:
        if self._failures:
            terminalreporter.section("Mongoeco wire diagnostics (after teardown)")
            for nodeid, captured in self._failures:
                terminalreporter.write_line(nodeid)
                terminalreporter.write_line(json.dumps(json.loads(captured), indent=2))
