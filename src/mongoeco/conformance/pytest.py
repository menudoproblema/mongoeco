from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from mongoeco.conformance.models import ConformanceReport


def assert_conformance(report: ConformanceReport) -> None:
    """Expose a pytest-friendly assertion without importing pytest at runtime."""
    report.require_success()
