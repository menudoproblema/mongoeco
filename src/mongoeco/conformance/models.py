from __future__ import annotations

import json

from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum


CONFORMANCE_REPORT_SCHEMA_VERSION = "mongoeco-conformance-report/v1"


class ConformanceProfile(StrEnum):
    SPI_V2_CORE = "spi-v2-core"
    SPI_V2_ATOMICITY = "spi-v2-atomicity"
    SPI_V2_CLOCK = "spi-v2-clock"
    SPI_V2_SNAPSHOTS = "spi-v2-snapshots"
    SPI_V2_CHANGE_DELIVERY = "spi-v2-change-delivery"
    SEARCH_V1 = "search-v1"


class ConformanceStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    NOT_APPLICABLE = "not-applicable"
    INAPPLICABLE = "not-applicable"  # Compatibility alias for 4.6 prereleases.


class ConformancePhase(StrEnum):
    CONTRACT = "contract"
    CLEANUP = "cleanup"


DEFAULT_SPI_V2_PROFILES = (
    ConformanceProfile.SPI_V2_CORE,
    ConformanceProfile.SPI_V2_ATOMICITY,
    ConformanceProfile.SPI_V2_CLOCK,
    ConformanceProfile.SPI_V2_SNAPSHOTS,
    ConformanceProfile.SPI_V2_CHANGE_DELIVERY,
    ConformanceProfile.SEARCH_V1,
)


@dataclass(frozen=True, slots=True, init=False)
class ConformanceCheckResult:
    profile: ConformanceProfile
    name: str
    passed: bool
    status: ConformanceStatus
    capability: str
    phase: ConformancePhase = ConformancePhase.CONTRACT
    duration_ms: float = 0.0
    detail: str | None = None
    evidence: dict[str, object] = field(default_factory=dict)
    cleanup_error: str | None = None
    check_id: str | None = None

    def __init__(  # noqa: PLR0913 - public 4.5 compatibility signature
        self,
        profile: ConformanceProfile,
        name: str,
        passed: bool | None = None,  # noqa: FBT001 - public 4.5 signature
        detail: str | None = None,
        *,
        status: ConformanceStatus | None = None,
        capability: str = "unspecified",
        phase: ConformancePhase = ConformancePhase.CONTRACT,
        duration_ms: float = 0.0,
        evidence: dict[str, object] | None = None,
        cleanup_error: str | None = None,
        check_id: str | None = None,
    ) -> None:
        """Build a typed result while retaining the public 4.5 constructor."""
        if status is None:
            if not isinstance(passed, bool):
                message = "conformance result requires status or passed"
                raise TypeError(message)
            status = ConformanceStatus.PASSED if passed else ConformanceStatus.FAILED
            if not passed and detail is None:
                detail = "legacy conformance check failed"
        derived_passed = status is ConformanceStatus.PASSED
        if passed is not None and passed is not derived_passed:
            message = "conformance passed alias contradicts status"
            raise ValueError(message)
        object.__setattr__(self, "profile", profile)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "passed", derived_passed)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "capability", capability)
        object.__setattr__(self, "phase", phase)
        object.__setattr__(self, "duration_ms", duration_ms)
        object.__setattr__(self, "detail", detail)
        object.__setattr__(self, "evidence", {} if evidence is None else evidence)
        object.__setattr__(self, "cleanup_error", cleanup_error)
        object.__setattr__(self, "check_id", check_id)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.profile, ConformanceProfile):
            message = "conformance profile must be ConformanceProfile"
            raise TypeError(message)
        if not isinstance(self.name, str) or not self.name:
            message = "conformance check name must be a non-empty string"
            raise ValueError(message)
        if not isinstance(self.status, ConformanceStatus):
            message = "conformance status must be ConformanceStatus"
            raise TypeError(message)
        if not isinstance(self.capability, str) or not self.capability:
            message = "conformance capability must be a non-empty string"
            raise ValueError(message)
        if not isinstance(self.phase, ConformancePhase):
            message = "conformance phase must be ConformancePhase"
            raise TypeError(message)
        if (
            not isinstance(self.duration_ms, (int, float))
            or isinstance(self.duration_ms, bool)
            or self.duration_ms < 0
        ):
            message = "conformance duration_ms must be non-negative"
            raise ValueError(message)
        if self.detail is not None and (
            not isinstance(self.detail, str) or not self.detail
        ):
            message = "conformance detail must be non-empty or None"
            raise ValueError(message)
        if not isinstance(self.evidence, dict):
            message = "conformance evidence must be a document"
            raise TypeError(message)
        if self.cleanup_error is not None and (
            not isinstance(self.cleanup_error, str) or not self.cleanup_error
        ):
            message = "cleanup_error must be non-empty or None"
            raise ValueError(message)
        identifier = self.check_id or f"{self.profile.value}:{self.name}"
        if not isinstance(identifier, str) or not identifier:
            message = "conformance check_id must be a non-empty string"
            raise ValueError(message)
        object.__setattr__(self, "check_id", identifier)
        object.__setattr__(self, "duration_ms", float(self.duration_ms))
        object.__setattr__(self, "evidence", deepcopy(self.evidence))
        if self.status is ConformanceStatus.PASSED and (
            self.detail is not None or self.cleanup_error is not None
        ):
            message = "passed conformance check cannot carry failure detail"
            raise ValueError(message)
        if self.status in {ConformanceStatus.FAILED, ConformanceStatus.ERROR} and (
            self.detail is None and self.cleanup_error is None
        ):
            message = "failed conformance check requires failure detail"
            raise ValueError(message)

    @property
    def applicable(self) -> bool:
        return self.status is not ConformanceStatus.NOT_APPLICABLE

    def to_document(self) -> dict[str, object | None]:
        return {
            "id": self.check_id,
            "profile": self.profile.value,
            "name": self.name,
            "status": self.status.value,
            "passed": self.passed,
            "applicable": self.applicable,
            "capability": self.capability,
            "phase": self.phase.value,
            "durationMs": self.duration_ms,
            "detail": self.detail,
            "evidence": deepcopy(self.evidence),
            "cleanupError": self.cleanup_error,
        }


@dataclass(frozen=True, slots=True)
class ConformanceReport:
    provider_name: str
    contract_version: str
    checks: tuple[ConformanceCheckResult, ...]
    schema_version: str = CONFORMANCE_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.provider_name, str) or not self.provider_name:
            message = "conformance provider_name must be a non-empty string"
            raise ValueError(message)
        if not isinstance(self.contract_version, str) or not self.contract_version:
            message = "conformance contract_version must be a non-empty string"
            raise ValueError(message)
        if self.schema_version != CONFORMANCE_REPORT_SCHEMA_VERSION:
            message = "unsupported conformance report schema version"
            raise ValueError(message)
        if not isinstance(self.checks, tuple) or not all(
            isinstance(check, ConformanceCheckResult) for check in self.checks
        ):
            message = "conformance checks must be a tuple of check results"
            raise TypeError(message)
        identifiers = [check.check_id for check in self.checks]
        if len(identifiers) != len(set(identifiers)):
            message = "conformance check IDs must be unique"
            raise ValueError(message)

    @property
    def passed(self) -> bool:
        applicable = tuple(check for check in self.checks if check.applicable)
        return bool(applicable) and all(check.passed for check in applicable)

    @property
    def failures(self) -> tuple[ConformanceCheckResult, ...]:
        return tuple(
            check
            for check in self.checks
            if check.status in {ConformanceStatus.FAILED, ConformanceStatus.ERROR}
        )

    @property
    def inapplicable(self) -> tuple[ConformanceCheckResult, ...]:
        return tuple(
            check
            for check in self.checks
            if check.status is ConformanceStatus.NOT_APPLICABLE
        )

    def require_success(self) -> None:
        if self.passed:
            return
        if not any(check.applicable for check in self.checks):
            message = f"{self.provider_name} ran no applicable conformance checks"
            raise AssertionError(message)
        detail = "; ".join(
            f"{failure.check_id}: {failure.detail or failure.cleanup_error}"
            for failure in self.failures
        )
        message = f"{self.provider_name} failed conformance: {detail}"
        raise AssertionError(message)

    def to_document(self) -> dict[str, object]:
        return {
            "schemaVersion": self.schema_version,
            "contractVersion": self.contract_version,
            "provider": self.provider_name,
            "passed": self.passed,
            "summary": {
                status.value: sum(check.status is status for check in self.checks)
                for status in ConformanceStatus
            },
            "checks": [check.to_document() for check in self.checks],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(
            self.to_document(),
            indent=indent,
            sort_keys=True,
            separators=(",", ":") if indent is None else None,
        )

    def human_summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        counts = self.to_document()["summary"]
        rendered = ", ".join(f"{name}={value}" for name, value in counts.items())
        return f"{status} {self.provider_name} [{self.contract_version}] {rendered}"
