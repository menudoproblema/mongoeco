from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ConformanceProfile(StrEnum):
    SPI_V2_CORE = "spi-v2-core"
    SPI_V2_ATOMICITY = "spi-v2-atomicity"
    SPI_V2_CLOCK = "spi-v2-clock"
    SPI_V2_SNAPSHOTS = "spi-v2-snapshots"
    SPI_V2_CHANGE_DELIVERY = "spi-v2-change-delivery"
    SEARCH_V1 = "search-v1"


DEFAULT_SPI_V2_PROFILES = (
    ConformanceProfile.SPI_V2_CORE,
    ConformanceProfile.SPI_V2_ATOMICITY,
    ConformanceProfile.SPI_V2_CLOCK,
    ConformanceProfile.SPI_V2_SNAPSHOTS,
    ConformanceProfile.SPI_V2_CHANGE_DELIVERY,
    ConformanceProfile.SEARCH_V1,
)


@dataclass(frozen=True, slots=True)
class ConformanceCheckResult:
    profile: ConformanceProfile
    name: str
    passed: bool
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ConformanceReport:
    provider_name: str
    contract_version: str
    checks: tuple[ConformanceCheckResult, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failures(self) -> tuple[ConformanceCheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed)

    def require_success(self) -> None:
        if self.passed:
            return
        detail = "; ".join(
            f"{failure.profile.value}/{failure.name}: {failure.detail}"
            for failure in self.failures
        )
        message = f"{self.provider_name} failed conformance: {detail}"
        raise AssertionError(message)
