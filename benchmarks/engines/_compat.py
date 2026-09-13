from __future__ import annotations

import inspect

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


SPILL_DIAGNOSTICS_CAPABILITY = "aggregation-spill-diagnostics"
SPILL_THRESHOLD_PARAMETER = "aggregation_spill_threshold"


def supports_keyword(callable_object: Callable[..., object], keyword: str) -> bool:
    """Return whether a historical engine constructor accepts a keyword."""
    try:
        parameters = inspect.signature(callable_object).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == keyword or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def spill_threshold_options(
    engine_type: Callable[..., object],
    threshold: int,
) -> dict[str, Any]:
    """Build optional tuning arguments without misrepresenting old engines."""
    if not supports_keyword(engine_type, SPILL_THRESHOLD_PARAMETER):
        return {}
    return {SPILL_THRESHOLD_PARAMETER: threshold}


def benchmark_capabilities_for(
    engine_type: Callable[..., object],
    capabilities: frozenset[str],
) -> frozenset[str]:
    """Remove spill diagnostics when their threshold cannot be controlled."""
    if supports_keyword(engine_type, SPILL_THRESHOLD_PARAMETER):
        return capabilities
    return capabilities - {SPILL_DIAGNOSTICS_CAPABILITY}
