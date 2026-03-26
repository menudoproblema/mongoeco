import time

from mongoeco.errors import ExecutionTimeout


def operation_deadline(max_time_ms: int | None) -> float | None:
    if max_time_ms is None or max_time_ms <= 0:
        return None
    return time.monotonic() + (max_time_ms / 1000)


def enforce_deadline(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() > deadline:
        raise ExecutionTimeout("operation exceeded time limit")
