"""FIFO async admission for finite jobs submitted to a shared executor."""

from __future__ import annotations

import asyncio
import threading
import weakref

from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from concurrent.futures import Executor


@dataclass(slots=True)
class _Waiter:
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[None]
    granted: bool = False


class ExecutorAdmission:
    """Bound submitted jobs without occupying workers while callers wait."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            message = "executor admission capacity must be positive"
            raise ValueError(message)
        self.capacity = capacity
        self._in_flight = 0
        self._waiters: deque[_Waiter] = deque()
        self._lock = threading.Lock()

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._in_flight < self.capacity and not self._waiters:
                self._in_flight += 1
                return
            waiter = _Waiter(loop, loop.create_future())
            self._waiters.append(waiter)
        try:
            await waiter.future
        except BaseException:
            release_grant = False
            with self._lock:
                if waiter.granted:
                    release_grant = True
                else:
                    try:
                        self._waiters.remove(waiter)
                    except ValueError:
                        release_grant = waiter.granted
            if release_grant:
                self.release()
            raise

    @staticmethod
    def _wake(waiter: _Waiter) -> None:
        if not waiter.future.done():
            waiter.future.set_result(None)

    def release(self) -> None:
        with self._lock:
            while self._waiters:
                waiter = self._waiters.popleft()
                if waiter.future.cancelled() or waiter.loop.is_closed():
                    continue
                waiter.granted = True
                try:
                    waiter.loop.call_soon_threadsafe(self._wake, waiter)
                except RuntimeError:
                    waiter.granted = False
                    continue
                return
            if self._in_flight <= 0:
                message = "executor admission released without ownership"
                raise RuntimeError(message)
            self._in_flight -= 1

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "capacity": self.capacity,
                "inFlight": self._in_flight,
                "waiting": len(self._waiters),
            }


_ADMISSIONS: weakref.WeakKeyDictionary[Executor, ExecutorAdmission] = (
    weakref.WeakKeyDictionary()
)
_ADMISSIONS_LOCK = threading.Lock()


def executor_admission(executor: Executor, capacity: int) -> ExecutorAdmission:
    with _ADMISSIONS_LOCK:
        admission = _ADMISSIONS.get(executor)
        if admission is None:
            admission = ExecutorAdmission(capacity)
            _ADMISSIONS[executor] = admission
        elif admission.capacity != capacity:
            message = "shared executor admission capacity mismatch"
            raise RuntimeError(message)
        return admission


def executor_admission_stats(executor: Executor | None) -> dict[str, int] | None:
    if executor is None:
        return None
    with _ADMISSIONS_LOCK:
        admission = _ADMISSIONS.get(executor)
    return None if admission is None else admission.stats()
