from __future__ import annotations

import threading

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(slots=True)
class _ConsumerGate:
    lock: threading.Lock = field(default_factory=threading.Lock)
    owner_thread_id: int | None = None
    users: int = 0
    retire_requested: bool = False


class ConsumerDispatchCoordinator:
    """Serialize delivery per consumer without coupling unrelated consumers."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._gates: dict[str, _ConsumerGate] = {}

    @contextmanager
    def hold(self, consumer_id: str) -> Iterator[None]:
        thread_id = threading.get_ident()
        with self._guard:
            gate = self._gates.setdefault(consumer_id, _ConsumerGate())
            if gate.owner_thread_id == thread_id:
                message = (
                    'change dispatch cannot be re-entered for the same '
                    'consumer'
                )
                raise RuntimeError(message)
            gate.users += 1
        acquired = False
        try:
            gate.lock.acquire()
            acquired = True
            with self._guard:
                gate.owner_thread_id = thread_id
            yield
        finally:
            if acquired:
                with self._guard:
                    gate.owner_thread_id = None
                gate.lock.release()
            with self._guard:
                gate.users -= 1
                if (
                    gate.retire_requested
                    and gate.users == 0
                    and self._gates.get(consumer_id) is gate
                ):
                    self._gates.pop(consumer_id, None)

    def retire(self, consumer_id: str) -> None:
        with self._guard:
            gate = self._gates.get(consumer_id)
            if gate is None:
                return
            gate.retire_requested = True
            if gate.users == 0:
                self._gates.pop(consumer_id, None)
