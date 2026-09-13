from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.core.operation_limits import enforce_deadline


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


DEADLINE_CHECK_INTERVAL = 256


class DeadlineCheckpoint:
    """Amortize deadline checks while bounding uninterrupted Python work."""

    __slots__ = ("_deadline", "_iterations")

    def __init__(self, deadline: float | None) -> None:
        self._deadline = deadline
        self._iterations = 0

    def __call__(self) -> None:
        if self._deadline is None:
            return
        if self._iterations % DEADLINE_CHECK_INTERVAL == 0:
            enforce_deadline(self._deadline)
        self._iterations += 1


def iter_with_deadline[T](
    items: Iterable[T],
    deadline: float | None,
) -> Iterator[T]:
    """Iterate with bounded extra work between monotonic deadline checks."""
    if deadline is None:
        return iter(items)
    return _iter_with_deadline(items, deadline)


def _iter_with_deadline[T](
    items: Iterable[T],
    deadline: float,
) -> Iterator[T]:
    checkpoint = DeadlineCheckpoint(deadline)
    for item in items:
        checkpoint()
        yield item
