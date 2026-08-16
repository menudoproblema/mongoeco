from __future__ import annotations

import asyncio
import math
import uuid

from collections.abc import AsyncIterable, AsyncIterator
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from typing import Self

from mongoeco.types import Document


class SnapshotPolicy(StrEnum):
    STABLE = "stable"
    MATERIALIZED = "materialized"
    LIVE = "live"


class SnapshotLifecycle(StrEnum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


_SUPERVISED_CLOSE_TASKS: set[asyncio.Task[None]] = set()
_SUPERVISED_CLOSE_TASK_LIMIT = 256


@dataclass(frozen=True, slots=True)
class SnapshotMetadata:
    snapshot_id: str
    policy: SnapshotPolicy
    operation_id: str | None = None
    close_timeout_seconds: float = 5.0


class ReadSnapshot(AsyncIterator[Document]):
    """Owned async read view with explicit consistency and lifecycle."""

    def __init__(
        self,
        source: AsyncIterable[Document],
        *,
        policy: SnapshotPolicy,
        operation_id: str | None = None,
        close_timeout_seconds: float = 5.0,
    ) -> None:
        if not isinstance(policy, SnapshotPolicy):
            message = "policy must be a SnapshotPolicy"
            raise TypeError(message)
        if (
            not isinstance(close_timeout_seconds, (int, float))
            or isinstance(close_timeout_seconds, bool)
            or not math.isfinite(close_timeout_seconds)
            or close_timeout_seconds <= 0
        ):
            message = "close_timeout_seconds must be a positive finite number"
            raise ValueError(message)
        self.metadata = SnapshotMetadata(
            snapshot_id=uuid.uuid4().hex,
            policy=policy,
            operation_id=operation_id,
            close_timeout_seconds=float(close_timeout_seconds),
        )
        self._source = source
        self._iterator: AsyncIterator[Document] | None = None
        self._lifecycle = SnapshotLifecycle.OPEN
        self._close_error: BaseException | None = None
        self._close_task: asyncio.Task[None] | None = None

    def __aiter__(self) -> Self:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: object,
        _exc: object,
        _traceback: object,
    ) -> None:
        try:
            await self.aclose()
        except BaseException:
            if exc_type is None:
                raise

    async def __anext__(self) -> Document:
        if self._lifecycle is not SnapshotLifecycle.OPEN:
            raise StopAsyncIteration
        try:
            if self._iterator is None:
                self._iterator = self._source.__aiter__()
            return deepcopy(await self._iterator.__anext__())
        except StopAsyncIteration:
            await self.aclose()
            raise
        except asyncio.CancelledError:
            await self._close_after_cancellation()
            raise
        except BaseException:
            with suppress(BaseException):
                await self.aclose()
            raise

    async def _close_after_cancellation(self) -> None:
        close_task = self._ensure_close_task()
        try:
            await self._await_close_task(close_task)
        except (TimeoutError, asyncio.CancelledError):
            self._supervise_close_task(close_task)
        except BaseException:
            return

    async def aclose(self) -> None:
        if self._lifecycle is SnapshotLifecycle.CLOSED:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._lifecycle = SnapshotLifecycle.CLOSING
            await self._close_owned_resources()
            return
        close_task = self._ensure_close_task()
        try:
            await self._await_close_task(close_task)
        except asyncio.CancelledError:
            await self._close_after_cancellation()
            raise
        except TimeoutError:
            self._supervise_close_task(close_task)
            message = (
                "snapshot cleanup exceeded "
                f"{self.metadata.close_timeout_seconds:g} seconds"
            )
            raise TimeoutError(message) from None

    def discard(self) -> None:
        """Reject this snapshot without leaking its asynchronously owned source."""
        if self._lifecycle is not SnapshotLifecycle.OPEN:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            with suppress(BaseException):
                asyncio.run(self.aclose())
            return
        close_task = self._ensure_close_task()
        self._supervise_close_task(close_task)

    async def _await_close_task(self, close_task: asyncio.Task[None]) -> None:
        done, _pending = await asyncio.wait(
            (close_task,),
            timeout=self.metadata.close_timeout_seconds,
        )
        if close_task not in done:
            raise TimeoutError
        await close_task

    def _supervise_close_task(self, close_task: asyncio.Task[None]) -> None:
        def consume_result(task: asyncio.Task[None]) -> None:
            _SUPERVISED_CLOSE_TASKS.discard(task)
            try:
                task.result()
            except BaseException as exc:
                self._close_error = exc
                self._lifecycle = SnapshotLifecycle.FAILED

        if close_task.done():
            consume_result(close_task)
        else:
            while len(_SUPERVISED_CLOSE_TASKS) >= _SUPERVISED_CLOSE_TASK_LIMIT:
                oldest = next(iter(_SUPERVISED_CLOSE_TASKS))
                _SUPERVISED_CLOSE_TASKS.discard(oldest)
                oldest.cancel()
            _SUPERVISED_CLOSE_TASKS.add(close_task)
            close_task.add_done_callback(consume_result)

    @staticmethod
    def _observe_close_task(close_task: asyncio.Task[None]) -> None:
        """Compatibility helper retained for callers of the provisional API."""

        def consume_result(task: asyncio.Task[None]) -> None:
            with suppress(BaseException):
                task.result()

        if close_task.done():
            consume_result(close_task)
        else:
            close_task.add_done_callback(consume_result)

    def _ensure_close_task(self) -> asyncio.Task[None]:
        if self._close_task is None:
            self._lifecycle = SnapshotLifecycle.CLOSING
            self._close_task = asyncio.create_task(
                self._close_owned_resources(),
            )
        return self._close_task

    async def _close_owned_resources(self) -> None:
        if self._lifecycle in {
            SnapshotLifecycle.CLOSED,
            SnapshotLifecycle.FAILED,
        }:
            return
        self._lifecycle = SnapshotLifecycle.CLOSING
        iterator = self._iterator
        self._iterator = None
        first_error: BaseException | None = None
        close = getattr(iterator, "aclose", None)
        if callable(close):
            try:
                await close()
            except BaseException as exc:
                first_error = exc
        if iterator is not self._source:
            close_source = getattr(self._source, "aclose", None)
            if callable(close_source):
                try:
                    await close_source()
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
        if first_error is not None:
            self._close_error = first_error
            self._lifecycle = SnapshotLifecycle.FAILED
            raise first_error
        self._lifecycle = SnapshotLifecycle.CLOSED

    @property
    def closed(self) -> bool:
        return self._lifecycle in {
            SnapshotLifecycle.CLOSED,
            SnapshotLifecycle.FAILED,
        }

    @property
    def lifecycle(self) -> SnapshotLifecycle:
        return self._lifecycle

    @property
    def cleanup_pending(self) -> bool:
        return self._lifecycle is SnapshotLifecycle.CLOSING

    @property
    def close_error(self) -> BaseException | None:
        return self._close_error
