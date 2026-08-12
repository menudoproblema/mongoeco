from __future__ import annotations

import uuid

from collections.abc import AsyncIterable, AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Self

from mongoeco.types import Document


class SnapshotPolicy(StrEnum):
    STABLE = 'stable'
    MATERIALIZED = 'materialized'
    LIVE = 'live'


@dataclass(frozen=True, slots=True)
class SnapshotMetadata:
    snapshot_id: str
    policy: SnapshotPolicy
    operation_id: str | None = None


class ReadSnapshot(AsyncIterator[Document]):
    """Owned async read view with explicit consistency and lifecycle."""

    def __init__(
        self,
        source: AsyncIterable[Document],
        *,
        policy: SnapshotPolicy,
        operation_id: str | None = None,
    ) -> None:
        if not isinstance(policy, SnapshotPolicy):
            message = 'policy must be a SnapshotPolicy'
            raise TypeError(message)
        self.metadata = SnapshotMetadata(
            snapshot_id=uuid.uuid4().hex,
            policy=policy,
            operation_id=operation_id,
        )
        self._source = source
        self._iterator: AsyncIterator[Document] | None = None
        self._closed = False

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
        if self._closed:
            raise StopAsyncIteration
        if self._iterator is None:
            self._iterator = self._source.__aiter__()
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException:
            with suppress(BaseException):
                await self.aclose()
            raise

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        iterator = self._iterator
        self._iterator = None
        first_error: BaseException | None = None
        close = getattr(iterator, 'aclose', None)
        if callable(close):
            try:
                await close()
            except BaseException as exc:
                first_error = exc
        if iterator is not self._source:
            close_source = getattr(self._source, 'aclose', None)
            if callable(close_source):
                try:
                    await close_source()
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
        if first_error is not None:
            raise first_error

    @property
    def closed(self) -> bool:
        return self._closed
