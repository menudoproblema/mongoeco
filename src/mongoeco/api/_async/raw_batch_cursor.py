import asyncio

from collections.abc import Awaitable, Callable


try:  # pragma: no cover - optional dependency
    from bson import BSON
except Exception:  # pragma: no cover - bson is optional
    BSON = None

from mongoeco.errors import OperationFailure
from mongoeco.types import Document
from mongoeco.wire._lifecycle import finish_cleanup
from mongoeco.wire.bson_bridge import encode_wire_value


def _encode_batch(documents: list[Document]) -> bytes:
    if BSON is None:  # pragma: no cover - guarded by callers
        raise OperationFailure(
            "raw BSON batches require the optional 'pymongo'/'bson' dependency"
        )
    return b"".join(BSON.encode(encode_wire_value(document)) for document in documents)


class AsyncRawBatchCursor:
    """Cursor async que expone lotes BSON concatenados."""

    def __init__(
        self,
        fetch_batch: Callable[[int | None], Awaitable[list[Document]]],
        *,
        batch_size: int | None = None,
        close: Callable[[], Awaitable[None]] | None = None,
        initialize: Callable[[], Awaitable[object]] | None = None,
        alive: Callable[[], bool] | None = None,
    ) -> None:
        self._fetch_batch = fetch_batch
        self._batch_size = batch_size or None
        self._close_callback = close
        self._initialize_callback = initialize
        self._alive_callback = alive
        self._initialized = False
        self._started = False
        self._closed = False
        self._failed = False

    def __await__(self):
        return self._initialize().__await__()

    async def _initialize(self):
        if not self._initialized and not self._closed:
            self._initialized = True
            try:
                if self._initialize_callback is not None:
                    await self._initialize_callback()
            except BaseException as error:
                await self._close_after_failure(error)
                raise
        return self

    async def _close_after_failure(self, error):
        self._failed = True
        try:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                await self.close()
            else:
                await finish_cleanup(self.close())
        except BaseException as cleanup_error:
            error.add_note(f"raw cursor cleanup failed: {cleanup_error}")

    async def __aenter__(self) -> "AsyncRawBatchCursor":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    def __aiter__(self) -> "AsyncRawBatchCursor":
        return self

    async def __anext__(self) -> bytes:
        if self._closed or self._failed:
            raise StopAsyncIteration
        await self._initialize()
        if not self.alive:
            await self.close()
            raise StopAsyncIteration
        try:
            maximum = self._batch_size if self._started else (self._batch_size or 101)
            self._started = True
            documents = await self._fetch_batch(maximum)
        except BaseException as error:
            await self._close_after_failure(error)
            raise
        if not documents:
            await self.close()
            raise StopAsyncIteration
        try:
            return _encode_batch(documents)
        except BaseException as error:
            await self._close_after_failure(error)
            raise

    async def to_list(self) -> list[bytes]:
        batches: list[bytes] = []
        async for batch in self:
            batches.append(batch)
        return batches

    async def first(self) -> bytes | None:
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            return None

    async def close(self) -> None:
        if self._closed:
            return
        if self._close_callback is not None:
            await self._close_callback()
        self._closed = True

    async def aclose(self) -> None:
        await self.close()

    @property
    def alive(self) -> bool:
        return not (self._closed or self._failed) and (
            self._alive_callback is None or self._alive_callback()
        )
