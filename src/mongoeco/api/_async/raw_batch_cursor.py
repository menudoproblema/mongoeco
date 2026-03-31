from collections.abc import Awaitable, Callable

try:  # pragma: no cover - optional dependency
    from bson import BSON
except Exception:  # pragma: no cover - bson is optional
    BSON = None

from mongoeco.errors import OperationFailure
from mongoeco.types import Document
from mongoeco.wire.bson_bridge import encode_wire_value


def _encode_batch(documents: list[Document]) -> bytes:
    if BSON is None:  # pragma: no cover - guarded by callers
        raise OperationFailure("raw BSON batches require the optional 'pymongo'/'bson' dependency")
    return b"".join(BSON.encode(encode_wire_value(document)) for document in documents)


class AsyncRawBatchCursor:
    """Cursor async que expone lotes BSON concatenados."""

    def __init__(
        self,
        fetch_batch: Callable[[int], Awaitable[list[Document]]],
        *,
        batch_size: int | None = None,
    ) -> None:
        self._fetch_batch = fetch_batch
        self._batch_size = batch_size or 101
        self._closed = False

    def __aiter__(self) -> "AsyncRawBatchCursor":
        return self

    async def __anext__(self) -> bytes:
        if self._closed:
            raise StopAsyncIteration
        documents = await self._fetch_batch(self._batch_size)
        if not documents:
            self._closed = True
            raise StopAsyncIteration
        return _encode_batch(documents)

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
