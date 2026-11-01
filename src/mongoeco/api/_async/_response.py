"""Form one BSON response before its public decoder can observe any documents."""

from mongoeco.core.bson_size import bson_cstring_size, bson_document_size
from mongoeco.core.error_documents import render_bson_diagnostic
from mongoeco.core.server_errors import ErrorScope, server_error


_RESPONSE_BYTES = 16 * 1024 * 1024
_RPC_HEADER_BYTES = 21  # OP_MSG header, flags and body-section discriminator.
_EMPTY_DOCUMENT_BYTES = 5
_OPEN_DOCUMENT_TERMINATORS = 3  # body, cursor, and batch are not finished yet.
_MAX_RETURNED_DOCUMENT_BYTES = 16 * 1024 * 1024 + 16 * 1024


class ResponseBatchReader:
    """Own the one evaluated document that did not fit in the preceding batch."""

    def __init__(self, *, dialect, aggregate=False):
        self._dialect = dialect
        self._aggregate = aggregate
        self._pending = None

    def clear(self):
        self._pending = None

    async def next_document(self, pull):
        if self._pending is not None:
            document, self._pending = self._pending, None
            return document
        return await pull()

    async def read(self, pull, maximum, *, initial):
        if maximum == 0:
            return []
        batch_field = "firstBatch" if initial else "nextBatch"
        used = (
            _RPC_HEADER_BYTES
            + bson_document_size({"cursor": {batch_field: []}})
            - _OPEN_DOCUMENT_TERMINATORS
        )
        # Batched find/getMore appending reserves even an empty resume token.
        # The 7.x first find and aggregate's firstBatch use the older appender.
        reserve = (
            0
            if initial and (self._aggregate or self._dialect.key == "7.0")
            else _EMPTY_DOCUMENT_BYTES
        )
        batch = []

        async def next_output():
            remaining = None if maximum is None else maximum - len(batch)
            return await pull(remaining)

        while maximum is None or len(batch) < maximum:
            try:
                document = await self.next_document(next_output)
            except StopAsyncIteration:
                break
            size = bson_document_size(document)
            if size > _MAX_RETURNED_DOCUMENT_BYTES:
                maximum_size = _MAX_RETURNED_DOCUMENT_BYTES
                if self._dialect.key == "9.0":
                    message = (
                        "Serializing Document failed :: caused by :: "
                        f"Size {size} exceeds maximum {maximum_size}"
                    )
                else:
                    key, value = next(iter(document.items()))
                    message = (
                        f"BSONObj size: {size} (0x{size:X}) is invalid. Size must be "
                        f"between 0 and {maximum_size}(16MB) First element: "
                        f"{key}: {render_bson_diagnostic(value)}"
                    )
                raise server_error(
                    message, code=10334, scope=ErrorScope.COMMAND,
                    reason="bson_document_size",
                )
            # Like mongod, accept a first document to ensure progress. Document
            # admission/maximum object size is a separate boundary contract.
            if batch and used + size + reserve > _RESPONSE_BYTES:
                self._pending = document
                break
            used += size + 1 + bson_cstring_size(str(len(batch)))
            batch.append(document)
        return batch
