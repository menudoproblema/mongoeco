class RawBatchCursor:
    """Adaptador sync sobre un cursor async de lotes BSON."""

    def __init__(self, client, async_cursor):
        self._client = client
        self._async_cursor = async_cursor

    def __iter__(self):
        while True:
            batch = self.first()
            if batch is None:
                return
            yield batch

    def to_list(self) -> list[bytes]:
        return self._client._run(self._async_cursor.to_list())

    def first(self) -> bytes | None:
        return self._client._run(self._async_cursor.first())

    def close(self) -> None:
        self._client._run(self._async_cursor.close())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    @property
    def alive(self) -> bool:
        return self._async_cursor.alive
