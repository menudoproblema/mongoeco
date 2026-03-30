import asyncio
import unittest

from bson import BSON

from mongoeco.api._async.raw_batch_cursor import AsyncRawBatchCursor
from mongoeco.api._sync.raw_batch_cursor import RawBatchCursor


class AsyncRawBatchCursorTests(unittest.TestCase):
    def test_async_raw_batch_cursor_first_to_list_and_stop_iteration(self):
        batches = [
            [{"_id": 1, "name": "Ada"}],
            [{"_id": 2}, {"_id": 3}],
            [],
        ]

        async def fetch_batch(batch_size: int):
            self.assertEqual(batch_size, 2)
            return batches.pop(0)

        cursor = AsyncRawBatchCursor(fetch_batch, batch_size=2)

        first_batch = asyncio.run(cursor.first())
        remaining = asyncio.run(cursor.to_list())
        no_more = asyncio.run(cursor.first())

        self.assertEqual(first_batch, BSON.encode({"_id": 1, "name": "Ada"}))
        self.assertEqual(
            remaining,
            [BSON.encode({"_id": 2}) + BSON.encode({"_id": 3})],
        )
        self.assertIsNone(no_more)

    def test_async_raw_batch_cursor_iterator_closes_after_empty_batch(self):
        async def fetch_batch(batch_size: int):
            del batch_size
            return []

        cursor = AsyncRawBatchCursor(fetch_batch)

        with self.assertRaises(StopAsyncIteration):
            asyncio.run(cursor.__anext__())
        with self.assertRaises(StopAsyncIteration):
            asyncio.run(cursor.__anext__())


class _SyncClient:
    def _run(self, awaitable):
        return asyncio.run(awaitable)


class _StubAsyncCursor:
    def __init__(self):
        self.first_batches = [b"batch-1", b"batch-2", None]

    async def to_list(self):
        return [b"batch-1", b"batch-2"]

    async def first(self):
        return self.first_batches.pop(0)


class RawBatchCursorTests(unittest.TestCase):
    def test_raw_batch_cursor_delegates_first_to_list_and_iteration(self):
        cursor = RawBatchCursor(_SyncClient(), _StubAsyncCursor())

        self.assertEqual(cursor.first(), b"batch-1")
        self.assertEqual(cursor.to_list(), [b"batch-1", b"batch-2"])
        self.assertEqual(list(cursor), [b"batch-2"])


if __name__ == "__main__":
    unittest.main()
