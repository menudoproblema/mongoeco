import asyncio
import unittest
from unittest.mock import patch

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

    def test_async_raw_batch_cursor_closes_after_provider_failure(self):
        closed = []

        async def fetch_batch(batch_size: int):
            del batch_size
            raise RuntimeError("boom")

        async def close():
            closed.append(True)

        cursor = AsyncRawBatchCursor(fetch_batch, close=close)

        with self.assertRaisesRegex(RuntimeError, "boom"):
            asyncio.run(cursor.__anext__())
        self.assertFalse(cursor.alive)
        self.assertEqual(closed, [True])
        with self.assertRaises(StopAsyncIteration):
            asyncio.run(cursor.__anext__())

    def test_async_raw_batch_cursor_is_awaitable_and_owned_context(self):
        closed = []

        async def _exercise():
            async def fetch_batch(_batch_size: int):
                return []

            async def close():
                closed.append(True)

            cursor = AsyncRawBatchCursor(fetch_batch, close=close)
            assert await cursor is cursor
            async with cursor as owned:
                assert owned is cursor
                assert owned.alive
            await cursor.aclose()
            assert not cursor.alive

        asyncio.run(_exercise())
        self.assertEqual(closed, [True])

    def test_provider_error_survives_close_error(self):
        async def fetch_batch(_batch_size: int):
            raise RuntimeError('provider failed')

        async def close():
            raise RuntimeError('close failed')

        cursor = AsyncRawBatchCursor(fetch_batch, close=close)

        with self.assertRaisesRegex(RuntimeError, 'provider failed'):
            asyncio.run(cursor.__anext__())

    def test_encoding_error_survives_close_error(self):
        async def fetch_batch(_batch_size: int):
            return [{'_id': 1}]

        async def close():
            raise RuntimeError('close failed')

        cursor = AsyncRawBatchCursor(fetch_batch, close=close)

        with patch(
            'mongoeco.api._async.raw_batch_cursor._encode_batch',
            side_effect=RuntimeError('encode failed'),
        ):
            with self.assertRaisesRegex(RuntimeError, 'encode failed'):
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

    def test_raw_batch_cursor_context_and_liveness_delegate(self):
        async_cursor = _StubAsyncCursor()
        async_cursor.alive = True

        async def close():
            async_cursor.alive = False

        async_cursor.close = close
        cursor = RawBatchCursor(_SyncClient(), async_cursor)

        with cursor as owned:
            self.assertIs(owned, cursor)
            self.assertTrue(owned.alive)

        self.assertFalse(cursor.alive)


if __name__ == "__main__":
    unittest.main()
