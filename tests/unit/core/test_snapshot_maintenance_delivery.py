"""Lazy maintenance reports its first read boundary without losing cleanup."""

import asyncio

import pytest

from mongoeco.engines.snapshots import (
    ReadSnapshot,
    SnapshotPolicy,
    _ImmediateOwnedReadSnapshot,
)


@pytest.mark.parametrize("factory", [ReadSnapshot, _ImmediateOwnedReadSnapshot])
@pytest.mark.parametrize("outcome", ["empty", "value", "error", "cancel"])
def test_first_read_notification_is_once_and_survives_terminal_paths(factory, outcome):
    async def exercise():
        observed = []
        entered = asyncio.Event()

        async def source():
            observed.append("read")
            entered.set()
            try:
                if outcome == "value":
                    yield {"_id": 1}
                elif outcome == "error":
                    message = "read failed after maintenance"
                    raise RuntimeError(message)
                elif outcome == "cancel":
                    await asyncio.Future()
            finally:
                observed.append("cleanup")

        snapshot = factory(source(), policy=SnapshotPolicy.STABLE)
        snapshot._set_first_read_callback(lambda: observed.append("delivery"))
        if outcome == "value":
            assert await anext(snapshot) == {"_id": 1}
            assert observed == ["read", "delivery"]
            await snapshot.aclose()
        elif outcome == "error":
            with pytest.raises(RuntimeError, match="read failed after maintenance"):
                await anext(snapshot)
        elif outcome == "cancel":
            task = asyncio.create_task(anext(snapshot))
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(StopAsyncIteration):
                await anext(snapshot)
        await snapshot.aclose()
        assert observed.count("delivery") == 1
        assert observed.count("cleanup") == 1
        with pytest.raises(StopAsyncIteration):
            await anext(snapshot)
        assert observed.count("delivery") == 1

    asyncio.run(exercise())


def test_closing_an_unread_snapshot_does_not_run_maintenance_or_keep_callback():
    async def exercise():
        observed = []

        async def source():
            observed.append("read")
            yield {}

        snapshot = ReadSnapshot(source(), policy=SnapshotPolicy.STABLE)
        snapshot._set_first_read_callback(lambda: observed.append("delivery"))
        await snapshot.aclose()
        assert observed == []
        assert snapshot._first_read_callback is None

    asyncio.run(exercise())
