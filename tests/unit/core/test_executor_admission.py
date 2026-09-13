import asyncio
import threading

from concurrent.futures import ThreadPoolExecutor

import pytest

from mongoeco.engines._executor_admission import (
    ExecutorAdmission,
    _Waiter,
    executor_admission,
    executor_admission_stats,
)
from mongoeco.engines.sqlite import SQLiteEngine


_WORKERS = 2
_JOBS = 10


def test_executor_admission_is_fifo_without_consuming_a_worker_while_waiting():
    async def exercise():
        admission = ExecutorAdmission(1)
        await admission.acquire()
        order = []

        async def wait(number):
            await admission.acquire()
            order.append(number)

        second = asyncio.create_task(wait(2))
        third = asyncio.create_task(wait(3))
        await asyncio.sleep(0)
        assert admission.stats() == {"capacity": 1, "inFlight": 1, "waiting": 2}

        admission.release()
        await second
        admission.release()
        await third
        admission.release()

        assert order == [2, 3]
        assert admission.stats() == {"capacity": 1, "inFlight": 0, "waiting": 0}

    asyncio.run(exercise())


def test_cancelled_waiter_does_not_consume_or_leak_admission():
    async def exercise():
        admission = ExecutorAdmission(1)
        await admission.acquire()
        waiting = asyncio.create_task(admission.acquire())
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        assert admission.stats()["waiting"] == 0
        admission.release()
        assert admission.stats()["inFlight"] == 0

    asyncio.run(exercise())


def test_sqlite_submission_capacity_is_shared_and_tracks_physical_cancellation():
    async def exercise():
        engine = SQLiteEngine(executor_workers=_WORKERS)
        release = threading.Event()
        all_workers_started = threading.Event()
        state_lock = threading.Lock()
        active = 0

        def blocked_job(number):
            nonlocal active
            with state_lock:
                active += 1
                if active == _WORKERS:
                    all_workers_started.set()
            release.wait()
            return number

        tasks = [
            asyncio.create_task(engine._run_blocking(blocked_job, number))
            for number in range(_JOBS)
        ]
        try:
            assert await asyncio.wait_for(
                asyncio.to_thread(all_workers_started.wait),
                2,
            )
            admission = executor_admission(engine._executor, _WORKERS)
            assert admission.stats() == {
                "capacity": _WORKERS,
                "inFlight": _WORKERS,
                "waiting": _JOBS - _WORKERS,
            }

            tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[0]
            assert admission.stats()["inFlight"] == _WORKERS
            assert admission.stats()["waiting"] == _JOBS - _WORKERS

            release.set()
            assert await asyncio.gather(*tasks[1:]) == list(range(1, _JOBS))
            assert admission.stats()["inFlight"] == 0
            assert admission.stats()["waiting"] == 0
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            engine._shutdown_executor()

    asyncio.run(exercise())


def test_same_executor_resolves_to_one_admission_owner():
    with ThreadPoolExecutor(max_workers=_WORKERS) as executor:
        first = executor_admission(executor, _WORKERS)
        second = executor_admission(executor, _WORKERS)

    assert first is second


def test_executor_admission_rejects_invalid_capacity_and_unowned_release():
    with pytest.raises(ValueError, match="capacity must be positive"):
        ExecutorAdmission(0)

    admission = ExecutorAdmission(1)
    with pytest.raises(RuntimeError, match="released without ownership"):
        admission.release()


def test_shared_executor_rejects_a_divergent_capacity():
    with ThreadPoolExecutor(max_workers=_WORKERS) as executor:
        executor_admission(executor, _WORKERS)
        with pytest.raises(RuntimeError, match="capacity mismatch"):
            executor_admission(executor, _WORKERS + 1)

    assert executor_admission_stats(None) is None


def test_cancel_after_grant_returns_the_transferred_admission():
    async def exercise():
        admission = ExecutorAdmission(1)
        await admission.acquire()
        waiting = asyncio.create_task(admission.acquire())
        await asyncio.sleep(0)

        admission.release()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        assert admission.stats() == {
            "capacity": 1,
            "inFlight": 0,
            "waiting": 0,
        }

    asyncio.run(exercise())


def test_cancelled_waiter_already_removed_does_not_release_another_owner():
    async def exercise():
        admission = ExecutorAdmission(1)
        await admission.acquire()
        waiting = asyncio.create_task(admission.acquire())
        await asyncio.sleep(0)
        with admission._lock:
            admission._waiters.clear()

        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        assert admission.stats()["inFlight"] == 1
        admission.release()

    asyncio.run(exercise())


def test_release_skips_cancelled_closed_and_unwakeupable_waiters():
    class ClosedLoop:
        @staticmethod
        def is_closed():
            return True

    class BrokenLoop:
        @staticmethod
        def is_closed():
            return False

        @staticmethod
        def call_soon_threadsafe(*_args):
            raise RuntimeError("loop stopped")

    async def exercise():
        admission = ExecutorAdmission(1)
        await admission.acquire()
        loop = asyncio.get_running_loop()
        cancelled = loop.create_future()
        cancelled.cancel()
        admission._waiters.extend(
            (
                _Waiter(loop, cancelled),
                _Waiter(ClosedLoop(), loop.create_future()),
                _Waiter(BrokenLoop(), loop.create_future()),
            )
        )

        admission.release()

        assert admission.stats() == {
            "capacity": 1,
            "inFlight": 0,
            "waiting": 0,
        }

    asyncio.run(exercise())
