import asyncio
import unittest

from unittest.mock import patch

from mongoeco.engines import snapshots as snapshots_module

# unittest is the repository's established async contract-test harness.
# ruff: noqa: PT027
from mongoeco.engines.adapter import adapt_engine
from mongoeco.engines.snapshots import (
    ReadSnapshot,
    SnapshotLifecycle,
    SnapshotPolicy,
)


_SOURCE_FAILED = 'source failed'
_CLOSE_FAILED = 'close failed'
_ITERATOR_CLOSE_FAILED = 'iterator close failed'
_BODY_FAILED = 'body failed'


class _Source:
    def __init__(self):
        self.values = iter([{'_id': 1}, {'_id': 2}])
        self.close_calls = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.values)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self):
        self.close_calls += 1


class _FailingSource(_Source):
    async def __anext__(self):
        raise RuntimeError(_SOURCE_FAILED)

    async def aclose(self):
        self.close_calls += 1
        raise RuntimeError(_CLOSE_FAILED)


class _SeparateIterator:
    def __init__(self):
        self.close_calls = 0

    async def __anext__(self):
        raise StopAsyncIteration

    async def aclose(self):
        self.close_calls += 1
        raise RuntimeError(_ITERATOR_CLOSE_FAILED)


class _SeparateSource(_Source):
    def __init__(self):
        super().__init__()
        self.iterator = _SeparateIterator()

    def __aiter__(self):
        return self.iterator


class _FailingIteratorFactory(_Source):
    def __aiter__(self):
        raise RuntimeError(_SOURCE_FAILED)


class _CancelledSource(_Source):
    async def __anext__(self):
        await asyncio.Event().wait()
        raise StopAsyncIteration


class _SlowCloseSource(_Source):
    def __init__(self):
        super().__init__()
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.close_completed = False

    async def aclose(self):
        self.close_calls += 1
        self.close_started.set()
        await self.allow_close.wait()
        self.close_completed = True


class _SlowFailCloseSource(_SlowCloseSource):
    async def aclose(self):
        await super().aclose()
        raise RuntimeError(_CLOSE_FAILED)


class _SuccessfulSeparateIterator:
    async def aclose(self):
        return None


class _SourceOnlyCloseFailure(_FailingSource):
    def __init__(self):
        super().__init__()
        self.iterator = _SuccessfulSeparateIterator()

    def __aiter__(self):
        return self.iterator


class ReadSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_rejects_undeclared_policy(self):
        with self.assertRaisesRegex(TypeError, 'SnapshotPolicy'):
            ReadSnapshot(_Source(), policy='stable')

    async def test_closed_snapshot_stops_without_reopening_source(self):
        snapshot = ReadSnapshot(_Source(), policy=SnapshotPolicy.STABLE)
        await snapshot.aclose()

        with self.assertRaises(StopAsyncIteration):
            await snapshot.__anext__()

    async def test_close_attempts_all_resources_and_keeps_first_error(self):
        source = _SeparateSource()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)
        snapshot._iterator = source.iterator

        with self.assertRaisesRegex(RuntimeError, 'iterator close failed'):
            await snapshot.aclose()

        assert source.iterator.close_calls == 1
        assert source.close_calls == 1

    async def test_snapshot_owns_and_closes_source_once(self):
        source = _Source()
        snapshot = ReadSnapshot(
            source,
            policy=SnapshotPolicy.STABLE,
            operation_id='operation',
        )

        assert await snapshot.__anext__() == {'_id': 1}
        await snapshot.aclose()
        await snapshot.aclose()

        assert snapshot.closed
        assert source.close_calls == 1
        assert snapshot.metadata.policy == SnapshotPolicy.STABLE
        assert snapshot.metadata.operation_id == 'operation'

    async def test_legacy_scan_is_wrapped_as_stable_snapshot(self):
        source = _Source()

        class LegacyEngine:
            def scan_find_semantics(self, *_args, **_kwargs):
                return source

        snapshot = adapt_engine(LegacyEngine()).open_read_snapshot(
            'db',
            'coll',
            object(),
        )

        assert isinstance(snapshot, ReadSnapshot)
        assert snapshot.metadata.policy == SnapshotPolicy.STABLE
        await snapshot.aclose()

    async def test_snapshot_closes_source_when_iteration_fails(self):
        source = _FailingSource()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)

        with self.assertRaisesRegex(RuntimeError, 'source failed'):
            await snapshot.__anext__()

        assert snapshot.closed
        assert source.close_calls == 1

    async def test_snapshot_closes_source_when_iterator_creation_fails(self):
        source = _FailingIteratorFactory()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)

        with self.assertRaisesRegex(RuntimeError, 'source failed'):
            await snapshot.__anext__()

        assert snapshot.closed
        assert source.close_calls == 1

    async def test_iteration_failure_is_not_masked_by_close_failure(self):
        source = _FailingSource()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)

        with self.assertRaisesRegex(RuntimeError, 'source failed'):
            await snapshot.__anext__()

        assert snapshot.closed
        assert source.close_calls == 1

    async def test_explicit_close_reports_cleanup_failure(self):
        source = _FailingSource()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)
        snapshot._iterator = source

        with self.assertRaisesRegex(RuntimeError, 'close failed'):
            await snapshot.aclose()

        assert snapshot.closed

    async def test_cancellation_cleanup_observes_close_failures(self):
        source = _FailingSource()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)
        snapshot._iterator = source

        await snapshot._close_after_cancellation()
        await snapshot._close_owned_resources()

        assert snapshot.closed
        assert source.close_calls == 1

    async def test_observe_close_task_consumes_an_already_finished_task(self):
        close_task = asyncio.create_task(asyncio.sleep(0))
        await close_task

        ReadSnapshot._observe_close_task(close_task)

    async def test_context_body_failure_is_not_masked_by_close_failure(self):
        source = _FailingSource()

        with self.assertRaisesRegex(RuntimeError, 'body failed'):
            async with ReadSnapshot(
                source,
                policy=SnapshotPolicy.STABLE,
            ) as snapshot:
                snapshot._iterator = source
                raise RuntimeError(_BODY_FAILED)

    async def test_context_exit_reports_close_failure_without_body_error(self):
        source = _FailingSource()

        with self.assertRaisesRegex(RuntimeError, 'close failed'):
            async with ReadSnapshot(
                source,
                policy=SnapshotPolicy.STABLE,
            ) as snapshot:
                snapshot._iterator = source

    async def test_cancellation_closes_the_owned_source(self):
        source = _CancelledSource()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)
        iteration = asyncio.create_task(snapshot.__anext__())
        await asyncio.sleep(0)

        iteration.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await iteration

        assert snapshot.closed
        assert source.close_calls == 1

    async def test_repeated_cancellation_preserves_snapshot_cleanup(self):
        source = _SlowCloseSource()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)
        close = asyncio.create_task(snapshot.aclose())
        await source.close_started.wait()

        close.cancel()
        await asyncio.sleep(0)
        close.cancel()
        source.allow_close.set()

        with self.assertRaises(asyncio.CancelledError):
            await close
        assert snapshot.closed
        assert source.close_calls == 1
        assert source.close_completed

    async def test_explicit_close_has_a_bounded_cleanup_deadline(self):
        source = _SlowCloseSource()
        snapshot = ReadSnapshot(
            source,
            policy=SnapshotPolicy.STABLE,
            close_timeout_seconds=0.01,
        )

        with self.assertRaisesRegex(TimeoutError, 'cleanup exceeded'):
            await snapshot.aclose()

        assert not snapshot.closed
        assert snapshot.lifecycle is SnapshotLifecycle.CLOSING
        assert snapshot.cleanup_pending
        assert source.close_calls == 1
        source.allow_close.set()
        await asyncio.sleep(0)
        assert source.close_completed
        assert snapshot.lifecycle is SnapshotLifecycle.CLOSED
        assert not snapshot.cleanup_pending

    async def test_natural_exhaustion_uses_the_bounded_cleanup_deadline(self):
        source = _SlowCloseSource()
        source.values = iter(())
        snapshot = ReadSnapshot(
            source,
            policy=SnapshotPolicy.STABLE,
            close_timeout_seconds=0.01,
        )

        with self.assertRaisesRegex(TimeoutError, 'cleanup exceeded'):
            await snapshot.__anext__()

        assert snapshot.lifecycle is SnapshotLifecycle.CLOSING
        assert source.close_calls == 1
        source.allow_close.set()
        await asyncio.sleep(0)
        assert snapshot.lifecycle is SnapshotLifecycle.CLOSED

    async def test_supervised_cleanup_registry_is_bounded(self):
        blockers = [asyncio.Event() for _ in range(3)]
        snapshots = [
            ReadSnapshot(_Source(), policy=SnapshotPolicy.STABLE)
            for _ in blockers
        ]
        tasks = [
            asyncio.create_task(blocker.wait())
            for blocker in blockers
        ]
        supervisor_limit = 2
        try:
            with patch.object(
                snapshots_module,
                '_SUPERVISED_CLOSE_TASK_LIMIT',
                supervisor_limit,
            ):
                for snapshot, task in zip(snapshots, tasks, strict=True):
                    snapshot._close_task = task
                    snapshot._lifecycle = SnapshotLifecycle.CLOSING
                    snapshot._supervise_close_task(task)

                assert (
                    len(snapshots_module._SUPERVISED_CLOSE_TASKS)
                    <= supervisor_limit
                )
                assert sum(task.cancelled() for task in tasks) <= 1
                await asyncio.sleep(0)
                assert sum(task.cancelled() for task in tasks) == 1
        finally:
            for blocker in blockers:
                blocker.set()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_late_cleanup_failure_is_supervised_and_observable(self):
        source = _SlowFailCloseSource()
        snapshot = ReadSnapshot(
            source,
            policy=SnapshotPolicy.STABLE,
            close_timeout_seconds=0.01,
        )
        loop = asyncio.get_running_loop()
        loop_errors = []
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(
            lambda _loop, context: loop_errors.append(context),
        )

        try:
            with self.assertRaisesRegex(TimeoutError, 'cleanup exceeded'):
                await snapshot.aclose()
            source.allow_close.set()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

        assert snapshot.lifecycle is SnapshotLifecycle.FAILED
        assert isinstance(snapshot.close_error, RuntimeError)
        assert loop_errors == []
        snapshot._supervise_close_task(snapshot._close_task)

    async def test_observe_close_task_supervises_a_pending_task(self):
        close_task = asyncio.create_task(asyncio.sleep(0))

        ReadSnapshot._observe_close_task(close_task)
        await close_task

    async def test_snapshot_rejects_an_unbounded_cleanup_deadline(self):
        for value in (0, -1, float('inf'), True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ReadSnapshot(
                    _Source(),
                    policy=SnapshotPolicy.STABLE,
                    close_timeout_seconds=value,
                )

    async def test_source_close_failure_after_iterator_closes(self):
        source = _SourceOnlyCloseFailure()
        snapshot = ReadSnapshot(source, policy=SnapshotPolicy.STABLE)
        snapshot._iterator = source.iterator

        with self.assertRaisesRegex(RuntimeError, 'close failed'):
            await snapshot.aclose()

    async def test_snapshot_supports_owned_async_context(self):
        source = _Source()

        async with ReadSnapshot(
            source,
            policy=SnapshotPolicy.MATERIALIZED,
        ) as snapshot:
            assert await snapshot.__anext__() == {'_id': 1}

        assert snapshot.closed
        assert source.close_calls == 1


if __name__ == '__main__':
    unittest.main()
