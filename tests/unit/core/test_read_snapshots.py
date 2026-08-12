import unittest

# unittest is the repository's established async contract-test harness.
# ruff: noqa: PT027
from mongoeco.engines.adapter import adapt_engine
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy


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

    async def test_context_body_failure_is_not_masked_by_close_failure(self):
        source = _FailingSource()

        with self.assertRaisesRegex(RuntimeError, 'body failed'):
            async with ReadSnapshot(
                source,
                policy=SnapshotPolicy.STABLE,
            ) as snapshot:
                snapshot._iterator = source
                raise RuntimeError(_BODY_FAILED)

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
