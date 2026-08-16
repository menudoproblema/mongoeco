from __future__ import annotations

import asyncio
import unittest

import pytest

from mongoeco.conformance import (
    CancellationScenario,
    ConcurrentBarrier,
    DeterministicClock,
    PartialBatchScenario,
    cancellation_factory,
    change_delivery_factory,
    outcome_factory,
    partial_batch_factory,
    runtime_metadata_factory,
    search_request_factory,
    snapshot_factory,
)
from mongoeco.core.runtime_metadata import RuntimeMetadataKey
from mongoeco.core.search_models import SearchExecutionMode
from mongoeco.engines.snapshots import SnapshotLifecycle


class ConformanceFactoryAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_barrier_releases_exactly_one_generation(self) -> None:
        barrier = ConcurrentBarrier(2)
        positions = await asyncio.gather(barrier.wait(), barrier.wait())

        self.assertEqual(sorted(positions), [0, 1])

    async def test_controlled_snapshot_exposes_cancellation_cleanup(self) -> None:
        snapshot, source = snapshot_factory([{"_id": 1}], controlled=True)
        self.assertIsNotNone(source)
        read = asyncio.create_task(snapshot.__anext__())
        await source.started.wait()

        read.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await read

        self.assertTrue(source.closed)
        self.assertIs(snapshot.lifecycle, SnapshotLifecycle.CLOSED)

    async def test_public_cancellation_factory_executes_the_full_scenario(self) -> None:
        scenario = cancellation_factory([{"_id": 1}])

        self.assertIsInstance(scenario, CancellationScenario)
        await scenario.cancel_pending_read()

        self.assertTrue(scenario.cancelled)
        self.assertTrue(scenario.snapshot.closed)
        self.assertTrue(scenario.source.closed)


def test_clock_outcomes_batch_and_change_factories_are_deterministic() -> None:
    expected_captures = 2
    clock = DeterministicClock()
    first = clock.capture_context()
    second = clock.capture_context()
    outcomes = outcome_factory(commit_sequence=1)
    batch = partial_batch_factory(failure_index=1)
    changes = change_delivery_factory(include_gap=True)

    assert first.expressions.now == second.expressions.now
    assert first is not second
    assert clock.captures == expected_captures
    assert outcomes.matched.modified_count == 1
    assert [item["_id"] for item in batch.acknowledged_prefix] == ["batch-0"]
    assert [item["_id"] for item in batch.rejected_suffix] == [
        "batch-1",
        "batch-2",
    ]
    assert changes[-1].is_gap


def test_search_and_runtime_metadata_factories_use_public_contracts() -> None:
    request = search_request_factory(metadata=True)
    state = runtime_metadata_factory()

    assert request.mode is SearchExecutionMode.METADATA
    assert request.effective_operator == "$search"
    found, score = state.metadata_value(RuntimeMetadataKey.TEXT_SCORE)
    assert found
    assert score == 1.0
    assert "searchHighlights" in state.public_document()
    assert "searchHighlights" not in state.persistence_document()

    with pytest.raises(ValueError, match="cannot include"):
        search_request_factory(metadata=True, highlight=True)


@pytest.mark.parametrize("parties", [0, -1, True])
def test_barrier_rejects_invalid_party_counts(parties) -> None:
    with pytest.raises(ValueError, match="parties"):
        ConcurrentBarrier(parties)


def test_uncontrolled_snapshot_exhausts_and_closes_source() -> None:
    async def run() -> None:
        snapshot, source = snapshot_factory([{"_id": 1}], controlled=False)

        assert source is None
        assert await snapshot.__anext__() == {"_id": 1}
        with pytest.raises(StopAsyncIteration):
            await snapshot.__anext__()

    asyncio.run(run())


def test_partial_batch_and_change_factories_reject_invalid_ranges() -> None:
    with pytest.raises(ValueError, match="requires documents"):
        PartialBatchScenario((), 0)
    with pytest.raises(ValueError, match="out of range"):
        PartialBatchScenario(({"_id": 1},), 1)
    with pytest.raises(ValueError, match="positive"):
        change_delivery_factory(count=0)


def test_search_factory_covers_hit_highlight_and_explicit_context() -> None:
    context = DeterministicClock().capture_context()
    request = search_request_factory(context=context, highlight=True)

    assert request.operation_context is context
    assert request.mode is SearchExecutionMode.HITS
    assert request.specification["highlight"] == {"path": "title"}
