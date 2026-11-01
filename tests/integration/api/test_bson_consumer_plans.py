"""Native BSON witnesses under forced spill and bounded lookup planning."""

import asyncio

from unittest.mock import patch

import pytest

from bson import DBRef

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.core.aggregation.lookup_physical import build_bounded_lookup_hash_plan
from mongoeco.core.aggregation.spill import AggregationGroupSpool
from mongoeco.engines import MemoryEngine, SQLiteEngine

from tests.differential._real_parity_base import _normalize
from tests.differential.semantic_bson_consumer_cases import (
    bson_group_consumers,
    bson_group_representatives,
    bson_lookup_consumers,
)
from tests.integration.api.test_semantic_group_regressions import _AsyncView, _capture


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("threshold", [None, 1])
@pytest.mark.parametrize("action", [bson_group_consumers, bson_group_representatives])
def test_native_groups_and_physical_representatives_with_forced_spill(
    dialect, engine_factory, surface, threshold, action
):
    name = "semantic_" + action.__name__
    expected = _capture(dialect, "bson_consumer")["cases"][name]
    original = AggregationGroupSpool._open_temporary_partition
    with patch.object(
        AggregationGroupSpool,
        "_open_temporary_partition",
        autospec=True,
        side_effect=original,
    ) as opened:
        if surface == "sync":
            with MongoClient(
                engine_factory(aggregation_spill_threshold=threshold),
                mongodb_dialect=dialect,
            ) as client:
                actual = action(client.consumers.rows, allow_disk_use=True)
        else:

            async def run():
                async with AsyncMongoClient(
                    engine_factory(aggregation_spill_threshold=threshold),
                    mongodb_dialect=dialect,
                ) as client:
                    return await asyncio.to_thread(
                        action,
                        _AsyncView(client.consumers.rows, asyncio.get_running_loop()),
                        allow_disk_use=True,
                    )

            actual = asyncio.run(run())
        assert _normalize(actual) == _normalize(expected)
        assert bool(opened.call_count) == (threshold is not None)


_HASH_BUDGET = 1000
_COLLATION_CASES = 2


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("budget", [None, 1, _HASH_BUDGET])
def test_native_lookup_candidates_with_actual_hash_or_fallback(
    dialect, engine_factory, budget
):
    plans = []

    def build(*args, **kwargs):
        result = build_bounded_lookup_hash_plan(*args, **kwargs)
        plans.append(result)
        return result

    with (
        patch(
            "mongoeco.core.aggregation.join_stages.build_bounded_lookup_hash_plan",
            side_effect=build,
        ),
        # Isolate candidate-index budget from the independent materialization cap.
        patch.object(
            AsyncAggregationCursor, "_lookup_hash_max_associations", return_value=budget
        ),
        MongoClient(engine_factory(), mongodb_dialect=dialect) as client,
    ):
        actual = bson_lookup_consumers(client.consumers.rows)
    expected = _capture(dialect, "bson_consumer")["cases"][
        "semantic_bson_lookup_consumers"
    ]
    assert _normalize(actual) == _normalize(expected)
    assert len(plans) == _COLLATION_CASES  # simple and collated
    assert (plans[0] is not None) == (budget == _HASH_BUDGET and dialect != "9.0")
    assert plans[1] is None  # collation retains its canonical fallback
    if plans[0] is not None:
        assert plans[
            0
        ].residual_indices  # DBRef representations are deliberately unsafe
        assert plans[0].candidate_indices([DBRef("items", "abc")]) is None
