"""Partition, context and work-control guarantees beyond fixed native witnesses."""

import decimal
import random

from itertools import pairwise
from unittest.mock import patch

import pytest

from mongoeco import MongoClient
from mongoeco.core.aggregation import grouping_stages
from mongoeco.core.aggregation.granularity import round_boundary
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import Decimal128

from tests.differential.semantic_granularity_cases import SERIES


_CHECKPOINT_LIMIT = 4
_TIME_LIMIT_ERROR = 50
_SMALL_POSITIVE = 1e-280


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("series", SERIES)
def test_preferred_buckets_partition_documents_once(factory, series):
    values = [random.Random(seed).randrange(1000) for seed in range(41)] + [0, 0, 1]  # noqa: S311 - deterministic test data
    with MongoClient(factory()) as client:
        c = client.db.rows
        c.insert_many([{"_id": i, "v": value} for i, value in enumerate(values)])
        result = list(
            c.aggregate(
                [
                    {
                        "$bucketAuto": {
                            "groupBy": "$v",
                            "buckets": 7,
                            "granularity": series,
                            "output": {"ids": {"$push": "$_id"}},
                        }
                    }
                ]
            )
        )
        ids = [i for row in result for i in row["ids"]]
        assert sorted(ids) == list(range(len(values)))
        for left, right in pairwise(result):
            assert left["_id"]["max"] == right["_id"]["min"]
        for index, row in enumerate(result):
            lower, upper = row["_id"]["min"], row["_id"]["max"]
            for i in row["ids"]:
                assert lower <= values[i]
                assert (
                    values[i] <= upper
                    if index == len(result) - 1
                    else values[i] < upper
                )


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
def test_decimal_boundaries_do_not_use_ambient_context(factory):
    with MongoClient(factory()) as client:
        c = client.db.rows
        c.insert_many([{"v": Decimal128(str(i))} for i in range(20)])
        pipeline = [
            {"$bucketAuto": {"groupBy": "$v", "buckets": 3, "granularity": "R5"}}
        ]
        expected = list(c.aggregate(pipeline))
        with decimal.localcontext() as context:
            context.prec = 6
            context.rounding = decimal.ROUND_DOWN
            context.traps[decimal.FloatOperation] = True
            actual = list(c.aggregate(pipeline))
        assert actual == expected


def test_long_rounding_obeys_shared_work_checkpoint():
    calls = 0

    def checkpoint():
        nonlocal calls
        calls += 1
        if calls == _CHECKPOINT_LIMIT:
            msg = "work budget exhausted"
            raise OperationFailure(msg, code=_TIME_LIMIT_ERROR)

    with pytest.raises(OperationFailure) as raised:
        round_boundary(_SMALL_POSITIVE, "R5", upward=True, checkpoint=checkpoint)
    assert raised.value.code == _TIME_LIMIT_ERROR


def test_public_grouping_passes_its_checkpoint_into_rounding():
    calls = 0

    def checkpoint():
        nonlocal calls
        calls += 1
        if calls == _CHECKPOINT_LIMIT:
            msg = "work budget exhausted"
            raise OperationFailure(msg, code=_TIME_LIMIT_ERROR)

    with MongoClient(MemoryEngine()) as client:
        c = client.db.rows
        c.insert_one({"v": _SMALL_POSITIVE})
        with (
            patch.object(
                grouping_stages, "DeadlineCheckpoint", return_value=checkpoint
            ),
            pytest.raises(OperationFailure) as raised,
        ):
            list(
                c.aggregate(
                    [
                        {
                            "$bucketAuto": {
                                "groupBy": "$v",
                                "buckets": 1,
                                "granularity": "R5",
                            }
                        }
                    ]
                )
            )
        assert raised.value.code == _TIME_LIMIT_ERROR
        assert c.find_one()["v"] == _SMALL_POSITIVE
