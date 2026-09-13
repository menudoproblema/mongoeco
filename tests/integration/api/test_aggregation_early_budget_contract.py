"""Existing aggregation budgets reject after the first conclusive source row."""

import asyncio

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.api._async.cursor import AsyncCursor
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import OperationFailure


_MATERIALIZATION_LIMIT = 5
_DOCUMENT_COUNT = 500


class _CountingMemoryEngine(MemoryEngine):
    def __init__(self):
        super().__init__(
            aggregation_materialization_limit=_MATERIALIZATION_LIMIT,
        )
        self.scanned_documents = 0

    def scan_find_semantics(self, *args, **kwargs):
        source = super().scan_find_semantics(*args, **kwargs)

        async def counted():
            async for document in source:
                self.scanned_documents += 1
                yield document

        return counted()


def test_blocking_pipeline_rejects_after_limit_plus_one_source_documents():
    async def exercise():
        engine = _CountingMemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": value, "group": value % 3} for value in range(_DOCUMENT_COUNT)]
            )
            with pytest.raises(OperationFailure, match="materialization budget"):
                await collection.aggregate(
                    [{"$group": {"_id": "$group", "count": {"$sum": 1}}}],
                    allow_disk_use=False,
                ).to_list()

            assert engine.scanned_documents == _MATERIALIZATION_LIMIT + 1

    asyncio.run(exercise())


def test_blocking_pipeline_accepts_source_exactly_at_existing_limit():
    async def exercise():
        engine = _CountingMemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": value, "group": value % 2} for value in range(5)]
            )
            result = await collection.aggregate(
                [{"$group": {"_id": "$group", "count": {"$sum": 1}}}],
                allow_disk_use=False,
            ).to_list()

            assert sorted(result, key=lambda item: item["_id"]) == [
                {"_id": 0, "count": 3},
                {"_id": 1, "count": 2},
            ]
            assert engine.scanned_documents == _MATERIALIZATION_LIMIT

    asyncio.run(exercise())


def test_group_reads_source_by_batch_without_using_find_to_list():
    async def exercise():
        engine = MemoryEngine(aggregation_materialization_limit=None)
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(
                [{"_id": value, "group": value % 3} for value in range(_DOCUMENT_COUNT)]
            )
            with patch.object(
                AsyncCursor,
                "to_list",
                side_effect=AssertionError("group source must be pulled by batch"),
            ):
                result = await collection.aggregate(
                    [
                        {
                            "$group": {
                                "_id": "$group",
                                "count": {"$sum": 1},
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ],
                    batch_size=17,
                ).to_list()

            assert result == [
                {"_id": 0, "count": 167},
                {"_id": 1, "count": 167},
                {"_id": 2, "count": 166},
            ]

    asyncio.run(exercise())
