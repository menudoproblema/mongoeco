"""Collection references after grouping work through both public cursor APIs."""

import unittest

from itertools import product

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


_TASKS = [
    {"_id": "t1", "enrollment_id": "e1", "completed": True},
    {"_id": "t2", "enrollment_id": "e1", "completed": True},
    {"_id": "t3", "enrollment_id": "e2", "completed": True},
    {"_id": "t4", "enrollment_id": "missing", "completed": True},
    {"_id": "t5", "enrollment_id": "excluded", "completed": False},
]
_ENROLLMENTS = [{"_id": "e1"}, {"_id": "e2"}, {"_id": "unrelated"}]
_FEES = [
    {"_id": "f1", "enrollment_id": "e1"},
    {"_id": "f2", "enrollment_id": "e2"},
    {"_id": "f3", "enrollment_id": "unrelated"},
]
_ARCHIVED = [{"_id": "archived", "active": True}]
_GROUPED = [{"_id": "e1"}, {"_id": "e2"}, {"_id": "missing"}]


def _reference_cases():
    yield (
        "lookup_fields",
        [
            {
                "$lookup": {
                    "from": "enrollments",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "e",
                },
            },
        ],
        [
            {"_id": "e1", "e": [{"_id": "e1"}]},
            {"_id": "e2", "e": [{"_id": "e2"}]},
            {"_id": "missing", "e": []},
        ],
    )
    yield (
        "lookup_pipelines",
        [
            {
                "$lookup": {
                    "from": "enrollments",
                    "let": {"enrollment_id": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$_id", "$$enrollment_id"]}}},
                    ],
                    "as": "e",
                },
            },
            {
                "$lookup": {
                    "from": "fees",
                    "let": {"enrollment_id": "$_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {"$eq": ["$enrollment_id", "$$enrollment_id"]},
                            },
                        },
                    ],
                    "as": "fees",
                },
            },
        ],
        [
            {"_id": "e1", "e": [{"_id": "e1"}], "fees": [_FEES[0]]},
            {"_id": "e2", "e": [{"_id": "e2"}], "fees": [_FEES[1]]},
            {"_id": "missing", "e": [], "fees": []},
        ],
    )
    yield (
        "union_collection",
        [{"$unionWith": "archived"}],
        [*_GROUPED, *_ARCHIVED],
    )
    yield (
        "union_pipeline",
        [
            {
                "$unionWith": {
                    "coll": "archived",
                    "pipeline": [
                        {"$match": {"active": True}},
                        {"$project": {"_id": 1}},
                    ],
                },
            },
        ],
        [*_GROUPED, {"_id": "archived"}],
    )


def _pipeline(suffix):
    return [
        {"$match": {"completed": True}},
        {"$group": {"_id": "$enrollment_id"}},
        *suffix,
    ]


class AsyncGroupCollectionReferencesTests(unittest.IsolatedAsyncioTestCase):
    async def test_collection_references_after_group(self):
        for engine_type, allow_disk_use, batch_size, consume in product(
            (MemoryEngine, SQLiteEngine), (False, True), (None, 1), ("list", "iterate")
        ):
            with self.subTest(
                engine=engine_type.__name__,
                allow_disk_use=allow_disk_use,
                batch_size=batch_size,
                consume=consume,
            ):
                async with AsyncMongoClient(
                    engine_type(aggregation_spill_threshold=1)
                ) as client:
                    database = client.probe
                    await database.tasks.insert_many(_TASKS)
                    await database.enrollments.insert_many(_ENROLLMENTS)
                    await database.fees.insert_many(_FEES)
                    await database.archived.insert_many(_ARCHIVED)
                    for name, suffix, expected in _reference_cases():
                        with self.subTest(case=name):
                            cursor = database.tasks.aggregate(
                                _pipeline(suffix),
                                allow_disk_use=allow_disk_use,
                                batch_size=batch_size,
                            )
                            actual = (
                                await cursor.to_list()
                                if consume == "list"
                                else [document async for document in cursor]
                            )
                            self.assertCountEqual(actual, expected)


class SyncGroupCollectionReferencesTests(unittest.TestCase):
    def test_collection_references_after_group(self):
        for engine_type, allow_disk_use, batch_size, consume in product(
            (MemoryEngine, SQLiteEngine), (False, True), (None, 1), ("list", "iterate")
        ):
            with (
                self.subTest(
                    engine=engine_type.__name__,
                    allow_disk_use=allow_disk_use,
                    batch_size=batch_size,
                    consume=consume,
                ),
                MongoClient(engine_type(aggregation_spill_threshold=1)) as client,
            ):
                database = client.probe
                database.tasks.insert_many(_TASKS)
                database.enrollments.insert_many(_ENROLLMENTS)
                database.fees.insert_many(_FEES)
                database.archived.insert_many(_ARCHIVED)
                for name, suffix, expected in _reference_cases():
                    with self.subTest(case=name):
                        cursor = database.tasks.aggregate(
                            _pipeline(suffix),
                            allow_disk_use=allow_disk_use,
                            batch_size=batch_size,
                        )
                        actual = cursor.to_list() if consume == "list" else list(cursor)
                        self.assertCountEqual(actual, expected)
