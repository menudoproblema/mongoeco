"""Semantic results and validation are independent of physical cursor routes."""

import unittest

from itertools import product
from unittest.mock import patch

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import Decimal128, SearchIndexModel


_ROWS = [
    {
        "_id": "t1",
        "enrollment_id": "e1",
        "key": "Ada",
        "value": 1,
        "title": "ada",
        "profile": {"keep": 1},
    },
    {
        "_id": "t2",
        "enrollment_id": "e2",
        "key": "other",
        "value": 2,
        "title": "ada",
        "profile": {"keep": 2},
    },
    {
        "_id": "t3",
        "enrollment_id": "e1",
        "key": "ada",
        "value": 1.0,
        "title": "ada",
        "profile": {"keep": 3},
    },
]
_FOREIGN = [{"_id": "e1", "key": "ADA"}, {"_id": "e2", "key": "ada"}]
_COLLATION = {"locale": "en", "strength": 2}
_SEARCH = {"$search": {"index": "by_text", "text": {"query": "ada", "path": "title"}}}
_TEXT_INDEX = SearchIndexModel(
    {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}},
    name="by_text",
)


def _valid_cases():
    yield (
        "group_numeric",
        [{"$group": {"_id": "$value", "n": {"$sum": 1}}}],
        {},
        [{"_id": 1, "n": 2}, {"_id": 2, "n": 1}],
    )
    yield (
        "group_collation",
        [{"$group": {"_id": "$key", "n": {"$sum": 1}}}],
        {"collation": _COLLATION},
        [{"_id": "Ada", "n": 2}, {"_id": "other", "n": 1}],
    )
    yield (
        "sort_by_count",
        [{"$sortByCount": "$key"}],
        {"collation": _COLLATION},
        [{"_id": "Ada", "count": 2}, {"_id": "other", "count": 1}],
    )
    yield (
        "window_partition",
        [
            {
                "$setWindowFields": {
                    "partitionBy": "$value",
                    "output": {
                        "n": {
                            "$sum": 1,
                            "window": {"documents": ["unbounded", "unbounded"]},
                        }
                    },
                }
            },
            {"$project": {"_id": 1, "n": 1}},
        ],
        {},
        [{"_id": "t1", "n": 2}, {"_id": "t2", "n": 1}, {"_id": "t3", "n": 2}],
    )
    yield (
        "collated_window_ranks",
        [
            {
                "$setWindowFields": {
                    "sortBy": {"key": 1},
                    "output": {"r": {"$rank": {}}, "d": {"$denseRank": {}}},
                }
            },
            {"$project": {"_id": 1, "r": 1, "d": 1}},
        ],
        {"collation": _COLLATION},
        [
            {"_id": "t1", "r": 1, "d": 1},
            {"_id": "t3", "r": 1, "d": 1},
            {"_id": "t2", "r": 3, "d": 2},
        ],
    )
    mixed_value = {"$cond": [{"$eq": ["$value", 2]}, "Z", "a"]}
    yield (
        "collated_group_min_max",
        [
            {
                "$group": {
                    "_id": None,
                    "lo": {"$min": mixed_value},
                    "hi": {"$max": mixed_value},
                }
            }
        ],
        {"collation": _COLLATION},
        [{"_id": None, "lo": "a", "hi": "Z"}],
    )
    yield (
        "collated_ordered_accumulators",
        [
            {"$set": {"mixed": mixed_value}},
            {
                "$group": {
                    "_id": None,
                    "low": {"$minN": {"input": "$mixed", "n": 1}},
                    "high": {"$maxN": {"input": "$mixed", "n": 1}},
                    "top": {"$top": {"sortBy": {"mixed": 1}, "output": "$mixed"}},
                    "bottom": {"$bottom": {"sortBy": {"mixed": 1}, "output": "$mixed"}},
                    "topN": {
                        "$topN": {"sortBy": {"mixed": 1}, "output": "$mixed", "n": 1}
                    },
                    "bottomN": {
                        "$bottomN": {"sortBy": {"mixed": 1}, "output": "$mixed", "n": 1}
                    },
                }
            },
        ],
        {"collation": _COLLATION},
        [
            {
                "_id": None,
                "low": ["a"],
                "high": ["Z"],
                "top": "a",
                "bottom": "Z",
                "topN": ["a"],
                "bottomN": ["Z"],
            }
        ],
    )
    yield (
        "expr_collation",
        [{"$match": {"$expr": {"$in": ["$key", ["ADA"]]}}}, {"$project": {"_id": 1}}],
        {"collation": _COLLATION},
        [{"_id": "t1"}, {"_id": "t3"}],
    )
    yield (
        "dotted_lookup_as",
        [
            {
                "$lookup": {
                    "from": "enrollments",
                    "localField": "enrollment_id",
                    "foreignField": "_id",
                    "as": "profile.e",
                }
            },
            {"$match": {"profile.e._id": "e1"}},
            {"$project": {"_id": 1, "profile.keep": 1}},
        ],
        {},
        [{"_id": "t1", "profile": {"keep": 1}}, {"_id": "t3", "profile": {"keep": 3}}],
    )
    yield (
        "lookup_expr_collation",
        [
            {
                "$lookup": {
                    "from": "enrollments",
                    "let": {"key": "$key"},
                    "pipeline": [{"$match": {"$expr": {"$eq": ["$key", "$$key"]}}}],
                    "as": "e",
                }
            },
            {"$project": {"_id": 1, "n": {"$size": "$e"}}},
        ],
        {"collation": _COLLATION},
        [{"_id": "t1", "n": 2}, {"_id": "t2", "n": 0}, {"_id": "t3", "n": 2}],
    )
    yield (
        "foreign_index_stats",
        [
            {"$group": {"_id": None}},
            {
                "$lookup": {
                    "from": "enrollments",
                    "pipeline": [
                        {"$indexStats": {}},
                        {"$match": {"name": "foreign_only"}},
                        {"$project": {"_id": 0, "name": 1}},
                    ],
                    "as": "stats",
                }
            },
        ],
        {},
        [{"_id": None, "stats": [{"name": "foreign_only"}]}],
    )
    yield (
        "foreign_coll_stats",
        [
            {"$group": {"_id": None}},
            {
                "$unionWith": {
                    "coll": "enrollments",
                    "pipeline": [
                        {"$collStats": {"count": {}}},
                        {"$project": {"_id": 0, "n": "$count"}},
                    ],
                }
            },
        ],
        {},
        [{"_id": None}, {"n": 2}],
    )
    yield (
        "foreign_plan_cache_stats",
        [
            {"$group": {"_id": None}},
            {
                "$lookup": {
                    "from": "enrollments",
                    "pipeline": [
                        {"$planCacheStats": {}},
                        {"$project": {"_id": 0, "ns": 1}},
                    ],
                    "as": "stats",
                }
            },
        ],
        {},
        [{"_id": None, "stats": [{"ns": "probe.enrollments"}]}],
    )
    yield (
        "nested_current_collection",
        [
            {"$group": {"_id": None}},
            {
                "$lookup": {
                    "from": "enrollments",
                    "pipeline": [{"$unionWith": {"pipeline": []}}, {"$count": "n"}],
                    "as": "e",
                }
            },
        ],
        {},
        [{"_id": None, "e": [{"n": 4}]}],
    )
    yield (
        "lookup_documents",
        [
            {"$lookup": {"pipeline": [{"$documents": [{"seed": 1}]}], "as": "e"}},
            {"$project": {"_id": 1, "e": 1}},
        ],
        {},
        [{"_id": row["_id"], "e": [{"seed": 1}]} for row in _ROWS],
    )
    yield (
        "union_documents",
        [
            {"$group": {"_id": None}},
            {"$unionWith": {"pipeline": [{"$documents": [{"seed": 1}]}]}},
        ],
        {},
        [{"_id": None}, {"seed": 1}],
    )


def _invalid_cases():
    yield (
        "empty_group_lookup",
        [{"$match": {"_id": "absent"}}, {"$group": {"_id": None}}, {"$lookup": {}}],
    )
    yield "empty_lookup", [{"$match": {"_id": "absent"}}, {"$lookup": {}}]
    for name in ("Invalid", "bad\n"):
        yield (
            f"let_{name!r}",
            [
                {
                    "$lookup": {
                        "from": "enrollments",
                        "let": {name: 1},
                        "pipeline": [],
                        "as": "e",
                    }
                }
            ],
        )
    for stage in (
        "$collStats",
        "$indexStats",
        "$planCacheStats",
        "$currentOp",
        "$listSessions",
        "$documents",
    ):
        spec = (
            []
            if stage == "$documents"
            else {"count": {}}
            if stage == "$collStats"
            else {}
        )
        yield f"position_{stage}", [{"$match": {}}, {stage: spec}]
    yield "union_string_name", [{"$unionWith": "$enrollments"}]
    yield (
        "lookup_unknown_argument",
        [
            {
                "$lookup": {
                    "from": "enrollments",
                    "localField": "enrollment_id",
                    "foreignField": "_id",
                    "as": "e",
                    "unexpected": 1,
                }
            }
        ],
    )
    yield "facet_stats", [{"$facet": {"x": [{"$indexStats": {}}]}}]
    yield "nested_facet", [{"$facet": {"x": [{"$facet": {"y": []}}]}}]
    for operator, spec in (
        ("$indexStats", {}), ("$collStats", {"count": {}}),
        ("$planCacheStats", {}), ("$facet", {"y": []}),
    ):
        for join in ("$lookup", "$unionWith"):
            nested = {"pipeline": [{operator: spec}]}
            if join == "$lookup":
                nested.update({"from": "enrollments", "as": "e"})
            else:
                nested["coll"] = "enrollments"
            yield f"facet_{join}_{operator}", [
                {"$match": {"_id": "absent"}}, {"$facet": {"x": [{join: nested}]}},
            ]
    for join in ("$lookup", "$unionWith"):
        nested = {"pipeline": [{"$documents": [{"seed": 1}]}]}
        if join == "$lookup":
            nested["as"] = "e"
        yield f"facet_{join}_documents", [{"$facet": {"x": [{join: nested}]}}]
    yield (
        "nested_invalid_lookup",
        [
            {
                "$lookup": {
                    "from": "enrollments",
                    "pipeline": [{"$lookup": {}}],
                    "as": "e",
                }
            }
        ],
    )


class AsyncAggregationSemanticContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_routes_share_semantics_and_validation(self):
        for engine_type, disk, batch in product(
            (MemoryEngine, SQLiteEngine), (False, True), (None, 1)
        ):
            async with AsyncMongoClient(
                engine_type(aggregation_spill_threshold=1)
            ) as client:
                db = client.probe
                await db.rows.insert_many(_ROWS)
                await db.enrollments.insert_many(_FOREIGN)
                await db.rows.create_index("value", name="root_only")
                await db.enrollments.create_index("key", name="foreign_only")
                await db.rows.create_search_index(_TEXT_INDEX)
                for search in (False, True):
                    prefix = [_SEARCH] if search else []
                    for name, pipeline, options, expected in _valid_cases():
                        with self.subTest(
                            engine=engine_type.__name__,
                            disk=disk,
                            batch=batch,
                            search=search,
                            case=name,
                        ):
                            rows = await db.rows.aggregate(
                                [*prefix, *pipeline],
                                allow_disk_use=disk,
                                batch_size=batch,
                                **options,
                            ).to_list()
                            self.assertCountEqual(rows, expected)
                    for name, pipeline in _invalid_cases():
                        with (
                            self.subTest(
                                engine=engine_type.__name__,
                                disk=disk,
                                batch=batch,
                                search=search,
                                invalid=name,
                            ),
                            self.assertRaises(OperationFailure),
                        ):
                            await db.rows.aggregate(
                                [*prefix, *pipeline],
                                allow_disk_use=disk,
                                batch_size=batch,
                            ).to_list()

    async def test_windows_preserve_collection_context_around_group_and_sort(self):
        lookup = {
            "$lookup": {
                "from": "enrollments",
                "localField": "_id",
                "foreignField": "_id",
                "as": "e",
            }
        }
        pipelines = [
            [
                {"$set": {"marker": 1}},
                {"$skip": 1},
                {"$limit": 1},
                {"$group": {"_id": "$enrollment_id"}},
                lookup,
            ],
            [
                {"$group": {"_id": "$enrollment_id"}},
                {"$sort": {"_id": 1}},
                lookup,
                {"$skip": 1},
                {"$limit": 1},
            ],
        ]
        expected = [{"_id": "e2", "e": [_FOREIGN[1]]}]
        for engine_type, disk, batch in product(
            (MemoryEngine, SQLiteEngine), (False, True), (None, 1)
        ):
            with self.subTest(engine=engine_type.__name__, disk=disk, batch=batch):
                async with AsyncMongoClient(
                    engine_type(aggregation_spill_threshold=1)
                ) as client:
                    await client.probe.rows.insert_many(_ROWS)
                    await client.probe.enrollments.insert_many(_FOREIGN)
                    for pipeline in pipelines:
                        self.assertEqual(
                            await client.probe.rows.aggregate(
                                pipeline, allow_disk_use=disk, batch_size=batch
                            ).to_list(),
                            expected,
                        )
                with MongoClient(engine_type(aggregation_spill_threshold=1)) as client:
                    client.probe.rows.insert_many(_ROWS)
                    client.probe.enrollments.insert_many(_FOREIGN)
                    for pipeline in pipelines:
                        self.assertEqual(
                            client.probe.rows.aggregate(
                                pipeline, allow_disk_use=disk, batch_size=batch
                            ).to_list(),
                            expected,
                        )

    async def test_dependencies_are_loaded_once_in_their_own_namespace(self):
        for engine_type in (MemoryEngine, SQLiteEngine):
            with self.subTest(engine=engine_type.__name__):
                async with AsyncMongoClient(engine_type()) as client:
                    await client.probe.foreign.insert_one({"_id": "foreign"})
                    cursor = client.probe.root.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "foreign",
                                    "as": "one",
                                    "pipeline": [{"$unionWith": {"pipeline": []}}],
                                }
                            },
                            {
                                "$lookup": {
                                    "from": "foreign",
                                    "as": "two",
                                    "pipeline": [{"$indexStats": {}}],
                                }
                            },
                            {
                                "$lookup": {
                                    "from": "foreign",
                                    "as": "three",
                                    "pipeline": [{"$indexStats": {}}],
                                }
                            },
                        ]
                    )
                    self.assertEqual(await cursor._load_index_stats_snapshot([]), [])
                    self.assertEqual(cursor._load_plan_cache_stats_snapshot([]), [])
                    self.assertEqual(cursor._load_list_sessions_snapshot([]), [])
                    with (
                        patch.object(
                            cursor,
                            "_load_collection_documents",
                            wraps=cursor._load_collection_documents,
                        ) as load,
                        patch.object(
                            cursor,
                            "_load_index_stats_snapshot",
                            wraps=cursor._load_index_stats_snapshot,
                        ) as indices,
                    ):
                        resources = await cursor._load_pipeline_resources()
                    self.assertEqual(
                        [call.args[0] for call in load.await_args_list], ["foreign"]
                    )
                    self.assertEqual(indices.await_count, 1)
                    self.assertEqual(
                        resources.for_collection("foreign")(
                            "__mongoeco_current_collection__"
                        ),
                        [{"_id": "foreign"}],
                    )
                    session = client.start_session()
                    sessions = await client.config.system.sessions.aggregate(
                        [
                            {"$listSessions": {}},
                            {"$match": {"_id.id": session.session_id}},
                        ],
                        session=session,
                    ).to_list()
                    self.assertEqual(
                        [row["_id"]["id"] for row in sessions], [session.session_id]
                    )
                    operations = await client.admin.operations.aggregate(
                        [{"$currentOp": {}}, {"$match": {"active": True}}]
                    ).to_list()
                    self.assertIsInstance(operations, list)


class SyncAggregationSemanticContextTests(unittest.TestCase):
    def test_routes_share_semantics_and_validation(self):
        for engine_type, disk, batch in product(
            (MemoryEngine, SQLiteEngine), (False, True), (None, 1)
        ):
            with MongoClient(engine_type(aggregation_spill_threshold=1)) as client:
                db = client.probe
                db.rows.insert_many(_ROWS)
                db.enrollments.insert_many(_FOREIGN)
                db.rows.create_index("value", name="root_only")
                db.enrollments.create_index("key", name="foreign_only")
                for name, pipeline, options, expected in _valid_cases():
                    with self.subTest(
                        engine=engine_type.__name__, disk=disk, batch=batch, case=name
                    ):
                        self.assertCountEqual(
                            db.rows.aggregate(
                                pipeline,
                                allow_disk_use=disk,
                                batch_size=batch,
                                **options,
                            ).to_list(),
                            expected,
                        )
                for name, pipeline in _invalid_cases():
                    with (
                        self.subTest(
                            engine=engine_type.__name__,
                            disk=disk,
                            batch=batch,
                            invalid=name,
                        ),
                        self.assertRaises(OperationFailure),
                    ):
                        db.rows.aggregate(
                            pipeline, allow_disk_use=disk, batch_size=batch
                        ).to_list()


class AggregationSpillNumericIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def test_signed_zero_has_one_group_before_and_after_spilling(self):
        documents = [
            {"_id": index, "value": value}
            for index, value in enumerate([0, 2, -0.0, 3, Decimal128("-0")])
        ]
        expected = [{"_id": 0, "n": 3}, {"_id": 2, "n": 1}, {"_id": 3, "n": 1}]
        pipeline = [{"$group": {"_id": "$value", "n": {"$sum": 1}}}]
        for engine_type, disk, batch in product(
            (MemoryEngine, SQLiteEngine), (False, True), (None, 1)
        ):
            with self.subTest(engine=engine_type.__name__, disk=disk, batch=batch):
                async with AsyncMongoClient(
                    engine_type(aggregation_spill_threshold=1)
                ) as client:
                    await client.probe.numbers.insert_many(documents)
                    self.assertCountEqual(
                        await client.probe.numbers.aggregate(
                            pipeline, allow_disk_use=disk, batch_size=batch
                        ).to_list(),
                        expected,
                    )
                with MongoClient(engine_type(aggregation_spill_threshold=1)) as client:
                    client.probe.numbers.insert_many(documents)
                    self.assertCountEqual(
                        client.probe.numbers.aggregate(
                            pipeline, allow_disk_use=disk, batch_size=batch
                        ).to_list(),
                        expected,
                    )
