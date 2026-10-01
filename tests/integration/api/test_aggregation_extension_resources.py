"""Extension delegation retains the resources of builtin aggregation stages."""

import unittest

from itertools import product

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.core.aggregation.extensions import (
    register_aggregation_stage,
    unregister_aggregation_stage,
)
from mongoeco.core.aggregation.stages import get_aggregation_stage_spec
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


_OPERATORS = (
    "$lookup",
    "$unionWith",
    "$indexStats",
    "$collStats",
    "$planCacheStats",
    "$currentOp",
    "$listSessions",
)
_ROW = {"_id": "r1", "fk": "f1"}
_FOREIGN = {"_id": "f1"}


def _register_delegates(mode):
    # Capture builtins before installing any overrides; preparation must not call them.
    handlers = {
        operator: get_aggregation_stage_spec(operator).handler
        for operator in _OPERATORS
    }
    for operator, handler in handlers.items():
        register_aggregation_stage(operator, handler, execution_mode=mode)


def _unregister_delegates():
    for operator in _OPERATORS:
        unregister_aggregation_stage(operator)


def _cases():
    group = {"$group": {"_id": "$fk"}}
    lookup = {
        "$lookup": {
            "from": "foreign",
            "localField": "_id",
            "foreignField": "_id",
            "as": "e",
        }
    }
    yield "group_lookup", [group, lookup], [{"_id": "f1", "e": [_FOREIGN]}]
    yield (
        "group_lookup_let",
        [
            group,
            {
                "$lookup": {
                    "from": "foreign",
                    "let": {"fk": "$_id"},
                    "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$fk"]}}}],
                    "as": "e",
                }
            },
        ],
        [{"_id": "f1", "e": [_FOREIGN]}],
    )
    yield "group_union", [group, {"$unionWith": "foreign"}], [{"_id": "f1"}, _FOREIGN]
    index_pipeline = [
        {"$indexStats": {}},
        {"$match": {"name": "foreign_only"}},
        {"$project": {"_id": 0, "name": 1}},
    ]
    yield (
        "foreign_index_stats",
        [
            {
                "$lookup": {
                    "from": "foreign",
                    "pipeline": index_pipeline,
                    "as": "e",
                }
            }
        ],
        [{**_ROW, "e": [{"name": "foreign_only"}]}],
    )
    yield (
        "foreign_coll_stats",
        [
            group,
            {
                "$unionWith": {
                    "coll": "foreign",
                    "pipeline": [{"$collStats": {"count": {}}}],
                }
            },
        ],
        [{"_id": "f1"}, {"ns": "probe.foreign", "count": 1}],
    )
    yield (
        "foreign_plan_cache_stats",
        [
            {
                "$lookup": {
                    "from": "foreign",
                    "pipeline": [{"$planCacheStats": {}}, {"$group": {"_id": "$ns"}}],
                    "as": "e",
                }
            }
        ],
        [{**_ROW, "e": [{"_id": "probe.foreign"}]}],
    )
    yield (
        "root_index_stats",
        [
            {"$indexStats": {}},
            {"$match": {"name": "root_only"}},
            {"$project": {"_id": 0, "name": 1}},
        ],
        [{"name": "root_only"}],
    )
    yield (
        "root_coll_stats",
        [
            {"$collStats": {"count": {}, "storageStats": {"scale": 2}}},
            {"$project": {"_id": 0, "ns": 1, "count": 1}},
        ],
        [
            {"ns": "probe.rows", "count": 1},
        ],
    )
    yield (
        "root_current_op",
        [
            {"$currentOp": {}},
            {"$match": {"ns": "absent.collection"}},
        ],
        [],
    )
    yield "root_list_sessions", [{"$listSessions": {}}], []
    yield (
        "facet_join",
        [{"$facet": {"x": [group, lookup]}}],
        [
            {"x": [{"_id": "f1", "e": [_FOREIGN]}]},
        ],
    )


class AggregationExtensionResourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_delegates_receive_root_and_foreign_resources(self):
        for engine_type, mode, batch in product(
            (MemoryEngine, SQLiteEngine), ("streamable", "materializing"), (None, 1)
        ):
            _register_delegates(mode)
            try:
                async with AsyncMongoClient(engine_type()) as client:
                    db = client.probe
                    await db.rows.insert_one(_ROW)
                    await db.foreign.insert_one(_FOREIGN)
                    await db.rows.create_index("fk", name="root_only")
                    await db.foreign.create_index("label", name="foreign_only")
                    for name, pipeline, expected in _cases():
                        with self.subTest(
                            engine=engine_type.__name__,
                            mode=mode,
                            batch=batch,
                            case=name,
                        ):
                            self.assertEqual(
                                await db.rows.aggregate(
                                    pipeline, batch_size=batch
                                ).to_list(),
                                expected,
                            )
            finally:
                _unregister_delegates()

    def test_sync_delegates_receive_root_and_foreign_resources(self):
        for engine_type, mode, batch in product(
            (MemoryEngine, SQLiteEngine), ("streamable", "materializing"), (None, 1)
        ):
            _register_delegates(mode)
            try:
                with MongoClient(engine_type()) as client:
                    db = client.probe
                    db.rows.insert_one(_ROW)
                    db.foreign.insert_one(_FOREIGN)
                    db.rows.create_index("fk", name="root_only")
                    db.foreign.create_index("label", name="foreign_only")
                    for name, pipeline, expected in _cases():
                        with self.subTest(
                            engine=engine_type.__name__,
                            mode=mode,
                            batch=batch,
                            case=name,
                        ):
                            self.assertEqual(
                                db.rows.aggregate(pipeline, batch_size=batch).to_list(),
                                expected,
                            )
            finally:
                _unregister_delegates()

    async def test_custom_information_parser_keeps_resources(
        self,
    ):
        def custom_handler(documents, spec, context):
            self.assertIn(spec, ["custom", {"storageStats": {"scale": 2}}])
            self.assertEqual(context.stage_index, 1)
            return [
                {"names": sorted(row["name"] for row in context.index_stats_resolver())}
            ]

        register_aggregation_stage("$indexStats", custom_handler)
        try:
            for engine_type, spec in product(
                (MemoryEngine, SQLiteEngine),
                ("custom", {"storageStats": {"scale": 2}}),
            ):
                async with AsyncMongoClient(engine_type()) as client:
                    await client.probe.rows.insert_one(_ROW)
                    self.assertEqual(
                        await client.probe.rows.aggregate(
                            [
                                {"$match": {}},
                                {"$indexStats": spec},
                            ]
                        ).to_list(),
                        [{"names": ["_id_"]}],
                    )
        finally:
            unregister_aggregation_stage("$indexStats")
