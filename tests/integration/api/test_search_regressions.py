import unittest

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure


ENGINE_FACTORIES = (MemoryEngine, SQLiteEngine)
USER_METADATA_FIELDS = {
    "__mongoeco_textScore__": "user-text",
    "__mongoeco_vectorSearchScore__": "user-vector",
    "__mongoeco_searchHighlights__": "user-highlight",
}
SEARCH_INDEX = SearchIndexModel(
    {
        "mappings": {
            "dynamic": False,
            "fields": {
                "title": {"type": "string"},
                "body": {"type": "string"},
                "kind": {"type": "token"},
            },
        },
    },
    name="by_text",
)
VECTOR_INDEX = SearchIndexModel(
    {
        "fields": [
            {
                "type": "vector",
                "path": "embedding",
                "numDimensions": 2,
                "similarity": "cosine",
            },
        ],
    },
    name="by_vector",
    type="vectorSearch",
)


def _contains_private_search_metadata(value):
    if isinstance(value, list):
        return any(_contains_private_search_metadata(item) for item in value)
    if isinstance(value, dict):
        return any(
            key == "\x00mongoeco_search_metadata"
            or _contains_private_search_metadata(item)
            for key, item in value.items()
        )
    return False


def _search_stage(*, highlight: bool = False):
    stage = {
        "index": "by_text",
        "text": {"query": "ada engine", "path": ["title", "body"]},
    }
    if highlight:
        stage["highlight"] = {"path": "title"}
    return {"$search": stage}


def _search_meta_stage():
    return {
        "$searchMeta": {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
            "count": {"type": "total"},
            "facet": {"path": "kind", "type": "token"},
        },
    }


class AsyncSearchRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_plan_is_shared_by_execution_and_explain(self):
        for factory in ENGINE_FACTORIES:
            with self.subTest(engine=factory.__name__):
                async with AsyncMongoClient(factory()) as client:
                    collection = client.plan.docs
                    await collection.insert_one(
                        {"_id": 1, "title": "Ada", "kind": "note"},
                    )
                    await collection.create_search_index(SEARCH_INDEX)

                    meta_explain = await collection.aggregate(
                        [
                            _search_meta_stage(),
                            {"$match": {"count.total": 1}},
                            {"$limit": 1},
                        ],
                    ).explain("queryPlanner")
                    self.assertIsNone(
                        meta_explain["pushdown"]["searchTopKStrategy"],
                    )
                    self.assertFalse(
                        meta_explain["pushdown"]["searchDownstreamFilterPrefilter"],
                    )
                    highlight_explain = await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {"$match": {"kind": "note"}},
                            {"$limit": 1},
                        ],
                    ).explain("queryPlanner")
                    self.assertFalse(
                        highlight_explain["pushdown"][
                            "searchDownstreamFilterPrefilter"
                        ],
                    )

                    empty_pipeline = [_search_stage(), {"$limit": 0}]
                    self.assertEqual(
                        await collection.aggregate(empty_pipeline).to_list(),
                        [],
                    )
                    empty_explain = await collection.aggregate(
                        empty_pipeline,
                    ).explain("queryPlanner")
                    self.assertEqual(
                        empty_explain["pushdown"]["searchTopKStrategy"],
                        "empty",
                    )
                    missing = collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "missing",
                                    "text": {"query": "ada", "path": "title"},
                                },
                            },
                            {"$limit": 0},
                        ],
                    )
                    with self.assertRaises(OperationFailure):
                        await missing.to_list()
                    with self.assertRaises(OperationFailure):
                        await missing.explain("queryPlanner")

    async def test_highlight_provenance_survives_pipeline_transformations(self):
        for factory in ENGINE_FACTORIES:
            with self.subTest(engine=factory.__name__):
                async with AsyncMongoClient(factory()) as client:
                    collection = client.highlight.docs
                    await collection.insert_one(
                        {"_id": 1, "title": "Ada", "kind": "note"},
                    )
                    await collection.create_search_index(SEARCH_INDEX)

                    await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {"$set": {"searchHighlights": "caller"}},
                            {"$merge": {"into": "overwritten"}},
                        ],
                    ).to_list()
                    overwritten = await client.highlight.overwritten.find_one()
                    self.assertEqual(overwritten["searchHighlights"], "caller")

                    await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {"$project": {"_id": 1, "searchHighlights": 1}},
                            {"$merge": {"into": "projected"}},
                        ],
                    ).to_list()
                    projected = await client.highlight.projected.find_one()
                    self.assertNotIn("searchHighlights", projected)

                    included = await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {"$project": {"_id": 1, "searchHighlights": 1}},
                        ],
                    ).to_list()
                    self.assertTrue(included[0]["searchHighlights"])

                    await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {
                                "$project": {
                                    "_id": 1,
                                    "searchHighlights": "$searchHighlights",
                                },
                            },
                            {"$merge": {"into": "explicit_same_name"}},
                        ],
                    ).to_list()
                    explicit_same_name = (
                        await client.highlight.explicit_same_name.find_one()
                    )
                    self.assertTrue(explicit_same_name["searchHighlights"])

                    await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {
                                "$project": {
                                    "_id": 1,
                                    "nested": {
                                        "searchHighlights": "$searchHighlights",
                                    },
                                },
                            },
                            {"$merge": {"into": "explicit_nested"}},
                        ],
                    ).to_list()
                    explicit_nested = await client.highlight.explicit_nested.find_one()
                    self.assertTrue(
                        explicit_nested["nested"]["searchHighlights"],
                    )

                    copied = await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {
                                "$project": {
                                    "_id": 1,
                                    "copied": "$searchHighlights",
                                },
                            },
                            {"$merge": {"into": "copied"}},
                        ],
                    ).to_list()
                    self.assertEqual(copied, [])
                    copied_document = await client.highlight.copied.find_one()
                    self.assertTrue(copied_document["copied"])
                    self.assertNotIn("searchHighlights", copied_document)

                    unset = await collection.aggregate(
                        [_search_stage(highlight=True), {"$unset": "searchHighlights"}],
                    ).to_list()
                    self.assertNotIn("searchHighlights", unset[0])

    async def test_pending_index_never_fabricates_execution_stats(self):
        for factory in ENGINE_FACTORIES:
            with self.subTest(engine=factory.__name__):
                async with AsyncMongoClient(
                    factory(simulate_search_index_latency=60),
                ) as client:
                    collection = client.pending.docs
                    await collection.insert_one({"_id": 1, "title": "Ada"})
                    await collection.create_search_index(SEARCH_INDEX)
                    cursor = collection.aggregate([_search_stage()])
                    planner = await cursor.explain("queryPlanner")
                    self.assertEqual(
                        planner["engine_plan"]["details"]["status"],
                        "PENDING",
                    )
                    with self.assertRaises(OperationFailure):
                        await cursor.explain("executionStats")

    async def test_search_boundaries_remain_equivalent_and_collision_free(self):
        for factory in ENGINE_FACTORIES:
            with self.subTest(engine=factory.__name__):
                async with AsyncMongoClient(factory()) as client:
                    collection = client.audit.docs
                    documents = [
                        {
                            "_id": 1,
                            "title": "Ada",
                            "body": "engine",
                            "kind": "ada",
                            "searchHighlights": "user-data",
                            **USER_METADATA_FIELDS,
                        },
                        {"_id": 2, "title": "Ada", "body": "none", "kind": "ada"},
                        {"_id": 3, "title": "none", "body": "engine", "kind": "ada"},
                    ]
                    await collection.insert_many(documents)
                    await collection.create_search_index(SEARCH_INDEX)

                    ordinary = await collection.aggregate([]).to_list()
                    self.assertEqual(
                        {key: ordinary[0][key] for key in USER_METADATA_FIELDS},
                        USER_METADATA_FIELDS,
                    )
                    await collection.aggregate(
                        [{"$merge": {"into": "merged"}}],
                    ).to_list()
                    merged = await client.audit.merged.find_one({"_id": 1})
                    self.assertEqual(
                        {key: merged[key] for key in USER_METADATA_FIELDS},
                        USER_METADATA_FIELDS,
                    )

                    hits = await collection.aggregate(
                        [_search_stage(), {"$project": {"_id": 1}}],
                    ).to_list()
                    self.assertEqual([item["_id"] for item in hits], [1, 2, 3])

                    meta = _search_meta_stage()
                    self.assertEqual(
                        await collection.aggregate([meta, {"$limit": 1}]).to_list(),
                        await collection.aggregate([meta]).to_list(),
                    )
                    matched_meta = await collection.aggregate(
                        [meta, {"$match": {"count.total": 2}}],
                    ).to_list()
                    self.assertEqual(matched_meta[0]["count"]["total"], 2)
                    matched_limited_meta = await collection.aggregate(
                        [meta, {"$match": {"count.total": 2}}, {"$limit": 1}],
                    ).to_list()
                    self.assertEqual(matched_limited_meta, matched_meta)
                    execution_explain = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {"query": "ada", "path": "title"},
                                },
                            },
                        ],
                    ).explain("executionStats")
                    self.assertEqual(
                        execution_explain["engine_plan"]["details"]["executionStats"][
                            "matchedCount"
                        ],
                        2,
                    )
                    execution_stats = execution_explain["engine_plan"]["details"][
                        "executionStats"
                    ]
                    self.assertTrue(execution_stats["executed"])
                    self.assertEqual(execution_stats["queryMatchedCount"], 2)
                    self.assertEqual(execution_stats["returnedHitCount"], 2)

                    collated = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {"query": "ada", "path": "title"},
                                },
                            },
                            {"$match": {"kind": "ADA"}},
                        ],
                        collation={"locale": "en", "strength": 2},
                    ).to_list()
                    self.assertEqual([item["_id"] for item in collated], [1, 2])

                    highlighted = await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {
                                "$project": {
                                    "_id": 1,
                                    "searchHighlights": 1,
                                    "matches": {"$meta": "searchHighlights"},
                                },
                            },
                        ],
                    ).to_list()
                    first = next(item for item in highlighted if item["_id"] == 1)
                    self.assertEqual(first["searchHighlights"], "user-data")
                    self.assertTrue(first["matches"])

                    filtered_highlight = await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {"$match": {"searchHighlights.0.path": "title"}},
                            {"$limit": 1},
                        ],
                    ).to_list()
                    self.assertEqual([item["_id"] for item in filtered_highlight], [2])

                    wrapped = await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {"$replaceWith": {"wrapped": "$$ROOT"}},
                        ],
                    ).to_list()
                    self.assertFalse(_contains_private_search_metadata(wrapped))

                    await collection.aggregate(
                        [
                            _search_stage(highlight=True),
                            {"$merge": {"into": "highlighted_merge"}},
                        ],
                    ).to_list()
                    generated = await client.audit.highlighted_merge.find_one(
                        {"_id": 2},
                    )
                    preserved = await client.audit.highlighted_merge.find_one(
                        {"_id": 1},
                    )
                    self.assertNotIn("searchHighlights", generated)
                    self.assertEqual(preserved["searchHighlights"], "user-data")

    async def test_vector_downstream_match_remains_after_top_k(self):
        for factory in ENGINE_FACTORIES:
            with self.subTest(engine=factory.__name__):
                async with AsyncMongoClient(factory()) as client:
                    collection = client.audit.vector_docs
                    await collection.insert_many(
                        [
                            {
                                "_id": "top",
                                "embedding": [1.0, 0.0],
                                "keep": False,
                            },
                            {
                                "_id": "second",
                                "embedding": [0.9, 0.1],
                                "keep": True,
                            },
                        ],
                    )
                    await collection.create_search_index(VECTOR_INDEX)
                    results = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0],
                                    "limit": 1,
                                    "numCandidates": 2,
                                },
                            },
                            {"$match": {"keep": True}},
                        ],
                    ).to_list()
                    self.assertEqual(results, [])
                    explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0],
                                    "limit": 1,
                                    "numCandidates": 2,
                                },
                            },
                            {"$match": {"keep": True}},
                        ],
                    ).explain("queryPlanner")
                    self.assertFalse(
                        explanation["pushdown"]["searchDownstreamFilterPrefilter"],
                    )


class SyncSearchRegressionTests(unittest.TestCase):
    def test_search_planning_metadata_and_pending_contract_match_async(self):
        for factory in ENGINE_FACTORIES:
            with (
                self.subTest(engine=factory.__name__),
                MongoClient(factory()) as client,
            ):
                collection = client.sync_contract.docs
                collection.insert_one({"_id": 1, "title": "Ada", "kind": "note"})
                collection.create_search_index(SEARCH_INDEX)

                empty_pipeline = [_search_stage(), {"$limit": 0}]
                self.assertEqual(collection.aggregate(empty_pipeline).to_list(), [])
                explain = collection.aggregate(empty_pipeline).explain("queryPlanner")
                self.assertEqual(
                    explain["pushdown"]["searchTopKStrategy"],
                    "empty",
                )

                collection.aggregate(
                    [
                        _search_stage(highlight=True),
                        {"$set": {"searchHighlights": "caller"}},
                        {"$merge": {"into": "overwritten"}},
                    ],
                ).to_list()
                overwritten = client.sync_contract.overwritten.find_one()
                self.assertEqual(overwritten["searchHighlights"], "caller")

                collection.aggregate(
                    [
                        _search_stage(highlight=True),
                        {"$project": {"_id": 1, "searchHighlights": 1}},
                        {"$merge": {"into": "projected"}},
                    ],
                ).to_list()
                self.assertNotIn(
                    "searchHighlights",
                    client.sync_contract.projected.find_one(),
                )

                collection.aggregate(
                    [
                        _search_stage(highlight=True),
                        {
                            "$project": {
                                "_id": 1,
                                "searchHighlights": "$searchHighlights",
                            },
                        },
                        {"$merge": {"into": "explicit_same_name"}},
                    ],
                ).to_list()
                self.assertTrue(
                    client.sync_contract.explicit_same_name.find_one()[
                        "searchHighlights"
                    ],
                )

            with (
                self.subTest(engine=f"{factory.__name__}-pending"),
                MongoClient(factory(simulate_search_index_latency=60)) as client,
            ):
                collection = client.sync_pending.docs
                collection.insert_one({"_id": 1, "title": "Ada"})
                collection.create_search_index(SEARCH_INDEX)
                cursor = collection.aggregate([_search_stage()])
                self.assertEqual(
                    cursor.explain("queryPlanner")["engine_plan"]["details"]["status"],
                    "PENDING",
                )
                with self.assertRaises(OperationFailure):
                    cursor.explain("executionStats")

    def test_search_boundaries_remain_equivalent_and_collision_free(self):
        for factory in ENGINE_FACTORIES:
            with (
                self.subTest(engine=factory.__name__),
                MongoClient(factory()) as client,
            ):
                collection = client.audit_sync.docs
                collection.insert_many(
                    [
                        {"_id": 1, "title": "Ada", "body": "engine", "kind": "ada"},
                        {"_id": 2, "title": "Ada", "body": "none", "kind": "ada"},
                        {"_id": 3, "title": "none", "body": "engine", "kind": "ada"},
                    ],
                )
                collection.create_search_index(SEARCH_INDEX)

                hits = collection.aggregate(
                    [_search_stage(), {"$project": {"_id": 1}}],
                ).to_list()
                self.assertEqual([item["_id"] for item in hits], [1, 2, 3])

                meta = _search_meta_stage()
                self.assertEqual(
                    collection.aggregate([meta, {"$limit": 1}]).to_list(),
                    collection.aggregate([meta]).to_list(),
                )
                matched_meta = collection.aggregate(
                    [meta, {"$match": {"count.total": 2}}],
                ).to_list()
                self.assertEqual(matched_meta[0]["count"]["total"], 2)
                self.assertEqual(
                    collection.aggregate(
                        [meta, {"$match": {"count.total": 2}}, {"$limit": 1}],
                    ).to_list(),
                    matched_meta,
                )

                collated = collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "text": {"query": "ada", "path": "title"},
                            },
                        },
                        {"$match": {"kind": "ADA"}},
                    ],
                    collation={"locale": "en", "strength": 2},
                ).to_list()
                self.assertEqual([item["_id"] for item in collated], [1, 2])

    def test_vector_downstream_match_remains_after_top_k(self):
        for factory in ENGINE_FACTORIES:
            with (
                self.subTest(engine=factory.__name__),
                MongoClient(factory()) as client,
            ):
                collection = client.audit_sync.vector_docs
                collection.insert_many(
                    [
                        {"_id": "top", "embedding": [1.0, 0.0], "keep": False},
                        {
                            "_id": "second",
                            "embedding": [0.9, 0.1],
                            "keep": True,
                        },
                    ],
                )
                collection.create_search_index(VECTOR_INDEX)
                results = collection.aggregate(
                    [
                        {
                            "$vectorSearch": {
                                "index": "by_vector",
                                "path": "embedding",
                                "queryVector": [1.0, 0.0],
                                "limit": 1,
                                "numCandidates": 2,
                            },
                        },
                        {"$match": {"keep": True}},
                    ],
                ).to_list()
                self.assertEqual(results, [])
                explanation = collection.aggregate(
                    [
                        {
                            "$vectorSearch": {
                                "index": "by_vector",
                                "path": "embedding",
                                "queryVector": [1.0, 0.0],
                                "limit": 1,
                                "numCandidates": 2,
                            },
                        },
                        {"$match": {"keep": True}},
                    ],
                ).explain("queryPlanner")
                self.assertFalse(
                    explanation["pushdown"]["searchDownstreamFilterPrefilter"],
                )
