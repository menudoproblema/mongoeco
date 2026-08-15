import unittest

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


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


class SyncSearchRegressionTests(unittest.TestCase):
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
