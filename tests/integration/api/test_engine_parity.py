import datetime
import unittest
import uuid

try:
    from bson.objectid import ObjectId as BsonObjectId
except Exception:  # pragma: no cover - optional dependency
    BsonObjectId = None

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, DuplicateKeyError, InvalidOperation, OperationFailure
from mongoeco.types import (
    DBRef,
    DeleteOne,
    InsertOne,
    ObjectId,
    ReplaceOne,
    ReturnDocument,
    SearchIndexModel,
    UNDEFINED,
    UpdateMany,
    UpdateOne,
)
from tests.support import open_client


class EngineParityTests(unittest.IsolatedAsyncioTestCase):
    async def _collect_find(self, engine_name: str, filter_spec, *, sort=None, skip=0, limit=None, projection=None):
        async with open_client(engine_name) as client:
            collection = client.get_database("db").get_collection("events")
            await collection.insert_one(
                {
                    "_id": ObjectId("0123456789abcdef01234567"),
                    "kind": "view",
                    "rank": 3,
                    "created_at": datetime.datetime(2025, 1, 2, 3, 4, 5),
                    "session_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                }
            )
            await collection.insert_one({"_id": "2", "kind": "view", "rank": 1})
            await collection.insert_one({"_id": "3", "kind": "click", "rank": 2})

            return [
                document
                async for document in collection.find(
                    filter_spec,
                    projection,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                )
            ]

    async def test_find_has_same_results_in_memory_and_sqlite_for_codec_aware_filter(self):
        memory_documents = await self._collect_find(
            "memory",
            {"session_id": uuid.UUID("12345678-1234-5678-1234-567812345678")},
        )
        sqlite_documents = await self._collect_find(
            "sqlite",
            {"session_id": uuid.UUID("12345678-1234-5678-1234-567812345678")},
        )

        self.assertEqual(memory_documents, sqlite_documents)

    async def test_find_has_same_results_in_memory_and_sqlite_for_sort_skip_limit(self):
        memory_documents = await self._collect_find(
            "memory",
            {"kind": "view"},
            sort=[("rank", 1)],
            skip=1,
            limit=1,
            projection={"rank": 1, "_id": 0},
        )
        sqlite_documents = await self._collect_find(
            "sqlite",
            {"kind": "view"},
            sort=[("rank", 1)],
            skip=1,
            limit=1,
            projection={"rank": 1, "_id": 0},
        )

        self.assertEqual(memory_documents, sqlite_documents)

    async def test_classic_text_query_and_text_score_match_in_memory_and_sqlite(self):
        results: dict[str, list[dict[str, object]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("articles")
                await collection.insert_many(
                    [
                        {"_id": "1", "content": "Ada wrote the first algorithm"},
                        {"_id": "2", "content": "Grace built the first compiler"},
                        {"_id": "3", "content": "Algorithm notes by Ada"},
                    ]
                )
                await collection.create_index({"content": "text"})
                results[engine_name] = await collection.find(
                    {"$text": {"$search": "algorithm ada"}},
                    {"_id": 1, "score": {"$meta": "textScore"}},
                    sort={"score": {"$meta": "textScore"}},
                ).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_dbref_subfield_filter_and_lookup_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, list[object]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                users = client.get_database("db").get_collection("users")
                posts = client.get_database("db").get_collection("posts")

                await users.insert_one({"_id": "u1", "name": "Ada", "tenant": "a"})
                await users.insert_one({"_id": "u2", "name": "Grace", "tenant": "b"})
                await posts.insert_one(
                    {
                        "_id": "p1",
                        "author": DBRef("users", "u1", database="db", extras={"tenant": "a", "meta": {"region": "eu"}}),
                    }
                )
                await posts.insert_one(
                    {
                        "_id": "p2",
                        "author": DBRef("users", "u2", database="db", extras={"tenant": "b"}),
                    }
                )

                results[engine_name] = {
                    "filter": [
                        document["_id"]
                        async for document in posts.find(
                            {"author.$id": "u1"},
                            sort=[("_id", 1)],
                        )
                    ],
                    "filter_by_tenant": [
                        document["_id"]
                        async for document in posts.find(
                            {"author.tenant": "a"},
                            sort=[("_id", 1)],
                        )
                    ],
                    "filter_by_region": [
                        document["_id"]
                        async for document in posts.find(
                            {"author.meta.region": "eu"},
                            sort=[("_id", 1)],
                        )
                    ],
                    "lookup": await posts.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "author.$id", "foreignField": "_id", "as": "user"}},
                            {"$lookup": {"from": "users", "localField": "author.tenant", "foreignField": "tenant", "as": "tenant_user"}},
                            {
                                "$project": {
                                    "_id": 1,
                                    "author_id": "$author.$id",
                                    "author_region": "$author.meta.region",
                                    "user_names": "$user.name",
                                    "tenant_user_names": "$tenant_user.name",
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list(),
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_find_has_same_results_in_memory_and_sqlite_for_array_scalar_equality(self):
        results: dict[str, list[str]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "tags": ["python", "mongodb"]})
                await collection.insert_one({"_id": "2", "tags": ["sqlite"]})
                await collection.insert_one({"_id": "3", "tags": "python"})
                await collection.insert_one({"_id": "4"})

                results[engine_name] = [
                    document["_id"]
                    async for document in collection.find({"tags": "python"}, sort=[("_id", 1)])
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_null_bool_mixed_comparison_and_codec_array_equality_match_in_memory_and_sqlite(self):
        object_id = ObjectId("0123456789abcdef01234567")
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        created_at = datetime.datetime(2025, 1, 2, 3, 4, 5)
        results: dict[str, dict[str, list[str]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "bool", "v": True})
                await collection.insert_one({"_id": "number", "v": 1})
                await collection.insert_one({"_id": "null", "v": None})
                await collection.insert_one({"_id": "missing"})
                await collection.insert_one({"_id": "string", "v": "abc"})
                await collection.insert_one({"_id": "uuid", "v": session_id})
                await collection.insert_one({"_id": "objectid", "v": object_id})
                await collection.insert_one({"_id": "datetime", "v": created_at})
                await collection.insert_one({"_id": "array-null", "v": [None, 1]})
                await collection.insert_one({"_id": "array-mixed", "v": [1, "a"]})
                await collection.insert_one({"_id": "array-oid", "v": [object_id]})

                results[engine_name] = {
                    "eq_none": [
                        document["_id"]
                        async for document in collection.find({"v": None}, sort=[("_id", 1)])
                    ],
                    "eq_one": [
                        document["_id"]
                        async for document in collection.find({"v": 1}, sort=[("_id", 1)])
                    ],
                    "eq_true": [
                        document["_id"]
                        async for document in collection.find({"v": True}, sort=[("_id", 1)])
                    ],
                    "gt_string": [
                        document["_id"]
                        async for document in collection.find({"v": {"$gt": "abc"}}, sort=[("_id", 1)])
                    ],
                    "eq_oid": [
                        document["_id"]
                        async for document in collection.find({"v": object_id}, sort=[("_id", 1)])
                    ],
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_null_equality_and_in_respect_undefined_delta_in_7_and_8_for_both_engines(self):
        expected_by_dialect = {
            MONGODB_DIALECT_70.key: ["array-null", "array-undefined", "missing", "null", "undefined"],
            MONGODB_DIALECT_80.key: ["array-null", "missing", "null"],
        }

        for engine_factory in (MemoryEngine, SQLiteEngine):
            for dialect in (MONGODB_DIALECT_70, MONGODB_DIALECT_80):
                with self.subTest(engine=engine_factory.__name__, dialect=dialect.key):
                    async with AsyncMongoClient(engine_factory(), mongodb_dialect=dialect) as client:
                        collection = client.get_database("db").get_collection("events")
                        await collection.insert_one({"_id": "null", "v": None})
                        await collection.insert_one({"_id": "undefined", "v": UNDEFINED})
                        await collection.insert_one({"_id": "array-null", "v": ["x", None]})
                        await collection.insert_one({"_id": "array-undefined", "v": ["x", UNDEFINED]})
                        await collection.insert_one({"_id": "missing"})

                        eq_ids = [
                            document["_id"]
                            async for document in collection.find({"v": None}, sort=[("_id", 1)])
                        ]
                        in_ids = [
                            document["_id"]
                            async for document in collection.find({"v": {"$in": [None]}}, sort=[("_id", 1)])
                        ]

                        self.assertEqual(eq_ids, expected_by_dialect[dialect.key])
                        self.assertEqual(in_ids, expected_by_dialect[dialect.key])

    async def test_lookup_respects_undefined_delta_in_7_and_8_for_both_engines(self):
        expected_joined_ids = {
            MONGODB_DIALECT_70.key: ["user-null", "user-undefined"],
            MONGODB_DIALECT_80.key: ["user-null"],
        }

        for engine_factory in (MemoryEngine, SQLiteEngine):
            for dialect in (MONGODB_DIALECT_70, MONGODB_DIALECT_80):
                with self.subTest(engine=engine_factory.__name__, dialect=dialect.key):
                    async with AsyncMongoClient(engine_factory(), mongodb_dialect=dialect) as client:
                        database = client.get_database("db")
                        events = database.get_collection("events")
                        users = database.get_collection("users")

                        await events.insert_one({"_id": "event-null", "tenant": None})
                        await users.insert_one({"_id": "user-null", "tenant": None})
                        await users.insert_one({"_id": "user-undefined", "tenant": UNDEFINED})

                        documents = await events.aggregate(
                            [
                                {
                                    "$lookup": {
                                        "from": "users",
                                        "localField": "tenant",
                                        "foreignField": "tenant",
                                        "as": "joined",
                                    }
                                }
                            ]
                        ).to_list()

                        self.assertEqual(
                            [joined["_id"] for joined in documents[0]["joined"]],
                            expected_joined_ids[dialect.key],
                        )

    async def test_nested_update_and_delete_match_in_memory_and_sqlite(self):
        results: dict[str, tuple[object, int]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view"})
                await collection.insert_one({"_id": "2", "payload": {"match": True}})

                update_result = await collection.update_one(
                    {"kind": "view"},
                    {"$set": {"profile.name": "Ada"}},
                )
                delete_result = await collection.delete_one({"payload": {"match": True}})
                found = await collection.find_one({"_id": "1"})

                results[engine_name] = (
                    (update_result.matched_count, update_result.modified_count, delete_result.deleted_count),
                    found,
                )

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_mixed_type_sort_with_bytes_matches_in_memory_and_sqlite(self):
        results: dict[str, list[str]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "null", "v": None})
                await collection.insert_one({"_id": "number", "v": 1})
                await collection.insert_one({"_id": "string", "v": "abc"})
                await collection.insert_one({"_id": "bytes", "v": b"abc"})
                await collection.insert_one({"_id": "uuid", "v": uuid.UUID("12345678-1234-5678-1234-567812345678")})
                await collection.insert_one({"_id": "objectid", "v": ObjectId("507f1f77bcf86cd799439011")})
                await collection.insert_one({"_id": "bool", "v": True})
                await collection.insert_one({"_id": "datetime", "v": datetime.datetime(2026, 3, 24, 12, 0, 0)})

                results[engine_name] = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("v", 1), ("_id", 1)])
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_bytes_and_uuid_range_filters_match_in_memory_and_sqlite(self):
        results: dict[str, list[str]] = {}
        target = uuid.UUID("12345678-1234-5678-1234-567812345678")
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "bytes0", "v": b"\x00"})
                await collection.insert_one({"_id": "uuid", "v": target})
                await collection.insert_one({"_id": "objectid", "v": ObjectId("507f1f77bcf86cd799439011")})

                results[engine_name] = [
                    document["_id"]
                    async for document in collection.find({"v": {"$gt": target}}, sort=[("_id", 1)])
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_top_level_array_range_filters_match_in_memory_and_sqlite(self):
        target = uuid.UUID("12345678-1234-5678-1234-567812345678")
        object_id = ObjectId("507f1f77bcf86cd799439011")
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "array-oid", "v": [object_id]})
                await collection.insert_one({"_id": "uuid", "v": target})
                await collection.insert_one({"_id": "string", "v": "abc"})

                matched = [
                    document["_id"]
                    async for document in collection.find({"v": {"$gt": target}}, sort=[("_id", 1)])
                ]
                count = await collection.count_documents({"v": {"$gt": target}})
                update_result = await collection.update_one(
                    {"v": {"$gt": target}},
                    {"$set": {"matched": True}},
                )
                updated = await collection.find_one({"_id": "array-oid"})
                delete_result = await collection.delete_one({"v": {"$gt": target}})
                remaining = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("_id", 1)])
                ]

                results[engine_name] = {
                    "matched": matched,
                    "count": count,
                    "update": (update_result.matched_count, update_result.modified_count),
                    "updated": updated,
                    "delete": delete_result.deleted_count,
                    "remaining": remaining,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_scalar_range_filters_on_arrays_only_match_array_elements_in_memory_and_sqlite(self):
        results: dict[str, list[int]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_many(
                    [
                        {"_id": 1, "a": [1, 10]},
                        {"_id": 2, "a": [2]},
                        {"_id": 3, "a": 20},
                    ]
                )
                await collection.create_index([("a", 1)])

                results[engine_name] = [
                    document["_id"]
                    async for document in collection.find({"a": {"$gt": 5}}, sort=[("_id", 1)])
                ]

        self.assertEqual(results["memory"], [1, 3])
        self.assertEqual(results["sqlite"], [1, 3])

    async def test_aggregate_field_path_array_traversal_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "items": [{"kind": "a"}, {"kind": "b"}]})

                results[engine_name] = [
                    document
                    async for document in collection.aggregate(
                        [{"$project": {"_id": 0, "kinds": "$items.kind"}}]
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_compound_filter_and_membership_match_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "role": "admin"})
                await collection.insert_one({"_id": "2", "kind": "view", "rank": 1, "role": "staff"})
                await collection.insert_one({"_id": "3", "kind": "click", "rank": 2, "role": "staff"})
                await collection.insert_one({"_id": "4", "kind": "view", "rank": 4, "role": "guest"})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {
                            "$and": [
                                {"kind": "view"},
                                {"role": {"$in": ["admin", "staff"]}},
                                {"rank": {"$gte": 2}},
                            ]
                        },
                        {"role": 1, "rank": 1, "_id": 0},
                        sort=[("rank", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_or_not_in_and_count_match_in_memory_and_sqlite(self):
        results: dict[str, tuple[list[dict], int]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view", "rank": 3})
                await collection.insert_one({"_id": "2", "kind": "view", "rank": 1})
                await collection.insert_one({"_id": "3", "kind": "click", "rank": 2})
                await collection.insert_one({"_id": "4", "kind": "open", "rank": 4})

                filter_spec = {
                    "$or": [
                        {"kind": "click"},
                        {"rank": {"$nin": [1, 4]}},
                    ]
                }
                documents = [
                    document
                    async for document in collection.find(
                        filter_spec,
                        {"kind": 1, "_id": 0},
                        sort=[("kind", 1)],
                    )
                ]
                count = await collection.count_documents(filter_spec)
                results[engine_name] = (documents, count)

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_datetime_comparison_and_sort_match_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "created_at": datetime.datetime(2025, 1, 2, 3, 4, 5)})
                await collection.insert_one({"_id": "2", "created_at": datetime.datetime(2025, 1, 3, 3, 4, 5)})
                await collection.insert_one({"_id": "3", "created_at": datetime.datetime(2025, 1, 1, 3, 4, 5)})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"created_at": {"$gte": datetime.datetime(2025, 1, 2, 0, 0, 0)}},
                        {"_id": 1},
                        sort=[("created_at", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_objectid_sort_match_in_memory_and_sqlite(self):
        ids = [
            ObjectId("0123456789abcdef01234567"),
            ObjectId("0123456789abcdef01234568"),
            ObjectId("0123456789abcdef01234566"),
        ]
        results: dict[str, list[ObjectId]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                for index, object_id in enumerate(ids, start=1):
                    await collection.insert_one({"_id": object_id, "rank": index})

                results[engine_name] = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("_id", 1)])
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_mixed_type_sort_match_in_memory_and_sqlite(self):
        mixed_values = [
            ("null", None),
            ("int", 7),
            ("str", "alpha"),
            ("dict", {"nested": 1}),
            ("list", [1, 2]),
            ("oid", ObjectId("0123456789abcdef01234567")),
            ("bool", True),
        ]
        ascending: dict[str, list[str]] = {}
        descending: dict[str, list[str]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                for doc_id, value in mixed_values:
                    await collection.insert_one({"_id": doc_id, "mixed": value})

                ascending[engine_name] = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("mixed", 1)])
                ]
                descending[engine_name] = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("mixed", -1)])
                ]

        self.assertEqual(ascending["memory"], ascending["sqlite"])
        self.assertEqual(descending["memory"], descending["sqlite"])

    async def test_async_cursor_contract_matches_in_memory_and_sqlite(self):
        results: dict[str, tuple[list[dict], dict | None]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view", "rank": 3})
                await collection.insert_one({"_id": "2", "kind": "view", "rank": 1})
                await collection.insert_one({"_id": "3", "kind": "view", "rank": 2})

                cursor = collection.find({"kind": "view"})
                documents = await cursor.sort([("rank", 1)]).skip(1).limit(1).to_list()
                first = await collection.find({"kind": "view"}).sort([("rank", 1)]).first()
                results[engine_name] = (documents, first)

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_regex_all_and_size_filters_match_in_memory_and_sqlite(self):
        results: dict[str, tuple[list[dict], int]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "name": "Ada", "tags": ["python", "mongodb"]})
                await collection.insert_one({"_id": "2", "name": "Grace", "tags": ["python"]})
                await collection.insert_one({"_id": "3", "name": "Alan", "tags": ["sqlite", "python"]})

                filter_spec = {
                    "$and": [
                        {"name": {"$regex": "^a", "$options": "i"}},
                        {"tags": {"$all": ["python"]}},
                        {"tags": {"$size": 2}},
                    ]
                }
                documents = [
                    document
                    async for document in collection.find(
                        filter_spec,
                        {"name": 1, "_id": 0},
                        sort=[("name", 1)],
                    )
                ]
                count = await collection.count_documents(filter_spec)
                results[engine_name] = (documents, count)

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_ne_in_and_nin_match_in_memory_and_sqlite_for_arrays_missing_and_codec_aware_values(self):
        results: dict[str, dict[str, object]] = {}
        oid1 = ObjectId("0123456789abcdef01234567")
        oid2 = ObjectId("abcdef0123456789abcdef01")
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "tags": ["python", "mongodb"], "ref": oid1, "a": [1, 2]})
                await collection.insert_one({"_id": "2", "tags": ["sqlite"], "ref": oid2})
                await collection.insert_one({"_id": "3", "a": 1, "refs": [oid1]})
                await collection.insert_one({"_id": "4", "a": [3], "refs": [oid2]})
                await collection.insert_one({"_id": "5"})

                ne_ids = [
                    document["_id"]
                    async for document in collection.find({"tags": {"$ne": "python"}}, sort=[("_id", 1)])
                ]
                in_ids = [
                    document["_id"]
                    async for document in collection.find({"a": {"$in": [1]}}, sort=[("_id", 1)])
                ]
                nin_ids = [
                    document["_id"]
                    async for document in collection.find({"a": {"$nin": [1]}}, sort=[("_id", 1)])
                ]
                codec_nin_ids = [
                    document["_id"]
                    async for document in collection.find({"ref": {"$nin": [oid1, oid2]}}, sort=[("_id", 1)])
                ]
                codec_array_in_ids = [
                    document["_id"]
                    async for document in collection.find({"refs": {"$in": [oid1]}}, sort=[("_id", 1)])
                ]
                codec_array_nin_ids = [
                    document["_id"]
                    async for document in collection.find({"refs": {"$nin": [oid1]}}, sort=[("_id", 1)])
                ]
                codec_array_ne_ids = [
                    document["_id"]
                    async for document in collection.find({"refs": {"$ne": oid1}}, sort=[("_id", 1)])
                ]
                update_result = await collection.update_one(
                    {"ref": {"$nin": [oid1, oid2]}},
                    {"$set": {"selected": True}},
                )
                updated = [
                    document["_id"]
                    async for document in collection.find({"selected": True}, sort=[("_id", 1)])
                ]
                results[engine_name] = {
                    "ne_ids": ne_ids,
                    "in_ids": in_ids,
                    "nin_ids": nin_ids,
                    "codec_nin_ids": codec_nin_ids,
                    "codec_array_in_ids": codec_array_in_ids,
                    "codec_array_nin_ids": codec_array_nin_ids,
                    "codec_array_ne_ids": codec_array_ne_ids,
                    "update": (update_result.matched_count, update_result.modified_count),
                    "updated": updated,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_null_and_missing_semantics_match_in_memory_and_sqlite_for_not_ne_nin_and_comparisons(self):
        results: dict[str, dict[str, list[str]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "x": 3})
                await collection.insert_one({"_id": "2", "x": None})
                await collection.insert_one({"_id": "3"})

                results[engine_name] = {
                    "not_eq": [
                        document["_id"]
                        async for document in collection.find({"x": {"$not": {"$eq": 3}}}, sort=[("_id", 1)])
                    ],
                    "lt": [
                        document["_id"]
                        async for document in collection.find({"x": {"$lt": 4}}, sort=[("_id", 1)])
                    ],
                    "ne": [
                        document["_id"]
                        async for document in collection.find({"x": {"$ne": 3}}, sort=[("_id", 1)])
                    ],
                    "nin": [
                        document["_id"]
                        async for document in collection.find({"x": {"$nin": [3]}}, sort=[("_id", 1)])
                    ],
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_descending_sort_on_mixed_scalars_matches_in_memory_and_sqlite(self):
        results: dict[str, list[str]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "null", "mixed": None})
                await collection.insert_one({"_id": "number", "mixed": 7})
                await collection.insert_one({"_id": "string", "mixed": "alpha"})
                await collection.insert_one({"_id": "objectid", "mixed": ObjectId("0123456789abcdef01234567")})
                await collection.insert_one({"_id": "bool", "mixed": True})
                await collection.insert_one({"_id": "datetime", "mixed": datetime.datetime(2025, 1, 2, 3, 4, 5)})

                results[engine_name] = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("mixed", -1)])
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_pure_sql_mixed_scalar_sort_including_uuid_matches_in_memory_and_sqlite(self):
        results: dict[str, tuple[list[str], list[str]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "null", "mixed": None})
                await collection.insert_one({"_id": "number", "mixed": 7})
                await collection.insert_one({"_id": "string", "mixed": "alpha"})
                await collection.insert_one({"_id": "uuid", "mixed": uuid.UUID("12345678-1234-5678-1234-567812345678")})
                await collection.insert_one({"_id": "objectid", "mixed": ObjectId("0123456789abcdef01234567")})
                await collection.insert_one({"_id": "bool", "mixed": True})
                await collection.insert_one({"_id": "datetime", "mixed": datetime.datetime(2025, 1, 2, 3, 4, 5)})

                ascending = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("mixed", 1)])
                ]
                descending = [
                    document["_id"]
                    async for document in collection.find({}, sort=[("mixed", -1)])
                ]
                results[engine_name] = (ascending, descending)

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_mod_filter_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "count": 10})
                await collection.insert_one({"_id": "2", "count": 11})
                await collection.insert_one({"_id": "3", "count": 12})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"count": {"$mod": [3, 1]}},
                        {"count": 1, "_id": 0},
                        sort=[("count", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_mod_filter_with_sort_skip_limit_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "count": -5})
                await collection.insert_one({"_id": "2", "count": 4})
                await collection.insert_one({"_id": "3", "count": 7})
                await collection.insert_one({"_id": "4", "count": 8})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"count": {"$mod": [3, 1]}},
                        {"count": 1, "_id": 0},
                        sort=[("count", 1)],
                        skip=1,
                        limit=1,
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_regex_prefix_filter_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "title": "Ada Lovelace"})
                await collection.insert_one({"_id": "2", "title": "Grace Hopper"})
                await collection.insert_one({"_id": "3", "title": 123})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"title": {"$regex": "^Ada"}},
                        {"title": 1, "_id": 0},
                        sort=[("title", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_regex_ascii_ignore_case_filter_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "title": "ada lovelace"})
                await collection.insert_one({"_id": "2", "title": "grace hopper"})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"title": {"$regex": "^Ada", "$options": "i"}},
                        {"title": 1, "_id": 0},
                        sort=[("title", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_all_filter_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "tags": ["python", "sqlite"]})
                await collection.insert_one({"_id": "2", "tags": ["python"]})
                await collection.insert_one({"_id": "3", "tags": "python"})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"tags": {"$all": ["python", "sqlite"]}},
                        {"_id": 1},
                        sort=[("_id", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_elem_match_scalar_array_filter_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "scores": [7, 2]})
                await collection.insert_one({"_id": "2", "scores": [4]})
                await collection.insert_one({"_id": "3", "scores": 7})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"scores": {"$elemMatch": {"$gt": 5}}},
                        {"_id": 1},
                        sort=[("_id", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_range_filter_with_mixed_scalars_and_arrays_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "value": 7})
                await collection.insert_one({"_id": "2", "value": [8, 1]})
                await collection.insert_one({"_id": "3", "value": [2]})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        {"value": {"$gt": 5}},
                        {"_id": 1},
                        sort=[("_id", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_not_and_elem_match_filters_match_in_memory_and_sqlite(self):
        results: dict[str, tuple[list[dict], list[dict]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "name": "Ada", "scores": [1, 4, 7], "items": [{"kind": "a"}, {"kind": "b"}]})
                await collection.insert_one({"_id": "2", "name": "Grace", "scores": [2, 5], "items": [{"kind": "c"}]})
                await collection.insert_one({"_id": "3", "name": "Alan", "scores": [3, 8], "items": [{"kind": "b"}]})

                not_documents = [
                    document
                    async for document in collection.find(
                        {"name": {"$not": {"$regex": "^gr", "$options": "i"}}},
                        {"name": 1, "_id": 0},
                        sort=[("name", 1)],
                    )
                ]
                elem_documents = [
                    document
                    async for document in collection.find(
                        {"scores": {"$elemMatch": {"$gt": 3, "$lt": 5}}},
                        {"name": 1, "_id": 0},
                        sort=[("name", 1)],
                    )
                ]
                results[engine_name] = (not_documents, elem_documents)

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_update_array_and_numeric_operators_match_in_memory_and_sqlite(self):
        results: dict[str, dict] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "count": 1, "tags": ["python"]})

                await collection.update_one({"_id": "1"}, {"$inc": {"count": 2}})
                await collection.update_one({"_id": "1"}, {"$push": {"tags": "mongodb"}})
                await collection.update_one({"_id": "1"}, {"$addToSet": {"tags": "python"}})
                await collection.update_one({"_id": "1"}, {"$addToSet": {"tags": "sqlite"}})
                await collection.update_one({"_id": "1"}, {"$pull": {"tags": "mongodb"}})

                results[engine_name] = await collection.find_one({"_id": "1"})

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_upsert_with_matching_document_respects_secondary_unique_index_in_both_engines(self):
        outcomes: dict[str, str] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.create_index(["email"], unique=True)
                await collection.insert_one({"_id": "1", "email": "a@example.com"})
                await collection.insert_one({"_id": "2", "email": "b@example.com"})

                with self.assertRaises(DuplicateKeyError):
                    await collection.update_one(
                        {"_id": "2"},
                        {"$set": {"email": "a@example.com"}},
                        upsert=True,
                    )

                outcomes[engine_name] = (await collection.find_one({"_id": "2"}))["email"]

        self.assertEqual(outcomes["memory"], "b@example.com")
        self.assertEqual(outcomes["sqlite"], "b@example.com")

    async def test_find_one_and_modify_family_matches_in_memory_and_sqlite(self):
        results: dict[str, tuple[object, object, object, list[dict]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_many(
                    [
                        {"_id": "1", "kind": "view", "rank": 2, "done": False},
                        {"_id": "2", "kind": "view", "rank": 1, "done": False},
                    ]
                )

                before = await collection.find_one_and_update(
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                    return_document=ReturnDocument.BEFORE,
                    projection={"rank": 1, "done": 1, "_id": 0},
                )
                after = await collection.find_one_and_replace(
                    {"kind": "view"},
                    {"kind": "view", "rank": 1, "done": False, "tag": "replaced"},
                    sort=[("rank", 1)],
                    return_document=ReturnDocument.AFTER,
                    projection={"rank": 1, "done": 1, "tag": 1, "_id": 0},
                )
                deleted = await collection.find_one_and_delete(
                    {"kind": "view"},
                    sort=[("rank", -1)],
                    projection={"rank": 1, "_id": 0},
                )
                remaining = await collection.find({}, sort=[("_id", 1)]).to_list()

                results[engine_name] = (before, after, deleted, remaining)

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_distinct_matches_in_memory_and_sqlite_for_arrays_documents_and_filters(self):
        results: dict[str, dict[str, list[object]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_many(
                    [
                        {"_id": "1", "kind": "view", "tags": ["python", "mongodb"], "profile": {"city": "Madrid"}, "items": [{"code": "a"}]},
                        {"_id": "2", "kind": "view", "tags": ["python"], "profile": {"city": "Sevilla"}, "items": [{"code": "b"}]},
                        {"_id": "3", "kind": "click", "tags": ["sqlite"], "profile": {"city": "Madrid"}, "items": [{"code": "a"}]},
                    ]
                )

                results[engine_name] = {
                    "tags": await collection.distinct("tags"),
                    "cities": await collection.distinct("profile.city", {"kind": "view"}),
                    "items": await collection.distinct("items"),
                    "codes": await collection.distinct("items.code"),
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_collation_sensitive_operations_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        collation = {"locale": "en", "strength": 2, "numericOrdering": True}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("users")
                await collection.insert_many(
                    [
                        {"_id": "1", "name": "Alice", "rank": "10"},
                        {"_id": "2", "name": "alice", "rank": "2"},
                        {"_id": "3", "name": "Bob", "rank": "11"},
                    ]
                )

                found = await collection.find({"name": "ALICE"}, sort=[("rank", 1)], collation=collation).to_list()
                updated = await collection.update_one(
                    {"name": "alice"},
                    {"$set": {"matched": True}},
                    collation=collation,
                )
                distinct = await collection.distinct("name", collation=collation)
                deleted = await collection.delete_one({"name": "bob"}, collation=collation)
                remaining = await collection.find({}, sort=[("_id", 1)]).to_list()

                results[engine_name] = {
                    "found": found,
                    "updated": (updated.matched_count, updated.modified_count),
                    "distinct": distinct,
                    "deleted": deleted.deleted_count,
                    "remaining": remaining,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_public_error_contracts_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.create_index([("email", 1)], unique=True)
                await collection.insert_one({"_id": "1", "email": "ada@example.com"})

                with self.assertRaises(DuplicateKeyError) as duplicate_ctx:
                    await collection.insert_one({"_id": "2", "email": "ada@example.com"})

                with self.assertRaises(OperationFailure) as hint_ctx:
                    await collection.find({"email": "ada@example.com"}, hint="missing_idx").to_list()

                with self.assertRaises(OperationFailure) as command_ctx:
                    await client.alpha.command({"unsupportedThing": 1})

                results[engine_name] = {
                    "duplicate": {
                        "type": type(duplicate_ctx.exception).__name__,
                        "code": duplicate_ctx.exception.code,
                        "message": str(duplicate_ctx.exception),
                    },
                    "hint": {
                        "type": type(hint_ctx.exception).__name__,
                        "code": hint_ctx.exception.code,
                        "message": str(hint_ctx.exception),
                    },
                    "unsupported": {
                        "type": type(command_ctx.exception).__name__,
                        "code": command_ctx.exception.code,
                        "message": str(command_ctx.exception),
                    },
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_bulk_write_error_contract_matches_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.create_index([("email", 1)], unique=True)
                await collection.insert_one({"_id": "1", "email": "a@example.com"})

                with self.assertRaises(BulkWriteError) as bulk_ctx:
                    await collection.bulk_write(
                        [
                            InsertOne({"_id": "1", "email": "x@example.com"}),
                            InsertOne({"_id": "2", "email": "a@example.com"}),
                            InsertOne({"_id": "3", "email": "b@example.com"}),
                        ],
                        ordered=False,
                    )

                details = bulk_ctx.exception.details
                remaining = await collection.find({}, sort=[("_id", 1)]).to_list()
                results[engine_name] = {
                    "type": type(bulk_ctx.exception).__name__,
                    "code": bulk_ctx.exception.code,
                    "details": {
                        "writeErrors": [
                            {
                                "index": item["index"],
                                "code": item["code"],
                                "op": item["op"],
                            }
                            for item in details["writeErrors"]
                        ],
                        "nInserted": details["nInserted"],
                        "nMatched": details["nMatched"],
                        "nModified": details["nModified"],
                        "nRemoved": details["nRemoved"],
                        "nUpserted": details["nUpserted"],
                        "upserted": details["upserted"],
                    },
                    "remaining": remaining,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_session_misuse_error_contract_matches_in_memory_and_sqlite_for_common_cases(self):
        results: dict[str, list[tuple[str, str, str]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                session = client.start_session()
                cases: list[tuple[str, str, str]] = []

                with self.assertRaises(InvalidOperation) as commit_ctx:
                    session.commit_transaction()
                cases.append(("commit_without_tx", type(commit_ctx.exception).__name__, str(commit_ctx.exception)))

                session.start_transaction()
                with self.assertRaises(InvalidOperation) as start_ctx:
                    session.start_transaction()
                cases.append(("double_start", type(start_ctx.exception).__name__, str(start_ctx.exception)))

                session.abort_transaction()
                with self.assertRaises(InvalidOperation) as abort_ctx:
                    session.abort_transaction()
                cases.append(("double_abort", type(abort_ctx.exception).__name__, str(abort_ctx.exception)))

                results[engine_name] = cases

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_index_admin_error_contract_matches_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.create_index([("alias", 1)], name="alias_one")
                await collection.create_index([("alias", 1)], name="alias_two")

                with self.assertRaises(OperationFailure) as missing_ctx:
                    await collection.drop_index([("missing", 1)])

                with self.assertRaises(OperationFailure) as ambiguous_ctx:
                    await collection.drop_index([("alias", 1)])

                await collection.drop_index("alias_one")
                remaining = await collection.list_indexes().to_list()
                results[engine_name] = {
                    "missing": (
                        type(missing_ctx.exception).__name__,
                        missing_ctx.exception.code,
                        str(missing_ctx.exception),
                    ),
                    "ambiguous": (
                        type(ambiguous_ctx.exception).__name__,
                        ambiguous_ctx.exception.code,
                        str(ambiguous_ctx.exception),
                    ),
                    "remaining": remaining,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_bulk_write_results_and_final_state_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                bulk = await collection.bulk_write(
                    [
                        InsertOne({"_id": "1", "kind": "view", "count": 1}),
                        InsertOne({"_id": "2", "kind": "view", "count": 2}),
                        UpdateOne({"_id": "1"}, {"$set": {"tag": "updated"}}),
                        UpdateMany({"kind": "view"}, {"$inc": {"count": 1}}),
                        ReplaceOne({"_id": "2"}, {"_id": "2", "kind": "view", "count": 9, "tag": "replaced"}),
                        DeleteOne({"_id": "1"}),
                    ],
                    ordered=True,
                )
                remaining = await collection.find({}, sort=[("_id", 1)]).to_list()

                results[engine_name] = {
                    "counts": {
                        "inserted": bulk.inserted_count,
                        "matched": bulk.matched_count,
                        "modified": bulk.modified_count,
                        "deleted": bulk.deleted_count,
                        "upserted": bulk.upserted_count,
                    },
                    "remaining": remaining,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_watch_resume_expiration_and_invalidate_contract_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name, change_stream_history_size=2) as client:
                collection = client.observe.get_collection("items")
                await collection.insert_one({"_id": "1", "name": "Ada"})
                await collection.insert_one({"_id": "2", "name": "Grace"})
                await collection.insert_one({"_id": "3", "name": "Linus"})

                with self.assertRaises(OperationFailure) as expired_ctx:
                    collection.watch(
                        resume_after={"_data": "1"},
                        max_await_time_ms=50,
                    )

                invalidate_stream = collection.watch(max_await_time_ms=50)
                await collection.drop()
                invalidate_event = await invalidate_stream.try_next()
                self.assertIsNotNone(invalidate_event)
                results[engine_name] = {
                    "expired": (
                        type(expired_ctx.exception).__name__,
                        expired_ctx.exception.code,
                        str(expired_ctx.exception),
                    ),
                    "invalidate": {
                        "operationType": invalidate_event["operationType"],
                        "ns": invalidate_event["ns"],
                        "alive": invalidate_stream.alive,
                    },
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_search_index_lifecycle_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict[str, object]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("docs")
                await collection.create_search_index({"mappings": {"dynamic": False}})
                await collection.create_search_indexes(
                    [
                        SearchIndexModel({"mappings": {"dynamic": True}}, name="by_text"),
                        SearchIndexModel(
                            {
                                "fields": [
                                    {
                                        "type": "vector",
                                        "path": "embedding",
                                        "numDimensions": 3,
                                        "similarity": "cosine",
                                    }
                                ]
                            },
                            name="by_vector",
                            type="vectorSearch",
                        ),
                    ]
                )
                await collection.update_search_index("default", {"mappings": {"dynamic": True}})
                await collection.drop_search_index("by_vector")
                listed = await collection.list_search_indexes().to_list()
                results[engine_name] = [
                    {
                        "name": item["name"],
                        "latestDefinition": item["latestDefinition"],
                        "queryMode": item["queryMode"],
                        "capabilities": item["capabilities"],
                        "status": item["status"],
                    }
                    for item in listed
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_database_command_list_and_explain_contracts_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.alpha.get_collection("events")

                empty_collections = await client.alpha.command({"listCollections": 1})
                empty_indexes = await client.alpha.command({"listIndexes": "events"})

                await collection.insert_one({"_id": "1", "kind": "view"})
                await collection.create_index([("kind", 1)], name="kind_idx")

                filled_collections = await client.alpha.command({"listCollections": 1})
                filled_indexes = await client.alpha.command({"listIndexes": "events"})
                aggregate_explain = await client.alpha.command(
                    {
                        "explain": {
                            "aggregate": "events",
                            "pipeline": [{"$densify": {"field": "kind", "range": {"step": 1, "bounds": "full"}}}],
                            "cursor": {"batchSize": 1},
                        }
                    }
                )

                results[engine_name] = {
                    "emptyCollections": empty_collections,
                    "emptyIndexes": empty_indexes,
                    "filledCollections": filled_collections,
                    "filledIndexes": filled_indexes,
                    "aggregateExplain": {
                        "remainingPipeline": aggregate_explain["remaining_pipeline"],
                        "pushdownMode": aggregate_explain["pushdown"]["mode"],
                        "planningMode": aggregate_explain["planning_mode"],
                    },
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_search_and_vector_search_contracts_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.search_runtime.get_collection("docs")
                await collection.insert_many(
                    [
                        {"_id": "1", "title": "Ada Lovelace", "body": "Analytical engine notes", "score": 9, "embedding": [1.0, 0.0, 0.0]},
                        {"_id": "2", "title": "Grace Hopper", "body": "Compiler pioneer", "score": 15, "embedding": [0.0, 1.0, 0.0]},
                        {"_id": "3", "title": "Notes", "body": "Ada wrote the first algorithm", "score": 11, "embedding": [0.9, 0.1, 0.0]},
                    ]
                )
                await collection.create_search_indexes(
                    [
                        SearchIndexModel(
                            {
                                "mappings": {
                                    "dynamic": False,
                                    "fields": {
                                        "title": {"type": "string"},
                                        "body": {"type": "string"},
                                        "score": {"type": "number"},
                                    },
                                }
                            },
                            name="by_text",
                        ),
                        SearchIndexModel(
                            {
                                "fields": [
                                    {
                                        "type": "vector",
                                        "path": "embedding",
                                        "numDimensions": 3,
                                        "similarity": "cosine",
                                    }
                                ]
                            },
                            name="by_vector",
                            type="vectorSearch",
                        ),
                    ]
                )

                search_hits = await collection.aggregate(
                    [
                        {"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}},
                        {"$sort": {"_id": 1}},
                    ]
                ).to_list()
                autocomplete_hits = await collection.aggregate(
                    [
                        {"$search": {"index": "by_text", "autocomplete": {"query": "ada", "path": ["title", "body"]}}},
                        {"$sort": {"_id": 1}},
                    ]
                ).to_list()
                wildcard_hits = await collection.aggregate(
                    [
                        {"$search": {"index": "by_text", "wildcard": {"query": "*algorithm*", "path": "body"}}},
                        {"$sort": {"_id": 1}},
                    ]
                ).to_list()
                exists_hits = await collection.aggregate(
                    [
                        {"$search": {"index": "by_text", "exists": {"path": "title"}}},
                        {"$sort": {"_id": 1}},
                    ]
                ).to_list()
                near_hits = await collection.aggregate(
                    [
                        {"$search": {"index": "by_text", "near": {"path": "score", "origin": 10, "pivot": 2}}},
                        {"$project": {"_id": 1}},
                    ]
                ).to_list()
                compound_hits = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "compound": {
                                    "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                    "filter": [{"wildcard": {"query": "*algorithm*", "path": "body"}}],
                                },
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                ).to_list()
                compound_should_near_hits = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "compound": {
                                    "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                    "should": [
                                        {"exists": {"path": "title"}},
                                        {"near": {"path": "score", "origin": 10, "pivot": 2}},
                                    ],
                                    "minimumShouldMatch": 0,
                                },
                            }
                        },
                        {"$project": {"_id": 1}},
                    ]
                ).to_list()
                compound_candidateable_should_hits = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "compound": {
                                    "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                    "should": [
                                        {"exists": {"path": "title"}},
                                        {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                        {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                    ],
                                    "minimumShouldMatch": 1,
                                },
                            }
                        },
                        {"$project": {"_id": 1}},
                    ]
                ).to_list()
                compound_candidateable_should_limited_hits = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "compound": {
                                    "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                    "should": [
                                        {"exists": {"path": "title"}},
                                        {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                        {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                    ],
                                    "minimumShouldMatch": 1,
                                },
                            }
                        },
                        {"$project": {"_id": 1}},
                        {"$limit": 1},
                    ]
                ).to_list()
                vector_hits = await collection.aggregate(
                    [
                        {
                            "$vectorSearch": {
                                "index": "by_vector",
                                "path": "embedding",
                                "queryVector": [1.0, 0.0, 0.0],
                                "limit": 2,
                                "numCandidates": 3,
                            }
                        },
                        {"$project": {"_id": 1, "title": 1}},
                    ]
                ).to_list()

                search_explain = await collection.aggregate(
                    [{"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}}]
                ).explain()
                autocomplete_explain = await collection.aggregate(
                    [{"$search": {"index": "by_text", "autocomplete": {"query": "ada", "path": ["title", "body"]}}}]
                ).explain()
                wildcard_explain = await collection.aggregate(
                    [{"$search": {"index": "by_text", "wildcard": {"query": "*algorithm*", "path": "body"}}}]
                ).explain()
                exists_explain = await collection.aggregate(
                    [{"$search": {"index": "by_text", "exists": {"path": "title"}}}]
                ).explain()
                near_explain = await collection.aggregate(
                    [{"$search": {"index": "by_text", "near": {"path": "score", "origin": 10, "pivot": 2}}}]
                ).explain()
                compound_explain = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "compound": {
                                    "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                    "filter": [{"wildcard": {"query": "*algorithm*", "path": "body"}}],
                                },
                            }
                        }
                    ]
                ).explain()
                compound_candidateable_should_limited_explain = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "compound": {
                                    "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                    "should": [
                                        {"exists": {"path": "title"}},
                                        {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                        {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                    ],
                                    "minimumShouldMatch": 1,
                                },
                            }
                        },
                        {"$project": {"_id": 1}},
                        {"$limit": 1},
                    ]
                ).explain()
                vector_explain = await collection.aggregate(
                    [
                        {
                            "$vectorSearch": {
                                "index": "by_vector",
                                "path": "embedding",
                                "queryVector": [1.0, 0.0, 0.0],
                                "limit": 2,
                                "numCandidates": 3,
                            }
                        }
                    ]
                ).explain()

                results[engine_name] = {
                    "search_hits": [document["_id"] for document in search_hits],
                    "autocomplete_hits": [document["_id"] for document in autocomplete_hits],
                    "wildcard_hits": [document["_id"] for document in wildcard_hits],
                    "exists_hits": [document["_id"] for document in exists_hits],
                    "near_hits": [document["_id"] for document in near_hits],
                    "compound_hits": [document["_id"] for document in compound_hits],
                    "compound_should_near_hits": [document["_id"] for document in compound_should_near_hits],
                    "compound_candidateable_should_hits": [document["_id"] for document in compound_candidateable_should_hits],
                    "compound_candidateable_should_limited_hits": [document["_id"] for document in compound_candidateable_should_limited_hits],
                    "vector_hits": [document["_id"] for document in vector_hits],
                    "search_explain": {
                        "hint": search_explain["hint"],
                        "batch_size": search_explain["batch_size"],
                        "planning_mode": search_explain["planning_mode"],
                        "remaining_pipeline": search_explain["remaining_pipeline"],
                        "query_operator": search_explain["engine_plan"]["details"]["queryOperator"],
                        "paths": search_explain["engine_plan"]["details"]["paths"],
                    },
                    "autocomplete_explain": {
                        "query_operator": autocomplete_explain["engine_plan"]["details"]["queryOperator"],
                        "paths": autocomplete_explain["engine_plan"]["details"]["paths"],
                    },
                    "wildcard_explain": {
                        "query_operator": wildcard_explain["engine_plan"]["details"]["queryOperator"],
                        "paths": wildcard_explain["engine_plan"]["details"]["paths"],
                    },
                    "exists_explain": {
                        "query_operator": exists_explain["engine_plan"]["details"]["queryOperator"],
                        "paths": exists_explain["engine_plan"]["details"]["paths"],
                    },
                    "near_explain": {
                        "query_operator": near_explain["engine_plan"]["details"]["queryOperator"],
                        "path": near_explain["engine_plan"]["details"]["path"],
                        "pivot": near_explain["engine_plan"]["details"]["pivot"],
                    },
                    "compound_explain": {
                        "query_operator": compound_explain["engine_plan"]["details"]["queryOperator"],
                        "compound": compound_explain["engine_plan"]["details"]["compound"],
                        "ranking": compound_explain["engine_plan"]["details"]["ranking"],
                    },
                    "compound_candidateable_should_limited_explain": {
                        "topk_limit_hint": compound_candidateable_should_limited_explain["engine_plan"]["details"]["topKLimitHint"],
                        "search_result_limit_hint": compound_candidateable_should_limited_explain["pushdown"]["searchResultLimitHint"],
                    },
                    "vector_explain": {
                        "hint": vector_explain["hint"],
                        "batch_size": vector_explain["batch_size"],
                        "planning_mode": vector_explain["planning_mode"],
                        "remaining_pipeline": vector_explain["remaining_pipeline"],
                        "path": vector_explain["engine_plan"]["details"]["path"],
                        "limit": vector_explain["engine_plan"]["details"]["limit"],
                        "num_candidates": vector_explain["engine_plan"]["details"]["numCandidates"],
                    },
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_search_runtime_error_contracts_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, tuple[str, object, str]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.search_runtime.get_collection("docs")
                await collection.insert_one({"_id": "1", "title": "Ada", "embedding": [1.0, 0.0, 0.0]})
                await collection.create_search_indexes(
                    [
                        SearchIndexModel({"mappings": {"dynamic": True}}, name="by_text"),
                        SearchIndexModel(
                            {
                                "fields": [
                                    {
                                        "type": "vector",
                                        "path": "embedding",
                                        "numDimensions": 3,
                                        "similarity": "cosine",
                                    }
                                ]
                            },
                            name="by_vector",
                            type="vectorSearch",
                        ),
                    ]
                )

                with self.assertRaises(OperationFailure) as wrong_cap_ctx:
                    await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_text",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "numCandidates": 3,
                                    "limit": 2,
                                }
                            }
                        ]
                    ).to_list()

                with self.assertRaises(OperationFailure) as missing_idx_ctx:
                    await collection.aggregate(
                        [{"$search": {"index": "missing", "text": {"query": "ada", "path": "title"}}}]
                    ).to_list()

                results[engine_name] = {
                    "wrong_capability": (
                        type(wrong_cap_ctx.exception).__name__,
                        wrong_cap_ctx.exception.code,
                        str(wrong_cap_ctx.exception),
                    ),
                    "missing_index": (
                        type(missing_idx_ctx.exception).__name__,
                        missing_idx_ctx.exception.code,
                        str(missing_idx_ctx.exception),
                    ),
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_watch_event_shape_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict[str, object]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.observe.get_collection("items")
                stream = collection.watch(max_await_time_ms=100, full_document="updateLookup")

                await collection.insert_one({"_id": "1", "name": "Ada"})
                await collection.update_one({"_id": "1"}, {"$set": {"name": "Ada Lovelace"}})
                await collection.delete_one({"_id": "1"})
                await collection.drop()

                captured: list[dict[str, object]] = []
                for _ in range(4):
                    event = await stream.try_next()
                    self.assertIsNotNone(event)
                    payload = {
                        "operationType": event["operationType"],
                        "ns": event["ns"],
                    }
                    if "fullDocument" in event:
                        payload["fullDocument"] = event["fullDocument"]
                    captured.append(payload)
                results[engine_name] = captured

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_with_transaction_and_find_one_and_update_upsert_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")

                session = client.start_session()
                transaction_result = await session.with_transaction(
                    lambda active_session: collection.insert_one(
                        {"_id": "tx", "kind": "transactional"},
                        session=active_session,
                    )
                )
                committed = await collection.find_one({"_id": "tx"})

                before = await collection.find_one_and_update(
                    {"_id": "missing-before"},
                    {"$set": {"kind": "new", "state": "upserted"}},
                    upsert=True,
                    return_document=ReturnDocument.BEFORE,
                )
                after = await collection.find_one_and_update(
                    {"_id": "missing-after"},
                    {"$set": {"kind": "new", "state": "upserted"}},
                    upsert=True,
                    return_document=ReturnDocument.AFTER,
                )
                after_state = await collection.find({}, sort=[("_id", 1)]).to_list()

                results[engine_name] = {
                    "transaction_result": transaction_result.inserted_id,
                    "committed": committed,
                    "before": before,
                    "after": after,
                    "after_state": after_state,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_session_transaction_visibility_and_commit_abort_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.alpha.get_collection("users")

                abort_session = client.start_session()
                abort_session.start_transaction()
                await collection.insert_one({"_id": "abort", "name": "Abort"}, session=abort_session)
                abort_visible_inside = await collection.find_one({"_id": "abort"}, session=abort_session)
                abort_session.abort_transaction()
                abort_after = await collection.find_one({"_id": "abort"})

                commit_session = client.start_session()
                commit_session.start_transaction()
                await collection.insert_one({"_id": "commit", "name": "Commit"}, session=commit_session)
                commit_visible_inside = await collection.find_one({"_id": "commit"}, session=commit_session)
                commit_session.commit_transaction()
                commit_after = await collection.find_one({"_id": "commit"})

                results[engine_name] = {
                    "abort_inside": abort_visible_inside,
                    "abort_after": abort_after,
                    "commit_inside": commit_visible_inside,
                    "commit_after": commit_after,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_session_bound_admin_visibility_matches_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                session = client.start_session()
                session.start_transaction()
                await client.alpha.create_collection("events", session=session)
                inside_collections = await client.alpha.list_collection_names(session=session)
                session.abort_transaction()
                after_abort = await client.alpha.list_collection_names()

                results[engine_name] = {
                    "inside": inside_collections,
                    "after_abort": after_abort,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_database_command_surface_matches_in_memory_and_sqlite_for_admin_subsets(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.alpha.get_collection("events")
                await collection.insert_many(
                    [
                        {"_id": "1", "kind": "view"},
                        {"_id": "2", "kind": "click"},
                    ]
                )
                await collection.create_index([("kind", 1)], name="kind_idx")

                coll_stats = await client.alpha.command({"collStats": "events"})
                db_stats = await client.alpha.command("dbStats")
                find_explain = await client.alpha.command(
                    {
                        "explain": {
                            "find": "events",
                            "filter": {"kind": "view"},
                            "hint": "kind_idx",
                            "comment": "find explain",
                            "maxTimeMS": 50,
                            "batchSize": 1,
                        }
                    }
                )
                aggregate_explain = await client.alpha.command(
                    {
                        "explain": {
                            "aggregate": "events",
                            "pipeline": [{"$match": {"kind": "view"}}],
                            "cursor": {"batchSize": 1},
                            "hint": "kind_idx",
                            "comment": "agg explain",
                            "maxTimeMS": 50,
                            "allowDiskUse": True,
                        }
                    }
                )

                results[engine_name] = {
                    "collStats": {
                        "ns": coll_stats["ns"],
                        "count": coll_stats["count"],
                        "nindexes": coll_stats["nindexes"],
                        "ok": coll_stats["ok"],
                    },
                    "dbStats": {
                        "db": db_stats["db"],
                        "collections": db_stats["collections"],
                        "objects": db_stats["objects"],
                        "indexes": db_stats["indexes"],
                        "ok": db_stats["ok"],
                    },
                    "findExplain": {
                        "hint": find_explain["hint"],
                        "hinted_index": find_explain["hinted_index"],
                        "comment": find_explain["comment"],
                        "max_time_ms": find_explain["max_time_ms"],
                        "batch_size": find_explain["batch_size"],
                        "ok": find_explain["ok"],
                    },
                    "aggregateExplain": {
                        "hint": aggregate_explain["hint"],
                        "comment": aggregate_explain["comment"],
                        "max_time_ms": aggregate_explain["max_time_ms"],
                        "batch_size": aggregate_explain["batch_size"],
                        "allow_disk_use": aggregate_explain["allow_disk_use"],
                        "ok": aggregate_explain["ok"],
                    },
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_coll_stats_stage_and_hidden_indexes_match_in_memory_and_sqlite(self):
        results: dict[str, dict[str, object]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.alpha.get_collection("events")
                await collection.insert_many([{"_id": "1", "kind": "view"}, {"_id": "2", "kind": "click"}])
                await client.alpha.command(
                    {
                        "createIndexes": "events",
                        "indexes": [{"key": {"kind": 1}, "name": "kind_hidden", "hidden": True}],
                    }
                )

                collstats = await collection.aggregate(
                    [{"$collStats": {"count": {}, "storageStats": {"scale": 2}}}]
                ).to_list()
                indexes = await collection.list_indexes().to_list()
                hidden_hint_error = None
                try:
                    await collection.find({"kind": "view"}, hint="kind_hidden").to_list()
                except Exception as exc:  # pragma: no branch
                    hidden_hint_error = str(exc)

                results[engine_name] = {
                    "collStats": collstats,
                    "indexes": indexes,
                    "hiddenHintError": hidden_hint_error,
                }

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_pop_update_matches_in_memory_and_sqlite(self):
        results: dict[str, dict] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "tags": ["python", "mongodb", "sqlite"]})

                await collection.update_one({"_id": "1"}, {"$pop": {"tags": -1}})
                await collection.update_one({"_id": "1"}, {"$pop": {"tags": 1}})

                results[engine_name] = await collection.find_one({"_id": "1"})

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_nested_pop_and_embedded_document_array_updates_match_in_memory_and_sqlite(self):
        results: dict[str, dict] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one(
                    {
                        "_id": "1",
                        "profile": {"tags": ["python"]},
                        "items": [{"kind": "a"}],
                    }
                )

                await collection.update_one({"_id": "1"}, {"$pop": {"profile.tags": 1}})
                await collection.update_one({"_id": "1"}, {"$addToSet": {"items": {"kind": "a"}}})
                await collection.update_one({"_id": "1"}, {"$addToSet": {"items": {"kind": "b"}}})
                await collection.update_one({"_id": "1"}, {"$pull": {"items": {"kind": "a"}}})

                results[engine_name] = await collection.find_one({"_id": "1"})

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_pull_with_predicate_document_matches_in_memory_and_sqlite(self):
        results: dict[str, dict] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one(
                    {
                        "_id": "1",
                        "items": [
                            {"kind": "a", "qty": 1},
                            {"kind": "b", "qty": 3},
                            {"kind": "c", "qty": 5},
                        ],
                    }
                )

                await collection.update_one({"_id": "1"}, {"$pull": {"items": {"qty": {"$gte": 3}}}})

                results[engine_name] = await collection.find_one({"_id": "1"})

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_pull_with_plain_document_matches_exact_document_in_both_engines(self):
        results: dict[str, dict] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one(
                    {
                        "_id": "1",
                        "items": [
                            {"kind": "a"},
                            {"kind": "a", "qty": 1},
                            {"kind": "b"},
                        ],
                    }
                )

                await collection.update_one({"_id": "1"}, {"$pull": {"items": {"kind": "a"}}})

                results[engine_name] = await collection.find_one({"_id": "1"})

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_pull_with_embedded_objectid_subset_matches_in_memory_and_sqlite(self):
        if BsonObjectId is None:
            self.skipTest("bson is not installed")

        results: dict[str, dict] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one(
                    {
                        "_id": "1",
                        "tasks": [
                            {
                                "enrollment_task_id": BsonObjectId("65f0a1000000000000000000"),
                                "name": "Ada",
                                "studied_at": None,
                            },
                            {
                                "enrollment_task_id": BsonObjectId("65f0a1000000000000000001"),
                                "name": "Grace",
                                "studied_at": None,
                            },
                        ],
                    }
                )

                await collection.update_one(
                    {"_id": "1"},
                    {
                        "$pull": {
                            "tasks": {
                                "enrollment_task_id": BsonObjectId("65f0a1000000000000000000"),
                            }
                        }
                    },
                )

                results[engine_name] = await collection.find_one({"_id": "1"})

        self.assertEqual(results["memory"], results["sqlite"])
        self.assertEqual(
            results["memory"],
            {
                "_id": "1",
                "tasks": [
                    {
                        "enrollment_task_id": ObjectId("65f0a1000000000000000001"),
                        "name": "Grace",
                        "studied_at": None,
                    }
                ],
            },
        )

    async def test_pull_duplicates_and_add_to_set_embedded_document_order_match_in_memory_and_sqlite(self):
        results: dict[str, dict] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one(
                    {
                        "_id": "1",
                        "tags": ["python", "mongodb", "python", "sqlite", "python"],
                        "items": [{"kind": "a", "qty": 1}],
                    }
                )

                await collection.update_one({"_id": "1"}, {"$pull": {"tags": "python"}})
                await collection.update_one({"_id": "1"}, {"$addToSet": {"items": {"qty": 1, "kind": "a"}}})

                results[engine_name] = await collection.find_one({"_id": "1"})

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_elem_match_with_compound_subdocument_filter_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        filter_spec = {
            "items": {
                "$elemMatch": {
                    "kind": "b",
                    "qty": {"$gte": 3},
                    "price": {"$lt": 10},
                }
            }
        }
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "items": [{"kind": "a", "qty": 1, "price": 5}]})
                await collection.insert_one({"_id": "2", "items": [{"kind": "b", "qty": 3, "price": 9}]})
                await collection.insert_one({"_id": "3", "items": [{"kind": "b", "qty": 5, "price": 12}]})

                results[engine_name] = [
                    document
                    async for document in collection.find(
                        filter_spec,
                        {"_id": 1},
                        sort=[("_id", 1)],
                    )
                ]

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_pipeline_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                await collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                await collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})

                results[engine_name] = await collection.aggregate(
                    [
                        {"$match": {"kind": "view"}},
                        {"$sort": {"rank": 1}},
                        {"$project": {"payload.city": 1, "_id": 0}},
                    ]
                ).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_unwind_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "tags": ["python", "mongodb"]})
                await collection.insert_one({"_id": "2", "tags": ["sqlite"]})
                await collection.insert_one({"_id": "3", "tags": []})

                results[engine_name] = await collection.aggregate(
                    [
                        {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True, "includeArrayIndex": "index"}},
                        {"$sort": {"_id": 1}},
                        {"$project": {"_id": 1, "tags": 1, "index": 1}},
                    ]
                ).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_group_and_expression_pipeline_matches_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {"$addFields": {"effective_amount": {"$add": ["$amount", {"$ifNull": ["$bonus", 0]}]}}},
            {"$match": {"$expr": {"$gt": ["$effective_amount", 4]}}},
            {
                "$group": {
                    "_id": "$kind",
                    "total": {"$sum": "$effective_amount"},
                    "minimum": {"$min": "$effective_amount"},
                    "maximum": {"$max": "$effective_amount"},
                    "average": {"$avg": "$effective_amount"},
                    "users": {"$push": "$user"},
                    "first_user": {"$first": "$user"},
                }
            },
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 1, "total": 1, "minimum": 1, "maximum": 1, "average": 1, "users": 1, "first_user": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view", "amount": 10, "user": "ada"})
                await collection.insert_one({"_id": "2", "kind": "view", "amount": 7, "bonus": 1, "user": "grace"})
                await collection.insert_one({"_id": "3", "kind": "click", "amount": 3, "bonus": 2, "user": "alan"})
                await collection.insert_one({"_id": "4", "kind": "click", "amount": 2, "user": "linus"})

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_lookup_and_replace_root_match_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "_id", "as": "user"}},
            {"$addFields": {"user": {"$arrayElemAt": ["$user", 0]}}},
            {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", "$user"]}}},
            {"$project": {"user": 0, "user_id": 0}},
            {"$sort": {"kind": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                users = client.get_database("db").get_collection("users")
                events = client.get_database("db").get_collection("events")
                await users.insert_one({"_id": "u1", "name": "Ada", "city": "Sevilla"})
                await users.insert_one({"_id": "u2", "name": "Grace", "city": "Madrid"})
                await events.insert_one({"_id": "1", "kind": "view", "user_id": "u1"})
                await events.insert_one({"_id": "2", "kind": "click", "user_id": "u2"})

                results[engine_name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_array_transform_expressions_match_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "mapped": {"$map": {"input": "$tags", "as": "tag", "in": {"$toString": "$$tag"}}},
                    "filtered": {"$filter": {"input": "$numbers", "as": "n", "cond": {"$gt": ["$$n", 2]}}},
                    "reduced": {"$reduce": {"input": "$numbers", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}},
                    "concatenated": {"$concatArrays": ["$tags", "$other_tags"]},
                    "unioned": {"$setUnion": ["$tags", "$other_tags"]},
                }
            }
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "tags": ["a", "b", "c"], "other_tags": ["b", "d"], "numbers": [1, 2, 3, 4]})

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_array_transform_expressions_return_null_for_missing_inputs_in_both_engines(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "mapped": {"$map": {"input": "$tags", "as": "tag", "in": {"$toString": "$$tag"}}},
                    "filtered": {"$filter": {"input": "$numbers", "as": "n", "cond": {"$gt": ["$$n", 2]}}},
                    "reduced": {"$reduce": {"input": "$numbers", "initialValue": 99, "in": {"$add": ["$$value", "$$this"]}}},
                }
            }
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1"})

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_group_ignores_non_numeric_values_for_sum_and_avg_in_both_engines(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$group": {
                    "_id": "$kind",
                    "total": {"$sum": "$amount"},
                    "average": {"$avg": "$amount"},
                }
            }
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view", "amount": 10})
                await collection.insert_one({"_id": "2", "kind": "view", "amount": "oops"})
                await collection.insert_one({"_id": "3", "kind": "view", "amount": None})
                await collection.insert_one({"_id": "4", "kind": "view", "amount": 6})

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_lookup_multiple_and_missing_matches_align_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}},
            {"$project": {"_id": 1, "users": 1}},
            {"$sort": {"_id": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                users = client.get_database("db").get_collection("users")
                events = client.get_database("db").get_collection("events")
                await users.insert_one({"_id": "u1", "tenant": "a"})
                await users.insert_one({"_id": "u2", "tenant": "a"})
                await users.insert_one({"_id": "u3"})
                await events.insert_one({"_id": "1", "tenant": "a"})
                await events.insert_one({"_id": "2", "tenant": "missing"})
                await events.insert_one({"_id": "3"})

                results[engine_name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_lookup_with_dotted_variable_paths_aligns_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$lookup": {
                    "from": "users",
                    "let": {"ctx": "$tenant"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$tenant", "$$ctx.id"]}}},
                        {"$project": {"_id": 0, "name": "$$ROOT.profile.name", "city": "$$ctx.id"}},
                    ],
                    "as": "users",
                }
            },
            {"$sort": {"_id": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                users = client.get_database("db").get_collection("users")
                events = client.get_database("db").get_collection("events")
                await users.insert_one({"_id": "u1", "tenant": "a", "profile": {"name": "Ada"}})
                await users.insert_one({"_id": "u2", "tenant": "b", "profile": {"name": "Linus"}})
                await events.insert_one({"_id": "1", "tenant": {"id": "a"}})
                await events.insert_one({"_id": "2", "tenant": {"id": "b"}})

                results[engine_name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_count_on_empty_input_aligns_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [{"$match": {"kind": "missing"}}, {"$count": "total"}]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view"})
                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])
        self.assertEqual(results["memory"], [{"total": 0}])

    async def test_aggregate_lookup_with_let_and_pipeline_align_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$lookup": {
                    "from": "users",
                    "let": {"tenantId": "$tenant"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                        {"$project": {"_id": 0, "name": 1}},
                        {"$sort": {"name": 1}},
                    ],
                    "as": "users",
                }
            },
            {"$sort": {"_id": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                users = client.get_database("db").get_collection("users")
                events = client.get_database("db").get_collection("events")
                await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                await events.insert_one({"_id": "1", "tenant": "a"})
                await events.insert_one({"_id": "2", "tenant": "b"})

                results[engine_name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_lookup_with_local_foreign_and_pipeline_align_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$lookup": {
                    "from": "users",
                    "localField": "tenant",
                    "foreignField": "tenant",
                    "let": {"tenantId": "$tenant"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                        {"$project": {"_id": 0, "name": 1}},
                        {"$sort": {"name": 1}},
                    ],
                    "as": "users",
                }
            },
            {"$sort": {"_id": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                users = client.get_database("db").get_collection("users")
                events = client.get_database("db").get_collection("events")
                await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                await users.insert_one({"_id": "u4", "tenant": "c", "name": "Nope"})
                await events.insert_one({"_id": "1", "tenant": "a"})
                await events.insert_one({"_id": "2", "tenant": "b"})

                results[engine_name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_lookup_with_missing_foreign_collection_aligns_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}},
            {"$project": {"_id": 1, "users": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                events = client.get_database("db").get_collection("events")
                await events.insert_one({"_id": "1", "tenant": "a"})

                results[engine_name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_nested_lookup_inside_lookup_pipeline_aligns_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$lookup": {
                    "from": "users",
                    "let": {"tenantId": "$tenant"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                        {"$lookup": {"from": "roles", "localField": "role_id", "foreignField": "_id", "as": "roles"}},
                        {"$project": {"_id": 0, "name": 1, "roles": 1}},
                        {"$sort": {"name": 1}},
                    ],
                    "as": "users",
                }
            }
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                roles = client.get_database("db").get_collection("roles")
                users = client.get_database("db").get_collection("users")
                events = client.get_database("db").get_collection("events")
                await roles.insert_one({"_id": "r1", "label": "admin"})
                await roles.insert_one({"_id": "r2", "label": "staff"})
                await users.insert_one({"_id": "u1", "tenant": "a", "role_id": "r1", "name": "Ada"})
                await users.insert_one({"_id": "u2", "tenant": "a", "role_id": "r2", "name": "Grace"})
                await events.insert_one({"_id": "1", "tenant": "a"})

                results[engine_name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_join_operator_combinations_align_in_memory_and_sqlite(self):
        pipelines = {
            "no_join": [
                {"$match": {"$expr": {"$eq": ["$tenant", "a"]}}},
                {"$project": {"_id": 1, "tenant": 1}},
            ],
            "inner_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$match": {"$expr": {"$gt": [{"$size": "$users"}, 0]}}},
                {"$project": {"_id": 1, "tenant": 1, "users": 1}},
                {"$sort": {"_id": 1}},
            ],
            "document_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$addFields": {"joined_user": {"$mergeObjects": [{"tenant": "$tenant"}, {"$first": "$users"}]}}},
                {"$project": {"_id": 1, "joined_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            "left_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {
                    "$addFields": {
                        "joined_user": {
                            "$cond": [
                                {"$gt": [{"$size": "$users"}, 0]},
                                {"$arrayElemAt": ["$users", 0]},
                                {"name": "unknown"},
                            ]
                        }
                    }
                },
                {"$project": {"_id": 1, "joined_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            "count_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$set": {"user_count": {"$size": "$users"}}},
                {"$project": {"_id": 1, "user_count": 1}},
                {"$sort": {"_id": 1}},
            ],
            "aggregated_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$set": {"primary_user": {"$ifNull": [{"$first": "$users"}, {"name": "unknown"}]}}},
                {"$project": {"_id": 1, "primary_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            "merge_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$user_id"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$_id", "$$ref_key"]}}},
                            {"$project": {"name": 1, "role": 1}},
                        ],
                        "as": "user_doc",
                    }
                },
                {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", {"$arrayElemAt": ["$user_doc", 0]}]}}},
                {"$project": {"user_doc": 0}},
                {"$sort": {"_id": 1}},
            ],
        }

        results: dict[str, dict[str, list[dict]]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                users = client.get_database("db").get_collection("users")
                events = client.get_database("db").get_collection("events")
                await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada", "role": "admin"})
                await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace", "role": "staff"})
                await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus", "role": "owner"})
                await events.insert_one({"_id": "e1", "tenant": "a", "user_id": "u1", "kind": "view"})
                await events.insert_one({"_id": "e2", "tenant": "b", "user_id": "u3", "kind": "click"})
                await events.insert_one({"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"})
                results[engine_name] = {}
                for name, pipeline in pipelines.items():
                    results[engine_name][name] = await events.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_replace_with_aligns_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "profile": {"name": "Ada"}})

                results[engine_name] = await collection.aggregate([{"$replaceWith": "$profile"}]).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_array_object_index_and_sort_helpers_align_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "mapped": {"$arrayToObject": "$pairs"},
                    "position": {"$indexOfArray": ["$numbers", 3]},
                    "sorted_numbers": {"$sortArray": {"input": "$numbers", "sortBy": 1}},
                    "sorted_items": {"$sortArray": {"input": "$items", "sortBy": {"rank": 1}}},
                }
            }
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one(
                    {
                        "_id": "1",
                        "pairs": [["a", 1], ["b", 2]],
                        "numbers": [4, 1, 3, 2],
                        "items": [
                            {"rank": 3, "name": "c"},
                            {"rank": 1, "name": "a"},
                            {"rank": 2, "name": "b"},
                        ],
                    }
                )

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_facet_and_date_trunc_align_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {"$addFields": {"bucket": {"$dateTrunc": {"date": "$created_at", "unit": "hour"}}}},
            {
                "$facet": {
                    "views": [
                        {"$match": {"kind": "view"}},
                        {"$project": {"_id": 0, "bucket": 1}},
                        {"$sort": {"bucket": 1}},
                    ],
                    "scores": [
                        {"$group": {"_id": "$kind", "total": {"$sum": "$score"}}},
                        {"$sort": {"_id": 1}},
                    ],
                }
            },
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one(
                    {"_id": "1", "kind": "view", "score": 5, "created_at": datetime.datetime(2026, 3, 24, 10, 37, 52)}
                )
                await collection.insert_one(
                    {"_id": "2", "kind": "view", "score": 7, "created_at": datetime.datetime(2026, 3, 24, 10, 5, 0)}
                )
                await collection.insert_one(
                    {"_id": "3", "kind": "click", "score": 3, "created_at": datetime.datetime(2026, 3, 24, 11, 10, 0)}
                )

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_bucket_aligns_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$bucket": {
                    "groupBy": "$score",
                    "boundaries": [0, 10, 20],
                    "default": "other",
                    "output": {
                        "count": {"$sum": 1},
                        "kinds": {"$push": "$kind"},
                    },
                }
            }
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "score": 5, "kind": "view"})
                await collection.insert_one({"_id": "2", "score": 12, "kind": "view"})
                await collection.insert_one({"_id": "3", "score": 17, "kind": "click"})
                await collection.insert_one({"_id": "4", "score": 25, "kind": "view"})

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])

    async def test_aggregate_bucket_auto_and_set_window_fields_align_in_memory_and_sqlite(self):
        bucket_results: dict[str, list[dict]] = {}
        window_results: dict[str, list[dict]] = {}
        bucket_pipeline = [
            {
                "$bucketAuto": {
                    "groupBy": "$score",
                    "buckets": 2,
                    "output": {"count": {"$sum": 1}},
                }
            }
        ]
        window_pipeline = [
            {
                "$setWindowFields": {
                    "partitionBy": "$tenant",
                    "sortBy": {"rank": 1},
                    "output": {
                        "runningTotal": {
                            "$sum": "$score",
                            "window": {"documents": ["unbounded", "current"]},
                        }
                    },
                }
            },
            {"$project": {"_id": 0, "tenant": 1, "rank": 1, "runningTotal": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "tenant": "a", "rank": 1, "score": 5, "kind": "view"})
                await collection.insert_one({"_id": "2", "tenant": "a", "rank": 2, "score": 7, "kind": "view"})
                await collection.insert_one({"_id": "3", "tenant": "b", "rank": 1, "score": 3, "kind": "click"})
                await collection.insert_one({"_id": "4", "tenant": "b", "rank": 2, "score": 9, "kind": "click"})

                bucket_results[engine_name] = await collection.aggregate(bucket_pipeline).to_list()
                window_results[engine_name] = await collection.aggregate(window_pipeline).to_list()

        self.assertEqual(bucket_results["memory"], bucket_results["sqlite"])
        self.assertEqual(window_results["memory"], window_results["sqlite"])

    async def test_aggregate_count_and_sort_by_count_align_in_memory_and_sqlite(self):
        counted_results: dict[str, list[dict]] = {}
        sorted_results: dict[str, list[dict]] = {}
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "kind": "view"})
                await collection.insert_one({"_id": "2", "kind": "view"})
                await collection.insert_one({"_id": "3", "kind": "click"})

                counted_results[engine_name] = await collection.aggregate([{"$count": "total"}]).to_list()
                sorted_results[engine_name] = await collection.aggregate([{"$sortByCount": "$kind"}]).to_list()

        self.assertEqual(counted_results["memory"], counted_results["sqlite"])
        self.assertEqual(sorted_results["memory"], sorted_results["sqlite"])

    async def test_aggregate_set_window_fields_numeric_range_align_in_memory_and_sqlite(self):
        results: dict[str, list[dict]] = {}
        pipeline = [
            {
                "$setWindowFields": {
                    "sortBy": {"score": 1},
                    "output": {
                        "nearbyTotal": {
                            "$sum": "$score",
                            "window": {"range": [-2, 2]},
                        }
                    },
                }
            },
            {"$project": {"_id": 1, "nearbyTotal": 1}},
        ]
        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "score": 5})
                await collection.insert_one({"_id": "2", "score": 7})
                await collection.insert_one({"_id": "3", "score": 12})

                results[engine_name] = await collection.aggregate(pipeline).to_list()

        self.assertEqual(results["memory"], results["sqlite"])


class SyncEngineParityTests(unittest.TestCase):
    def test_sync_cursor_contract_matches_in_memory_and_sqlite(self):
        results: dict[str, tuple[list[dict], dict | None, list[dict]]] = {}
        engines = {
            "memory": MemoryEngine,
            "sqlite": SQLiteEngine,
        }
        for engine_name, factory in engines.items():
            with MongoClient(factory()) as client:
                collection = client.get_database("db").get_collection("events")
                collection.insert_one({"_id": "1", "kind": "view", "rank": 3})
                collection.insert_one({"_id": "2", "kind": "view", "rank": 1})
                collection.insert_one({"_id": "3", "kind": "view", "rank": 2})

                cursor = collection.find({"kind": "view"})
                documents = cursor.sort([("rank", 1)]).skip(1).limit(1).to_list()
                first = collection.find({"kind": "view"}).sort([("rank", 1)]).first()
                iterated = list(collection.find({"kind": "view"}).sort([("rank", 1)]))
                results[engine_name] = (documents, first, iterated)

        self.assertEqual(results["memory"], results["sqlite"])
