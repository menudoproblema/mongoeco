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
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import DBRef, ObjectId, UNDEFINED
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

    async def test_external_bson_object_id_smoke_matches_in_memory_and_sqlite(self):
        if BsonObjectId is None:
            self.skipTest("PyMongo bson no esta instalado")

        canonical = ObjectId("0123456789abcdef01234567")
        external = BsonObjectId(str(canonical))
        results: dict[str, dict[str, object]] = {}

        for engine_name in ("memory", "sqlite"):
            async with open_client(engine_name) as client:
                collection = client.get_database("db").get_collection("events")
                await collection.insert_one({"_id": "1", "owner_id": canonical, "owners": [canonical]})

                matched = await collection.find_one({"owner_id": external})
                updated = await collection.update_one({"_id": "1"}, {"$addToSet": {"owners": external}})
                final_document = await collection.find_one({"_id": "1"})

                results[engine_name] = {
                    "matched_id": None if matched is None else matched["_id"],
                    "modified_count": updated.modified_count,
                    "owners": final_document["owners"],
                }

        self.assertEqual(
            results,
            {
                "memory": {"matched_id": "1", "modified_count": 0, "owners": [canonical]},
                "sqlite": {"matched_id": "1", "modified_count": 0, "owners": [canonical]},
            },
        )

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
