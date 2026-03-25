import threading
import datetime
import unittest

from mongoeco import ClientSession, MongoClient, ObjectId
from mongoeco.api._sync.aggregation_cursor import AggregationCursor
from mongoeco.api._sync.cursor import Cursor
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError, InvalidOperation, OperationFailure
SYNC_ENGINE_FACTORIES = {
    "memory": MemoryEngine,
    "sqlite": SQLiteEngine,
}


class SyncApiIntegrationTests(unittest.TestCase):
    def test_client_supports_attribute_and_item_access_with_lazy_connect(self):
        client = MongoClient()
        try:
            client["alpha"]["users"].insert_one({"_id": "1", "name": "Ada"})

            found = client.alpha.users.find_one({"_id": "1"})
            collections = client["alpha"].list_collection_names()

            self.assertEqual(found, {"_id": "1", "name": "Ada"})
            self.assertEqual(collections, ["users"])
        finally:
            client.close()

    def test_insert_find_and_list_names(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    result = collection.insert_one({"name": "Ada"})
                    found = collection.find_one({"_id": result.inserted_id})

                    self.assertIsInstance(result.inserted_id, ObjectId)
                    self.assertEqual(found["name"], "Ada")
                    self.assertEqual(set(client.list_database_names()), {"test"})
                    self.assertEqual(client.test.list_collection_names(), ["users"])

    def test_duplicate_id_raises(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "same", "name": "Ada"})

                    with self.assertRaises(DuplicateKeyError):
                        collection.insert_one({"_id": "same", "name": "Grace"})

    def test_update_and_projection(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "user-1", "profile": {"name": "Ada"}})

                    result = collection.update_one(
                        {"_id": "user-1"},
                        {"$set": {"profile.role": "admin"}},
                    )
                    found = collection.find_one({"_id": "user-1"}, {"profile.role": 1, "_id": 0})

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(found, {"profile": {"role": "admin"}})

    def test_find_supports_sort_skip_and_limit(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "rank": 3})
                    collection.insert_one({"_id": "2", "rank": 1})
                    collection.insert_one({"_id": "3", "rank": 2})

                    documents = collection.find({}, sort=[("rank", 1)], skip=1, limit=1).to_list()

                    self.assertEqual(documents, [{"_id": "3", "rank": 2}])

    def test_find_supports_first_call_without_prior_connection(self):
        client = MongoClient()
        try:
            documents = client.test.users.find({}).to_list()
        finally:
            client.close()

        self.assertEqual(documents, [])

    def test_update_one_sort_is_profile_gated(self):
        with MongoClient(MemoryEngine(), pymongo_profile='4.9') as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
            collection.insert_one({"_id": "2", "kind": "view", "rank": 1})

            with self.assertRaises(TypeError):
                collection.update_one(
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                )

    def test_update_one_sort_updates_first_sorted_document(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory(), pymongo_profile='4.11') as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "kind": "view", "rank": 2, "done": False})
                    collection.insert_one({"_id": "2", "kind": "view", "rank": 1, "done": False})

                    result = collection.update_one(
                        {"kind": "view"},
                        {"$set": {"done": True}},
                        sort=[("rank", 1)],
                    )
                    first = collection.find_one({"_id": "1"})
                    second = collection.find_one({"_id": "2"})

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(result.modified_count, 1)
                    self.assertFalse(first["done"])
                    self.assertTrue(second["done"])

    def test_find_preserves_async_validation_for_falsy_invalid_filters(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "name": "Ada"})

                    with self.assertRaises(TypeError):
                        collection.find([]).to_list()
                    with self.assertRaises(TypeError):
                        collection.find(False).to_list()

    def test_find_supports_filter_sort_skip_limit_and_projection_together(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                    collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                    collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})
                    collection.insert_one({"_id": "4", "kind": "view", "rank": 4, "payload": {"city": "Valencia"}})

                    documents = collection.find(
                        {"kind": "view"},
                        {"payload.city": 1, "_id": 0},
                        sort=[("rank", 1)],
                        skip=1,
                        limit=1,
                    ).to_list()

                    self.assertEqual(documents, [{"payload": {"city": "Sevilla"}}])

    def test_find_supports_array_sort_and_projection_together(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "rank": [3, 8], "payload": {"city": "Sevilla"}})
                    collection.insert_one({"_id": "2", "rank": [1, 9], "payload": {"city": "Madrid"}})
                    collection.insert_one({"_id": "3", "rank": [2, 4], "payload": {"city": "Bilbao"}})

                    documents = collection.find({}, {"payload.city": 1, "_id": 0}, sort=[("rank", 1)]).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"payload": {"city": "Madrid"}},
                            {"payload": {"city": "Bilbao"}},
                            {"payload": {"city": "Sevilla"}},
                        ],
                    )

    def test_find_returns_cursor_with_iteration_to_list_and_first(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "rank": 3})
                    collection.insert_one({"_id": "2", "rank": 1})
                    collection.insert_one({"_id": "3", "rank": 2})

                    cursor = collection.find({})

                    self.assertIsInstance(cursor, Cursor)
                    self.assertEqual(cursor.sort([("rank", 1)]).skip(1).limit(1).to_list(), [{"_id": "3", "rank": 2}])
                    self.assertEqual(collection.find({}).sort([("rank", 1)]).first(), {"_id": "2", "rank": 1})

    def test_find_cursor_rejects_mutation_after_iteration_starts(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "rank": 1})
                    collection.insert_one({"_id": "2", "rank": 2})

                    cursor = collection.find({})
                    self.assertEqual(cursor.first(), {"_id": "1", "rank": 1})

                    with self.assertRaises(InvalidOperation):
                        cursor.limit(1)
                    with self.assertRaises(InvalidOperation):
                        cursor.skip(1)
                    with self.assertRaises(InvalidOperation):
                        cursor.sort([("rank", 1)])

    def test_sqlite_find_iteration_finishes_without_cancelled_error(self):
        with MongoClient(SQLiteEngine()) as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "rank": 1})
            collection.insert_one({"_id": "2", "rank": 2})
            collection.insert_one({"_id": "3", "rank": 3})

            documents = []
            for document in collection.find({}, sort=[("rank", 1)]):
                documents.append(document)

            self.assertEqual(
                documents,
                [
                    {"_id": "1", "rank": 1},
                    {"_id": "2", "rank": 2},
                    {"_id": "3", "rank": 3},
                ],
            )

    def test_aggregate_supports_minimal_pipeline(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                    collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                    collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})

                    cursor = collection.aggregate(
                        [
                            {"$match": {"kind": "view"}},
                            {"$sort": {"rank": 1}},
                            {"$project": {"payload.city": 1, "_id": 0}},
                        ]
                    )

                    self.assertIsInstance(cursor, AggregationCursor)
                    self.assertEqual(
                        cursor.to_list(),
                        [{"payload": {"city": "Bilbao"}}, {"payload": {"city": "Sevilla"}}],
                    )
                    self.assertEqual(
                        collection.aggregate([{"$sort": {"rank": 1}}]).first(),
                        {"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}},
                    )

    def test_aggregate_supports_sync_iteration(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
                    collection.insert_one({"_id": "2", "kind": "click", "rank": 1})

                    documents = list(collection.aggregate([{"$sort": {"rank": 1}}]))

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "2", "kind": "click", "rank": 1},
                            {"_id": "1", "kind": "view", "rank": 2},
                        ],
                    )

    def test_aggregate_supports_unwind(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "tags": ["python", "mongodb"]})
                    collection.insert_one({"_id": "2", "tags": ["sqlite"]})

                    documents = collection.aggregate(
                        [
                            {"$unwind": "$tags"},
                            {"$sort": {"tags": 1}},
                            {"$project": {"tags": 1, "_id": 0}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"tags": "mongodb"},
                            {"tags": "python"},
                            {"tags": "sqlite"},
                        ],
                    )

    def test_aggregate_supports_group_and_computed_expressions(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "kind": "view", "amount": 10, "user": "ada"})
                    collection.insert_one({"_id": "2", "kind": "view", "amount": 7, "bonus": 1, "user": "grace"})
                    collection.insert_one({"_id": "3", "kind": "click", "amount": 3, "bonus": 2, "user": "alan"})

                    documents = collection.aggregate(
                        [
                            {"$addFields": {"effective_amount": {"$add": ["$amount", {"$ifNull": ["$bonus", 0]}]}}},
                            {"$match": {"$expr": {"$gte": ["$effective_amount", 5]}}},
                            {"$group": {"_id": "$kind", "total": {"$sum": "$effective_amount"}, "first_user": {"$first": "$user"}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "click", "total": 5, "first_user": "alan"},
                            {"_id": "view", "total": 18, "first_user": "ada"},
                        ],
                    )

    def test_aggregate_supports_lookup_and_replace_root(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one({"_id": "u1", "name": "Ada", "city": "Sevilla"})
                    users.insert_one({"_id": "u2", "name": "Grace", "city": "Madrid"})
                    events.insert_one({"_id": "1", "kind": "view", "user_id": "u1"})
                    events.insert_one({"_id": "2", "kind": "click", "user_id": "u2"})

                    documents = events.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "_id", "as": "user"}},
                            {"$addFields": {"user": {"$arrayElemAt": ["$user", 0]}}},
                            {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", "$user"]}}},
                            {"$project": {"user": 0, "user_id": 0}},
                            {"$sort": {"kind": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "u2", "kind": "click", "name": "Grace", "city": "Madrid"},
                            {"_id": "u1", "kind": "view", "name": "Ada", "city": "Sevilla"},
                        ],
                    )

    def test_aggregate_supports_array_transform_expressions(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "tags": ["a", "b", "c"], "other_tags": ["b", "d"], "numbers": [1, 2, 3, 4]})

                    documents = collection.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "mapped": ["a", "b", "c"],
                                "filtered": [3, 4],
                                "reduced": 10,
                                "concatenated": ["a", "b", "c", "b", "d"],
                                "unioned": ["a", "b", "c", "d"],
                            }
                        ],
                    )

    def test_aggregate_supports_lookup_with_multiple_and_missing_matches(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one({"_id": "u1", "tenant": "a"})
                    users.insert_one({"_id": "u2", "tenant": "a"})
                    users.insert_one({"_id": "u3"})
                    events.insert_one({"_id": "1", "tenant": "a"})
                    events.insert_one({"_id": "2", "tenant": "missing"})
                    events.insert_one({"_id": "3"})

                    documents = events.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}},
                            {"$project": {"_id": 1, "users": 1}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "users": [{"_id": "u1", "tenant": "a"}, {"_id": "u2", "tenant": "a"}]},
                            {"_id": "2", "users": []},
                            {"_id": "3", "users": [{"_id": "u3"}]},
                        ],
                    )

    def test_aggregate_supports_lookup_with_let_and_pipeline(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                    users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                    users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                    events.insert_one({"_id": "1", "tenant": "a"})
                    events.insert_one({"_id": "2", "tenant": "b"})

                    documents = events.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                            {"_id": "2", "tenant": "b", "users": [{"name": "Linus"}]},
                        ],
                    )

    def test_aggregate_supports_lookup_with_local_foreign_and_pipeline(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                    users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                    users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                    users.insert_one({"_id": "u4", "tenant": "c", "name": "Nope"})
                    events.insert_one({"_id": "1", "tenant": "a"})
                    events.insert_one({"_id": "2", "tenant": "b"})

                    documents = events.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                            {"_id": "2", "tenant": "b", "users": [{"name": "Linus"}]},
                        ],
                    )

    def test_aggregate_supports_lookup_with_missing_foreign_collection(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    events.insert_one({"_id": "1", "tenant": "a"})

                    documents = events.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}},
                            {"$project": {"_id": 1, "users": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "1", "users": []}])

    def test_aggregate_supports_nested_lookup_inside_lookup_pipeline(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    roles = client.test.roles
                    roles.insert_one({"_id": "r1", "label": "admin"})
                    roles.insert_one({"_id": "r2", "label": "staff"})
                    users.insert_one({"_id": "u1", "tenant": "a", "role_id": "r1", "name": "Ada"})
                    users.insert_one({"_id": "u2", "tenant": "a", "role_id": "r2", "name": "Grace"})
                    events.insert_one({"_id": "1", "tenant": "a"})

                    documents = events.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "tenant": "a",
                                "users": [
                                    {"name": "Ada", "roles": [{"_id": "r1", "label": "admin"}]},
                                    {"name": "Grace", "roles": [{"_id": "r2", "label": "staff"}]},
                                ],
                            }
                        ],
                    )

    def test_aggregate_supports_join_operator_combinations(self):
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
        expected = {
            "no_join": [{"_id": "e1", "tenant": "a"}],
            "inner_join": [
                {"_id": "e1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                {"_id": "e2", "tenant": "b", "users": [{"name": "Linus"}]},
            ],
            "document_join": [
                {"_id": "e1", "joined_user": {"tenant": "a", "name": "Ada"}},
                {"_id": "e2", "joined_user": {"tenant": "b", "name": "Linus"}},
                {"_id": "e3", "joined_user": {"tenant": "missing"}},
            ],
            "left_join": [
                {"_id": "e1", "joined_user": {"name": "Ada"}},
                {"_id": "e2", "joined_user": {"name": "Linus"}},
                {"_id": "e3", "joined_user": {"name": "unknown"}},
            ],
            "count_join": [
                {"_id": "e1", "user_count": 2},
                {"_id": "e2", "user_count": 1},
                {"_id": "e3", "user_count": 0},
            ],
            "aggregated_join": [
                {"_id": "e1", "primary_user": {"name": "Ada"}},
                {"_id": "e2", "primary_user": {"name": "Linus"}},
                {"_id": "e3", "primary_user": {"name": "unknown"}},
            ],
            "merge_join": [
                {"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"},
                {"_id": "u1", "tenant": "a", "user_id": "u1", "kind": "view", "name": "Ada", "role": "admin"},
                {"_id": "u3", "tenant": "b", "user_id": "u3", "kind": "click", "name": "Linus", "role": "owner"},
            ],
        }

        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada", "role": "admin"})
                    users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace", "role": "staff"})
                    users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus", "role": "owner"})
                    events.insert_one({"_id": "e1", "tenant": "a", "user_id": "u1", "kind": "view"})
                    events.insert_one({"_id": "e2", "tenant": "b", "user_id": "u3", "kind": "click"})
                    events.insert_one({"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"})

                    for name, pipeline in pipelines.items():
                        with self.subTest(engine=engine_name, pipeline=name):
                            documents = events.aggregate(pipeline).to_list()
                            self.assertEqual(documents, expected[name])

    def test_aggregate_match_supports_nested_expr_inside_and_or(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "a": 5, "b": 11})
                    collection.insert_one({"_id": "2", "a": 5, "b": 9})
                    collection.insert_one({"_id": "3", "a": 4, "b": 20})

                    documents = collection.aggregate(
                        [{"$match": {"$and": [{"a": 5}, {"$expr": {"$gt": ["$b", 10]}}]}}]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "1", "a": 5, "b": 11}])

    def test_aggregate_supports_replace_with(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "profile": {"name": "Ada"}})

                    documents = collection.aggregate([{"$replaceWith": "$profile"}]).to_list()

                    self.assertEqual(documents, [{"name": "Ada"}])

    def test_aggregate_supports_array_to_object_index_of_array_and_sort_array(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one(
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

                    documents = collection.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "mapped": {"a": 1, "b": 2},
                                "position": 2,
                                "sorted_numbers": [1, 2, 3, 4],
                                "sorted_items": [
                                    {"rank": 1, "name": "a"},
                                    {"rank": 2, "name": "b"},
                                    {"rank": 3, "name": "c"},
                                ],
                            }
                        ],
                    )

    def test_aggregate_supports_facet_and_date_trunc(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one(
                        {"_id": "1", "kind": "view", "score": 5, "created_at": datetime.datetime(2026, 3, 24, 10, 37, 52)}
                    )
                    collection.insert_one(
                        {"_id": "2", "kind": "view", "score": 7, "created_at": datetime.datetime(2026, 3, 24, 10, 5, 0)}
                    )
                    collection.insert_one(
                        {"_id": "3", "kind": "click", "score": 3, "created_at": datetime.datetime(2026, 3, 24, 11, 10, 0)}
                    )

                    documents = collection.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "views": [
                                    {"bucket": datetime.datetime(2026, 3, 24, 10, 0, 0)},
                                    {"bucket": datetime.datetime(2026, 3, 24, 10, 0, 0)},
                                ],
                                "scores": [{"_id": "click", "total": 3}, {"_id": "view", "total": 12}],
                            }
                        ],
                    )

    def test_aggregate_supports_bucket(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "score": 5, "kind": "view"})
                    collection.insert_one({"_id": "2", "score": 12, "kind": "view"})
                    collection.insert_one({"_id": "3", "score": 17, "kind": "click"})
                    collection.insert_one({"_id": "4", "score": 25, "kind": "view"})

                    documents = collection.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": 0, "count": 1, "kinds": ["view"]},
                            {"_id": 10, "count": 2, "kinds": ["view", "click"]},
                            {"_id": "other", "count": 1, "kinds": ["view"]},
                        ],
                    )

    def test_aggregate_supports_bucket_auto_and_set_window_fields(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "tenant": "a", "rank": 1, "score": 5, "kind": "view"})
                    collection.insert_one({"_id": "2", "tenant": "a", "rank": 2, "score": 7, "kind": "view"})
                    collection.insert_one({"_id": "3", "tenant": "b", "rank": 1, "score": 3, "kind": "click"})
                    collection.insert_one({"_id": "4", "tenant": "b", "rank": 2, "score": 9, "kind": "click"})

                    bucketed = collection.aggregate(
                        [
                            {
                                "$bucketAuto": {
                                    "groupBy": "$score",
                                    "buckets": 2,
                                    "output": {"count": {"$sum": 1}},
                                }
                            }
                        ]
                    ).to_list()
                    windowed = collection.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        bucketed,
                        [
                            {"_id": {"min": 3, "max": 7}, "count": 2},
                            {"_id": {"min": 7, "max": 9}, "count": 2},
                        ],
                    )
                    self.assertEqual(
                        windowed,
                        [
                            {"tenant": "a", "rank": 1, "runningTotal": 5},
                            {"tenant": "a", "rank": 2, "runningTotal": 12},
                            {"tenant": "b", "rank": 1, "runningTotal": 3},
                            {"tenant": "b", "rank": 2, "runningTotal": 12},
                        ],
                    )

    def test_aggregate_supports_count_and_sort_by_count(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "kind": "view"})
                    collection.insert_one({"_id": "2", "kind": "view"})
                    collection.insert_one({"_id": "3", "kind": "click"})

                    counted = collection.aggregate([{"$count": "total"}]).to_list()
                    sorted_counts = collection.aggregate([{"$sortByCount": "$kind"}]).to_list()

                    self.assertEqual(counted, [{"total": 3}])
                    self.assertEqual(sorted_counts, [{"_id": "view", "count": 2}, {"_id": "click", "count": 1}])

    def test_aggregate_supports_set_window_fields_numeric_range(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "score": 5})
                    collection.insert_one({"_id": "2", "score": 7})
                    collection.insert_one({"_id": "3", "score": 12})

                    documents = collection.aggregate(
                        [
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
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "nearbyTotal": 12},
                            {"_id": "2", "nearbyTotal": 12},
                            {"_id": "3", "nearbyTotal": 12},
                        ],
                    )

    def test_aggregate_propagates_operation_failure_for_invalid_stage(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1"})

                    with self.assertRaises(OperationFailure):
                        collection.aggregate([{"$densify": {}}]).to_list()

    def test_index_metadata_round_trip(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    name = collection.create_index(["profile.name"], unique=False)
                    indexes = collection.list_indexes()

                    self.assertEqual(name, "profile.name_1")
                    self.assertEqual(
                        indexes,
                        [{"name": "profile.name_1", "fields": ["profile.name"], "unique": False}],
                    )

    def test_unique_index_is_enforced(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.create_index(["email"], unique=True)
                    collection.insert_one({"_id": "1", "email": "a@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        collection.insert_one({"_id": "2", "email": "a@example.com"})

    def test_compound_unique_index_is_enforced(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.create_index(["tenant", "email"], unique=True)
                    collection.insert_one({"_id": "1", "tenant": "a", "email": "x@example.com"})
                    collection.insert_one({"_id": "2", "tenant": "b", "email": "x@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        collection.insert_one({"_id": "3", "tenant": "a", "email": "x@example.com"})

    def test_nested_unique_index_is_enforced(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.create_index(["profile.email"], unique=True)
                    collection.insert_one({"_id": "1", "profile": {"email": "a@example.com"}})

                    with self.assertRaises(DuplicateKeyError):
                        collection.insert_one({"_id": "2", "profile": {"email": "a@example.com"}})

    def test_sync_client_exposes_session_placeholder_and_accepts_it(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()
                    self.assertIsInstance(session, ClientSession)

                    with session:
                        result = client.test.users.insert_one({"name": "Ada"}, session=session)
                        found = client.test.users.find_one({"_id": result.inserted_id}, session=session)

                    self.assertFalse(session.active)
                    self.assertEqual(found["name"], "Ada")

    def test_sync_session_state_reflects_connected_engine(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()
                    self.assertEqual(len(session.engine_state), 1)
                    state = next(iter(session.engine_state.values()))

                    self.assertTrue(
                        state["connected"]
                    )

    def test_sync_client_accepts_sqlite_engine(self):
        with MongoClient(SQLiteEngine()) as client:
            collection = client.test.users
            result = collection.insert_one({"name": "Ada"})
            found = collection.find_one({"_id": result.inserted_id})

            self.assertEqual(found["name"], "Ada")

    def test_sync_session_transaction_placeholder_round_trip(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()

                    session.start_transaction()
                    self.assertTrue(session.transaction_active)
                    client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=session)
                    session.end_transaction()

                    self.assertFalse(session.transaction_active)

    def test_update_and_delete_support_embedded_document_id(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    collection.insert_one({"_id": doc_id, "name": "Ada"})

                    update_result = collection.update_one(
                        {"_id": {"tenant": 1, "user": 2}},
                        {"$set": {"active": True}},
                    )
                    delete_result = collection.delete_one({"_id": {"tenant": 1, "user": 2}})

                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(delete_result.deleted_count, 1)

    def test_find_one_projection_supports_embedded_document_id(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    collection.insert_one({"_id": doc_id, "name": "Ada", "role": "admin"})

                    found = collection.find_one({"_id": {"tenant": 1, "user": 2}}, {"name": 1, "_id": 0})

                    self.assertEqual(found, {"name": "Ada"})

    def test_shared_memory_engine_is_safe_across_sync_threads(self):
        engine = MemoryEngine()
        errors: list[tuple[int, object]] = []

        def worker(i: int) -> None:
            try:
                with MongoClient(engine) as client:
                    client.test.users.insert_one({"_id": str(i), "n": i})
                    found = client.test.users.find_one({"_id": str(i)})
                    if found is None or found["n"] != i:
                        errors.append((i, found))
            except Exception as exc:  # pragma: no cover
                errors.append((i, exc))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])

    def test_invalid_input_types_raise_type_error(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    with self.assertRaises(TypeError):
                        collection.insert_one([])

                    with self.assertRaises(TypeError):
                        collection.find_one([])

                    with self.assertRaises(TypeError):
                        collection.find_one({}, [])

                    with self.assertRaises(TypeError):
                        collection.update_one([], {})

                    with self.assertRaises(TypeError):
                        collection.update_one({}, [])

                    with self.assertRaises(TypeError):
                        collection.delete_one([])

                    with self.assertRaises(TypeError):
                        collection.count_documents([])

    def test_update_one_rejects_invalid_update_shapes(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    with self.assertRaises(ValueError):
                        collection.update_one({}, {})

                    with self.assertRaises(ValueError):
                        collection.update_one({}, {"name": "Ada"})

                    with self.assertRaises(TypeError):
                        collection.update_one({}, {"$set": []})

    def test_operations_after_close_raise_invalid_operation(self):
        client = MongoClient()
        collection = client.test.users
        client.close()

        with self.assertRaises(InvalidOperation):
            client.list_database_names()

        with self.assertRaises(InvalidOperation):
            collection.find_one({})


class SyncApiLoopSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_client_rejects_usage_inside_active_event_loop(self):
        client = MongoClient()
        try:
            with self.assertRaises(InvalidOperation):
                client.list_database_names()
        finally:
            client.close()
