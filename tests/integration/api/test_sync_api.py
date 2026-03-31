import asyncio
import datetime
import decimal
import re
import threading
import time
import unittest
import uuid
from bson import decode_all

from mongoeco import (
    CodecOptions,
    ClientSession,
    DeleteMany,
    DeleteOne,
    IndexModel,
    InsertOne,
    MongoClient,
    ObjectId,
    ReadConcern,
    ReadPreference,
    ReadPreferenceMode,
    ReplaceOne,
    ReturnDocument,
    SearchIndexModel,
    TransactionOptions,
    UpdateMany,
    UpdateOne,
    WriteConcern,
)
from mongoeco.api._sync.aggregation_cursor import AggregationCursor
from mongoeco.api._sync.cursor import Cursor
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import (
    BulkWriteError,
    CollectionInvalid,
    DocumentValidationFailure,
    DuplicateKeyError,
    InvalidOperation,
    OperationFailure,
)
from tests.integration.api.admin_command_cases import (
    assert_build_info_command_shares_source_of_truth_with_server_info,
    assert_client_server_info_reflects_target_dialect,
    assert_database_command_count_supports_skip_limit_hint_and_comment,
    assert_database_command_distinct_supports_hint_comment_and_max_time,
    assert_database_command_index_commands_support_comment_and_max_time,
    assert_database_command_rejects_invalid_command_shapes,
    assert_database_command_rejects_unsupported_commands,
    assert_database_command_supports_coll_stats_and_db_stats,
    assert_database_command_supports_collection_index_count_and_distinct_commands,
    assert_database_command_supports_explain_for_find_and_aggregate,
    assert_database_command_supports_explain_for_update_and_delete,
    assert_database_command_supports_find_and_aggregate,
    assert_database_command_supports_find_and_modify,
    assert_database_command_supports_insert_update_and_delete,
    assert_database_command_supports_ping_list_collections_and_drop_database,
    assert_database_command_supports_rename_collection_within_current_database,
    assert_database_command_write_commands_surface_write_errors,
    assert_hello_and_is_master_commands_return_handshake_metadata,
    assert_host_info_whats_my_uri_and_cmd_line_opts_commands_return_local_metadata,
    assert_list_collections_command_supports_name_only,
    assert_list_commands_and_connection_status_commands_return_local_admin_metadata,
    assert_server_status_command_returns_local_runtime_metadata,
    assert_validate_collection_returns_metadata_and_rejects_missing_namespace,
)
from tests.support import ENGINE_FACTORIES as SYNC_ENGINE_FACTORIES, open_sync_client


class SyncApiIntegrationTests(unittest.TestCase):
    def test_collection_search_index_lifecycle(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.search.get_collection("docs")

                    created = collection.create_search_index({"mappings": {"dynamic": False}})
                    self.assertEqual(created, "default")

                    created_many = collection.create_search_indexes(
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
                                name="by_keyword",
                                type="vectorSearch",
                            ),
                        ]
                    )
                    self.assertEqual(created_many, ["by_text", "by_keyword"])

                    listed = collection.list_search_indexes().to_list()
                    self.assertEqual(
                        [document["name"] for document in listed],
                        ["by_keyword", "by_text", "default"],
                    )
                    by_keyword = next(
                        document for document in listed if document["name"] == "by_keyword"
                    )
                    self.assertTrue(by_keyword["queryable"])
                    self.assertEqual(by_keyword["status"], "READY")
                    self.assertEqual(by_keyword["queryMode"], "vector")
                    self.assertTrue(by_keyword["experimental"])
                    self.assertEqual(by_keyword["capabilities"], ["vectorSearch"])

                    only_default = collection.list_search_indexes("default").to_list()
                    self.assertEqual(len(only_default), 1)
                    self.assertEqual(only_default[0]["name"], "default")
                    self.assertEqual(only_default[0]["queryMode"], "text")
                    self.assertEqual(only_default[0]["capabilities"], ["text", "phrase"])

                    collection.update_search_index("default", {"mappings": {"dynamic": True}})
                    updated = collection.list_search_indexes("default").first()
                    self.assertEqual(updated["latestDefinition"], {"mappings": {"dynamic": True}})

                    collection.drop_search_index("by_keyword")
                    remaining = collection.list_search_indexes().to_list()
                    self.assertEqual([document["name"] for document in remaining], ["by_text", "default"])

    def test_aggregate_search_executes_text_search_and_rejects_invalid_runtime(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.search_runtime.get_collection("docs")
                    collection.insert_many(
                        [
                            {"_id": 1, "title": "Ada", "body": "Analytical engine notes", "embedding": [1.0, 0.0, 0.0]},
                            {"_id": 2, "title": "Grace", "body": "Compiler pioneer", "embedding": [0.1, 0.9, 0.0]},
                            {"_id": 3, "title": "Notes", "body": "Ada wrote the first algorithm", "embedding": [0.9, 0.1, 0.0]},
                        ]
                    )
                    collection.create_search_indexes(
                        [
                            SearchIndexModel(
                                {
                                    "mappings": {
                                        "dynamic": False,
                                        "fields": {
                                            "title": {"type": "string"},
                                            "body": {"type": "string"},
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

                    hits = collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in hits], [1, 3])

                    collection.update_one({"_id": 2}, {"$set": {"body": "Ada and Grace built compilers"}})
                    collection.delete_one({"_id": 1})
                    updated_hits = collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in updated_hits], [2, 3])

                    explanation = collection.aggregate(
                        [{"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}}]
                    ).explain()
                    self.assertEqual(explanation["engine_plan"]["strategy"], "search")
                    if engine_name == "sqlite":
                        self.assertEqual(explanation["engine_plan"]["details"]["backend"], "fts5")
                        self.assertEqual(explanation["engine_plan"]["details"]["fts5_match"], '"ada"')

                    vector_hits = collection.aggregate(
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
                    ).to_list()
                    self.assertEqual([document["_id"] for document in vector_hits], [3, 2])

                    vector_explanation = collection.aggregate(
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
                    self.assertEqual(vector_explanation["engine_plan"]["details"]["backend"], "python")
                    self.assertEqual(vector_explanation["engine_plan"]["details"]["path"], "embedding")

                    with self.assertRaises(OperationFailure):
                        collection.aggregate(
                            [
                                {"$match": {"_id": {"$gt": 0}}},
                                {"$search": {"index": "by_text", "text": {"query": "ada"}}},
                            ]
                        )
                    with self.assertRaises(OperationFailure):
                        collection.aggregate([{"$vectorSearch": {"index": "vec"}}]).to_list()
                    phrase_hits = collection.aggregate(
                        [{"$search": {"index": "by_text", "phrase": {"query": "Ada wrote the first algorithm", "path": "body"}}}]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in phrase_hits], [3])
                    phrase_explanation = collection.aggregate(
                        [{"$search": {"index": "by_text", "phrase": {"query": "Ada wrote the first algorithm", "path": "body"}}}]
                    ).explain()
                    self.assertEqual(phrase_explanation["engine_plan"]["details"]["queryOperator"], "phrase")
                    if engine_name == "sqlite":
                        self.assertEqual(
                            phrase_explanation["engine_plan"]["details"]["fts5_match"],
                            '"Ada wrote the first algorithm"',
                        )
                    with self.assertRaises(OperationFailure):
                        collection.create_search_index({"mappings": {"fields": {"title": {"type": "number"}}}})

    def test_search_index_latency_can_surface_pending_state(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory(simulate_search_index_latency=0.02)) as client:
                    collection = client.search_latency.get_collection("docs")
                    collection.create_search_index(
                        SearchIndexModel(
                            {
                                "mappings": {
                                    "dynamic": False,
                                    "fields": {"title": {"type": "string"}},
                                }
                            },
                            name="by_text",
                        )
                    )
                    listed = collection.list_search_indexes("by_text").to_list()
                    self.assertEqual(listed[0]["status"], "PENDING")
                    self.assertEqual(listed[0]["statusDetail"], "pending-build")
                    self.assertIsNotNone(listed[0]["readyAtEpoch"])
                    with self.assertRaises(OperationFailure):
                        collection.aggregate(
                            [{"$search": {"index": "by_text", "text": {"query": "ada", "path": "title"}}}]
                        ).to_list()
                    time.sleep(0.03)
                    ready = collection.list_search_indexes("by_text").to_list()
                    self.assertEqual(ready[0]["status"], "READY")
                    self.assertEqual(ready[0]["statusDetail"], "ready")

    def test_watch_surfaces_client_database_and_collection_scopes(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.observe.get_collection("items")
                    other = client.observe.get_collection("other")
                    client_stream = client.watch(max_await_time_ms=100)
                    database_stream = client.observe.watch(max_await_time_ms=100)
                    collection_stream = collection.watch(
                        [{"$match": {"operationType": "insert"}}],
                        max_await_time_ms=100,
                    )

                    collection.insert_one({"_id": 1, "name": "Ada"})
                    other.insert_one({"_id": 2, "name": "Bob"})

                    collection_event = collection_stream.try_next()
                    self.assertIsNotNone(collection_event)
                    self.assertEqual(collection_event["operationType"], "insert")
                    self.assertEqual(collection_event["ns"], {"db": "observe", "coll": "items"})

                    database_event = database_stream.try_next()
                    self.assertIsNotNone(database_event)
                    self.assertEqual(database_event["ns"]["db"], "observe")

                    client_event = client_stream.try_next()
                    self.assertIsNotNone(client_event)
                    self.assertIn(client_event["ns"]["coll"], {"items", "other"})
                    second_insert = client_stream.try_next()
                    self.assertIsNotNone(second_insert)
                    self.assertEqual(second_insert["operationType"], "insert")

                    collection.update_one({"_id": 1}, {"$set": {"name": "Ada Lovelace"}})
                    collection.delete_one({"_id": 1})
                    update_event = client_stream.try_next()
                    delete_event = client_stream.try_next()
                    self.assertEqual(update_event["operationType"], "update")
                    self.assertEqual(delete_event["operationType"], "delete")

    def test_collection_json_schema_validator_rejects_invalid_inserts_and_updates(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    database = client.get_database("validation")
                    collection = database.create_collection(
                        "users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {
                                    "name": {"bsonType": "string"},
                                    "age": {"bsonType": "int"},
                                },
                            }
                        },
                    )

                    with self.assertRaises(DocumentValidationFailure) as insert_error:
                        collection.insert_one({"_id": "1", "age": 10})
                    self.assertEqual(insert_error.exception.code, 121)

                    collection.insert_one({"_id": "1", "name": "Ada", "age": 10})
                    with self.assertRaises(DocumentValidationFailure):
                        collection.update_one({"_id": "1"}, {"$set": {"age": "old"}})

    def test_collection_json_schema_warn_mode_allows_write(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.validation.create_collection(
                        "warn_users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {"name": {"bsonType": "string"}},
                            }
                        },
                        validationAction="warn",
                    )

                    collection.insert_one({"_id": "1"})
                    self.assertEqual(collection.find_one({"_id": "1"}), {"_id": "1"})

    def test_find_supports_top_level_json_schema_filter(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.validation.get_collection("query_users")
                    collection.insert_many(
                        [
                            {"_id": "1", "name": "Ada", "age": 10},
                            {"_id": "2", "name": "Bob", "age": "old"},
                            {"_id": "3", "age": 11},
                        ]
                    )

                    result = collection.find(
                        {
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {
                                    "name": {"bsonType": "string"},
                                    "age": {"bsonType": "int"},
                                },
                            }
                        }
                    ).to_list()

                    self.assertEqual(result, [{"_id": "1", "name": "Ada", "age": 10}])

    def test_find_supports_top_level_json_schema_inside_logical_clauses(self):
        schema_clause = {
            "$jsonSchema": {
                "required": ["name", "age"],
                "properties": {
                    "name": {"bsonType": "string"},
                    "age": {"bsonType": "int"},
                },
            }
        }

        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.validation.get_collection("logical_query_users")
                    collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "name": "Ada", "age": 10},
                            {"_id": "2", "tenant": "a", "name": "Bob", "age": "old"},
                            {"_id": "3", "tenant": "b", "age": 11},
                            {"_id": "4", "tenant": "c", "name": "Cora", "age": 12},
                        ]
                    )

                    and_result = collection.find(
                        {"$and": [schema_clause, {"tenant": "a"}]},
                        sort=[("_id", 1)],
                    ).to_list()
                    or_result = collection.find(
                        {"$or": [schema_clause, {"tenant": "b"}]},
                        sort=[("_id", 1)],
                    ).to_list()
                    nor_result = collection.find(
                        {"$nor": [schema_clause]},
                        sort=[("_id", 1)],
                    ).to_list()

                    self.assertEqual(and_result, [{"_id": "1", "tenant": "a", "name": "Ada", "age": 10}])
                    self.assertEqual(
                        or_result,
                        [
                            {"_id": "1", "tenant": "a", "name": "Ada", "age": 10},
                            {"_id": "3", "tenant": "b", "age": 11},
                            {"_id": "4", "tenant": "c", "name": "Cora", "age": 12},
                        ],
                    )
                    self.assertEqual(
                        nor_result,
                        [
                            {"_id": "2", "tenant": "a", "name": "Bob", "age": "old"},
                            {"_id": "3", "tenant": "b", "age": 11},
                        ],
                    )

    def test_bypass_document_validation_allows_invalid_writes(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.validation.create_collection(
                        "bypass_users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {
                                    "name": {"bsonType": "string"},
                                    "age": {"bsonType": "int"},
                                },
                            }
                        },
                    )

                    collection.insert_one({"_id": "1", "age": 10}, bypass_document_validation=True)
                    collection.update_one(
                        {"_id": "1"},
                        {"$set": {"age": "old"}},
                        bypass_document_validation=True,
                    )
                    collection.replace_one(
                        {"_id": "1"},
                        {"_id": "1", "age": "stale"},
                        bypass_document_validation=True,
                    )
                    collection.bulk_write(
                        [
                            InsertOne({"_id": "2"}),
                            UpdateOne({"_id": "2"}, {"$set": {"age": "legacy"}}),
                        ],
                        bypass_document_validation=True,
                    )

                    self.assertEqual(collection.find_one({"_id": "1"}), {"_id": "1", "age": "stale"})
                    self.assertEqual(collection.find_one({"_id": "2"}), {"_id": "2", "age": "legacy"})

    def test_collation_applies_to_query_sort_update_delete_and_distinct(self):
        collation = {"locale": "en", "strength": 2, "numericOrdering": True}
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.alpha.names
                    collection.insert_many(
                        [
                            {"_id": 1, "name": "Alice", "code": "10"},
                            {"_id": 2, "name": "alice", "code": "2"},
                            {"_id": 3, "name": "Bob", "code": "3"},
                        ]
                    )

                    found = collection.find_one({"name": "ALICE"}, collation=collation)
                    self.assertEqual(found["_id"], 1)

                    ordered = collection.find(
                        {},
                        {"_id": 0, "code": 1},
                        sort=[("code", 1)],
                        collation=collation,
                    ).to_list()
                    self.assertEqual(ordered, [{"code": "2"}, {"code": "3"}, {"code": "10"}])

                    update_result = collection.update_one(
                        {"name": "ALICE"},
                        {"$set": {"matched": True}},
                        collation=collation,
                    )
                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(collection.find_one({"_id": 1}), {"_id": 1, "name": "Alice", "code": "10", "matched": True})

                    distinct_names = collection.distinct("name", collation=collation)
                    self.assertEqual(distinct_names, ["Alice", "Bob"])

                    delete_result = collection.delete_one({"name": "bob"}, collation=collation)
                    self.assertEqual(delete_result.deleted_count, 1)
                    self.assertIsNone(collection.find_one({"_id": 3}))

    def test_collation_applies_to_aggregate_and_aggregate_command(self):
        collation = {"locale": "en", "strength": 2, "numericOrdering": True}
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.alpha.events
                    collection.insert_many(
                        [
                            {"_id": 1, "kind": "View", "rank": "10"},
                            {"_id": 2, "kind": "view", "rank": "2"},
                            {"_id": 3, "kind": "click", "rank": "3"},
                        ]
                    )

                    aggregated = collection.aggregate(
                        [
                            {"$match": {"kind": "VIEW"}},
                            {"$sort": {"rank": 1}},
                            {"$project": {"_id": 0, "rank": 1}},
                        ],
                        collation=collation,
                    ).to_list()
                    self.assertEqual(aggregated, [{"rank": "2"}, {"rank": "10"}])

                    command_result = client.alpha.command(
                        {
                            "aggregate": "events",
                            "pipeline": [
                                {"$match": {"kind": "VIEW"}},
                                {"$sort": {"rank": 1}},
                                {"$project": {"_id": 0, "rank": 1}},
                            ],
                            "collation": collation,
                            "cursor": {"batchSize": 1},
                        }
                    )
                    self.assertEqual(
                        command_result,
                        {
                            "cursor": {"id": 0, "ns": "alpha.events", "firstBatch": [{"rank": "2"}, {"rank": "10"}]},
                            "ok": 1.0,
                        },
                    )

    def test_collation_applies_to_array_update_operators_and_array_filters(self):
        collation = {"locale": "en", "strength": 2}
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.alpha.tags
                    collection.insert_one(
                        {
                            "_id": "1",
                            "tags": ["Ada", "Grace"],
                            "items": [{"kind": "Ada"}, {"kind": "Grace"}],
                        }
                    )

                    add_result = collection.update_one(
                        {"_id": "1"},
                        {"$addToSet": {"tags": "ada"}},
                        collation=collation,
                    )
                    pull_result = collection.update_one(
                        {"_id": "1"},
                        {"$pull": {"tags": "ada"}},
                        collation=collation,
                    )
                    filter_result = collection.update_one(
                        {"_id": "1"},
                        {"$set": {"items.$[item].matched": True}},
                        array_filters=[{"item.kind": "ada"}],
                        collation=collation,
                    )
                    pull_all_result = collection.update_one(
                        {"_id": "1"},
                        {"$pullAll": {"tags": ["grace"]}},
                        collation=collation,
                    )
                    updated = collection.find_one({"_id": "1"})

                    self.assertEqual(add_result.modified_count, 0)
                    self.assertEqual(pull_result.modified_count, 1)
                    self.assertEqual(filter_result.modified_count, 1)
                    self.assertEqual(pull_all_result.modified_count, 1)
                    self.assertEqual(
                        updated,
                        {
                            "_id": "1",
                            "tags": [],
                            "items": [{"kind": "Ada", "matched": True}, {"kind": "Grace"}],
                        },
                    )

    def test_database_command_supports_bypass_document_validation_and_collation(self):
        collation = {"locale": "en", "strength": 2}
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    database = client.validation
                    collection = database.create_collection(
                        "cmd_users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {"name": {"bsonType": "string"}},
                            }
                        },
                    )
                    database.command(
                        {
                            "insert": "cmd_users",
                            "documents": [{"_id": "1"}],
                            "bypassDocumentValidation": True,
                        }
                    )
                    collection.insert_many(
                        [
                            {"_id": "2", "name": "Alice"},
                            {"_id": "3", "name": "alice"},
                            {"_id": "4", "name": "Bob"},
                        ]
                    )

                    count_result = database.command(
                        {"count": "cmd_users", "query": {"name": "ALICE"}, "collation": collation}
                    )
                    self.assertEqual(count_result["n"], 2)

                    database.command(
                        {
                            "update": "cmd_users",
                            "updates": [
                                {
                                    "q": {"name": "ALICE"},
                                    "u": {"$set": {"matched": True}},
                                    "collation": collation,
                                }
                            ],
                        }
                    )
                    self.assertTrue(collection.find_one({"_id": "2"})["matched"])

                    distinct_result = database.command(
                        {
                            "distinct": "cmd_users",
                            "key": "name",
                            "query": {"name": {"$exists": True}},
                            "collation": collation,
                        }
                    )
                    self.assertEqual(distinct_result["values"], ["Alice", "Bob"])

                    database.command(
                        {
                            "delete": "cmd_users",
                            "deletes": [{"q": {"name": "bob"}, "limit": 1, "collation": collation}],
                        }
                    )
                    self.assertIsNone(collection.find_one({"_id": "4"}))

    def test_create_collection_rejects_invalid_json_schema_options(self):
        with MongoClient(MemoryEngine()) as client:
            with self.assertRaises(OperationFailure):
                client.validation.create_collection(
                    "broken",
                    validator={"$jsonSchema": {"required": "name"}},
                )

    def test_profile_command_and_system_profile_capture_operations(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    database = client.get_database("profiled")
                    profile_enabled = database.command({"profile": 2, "slowms": 0})
                    self.assertEqual(profile_enabled["was"], 0)

                    collection = database.get_collection("users")
                    collection.insert_one({"_id": "1", "name": "Ada"})
                    collection.find_one({"_id": "1"})
                    collection.update_one({"_id": "1"}, {"$set": {"active": True}})

                    profile_status = database.command({"profile": -1})
                    self.assertEqual(profile_status["was"], 2)
                    profile_entries = database.get_collection("system.profile").find().to_list()

                    self.assertTrue(any(entry["op"] == "insert" for entry in profile_entries))
                    self.assertTrue(any(entry["op"] == "query" for entry in profile_entries))
                    self.assertTrue(any(entry["op"] == "update" for entry in profile_entries))

    def test_memory_engine_transactions_use_isolated_mvcc_snapshots(self):
        with MongoClient(MemoryEngine()) as client:
            session = client.start_session()
            session.start_transaction()

            client.alpha.users.insert_one({"_id": "1", "name": "Ada"}, session=session)

            self.assertIsNone(client.alpha.users.find_one({"_id": "1"}))
            self.assertEqual(
                client.alpha.users.find_one({"_id": "1"}, session=session),
                {"_id": "1", "name": "Ada"},
            )

            session.commit_transaction()

            self.assertEqual(client.alpha.users.find_one({"_id": "1"}), {"_id": "1", "name": "Ada"})

    def test_memory_engine_transactions_detect_write_conflicts_on_commit(self):
        with MongoClient(MemoryEngine()) as client:
            first = client.start_session()
            second = client.start_session()
            first.start_transaction()
            second.start_transaction()

            client.alpha.users.insert_one({"_id": "1", "name": "Ada"}, session=first)
            client.alpha.users.insert_one({"_id": "1", "name": "Grace"}, session=second)

            first.commit_transaction()
            with self.assertRaisesRegex(OperationFailure, "Write conflict"):
                second.commit_transaction()

            self.assertEqual(client.alpha.users.find_one({"_id": "1"}), {"_id": "1", "name": "Ada"})

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

    def test_client_server_info_reflects_target_dialect(self):
        asyncio.run(assert_client_server_info_reflects_target_dialect(self, open_sync_client))

    def test_build_info_command_shares_source_of_truth_with_server_info(self):
        asyncio.run(assert_build_info_command_shares_source_of_truth_with_server_info(self, open_sync_client))

    def test_hello_and_is_master_commands_return_handshake_metadata(self):
        asyncio.run(assert_hello_and_is_master_commands_return_handshake_metadata(self, open_sync_client))

    def test_list_commands_and_connection_status_commands_return_local_admin_metadata(self):
        asyncio.run(assert_list_commands_and_connection_status_commands_return_local_admin_metadata(self, open_sync_client))

    def test_server_status_command_returns_local_runtime_metadata(self):
        asyncio.run(assert_server_status_command_returns_local_runtime_metadata(self, open_sync_client))

    def test_host_info_whats_my_uri_and_cmd_line_opts_commands_return_local_metadata(self):
        asyncio.run(assert_host_info_whats_my_uri_and_cmd_line_opts_commands_return_local_metadata(self, open_sync_client))

    def test_list_collections_command_supports_name_only(self):
        asyncio.run(assert_list_collections_command_supports_name_only(self, SYNC_ENGINE_FACTORIES, open_sync_client))

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

    def test_find_supports_implicit_regex_literals_and_in_regex(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    client.alpha.users.insert_many(
                        [
                            {"_id": "1", "name": "MongoDB", "tags": ["beta", "stable"]},
                            {"_id": "2", "name": "Postgres", "tags": ["alpha", "stable"]},
                        ]
                    )

                    implicit = client.alpha.users.find(
                        {"name": re.compile("^mongo", re.IGNORECASE)},
                        sort=[("_id", 1)],
                    ).to_list()
                    eq_regex = client.alpha.users.find(
                        {"name": {"$eq": re.compile("^mongo", re.IGNORECASE)}},
                        sort=[("_id", 1)],
                    ).to_list()
                    in_regex = client.alpha.users.find(
                        {"tags": {"$in": [re.compile("^be"), re.compile("^zz")]}},
                        sort=[("_id", 1)],
                    ).to_list()

                    self.assertEqual([document["_id"] for document in implicit], ["1"])
                    self.assertEqual([document["_id"] for document in eq_regex], ["1"])
                    self.assertEqual([document["_id"] for document in in_regex], ["1"])

    def test_create_collection_registers_empty_namespace_and_rejects_duplicates(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    created = client.alpha.create_collection("events")

                    self.assertEqual(created.name, "events")
                    self.assertEqual(client.alpha.list_collection_names(), ["events"])
                    self.assertIn("alpha", client.list_database_names())

                    with self.assertRaises(CollectionInvalid):
                        client.alpha.create_collection("events")

    def test_list_collections_returns_cursor_documents_and_supports_filter(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    client.alpha.create_collection("events")
                    client.alpha.create_collection("logs")

                    cursor = client.alpha.list_collections({"name": "events"})

                    self.assertEqual(
                        cursor.to_list(),
                        [
                            {
                                "name": "events",
                                "type": "collection",
                                "options": {},
                                "info": {"readOnly": False},
                            }
                        ],
                    )
                    self.assertEqual(
                        client.alpha.list_collection_names({"name": "logs"}),
                        ["logs"],
                    )

    def test_collection_options_round_trip_through_admin_metadata(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.alpha.create_collection(
                        "events",
                        capped=True,
                        size=1024,
                    )

                    self.assertEqual(
                        collection.options(),
                        {"capped": True, "size": 1024},
                    )
                    self.assertEqual(
                        client.alpha.list_collections({"name": "events"}).to_list(),
                        [
                            {
                                "name": "events",
                                "type": "collection",
                                "options": {"capped": True, "size": 1024},
                                "info": {"readOnly": False},
                            }
                        ],
                    )

    def test_list_collections_rejects_invalid_filter(self):
        with MongoClient(MemoryEngine()) as client:
            with self.assertRaises(TypeError):
                client.alpha.list_collections(["bad"])  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                client.alpha.list_collection_names(["bad"])  # type: ignore[arg-type]

    def test_database_admin_methods_respect_session_bound_sqlite_transactions(self):
        with MongoClient(SQLiteEngine()) as client:
            session = client.start_session()
            session.start_transaction()

            client.alpha.create_collection("events", session=session)

            self.assertEqual(client.list_database_names(session=session), ["alpha"])
            self.assertEqual(client.alpha.list_collection_names(session=session), ["events"])

            session.abort_transaction()

            self.assertEqual(client.list_database_names(), [])
            self.assertEqual(client.alpha.list_collection_names(), [])

    def test_database_command_supports_ping_list_collections_and_drop_database(self):
        asyncio.run(assert_database_command_supports_ping_list_collections_and_drop_database(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_supports_collection_index_count_and_distinct_commands(self):
        asyncio.run(assert_database_command_supports_collection_index_count_and_distinct_commands(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_index_commands_support_comment_and_max_time(self):
        asyncio.run(assert_database_command_index_commands_support_comment_and_max_time(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_count_supports_skip_limit_hint_and_comment(self):
        asyncio.run(assert_database_command_count_supports_skip_limit_hint_and_comment(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_distinct_supports_hint_comment_and_max_time(self):
        asyncio.run(assert_database_command_distinct_supports_hint_comment_and_max_time(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_supports_rename_collection_within_current_database(self):
        asyncio.run(assert_database_command_supports_rename_collection_within_current_database(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_supports_find_and_aggregate(self):
        asyncio.run(assert_database_command_supports_find_and_aggregate(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_supports_explain_for_find_and_aggregate(self):
        asyncio.run(assert_database_command_supports_explain_for_find_and_aggregate(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_supports_explain_for_update_and_delete(self):
        asyncio.run(assert_database_command_supports_explain_for_update_and_delete(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_supports_insert_update_and_delete(self):
        asyncio.run(assert_database_command_supports_insert_update_and_delete(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_write_commands_surface_write_errors(self):
        asyncio.run(assert_database_command_write_commands_surface_write_errors(self, open_sync_client))

    def test_database_command_supports_find_and_modify(self):
        asyncio.run(assert_database_command_supports_find_and_modify(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_supports_coll_stats_and_db_stats(self):
        asyncio.run(assert_database_command_supports_coll_stats_and_db_stats(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_database_command_rejects_unsupported_commands(self):
        asyncio.run(assert_database_command_rejects_unsupported_commands(self, open_sync_client))

    def test_database_command_rejects_invalid_command_shapes(self):
        asyncio.run(assert_database_command_rejects_invalid_command_shapes(self, open_sync_client))

    def test_validate_collection_returns_metadata_and_rejects_missing_namespace(self):
        asyncio.run(assert_validate_collection_returns_metadata_and_rejects_missing_namespace(self, SYNC_ENGINE_FACTORIES, open_sync_client))

    def test_collection_rename_moves_documents_and_indexes(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.alpha.events
                    collection.insert_one({"_id": "1", "kind": "view"})
                    collection.create_index([("kind", 1)], name="kind_idx")

                    renamed = collection.rename("archived")

                    self.assertEqual(renamed.name, "archived")
                    self.assertEqual(client.alpha.list_collection_names(), ["archived"])
                    self.assertEqual(renamed.find_one({"_id": "1"}), {"_id": "1", "kind": "view"})
                    self.assertIn("kind_idx", renamed.index_information())
                    self.assertEqual(renamed.options(), {})

    def test_collection_rename_preserves_options_metadata(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.alpha.create_collection(
                        "events",
                        capped=True,
                        size=512,
                    )

                    renamed = collection.rename("archived")

                    self.assertEqual(
                        renamed.options(),
                        {"capped": True, "size": 512},
                    )

    def test_collection_rename_rejects_conflicting_or_identical_names(self):
        with MongoClient(MemoryEngine()) as client:
            client.alpha.events.insert_one({"_id": "1"})
            client.alpha.logs.insert_one({"_id": "2"})

            with self.assertRaises(CollectionInvalid):
                client.alpha.events.rename("logs")
            with self.assertRaises(CollectionInvalid):
                client.alpha.events.rename("events")

    def test_client_database_and_collection_expose_configured_pymongo_options(self):
        write_concern = WriteConcern("majority", j=True)
        read_concern = ReadConcern("majority")
        read_preference = ReadPreference(ReadPreferenceMode.SECONDARY)
        codec_options = CodecOptions(dict, tz_aware=True)
        transaction_options = TransactionOptions(
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
            max_commit_time_ms=250,
        )

        with MongoClient(
            MemoryEngine(),
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
            codec_options=codec_options,
            transaction_options=transaction_options,
        ) as client:
            database = client.get_database("alpha")
            collection = database.get_collection("events")

            self.assertIs(client.write_concern, write_concern)
            self.assertIs(client.read_concern, read_concern)
            self.assertIs(client.read_preference, read_preference)
            self.assertIs(client.codec_options, codec_options)
            self.assertIs(client.transaction_options, transaction_options)
            self.assertIs(database.write_concern, write_concern)
            self.assertIs(database.read_concern, read_concern)
            self.assertIs(database.read_preference, read_preference)
            self.assertIs(database.codec_options, codec_options)
            self.assertIs(collection.write_concern, write_concern)
            self.assertIs(collection.read_concern, read_concern)
            self.assertIs(collection.read_preference, read_preference)
            self.assertIs(collection.codec_options, codec_options)

    def test_with_options_clones_database_and_collection_without_mutating_parent(self):
        with MongoClient(MemoryEngine()) as client:
            base_database = client.get_database("alpha")
            tuned_database = base_database.with_options(
                write_concern=WriteConcern(1),
                read_concern=ReadConcern("local"),
            )
            base_collection = tuned_database.get_collection("events")
            tuned_collection = base_collection.with_options(
                read_preference=ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
                codec_options=CodecOptions(dict, tz_aware=True),
            )

            self.assertEqual(base_database.write_concern, WriteConcern())
            self.assertEqual(base_database.read_concern, ReadConcern())
            self.assertEqual(tuned_database.write_concern, WriteConcern(1))
            self.assertEqual(tuned_database.read_concern, ReadConcern("local"))
            self.assertEqual(base_collection.read_preference, ReadPreference())
            self.assertEqual(tuned_collection.read_preference, ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED))
            self.assertEqual(tuned_collection.codec_options, CodecOptions(dict, tz_aware=True))

    def test_client_with_options_clones_client_without_mutating_parent(self):
        with MongoClient(MemoryEngine()) as client:
            tuned = client.with_options(
                write_concern=WriteConcern(1),
                read_concern=ReadConcern("local"),
            )

            self.assertEqual(client.write_concern, WriteConcern())
            self.assertEqual(client.read_concern, ReadConcern())
            self.assertEqual(tuned.write_concern, WriteConcern(1))
            self.assertEqual(tuned.read_concern, ReadConcern("local"))

    def test_get_default_database_uses_uri_name_or_explicit_default_and_preserves_options(self):
        read_preference = ReadPreference(ReadPreferenceMode.SECONDARY)
        codec_options = CodecOptions(dict, tz_aware=True)

        with MongoClient(
            MemoryEngine(),
            uri="mongodb://localhost/observe",
            read_preference=read_preference,
            codec_options=codec_options,
        ) as client:
            from_uri = client.get_default_database()
            overridden = client.get_default_database(
                "ignored",
                write_concern=WriteConcern(1),
            )

            self.assertEqual(from_uri._name, "observe")
            self.assertEqual(overridden._name, "observe")
            self.assertIs(from_uri.read_preference, read_preference)
            self.assertIs(from_uri.codec_options, codec_options)
            self.assertEqual(overridden.write_concern, WriteConcern(1))

        with MongoClient(MemoryEngine(), uri="mongodb://localhost") as client:
            explicit = client.get_default_database("fallback")
            self.assertEqual(explicit._name, "fallback")
            with self.assertRaises(InvalidOperation):
                client.get_default_database()

    def test_client_context_manager_and_server_info_are_stable(self):
        client = MongoClient(MemoryEngine())
        with client as managed:
            self.assertIs(managed, client)
            first = client.server_info()
            second = client.server_info()

            self.assertTrue(first["version"].startswith(f"{client.mongodb_dialect.server_version}."))
            self.assertTrue(second["version"].startswith(f"{client.mongodb_dialect.server_version}."))
            self.assertEqual(first["versionArray"], second["versionArray"])

    def test_collection_namespace_helpers_and_cursor_metadata(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.alpha.get_collection(
                        "events",
                        read_concern=ReadConcern("majority"),
                    )
                    child = collection.daily
                    child_underscore = collection["daily_logs"]
                    sibling = collection.database.get_collection(
                        "logs",
                        read_concern=ReadConcern("local"),
                    )

                    self.assertEqual(collection.name, "events")
                    self.assertEqual(collection.full_name, "alpha.events")
                    self.assertEqual(collection.database.name, "alpha")
                    self.assertEqual(child.name, "events.daily")
                    self.assertEqual(child.full_name, "alpha.events.daily")
                    self.assertEqual(child_underscore.name, "events.daily_logs")
                    self.assertEqual(child_underscore.full_name, "alpha.events.daily_logs")
                    self.assertEqual(sibling.name, "logs")
                    self.assertEqual(sibling.full_name, "alpha.logs")
                    self.assertEqual(sibling.read_concern, ReadConcern("local"))
                    self.assertEqual(collection.read_concern, ReadConcern("majority"))

                    collection.insert_many(
                        [
                            {"_id": "1", "kind": "view"},
                            {"_id": "2", "kind": "click"},
                        ]
                    )
                    collection.create_index([("kind", 1)], name="kind_idx")

                    cursor = collection.find({"kind": "view"}).hint("kind_idx")
                    clone = cursor.clone().limit(1)

                    self.assertEqual(cursor.collection.full_name, "alpha.events")
                    self.assertEqual(clone.collection.full_name, "alpha.events")
                    self.assertTrue(cursor.alive)
                    self.assertEqual(clone.to_list(), [{"_id": "1", "kind": "view"}])
                    self.assertEqual(cursor.to_list(), [{"_id": "1", "kind": "view"}])

    def test_start_session_inherits_default_transaction_options_from_client(self):
        transaction_options = TransactionOptions(
            write_concern=WriteConcern("majority"),
            max_commit_time_ms=200,
        )

        with MongoClient(
            MemoryEngine(),
            transaction_options=transaction_options,
        ) as client:
            session = client.start_session()
            session.start_transaction()

            self.assertEqual(session.default_transaction_options, transaction_options)
            self.assertEqual(session.transaction_options, transaction_options)

    def test_duplicate_id_raises(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "same", "name": "Ada"})

                    with self.assertRaises(DuplicateKeyError):
                        collection.insert_one({"_id": "same", "name": "Grace"})

    def test_insert_many_update_many_and_delete_many(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    inserted = collection.insert_many(
                        [{"_id": "1", "kind": "view", "done": False}, {"_id": "2", "kind": "view", "done": False}]
                    )
                    collection.insert_one({"_id": "3", "kind": "click", "done": False})

                    updated = collection.update_many({"kind": "view"}, {"$set": {"done": True}})
                    deleted = collection.delete_many({"kind": "view"})
                    remaining = collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(inserted.inserted_ids, ["1", "2"])
                    self.assertEqual(updated.matched_count, 2)
                    self.assertEqual(updated.modified_count, 2)
                    self.assertEqual(deleted.deleted_count, 2)
                    self.assertEqual(remaining, [{"_id": "3", "kind": "click", "done": False}])

    def test_delete_last_document_keeps_collection_visible_until_drop(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    client.alpha.events.insert_one({"_id": "1"})
                    client.alpha.events.delete_one({"_id": "1"})

                    self.assertEqual(client.alpha.list_collection_names(), ["events"])

    def test_bulk_write_supports_ordered_and_unordered_execution(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory(), pymongo_profile="4.11") as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "seed", "kind": "view", "rank": 2, "done": False})
                    collection.insert_one({"_id": "other", "kind": "view", "rank": 1, "done": False})

                    success = collection.bulk_write(
                        [
                            InsertOne({"_id": "new", "kind": "click"}),
                            UpdateOne({"kind": "view"}, {"$set": {"done": True}}, sort=[("rank", 1)]),
                            UpdateMany({"kind": "view"}, {"$set": {"tag": "seen"}}),
                            ReplaceOne({"_id": "new"}, {"kind": "click", "done": True}),
                            DeleteOne({"_id": "seed"}),
                            DeleteMany({"kind": "view"}),
                        ]
                    )

                    self.assertEqual(success.inserted_count, 1)
                    self.assertEqual(success.matched_count, 4)
                    self.assertEqual(success.modified_count, 4)
                    self.assertEqual(success.deleted_count, 2)
                    self.assertEqual(success.upserted_count, 0)
                    self.assertEqual(
                        collection.find({}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "new", "kind": "click", "done": True}],
                    )

                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "dup", "done": False})

                    with self.assertRaises(BulkWriteError) as ctx:
                        collection.bulk_write(
                            [
                                InsertOne({"_id": "dup"}),
                                UpdateOne({"_id": "dup"}, {"$set": {"done": True}}),
                                DeleteOne({"_id": "dup"}),
                            ],
                            ordered=False,
                        )

                    self.assertEqual(ctx.exception.details["writeErrors"][0]["index"], 0)
                    self.assertEqual(ctx.exception.details["nModified"], 1)
                    self.assertEqual(ctx.exception.details["nRemoved"], 1)
                    self.assertEqual(collection.find({}).to_list(), [])

    def test_replace_one_and_find_one_and_family(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory(), pymongo_profile='4.11') as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "kind": "view", "rank": 2, "done": False})
                    collection.insert_one({"_id": "2", "kind": "view", "rank": 1, "done": False})

                    replaced = collection.replace_one(
                        {"kind": "view"},
                        {"kind": "view", "rank": 1, "done": True, "tag": "replaced"},
                        sort=[("rank", 1)],
                    )
                    before = collection.find_one_and_update(
                        {"kind": "view"},
                        {"$set": {"done": False}},
                        sort=[("rank", 1)],
                        projection={"done": 1, "_id": 0},
                    )
                    after = collection.find_one_and_replace(
                        {"kind": "view"},
                        {"kind": "view", "rank": 1, "done": True, "tag": "after"},
                        sort=[("rank", 1)],
                        return_document=ReturnDocument.AFTER,
                        projection={"done": 1, "tag": 1, "_id": 0},
                    )
                    deleted = collection.find_one_and_delete(
                        {"kind": "view"},
                        sort=[("rank", -1)],
                        projection={"rank": 1, "_id": 0},
                    )
                    remaining = collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(replaced.matched_count, 1)
                    self.assertEqual(replaced.modified_count, 1)
                    self.assertEqual(before, {"done": True})
                    self.assertEqual(after, {"done": True, "tag": "after"})
                    self.assertEqual(deleted, {"rank": 2})
                    self.assertEqual(
                        remaining,
                        [{"_id": "2", "kind": "view", "rank": 1, "done": True, "tag": "after"}],
                    )

    def test_find_one_and_update_upsert_returns_none_or_after_document(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    before = collection.find_one_and_update(
                        {"kind": "missing", "tenant": "a"},
                        {"$set": {"done": True}},
                        upsert=True,
                    )
                    after = collection.find_one_and_update(
                        {"kind": "another", "tenant": "b"},
                        {"$set": {"done": True}},
                        upsert=True,
                        return_document=ReturnDocument.AFTER,
                        projection={"kind": 1, "tenant": 1, "done": 1, "_id": 0},
                    )

                    self.assertIsNone(before)
                    self.assertEqual(after, {"kind": "another", "tenant": "b", "done": True})

    def test_update_operators_min_max_mul_and_rename_are_observable_via_api(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_many(
                        [
                            {"_id": "1", "score": 10, "rank": 2, "profile": {"name": "Ada"}},
                            {"_id": "2", "score": 4, "rank": 1},
                        ]
                    )

                    min_result = collection.update_one({"_id": "1"}, {"$min": {"score": 7}})
                    max_result = collection.update_one({"_id": "2"}, {"$max": {"score": 8}})
                    mul_result = collection.update_many({}, {"$mul": {"score": 2}})
                    renamed = collection.find_one_and_update(
                        {"_id": "1"},
                        {"$rename": {"profile.name": "profile.alias"}},
                        return_document=ReturnDocument.AFTER,
                    )
                    bulk = collection.bulk_write(
                        [
                            UpdateOne({"_id": "1"}, {"$max": {"rank": 5}}),
                            UpdateOne({"_id": "2"}, {"$rename": {"score": "points"}}),
                        ]
                    )
                    first = collection.find_one({"_id": "1"})
                    second = collection.find_one({"_id": "2"})

                    self.assertEqual(min_result.modified_count, 1)
                    self.assertEqual(max_result.modified_count, 1)
                    self.assertEqual(mul_result.modified_count, 2)
                    self.assertEqual(renamed["profile"], {"alias": "Ada"})
                    self.assertEqual(bulk.modified_count, 2)
                    self.assertEqual(first["score"], 14)
                    self.assertEqual(first["rank"], 5)
                    self.assertEqual(second, {"_id": "2", "rank": 1, "points": 16})

    def test_current_date_and_set_on_insert_are_observable_via_api(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "name": "Ada"})

                    updated = collection.update_one(
                        {"_id": "1"},
                        {"$currentDate": {"updated_at": True}},
                    )
                    inserted = collection.update_one(
                        {"_id": "2"},
                        {
                            "$set": {"name": "Grace"},
                            "$setOnInsert": {"created_at": "seeded"},
                        },
                        upsert=True,
                    )
                    existing = collection.update_one(
                        {"_id": "1"},
                        {"$setOnInsert": {"created_at": "ignored"}},
                    )
                    first = collection.find_one({"_id": "1"})
                    second = collection.find_one({"_id": "2"})

                    self.assertEqual(updated.modified_count, 1)
                    self.assertEqual(inserted.upserted_id, "2")
                    self.assertEqual(existing.modified_count, 0)
                    self.assertIsInstance(first["updated_at"], datetime.datetime)
                    self.assertNotIn("created_at", first)
                    self.assertEqual(second["created_at"], "seeded")

    def test_array_filters_and_all_positional_updates_are_observable_via_api(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "items": [{"qty": 1}, {"qty": 3}]})

                    update_one_result = collection.update_one(
                        {"_id": "1"},
                        {"$inc": {"items.$[high].qty": 2}},
                        array_filters=[{"high.qty": {"$gte": 2}}],
                    )
                    update_many_result = collection.update_many(
                        {"_id": "1"},
                        {"$set": {"items.$[].flag": True}},
                    )
                    bulk_result = collection.bulk_write(
                        [
                            UpdateOne(
                                {"_id": "1"},
                                {"$max": {"items.$[high].qty": 10}},
                                array_filters=[{"high.qty": {"$gte": 5}}],
                            )
                        ]
                    )
                    updated = collection.find_one_and_update(
                        {"_id": "1"},
                        {"$min": {"items.$[flagged].qty": 8}},
                        array_filters=[{"flagged.flag": True}],
                        return_document=ReturnDocument.AFTER,
                    )

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(update_many_result.modified_count, 1)
                    self.assertEqual(bulk_result.modified_count, 1)
                    self.assertEqual(
                        updated["items"],
                        [{"qty": 1, "flag": True}, {"qty": 8, "flag": True}],
                    )

    def test_legacy_positional_operator_is_observable_via_api(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "items": [{"qty": 1}, {"qty": 3}, {"qty": 4}]})

                    result = collection.update_one(
                        {"items.qty": {"$gte": 3}},
                        {"$inc": {"items.$.qty": 2}},
                    )
                    updated = collection.find_one({"_id": "1"})

                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(updated["items"], [{"qty": 1}, {"qty": 5}, {"qty": 4}])

    def test_bit_update_operator_is_observable_via_api(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "score": 13, "flags": [1, 3, 4]})

                    update_one_result = collection.update_one(
                        {"_id": "1"},
                        {"$bit": {"score": {"and": 10}}},
                    )
                    update_many_result = collection.update_many(
                        {"_id": "1"},
                        {"$bit": {"flags.$[flag]": {"xor": 2}}},
                        array_filters=[{"flag": {"$gte": 3}}],
                    )
                    updated = collection.find_one({"_id": "1"})

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(update_many_result.modified_count, 1)
                    self.assertEqual(updated, {"_id": "1", "score": 8, "flags": [1, 1, 6]})

    def test_pull_all_update_operator_is_observable_via_api(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "tags": ["python", "async", "python"], "nums": [1, 2, 3, 2]})

                    update_one_result = collection.update_one(
                        {"_id": "1"},
                        {"$pullAll": {"tags": ["python"], "nums": [2, 4]}},
                    )
                    updated = collection.find_one({"_id": "1"})

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(updated, {"_id": "1", "tags": ["async"], "nums": [1, 3]})

    def test_array_mutation_operators_support_positional_paths_via_api(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one(
                        {
                            "_id": "1",
                            "groups": [
                                {"name": "alpha", "tags": ["a"], "scores": [1, 2]},
                                {"name": "beta", "tags": ["b"], "scores": [2, 3]},
                            ],
                        }
                    )

                    push_result = collection.update_one(
                        {"_id": "1"},
                        {"$push": {"groups.$[group].tags": {"$each": ["x"], "$position": 0}}},
                        array_filters=[{"group.name": "beta"}],
                    )
                    add_to_set_result = collection.update_one(
                        {"_id": "1"},
                        {"$addToSet": {"groups.$[].tags": "common"}},
                    )
                    pull_result = collection.update_one(
                        {"_id": "1"},
                        {"$pull": {"groups.$[group].tags": "x"}},
                        array_filters=[{"group.name": "beta"}],
                    )
                    pull_all_result = collection.update_one(
                        {"_id": "1"},
                        {"$pullAll": {"groups.$[].scores": [2]}},
                    )
                    pop_result = collection.update_one(
                        {"groups.name": "beta"},
                        {"$pop": {"groups.$.scores": 1}},
                    )
                    updated = collection.find_one({"_id": "1"})

                    self.assertEqual(push_result.modified_count, 1)
                    self.assertEqual(add_to_set_result.modified_count, 1)
                    self.assertEqual(pull_result.modified_count, 1)
                    self.assertEqual(pull_all_result.modified_count, 1)
                    self.assertEqual(pop_result.modified_count, 1)
                    self.assertEqual(
                        updated["groups"],
                        [
                            {"name": "alpha", "tags": ["a", "common"], "scores": [1]},
                            {"name": "beta", "tags": ["b", "common"], "scores": []},
                        ],
                    )

    def test_distinct_supports_scalars_arrays_nested_paths_and_filter(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one(
                        {"_id": "1", "kind": "view", "tags": ["a", "b"], "profile": {"city": "Madrid"}}
                    )
                    collection.insert_one(
                        {"_id": "2", "kind": "view", "tags": ["b", "c"], "profile": {"city": "Sevilla"}}
                    )
                    collection.insert_one(
                        {"_id": "3", "kind": "click", "tags": [], "profile": {"city": "Madrid"}}
                    )

                    tags = collection.distinct("tags")
                    cities = collection.distinct("profile.city", {"kind": "view"})

                    self.assertEqual(tags, ["a", "b", "c"])
                    self.assertEqual(cities, ["Madrid", "Sevilla"])

    def test_distinct_supports_document_values_and_arrays_of_documents(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_many(
                        [
                            {
                                "_id": "1",
                                "profile": {"city": "Madrid"},
                                "items": [{"code": "a"}, {"code": "b"}],
                                "nested": {"items": [{"code": "a"}, {"code": "b"}]},
                            },
                            {
                                "_id": "2",
                                "profile": {"city": "Sevilla"},
                                "items": [{"code": "b"}, {"code": "c"}],
                                "nested": {"items": [{"code": "b"}, {"code": "c"}]},
                            },
                        ]
                    )

                    profiles = collection.distinct("profile")
                    item_documents = collection.distinct("items")
                    nested_codes = collection.distinct("nested.items.code")

                    self.assertEqual(profiles, [{"city": "Madrid"}, {"city": "Sevilla"}])
                    self.assertEqual(
                        item_documents,
                        [{"code": "a"}, {"code": "b"}, {"code": "c"}],
                    )
                    self.assertEqual(nested_codes, ["a", "b", "c"])

    def test_distinct_supports_nested_scalar_arrays(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_many(
                        [
                            {"_id": "1", "matrix": [[1, 2, 3], [3, 4]]},
                            {"_id": "2", "matrix": [[1, 2, 3], [5, 6]]},
                        ]
                    )

                    values = collection.distinct("matrix")

                    self.assertEqual(values, [[1, 2, 3], [3, 4], [5, 6]])

    def test_distinct_supports_hint_comment_and_max_time(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    collection.insert_many(
                        [
                            {"_id": "1", "kind": "view", "tag": "python"},
                            {"_id": "2", "kind": "view", "tag": "mongodb"},
                            {"_id": "3", "kind": "click", "tag": "python"},
                        ],
                        session=session,
                    )
                    collection.create_index([("kind", 1)], name="kind_idx", session=session)

                    values = collection.distinct(
                        "tag",
                        {"kind": "view"},
                        hint="kind_idx",
                        comment="trace-distinct",
                        max_time_ms=25,
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(values, ["python", "mongodb"])
                    self.assertEqual(state["last_operation"]["comment"], "trace-distinct")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)

    def test_find_supports_type_query_operator(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_many(
                        [
                            {"_id": "1", "value": 7},
                            {"_id": "2", "value": "7"},
                            {"_id": "3", "value": [1, "x"]},
                        ]
                    )

                    numbers = [doc["_id"] for doc in collection.find({"value": {"$type": "number"}})]
                    arrays = [doc["_id"] for doc in collection.find({"value": {"$type": "array"}})]
                    strings_in_arrays = [doc["_id"] for doc in collection.find({"value": {"$type": "string"}})]

                    self.assertEqual(numbers, ["1", "3"])
                    self.assertEqual(arrays, ["3"])
                    self.assertEqual(strings_in_arrays, ["2", "3"])

    def test_find_supports_bitwise_query_operators(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_many(
                        [
                            {"_id": "1", "mask": 0b1010},
                            {"_id": "2", "mask": 0b0101},
                            {"_id": "3", "mask": bytes([0b00000101])},
                        ]
                    )

                    all_set = [doc["_id"] for doc in collection.find({"mask": {"$bitsAllSet": 0b1000}})]
                    any_clear = [doc["_id"] for doc in collection.find({"mask": {"$bitsAnyClear": [1, 3]}})]
                    binary = [doc["_id"] for doc in collection.find({"mask": {"$bitsAllSet": bytes([0b00000101])}})]

                    self.assertEqual(all_set, ["1"])
                    self.assertEqual(any_clear, ["2", "3"])
                    self.assertEqual(binary, ["2", "3"])

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

    def test_estimated_document_count_and_drop_collection_and_database(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    users = client.alpha.users
                    logs = client.alpha.logs
                    users.insert_one({"_id": "1", "name": "Ada"})
                    logs.insert_one({"_id": "1", "kind": "view"})

                    self.assertEqual(users.estimated_document_count(), 1)
                    client.alpha.drop_collection("logs")
                    self.assertEqual(client.alpha.list_collection_names(), ["users"])

                    users.drop()
                    self.assertEqual(client.alpha.list_collection_names(), [])

                    client.drop_database("alpha")
                    self.assertNotIn("alpha", client.list_database_names())

    def test_estimated_document_count_supports_comment_and_max_time(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    collection.insert_many(
                        [
                            {"_id": "1", "kind": "view"},
                            {"_id": "2", "kind": "click"},
                        ],
                        session=session,
                    )

                    count = collection.estimated_document_count(
                        comment="trace-estimated-count",
                        max_time_ms=25,
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(count, 2)
                    self.assertEqual(state["last_operation"]["comment"], "trace-estimated-count")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)

    def test_drop_operations_on_missing_targets_are_noops(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    client.alpha.drop_collection("missing")
                    client.alpha.users.drop()
                    client.drop_database("missing")
                    self.assertEqual(client.list_database_names(), [])

    def test_drop_database_removes_sqlite_database_with_only_index_metadata(self):
        with MongoClient(SQLiteEngine()) as client:
            client.alpha.users.create_index(["email"], unique=False)

            self.assertIn("alpha", client.list_database_names())
            self.assertEqual(client.alpha.list_collection_names(), ["users"])

            client.drop_database("alpha")

            self.assertNotIn("alpha", client.list_database_names())
            self.assertEqual(client.alpha.list_collection_names(), [])

    def test_drop_database_removes_memory_database_with_only_index_metadata(self):
        with MongoClient(MemoryEngine()) as client:
            client.alpha.users.create_index(["email"], unique=False)

            self.assertIn("alpha", client.list_database_names())
            self.assertEqual(client.alpha.list_collection_names(), ["users"])

            client.drop_database("alpha")

            self.assertNotIn("alpha", client.list_database_names())
            self.assertEqual(client.alpha.list_collection_names(), [])

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

    def test_find_and_aggregate_raw_batches_return_bson_batches(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "rank": 3})
                    collection.insert_one({"_id": "2", "rank": 1})
                    collection.insert_one({"_id": "3", "rank": 2})

                    find_batches = collection.find_raw_batches(
                        {},
                        sort=[("rank", 1)],
                        batch_size=2,
                    ).to_list()
                    aggregate_batches = collection.aggregate_raw_batches(
                        [{"$sort": {"rank": 1}}, {"$project": {"_id": 1, "rank": 1}}],
                        batch_size=2,
                    ).to_list()

                    self.assertEqual(len(find_batches), 2)
                    self.assertEqual(decode_all(find_batches[0]), [{"_id": "2", "rank": 1}, {"_id": "3", "rank": 2}])
                    self.assertEqual(decode_all(find_batches[1]), [{"_id": "1", "rank": 3}])
                    self.assertEqual(len(aggregate_batches), 2)
                    self.assertEqual(decode_all(aggregate_batches[0]), [{"_id": "2", "rank": 1}, {"_id": "3", "rank": 2}])
                    self.assertEqual(decode_all(aggregate_batches[1]), [{"_id": "1", "rank": 3}])

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

    def test_aggregate_supports_is_number_and_type_expressions(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "value": 7, "text": "Ada"})
                    collection.insert_one({"_id": "2", "value": "7"})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "value_type": {"$type": "$value"},
                                    "value_is_number": {"$isNumber": "$value"},
                                    "missing_type": {"$type": "$missing"},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "value_type": "int", "value_is_number": True, "missing_type": "missing"},
                            {"_id": "2", "value_type": "string", "value_is_number": False, "missing_type": "missing"},
                        ],
                    )

    def test_aggregate_supports_scalar_coercion_expressions(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "int_text": "42", "float_text": "3.5", "truthy_text": "false", "zero": 0})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "as_int": {"$toInt": "$int_text"},
                                    "as_double": {"$toDouble": "$float_text"},
                                    "as_bool": {"$toBool": "$truthy_text"},
                                    "zero_bool": {"$toBool": "$zero"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "as_int": 42, "as_double": 3.5, "as_bool": True, "zero_bool": False}],
                    )

    def test_aggregate_supports_date_math_expressions(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one(
                        {
                            "_id": "1",
                            "start": datetime.datetime(2026, 3, 25, 10, 0, 0),
                            "end": datetime.datetime(2026, 3, 27, 9, 0, 0),
                        }
                    )

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "added": {"$dateAdd": {"startDate": "$start", "unit": "day", "amount": 2}},
                                    "subtracted": {"$dateSubtract": {"startDate": "$start", "unit": "hour", "amount": 3}},
                                    "diff_days": {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "day"}},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "added": datetime.datetime(2026, 3, 27, 10, 0, 0),
                                "subtracted": datetime.datetime(2026, 3, 25, 7, 0, 0),
                                "diff_days": 2,
                            }
                        ],
                    )

    def test_aggregate_supports_substr_and_strlen_variants(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "text": "é寿司A"})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "byte_substr": {"$substrBytes": ["$text", 0, 2]},
                                    "cp_substr": {"$substrCP": ["$text", 1, 2]},
                                    "byte_len": {"$strLenBytes": "$text"},
                                    "cp_len": {"$strLenCP": "$text"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "byte_substr": "é", "cp_substr": "寿司", "byte_len": 9, "cp_len": 4}],
                    )

    def test_aggregate_supports_string_index_and_binary_size_variants(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "text": "é寿司A", "blob": b"abcd"})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "byte_index": {"$indexOfBytes": ["$text", "寿", 0, 9]},
                                    "cp_index": {"$indexOfCP": ["$text", "司A", 0, 4]},
                                    "blob_size": {"$binarySize": "$blob"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "byte_index": 2, "cp_index": 2, "blob_size": 4}],
                    )

    def test_aggregate_supports_regex_match_find_and_find_all(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "text": "Ada and ada"})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "matched": {"$regexMatch": {"input": "$text", "regex": "^ada", "options": "i"}},
                                    "found": {"$regexFind": {"input": "$text", "regex": "(a)(d)a", "options": "i"}},
                                    "found_all": {"$regexFindAll": {"input": "$text", "regex": "a", "options": "i"}},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "matched": True,
                                "found": {"match": "Ada", "idx": 0, "captures": ["A", "d"]},
                                "found_all": [
                                    {"match": "A", "idx": 0, "captures": []},
                                    {"match": "a", "idx": 2, "captures": []},
                                    {"match": "a", "idx": 4, "captures": []},
                                    {"match": "a", "idx": 8, "captures": []},
                                    {"match": "a", "idx": 10, "captures": []},
                                ],
                            }
                        ],
                    )

    def test_aggregate_supports_numeric_math_variants(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "value": 19.25, "base": 100})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "absolute": {"$abs": -4},
                                    "natural_log": {"$ln": {"$exp": 1}},
                                    "base_log": {"$log": ["$base", 10]},
                                    "base10_log": {"$log10": "$base"},
                                    "power": {"$pow": [2, 3]},
                                    "rounded": {"$round": ["$value", 1]},
                                    "truncated": {"$trunc": ["$value", -1]},
                                    "root": {"$sqrt": 25},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(documents[0]["_id"], "1")
                    self.assertEqual(documents[0]["absolute"], 4)
                    self.assertAlmostEqual(documents[0]["natural_log"], 1.0)
                    self.assertAlmostEqual(documents[0]["base_log"], 2.0)
                    self.assertAlmostEqual(documents[0]["base10_log"], 2.0)
                    self.assertEqual(documents[0]["power"], 8.0)
                    self.assertEqual(documents[0]["rounded"], 19.2)
                    self.assertEqual(documents[0]["truncated"], 10)
                    self.assertEqual(documents[0]["root"], 5.0)

    def test_aggregate_supports_week_variants(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "created_at": datetime.datetime(2026, 1, 1, 23, 30, 0)})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "week": {"$week": "$created_at"},
                                    "iso_week": {"$isoWeek": "$created_at"},
                                    "iso_week_year": {"$isoWeekYear": "$created_at"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "week": 0, "iso_week": 1, "iso_week_year": 2026}],
                    )

    def test_aggregate_supports_date_string_parts_and_parsing_variants(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "created_at": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000)})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "formatted": {
                                        "$dateToString": {
                                            "date": "$created_at",
                                            "format": "%Y-%m-%d %H:%M:%S.%L",
                                            "timezone": "UTC",
                                        }
                                    },
                                    "parts": {"$dateToParts": {"date": "$created_at", "timezone": "UTC"}},
                                    "parsed": {
                                        "$dateFromString": {
                                            "dateString": "2026-03-25 12:05:06.789",
                                            "format": "%Y-%m-%d %H:%M:%S.%L",
                                            "timezone": "+02:00",
                                        }
                                    },
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(documents[0]["_id"], "1")
                    self.assertEqual(documents[0]["formatted"], "2026-03-25 10:05:06.789")
                    self.assertEqual(
                        documents[0]["parts"],
                        {
                            "year": 2026,
                            "month": 3,
                            "day": 25,
                            "hour": 10,
                            "minute": 5,
                            "second": 6,
                            "millisecond": 789,
                        },
                    )
                    self.assertEqual(documents[0]["parsed"], datetime.datetime(2026, 3, 25, 10, 5, 6, 789000))

    def test_aggregate_supports_object_to_array_and_zip(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "doc": {"a": 1, "b": 2}, "left": ["a", "b"], "right": [1], "defaults": ["x", 0]})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "pairs": {"$objectToArray": "$doc"},
                                    "zipped": {
                                        "$zip": {
                                            "inputs": ["$left", "$right"],
                                            "useLongestLength": True,
                                            "defaults": "$defaults",
                                        }
                                    },
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "pairs": [{"k": "a", "v": 1}, {"k": "b", "v": 2}],
                                "zipped": [["a", 1], ["b", 0]],
                            }
                        ],
                    )

    def test_aggregate_supports_to_date_and_date_from_parts(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "millis": 1_711_361_506_789, "text": "2026-03-25T10:05:06.789Z"})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "as_date": {"$toDate": "$text"},
                                    "from_parts": {
                                        "$dateFromParts": {
                                            "year": 2026,
                                            "month": 3,
                                            "day": 25,
                                            "hour": 12,
                                            "timezone": "+02:00",
                                        }
                                    },
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "as_date": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
                                "from_parts": datetime.datetime(2026, 3, 25, 10, 0, 0),
                            }
                        ],
                    )

    def test_aggregate_supports_to_object_id(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "oid_text": "65f0a1000000000000000000"})

                    documents = collection.aggregate(
                        [{"$project": {"_id": 1, "oid": {"$toObjectId": "$oid_text"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "oid": ObjectId("65f0a1000000000000000000")}],
                    )

    def test_aggregate_supports_to_decimal(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "value": "10.25"})

                    documents = collection.aggregate(
                        [{"$project": {"_id": 1, "decimal": {"$toDecimal": "$value"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "decimal": decimal.Decimal("10.25")}],
                    )

    def test_aggregate_supports_to_uuid(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one(
                        {
                            "_id": "1",
                            "uuid_text": "12345678-1234-5678-1234-567812345678",
                        }
                    )

                    documents = collection.aggregate(
                        [{"$project": {"_id": 1, "uuid": {"$toUUID": "$uuid_text"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                            }
                        ],
                    )

    def test_aggregate_supports_documents_stage(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "persisted", "score": 1})

                    documents = collection.aggregate(
                        [
                            {
                                "$documents": [
                                    {"_id": "1", "score": 10},
                                    {"_id": "2", "score": 20},
                                ]
                            },
                            {"$match": {"score": {"$gte": 15}}},
                        ]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "2", "score": 20}])

    def test_aggregate_supports_date_part_extractors(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one(
                        {
                            "_id": "1",
                            "created_at": datetime.datetime(2026, 3, 29, 22, 5, 6, 789000),
                        }
                    )

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "year": {"$year": "$created_at"},
                                    "month": {"$month": "$created_at"},
                                    "day_of_month": {
                                        "$dayOfMonth": {
                                            "date": "$created_at",
                                            "timezone": "+02:00",
                                        }
                                    },
                                    "day_of_week": {
                                        "$dayOfWeek": {
                                            "date": "$created_at",
                                            "timezone": "+02:00",
                                        }
                                    },
                                    "day_of_year": {"$dayOfYear": "$created_at"},
                                    "hour": {
                                        "$hour": {
                                            "date": "$created_at",
                                            "timezone": "+02:00",
                                        }
                                    },
                                    "minute": {"$minute": "$created_at"},
                                    "second": {"$second": "$created_at"},
                                    "millisecond": {"$millisecond": "$created_at"},
                                    "iso_day_of_week": {"$isoDayOfWeek": "$created_at"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "year": 2026,
                                "month": 3,
                                "day_of_month": 30,
                                "day_of_week": 2,
                                "day_of_year": 88,
                                "hour": 0,
                                "minute": 5,
                                "second": 6,
                                "millisecond": 789,
                                "iso_day_of_week": 7,
                            }
                        ],
                    )

    def test_aggregate_supports_slice_is_array_and_cmp(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one(
                        {
                            "_id": "1",
                            "values": [1, 2, 3, 4],
                            "nested": {"a": 1},
                        }
                    )

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "head": {"$slice": ["$values", 2]},
                                    "tail": {"$slice": ["$values", -2]},
                                    "middle": {"$slice": ["$values", 1, 2]},
                                    "is_array": {"$isArray": "$values"},
                                    "cmp": {"$cmp": [3, 2]},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "head": [1, 2],
                                "tail": [3, 4],
                                "middle": [2, 3],
                                "is_array": True,
                                "cmp": 1,
                            }
                        ],
                    )

    def test_aggregate_supports_convert_set_field_and_unset_field(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "value": "10", "nested": {"a": 1}})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "converted": {"$convert": {"input": "$value", "to": "int"}},
                                    "updated": {"$setField": {"field": "name", "input": "$nested", "value": "Ada"}},
                                    "trimmed": {"$unsetField": {"field": "a", "input": "$nested"}},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "converted": 10, "updated": {"a": 1, "name": "Ada"}, "trimmed": {}}],
                    )

    def test_aggregate_supports_bson_size_and_rand(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "doc": {"a": 1, "name": "Ada"}})

                    documents = collection.aggregate(
                        [{"$project": {"_id": 1, "size": {"$bsonSize": "$doc"}, "random": {"$rand": {}}}}]
                    ).to_list()

                    self.assertEqual(documents[0]["_id"], "1")
                    self.assertEqual(documents[0]["size"], 26)
                    self.assertGreaterEqual(documents[0]["random"], 0.0)
                    self.assertLess(documents[0]["random"], 1.0)

    def test_aggregate_supports_switch_and_bitwise_variants(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one({"_id": "1", "value": 5})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "size": {
                                        "$switch": {
                                            "branches": [{"case": {"$gt": ["$value", 10]}, "then": "big"}],
                                            "default": "small",
                                        }
                                    },
                                    "anded": {"$bitAnd": [7, 3]},
                                    "notted": {"$bitNot": 7},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "size": "small", "anded": 3, "notted": -8}],
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

    def test_aggregate_supports_merge_objects_over_lookup_result_array(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    deliveries = client.test.deliveries
                    units = client.test.units
                    deliveries.insert_one({"_id": "d1", "tenant": "a"})
                    units.insert_one({"_id": "u1", "fulfillment_id": "d1", "name": "Box"})
                    units.insert_one({"_id": "u2", "fulfillment_id": "d1", "status": "packed"})

                    documents = deliveries.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "units",
                                    "as": "unit",
                                    "let": {"ref_key": "$_id"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$eq": ["$fulfillment_id", "$$ref_key"]},
                                                    ]
                                                }
                                            }
                                        }
                                    ],
                                }
                            },
                            {"$match": {"$expr": {"$gt": [{"$size": "$unit"}, 0]}}},
                            {"$addFields": {"unit_list": "$unit", "unit": {"$mergeObjects": "$unit"}}},
                            {"$project": {"_id": 0, "unit_list": 1, "unit": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(len(documents), 1)
                    self.assertIsInstance(documents[0]["unit_list"], list)
                    self.assertEqual(len(documents[0]["unit_list"]), 2)
                    self.assertEqual(documents[0]["unit"]["name"], "Box")
                    self.assertEqual(documents[0]["unit"]["status"], "packed")

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

    def test_aggregate_supports_field_bound_logical_expr_conditions_inside_lookup(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one({"_id": "u1", "tenant": "a", "role": "admin", "score": 3})
                    users.insert_one({"_id": "u2", "tenant": "a", "role": "staff", "score": 4})
                    users.insert_one({"_id": "u3", "tenant": "a", "role": "guest", "score": 4})
                    users.insert_one({"_id": "u4", "tenant": "a", "role": "admin", "score": 7})
                    events.insert_one({"_id": "1", "tenant": "a"})

                    documents = events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"tenantId": "$tenant"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$eq": ["$tenant", "$$tenantId"]},
                                                        {"$or": ["$role", [{"$eq": "admin"}, {"$eq": "staff"}]]},
                                                        {"$and": ["$score", [{"$gte": 3}, {"$lt": 5}]]},
                                                    ]
                                                }
                                            }
                                        },
                                        {"$project": {"_id": 0, "role": 1, "score": 1}},
                                        {"$sort": {"role": 1}},
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
                                    {"role": "admin", "score": 3},
                                    {"role": "staff", "score": 4},
                                ],
                            }
                        ],
                    )

    def test_aggregate_supports_field_bound_query_filter_expr_conditions_inside_lookup(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one(
                        {
                            "_id": "u1",
                            "tenant": "a",
                            "tags": ["a", "b"],
                            "status": "active",
                            "items": [{"kind": "x", "qty": 1}, {"kind": "y", "qty": 3}],
                        }
                    )
                    users.insert_one(
                        {
                            "_id": "u2",
                            "tenant": "a",
                            "tags": ["a"],
                            "status": "archived",
                            "items": [{"kind": "y", "qty": 1}],
                        }
                    )
                    users.insert_one({"_id": "u3", "tenant": "a", "status": "active"})
                    events.insert_one({"_id": "1", "tenant": "a"})

                    documents = events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"tenantId": "$tenant"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$eq": ["$tenant", "$$tenantId"]},
                                                        {"$exists": ["$tags", True]},
                                                        {"$all": ["$tags", ["a", "b"]]},
                                                        {"$nin": ["$status", ["archived"]]},
                                                        {"$elemMatch": ["$items", {"kind": "y", "qty": {"$gte": 2}}]},
                                                    ]
                                                }
                                            }
                                        },
                                        {"$project": {"_id": 1}},
                                    ],
                                    "as": "users",
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "tenant": "a", "users": [{"_id": "u1"}]}],
                    )

    def test_aggregate_supports_correlated_list_lookup_with_in_and_dotted_variable_path(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.test.events
                    users = client.test.users
                    users.insert_one({"_id": "u1", "name": "Ada"})
                    users.insert_one({"_id": "u2", "name": "Grace"})
                    users.insert_one({"_id": "u3", "name": "Linus"})
                    events.insert_one({"_id": "1", "links": [{"id": "u1"}, {"id": "u3"}]})
                    events.insert_one({"_id": "2", "links": []})
                    events.insert_one({"_id": "3"})

                    documents = events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"ref_key": "$links"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$gt": [{"$size": {"$ifNull": ["$$ref_key.id", []]}}, 0]},
                                                        {"$in": ["$_id", "$$ref_key.id"]},
                                                    ]
                                                }
                                            }
                                        },
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
                            {"_id": "1", "links": [{"id": "u1"}, {"id": "u3"}], "users": [{"name": "Ada"}, {"name": "Linus"}]},
                            {"_id": "2", "links": [], "users": []},
                            {"_id": "3", "users": []},
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

    def test_aggregate_let_bindings_are_visible_to_pipeline_expressions(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "tenant": "a", "name": "Ada"})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "tenant_match": {"$eq": ["$tenant", "$$tenant"]},
                                    "label": {"$concat": ["$$prefix", "$name"]},
                                }
                            }
                        ],
                        let={"tenant": "a", "prefix": "user:"},
                    ).to_list()

                    self.assertEqual(documents, [{"tenant_match": True, "label": "user:Ada"}])

    def test_write_operations_support_let_through_expr_filters(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "matched": False},
                            {"_id": "2", "tenant": "b", "matched": False},
                        ]
                    )

                    result = collection.update_many(
                        {"$expr": {"$eq": ["$tenant", "$$tenant"]}},
                        {"$set": {"matched": True}},
                        let={"tenant": "a"},
                    )

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(
                        collection.find({}, {"_id": 1, "matched": 1}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "1", "matched": True}, {"_id": "2", "matched": False}],
                    )

    def test_bulk_write_inherits_let_for_expr_filters(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "flag": False},
                            {"_id": "2", "tenant": "b", "flag": False},
                        ]
                    )

                    result = collection.bulk_write(
                        [
                            UpdateOne(
                                {"$expr": {"$eq": ["$tenant", "$$tenant"]}},
                                {"$set": {"flag": True}},
                            )
                        ],
                        let={"tenant": "b"},
                    )

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(
                        collection.find({}, {"_id": 1, "flag": 1}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "1", "flag": False}, {"_id": "2", "flag": True}],
                    )

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

    def test_aggregate_supports_union_with_and_union_with_inside_facet(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    events = client.analytics.events
                    archived = client.analytics.archived_events
                    events.insert_many(
                        [
                            {"_id": "e1", "kind": "event", "rank": 2},
                            {"_id": "e2", "kind": "event", "rank": 1},
                        ]
                    )
                    archived.insert_many(
                        [
                            {"_id": "a1", "kind": "archive", "rank": 3},
                            {"_id": "a2", "kind": "archive", "rank": 0},
                        ]
                    )

                    unioned = events.aggregate(
                        [
                            {"$unionWith": {"coll": "archived_events", "pipeline": [{"$sort": {"rank": 1}}]}},
                            {"$project": {"_id": 1, "kind": 1, "rank": 1}},
                        ]
                    ).to_list()
                    faceted = events.aggregate(
                        [
                            {
                                "$facet": {
                                    "combined": [
                                        {"$unionWith": "archived_events"},
                                        {"$sort": {"rank": 1}},
                                        {"$project": {"_id": 1}},
                                    ]
                                }
                            }
                        ]
                    ).to_list()
                    events.insert_one({"_id": "e3", "kind": "archive", "rank": 0})
                    current_only = events.aggregate(
                        [
                            {"$match": {"kind": "event"}},
                            {"$unionWith": {"pipeline": [{"$match": {"kind": "archive"}}]}},
                            {"$sort": {"rank": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        unioned,
                        [
                            {"_id": "e1", "kind": "event", "rank": 2},
                            {"_id": "e2", "kind": "event", "rank": 1},
                            {"_id": "a2", "kind": "archive", "rank": 0},
                            {"_id": "a1", "kind": "archive", "rank": 3},
                        ],
                    )
                    self.assertEqual(
                        faceted,
                        [{"combined": [{"_id": "a2"}, {"_id": "e2"}, {"_id": "e1"}, {"_id": "a1"}]}],
                    )
                    self.assertEqual(
                        current_only,
                        [{"_id": "e3"}, {"_id": "e2"}, {"_id": "e1"}],
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

    def test_aggregate_supports_stddev_accumulators(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 2},
                            {"_id": "2", "group": "a", "rank": 2, "score": 4},
                            {"_id": "3", "group": "a", "rank": 3, "score": 4},
                            {"_id": "4", "group": "b", "rank": 1, "score": 10},
                        ]
                    )

                    grouped = collection.aggregate(
                        [{"$group": {"_id": "$group", "pop": {"$stdDevPop": "$score"}, "samp": {"$stdDevSamp": "$score"}}}]
                    ).to_list()
                    windowed = collection.aggregate(
                        [
                            {
                                "$setWindowFields": {
                                    "partitionBy": "$group",
                                    "sortBy": {"rank": 1},
                                    "output": {
                                        "runningPop": {"$stdDevPop": "$score", "window": {"documents": ["unbounded", "current"]}},
                                    },
                                }
                            },
                            {"$project": {"_id": 0, "group": 1, "rank": 1, "runningPop": 1}},
                        ]
                    ).to_list()

                    self.assertAlmostEqual(grouped[0]["pop"], 0.94280904158, places=10)
                    self.assertAlmostEqual(grouped[0]["samp"], 1.15470053838, places=10)
                    self.assertEqual(grouped[1], {"_id": "b", "pop": 0.0, "samp": None})
                    self.assertEqual(windowed[0]["runningPop"], 0.0)
                    self.assertAlmostEqual(windowed[1]["runningPop"], 1.0, places=10)
                    self.assertAlmostEqual(windowed[2]["runningPop"], 0.94280904158, places=10)

    def test_aggregate_supports_stddev_expression_forms(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.events
                    collection.insert_one({"_id": "1", "scores": [2, 4, 4, "x"], "a": 2, "b": 4, "c": 4})

                    documents = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "popArray": {"$stdDevPop": "$scores"},
                                    "sampArray": {"$stdDevSamp": "$scores"},
                                    "popList": {"$stdDevPop": ["$a", "$b", "$c"]},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertAlmostEqual(documents[0]["popArray"], 0.94280904158, places=10)
                    self.assertAlmostEqual(documents[0]["sampArray"], 1.15470053838, places=10)
                    self.assertAlmostEqual(documents[0]["popList"], 0.94280904158, places=10)

    def test_aggregate_supports_first_n_and_last_n(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.analytics.events
                    collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 2},
                            {"_id": "2", "group": "a", "rank": 2, "score": 4},
                            {"_id": "3", "group": "a", "rank": 3, "score": 6},
                            {"_id": "4", "group": "b", "rank": 1, "score": 9},
                            {"_id": "5", "group": "b", "rank": 2},
                        ]
                    )

                    grouped = collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$group",
                                    "firstTwo": {"$firstN": {"input": "$score", "n": 2}},
                                    "lastTwo": {"$lastN": {"input": "$score", "n": 2}},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    projected = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "firstScores": {"$firstN": {"input": [10, 20, 30, 40], "n": 3}},
                                    "lastScores": {"$lastN": {"input": [10, 20, 30, 40], "n": 2}},
                                    "topScores": {"$maxN": {"input": [1, None, 3, 2], "n": 2}},
                                    "bottomScores": {"$minN": {"input": [1, None, 3, 2], "n": 2}},
                                }
                            },
                            {"$limit": 1},
                        ]
                    ).to_list()

                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "firstTwo": [2, 4], "lastTwo": [4, 6]},
                            {"_id": "b", "firstTwo": [9, None], "lastTwo": [9, None]},
                        ],
                    )
                    self.assertEqual(
                        projected,
                        [
                            {
                                "firstScores": [10, 20, 30],
                                "lastScores": [30, 40],
                                "topScores": [3, 2],
                                "bottomScores": [1, 2],
                            }
                        ],
                    )

    def test_aggregate_supports_top_and_bottom_accumulators(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.analytics.events
                    collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 2, "label": "a1"},
                            {"_id": "2", "group": "a", "rank": 2, "score": 4, "label": "a2"},
                            {"_id": "3", "group": "a", "rank": 3, "score": 4, "label": "a3"},
                            {"_id": "4", "group": "b", "rank": 1, "score": None, "label": "b1"},
                            {"_id": "5", "group": "b", "rank": 2, "label": "b2"},
                        ]
                    )

                    grouped = collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$group",
                                    "topLabel": {"$top": {"sortBy": {"score": -1}, "output": "$label"}},
                                    "bottomLabel": {"$bottom": {"sortBy": {"score": -1}, "output": "$label"}},
                                    "topTwo": {"$topN": {"sortBy": {"score": -1}, "output": "$label", "n": 2}},
                                    "bottomTwo": {"$bottomN": {"sortBy": {"score": -1}, "output": "$label", "n": 2}},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "topLabel": "a2", "bottomLabel": "a1", "topTwo": ["a2", "a3"], "bottomTwo": ["a2", "a1"]},
                            {"_id": "b", "topLabel": "b1", "bottomLabel": "b1", "topTwo": ["b1", "b2"], "bottomTwo": ["b1", "b2"]},
                        ],
                    )

    def test_aggregate_supports_percentile_and_median(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.analytics.events
                    collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 1, "scores": [1, 2, 3, 4]},
                            {"_id": "2", "group": "a", "rank": 2, "score": 5, "scores": [10, 20, 30, 40]},
                            {"_id": "3", "group": "a", "rank": 3, "score": "x", "scores": [7, "x", 9]},
                            {"_id": "4", "group": "b", "rank": 1, "score": 2},
                        ]
                    )

                    grouped = collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$group",
                                    "medianScore": {"$median": {"input": "$score", "method": "approximate"}},
                                    "percentiles": {"$percentile": {"input": "$score", "p": [0.0, 0.5, 1.0], "method": "approximate"}},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "medianScore": 1, "percentiles": [1, 1, 5]},
                            {"_id": "b", "medianScore": 2, "percentiles": [2, 2, 2]},
                        ],
                    )

    def test_aggregate_supports_rank_family_window_operators(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.analytics.events
                    collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "score": 10},
                            {"_id": "2", "group": "a", "score": 20},
                            {"_id": "3", "group": "a", "score": 20},
                            {"_id": "4", "group": "b", "score": 5},
                        ]
                    )

                    documents = collection.aggregate(
                        [
                            {
                                "$setWindowFields": {
                                    "partitionBy": "$group",
                                    "sortBy": {"score": 1},
                                    "output": {
                                        "rank": {"$rank": {}},
                                        "dense": {"$denseRank": {}},
                                        "docnum": {"$documentNumber": {}},
                                    },
                                }
                            },
                            {"$project": {"_id": 1, "group": 1, "score": 1, "rank": 1, "dense": 1, "docnum": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "group": "a", "score": 10, "rank": 1, "dense": 1, "docnum": 1},
                            {"_id": "2", "group": "a", "score": 20, "rank": 2, "dense": 2, "docnum": 2},
                            {"_id": "3", "group": "a", "score": 20, "rank": 2, "dense": 2, "docnum": 3},
                            {"_id": "4", "group": "b", "score": 5, "rank": 1, "dense": 1, "docnum": 1},
                        ],
                    )

    def test_aggregate_supports_string_expressions_last_and_add_to_set(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.analytics.events
                    collection.insert_one({"_id": "1", "tenant": "a", "rank": 1, "kind": "view", "label": "Ada"})
                    collection.insert_one({"_id": "2", "tenant": "a", "rank": 2, "kind": "view", "label": "Lovelace"})
                    collection.insert_one({"_id": "3", "tenant": "a", "rank": 3, "kind": "click", "label": "Analytical"})

                    projected = collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "full": {"$concat": ["$label", "-", "$kind"]},
                                    "lower": {"$toLower": "$label"},
                                    "upper": {"$toUpper": "$kind"},
                                    "prefix": {"$substr": ["$label", 0, 3]},
                                    "compare": {"$strcasecmp": ["$kind", "VIEW"]},
                                }
                            },
                            {"$sort": {"full": 1}},
                        ]
                    ).to_list()
                    grouped = collection.aggregate(
                        [
                            {"$sort": {"rank": 1}},
                            {
                                "$group": {
                                    "_id": "$tenant",
                                    "lastKind": {"$last": "$kind"},
                                    "kinds": {"$addToSet": "$kind"},
                                }
                            },
                        ]
                    ).to_list()

                    self.assertEqual(
                        projected,
                        [
                            {"full": "Ada-view", "lower": "ada", "upper": "VIEW", "prefix": "Ada", "compare": 0},
                            {"full": "Analytical-click", "lower": "analytical", "upper": "CLICK", "prefix": "Ana", "compare": -1},
                            {"full": "Lovelace-view", "lower": "lovelace", "upper": "VIEW", "prefix": "Lov", "compare": 0},
                        ],
                    )
                    self.assertEqual(
                        grouped,
                        [{"_id": "a", "lastKind": "click", "kinds": ["view", "click"]}],
                    )

    def test_aggregate_supports_split_count_and_merge_objects(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.analytics.events
                    collection.insert_one({"_id": "1", "tenant": "a", "text": "Ada Lovelace", "meta": {"x": 1}})
                    collection.insert_one({"_id": "2", "tenant": "a", "text": "Grace Hopper", "meta": {"y": 2}})
                    collection.insert_one({"_id": "3", "tenant": "b", "text": "Alan Turing", "meta": None})

                    projected = collection.aggregate(
                        [
                            {"$project": {"_id": 0, "parts": {"$split": ["$text", " "]}}},
                            {"$limit": 1},
                        ]
                    ).to_list()
                    grouped = collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$tenant",
                                    "count": {"$count": {}},
                                    "merged": {"$mergeObjects": "$meta"},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(projected, [{"parts": ["Ada", "Lovelace"]}])
                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "count": 2, "merged": {"x": 1, "y": 2}},
                            {"_id": "b", "count": 1, "merged": {}},
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

                    name = collection.create_index([("profile.name", -1)], unique=False)
                    indexes = collection.list_indexes().to_list()
                    info = collection.index_information()

                    self.assertEqual(name, "profile.name_-1")
                    self.assertEqual(
                        indexes,
                        [
                            {"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True},
                            {
                                "name": "profile.name_-1",
                                "fields": ["profile.name"],
                                "key": {"profile.name": -1},
                                "unique": False,
                            },
                        ],
                    )
                    self.assertEqual(
                        info,
                        {
                            "_id_": {"key": [("_id", 1)], "unique": True},
                            "profile.name_-1": {"key": [("profile.name", -1)]},
                        },
                    )

    def test_collection_accepts_mapping_key_spec_for_create_index(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    name = collection.create_index({"email": 1, "created_at": -1})
                    info = collection.index_information()

                    self.assertEqual(name, "email_1_created_at_-1")
                    self.assertEqual(
                        info["email_1_created_at_-1"],
                        {"key": [("email", 1), ("created_at", -1)]},
                    )

    def test_collection_can_create_and_drop_multiple_indexes(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    names = collection.create_indexes(
                        [
                            IndexModel([("email", 1)], unique=True),
                            IndexModel([("tenant", 1), ("created_at", -1)], name="tenant_created"),
                        ]
                    )
                    collection.drop_index("tenant_created")
                    indexes_after_drop = collection.list_indexes().to_list()
                    collection.drop_indexes()
                    indexes_after_drop_all = collection.list_indexes().to_list()

                    self.assertEqual(names, ["email_1", "tenant_created"])
                    self.assertEqual(
                        indexes_after_drop,
                        [
                            {"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True},
                            {"name": "email_1", "fields": ["email"], "key": {"email": 1}, "unique": True},
                        ],
                    )
                    self.assertEqual(
                        indexes_after_drop_all,
                        [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}],
                    )

    def test_hint_requires_existing_index_and_is_reflected_in_explain(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_many(
                        [
                            {"_id": "1", "kind": "view", "rank": 2},
                            {"_id": "2", "kind": "view", "rank": 1},
                        ]
                    )
                    collection.create_index([("kind", 1)], name="kind_idx")

                    documents = collection.find(
                        {"kind": "view"},
                        sort=[("rank", 1)],
                        hint="kind_idx",
                    ).to_list()
                    explanation = collection.find(
                        {"kind": "view"},
                        hint="kind_idx",
                    ).explain()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "2", "kind": "view", "rank": 1},
                            {"_id": "1", "kind": "view", "rank": 2},
                        ],
                    )
                    self.assertEqual(explanation["hint"], "kind_idx")
                    self.assertEqual(explanation["hinted_index"], "kind_idx")

                    with self.assertRaises(OperationFailure):
                        collection.find({"kind": "view"}, hint="missing_idx").to_list()

    def test_create_indexes_rolls_back_batch_on_failure(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    with self.assertRaises(OperationFailure):
                        collection.create_indexes(
                            [
                                IndexModel([("email", 1)], name="idx_email"),
                                IndexModel([("email", 1)], unique=True, name="idx_email_unique"),
                            ]
                        )

                    self.assertEqual(
                        collection.list_indexes().to_list(),
                        [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}],
                    )

    def test_drop_index_by_spec_rejects_custom_named_index(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.create_index([("email", 1)], name="idx_email")

                    with self.assertRaises(OperationFailure):
                        collection.drop_index([("email", 1)])

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

    def test_sync_client_exposes_client_session_and_accepts_it(self):
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

    def test_find_with_comment_and_max_time_updates_session_state_and_explain(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    collection.insert_one({"_id": "1", "kind": "view"}, session=session)

                    documents = collection.find(
                        {"kind": "view"},
                        comment="trace-find",
                        max_time_ms=25,
                        session=session,
                    ).to_list()
                    explanation = collection.find(
                        {"kind": "view"},
                        comment="trace-find",
                        max_time_ms=25,
                        session=session,
                    ).explain()
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(documents, [{"_id": "1", "kind": "view"}])
                    self.assertEqual(state["last_operation"]["comment"], "trace-find")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)
                    self.assertEqual(explanation["comment"], "trace-find")
                    self.assertEqual(explanation["max_time_ms"], 25)

    def test_write_and_index_comments_update_session_state(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    collection.insert_one({"_id": "1", "kind": "view"}, session=session)

                    collection.update_one(
                        {"_id": "1"},
                        {"$set": {"done": True}},
                        comment="trace-update",
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))
                    self.assertEqual(state["last_operation"]["operation"], "update_one")
                    self.assertEqual(state["last_operation"]["comment"], "trace-update")

                    collection.create_index(
                        [("kind", 1)],
                        comment="trace-index",
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))
                    self.assertEqual(state["last_operation"]["operation"], "create_index")
                    self.assertEqual(state["last_operation"]["comment"], "trace-index")

    def test_sync_client_accepts_sqlite_engine(self):
        with MongoClient(SQLiteEngine()) as client:
            collection = client.test.users
            result = collection.insert_one({"name": "Ada"})
            found = collection.find_one({"_id": result.inserted_id})

            self.assertEqual(found["name"], "Ada")

    def test_sync_session_transaction_round_trip(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()

                    session.start_transaction()
                    self.assertTrue(session.transaction_active)
                    client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=session)
                    session.commit_transaction()

                    self.assertFalse(session.transaction_active)

    def test_sync_session_with_transaction_commits_and_returns_result(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()

                    def _run(active: ClientSession) -> str:
                        client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=active)
                        return "ok"

                    result = session.with_transaction(_run)

                    self.assertEqual(result, "ok")
                    self.assertFalse(session.in_transaction)
                    self.assertEqual(client.test.users.count_documents({}), 1)

    def test_sync_sqlite_session_transaction_is_isolated_and_abortable(self):
        with MongoClient(SQLiteEngine()) as client:
            session = client.start_session()
            self.assertTrue(session.get_engine_state(next(iter(session.engine_state)))["supports_transactions"])

            session.start_transaction()
            client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=session)

            self.assertEqual(client.test.users.count_documents({}, session=session), 1)
            with self.assertRaises(InvalidOperation):
                client.test.users.count_documents({})

            session.abort_transaction()

            self.assertEqual(client.test.users.count_documents({}), 0)

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

    def test_find_projection_supports_slice_and_elem_match_operators(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_one(
                        {
                            "_id": "user-1",
                            "name": "Ada",
                            "tags": ["a", "b", "c", "d"],
                            "students": [
                                {"school": 100, "age": 7},
                                {"school": 102, "age": 10},
                                {"school": 102, "age": 11},
                            ],
                            "role": "admin",
                        }
                    )

                    sliced = collection.find_one({"_id": "user-1"}, {"tags": {"$slice": [1, 2]}, "role": 0})
                    matched = collection.find_one(
                        {"_id": "user-1"},
                        {"name": 1, "students": {"$elemMatch": {"school": 102, "age": {"$gt": 10}}}, "_id": 0},
                    )

                    self.assertEqual(
                        sliced,
                        {
                            "_id": "user-1",
                            "name": "Ada",
                            "tags": ["b", "c"],
                            "students": [
                                {"school": 100, "age": 7},
                                {"school": 102, "age": 10},
                                {"school": 102, "age": 11},
                            ],
                        },
                    )
                    self.assertEqual(matched, {"name": "Ada", "students": [{"school": 102, "age": 11}]})

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
                        collection.update_one({}, [1])

                    with self.assertRaises(TypeError):
                        collection.delete_one([])

                    with self.assertRaises(TypeError):
                        collection.count_documents([])

    def test_count_documents_supports_skip_limit_hint_comment_and_max_time(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    collection.insert_many(
                        [
                            {"_id": "1", "kind": "view"},
                            {"_id": "2", "kind": "view"},
                            {"_id": "3", "kind": "view"},
                        ],
                        session=session,
                    )
                    collection.create_index([("kind", 1)], name="kind_idx", session=session)

                    count = collection.count_documents(
                        {"kind": "view"},
                        skip=1,
                        limit=1,
                        hint="kind_idx",
                        comment="trace-count-documents",
                        max_time_ms=25,
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(count, 1)
                    self.assertEqual(state["last_operation"]["comment"], "trace-count-documents")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)

    def test_update_one_rejects_invalid_update_shapes(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users

                    with self.assertRaises(ValueError):
                        collection.update_one({}, {})

                    with self.assertRaises(ValueError):
                        collection.update_one({}, {"name": "Ada"})

                    with self.assertRaises(ValueError):
                        collection.update_one({}, [])

                    with self.assertRaises(TypeError):
                        collection.update_one({}, {"$set": []})

    def test_update_operations_support_aggregation_pipeline_updates(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    collection = client.test.users
                    collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "name": "Ada", "points": 2, "bonus": 3, "legacy": "x"},
                            {"_id": "2", "tenant": "a", "name": "Bob", "points": 5, "bonus": 1, "legacy": "y"},
                        ]
                    )

                    first = collection.update_one(
                        {"_id": "1"},
                        [
                            {"$set": {"total": {"$add": ["$points", "$bonus"]}, "label": "$$tag"}},
                            {"$unset": "legacy"},
                        ],
                        let={"tag": "single"},
                    )
                    many = collection.update_many(
                        {"tenant": "a"},
                        [{"$set": {"tenant_label": {"$concat": ["$tenant", "-active"]}}}],
                    )
                    after = collection.find_one_and_update(
                        {"_id": "2"},
                        [
                            {
                                "$replaceWith": {
                                    "$mergeObjects": [
                                        "$$ROOT",
                                        {"summary": {"$concat": ["$name", ":", "$$suffix"]}},
                                    ]
                                }
                            }
                        ],
                        let={"suffix": "done"},
                        return_document=ReturnDocument.AFTER,
                    )
                    bulk = collection.bulk_write(
                        [
                            UpdateOne({"_id": "1"}, [{"$set": {"bulk_rank": {"$literal": 1}}}]),
                            UpdateMany({"tenant": "a"}, [{"$set": {"bulk_tag": {"$literal": "batch"}}}]),
                            UpdateOne(
                                {"tenant": "b", "kind": {"$eq": "new"}},
                                [{"$set": {"created_from_filter": "$tenant", "state": {"$literal": "upserted"}}}],
                                upsert=True,
                            ),
                        ]
                    )

                    documents = collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(first.matched_count, 1)
                    self.assertEqual(first.modified_count, 1)
                    self.assertEqual(many.matched_count, 2)
                    self.assertEqual(many.modified_count, 2)
                    self.assertEqual(after["_id"], "2")
                    self.assertEqual(after["summary"], "Bob:done")
                    self.assertEqual(bulk.matched_count, 3)
                    self.assertEqual(bulk.modified_count, 3)
                    self.assertEqual(bulk.upserted_count, 1)
                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "tenant": "a",
                                "name": "Ada",
                                "points": 2,
                                "bonus": 3,
                                "total": 5,
                                "label": "single",
                                "tenant_label": "a-active",
                                "bulk_rank": 1,
                                "bulk_tag": "batch",
                            },
                            {
                                "_id": "2",
                                "tenant": "a",
                                "name": "Bob",
                                "points": 5,
                                "bonus": 1,
                                "legacy": "y",
                                "tenant_label": "a-active",
                                "summary": "Bob:done",
                                "bulk_tag": "batch",
                            },
                            {
                                "_id": bulk.upserted_ids[2],
                                "tenant": "b",
                                "kind": "new",
                                "created_from_filter": "b",
                                "state": "upserted",
                            },
                        ],
                    )

    def test_operations_after_close_raise_invalid_operation(self):
        client = MongoClient()
        collection = client.test.users
        client.close()

        with self.assertRaises(InvalidOperation):
            client.list_database_names()

        with self.assertRaises(InvalidOperation):
            collection.find_one({})


class SyncApiLoopSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_client_supports_usage_inside_active_event_loop(self):
        client = MongoClient(MemoryEngine())
        try:
            self.assertEqual(client.list_database_names(), [])
            client.test.users.insert_one({"_id": "1", "name": "Ada"})
            self.assertEqual(client.test.users.find_one({"_id": "1"}), {"_id": "1", "name": "Ada"})
        finally:
            client.close()


class SyncApiFilterAliasTests(unittest.TestCase):
    def test_sync_collection_supports_filter_keyword_alias(self):
        with MongoClient(MemoryEngine()) as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "name": "Ada", "kind": "view"})

            found = collection.find_one(filter={"_id": "1"})
            count = collection.count_documents(filter={"kind": "view"})
            distinct = collection.distinct("kind", filter={"_id": "1"})

            self.assertEqual(found, {"_id": "1", "name": "Ada", "kind": "view"})
            self.assertEqual(count, 1)
            self.assertEqual(distinct, ["view"])

    def test_sync_collection_rejects_filter_alias_conflict(self):
        with MongoClient(MemoryEngine()) as client:
            collection = client.test.users

            with self.assertRaises(TypeError):
                collection.find_one({"_id": "1"}, filter={"_id": "1"})

    def test_sync_collection_supports_update_keyword_alias(self):
        with MongoClient(MemoryEngine()) as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "name": "Ada", "count": 1})

            collection.update_one(filter={"_id": "1"}, update={"$inc": {"count": 1}})

            self.assertEqual(
                collection.find_one({"_id": "1"}),
                {"_id": "1", "name": "Ada", "count": 2},
            )

    def test_sync_collection_accepts_profile_normalized_kwargs(self):
        with MongoClient(MemoryEngine()) as client:
            collection = client.test.users
            collection.insert_one({"_id": "1", "name": "Ada", "rank": 2})
            collection.insert_one({"_id": "2", "name": "Grace", "rank": 1})

            found = collection.find_one(
                filter={"name": {"$in": ["Ada", "Grace"]}},
                sort=[("rank", 1)],
                skip=1,
            )

            self.assertEqual(found, {"_id": "1", "name": "Ada", "rank": 2})

    def test_sync_collection_rejects_unknown_public_kwargs(self):
        with MongoClient(MemoryEngine()) as client:
            collection = client.test.users

            with self.assertRaises(TypeError):
                collection.find_one(filter={"_id": "1"}, unsupported=True)

    def test_sync_database_supports_filter_keyword_alias(self):
        with MongoClient(MemoryEngine()) as client:
            database = client.get_database("test")
            client.test.users.insert_one({"_id": "1"})

            names = database.list_collection_names(filter={"name": "users"})
            listings = database.list_collections(filter={"name": "users"}).to_list()

            self.assertEqual(names, ["users"])
            self.assertEqual([document["name"] for document in listings], ["users"])
