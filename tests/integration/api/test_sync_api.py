import datetime
import threading
import unittest

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
    TransactionOptions,
    UpdateMany,
    UpdateOne,
    WriteConcern,
)
from mongoeco.api._sync.aggregation_cursor import AggregationCursor
from mongoeco.api._sync.cursor import Cursor
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, CollectionInvalid, DuplicateKeyError, InvalidOperation, OperationFailure
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
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    client.alpha.create_collection("events", capped=True)

                    self.assertEqual(client.alpha.command("ping"), {"ok": 1.0})
                    self.assertEqual(
                        client.alpha.command("listCollections", filter={"name": "events"}),
                        {
                            "cursor": {
                                "id": 0,
                                "ns": "alpha.$cmd.listCollections",
                                "firstBatch": [
                                    {
                                        "name": "events",
                                        "type": "collection",
                                        "options": {"capped": True},
                                        "info": {"readOnly": False},
                                    }
                                ],
                            },
                            "ok": 1.0,
                        },
                    )
                    self.assertEqual(
                        client.alpha.command({"dropDatabase": 1}),
                        {"dropped": "alpha", "ok": 1.0},
                    )
                    self.assertNotIn("alpha", client.list_database_names())

    def test_database_command_supports_collection_index_count_and_distinct_commands(self):
        for engine_name, factory in SYNC_ENGINE_FACTORIES.items():
            with self.subTest(engine=engine_name):
                with MongoClient(factory()) as client:
                    self.assertEqual(
                        client.alpha.command({"create": "events", "capped": True}),
                        {"ok": 1.0},
                    )
                    client.alpha.events.insert_many(
                        [
                            {"_id": "1", "kind": "view", "tag": "python"},
                            {"_id": "2", "kind": "view", "tag": "mongodb"},
                        ]
                    )
                    self.assertEqual(
                        client.alpha.command(
                            {
                                "createIndexes": "events",
                                "indexes": [
                                    {
                                        "key": {"kind": 1},
                                        "name": "kind_idx",
                                    }
                                ],
                            }
                        ),
                        {
                            "numIndexesBefore": 1,
                            "numIndexesAfter": 2,
                            "createdCollectionAutomatically": False,
                            "ok": 1.0,
                        },
                    )
                    self.assertEqual(
                        client.alpha.command({"count": "events", "query": {"kind": "view"}}),
                        {"n": 2, "ok": 1.0},
                    )
                    self.assertEqual(
                        client.alpha.command({"distinct": "events", "key": "tag"}),
                        {"values": ["python", "mongodb"], "ok": 1.0},
                    )
                    self.assertEqual(
                        client.alpha.command({"listIndexes": "events"}),
                        {
                            "cursor": {
                                "id": 0,
                                "ns": "alpha.events",
                                "firstBatch": [
                                    {"name": "_id_", "key": {"_id": 1}, "fields": ["_id"], "unique": True},
                                    {"name": "kind_idx", "key": {"kind": 1}, "fields": ["kind"], "unique": False},
                                ],
                            },
                            "ok": 1.0,
                        },
                    )
                    self.assertEqual(
                        client.alpha.command({"dropIndexes": "events", "index": "kind_idx"}),
                        {"nIndexesWas": 2, "ok": 1.0},
                    )
                    self.assertEqual(
                        client.alpha.command({"drop": "events"}),
                        {"ns": "alpha.events", "ok": 1.0},
                    )

    def test_database_command_rejects_unsupported_commands(self):
        with MongoClient(MemoryEngine()) as client:
            with self.assertRaises(OperationFailure):
                client.alpha.command("collStats")

    def test_database_command_rejects_invalid_command_shapes(self):
        with MongoClient(MemoryEngine()) as client:
            with self.assertRaises(TypeError):
                client.alpha.command({"createIndexes": "events", "indexes": ()})  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                client.alpha.command({"distinct": "events", "key": 1})  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                client.alpha.command({"dropIndexes": "events", "index": 1.5})  # type: ignore[arg-type]

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
