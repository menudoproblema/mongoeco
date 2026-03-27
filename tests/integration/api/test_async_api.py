import asyncio
import datetime
import decimal
import re
import threading
import unittest
import uuid

from mongoeco import (
    AsyncMongoClient,
    CodecOptions,
    ClientSession,
    DeleteMany,
    DeleteOne,
    IndexModel,
    InsertOne,
    MongoClient,
    MongoDialect80,
    ObjectId,
    PyMongoProfile413,
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
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api._async.cursor import AsyncCursor
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, CollectionInvalid, DuplicateKeyError, InvalidOperation, OperationFailure
from tests.support import ENGINE_FACTORIES, open_client


class AsyncApiIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_propagates_dialect_and_profile_to_database_and_collection(self):
        engine = MemoryEngine()

        async with AsyncMongoClient(
            engine,
            mongodb_dialect='8.0',
            pymongo_profile='4.13',
        ) as client:
            database = client.get_database('alpha')
            collection = database.get_collection('users')

            self.assertEqual(client.mongodb_dialect, MongoDialect80())
            self.assertEqual(client.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(client.pymongo_profile, PyMongoProfile413())
            self.assertEqual(client.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(database.mongodb_dialect, MongoDialect80())
            self.assertEqual(database.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(database.pymongo_profile, PyMongoProfile413())
            self.assertEqual(database.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(collection.mongodb_dialect, MongoDialect80())
            self.assertEqual(collection.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(collection.pymongo_profile, PyMongoProfile413())
            self.assertEqual(collection.pymongo_profile_resolution.resolution_mode, 'explicit-alias')

    async def test_client_supports_attribute_and_item_access(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client["alpha"]["users"].insert_one({"_id": "1", "name": "Ada"})

                    found = await client.alpha.users.find_one({"_id": "1"})
                    collections = await client["alpha"].list_collection_names()

                    self.assertEqual(found, {"_id": "1", "name": "Ada"})
                    self.assertEqual(collections, ["users"])

    async def test_client_server_info_reflects_target_dialect(self):
        async with AsyncMongoClient(MemoryEngine(), mongodb_dialect="8.0") as client:
            server_info = await client.server_info()

            self.assertEqual(server_info["version"], "8.0.0")
            self.assertEqual(server_info["versionArray"], [8, 0, 0, 0])
            self.assertEqual(server_info["gitVersion"], "mongoeco")
            self.assertEqual(server_info["ok"], 1.0)

    async def test_create_collection_registers_empty_namespace_and_rejects_duplicates(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    created = await client.alpha.create_collection("events")

                    self.assertEqual(created.name, "events")
                    self.assertEqual(await client.alpha.list_collection_names(), ["events"])
                    self.assertIn("alpha", await client.list_database_names())

                    with self.assertRaises(CollectionInvalid):
                        await client.alpha.create_collection("events")

    async def test_list_collections_returns_cursor_documents_and_supports_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.create_collection("events")
                    await client.alpha.create_collection("logs")

                    cursor = client.alpha.list_collections({"name": "events"})

                    self.assertEqual(
                        await cursor.to_list(),
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
                        await client.alpha.list_collection_names({"name": "logs"}),
                        ["logs"],
                    )

    async def test_collection_options_round_trip_through_admin_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = await client.alpha.create_collection(
                        "events",
                        capped=True,
                        size=1024,
                    )

                    self.assertEqual(
                        await collection.options(),
                        {"capped": True, "size": 1024},
                    )
                    self.assertEqual(
                        await client.alpha.list_collections({"name": "events"}).to_list(),
                        [
                            {
                                "name": "events",
                                "type": "collection",
                                "options": {"capped": True, "size": 1024},
                                "info": {"readOnly": False},
                            }
                        ],
                    )

    async def test_client_database_and_collection_expose_configured_pymongo_options(self):
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

        async with AsyncMongoClient(
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

    async def test_with_options_clones_database_and_collection_without_mutating_parent(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
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
            self.assertEqual(
                tuned_collection.read_preference,
                ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
            )
            self.assertEqual(
                tuned_collection.codec_options,
                CodecOptions(dict, tz_aware=True),
            )

    async def test_client_with_options_clones_client_without_mutating_parent(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            tuned = client.with_options(
                write_concern=WriteConcern(1),
                read_concern=ReadConcern("local"),
            )

            self.assertEqual(client.write_concern, WriteConcern())
            self.assertEqual(client.read_concern, ReadConcern())
            self.assertEqual(tuned.write_concern, WriteConcern(1))
            self.assertEqual(tuned.read_concern, ReadConcern("local"))

    async def test_start_session_inherits_default_transaction_options_from_client(self):
        transaction_options = TransactionOptions(
            write_concern=WriteConcern("majority"),
            max_commit_time_ms=200,
        )

        async with AsyncMongoClient(
            MemoryEngine(),
            transaction_options=transaction_options,
        ) as client:
            session = client.start_session()
            session.start_transaction()

            self.assertEqual(session.default_transaction_options, transaction_options)
            self.assertEqual(session.transaction_options, transaction_options)

    async def test_list_collections_rejects_invalid_filter(self):
        async with open_client("memory") as client:
            with self.assertRaises(TypeError):
                client.alpha.list_collections(["bad"])  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                await client.alpha.list_collection_names(["bad"])  # type: ignore[arg-type]

    async def test_database_admin_methods_respect_session_bound_sqlite_transactions(self):
        async with AsyncMongoClient(SQLiteEngine()) as client:
            session = client.start_session()
            session.start_transaction()

            await client.alpha.create_collection("events", session=session)

            self.assertEqual(await client.list_database_names(session=session), ["alpha"])
            self.assertEqual(await client.alpha.list_collection_names(session=session), ["events"])

            session.abort_transaction()

            self.assertEqual(await client.list_database_names(), [])
            self.assertEqual(await client.alpha.list_collection_names(), [])

    async def test_database_command_supports_ping_list_collections_and_drop_database(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.create_collection("events", capped=True)

                    self.assertEqual(await client.alpha.command("ping"), {"ok": 1.0})
                    self.assertEqual(
                        await client.alpha.command("listCollections", filter={"name": "events"}),
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
                        await client.alpha.command({"dropDatabase": 1}),
                        {"dropped": "alpha", "ok": 1.0},
                    )
                    self.assertNotIn("alpha", await client.list_database_names())

    async def test_database_command_supports_collection_index_count_and_distinct_commands(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    self.assertEqual(
                        await client.alpha.command({"create": "events", "capped": True}),
                        {"ok": 1.0},
                    )
                    await client.alpha.events.insert_many(
                        [
                            {"_id": "1", "kind": "view", "tag": "python"},
                            {"_id": "2", "kind": "view", "tag": "mongodb"},
                        ]
                    )
                    self.assertEqual(
                        await client.alpha.command(
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
                        await client.alpha.command({"count": "events", "query": {"kind": "view"}}),
                        {"n": 2, "ok": 1.0},
                    )
                    self.assertEqual(
                        await client.alpha.command({"distinct": "events", "key": "tag"}),
                        {"values": ["python", "mongodb"], "ok": 1.0},
                    )
                    self.assertEqual(
                        await client.alpha.command({"listIndexes": "events"}),
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
                        await client.alpha.command({"dropIndexes": "events", "index": "kind_idx"}),
                        {"nIndexesWas": 2, "ok": 1.0},
                    )
                    self.assertEqual(
                        await client.alpha.command({"drop": "events"}),
                        {"ns": "alpha.events", "ok": 1.0},
                    )

    async def test_database_command_supports_coll_stats_and_db_stats(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.events.insert_one({"_id": "1", "kind": "view"})
                    await client.alpha.events.create_index([("kind", 1)], name="kind_idx")

                    coll_stats = await client.alpha.command({"collStats": "events"})
                    db_stats = await client.alpha.command("dbStats")

                    self.assertEqual(coll_stats["ns"], "alpha.events")
                    self.assertEqual(coll_stats["count"], 1)
                    self.assertEqual(coll_stats["nindexes"], 2)
                    self.assertGreater(coll_stats["size"], 0)
                    self.assertEqual(coll_stats["storageSize"], coll_stats["size"])
                    self.assertEqual(coll_stats["ok"], 1.0)
                    self.assertEqual(db_stats["db"], "alpha")
                    self.assertEqual(db_stats["collections"], 1)
                    self.assertEqual(db_stats["objects"], 1)
                    self.assertEqual(db_stats["indexes"], 2)
                    self.assertEqual(db_stats["storageSize"], db_stats["dataSize"])
                    self.assertEqual(db_stats["ok"], 1.0)

    async def test_database_command_rejects_unsupported_commands(self):
        async with open_client("memory") as client:
            with self.assertRaises(OperationFailure):
                await client.alpha.command("serverStatus")

    async def test_database_command_rejects_invalid_command_shapes(self):
        async with open_client("memory") as client:
            with self.assertRaises(TypeError):
                await client.alpha.command({"createIndexes": "events", "indexes": ()})  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                await client.alpha.command({"distinct": "events", "key": 1})  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                await client.alpha.command({"dropIndexes": "events", "index": 1.5})  # type: ignore[arg-type]

    async def test_validate_collection_returns_metadata_and_rejects_missing_namespace(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.events.insert_one({"_id": "1", "kind": "view"})
                    await client.alpha.events.create_index([("kind", 1)], name="kind_idx")

                    validated = await client.alpha.validate_collection("events")
                    validated_from_command = await client.alpha.command({"validate": "events"})

                    self.assertEqual(validated["ns"], "alpha.events")
                    self.assertEqual(validated["nrecords"], 1)
                    self.assertEqual(validated["nIndexes"], 2)
                    self.assertEqual(validated["keysPerIndex"], {"_id_": 1, "kind_idx": 1})
                    self.assertTrue(validated["valid"])
                    self.assertEqual(validated_from_command, validated)

        async with open_client("memory") as client:
            with self.assertRaises(CollectionInvalid):
                await client.alpha.validate_collection("missing")

    async def test_collection_rename_moves_documents_and_indexes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.alpha.events
                    await collection.insert_one({"_id": "1", "kind": "view"})
                    await collection.create_index([("kind", 1)], name="kind_idx")

                    renamed = await collection.rename("archived")

                    self.assertEqual(renamed.name, "archived")
                    self.assertEqual(await client.alpha.list_collection_names(), ["archived"])
                    self.assertEqual(await renamed.find_one({"_id": "1"}), {"_id": "1", "kind": "view"})
                    self.assertIn("kind_idx", await renamed.index_information())
                    self.assertEqual(await renamed.options(), {})

    async def test_collection_rename_preserves_options_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = await client.alpha.create_collection(
                        "events",
                        capped=True,
                        size=512,
                    )

                    renamed = await collection.rename("archived")

                    self.assertEqual(
                        await renamed.options(),
                        {"capped": True, "size": 512},
                    )

    async def test_collection_rename_rejects_conflicting_or_identical_names(self):
        async with open_client("memory") as client:
            await client.alpha.events.insert_one({"_id": "1"})
            await client.alpha.logs.insert_one({"_id": "2"})

            with self.assertRaises(CollectionInvalid):
                await client.alpha.events.rename("logs")
            with self.assertRaises(CollectionInvalid):
                await client.alpha.events.rename("events")

    async def test_find_one_without_filter_returns_first_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    found = await collection.find_one()

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_find_supports_implicit_regex_literals_and_in_regex(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.users.insert_many(
                        [
                            {"_id": "1", "name": "MongoDB", "tags": ["beta", "stable"]},
                            {"_id": "2", "name": "Postgres", "tags": ["alpha", "stable"]},
                        ]
                    )

                    implicit = await client.alpha.users.find(
                        {"name": re.compile("^mongo", re.IGNORECASE)},
                        sort=[("_id", 1)],
                    ).to_list()
                    in_regex = await client.alpha.users.find(
                        {"tags": {"$in": [re.compile("^be"), re.compile("^zz")]}},
                        sort=[("_id", 1)],
                    ).to_list()

                    self.assertEqual([document["_id"] for document in implicit], ["1"])
                    self.assertEqual([document["_id"] for document in in_regex], ["1"])

    async def test_update_one_sort_is_profile_gated(self):
        engine = MemoryEngine()

        async with AsyncMongoClient(engine, pymongo_profile='4.9') as client:
            collection = client.test.users
            await collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
            await collection.insert_one({"_id": "2", "kind": "view", "rank": 1})

            with self.assertRaises(TypeError):
                await collection.update_one(
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                )

    async def test_update_one_sort_updates_first_sorted_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                engine = ENGINE_FACTORIES[engine_name]()
                async with AsyncMongoClient(engine, pymongo_profile='4.11') as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2, "done": False})
                    await collection.insert_one({"_id": "2", "kind": "view", "rank": 1, "done": False})

                    result = await collection.update_one(
                        {"kind": "view"},
                        {"$set": {"done": True}},
                        sort=[("rank", 1)],
                    )
                    first = await collection.find_one({"_id": "1"})
                    second = await collection.find_one({"_id": "2"})

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(result.modified_count, 1)
                    self.assertFalse(first["done"])
                    self.assertTrue(second["done"])

    async def test_insert_one_generates_id_and_persists_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    payload = {
                        "name": "Ada",
                        "created_at": datetime.datetime(2026, 3, 23, 10, 0, 0),
                        "owner_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                    }

                    result = await collection.insert_one(payload)
                    found = await collection.find_one({"_id": result.inserted_id})

                    self.assertTrue(result.inserted_id)
                    self.assertEqual(found["_id"], result.inserted_id)
                    self.assertEqual(found["name"], "Ada")
                    self.assertEqual(found["created_at"], payload["created_at"])
                    self.assertEqual(found["owner_id"], payload["owner_id"])
                    self.assertEqual(payload["_id"], result.inserted_id)

    async def test_insert_one_duplicate_id_raises_duplicate_key_error(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "same", "name": "Ada"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "same", "name": "Grace"})

    async def test_insert_many_returns_ids_and_persists_documents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    result = await collection.insert_many(
                        [{"name": "Ada"}, {"_id": "grace", "name": "Grace"}]
                    )
                    documents = await collection.find({}, sort=[("name", 1)]).to_list()

                    self.assertEqual(len(result.inserted_ids), 2)
                    self.assertEqual(result.inserted_ids[1], "grace")
                    self.assertEqual([document["name"] for document in documents], ["Ada", "Grace"])
                    self.assertTrue(result.inserted_ids[0])

    async def test_bulk_write_supports_ordered_and_unordered_execution(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(ENGINE_FACTORIES[engine_name](), pymongo_profile="4.11") as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "seed", "kind": "view", "rank": 2, "done": False})
                    await collection.insert_one({"_id": "other", "kind": "view", "rank": 1, "done": False})

                    success = await collection.bulk_write(
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
                        await collection.find({}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "new", "kind": "click", "done": True}],
                    )

                async with AsyncMongoClient(ENGINE_FACTORIES[engine_name]()) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "dup", "done": False})

                    with self.assertRaises(BulkWriteError) as ctx:
                        await collection.bulk_write(
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
                    self.assertEqual(await collection.find({}).to_list(), [])

    async def test_update_many_updates_all_matching_documents_and_supports_upsert(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view", "done": False})
                    await collection.insert_one({"_id": "2", "kind": "view", "done": False})
                    await collection.insert_one({"_id": "3", "kind": "click", "done": False})

                    result = await collection.update_many(
                        {"kind": "view"},
                        {"$set": {"done": True}},
                    )
                    upserted = await collection.update_many(
                        {"kind": "missing", "tenant": "a"},
                        {"$set": {"done": True}},
                        upsert=True,
                    )
                    documents = await collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(result.matched_count, 2)
                    self.assertEqual(result.modified_count, 2)
                    self.assertEqual(upserted.matched_count, 0)
                    self.assertEqual(upserted.modified_count, 0)
                    self.assertTrue(upserted.upserted_id)
                    self.assertEqual(
                        [(doc["_id"], doc["done"]) for doc in documents if doc["kind"] != "missing"],
                        [("1", True), ("2", True), ("3", False)],
                    )
                    upserted_document = await collection.find_one({"_id": upserted.upserted_id})
                    self.assertEqual(
                        upserted_document,
                        {"_id": upserted.upserted_id, "kind": "missing", "tenant": "a", "done": True},
                    )

    async def test_update_operators_min_max_mul_and_rename_are_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "score": 10, "rank": 2, "profile": {"name": "Ada"}},
                            {"_id": "2", "score": 4, "rank": 1},
                        ]
                    )

                    min_result = await collection.update_one({"_id": "1"}, {"$min": {"score": 7}})
                    max_result = await collection.update_one({"_id": "2"}, {"$max": {"score": 8}})
                    mul_result = await collection.update_many({}, {"$mul": {"score": 2}})
                    renamed = await collection.find_one_and_update(
                        {"_id": "1"},
                        {"$rename": {"profile.name": "profile.alias"}},
                        return_document=ReturnDocument.AFTER,
                    )
                    bulk = await collection.bulk_write(
                        [
                            UpdateOne({"_id": "1"}, {"$max": {"rank": 5}}),
                            UpdateOne({"_id": "2"}, {"$rename": {"score": "points"}}),
                        ]
                    )
                    first = await collection.find_one({"_id": "1"})
                    second = await collection.find_one({"_id": "2"})

                    self.assertEqual(min_result.modified_count, 1)
                    self.assertEqual(max_result.modified_count, 1)
                    self.assertEqual(mul_result.modified_count, 2)
                    self.assertEqual(renamed["profile"], {"alias": "Ada"})
                    self.assertEqual(bulk.modified_count, 2)
                    self.assertEqual(first["score"], 14)
                    self.assertEqual(first["rank"], 5)
                    self.assertEqual(second, {"_id": "2", "rank": 1, "points": 16})

    async def test_current_date_and_set_on_insert_are_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "name": "Ada"})

                    updated = await collection.update_one(
                        {"_id": "1"},
                        {"$currentDate": {"updated_at": True}},
                    )
                    inserted = await collection.update_one(
                        {"_id": "2"},
                        {
                            "$set": {"name": "Grace"},
                            "$setOnInsert": {"created_at": "seeded"},
                        },
                        upsert=True,
                    )
                    existing = await collection.update_one(
                        {"_id": "1"},
                        {"$setOnInsert": {"created_at": "ignored"}},
                    )
                    first = await collection.find_one({"_id": "1"})
                    second = await collection.find_one({"_id": "2"})

                    self.assertEqual(updated.modified_count, 1)
                    self.assertEqual(inserted.upserted_id, "2")
                    self.assertEqual(existing.modified_count, 0)
                    self.assertIsInstance(first["updated_at"], datetime.datetime)
                    self.assertNotIn("created_at", first)
                    self.assertEqual(second["created_at"], "seeded")

    async def test_array_filters_and_all_positional_updates_are_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "items": [{"qty": 1}, {"qty": 3}]})

                    update_one_result = await collection.update_one(
                        {"_id": "1"},
                        {"$inc": {"items.$[high].qty": 2}},
                        array_filters=[{"high.qty": {"$gte": 2}}],
                    )
                    update_many_result = await collection.update_many(
                        {"_id": "1"},
                        {"$set": {"items.$[].flag": True}},
                    )
                    bulk_result = await collection.bulk_write(
                        [
                            UpdateOne(
                                {"_id": "1"},
                                {"$max": {"items.$[high].qty": 10}},
                                array_filters=[{"high.qty": {"$gte": 5}}],
                            )
                        ]
                    )
                    updated = await collection.find_one_and_update(
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

    async def test_legacy_positional_operator_is_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "items": [{"qty": 1}, {"qty": 3}, {"qty": 4}]})

                    result = await collection.update_one(
                        {"items.qty": {"$gte": 3}},
                        {"$inc": {"items.$.qty": 2}},
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(updated["items"], [{"qty": 1}, {"qty": 5}, {"qty": 4}])

    async def test_bit_update_operator_is_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "score": 13, "flags": [1, 3, 4]})

                    update_one_result = await collection.update_one(
                        {"_id": "1"},
                        {"$bit": {"score": {"and": 10}}},
                    )
                    update_many_result = await collection.update_many(
                        {"_id": "1"},
                        {"$bit": {"flags.$[flag]": {"xor": 2}}},
                        array_filters=[{"flag": {"$gte": 3}}],
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(update_many_result.modified_count, 1)
                    self.assertEqual(updated, {"_id": "1", "score": 8, "flags": [1, 1, 6]})

    async def test_pull_all_update_operator_is_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "tags": ["python", "async", "python"], "nums": [1, 2, 3, 2]})

                    update_one_result = await collection.update_one(
                        {"_id": "1"},
                        {"$pullAll": {"tags": ["python"], "nums": [2, 4]}},
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(updated, {"_id": "1", "tags": ["async"], "nums": [1, 3]})

    async def test_delete_many_deletes_all_matching_documents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view"})
                    await collection.insert_one({"_id": "2", "kind": "view"})
                    await collection.insert_one({"_id": "3", "kind": "click"})

                    result = await collection.delete_many({"kind": "view"})
                    remaining = await collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(result.deleted_count, 2)
                    self.assertEqual(remaining, [{"_id": "3", "kind": "click"}])

    async def test_replace_one_and_find_one_and_family(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(ENGINE_FACTORIES[engine_name](), pymongo_profile='4.11') as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2, "done": False})
                    await collection.insert_one({"_id": "2", "kind": "view", "rank": 1, "done": False})

                    replaced = await collection.replace_one(
                        {"kind": "view"},
                        {"kind": "view", "rank": 1, "done": True, "tag": "replaced"},
                        sort=[("rank", 1)],
                    )
                    before = await collection.find_one_and_update(
                        {"kind": "view"},
                        {"$set": {"done": False}},
                        sort=[("rank", 1)],
                        projection={"done": 1, "_id": 0},
                    )
                    after = await collection.find_one_and_replace(
                        {"kind": "view"},
                        {"kind": "view", "rank": 1, "done": True, "tag": "after"},
                        sort=[("rank", 1)],
                        return_document=ReturnDocument.AFTER,
                        projection={"done": 1, "tag": 1, "_id": 0},
                    )
                    deleted = await collection.find_one_and_delete(
                        {"kind": "view"},
                        sort=[("rank", -1)],
                        projection={"rank": 1, "_id": 0},
                    )
                    remaining = await collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(replaced.matched_count, 1)
                    self.assertEqual(replaced.modified_count, 1)
                    self.assertEqual(before, {"done": True})
                    self.assertEqual(after, {"done": True, "tag": "after"})
                    self.assertEqual(deleted, {"rank": 2})
                    self.assertEqual(
                        remaining,
                        [{"_id": "2", "kind": "view", "rank": 1, "done": True, "tag": "after"}],
                    )

    async def test_find_one_and_update_upsert_returns_none_or_after_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    before = await collection.find_one_and_update(
                        {"kind": "missing", "tenant": "a"},
                        {"$set": {"done": True}},
                        upsert=True,
                    )
                    after = await collection.find_one_and_update(
                        {"kind": "another", "tenant": "b"},
                        {"$set": {"done": True}},
                        upsert=True,
                        return_document=ReturnDocument.AFTER,
                        projection={"kind": 1, "tenant": 1, "done": 1, "_id": 0},
                    )

                    self.assertIsNone(before)
                    self.assertEqual(after, {"kind": "another", "tenant": "b", "done": True})

    async def test_distinct_supports_scalars_arrays_nested_paths_and_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one(
                        {"_id": "1", "kind": "view", "tags": ["a", "b"], "profile": {"city": "Madrid"}}
                    )
                    await collection.insert_one(
                        {"_id": "2", "kind": "view", "tags": ["b", "c"], "profile": {"city": "Sevilla"}}
                    )
                    await collection.insert_one(
                        {"_id": "3", "kind": "click", "tags": [], "profile": {"city": "Madrid"}}
                    )

                    tags = await collection.distinct("tags")
                    cities = await collection.distinct("profile.city", {"kind": "view"})

                    self.assertEqual(tags, ["a", "b", "c"])
                    self.assertEqual(cities, ["Madrid", "Sevilla"])

    async def test_find_supports_type_query_operator(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "value": 7},
                            {"_id": "2", "value": "7"},
                            {"_id": "3", "value": [1, "x"]},
                        ]
                    )

                    numbers = [doc["_id"] async for doc in collection.find({"value": {"$type": "number"}})]
                    arrays = [doc["_id"] async for doc in collection.find({"value": {"$type": "array"}})]
                    strings_in_arrays = [doc["_id"] async for doc in collection.find({"value": {"$type": "string"}})]

                    self.assertEqual(numbers, ["1", "3"])
                    self.assertEqual(arrays, ["3"])
                    self.assertEqual(strings_in_arrays, ["2", "3"])

    async def test_find_supports_bitwise_query_operators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "mask": 0b1010},
                            {"_id": "2", "mask": 0b0101},
                            {"_id": "3", "mask": bytes([0b00000101])},
                        ]
                    )

                    all_set = [doc["_id"] async for doc in collection.find({"mask": {"$bitsAllSet": 0b1000}})]
                    any_clear = [doc["_id"] async for doc in collection.find({"mask": {"$bitsAnyClear": [1, 3]}})]
                    binary = [doc["_id"] async for doc in collection.find({"mask": {"$bitsAllSet": bytes([0b00000101])}})]

                    self.assertEqual(all_set, ["1"])
                    self.assertEqual(any_clear, ["2", "3"])
                    self.assertEqual(binary, ["2", "3"])

    async def test_find_one_projection_supports_inclusion_and_id_exclusion(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": "user-1"}, {"name": 1, "_id": 0})

                    self.assertEqual(found, {"name": "Ada"})

    async def test_find_one_projection_supports_exclusion_with_id_included(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": "user-1"}, {"role": 0, "_id": 1})

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_find_one_supports_id_operator_filter_without_direct_lookup_crash(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    found = await collection.find_one({"_id": {"$eq": "user-1"}})

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_update_and_delete_support_id_operator_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    update_result = await collection.update_one(
                        {"_id": {"$eq": "user-1"}},
                        {"$set": {"active": True}},
                    )
                    delete_result = await collection.delete_one({"_id": {"$eq": "user-1"}})

                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(delete_result.deleted_count, 1)

    async def test_insert_one_supports_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}

                    result = await collection.insert_one({"_id": doc_id, "name": "Ada"})
                    found = await collection.find_one({"_id": {"tenant": 1, "user": 2}})

                    self.assertEqual(result.inserted_id, doc_id)
                    self.assertEqual(found, {"_id": doc_id, "name": "Ada"})

    async def test_find_one_supports_list_id_via_direct_lookup(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = [1, 2, 3]
                    await collection.insert_one({"_id": doc_id, "name": "Ada"})

                    found = await collection.find_one({"_id": [1, 2, 3]})

                    self.assertEqual(found, {"_id": doc_id, "name": "Ada"})

    async def test_count_documents_uses_core_filtering_rules(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "payload": {"kind": "view"}})
                    await collection.insert_one({"_id": "2", "payload": {"kind": "click"}})
                    await collection.insert_one({"_id": "3", "payload": {"kind": "view"}})

                    count = await collection.count_documents({"payload.kind": "view"})

                    self.assertEqual(count, 2)

    async def test_estimated_document_count_and_drop_collection_and_database(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    users = client.alpha.users
                    logs = client.alpha.logs
                    beta = client.beta.events
                    await users.insert_one({"_id": "1", "name": "Ada"})
                    await logs.insert_one({"_id": "1", "kind": "view"})
                    await beta.insert_one({"_id": "1", "kind": "beta"})

                    self.assertEqual(await users.estimated_document_count(), 1)
                    await client.alpha.drop_collection("logs")
                    self.assertEqual(await client.alpha.list_collection_names(), ["users"])

                    await users.drop()
                    self.assertEqual(await client.alpha.list_collection_names(), [])

                    await client.drop_database("beta")
                    self.assertNotIn("beta", await client.list_database_names())

                    await client.drop_database("alpha")
                    self.assertNotIn("alpha", await client.list_database_names())

    async def test_drop_operations_on_missing_targets_are_noops(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.drop_collection("missing")
                    await client.alpha.users.drop()
                    await client.drop_database("missing")
                    self.assertEqual(await client.list_database_names(), [])

    async def test_delete_last_document_keeps_collection_visible_until_drop(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.events.insert_one({"_id": "1"})
                    await client.alpha.events.delete_one({"_id": "1"})

                    self.assertEqual(await client.alpha.list_collection_names(), ["events"])

    async def test_drop_database_removes_sqlite_database_with_only_index_metadata(self):
        async with AsyncMongoClient(SQLiteEngine()) as client:
            await client.alpha.users.create_index(["email"], unique=False)

            self.assertIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), ["users"])

            await client.drop_database("alpha")

            self.assertNotIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), [])

    async def test_drop_database_removes_memory_database_with_only_index_metadata(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            await client.alpha.users.create_index(["email"], unique=False)

            self.assertIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), ["users"])

            await client.drop_database("alpha")

            self.assertNotIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), [])

    async def test_find_supports_iteration_with_sort_skip_and_limit(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 3})
                    await collection.insert_one({"_id": "2", "rank": 1})
                    await collection.insert_one({"_id": "3", "rank": 2})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {},
                            sort=[("rank", 1)],
                            skip=1,
                            limit=1,
                        )
                    ]

                    self.assertEqual(documents, [{"_id": "3", "rank": 2}])

    async def test_find_returns_async_cursor_with_to_list_and_first(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 3})
                    await collection.insert_one({"_id": "2", "rank": 1})
                    await collection.insert_one({"_id": "3", "rank": 2})

                    cursor = collection.find({})

                    self.assertIsInstance(cursor, AsyncCursor)
                    self.assertEqual(
                        await cursor.sort([("rank", 1)]).skip(1).limit(1).to_list(),
                        [{"_id": "3", "rank": 2}],
                    )
                    self.assertEqual(
                        await collection.find({}).sort([("rank", 1)]).first(),
                        {"_id": "2", "rank": 1},
                    )

    async def test_find_cursor_rejects_mutation_after_iteration_starts(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 1})
                    await collection.insert_one({"_id": "2", "rank": 2})

                    cursor = collection.find({})
                    iterator = cursor.__aiter__()

                    self.assertEqual(await iterator.__anext__(), {"_id": "1", "rank": 1})

                    with self.assertRaises(InvalidOperation):
                        cursor.limit(1)
                    with self.assertRaises(InvalidOperation):
                        cursor.skip(1)
                    with self.assertRaises(InvalidOperation):
                        cursor.sort([("rank", 1)])

    async def test_aggregate_supports_minimal_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})

                    cursor = collection.aggregate(
                        [
                            {"$match": {"kind": "view"}},
                            {"$sort": {"rank": 1}},
                            {"$project": {"payload.city": 1, "_id": 0}},
                        ]
                    )

                    self.assertIsInstance(cursor, AsyncAggregationCursor)
                    self.assertEqual(
                        await cursor.to_list(),
                        [{"payload": {"city": "Bilbao"}}, {"payload": {"city": "Sevilla"}}],
                    )
                    self.assertEqual(
                        await collection.aggregate([{"$sort": {"rank": 1}}]).first(),
                        {"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}},
                    )

    async def test_aggregate_supports_is_number_and_type_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": 7, "text": "Ada"})
                    await collection.insert_one({"_id": "2", "value": "7"})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_scalar_coercion_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "int_text": "42", "float_text": "3.5", "truthy_text": "false", "zero": 0})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_date_math_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "start": datetime.datetime(2026, 3, 25, 10, 0, 0),
                            "end": datetime.datetime(2026, 3, 27, 9, 0, 0),
                        }
                    )

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_substr_and_strlen_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "text": "é寿司A"})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_string_index_and_binary_size_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "text": "é寿司A", "blob": b"abcd"})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_regex_match_find_and_find_all(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "text": "Ada and ada"})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_numeric_math_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": 19.25, "base": 100})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_week_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "created_at": datetime.datetime(2026, 1, 1, 23, 30, 0)})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_date_string_parts_and_parsing_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "created_at": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000)})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_object_to_array_and_zip(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "doc": {"a": 1, "b": 2}, "left": ["a", "b"], "right": [1], "defaults": ["x", 0]})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_to_date_and_date_from_parts(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "millis": 1_711_361_506_789, "text": "2026-03-25T10:05:06.789Z"})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_to_object_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "oid_text": "65f0a1000000000000000000"})

                    documents = await collection.aggregate(
                        [{"$project": {"_id": 1, "oid": {"$toObjectId": "$oid_text"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "oid": ObjectId("65f0a1000000000000000000")}],
                    )

    async def test_aggregate_supports_to_decimal(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": "10.25"})

                    documents = await collection.aggregate(
                        [{"$project": {"_id": 1, "decimal": {"$toDecimal": "$value"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "decimal": decimal.Decimal("10.25")}],
                    )

    async def test_aggregate_supports_convert_set_field_and_unset_field(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": "10", "nested": {"a": 1}})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_bson_size_and_rand(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "doc": {"a": 1, "name": "Ada"}})

                    documents = await collection.aggregate(
                        [{"$project": {"_id": 1, "size": {"$bsonSize": "$doc"}, "random": {"$rand": {}}}}]
                    ).to_list()

                    self.assertEqual(documents[0]["_id"], "1")
                    self.assertEqual(documents[0]["size"], 26)
                    self.assertGreaterEqual(documents[0]["random"], 0.0)
                    self.assertLess(documents[0]["random"], 1.0)

    async def test_aggregate_supports_switch_and_bitwise_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": 5})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_async_iteration_and_validates_pipeline_type(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
                    await collection.insert_one({"_id": "2", "kind": "click", "rank": 1})

                    documents = [
                        document
                        async for document in collection.aggregate([{"$sort": {"rank": 1}}])
                    ]

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "2", "kind": "click", "rank": 1},
                            {"_id": "1", "kind": "view", "rank": 2},
                        ],
                    )
                    with self.assertRaises(TypeError):
                        collection.aggregate({})

    async def test_aggregate_supports_unwind(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tags": ["python", "mongodb"]})
                    await collection.insert_one({"_id": "2", "tags": ["sqlite"]})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_group_and_computed_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "amount": 10, "user": "ada"})
                    await collection.insert_one({"_id": "2", "kind": "view", "amount": 7, "bonus": 1, "user": "grace"})
                    await collection.insert_one({"_id": "3", "kind": "click", "amount": 3, "bonus": 2, "user": "alan"})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_lookup_and_replace_root(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "name": "Ada", "city": "Sevilla"})
                    await users.insert_one({"_id": "u2", "name": "Grace", "city": "Madrid"})
                    await events.insert_one({"_id": "1", "kind": "view", "user_id": "u1"})
                    await events.insert_one({"_id": "2", "kind": "click", "user_id": "u2"})

                    documents = await events.aggregate(
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

    async def test_aggregate_supports_array_transform_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {"_id": "1", "tags": ["a", "b", "c"], "other_tags": ["b", "d"], "numbers": [1, 2, 3, 4]}
                    )

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_lookup_with_multiple_and_missing_matches(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a"})
                    await users.insert_one({"_id": "u2", "tenant": "a"})
                    await users.insert_one({"_id": "u3"})
                    await events.insert_one({"_id": "1", "tenant": "a"})
                    await events.insert_one({"_id": "2", "tenant": "missing"})
                    await events.insert_one({"_id": "3"})

                    documents = await events.aggregate(
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

    async def test_aggregate_supports_lookup_with_let_and_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                    await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                    await events.insert_one({"_id": "1", "tenant": "a"})
                    await events.insert_one({"_id": "2", "tenant": "b"})

                    documents = await events.aggregate(
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

    async def test_aggregate_supports_lookup_with_local_foreign_and_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                    await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                    await users.insert_one({"_id": "u4", "tenant": "c", "name": "Nope"})
                    await events.insert_one({"_id": "1", "tenant": "a"})
                    await events.insert_one({"_id": "2", "tenant": "b"})

                    documents = await events.aggregate(
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

    async def test_aggregate_supports_lookup_with_missing_foreign_collection(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    await events.insert_one({"_id": "1", "tenant": "a"})

                    documents = await events.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}},
                            {"$project": {"_id": 1, "users": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "1", "users": []}])

    async def test_aggregate_supports_nested_lookup_inside_lookup_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    roles = client.analytics.roles
                    await roles.insert_one({"_id": "r1", "label": "admin"})
                    await roles.insert_one({"_id": "r2", "label": "staff"})
                    await users.insert_one({"_id": "u1", "tenant": "a", "role_id": "r1", "name": "Ada"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "role_id": "r2", "name": "Grace"})
                    await events.insert_one({"_id": "1", "tenant": "a"})

                    documents = await events.aggregate(
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

    async def test_aggregate_supports_join_operator_combinations(self):
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

        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada", "role": "admin"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace", "role": "staff"})
                    await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus", "role": "owner"})
                    await events.insert_one({"_id": "e1", "tenant": "a", "user_id": "u1", "kind": "view"})
                    await events.insert_one({"_id": "e2", "tenant": "b", "user_id": "u3", "kind": "click"})
                    await events.insert_one({"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"})

                    for name, pipeline in pipelines.items():
                        with self.subTest(engine=engine_name, pipeline=name):
                            documents = await events.aggregate(pipeline).to_list()
                            self.assertEqual(documents, expected[name])

    async def test_aggregate_match_supports_nested_expr_inside_and_or(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "a": 5, "b": 11})
                    await collection.insert_one({"_id": "2", "a": 5, "b": 9})
                    await collection.insert_one({"_id": "3", "a": 4, "b": 20})

                    documents = await collection.aggregate(
                        [{"$match": {"$and": [{"a": 5}, {"$expr": {"$gt": ["$b", 10]}}]}}]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "1", "a": 5, "b": 11}])

    async def test_aggregate_supports_replace_with(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "profile": {"name": "Ada"}})

                    documents = await collection.aggregate([{"$replaceWith": "$profile"}]).to_list()

                    self.assertEqual(documents, [{"name": "Ada"}])

    async def test_aggregate_let_bindings_are_visible_to_pipeline_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "name": "Ada"})

                    documents = await collection.aggregate(
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

    async def test_write_operations_support_let_through_expr_filters(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "matched": False},
                            {"_id": "2", "tenant": "b", "matched": False},
                        ]
                    )

                    result = await collection.update_many(
                        {"$expr": {"$eq": ["$tenant", "$$tenant"]}},
                        {"$set": {"matched": True}},
                        let={"tenant": "a"},
                    )

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(
                        await collection.find({}, {"_id": 1, "matched": 1}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "1", "matched": True}, {"_id": "2", "matched": False}],
                    )

    async def test_bulk_write_inherits_let_for_expr_filters(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "flag": False},
                            {"_id": "2", "tenant": "b", "flag": False},
                        ]
                    )

                    result = await collection.bulk_write(
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
                        await collection.find({}, {"_id": 1, "flag": 1}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "1", "flag": False}, {"_id": "2", "flag": True}],
                    )

    async def test_aggregate_supports_array_to_object_index_of_array_and_sort_array(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
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

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_facet_and_date_trunc(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {"_id": "1", "kind": "view", "score": 5, "created_at": datetime.datetime(2026, 3, 24, 10, 37, 52)}
                    )
                    await collection.insert_one(
                        {"_id": "2", "kind": "view", "score": 7, "created_at": datetime.datetime(2026, 3, 24, 10, 5, 0)}
                    )
                    await collection.insert_one(
                        {"_id": "3", "kind": "click", "score": 3, "created_at": datetime.datetime(2026, 3, 24, 11, 10, 0)}
                    )

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_union_with_and_union_with_inside_facet(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    archived = client.analytics.archived_events
                    await events.insert_many(
                        [
                            {"_id": "e1", "kind": "event", "rank": 2},
                            {"_id": "e2", "kind": "event", "rank": 1},
                        ]
                    )
                    await archived.insert_many(
                        [
                            {"_id": "a1", "kind": "archive", "rank": 3},
                            {"_id": "a2", "kind": "archive", "rank": 0},
                        ]
                    )

                    unioned = await events.aggregate(
                        [
                            {"$unionWith": {"coll": "archived_events", "pipeline": [{"$sort": {"rank": 1}}]}},
                            {"$project": {"_id": 1, "kind": 1, "rank": 1}},
                        ]
                    ).to_list()
                    faceted = await events.aggregate(
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
                    await events.insert_one({"_id": "e3", "kind": "archive", "rank": 0})
                    current_only = await events.aggregate(
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

    async def test_aggregate_supports_bucket(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "score": 5, "kind": "view"})
                    await collection.insert_one({"_id": "2", "score": 12, "kind": "view"})
                    await collection.insert_one({"_id": "3", "score": 17, "kind": "click"})
                    await collection.insert_one({"_id": "4", "score": 25, "kind": "view"})

                    documents = await collection.aggregate(
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

    async def test_aggregate_supports_bucket_auto_and_set_window_fields(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "rank": 1, "score": 5, "kind": "view"})
                    await collection.insert_one({"_id": "2", "tenant": "a", "rank": 2, "score": 7, "kind": "view"})
                    await collection.insert_one({"_id": "3", "tenant": "b", "rank": 1, "score": 3, "kind": "click"})
                    await collection.insert_one({"_id": "4", "tenant": "b", "rank": 2, "score": 9, "kind": "click"})

                    bucketed = await collection.aggregate(
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
                    windowed = await collection.aggregate(
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

    async def test_aggregate_supports_stddev_accumulators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 2},
                            {"_id": "2", "group": "a", "rank": 2, "score": 4},
                            {"_id": "3", "group": "a", "rank": 3, "score": 4},
                            {"_id": "4", "group": "b", "rank": 1, "score": 10},
                        ]
                    )

                    grouped = await collection.aggregate(
                        [{"$group": {"_id": "$group", "pop": {"$stdDevPop": "$score"}, "samp": {"$stdDevSamp": "$score"}}}]
                    ).to_list()
                    windowed = await collection.aggregate(
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

    async def test_aggregate_supports_string_expressions_last_and_add_to_set(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "rank": 1, "kind": "view", "label": "Ada"})
                    await collection.insert_one({"_id": "2", "tenant": "a", "rank": 2, "kind": "view", "label": "Lovelace"})
                    await collection.insert_one({"_id": "3", "tenant": "a", "rank": 3, "kind": "click", "label": "Analytical"})

                    projected = await collection.aggregate(
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
                    grouped = await collection.aggregate(
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

    async def test_aggregate_supports_split_count_and_merge_objects(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "text": "Ada Lovelace", "meta": {"x": 1}})
                    await collection.insert_one({"_id": "2", "tenant": "a", "text": "Grace Hopper", "meta": {"y": 2}})
                    await collection.insert_one({"_id": "3", "tenant": "b", "text": "Alan Turing", "meta": None})

                    projected = await collection.aggregate(
                        [
                            {"$project": {"_id": 0, "parts": {"$split": ["$text", " "]}}},
                            {"$limit": 1},
                        ]
                    ).to_list()
                    grouped = await collection.aggregate(
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

    async def test_aggregate_supports_count_and_sort_by_count(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view"})
                    await collection.insert_one({"_id": "2", "kind": "view"})
                    await collection.insert_one({"_id": "3", "kind": "click"})

                    counted = await collection.aggregate([{"$count": "total"}]).to_list()
                    sorted_counts = await collection.aggregate([{"$sortByCount": "$kind"}]).to_list()

                    self.assertEqual(counted, [{"total": 3}])
                    self.assertEqual(sorted_counts, [{"_id": "view", "count": 2}, {"_id": "click", "count": 1}])

    async def test_aggregate_supports_set_window_fields_numeric_range(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "score": 5})
                    await collection.insert_one({"_id": "2", "score": 7})
                    await collection.insert_one({"_id": "3", "score": 12})

                    documents = await collection.aggregate(
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

    async def test_aggregate_propagates_operation_failure_for_invalid_stage(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1"})

                    with self.assertRaises(OperationFailure):
                        await collection.aggregate([{"$densify": {}}]).to_list()

    async def test_sqlite_cursor_early_break_does_not_block_follow_up_writes(self):
        async with open_client("sqlite") as client:
            collection = client.analytics.events
            for i in range(250):
                await collection.insert_one({"_id": str(i), "kind": "view", "rank": i})

            seen: list[str] = []
            async for document in collection.find({"kind": "view"}, sort=[("rank", 1)]):
                seen.append(document["_id"])
                if len(seen) == 5:
                    break

            result = await collection.insert_one({"_id": "after-break", "kind": "view", "rank": 999})
            found = await collection.find_one({"_id": result.inserted_id})

            self.assertEqual(seen, ["0", "1", "2", "3", "4"])
            self.assertEqual(found, {"_id": "after-break", "kind": "view", "rank": 999})

    async def test_shared_memory_engine_is_safe_across_async_and_sync_clients(self):
        engine = MemoryEngine()
        errors: list[object] = []

        def sync_worker() -> None:
            try:
                with MongoClient(engine) as client:
                    for i in range(20):
                        doc_id = f"sync-{i}"
                        client.test.users.insert_one({"_id": doc_id, "origin": "sync", "n": i})
                        found = client.test.users.find_one({"_id": doc_id})
                        if found != {"_id": doc_id, "origin": "sync", "n": i}:
                            errors.append(("sync-mismatch", found))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        thread = threading.Thread(target=sync_worker)

        async with AsyncMongoClient(engine) as client:
            thread.start()
            for i in range(20):
                doc_id = f"async-{i}"
                await client.test.users.insert_one({"_id": doc_id, "origin": "async", "n": i})
                found = await client.test.users.find_one({"_id": doc_id})
                if found != {"_id": doc_id, "origin": "async", "n": i}:
                    errors.append(("async-mismatch", found))
            await asyncio.to_thread(thread.join)

            self.assertEqual(await client.test.users.count_documents({"origin": "sync"}), 20)
            self.assertEqual(await client.test.users.count_documents({"origin": "async"}), 20)

        self.assertEqual(errors, [])

    async def test_find_one_and_cursor_first_do_not_hang_with_multiple_matches(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
                    await collection.insert_one({"_id": "2", "kind": "view", "rank": 1})
                    await collection.insert_one({"_id": "3", "kind": "click", "rank": 3})

                    found = await asyncio.wait_for(collection.find_one({"kind": "view"}), timeout=1)
                    first = await asyncio.wait_for(
                        collection.find({"kind": "view"}).sort([("rank", 1)]).first(),
                        timeout=1,
                    )

                    self.assertEqual(found["kind"], "view")
                    self.assertEqual(first, {"_id": "2", "kind": "view", "rank": 1})

    async def test_find_supports_filter_sort_skip_limit_and_projection_together(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})
                    await collection.insert_one({"_id": "4", "kind": "view", "rank": 4, "payload": {"city": "Valencia"}})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {"kind": "view"},
                            {"payload.city": 1, "_id": 0},
                            sort=[("rank", 1)],
                            skip=1,
                            limit=1,
                        )
                    ]

                    self.assertEqual(documents, [{"payload": {"city": "Sevilla"}}])

    async def test_find_supports_array_sort_and_projection_together(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": [3, 8], "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "rank": [1, 9], "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "rank": [2, 4], "payload": {"city": "Bilbao"}})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {},
                            {"payload.city": 1, "_id": 0},
                            sort=[("rank", 1)],
                        )
                    ]

                    self.assertEqual(
                        documents,
                        [
                            {"payload": {"city": "Madrid"}},
                            {"payload": {"city": "Bilbao"}},
                            {"payload": {"city": "Sevilla"}},
                        ],
                    )

    async def test_async_client_exposes_client_session_and_accepts_it(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    self.assertIsInstance(session, ClientSession)
                    async with session:
                        result = await client.test.users.insert_one({"name": "Ada"}, session=session)
                        found = await client.test.users.find_one({"_id": result.inserted_id}, session=session)

                    self.assertFalse(session.active)
                    self.assertEqual(found["name"], "Ada")

    async def test_async_session_with_transaction_commits_and_returns_result(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()

                    async def _run(active: ClientSession) -> str:
                        await client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=active)
                        return "ok"

                    result = await session.with_transaction(_run)

                    self.assertEqual(result, "ok")
                    self.assertFalse(session.in_transaction)
                    self.assertEqual(await client.test.users.count_documents({}), 1)

    async def test_async_sqlite_session_transaction_is_isolated_and_abortable(self):
        async with AsyncMongoClient(SQLiteEngine()) as client:
            session = client.start_session()
            self.assertTrue(session.get_engine_state(next(iter(session.engine_state)))["supports_transactions"])

            session.start_transaction()
            await client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=session)

            self.assertEqual(await client.test.users.count_documents({}, session=session), 1)
            with self.assertRaises(InvalidOperation):
                await client.test.users.count_documents({})

            session.abort_transaction()

            self.assertEqual(await client.test.users.count_documents({}), 0)

    async def test_async_session_state_reflects_connected_engine(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    self.assertEqual(len(session.engine_state), 1)
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(
                        state["connected"],
                        True,
                    )

    async def test_find_with_comment_and_max_time_updates_session_state_and_explain(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view"}, session=session)

                    documents = await collection.find(
                        {"kind": "view"},
                        comment="trace-find",
                        max_time_ms=25,
                        session=session,
                    ).to_list()
                    explanation = await collection.find(
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

    async def test_write_and_index_comments_update_session_state(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view"}, session=session)

                    await collection.update_one(
                        {"_id": "1"},
                        {"$set": {"done": True}},
                        comment="trace-update",
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))
                    self.assertEqual(state["last_operation"]["operation"], "update_one")
                    self.assertEqual(state["last_operation"]["comment"], "trace-update")

                    await collection.create_index(
                        [("kind", 1)],
                        comment="trace-index",
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))
                    self.assertEqual(state["last_operation"]["operation"], "create_index")
                    self.assertEqual(state["last_operation"]["comment"], "trace-index")

    async def test_collection_can_manage_index_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    name = await collection.create_index([("payload.kind", -1)], unique=False)
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()

                    self.assertEqual(name, "payload.kind_-1")
                    self.assertEqual(
                        indexes,
                        [
                            {"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True},
                            {
                                "name": "payload.kind_-1",
                                "fields": ["payload.kind"],
                                "key": {"payload.kind": -1},
                                "unique": False,
                            },
                        ],
                    )
                    self.assertEqual(
                        info,
                        {
                            "_id_": {"key": [("_id", 1)], "unique": True},
                            "payload.kind_-1": {"key": [("payload.kind", -1)]},
                        },
                    )

    async def test_collection_accepts_mapping_key_spec_for_create_index(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    name = await collection.create_index({"email": 1, "created_at": -1})
                    info = await collection.index_information()

                    self.assertEqual(name, "email_1_created_at_-1")
                    self.assertEqual(
                        info["email_1_created_at_-1"],
                        {"key": [("email", 1), ("created_at", -1)]},
                    )

    async def test_collection_can_create_and_drop_multiple_indexes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    names = await collection.create_indexes(
                        [
                            IndexModel([("email", 1)], unique=True),
                            IndexModel([("tenant", 1), ("created_at", -1)], name="tenant_created"),
                        ]
                    )
                    await collection.drop_index("tenant_created")
                    indexes_after_drop = await collection.list_indexes().to_list()
                    await collection.drop_indexes()
                    indexes_after_drop_all = await collection.list_indexes().to_list()

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

    async def test_hint_requires_existing_index_and_is_reflected_in_explain(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "kind": "view", "rank": 2},
                            {"_id": "2", "kind": "view", "rank": 1},
                        ]
                    )
                    await collection.create_index([("kind", 1)], name="kind_idx")

                    documents = await collection.find(
                        {"kind": "view"},
                        sort=[("rank", 1)],
                        hint="kind_idx",
                    ).to_list()
                    explanation = await collection.find(
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
                        await collection.find({"kind": "view"}, hint="missing_idx").to_list()

    async def test_create_indexes_rolls_back_batch_on_failure(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    with self.assertRaises(OperationFailure):
                        await collection.create_indexes(
                            [
                                IndexModel([("email", 1)], name="idx_email"),
                                IndexModel([("email", 1)], unique=True, name="idx_email_unique"),
                            ]
                        )

                    self.assertEqual(
                        await collection.list_indexes().to_list(),
                        [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}],
                    )

    async def test_drop_index_by_spec_rejects_custom_named_index(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index([("email", 1)], name="idx_email")

                    with self.assertRaises(OperationFailure):
                        await collection.drop_index([("email", 1)])

    async def test_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["email"], unique=True)
                    await collection.insert_one({"_id": "1", "email": "a@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "2", "email": "a@example.com"})

    async def test_compound_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["tenant", "email"], unique=True)
                    await collection.insert_one({"_id": "1", "tenant": "a", "email": "x@example.com"})
                    await collection.insert_one({"_id": "2", "tenant": "b", "email": "x@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "3", "tenant": "a", "email": "x@example.com"})

    async def test_nested_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["profile.email"], unique=True)
                    await collection.insert_one({"_id": "1", "profile": {"email": "a@example.com"}})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "2", "profile": {"email": "a@example.com"}})

    async def test_delete_one_removes_first_matching_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "role": "admin"})

                    result = await collection.delete_one({"role": "admin"})

                    self.assertEqual(result.deleted_count, 1)
                    self.assertIsNone(await collection.find_one({"_id": "user-1"}))

    async def test_delete_one_without_match_returns_zero(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    result = await client.test.users.delete_one({"role": "admin"})

                    self.assertEqual(result.deleted_count, 0)

    async def test_update_and_delete_support_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    await collection.insert_one({"_id": doc_id, "name": "Ada"})

                    update_result = await collection.update_one(
                        {"_id": {"tenant": 1, "user": 2}},
                        {"$set": {"active": True}},
                    )
                    delete_result = await collection.delete_one({"_id": {"tenant": 1, "user": 2}})

                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(delete_result.deleted_count, 1)

    async def test_find_one_projection_supports_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    await collection.insert_one({"_id": doc_id, "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": {"tenant": 1, "user": 2}}, {"name": 1, "_id": 0})

                    self.assertEqual(found, {"name": "Ada"})

    async def test_client_and_database_expose_collection_and_database_names(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.users.insert_one({"_id": "1"})
                    await client.beta.events.insert_one({"_id": "2"})

                    databases = await client.list_database_names()
                    collections = await client.alpha.list_collection_names()

                    self.assertEqual(set(databases), {"alpha", "beta"})
                    self.assertEqual(collections, ["users"])

    async def test_invalid_input_types_raise_type_error(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    with self.assertRaises(TypeError):
                        await collection.insert_one([])

                    with self.assertRaises(TypeError):
                        await collection.find_one([])

                    with self.assertRaises(TypeError):
                        await collection.find_one({}, [])

                    with self.assertRaises(TypeError):
                        await collection.update_one([], {})

                    with self.assertRaises(TypeError):
                        await collection.update_one({}, [])

                    with self.assertRaises(TypeError):
                        await collection.delete_one([])

                    with self.assertRaises(TypeError):
                        await collection.count_documents([])

    async def test_update_one_rejects_invalid_update_shapes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    with self.assertRaises(ValueError):
                        await collection.update_one({}, {})

                    with self.assertRaises(ValueError):
                        await collection.update_one({}, {"name": "Ada"})

                    with self.assertRaises(TypeError):
                        await collection.update_one({}, {"$set": []})
