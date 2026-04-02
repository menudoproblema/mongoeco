import asyncio
import unittest

try:
    from pymongo import MongoClient as PyMongoClient
    from pymongo import ReturnDocument as PyMongoReturnDocument
except Exception:  # pragma: no cover
    PyMongoClient = None
    PyMongoReturnDocument = None

from mongoeco.engines.memory import MemoryEngine
from mongoeco.wire import AsyncMongoEcoProxyServer


@unittest.skipIf(PyMongoClient is None, "pymongo is required for wire proxy tests")
class WireProxyIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_proxy_supports_ping_insert_find_and_aggregate_through_pymongo(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine(), mongodb_dialect="8.0") as proxy:
            uri = proxy.address.uri

            def _exercise() -> tuple[dict, dict, list[dict]]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    ping = client.admin.command("ping")
                    collection = client.alpha.events
                    insert_result = collection.insert_one({"kind": "view", "score": 2})
                    found = collection.find_one({"_id": insert_result.inserted_id})
                    aggregated = list(
                        collection.aggregate(
                            [
                                {"$match": {"kind": "view"}},
                                {"$project": {"_id": 0, "score": {"$add": ["$score", 1]}}},
                            ],
                            allowDiskUse=True,
                        )
                    )
                    return ping, found, aggregated
                finally:
                    client.close()

            ping, found, aggregated = await asyncio.to_thread(_exercise)

            self.assertEqual(ping["ok"], 1.0)
            self.assertEqual(found["kind"], "view")
            self.assertEqual(found["score"], 2)
            self.assertEqual(aggregated, [{"score": 3}])

    async def test_proxy_supports_get_more_batches_through_pymongo_cursor(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine(), mongodb_dialect="8.0") as proxy:
            uri = proxy.address.uri

            def _exercise() -> list[int]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    collection = client.alpha.events
                    collection.insert_many(
                        [{"seq": 1}, {"seq": 2}, {"seq": 3}, {"seq": 4}]
                    )
                    return [doc["seq"] for doc in collection.find({}, sort=[("seq", 1)], batch_size=2)]
                finally:
                    client.close()

            sequences = await asyncio.to_thread(_exercise)

            self.assertEqual(sequences, [1, 2, 3, 4])

    async def test_proxy_supports_explicit_pymongo_sessions(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
            uri = proxy.address.uri

            def _exercise() -> tuple[str, int]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    collection = client.alpha.events
                    with client.start_session() as session:
                        insert_result = collection.insert_one(
                            {"kind": "session", "score": 7},
                            session=session,
                        )
                        found = collection.find_one(
                            {"_id": insert_result.inserted_id},
                            session=session,
                        )
                    return found["kind"], found["score"]
                finally:
                    client.close()

            kind, score = await asyncio.to_thread(_exercise)

            self.assertEqual(kind, "session")
            self.assertEqual(score, 7)

    async def test_proxy_supports_admin_listing_stats_and_index_surfaces_through_pymongo(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
            uri = proxy.address.uri

            def _exercise() -> tuple[list[str], list[str], dict, dict, dict[str, dict]]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    collection = client.alpha.events
                    collection.insert_many([{"kind": "view"}, {"kind": "click"}])
                    collection.create_index("kind", name="kind_idx")
                    collection_names = client.alpha.list_collection_names()
                    database_names = client.list_database_names()
                    coll_stats = client.alpha.command({"collStats": "events"})
                    db_stats = client.alpha.command({"dbStats": 1})
                    index_information = collection.index_information()
                    return collection_names, database_names, coll_stats, db_stats, index_information
                finally:
                    client.close()

            collection_names, database_names, coll_stats, db_stats, index_information = await asyncio.to_thread(_exercise)

            self.assertIn("events", collection_names)
            self.assertIn("alpha", database_names)
            self.assertEqual(coll_stats["ns"], "alpha.events")
            self.assertGreater(coll_stats["totalIndexSize"], 0)
            self.assertEqual(db_stats["db"], "alpha")
            self.assertGreater(db_stats["indexSize"], 0)
            self.assertIn("_id_", index_information)
            self.assertIn("kind_idx", index_information)

    async def test_proxy_supports_find_and_modify_and_index_drop_through_pymongo(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
            uri = proxy.address.uri

            def _exercise() -> tuple[dict, dict[str, dict]]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    collection = client.alpha.events
                    collection.insert_one({"kind": "view", "score": 1})
                    collection.create_index("kind", name="kind_idx")
                    updated = collection.find_one_and_update(
                        {"kind": "view"},
                        {"$set": {"score": 2}},
                        return_document=PyMongoReturnDocument.AFTER,
                    )
                    collection.drop_index("kind_idx")
                    return updated, collection.index_information()
                finally:
                    client.close()

            updated, index_information = await asyncio.to_thread(_exercise)

            self.assertEqual(updated["kind"], "view")
            self.assertEqual(updated["score"], 2)
            self.assertEqual(set(index_information), {"_id_"})

    async def test_proxy_supports_count_distinct_validate_and_explain_through_pymongo_commands(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
            uri = proxy.address.uri

            def _exercise() -> tuple[dict, dict, dict, dict]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    collection = client.alpha.events
                    collection.insert_many(
                        [
                            {"kind": "view", "tag": "python", "rank": 2, "expires_at": "soon"},
                            {"kind": "view", "tag": "mongodb", "rank": 1},
                            {"kind": "click", "tag": "python", "rank": 3},
                        ]
                    )
                    collection.create_index("kind", name="kind_idx")
                    collection.create_index("expires_at", name="expires_at_ttl", expireAfterSeconds=30)
                    counted = client.alpha.command(
                        {
                            "count": "events",
                            "query": {"kind": "view"},
                            "hint": "kind_idx",
                            "skip": 1,
                            "limit": 1,
                        }
                    )
                    distinct = client.alpha.command(
                        {
                            "distinct": "events",
                            "key": "tag",
                            "query": {"kind": "view"},
                            "hint": "kind_idx",
                        }
                    )
                    validated = client.alpha.command(
                        {
                            "validate": "events",
                            "full": True,
                            "comment": "wire validate",
                        }
                    )
                    explained = client.alpha.command(
                        {
                            "explain": {
                                "count": "events",
                                "query": {"kind": "view"},
                                "hint": "kind_idx",
                                "comment": "wire count explain",
                            },
                            "verbosity": "queryPlanner",
                        }
                    )
                    return counted, distinct, validated, explained
                finally:
                    client.close()

            counted, distinct, validated, explained = await asyncio.to_thread(_exercise)

            self.assertEqual(counted, {"n": 1, "ok": 1.0})
            self.assertEqual(distinct, {"values": ["python", "mongodb"], "ok": 1.0})
            self.assertEqual(validated["ns"], "alpha.events")
            self.assertTrue(validated["valid"])
            self.assertEqual(
                validated["warnings"],
                [
                    "validate full is accepted for compatibility but does not change local validation behavior",
                    "TTL index 'expires_at_ttl' on 'events.expires_at' has 1 document(s) with no date values; those documents will not expire under local TTL semantics",
                ],
            )
            self.assertEqual(explained["verbosity"], "queryPlanner")
            self.assertEqual(explained["command"], "count")
            self.assertEqual(explained["hint"], "kind_idx")
            self.assertEqual(explained["comment"], "wire count explain")
            self.assertEqual(explained["ok"], 1.0)

    async def test_proxy_supports_admin_introspection_and_profile_commands_through_pymongo(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine(), mongodb_dialect="8.0") as proxy:
            uri = proxy.address.uri

            def _exercise() -> tuple[dict, dict, dict, dict, dict, dict, dict, dict, dict]:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    client.alpha.users.insert_many([{"_id": "1", "name": "Ada"}, {"_id": "2", "name": "Bob"}])
                    build_info = client.alpha.command("buildInfo")
                    list_commands = client.alpha.command("listCommands")
                    connection_status = client.alpha.command({"connectionStatus": 1, "showPrivileges": True})
                    server_status = client.alpha.command("serverStatus")
                    host_info = client.alpha.command("hostInfo")
                    cmd_line_opts = client.alpha.command("getCmdLineOpts")
                    whats_my_uri = client.alpha.command("whatsmyuri")
                    db_hash = client.alpha.command({"dbHash": 1, "collections": ["users"]})
                    profile_status = client.alpha.command({"profile": -1})
                    return (
                        build_info,
                        list_commands,
                        connection_status,
                        server_status,
                        host_info,
                        cmd_line_opts,
                        whats_my_uri,
                        db_hash,
                        profile_status,
                    )
                finally:
                    client.close()

            (
                build_info,
                list_commands,
                connection_status,
                server_status,
                host_info,
                cmd_line_opts,
                whats_my_uri,
                db_hash,
                profile_status,
            ) = await asyncio.to_thread(_exercise)

            self.assertEqual(build_info["version"], "8.0.0")
            self.assertIn("find", list_commands["commands"])
            self.assertIn("validate", list_commands["commands"])
            self.assertEqual(list_commands["commands"]["find"]["adminFamily"], "admin_read")
            self.assertTrue(list_commands["commands"]["find"]["supportsWire"])
            self.assertTrue(list_commands["commands"]["find"]["supportsExplain"])
            self.assertTrue(list_commands["commands"]["find"]["supportsComment"])
            self.assertIn("comment", list_commands["commands"]["find"]["supportedOptions"])
            self.assertEqual(connection_status["authInfo"]["authenticatedUsers"], [])
            self.assertEqual(connection_status["authInfo"]["authenticatedUserPrivileges"], [])
            self.assertEqual(server_status["storageEngine"]["name"], "memory")
            self.assertTrue(server_status["mongoeco"]["embedded"])
            self.assertGreaterEqual(server_status["mongoeco"]["adminCommandSurfaceCount"], 1)
            self.assertGreaterEqual(server_status["mongoeco"]["adminFamilies"]["admin_read"], 1)
            self.assertGreaterEqual(server_status["mongoeco"]["explainableCommandCount"], 1)
            self.assertIn("collation", server_status["mongoeco"])
            self.assertIn("sdam", server_status["mongoeco"])
            self.assertIn("changeStreams", server_status["mongoeco"])
            self.assertIn("hostname", host_info["system"])
            self.assertEqual(cmd_line_opts["parsed"]["net"]["bindIp"], "127.0.0.1")
            self.assertEqual(whats_my_uri, {"you": "127.0.0.1:0", "ok": 1.0})
            self.assertEqual(list(db_hash["collections"]), ["users"])
            self.assertEqual(len(db_hash["md5"]), 32)
            self.assertIn("was", profile_status)
            self.assertIn("level", profile_status)
            self.assertIn("entryCount", profile_status)
            self.assertIn("namespaceVisible", profile_status)
            self.assertIn("trackedDatabases", profile_status)
            self.assertIn("visibleNamespaces", profile_status)
            self.assertEqual(profile_status["ok"], 1.0)

    async def test_proxy_server_status_reports_live_local_opcounters(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine(), mongodb_dialect="8.0") as proxy:
            uri = proxy.address.uri

            def _exercise() -> dict:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    collection = client.alpha.events
                    collection.insert_many(
                        [
                            {"_id": "1", "seq": 1, "kind": "view"},
                            {"_id": "2", "seq": 2, "kind": "view"},
                            {"_id": "3", "seq": 3, "kind": "view"},
                        ]
                    )
                    list(collection.find({}, sort=[("seq", 1)], batch_size=1))
                    collection.find_one({"_id": "1"})
                    collection.update_one({"_id": "1"}, {"$set": {"kind": "click"}})
                    collection.delete_one({"_id": "3"})
                    list(collection.aggregate([{"$match": {"kind": "view"}}]))
                    client.alpha.command("ping")
                    return client.alpha.command("serverStatus")
                finally:
                    client.close()

            status = await asyncio.to_thread(_exercise)

            self.assertGreaterEqual(status["opcounters"]["insert"], 1)
            self.assertGreaterEqual(status["opcounters"]["query"], 1)
            self.assertGreaterEqual(status["opcounters"]["update"], 1)
            self.assertGreaterEqual(status["opcounters"]["delete"], 1)
            self.assertGreaterEqual(status["opcounters"]["getmore"], 1)
            self.assertGreaterEqual(status["opcounters"]["command"], 2)

    async def test_proxy_surfaces_database_command_shape_errors_through_pymongo(self):
        async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
            uri = proxy.address.uri

            def _exercise() -> None:
                client = PyMongoClient(
                    uri,
                    serverSelectionTimeoutMS=3000,
                    directConnection=True,
                )
                try:
                    with self.assertRaisesRegex(Exception, "wire distinct requires a non-empty key string"):
                        client.alpha.command({"distinct": "events", "key": ""})
                    with self.assertRaisesRegex(Exception, "wire validate comment must be a string"):
                        client.alpha.command({"validate": "events", "comment": 1})
                    with self.assertRaisesRegex(Exception, "wire explain verbosity must be a string"):
                        client.alpha.command({"explain": {"find": "events"}, "verbosity": 1})
                    with self.assertRaisesRegex(Exception, "wire count skip must be a non-negative integer"):
                        client.alpha.command({"count": "events", "skip": -1})
                    with self.assertRaisesRegex(Exception, "wire count filter_spec must be a dict"):
                        client.alpha.command({"count": "events", "query": 1})
                    with self.assertRaisesRegex(Exception, "wire find projection must be a dict"):
                        client.alpha.command({"find": "events", "projection": 1})
                    with self.assertRaisesRegex(Exception, "wire aggregate allowDiskUse must be a bool"):
                        client.alpha.command({"aggregate": "events", "pipeline": [], "allowDiskUse": 1})
                    with self.assertRaisesRegex(Exception, "wire listCollections comment must be a string"):
                        client.alpha.command({"listCollections": 1, "comment": 1})
                    with self.assertRaisesRegex(Exception, "wire createIndexes each index specification must be a dict"):
                        client.alpha.command({"createIndexes": "events", "indexes": [1]})
                    with self.assertRaisesRegex(Exception, "wire dropIndexes index must be '\\*', a name, or a key specification"):
                        client.alpha.command({"dropIndexes": "events", "index": 1.5})
                    with self.assertRaisesRegex(Exception, "wire findAndModify let must be a dict"):
                        client.alpha.command(
                            {
                                "findAndModify": "events",
                                "query": {},
                                "update": {"$set": {"done": True}},
                                "let": 1,
                            }
                        )
                    with self.assertRaisesRegex(Exception, "wire connectionStatus showPrivileges must be a bool"):
                        client.alpha.command({"connectionStatus": 1, "showPrivileges": 1})
                    with self.assertRaisesRegex(Exception, "wire dbStats scale must be a positive integer"):
                        client.alpha.command({"dbStats": 1, "scale": 0})
                    with self.assertRaisesRegex(Exception, "wire dbHash collections must be a list of non-empty strings"):
                        client.alpha.command({"dbHash": 1, "collections": ["events", ""]})
                    with self.assertRaisesRegex(Exception, "wire dbHash comment must be a string"):
                        client.alpha.command({"dbHash": 1, "comment": 1})
                    with self.assertRaisesRegex(Exception, "wire collStats scale must be a positive integer"):
                        client.alpha.command({"collStats": "events", "scale": 0})
                    with self.assertRaisesRegex(Exception, "wire profile level must be an integer"):
                        client.alpha.command({"profile": "bad"})
                    with self.assertRaisesRegex(Exception, "wire profile slowms must be an integer"):
                        client.alpha.command({"profile": 2, "slowms": "bad"})
                finally:
                    client.close()

            await asyncio.to_thread(_exercise)
