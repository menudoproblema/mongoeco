import inspect
from typing import Any, Awaitable, Callable

from mongoeco.errors import BulkWriteError, CollectionInvalid, OperationFailure


OpenClient = Callable[..., Any]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def assert_client_server_info_reflects_target_dialect(case, open_client: OpenClient) -> None:
    async with open_client("memory", mongodb_dialect="8.0") as client:
        server_info = await _maybe_await(client.server_info())

        case.assertEqual(server_info["version"], "8.0.0")
        case.assertEqual(server_info["versionArray"], [8, 0, 0, 0])
        case.assertEqual(server_info["gitVersion"], "mongoeco")
        case.assertEqual(server_info["ok"], 1.0)


async def assert_build_info_command_shares_source_of_truth_with_server_info(case, open_client: OpenClient) -> None:
    async with open_client("memory", mongodb_dialect="8.0") as client:
        server_info = await _maybe_await(client.server_info())
        build_info = await _maybe_await(client.alpha.command("buildInfo"))

        case.assertEqual(build_info, server_info)


async def assert_hello_and_is_master_commands_return_handshake_metadata(case, open_client: OpenClient) -> None:
    async with open_client("memory", mongodb_dialect="8.0") as client:
        hello = await _maybe_await(client.alpha.command("hello"))
        is_master = await _maybe_await(client.alpha.command("isMaster"))

        case.assertTrue(hello["helloOk"])
        case.assertTrue(hello["isWritablePrimary"])
        case.assertEqual(hello["maxBsonObjectSize"], 16 * 1024 * 1024)
        case.assertEqual(hello["maxMessageSizeBytes"], 48_000_000)
        case.assertEqual(hello["maxWriteBatchSize"], 100_000)
        case.assertEqual(hello["logicalSessionTimeoutMinutes"], 30)
        case.assertEqual(hello["maxWireVersion"], 20)
        case.assertFalse(hello["readOnly"])
        case.assertEqual(hello["version"], "8.0.0")
        case.assertEqual(hello["ok"], 1.0)
        case.assertTrue(is_master["ismaster"])
        case.assertEqual(is_master["version"], "8.0.0")


async def assert_list_commands_and_connection_status_commands_return_local_admin_metadata(case, open_client: OpenClient) -> None:
    async with open_client("memory", mongodb_dialect="8.0") as client:
        commands = await _maybe_await(client.alpha.command("listCommands"))
        connection_status = await _maybe_await(
            client.alpha.command({"connectionStatus": 1, "showPrivileges": True})
        )

        case.assertIn("find", commands["commands"])
        case.assertIn("aggregate", commands["commands"])
        case.assertIn("explain", commands["commands"])
        case.assertEqual(commands["ok"], 1.0)
        case.assertEqual(connection_status["authInfo"]["authenticatedUsers"], [])
        case.assertEqual(connection_status["authInfo"]["authenticatedUserRoles"], [])
        case.assertEqual(connection_status["authInfo"]["authenticatedUserPrivileges"], [])
        case.assertEqual(connection_status["ok"], 1.0)


async def assert_server_status_command_returns_local_runtime_metadata(case, open_client: OpenClient) -> None:
    async with open_client("memory", mongodb_dialect="8.0") as client:
        status = await _maybe_await(client.alpha.command("serverStatus"))

        case.assertEqual(status["process"], "mongod")
        case.assertEqual(status["version"], "8.0.0")
        case.assertIsInstance(status["pid"], int)
        case.assertGreaterEqual(status["uptime"], 0)
        case.assertGreaterEqual(status["uptimeMillis"], 0)
        case.assertEqual(status["connections"]["current"], 1)
        case.assertEqual(status["storageEngine"]["name"], "memory")
        case.assertEqual(status["ok"], 1.0)


async def assert_host_info_whats_my_uri_and_cmd_line_opts_commands_return_local_metadata(case, open_client: OpenClient) -> None:
    async with open_client("memory", mongodb_dialect="8.0") as client:
        host_info = await _maybe_await(client.alpha.command("hostInfo"))
        whats_my_uri = await _maybe_await(client.alpha.command("whatsmyuri"))
        cmd_line_opts = await _maybe_await(client.alpha.command("getCmdLineOpts"))

        case.assertIn("hostname", host_info["system"])
        case.assertGreaterEqual(host_info["system"]["numCores"], 1)
        case.assertIn("pythonVersion", host_info["extra"])
        case.assertEqual(host_info["ok"], 1.0)
        case.assertEqual(whats_my_uri, {"you": "127.0.0.1:0", "ok": 1.0})
        case.assertIsInstance(cmd_line_opts["argv"], list)
        case.assertEqual(cmd_line_opts["parsed"]["net"]["bindIp"], "127.0.0.1")
        case.assertEqual(cmd_line_opts["ok"], 1.0)


async def assert_list_collections_command_supports_name_only(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(client.alpha.create_collection("events", capped=True, size=512))
                await _maybe_await(client.alpha.create_collection("logs"))

                result = await _maybe_await(
                    client.alpha.command(
                        "listCollections",
                        nameOnly=True,
                        authorizedCollections=True,
                        filter={"name": "events"},
                    )
                )

                case.assertEqual(
                    result,
                    {
                        "cursor": {
                            "id": 0,
                            "ns": "alpha.$cmd.listCollections",
                            "firstBatch": [{"name": "events", "type": "collection"}],
                        },
                        "ok": 1.0,
                    },
                )


async def assert_database_command_supports_ping_list_collections_and_drop_database(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(client.alpha.create_collection("events", capped=True))
                await _maybe_await(client.beta.create_collection("logs"))

                case.assertEqual(await _maybe_await(client.alpha.command("ping")), {"ok": 1.0})
                case.assertEqual(
                    await _maybe_await(client.alpha.command("listCollections", filter={"name": "events"})),
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
                case.assertEqual(
                    await _maybe_await(client.alpha.command("listDatabases", filter={"name": "alpha"})),
                    {
                        "databases": [{"name": "alpha", "sizeOnDisk": 0, "empty": False}],
                        "totalSize": 0,
                        "ok": 1.0,
                    },
                )
                case.assertEqual(
                    await _maybe_await(client.alpha.command("listDatabases", nameOnly=True)),
                    {
                        "databases": [{"name": "alpha"}, {"name": "beta"}],
                        "totalSize": 0,
                        "ok": 1.0,
                    },
                )
                case.assertEqual(
                    await _maybe_await(client.alpha.command({"dropDatabase": 1})),
                    {"dropped": "alpha", "ok": 1.0},
                )
                case.assertNotIn("alpha", await _maybe_await(client.list_database_names()))


async def assert_database_command_supports_collection_index_count_and_distinct_commands(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                case.assertEqual(
                    await _maybe_await(client.alpha.command({"create": "events", "capped": True})),
                    {"ok": 1.0},
                )
                await _maybe_await(
                    client.alpha.events.insert_many(
                        [
                            {"_id": "1", "kind": "view", "tag": "python"},
                            {"_id": "2", "kind": "view", "tag": "mongodb"},
                        ]
                    )
                )
                case.assertEqual(
                    await _maybe_await(
                        client.alpha.command(
                            {
                                "createIndexes": "events",
                                "indexes": [{"key": {"kind": 1}, "name": "kind_idx"}],
                            }
                        )
                    ),
                    {
                        "numIndexesBefore": 1,
                        "numIndexesAfter": 2,
                        "createdCollectionAutomatically": False,
                        "ok": 1.0,
                    },
                )
                case.assertEqual(
                    await _maybe_await(client.alpha.command({"count": "events", "query": {"kind": "view"}})),
                    {"n": 2, "ok": 1.0},
                )
                case.assertEqual(
                    await _maybe_await(client.alpha.command({"distinct": "events", "key": "tag"})),
                    {"values": ["python", "mongodb"], "ok": 1.0},
                )
                case.assertEqual(
                    await _maybe_await(client.alpha.command({"listIndexes": "events"})),
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
                case.assertEqual(
                    await _maybe_await(client.alpha.command({"dropIndexes": "events", "index": "kind_idx"})),
                    {"nIndexesWas": 2, "ok": 1.0},
                )
                case.assertEqual(
                    await _maybe_await(client.alpha.command({"drop": "events"})),
                    {"ns": "alpha.events", "ok": 1.0},
                )


async def assert_database_command_index_commands_support_comment_and_max_time(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(client.alpha.events.insert_one({"_id": "1", "kind": "view"}))

                created = await _maybe_await(
                    client.alpha.command(
                        {
                            "createIndexes": "events",
                            "indexes": [{"key": {"kind": 1}, "name": "kind_idx"}],
                            "comment": "create indexes command",
                            "maxTimeMS": 50,
                        }
                    )
                )
                listed = await _maybe_await(
                    client.alpha.command({"listIndexes": "events", "comment": "list indexes command"})
                )
                dropped = await _maybe_await(
                    client.alpha.command(
                        {"dropIndexes": "events", "index": "kind_idx", "comment": "drop indexes command"}
                    )
                )

                case.assertEqual(created["numIndexesBefore"], 1)
                case.assertEqual(created["numIndexesAfter"], 2)
                case.assertEqual(created["ok"], 1.0)
                case.assertEqual(listed["cursor"]["firstBatch"][1]["name"], "kind_idx")
                case.assertEqual(dropped, {"nIndexesWas": 2, "ok": 1.0})


async def assert_database_command_count_supports_skip_limit_hint_and_comment(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(
                    client.alpha.events.insert_many(
                        [
                            {"_id": "1", "kind": "view", "rank": 1},
                            {"_id": "2", "kind": "view", "rank": 2},
                            {"_id": "3", "kind": "view", "rank": 3},
                        ]
                    )
                )
                await _maybe_await(client.alpha.events.create_index([("kind", 1)], name="kind_idx"))

                counted = await _maybe_await(
                    client.alpha.command(
                        {
                            "count": "events",
                            "query": {"kind": "view"},
                            "skip": 1,
                            "limit": 1,
                            "hint": "kind_idx",
                            "comment": "count command",
                            "maxTimeMS": 50,
                        }
                    )
                )

                case.assertEqual(counted, {"n": 1, "ok": 1.0})


async def assert_database_command_distinct_supports_hint_comment_and_max_time(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(
                    client.alpha.events.insert_many(
                        [
                            {"_id": "1", "kind": "view", "tag": "python"},
                            {"_id": "2", "kind": "view", "tag": "mongodb"},
                            {"_id": "3", "kind": "click", "tag": "python"},
                        ]
                    )
                )
                await _maybe_await(client.alpha.events.create_index([("kind", 1)], name="kind_idx"))

                distinct = await _maybe_await(
                    client.alpha.command(
                        {
                            "distinct": "events",
                            "key": "tag",
                            "query": {"kind": "view"},
                            "hint": "kind_idx",
                            "comment": "distinct command",
                            "maxTimeMS": 50,
                        }
                    )
                )

                case.assertEqual(distinct, {"values": ["python", "mongodb"], "ok": 1.0})


async def assert_database_command_supports_rename_collection_within_current_database(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(client.alpha.events.insert_one({"_id": "1", "kind": "view"}))
                await _maybe_await(client.alpha.events.create_index([("kind", 1)], name="kind_idx"))
                await _maybe_await(client.alpha.archived.insert_one({"_id": "existing"}))

                renamed = await _maybe_await(
                    client.alpha.command(
                        {"renameCollection": "alpha.events", "to": "alpha.archived", "dropTarget": True}
                    )
                )

                case.assertEqual(renamed, {"ok": 1.0})
                case.assertEqual(await _maybe_await(client.alpha.list_collection_names()), ["archived"])
                case.assertEqual(
                    await _maybe_await(client.alpha.archived.find_one({"_id": "1"})),
                    {"_id": "1", "kind": "view"},
                )
                case.assertIn("kind_idx", await _maybe_await(client.alpha.archived.index_information()))


async def assert_database_command_supports_find_and_aggregate(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(
                    client.alpha.events.insert_many(
                        [
                            {"_id": "1", "kind": "view", "rank": 2},
                            {"_id": "2", "kind": "view", "rank": 1},
                            {"_id": "3", "kind": "click", "rank": 3},
                        ]
                    )
                )

                found = await _maybe_await(
                    client.alpha.command(
                        {
                            "find": "events",
                            "filter": {"kind": "view"},
                            "projection": {"rank": 1, "_id": 0},
                            "sort": {"rank": 1},
                            "batchSize": 1,
                        }
                    )
                )
                aggregated = await _maybe_await(
                    client.alpha.command(
                        {
                            "aggregate": "events",
                            "pipeline": [
                                {"$match": {"kind": "view"}},
                                {"$sort": {"rank": 1}},
                                {"$project": {"rank": 1, "_id": 0}},
                            ],
                            "cursor": {"batchSize": 1},
                        }
                    )
                )

                expected = {
                    "cursor": {"id": 0, "ns": "alpha.events", "firstBatch": [{"rank": 1}, {"rank": 2}]},
                    "ok": 1.0,
                }
                case.assertEqual(found, expected)
                case.assertEqual(aggregated, expected)


async def assert_database_command_supports_explain_for_find_and_aggregate(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(
                    client.alpha.events.insert_many(
                        [{"_id": "1", "kind": "view", "rank": 2}, {"_id": "2", "kind": "view", "rank": 1}]
                    )
                )
                await _maybe_await(client.alpha.events.create_index([("kind", 1)], name="kind_idx"))

                find_explain = await _maybe_await(
                    client.alpha.command(
                        {
                            "explain": {
                                "find": "events",
                                "filter": {"kind": "view"},
                                "sort": {"rank": 1},
                                "hint": "kind_idx",
                                "comment": "find explain",
                                "maxTimeMS": 50,
                                "batchSize": 1,
                            },
                            "verbosity": "queryPlanner",
                        }
                    )
                )
                aggregate_explain = await _maybe_await(
                    client.alpha.command(
                        {
                            "explain": {
                                "aggregate": "events",
                                "pipeline": [{"$match": {"kind": "view"}}],
                                "cursor": {"batchSize": 1},
                                "hint": "kind_idx",
                                "comment": "agg explain",
                                "maxTimeMS": 50,
                            }
                        }
                    )
                )

                case.assertEqual(find_explain["verbosity"], "queryPlanner")
                case.assertEqual(find_explain["hint"], "kind_idx")
                case.assertEqual(find_explain["comment"], "find explain")
                case.assertEqual(find_explain["max_time_ms"], 50)
                case.assertEqual(find_explain["batch_size"], 1)
                case.assertEqual(find_explain["ok"], 1.0)
                case.assertEqual(aggregate_explain["hint"], "kind_idx")
                case.assertEqual(aggregate_explain["comment"], "agg explain")
                case.assertEqual(aggregate_explain["max_time_ms"], 50)
                case.assertEqual(aggregate_explain["batch_size"], 1)
                case.assertEqual(aggregate_explain["ok"], 1.0)


async def assert_database_command_supports_explain_for_update_and_delete(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(
                    client.alpha.events.insert_many([{"_id": "1", "kind": "view"}, {"_id": "2", "kind": "click"}])
                )
                await _maybe_await(client.alpha.events.create_index([("kind", 1)], name="kind_idx"))

                update_explain = await _maybe_await(
                    client.alpha.command(
                        {
                            "explain": {
                                "update": "events",
                                "updates": [
                                    {"q": {"kind": "view"}, "u": {"$set": {"done": True}}, "multi": False, "hint": "kind_idx"}
                                ],
                                "comment": "update explain",
                                "maxTimeMS": 50,
                            }
                        }
                    )
                )
                delete_explain = await _maybe_await(
                    client.alpha.command(
                        {
                            "explain": {
                                "delete": "events",
                                "deletes": [{"q": {"kind": "click"}, "limit": 1, "hint": "kind_idx"}],
                                "comment": "delete explain",
                                "maxTimeMS": 50,
                            }
                        }
                    )
                )

                case.assertEqual(update_explain["command"], "update")
                case.assertFalse(update_explain["multi"])
                case.assertEqual(update_explain["hint"], "kind_idx")
                case.assertEqual(update_explain["comment"], "update explain")
                case.assertEqual(update_explain["max_time_ms"], 50)
                case.assertEqual(update_explain["ok"], 1.0)
                case.assertEqual(delete_explain["command"], "delete")
                case.assertEqual(delete_explain["limit"], 1)
                case.assertEqual(delete_explain["hint"], "kind_idx")
                case.assertEqual(delete_explain["comment"], "delete explain")
                case.assertEqual(delete_explain["max_time_ms"], 50)
                case.assertEqual(delete_explain["ok"], 1.0)


async def assert_database_command_supports_insert_update_and_delete(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                inserted = await _maybe_await(
                    client.alpha.command(
                        {
                            "insert": "events",
                            "documents": [
                                {"_id": "1", "kind": "view", "rank": 1},
                                {"_id": "2", "kind": "click", "rank": 2},
                            ],
                        }
                    )
                )
                updated = await _maybe_await(
                    client.alpha.command(
                        {
                            "update": "events",
                            "updates": [
                                {"q": {"kind": "view"}, "u": {"$set": {"done": True}}, "multi": True},
                                {"q": {"kind": "missing"}, "u": {"kind": "missing", "done": True}, "upsert": True},
                            ],
                        }
                    )
                )
                deleted = await _maybe_await(
                    client.alpha.command(
                        {
                            "delete": "events",
                            "deletes": [{"q": {"kind": "click"}, "limit": 1}, {"q": {"kind": "missing"}, "limit": 0}],
                        }
                    )
                )

                case.assertEqual(inserted, {"n": 2, "ok": 1.0})
                case.assertEqual(updated["n"], 1)
                case.assertEqual(updated["nModified"], 1)
                case.assertEqual(updated["ok"], 1.0)
                case.assertEqual(len(updated["upserted"]), 1)
                case.assertEqual(deleted, {"n": 2, "ok": 1.0})


async def assert_database_command_write_commands_surface_write_errors(case, open_client: OpenClient) -> None:
    async with open_client("memory") as client:
        await _maybe_await(client.alpha.events.insert_one({"_id": "1", "kind": "view"}))

        with case.assertRaises(BulkWriteError):
            await _maybe_await(
                client.alpha.command(
                    {
                        "insert": "events",
                        "documents": [{"_id": "1", "kind": "duplicate"}, {"_id": "2", "kind": "ok"}],
                    }
                )
            )


async def assert_database_command_supports_find_and_modify(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(
                    client.alpha.events.insert_many(
                        [
                            {"_id": "1", "kind": "view", "rank": 2, "done": False},
                            {"_id": "2", "kind": "view", "rank": 1, "done": False},
                        ]
                    )
                )

                updated = await _maybe_await(
                    client.alpha.command(
                        {
                            "findAndModify": "events",
                            "query": {"kind": "view"},
                            "sort": {"rank": 1},
                            "update": {"$set": {"done": True}},
                            "fields": {"done": 1, "_id": 0},
                            "new": True,
                        }
                    )
                )
                removed = await _maybe_await(
                    client.alpha.command(
                        {
                            "findAndModify": "events",
                            "query": {"kind": "view"},
                            "remove": True,
                            "fields": {"rank": 1, "_id": 0},
                        }
                    )
                )
                upserted = await _maybe_await(
                    client.alpha.command(
                        {
                            "findAndModify": "events",
                            "query": {"kind": "missing"},
                            "update": {"kind": "missing", "done": True},
                            "upsert": True,
                            "new": True,
                            "fields": {"kind": 1, "done": 1, "_id": 0},
                        }
                    )
                )

                case.assertEqual(updated, {"lastErrorObject": {"n": 1, "updatedExisting": True}, "value": {"done": True}, "ok": 1.0})
                case.assertEqual(removed, {"lastErrorObject": {"n": 1}, "value": {"rank": 2}, "ok": 1.0})
                case.assertEqual(upserted["lastErrorObject"]["n"], 1)
                case.assertFalse(upserted["lastErrorObject"]["updatedExisting"])
                case.assertIn("upserted", upserted["lastErrorObject"])
                case.assertEqual(upserted["value"], {"kind": "missing", "done": True})


async def assert_database_command_supports_coll_stats_and_db_stats(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(client.alpha.events.insert_one({"_id": "1", "kind": "view"}))
                await _maybe_await(client.alpha.events.create_index([("kind", 1)], name="kind_idx"))

                coll_stats = await _maybe_await(client.alpha.command({"collStats": "events"}))
                db_stats = await _maybe_await(client.alpha.command("dbStats"))
                scaled_coll_stats = await _maybe_await(client.alpha.command({"collStats": "events", "scale": 2}))
                scaled_db_stats = await _maybe_await(client.alpha.command({"dbStats": 1, "scale": 2}))

                case.assertEqual(coll_stats["ns"], "alpha.events")
                case.assertEqual(coll_stats["count"], 1)
                case.assertEqual(coll_stats["nindexes"], 2)
                case.assertGreater(coll_stats["size"], 0)
                case.assertEqual(coll_stats["storageSize"], coll_stats["size"])
                case.assertEqual(coll_stats["ok"], 1.0)
                case.assertEqual(db_stats["db"], "alpha")
                case.assertEqual(db_stats["collections"], 1)
                case.assertEqual(db_stats["objects"], 1)
                case.assertEqual(db_stats["indexes"], 2)
                case.assertEqual(db_stats["storageSize"], db_stats["dataSize"])
                case.assertEqual(db_stats["ok"], 1.0)
                case.assertLessEqual(scaled_coll_stats["size"], coll_stats["size"])
                case.assertLessEqual(scaled_coll_stats["storageSize"], coll_stats["storageSize"])
                case.assertLessEqual(scaled_db_stats["dataSize"], db_stats["dataSize"])
                case.assertLessEqual(scaled_db_stats["storageSize"], db_stats["storageSize"])


async def assert_database_command_rejects_unsupported_commands(case, open_client: OpenClient) -> None:
    async with open_client("memory") as client:
        with case.assertRaises(OperationFailure):
            await _maybe_await(client.alpha.command("top"))


async def assert_database_command_rejects_invalid_command_shapes(case, open_client: OpenClient) -> None:
    async with open_client("memory") as client:
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"createIndexes": "events", "indexes": ()}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"distinct": "events", "key": 1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"dropIndexes": "events", "index": 1.5}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command("listDatabases", nameOnly=1))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command("listCollections", nameOnly=1))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command("listCollections", authorizedCollections=1))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"count": "events", "skip": -1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"count": "events", "limit": -1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"dbStats": 1, "scale": 0}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"collStats": "events", "scale": -1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"insert": "events", "documents": {}}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"update": "events", "updates": {}}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"delete": "events", "deletes": {}}))  # type: ignore[arg-type]
        with case.assertRaises(ValueError):
            await _maybe_await(client.alpha.command({"find": "events", "batchSize": -1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"aggregate": "events", "pipeline": [], "cursor": 1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"explain": 1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"connectionStatus": 1, "showPrivileges": 1}))  # type: ignore[arg-type]
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"renameCollection": "events", "to": "alpha.logs"}))  # type: ignore[arg-type]
        with case.assertRaises(OperationFailure):
            await _maybe_await(client.alpha.command({"findAndModify": "events"}))
        with case.assertRaises(OperationFailure):
            await _maybe_await(client.alpha.command({"explain": {"count": "events"}}))
        with case.assertRaises(TypeError):
            await _maybe_await(client.alpha.command({"explain": {"update": "events", "updates": []}}))
        with case.assertRaises(OperationFailure):
            await _maybe_await(client.alpha.command({"renameCollection": "alpha.events", "to": "beta.logs"}))


async def assert_validate_collection_returns_metadata_and_rejects_missing_namespace(case, engine_names, open_client: OpenClient) -> None:
    for engine_name in engine_names:
        with case.subTest(engine=engine_name):
            async with open_client(engine_name) as client:
                await _maybe_await(client.alpha.events.insert_one({"_id": "1", "kind": "view"}))
                await _maybe_await(client.alpha.events.create_index([("kind", 1)], name="kind_idx"))

                validated = await _maybe_await(client.alpha.validate_collection("events"))
                validated_from_command = await _maybe_await(client.alpha.command({"validate": "events"}))

                case.assertEqual(validated["ns"], "alpha.events")
                case.assertEqual(validated["nrecords"], 1)
                case.assertEqual(validated["nIndexes"], 2)
                case.assertEqual(validated["keysPerIndex"], {"_id_": 1, "kind_idx": 1})
                case.assertTrue(validated["valid"])
                case.assertEqual(validated_from_command, validated)

    async with open_client("memory") as client:
        with case.assertRaises(CollectionInvalid):
            await _maybe_await(client.alpha.validate_collection("missing"))
