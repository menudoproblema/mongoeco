import asyncio
from contextlib import asynccontextmanager
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mongoeco.api._async import _database_command_contract as command_contract
from mongoeco.api._async.database_commands import (
    AsyncDatabaseCommandService,
    BuildInfoResult,
    _LegacyAdminRoutingAdapter,
    _storage_engine_name,
    build_info_document,
    cmd_line_opts_document,
    connection_status_document,
    hello_document,
    host_info_document,
    list_commands_document,
    server_status_document,
    whats_my_uri_document,
)
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import CollectionStatsSnapshot, CollectionValidationSnapshot, DatabaseStatsSnapshot, FindAndModifyLastErrorObject, FindAndModifyCommandResult


class _FakeAdmin:
    def __init__(self):
        self._db_name = "db"
        self._engine = MemoryEngine()
        self._mongodb_dialect = MONGODB_DIALECT_70
        self.find_operation = object()
        self.aggregate_operation = object()
        self.count_operation = object()
        self.distinct_operation = object()
        self.find_and_modify_options = object()

    def _compile_command_find_operation(self, spec, *, collection_field):
        del spec, collection_field
        return "users", self.find_operation

    def _compile_command_aggregate_operation(self, spec):
        del spec
        return "users", self.aggregate_operation

    def _compile_command_count_operation(self, spec):
        del spec
        return "users", self.count_operation

    def _compile_command_distinct_operation(self, spec):
        del spec
        return "users", "name", self.distinct_operation

    async def _collection_stats(self, collection_name, *, scale, session=None):
        del session
        return CollectionStatsSnapshot(
            namespace=f"db.{collection_name}",
            count=1,
            data_size=scale,
            index_count=1,
            total_index_size=scale,
            scale=scale,
        )

    async def _database_stats(self, *, scale=1, session=None):
        del session
        return DatabaseStatsSnapshot(
            db_name="db",
            collection_count=1,
            object_count=2,
            data_size=scale,
            index_count=1,
            index_size=scale,
            scale=scale,
        )

    async def _build_collection_validation_snapshot(self, collection_name, *, scandata=False, full=False, background=None, session=None):
        del scandata, full, background, session
        return CollectionValidationSnapshot(namespace=f"db.{collection_name}", record_count=1, index_count=1, keys_per_index={"_id_": 1})

    async def _execute_find_and_modify(self, options, *, session=None):
        del options, session
        return FindAndModifyCommandResult(last_error_object=FindAndModifyLastErrorObject(count=1), value={"_id": 1})

    async def _execute_find_command(self, collection_name, operation, *, session=None):
        del collection_name, operation, session
        return {"cursor": {"firstBatch": []}}

    async def _execute_aggregate_command(self, collection_name, operation, *, session=None):
        del collection_name, operation, session
        return {"cursor": {"firstBatch": []}}

    async def _execute_count_command(self, collection_name, operation, *, session=None):
        del collection_name, operation, session
        return {"n": 1}

    async def _execute_distinct_command(self, collection_name, key, operation, *, session=None):
        del collection_name, key, operation, session
        return {"values": ["Ada"]}

    async def _execute_db_hash_command(self, collections, *, comment=None, session=None):
        del collections, comment, session
        return {"host": "local", "collections": {"users": "abc"}, "md5": "abc", "timeMillis": 1}

    async def _execute_list_indexes_command(self, collection_name, *, comment=None, session=None):
        del collection_name, comment, session
        return {"cursor": {"firstBatch": []}}

    async def _command_list_collections(self, spec, *, session=None):
        del spec, session
        return {"cursor": {"firstBatch": []}}

    async def _command_drop_database(self, *, session=None):
        del session
        return {"ok": 1.0}


class _RegistryEngine(MemoryEngine):
    @asynccontextmanager
    async def operation(self, opid: str, *, killable: bool = True):
        record = self._begin_active_operation(
            command_name="aggregate",
            operation_type="aggregate",
            namespace="db.users",
            session_id=None,
            comment=None,
            max_time_ms=None,
            metadata={"opid": opid},
            killable=killable,
        )
        record.task = None
        try:
            yield record.opid
        finally:
            self._complete_active_operation(record.opid)


class _ProfilingEngine(MemoryEngine):
    def __init__(self):
        super().__init__()
        self.profile_events = []

    def _record_profile_event(self, db_name, **kwargs):
        self.profile_events.append((db_name, kwargs))


class AsyncDatabaseCommandServiceTests(unittest.TestCase):
    def test_database_command_contract_is_shared_source_of_truth_for_runtime_metadata(self):
        list_commands = list_commands_document()
        find_document = list_commands["commands"]["find"]
        self.assertEqual(
            find_document,
            command_contract.command_help_document("find"),
        )
        self.assertEqual(
            list_commands["commands"],
            command_contract.list_commands_document_payload(
                tuple(list_commands["commands"].keys())
            ),
        )

        server_status = server_status_document(MONGODB_DIALECT_70, engine=MemoryEngine())
        mongoeco = server_status["mongoeco"]
        self.assertEqual(
            mongoeco["adminFamilies"],
            command_contract.admin_family_counts(),
        )
        self.assertEqual(
            mongoeco["wireCommandSurfaceCount"],
            command_contract.wire_command_surface_count(),
        )
        self.assertEqual(
            mongoeco["explainableCommandCount"],
            command_contract.explainable_command_count(),
        )
        self.assertEqual(
            command_contract.command_help_document("unknownCommand"),
            {
                "help": "mongoeco local support for the unknownCommand command",
                "supportsExplain": False,
                "supportsComment": False,
            },
        )
        with patch.object(
            command_contract,
            "command_supported_options",
            return_value=("comment", "maxTimeMS"),
        ), patch.object(
            command_contract,
            "command_supports_comment",
            return_value=True,
        ):
            self.assertEqual(
                command_contract.command_help_document("unknownCommand")["supportedOptions"],
                ["comment", "maxTimeMS"],
            )

    def test_helper_result_builders_cover_document_helpers_and_engine_name_detection(self):
        short_dialect = SimpleNamespace(server_version="8")
        self.assertEqual(build_info_document(short_dialect)["versionArray"], [8, 0, 0, 0])
        self.assertIn("helloOk", hello_document(short_dialect))
        self.assertEqual(connection_status_document(show_privileges=True)["authInfo"]["authenticatedUserPrivileges"], [])
        self.assertEqual(whats_my_uri_document()["you"], "127.0.0.1:0")
        self.assertEqual(cmd_line_opts_document()["parsed"]["net"]["port"], 0)
        self.assertTrue(list_commands_document()["commands"]["find"]["supportsExplain"])
        self.assertTrue(list_commands_document()["commands"]["find"]["supportsComment"])
        self.assertIn("comment", list_commands_document()["commands"]["find"]["supportedOptions"])
        self.assertIn("system", host_info_document())
        server_status = server_status_document(MONGODB_DIALECT_70, engine=MemoryEngine())
        self.assertIn("storageEngine", server_status)
        self.assertTrue(server_status["mongoeco"]["embedded"])
        self.assertIn("adminFamilies", server_status["mongoeco"])
        self.assertIn("explainableCommandCount", server_status["mongoeco"])
        self.assertIn("collation", server_status["mongoeco"])
        self.assertIn("sdam", server_status["mongoeco"])
        self.assertIn("changeStreams", server_status["mongoeco"])
        self.assertIn("engineRuntime", server_status["mongoeco"])
        self.assertGreaterEqual(server_status["mongoeco"]["adminFamilies"]["admin_read"], 1)
        self.assertGreaterEqual(server_status["mongoeco"]["explainableCommandCount"], 1)
        self.assertIn("selectedBackend", server_status["mongoeco"]["collation"])
        self.assertIn("fullSdam", server_status["mongoeco"]["sdam"])
        self.assertIn("implementation", server_status["mongoeco"]["changeStreams"])
        self.assertEqual(server_status["mongoeco"]["engineRuntime"]["planner"]["engine"], "python")
        self.assertEqual(server_status["mongoeco"]["engineRuntime"]["search"]["backend"], "python")

        sqlite_status = server_status_document(MONGODB_DIALECT_70, engine=SQLiteEngine())
        self.assertEqual(sqlite_status["storageEngine"]["name"], "sqlite")
        self.assertEqual(sqlite_status["mongoeco"]["engineRuntime"]["planner"]["engine"], "sqlite")
        self.assertIn("fts5Available", sqlite_status["mongoeco"]["engineRuntime"]["search"])
        self.assertIn("indexCacheEntries", sqlite_status["mongoeco"]["engineRuntime"]["caches"])

        class SQLiteLikeEngine:
            pass

        class CustomEngine:
            pass

        SQLiteLikeEngine.__name__ = "SQLiteTestEngine"
        CustomEngine.__name__ = "CustomEngine"
        self.assertEqual(_storage_engine_name(MemoryEngine()), "memory")
        self.assertEqual(_storage_engine_name(SQLiteLikeEngine()), "sqlite")
        self.assertEqual(_storage_engine_name(CustomEngine()), "CustomEngine")

    def test_parse_raw_command_covers_static_typed_and_delegated_variants(self):
        admin = _FakeAdmin()
        service = AsyncDatabaseCommandService(admin)

        self.assertIsInstance(service.parse_raw_command({"ping": 1}), service.StaticAdminCommand)
        self.assertIsInstance(service.parse_raw_command({"connectionStatus": 1, "showPrivileges": True}), service.ConnectionStatusCommand)
        self.assertIsInstance(service.parse_raw_command({"collStats": "users", "scale": 2}), service.CollectionStatsCommand)
        self.assertIsInstance(service.parse_raw_command({"dbStats": 1, "scale": 2}), service.DatabaseStatsCommand)
        self.assertIsInstance(service.parse_raw_command({"profile": 1, "slowms": 5}), service.ProfileCommand)
        self.assertIsInstance(service.parse_raw_command({"currentOp": 1}), service.CurrentOpCommand)
        self.assertIsInstance(service.parse_raw_command({"dbHash": 1}), service.DatabaseHashCommand)
        self.assertIsInstance(service.parse_raw_command({"killOp": 1, "op": "op-1"}), service.KillOpCommand)
        self.assertIsInstance(service.parse_raw_command({"validate": "users"}), service.ValidateCollectionCommand)
        self.assertIsInstance(service.parse_raw_command({"findAndModify": "users", "query": {}, "update": {"$set": {"a": 1}}}), service.FindAndModifyCommand)
        self.assertIsInstance(service.parse_raw_command({"find": "users"}), service.FindCommand)
        self.assertIsInstance(service.parse_raw_command({"aggregate": "users", "pipeline": []}), service.AggregateCommand)
        self.assertIsInstance(service.parse_raw_command({"count": "users"}), service.CountCommand)
        self.assertIsInstance(service.parse_raw_command({"distinct": "users", "key": "name"}), service.DistinctCommand)
        self.assertIsInstance(service.parse_raw_command({"listIndexes": "users"}), service.ListIndexesCommand)
        delegated = service.parse_raw_command({"dropDatabase": 1})
        self.assertIsInstance(delegated, service.DelegatedAdminCommand)
        self.assertFalse(delegated.route.passes_spec)

        with self.assertRaisesRegex(TypeError, "showPrivileges must be a bool"):
            service.parse_raw_command({"connectionStatus": 1, "showPrivileges": "yes"})
        with self.assertRaisesRegex(TypeError, "profile level must be an integer"):
            service.parse_raw_command({"profile": "bad"})
        with self.assertRaisesRegex(TypeError, "dbHash command value must be 1"):
            service.parse_raw_command({"dbHash": 0})
        with self.assertRaisesRegex(TypeError, "currentOp command value must be 1"):
            service.parse_raw_command({"currentOp": 0})
        with self.assertRaisesRegex(TypeError, "killOp requires a non-empty string op"):
            service.parse_raw_command({"killOp": 1, "op": ""})
        with self.assertRaisesRegex(TypeError, "collections must be a list of non-empty strings"):
            service.parse_raw_command({"dbHash": 1, "collections": ["users", ""]})
        with self.assertRaisesRegex(TypeError, "comment must be a string"):
            service.parse_raw_command({"dbHash": 1, "comment": 1})
        with self.assertRaisesRegex(TypeError, "killOp command value must be 1"):
            service.parse_raw_command({"killOp": 0, "op": "op-1"})
        with self.assertRaisesRegex(TypeError, "slowms must be an integer"):
            service.parse_raw_command({"profile": 1, "slowms": "bad"})
        with self.assertRaisesRegex(TypeError, "command name must be a string"):
            service.parse_raw_command({1: "bad"})  # type: ignore[arg-type]
        with self.assertRaisesRegex(OperationFailure, "Unsupported command"):
            service.parse_raw_command({"unsupported": 1})

    def test_internal_dispatch_guards_cover_unexpected_static_and_command_types(self):
        admin = _FakeAdmin()
        service = AsyncDatabaseCommandService(admin)

        with self.assertRaisesRegex(AssertionError, "Unexpected static admin command"):
            service._execute_static(
                service.StaticAdminCommand(db_name="db", command_name="weird", spec={"weird": 1})
            )

        async def _run():
            with self.assertRaisesRegex(AssertionError, "Unexpected admin command type"):
                await service.execute(
                    service.AdminCommand(db_name="db", command_name="weird", spec={"weird": 1})
                )

        asyncio.run(_run())

    def test_current_op_and_kill_op_use_local_active_operation_registry(self):
        admin = _FakeAdmin()
        admin._engine = _RegistryEngine()
        service = AsyncDatabaseCommandService(admin)

        async def _run() -> None:
            async with admin._engine.operation("op-1") as operation_id:
                current = await service.execute_document({"currentOp": 1})
                self.assertEqual(len(current["inprog"]), 1)
                self.assertEqual(current["inprog"][0]["opid"], operation_id)
                self.assertEqual(current["inprog"][0]["command"], "aggregate")

                killed = await service.execute_document({"killOp": 1, "op": operation_id})
                self.assertEqual(killed["numKilled"], 1)

            current = await service.execute_document({"currentOp": 1})
            self.assertEqual(current["inprog"], [])

        asyncio.run(_run())

    def test_legacy_admin_routing_adapter_delegates_find_and_modify_calls(self):
        admin = _FakeAdmin()
        adapter = _LegacyAdminRoutingAdapter(admin)

        async def _run():
            result = await adapter.execute_find_and_modify("opts", session="session-token")
            self.assertEqual(result.value, {"_id": 1})

        asyncio.run(_run())

    def test_legacy_admin_routing_adapter_covers_command_passthrough_and_getattr(self):
        admin = _FakeAdmin()
        adapter = _LegacyAdminRoutingAdapter(admin)

        async def _run():
            self.assertEqual(
                await adapter.execute_find_command("users", admin.find_operation, session="session-token"),
                {"cursor": {"firstBatch": []}},
            )
            self.assertEqual(
                await adapter.execute_aggregate_command("users", admin.aggregate_operation, session="session-token"),
                {"cursor": {"firstBatch": []}},
            )
            self.assertEqual(
                await adapter.execute_count_command("users", admin.count_operation, session="session-token"),
                {"n": 1},
            )
            self.assertEqual(
                await adapter.execute_db_hash_command(("users",), comment="trace", session="session-token"),
                {"host": "local", "collections": {"users": "abc"}, "md5": "abc", "timeMillis": 1},
            )
            self.assertEqual(
                await adapter.execute_distinct_command("users", "name", admin.distinct_operation, session="session-token"),
                {"values": ["Ada"]},
            )
            self.assertEqual(
                await adapter.execute_list_indexes_command("users", comment="trace", session="session-token"),
                {"cursor": {"firstBatch": []}},
            )
            self.assertIs(adapter.find_operation, admin.find_operation)

        asyncio.run(_run())

    def test_execute_and_execute_document_cover_dispatch_and_profiling(self):
        admin = _FakeAdmin()
        admin._engine = _ProfilingEngine()
        service = AsyncDatabaseCommandService(admin)

        async def _run():
            static_result = await service.execute(service.parse_raw_command({"buildInfo": 1}))
            self.assertIsInstance(static_result, BuildInfoResult)
            connection_status = await service.execute(
                service.parse_raw_command({"connectionStatus": 1, "showPrivileges": True})
            )
            self.assertEqual(
                connection_status.to_document()["authInfo"]["authenticatedUserPrivileges"],
                [],
            )
            self.assertEqual((await service.execute(service.parse_raw_command({"find": "users"})))["cursor"]["firstBatch"], [])
            self.assertEqual((await service.execute(service.parse_raw_command({"aggregate": "users", "pipeline": []})))["cursor"]["firstBatch"], [])
            self.assertEqual((await service.execute(service.parse_raw_command({"count": "users"})))["n"], 1)
            self.assertEqual((await service.execute(service.parse_raw_command({"dbHash": 1})))["md5"], "abc")
            self.assertEqual((await service.execute(service.parse_raw_command({"distinct": "users", "key": "name"})))["values"], ["Ada"])
            self.assertEqual((await service.execute(service.parse_raw_command({"listIndexes": "users"})))["cursor"]["firstBatch"], [])
            self.assertEqual((await service.execute(service.parse_raw_command({"dropDatabase": 1})))["ok"], 1.0)

            serialized = await service.execute_document({"ping": 1})
            self.assertEqual(serialized, {"ok": 1.0})
            self.assertEqual(admin._engine.profile_events[-1][1]["command"], {"ping": 1})

            with self.assertRaisesRegex(TypeError, "Unsupported admin command result type"):
                service.serialize_result(object())

            original_execute = service.execute

            async def _boom(command, *, session=None):
                del command, session
                raise RuntimeError("boom")

            service.execute = _boom  # type: ignore[method-assign]
            try:
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    await service.execute_document({"find": "users"})
            finally:
                service.execute = original_execute  # type: ignore[method-assign]

            self.assertEqual(admin._engine.profile_events[-1][1]["errmsg"], "boom")

        asyncio.run(_run())
