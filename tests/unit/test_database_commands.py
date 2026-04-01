import asyncio
import unittest
from types import SimpleNamespace

from mongoeco.api._async.database_commands import (
    AsyncDatabaseCommandService,
    BuildInfoResult,
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
        return CollectionStatsSnapshot(namespace=f"db.{collection_name}", size=scale, count=1, storage_size=scale)

    async def _database_stats(self, *, scale=1, session=None):
        del session
        return DatabaseStatsSnapshot(database_name="db", collection_count=1, object_count=2, data_size=scale, storage_size=scale, index_count=1, index_size=scale)

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

    async def _execute_list_indexes_command(self, collection_name, *, comment=None, session=None):
        del collection_name, comment, session
        return {"cursor": {"firstBatch": []}}

    async def _command_list_collections(self, spec, *, session=None):
        del spec, session
        return {"cursor": {"firstBatch": []}}

    async def _command_drop_database(self, *, session=None):
        del session
        return {"ok": 1.0}


class _ProfilingEngine(MemoryEngine):
    def __init__(self):
        super().__init__()
        self.profile_events = []

    def _record_profile_event(self, db_name, **kwargs):
        self.profile_events.append((db_name, kwargs))


class AsyncDatabaseCommandServiceTests(unittest.TestCase):
    def test_helper_result_builders_cover_document_helpers_and_engine_name_detection(self):
        short_dialect = SimpleNamespace(server_version="8")
        self.assertEqual(build_info_document(short_dialect)["versionArray"], [8, 0, 0, 0])
        self.assertIn("helloOk", hello_document(short_dialect))
        self.assertEqual(connection_status_document(show_privileges=True)["authInfo"]["authenticatedUserPrivileges"], [])
        self.assertEqual(whats_my_uri_document()["you"], "127.0.0.1:0")
        self.assertEqual(cmd_line_opts_document()["parsed"]["net"]["port"], 0)
        self.assertIn("commands", list_commands_document())
        self.assertIn("system", host_info_document())
        self.assertIn("storageEngine", server_status_document(MONGODB_DIALECT_70, engine=MemoryEngine()))

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
