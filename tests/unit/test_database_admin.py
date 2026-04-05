import asyncio
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mongoeco.api._async.client import AsyncDatabase
from mongoeco.api._async._database_admin_command_compiler import DatabaseAdminCommandCompiler
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import BulkWriteError, CollectionInvalid, OperationFailure


class _AsyncCursor:
    def __init__(self, documents):
        self._documents = list(documents)

    def __aiter__(self):
        async def _iterate():
            for document in self._documents:
                yield document

        return _iterate()

    async def to_list(self):
        return list(self._documents)

    async def explain(self):
        return {
            "engine": "memory",
            "strategy": "python",
            "plan": "collection_scan",
            "planning_issues": [],
            "ok": 1.0,
        }


class _FakeCollection:
    def __init__(self, documents=None):
        self.documents = list(documents or [])
        self.insert_calls = []
        self.update_one_calls = []
        self.delete_one_calls = []
        self.find_one_and_delete_calls = []
        self.find_one_and_update_calls = []
        self.find_one_and_replace_calls = []
        self.list_indexes_calls = []
        self.drop_indexes_calls = []
        self.drop_index_calls = []
        self.index_information_result = {"_id_": {"key": [("_id", 1)]}}
        self.list_indexes_result = [{"name": "_id_", "key": {"_id": 1}, "fields": [("_id", 1)]}]

    def _build_cursor(self, operation, *, session=None):
        del operation, session
        return _AsyncCursor(self.documents)

    def _build_aggregation_cursor(self, operation, *, session=None):
        del operation, session
        return _AsyncCursor(self.documents)

    async def insert_one(self, document, **kwargs):
        self.insert_calls.append((document, kwargs))
        if document.get("fail"):
            raise RuntimeError("insert failed")
        return SimpleNamespace()

    async def update_one(self, query, update, **kwargs):
        self.update_one_calls.append((query, update, kwargs))
        return SimpleNamespace(matched_count=1, modified_count=1, upserted_id="upserted-1")

    async def update_many(self, query, update, **kwargs):
        self.update_one_calls.append((query, update, kwargs))
        return SimpleNamespace(matched_count=2, modified_count=2, upserted_id=None)

    async def replace_one(self, query, replacement, **kwargs):
        self.update_one_calls.append((query, replacement, kwargs))
        return SimpleNamespace(matched_count=1, modified_count=1, upserted_id=None)

    async def delete_one(self, query, **kwargs):
        self.delete_one_calls.append((query, kwargs))
        return SimpleNamespace(deleted_count=1)

    async def delete_many(self, query, **kwargs):
        self.delete_one_calls.append((query, kwargs))
        return SimpleNamespace(deleted_count=2)

    async def find_one_and_delete(self, query, **kwargs):
        self.find_one_and_delete_calls.append((query, kwargs))
        return {"_id": 1}

    async def find_one_and_update(self, query, update, **kwargs):
        self.find_one_and_update_calls.append((query, update, kwargs))
        return {"_id": 1, "updated": True}

    async def find_one_and_replace(self, query, replacement, **kwargs):
        self.find_one_and_replace_calls.append((query, replacement, kwargs))
        return {"_id": 1, "replaced": True}

    def list_indexes(self, **kwargs):
        self.list_indexes_calls.append(kwargs)
        return _AsyncCursor(self.list_indexes_result)

    async def index_information(self, **kwargs):
        del kwargs
        return dict(self.index_information_result)

    async def drop_indexes(self, **kwargs):
        self.drop_indexes_calls.append(kwargs)

    async def drop_index(self, target, **kwargs):
        self.drop_index_calls.append((target, kwargs))


class AsyncDatabaseAdminServiceTests(unittest.TestCase):
    def test_database_admin_modules_keep_facade_and_routing_split(self):
        admin_module_path = Path(__file__).resolve().parents[2] / "src" / "mongoeco" / "api" / "_async" / "database_admin.py"
        commands_module_path = Path(__file__).resolve().parents[2] / "src" / "mongoeco" / "api" / "_async" / "database_commands.py"

        admin_tree = ast.parse(admin_module_path.read_text(encoding="utf-8"))
        imported_modules = {
            node.module
            for node in admin_tree.body
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }

        self.assertIn(
            "mongoeco.api._async._database_admin_routing",
            imported_modules,
        )
        commands_source = commands_module_path.read_text(encoding="utf-8")
        self.assertIn("_database_command_contract", commands_source)
        self.assertIn("self._routing", commands_source)
        self.assertIn("self._compiler", commands_source)

    def test_create_collection_rejects_invalid_capped_options(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            with self.assertRaisesRegex(OperationFailure, "positive size option"):
                await service.create_collection("events", capped=True)
            with self.assertRaisesRegex(TypeError, "size must be a positive integer"):
                await service.create_collection("events", capped=True, size="1024")
            with self.assertRaisesRegex(ValueError, "size must be > 0"):
                await service.create_collection("events", capped=True, size=0)
            with self.assertRaisesRegex(ValueError, "max must be > 0"):
                await service.create_collection("events", capped=True, size=1024, max=0)

        asyncio.run(_run())

    def test_database_admin_normalizer_facade_helpers_delegate_and_validate(self):
        service = AsyncDatabase(MemoryEngine(), "db")._admin

        with self.assertRaisesRegex(TypeError, "capped must be a bool"):
            service._validate_create_collection_options({"capped": "yes"})

        self.assertEqual(service._normalize_command({"ping": 1}, {}), {"ping": 1})
        self.assertEqual(service._require_collection_name("users", "find"), "users")
        self.assertEqual(service._resolve_collection_reference("db.users", "to"), "db.users")
        self.assertEqual(service._normalize_index_models_from_command([{"key": {"name": 1}, "name": "name_1"}])[0].document["name"], "name_1")
        self.assertEqual(service._normalize_sort_document({"name": 1}), [("name", 1)])
        self.assertEqual(service._normalize_projection_from_command({"name": 1}), {"name": 1})
        self.assertEqual(service._normalize_batch_size_from_command(5), 5)
        self.assertEqual(service._normalize_scale_from_command(2), 2)
        self.assertEqual(service._normalize_namespace("db.users", "renameCollection"), ("db", "users"))
        self.assertEqual(service._normalize_insert_documents([{"_id": 1}]), [{"_id": 1}])
        self.assertEqual(service._normalize_update_specs([{"q": {}, "u": {"$set": {"x": 1}}}])[0]["q"], {})
        self.assertEqual(service._normalize_delete_specs([{"q": {}, "limit": 1}])[0]["limit"], 1)
        self.assertTrue(service._is_operator_update({"$set": {"x": 1}}))
        self.assertFalse(service._is_operator_update({"x": 1}))

    def test_database_admin_command_compiler_facade_normalizers_delegate(self):
        compiler = DatabaseAdminCommandCompiler(AsyncDatabase(MemoryEngine(), "db")._admin)

        self.assertEqual(compiler.normalize_command({"ping": 1}, {}), {"ping": 1})
        self.assertEqual(compiler.require_collection_name("users", "find"), "users")
        self.assertEqual(compiler.resolve_collection_reference("db.users", "to"), "db.users")
        self.assertEqual(compiler.normalize_index_models([{"key": {"name": 1}, "name": "name_1"}])[0].document["name"], "name_1")
        self.assertEqual(compiler.normalize_sort_document({"name": 1}), [("name", 1)])
        self.assertEqual(compiler.normalize_projection({"name": 1}), {"name": 1})
        self.assertEqual(compiler.normalize_namespace("db.users", "renameCollection"), ("db", "users"))
        self.assertEqual(compiler.normalize_insert_documents([{"_id": 1}]), [{"_id": 1}])
        self.assertEqual(compiler.normalize_update_specs([{"q": {}, "u": {"$set": {"x": 1}}}])[0]["q"], {})
        self.assertEqual(compiler.normalize_delete_specs([{"q": {}, "limit": 1}])[0]["limit"], 1)

    def test_database_admin_compiler_wrapper_helpers_delegate_to_command_compiler(self):
        service = AsyncDatabase(MemoryEngine(), "db")._admin
        compiler = service._command_compiler

        with patch.object(compiler, "compile_update_selection_operation", return_value=SimpleNamespace(kind="update")) as update_mock:
            result = service._compile_command_update_selection_operation(
                {"q": {}, "u": {"$set": {"x": 1}}},
                comment="trace",
                max_time_ms=5,
                limit=1,
            )
        self.assertEqual(result.kind, "update")
        update_mock.assert_called_once()

        with patch.object(compiler, "compile_delete_selection_operation", return_value=SimpleNamespace(kind="delete")) as delete_mock:
            result = service._compile_command_delete_selection_operation(
                {"q": {}, "limit": 1},
                comment="trace",
                max_time_ms=5,
                limit=1,
            )
        self.assertEqual(result.kind, "delete")
        delete_mock.assert_called_once()

        with patch.object(compiler, "compile_find_and_modify_selection_operation", return_value=SimpleNamespace(kind="fam")) as fam_mock:
            result = service._compile_find_and_modify_selection_operation(
                SimpleNamespace(query={}, collation=None, sort=None, hint=None, comment=None, max_time_ms=None, let=None),
            )
        self.assertEqual(result.kind, "fam")
        fam_mock.assert_called_once()

        with patch.object(compiler, "compile_id_lookup_operation", return_value=SimpleNamespace(kind="id")) as id_mock:
            result = service._compile_id_lookup_operation("doc-1", projection={"name": 1})
        self.assertEqual(result.kind, "id")
        id_mock.assert_called_once_with("doc-1", projection={"name": 1})

    def test_compile_command_helpers_validate_find_and_aggregate_shapes(self):
        service = AsyncDatabase(MemoryEngine(), "db")._admin

        with self.assertRaises(TypeError):
            service._compile_command_find_operation(
                {"find": "users", "skip": -1},
                collection_field="find",
            )
        with self.assertRaises(TypeError):
            service._compile_command_find_operation(
                {"find": "users", "limit": -1},
                collection_field="find",
            )
        with self.assertRaises(TypeError):
            service._compile_command_aggregate_operation(
                {"aggregate": "users", "pipeline": {}},
            )
        with self.assertRaises(TypeError):
            service._compile_command_aggregate_operation(
                {"aggregate": "users", "pipeline": [], "cursor": []},
            )

        collection_name, operation = service._compile_command_aggregate_operation(
            {
                "aggregate": "users",
                "pipeline": [],
                "cursor": {"batchSize": 5},
                "comment": "trace",
            }
        )
        self.assertEqual(collection_name, "users")
        self.assertEqual(operation.batch_size, 5)
        self.assertEqual(operation.comment, "trace")

    def test_command_rename_collection_validates_namespaces_and_can_drop_target(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            await database.create_collection("source")
            await database.create_collection("target")
            await database.source.insert_one({"_id": 1})

            with self.assertRaises(OperationFailure):
                await service._command_rename_collection(
                    {"renameCollection": "other.source", "to": "db.target"},
                )
            with self.assertRaises(TypeError):
                await service._command_rename_collection(
                    {"renameCollection": "db.source", "to": "db.target", "dropTarget": "yes"},
                )

            result = await service._command_rename_collection(
                {"renameCollection": "db.source", "to": "db.target", "dropTarget": True},
            )
            names = await database.list_collection_names()
            documents = await database.target.find({}).to_list()
            return result, names, documents

        result, names, documents = asyncio.run(_run())

        self.assertEqual(result.to_document(), {"ok": 1.0})
        self.assertEqual(names, ["target"])
        self.assertEqual(documents, [{"_id": 1}])

    def test_database_admin_command_service_uses_routing_service(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            parsed = service._commands.parse_raw_command({"count": "events"})
            with patch.object(service._routing, "execute_count_command", return_value={"n": 1}) as count_mock:
                result = await service._commands.execute(parsed)
                self.assertEqual(result, {"n": 1})
                count_mock.assert_awaited_once()
            parsed_drop = service._commands.parse_raw_command({"dropDatabase": 1})
            with patch.object(
                service._routing,
                "command_drop_database",
                return_value={"ok": 1.0},
            ) as drop_mock:
                result = await service._commands.execute(parsed_drop)
                self.assertEqual(result, {"ok": 1.0})
                drop_mock.assert_awaited_once_with(session=None)

        asyncio.run(_run())

    def test_command_update_and_find_and_modify_cover_remaining_write_paths(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin
        collection = _FakeCollection()

        async def _run():
            with patch.object(database, "get_collection", return_value=collection):
                with self.assertRaises(BulkWriteError) as type_error:
                    await service._command_update({"update": "events", "updates": [{"q": {}, "u": 1}]})
                self.assertIn("u must be a document or pipeline", str(type_error.exception.details))

                with self.assertRaises(BulkWriteError) as upsert_error:
                    await service._command_update(
                        {"update": "events", "updates": [{"q": {}, "u": {"$set": {"x": 1}}, "upsert": "yes"}]}
                    )
                self.assertIn("upsert must be a bool", str(upsert_error.exception.details))

                with self.assertRaises(BulkWriteError) as let_error:
                    await service._command_update(
                        {"update": "events", "updates": [{"q": {}, "u": {"$set": {"x": 1}}, "let": 1}]}
                    )
                self.assertIn("let must be a dict", str(let_error.exception.details))

                with self.assertRaises(BulkWriteError) as multi_error:
                    await service._command_update(
                        {"update": "events", "updates": [{"q": {}, "u": {"x": 1}, "multi": True}]}
                    )
                self.assertIn("replacement updates cannot be multi", str(multi_error.exception.details))

                options = SimpleNamespace(
                    collection_name="events",
                    query={"_id": 1},
                    update_spec={"name": "Ada"},
                    fields={"name": 1},
                    collation=None,
                    sort=[("_id", 1)],
                    upsert=False,
                    return_new=False,
                    array_filters=None,
                    hint=None,
                    comment="trace",
                    max_time_ms=50,
                    let=None,
                    bypass_document_validation=False,
                )
                with patch.object(service._write_commands, "find_and_modify_before_full", return_value={"_id": 1}):
                    result = await service._execute_find_and_modify_replacement(collection, options)
                self.assertEqual(result.value, {"_id": 1, "replaced": True})

        asyncio.run(_run())

    def test_is_operator_update_treats_pipeline_as_operator_style(self):
        service = AsyncDatabase(MemoryEngine(), "db")._admin

        self.assertTrue(service._is_operator_update([{"$set": {"x": 1}}]))

    def test_database_admin_routing_service_delegates_all_command_families(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin
        routing = service._routing

        async def _run():
            with (
                patch.object(service._read_commands, "command_count", return_value={"ok": 1.0}) as count_mock,
                patch.object(service._read_commands, "command_distinct", return_value={"ok": 1.0}) as distinct_mock,
                patch.object(service._read_commands, "command_db_hash", return_value={"ok": 1.0}) as db_hash_mock,
                patch.object(service._read_commands, "command_find", return_value={"ok": 1.0}) as find_mock,
                patch.object(service._read_commands, "command_aggregate", return_value={"ok": 1.0}) as aggregate_mock,
                patch.object(service._write_commands, "command_insert", return_value={"ok": 1.0}) as insert_mock,
                patch.object(service._write_commands, "command_update", return_value={"ok": 1.0}) as update_mock,
                patch.object(service._write_commands, "command_delete", return_value={"ok": 1.0}) as delete_mock,
                patch.object(service._commands, "command", return_value={"ok": 1.0}) as command_mock,
            ):
                await routing.command_count({"count": "events"})
                await routing.command_distinct({"distinct": "events", "key": "kind"})
                await routing.command_db_hash({"dbHash": 1})
                await routing.command_find({"find": "events"})
                await routing.command_aggregate({"aggregate": "events", "pipeline": []})
                await routing.command_insert({"insert": "events", "documents": [{"_id": 1}]})
                await routing.command_update({"update": "events", "updates": [{"q": {}, "u": {"$set": {"x": 1}}}]})
                await routing.command_delete({"delete": "events", "deletes": [{"q": {}, "limit": 1}]})
                await routing.command_current_op({"currentOp": 1})
                await routing.command_kill_op({"killOp": 1, "op": "op-1"})
                await routing._command_count({"count": "events"})
                await routing._command_db_hash({"dbHash": 1})
                await routing._command_distinct({"distinct": "events", "key": "kind"})
                await routing._command_find({"find": "events"})
                await routing._command_aggregate({"aggregate": "events", "pipeline": []})
                await routing._command_current_op({"currentOp": 1})
                await routing._command_kill_op({"killOp": 1, "op": "op-1"})

            self.assertGreaterEqual(count_mock.call_count, 2)
            self.assertGreaterEqual(distinct_mock.call_count, 2)
            self.assertGreaterEqual(db_hash_mock.call_count, 2)
            self.assertGreaterEqual(find_mock.call_count, 2)
            self.assertGreaterEqual(aggregate_mock.call_count, 2)
            insert_mock.assert_called_once()
            update_mock.assert_called_once()
            delete_mock.assert_called_once()
            self.assertEqual(command_mock.call_count, 4)

        asyncio.run(_run())

    def test_database_admin_facade_wrappers_delegate_to_compiler_and_services(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            with (
                patch.object(service._command_compiler, "compile_count_operation", return_value=("events", "count-op")) as count_compile,
                patch.object(service._command_compiler, "compile_distinct_operation", return_value=("events", "kind", "distinct-op")) as distinct_compile,
                patch.object(service._command_compiler, "compile_find_operation", return_value=("events", "find-op")) as find_compile,
                patch.object(service._command_compiler, "compile_aggregate_operation", return_value=("events", "agg-op")) as aggregate_compile,
                patch.object(service._read_commands, "execute_count_command", return_value={"ok": 1.0}) as count_exec,
                patch.object(service._read_commands, "execute_distinct_command", return_value={"ok": 1.0}) as distinct_exec,
                patch.object(service._read_commands, "execute_find_command", return_value={"ok": 1.0}) as find_exec,
                patch.object(service._read_commands, "execute_aggregate_command", return_value={"ok": 1.0}) as aggregate_exec,
                patch.object(service._write_commands, "execute_find_and_modify", return_value={"ok": 1.0}) as fam_exec,
                patch.object(service._write_commands, "find_and_modify_before_full", return_value={"_id": 1}) as before_full,
                patch.object(service._write_commands, "find_and_modify_fetch_upserted_value", return_value={"_id": 2}) as upserted,
                patch.object(service._write_commands, "execute_find_and_modify_operator_update", return_value={"ok": 1.0}) as operator_update,
                patch.object(service._write_commands, "execute_find_and_modify_replacement", return_value={"ok": 1.0}) as replacement,
                patch.object(service._write_commands, "command_create_indexes", return_value={"ok": 1.0}) as create_indexes,
                patch.object(service._write_commands, "command_drop_indexes", return_value={"ok": 1.0}) as drop_indexes,
                patch.object(service._write_commands, "command_drop_database", return_value={"ok": 1.0}) as drop_database,
                patch.object(service._read_commands, "command_list_indexes", return_value={"ok": 1.0}) as list_indexes,
            ):
                self.assertEqual(await service._command_count({"count": "events"}), {"ok": 1.0})
                self.assertEqual(await service._command_distinct({"distinct": "events", "key": "kind"}), {"ok": 1.0})
                self.assertEqual(await service._command_find({"find": "events"}), {"ok": 1.0})
                self.assertEqual(await service._command_aggregate({"aggregate": "events", "pipeline": []}), {"ok": 1.0})
                self.assertEqual(await service._execute_find_and_modify("opts"), {"ok": 1.0})
                self.assertEqual(await service._find_and_modify_before_full("opts"), {"_id": 1})
                self.assertEqual(await service._find_and_modify_fetch_upserted_value("events", "upserted", None), {"_id": 2})
                self.assertEqual(await service._execute_find_and_modify_operator_update("collection", "opts"), {"ok": 1.0})
                self.assertEqual(await service._execute_find_and_modify_replacement("collection", "opts"), {"ok": 1.0})
                self.assertEqual(await service._command_create_indexes({"createIndexes": "events", "indexes": []}), {"ok": 1.0})
                self.assertEqual(await service._command_drop_indexes({"dropIndexes": "events", "index": "*"}), {"ok": 1.0})
                self.assertEqual(await service._command_drop_database(), {"ok": 1.0})
                self.assertEqual(await service._command_list_indexes({"listIndexes": "events"}), {"ok": 1.0})

            count_compile.assert_called_once()
            distinct_compile.assert_called_once()
            find_compile.assert_called_once()
            aggregate_compile.assert_called_once()
            count_exec.assert_called_once_with("events", "count-op", session=None)
            distinct_exec.assert_called_once_with("events", "kind", "distinct-op", session=None)
            find_exec.assert_called_once_with("events", "find-op", session=None)
            aggregate_exec.assert_called_once_with("events", "agg-op", session=None)
            fam_exec.assert_called_once_with("opts", session=None)
            before_full.assert_called_once_with("opts", session=None)
            upserted.assert_called_once_with("events", "upserted", None, session=None)
            operator_update.assert_called_once_with("collection", "opts", session=None)
            replacement.assert_called_once_with("collection", "opts", session=None)
            create_indexes.assert_called_once()
            drop_indexes.assert_called_once()
            drop_database.assert_called_once_with(session=None)
            list_indexes.assert_called_once()

        asyncio.run(_run())

    def test_execute_distinct_command_handles_missing_values_list_fallback_and_deduplication(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin
        fake_collection = _FakeCollection([{"_id": 1}, {"_id": 2}, {"_id": 3}, {"_id": 4}, {"_id": 5}])

        async def _run():
            with patch.object(database, "get_collection", return_value=fake_collection), patch.object(
                QueryEngine,
                "extract_values",
                side_effect=[[], [], [], ["Ada"], ["Ada"]],
            ), patch.object(
                QueryEngine,
                "_get_field_value",
                side_effect=[
                    (False, None),
                    (True, []),
                    (True, "scalar"),
                ],
            ):
                operation = service._compile_command_find_operation(
                    {"find": "users"},
                    collection_field="find",
                )[1]
                result = await service._execute_distinct_command("users", "name", operation)
                return result.values

        values = asyncio.run(_run())

        self.assertEqual(values, [None, "scalar", "Ada"])

    def test_command_insert_collects_unordered_errors_after_successes(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin
        fake_collection = _FakeCollection()

        async def _run():
            with patch.object(database, "get_collection", return_value=fake_collection):
                with self.assertRaises(BulkWriteError) as exc_info:
                    await service._command_insert(
                        {
                            "insert": "users",
                            "documents": [{"fail": True}, {"_id": 2}],
                            "ordered": False,
                        }
                    )
                return exc_info.exception.details

        details = asyncio.run(_run())

        self.assertEqual(details["nInserted"], 1)
        self.assertEqual(details["writeErrors"][0]["index"], 0)

    def test_command_update_and_delete_collect_partial_results_when_unordered(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin
        fake_collection = _FakeCollection()

        async def _run_update():
            with patch.object(database, "get_collection", return_value=fake_collection):
                with self.assertRaises(BulkWriteError) as exc_info:
                    await service._command_update(
                        {
                            "update": "users",
                            "ordered": False,
                            "updates": [
                                {"q": {}, "u": {"$set": {"name": "Ada"}}, "multi": "yes"},
                                {"q": {}, "u": {"$set": {"name": "Grace"}}, "upsert": True},
                            ],
                        }
                    )
                return exc_info.exception.details

        async def _run_delete():
            with patch.object(database, "get_collection", return_value=fake_collection):
                with self.assertRaises(BulkWriteError) as exc_info:
                    await service._command_delete(
                        {
                            "delete": "users",
                            "ordered": False,
                            "deletes": [
                                {"q": {}, "limit": 2},
                                {"q": {}, "limit": 1},
                            ],
                        }
                    )
                return exc_info.exception.details

        update_details = asyncio.run(_run_update())
        delete_details = asyncio.run(_run_delete())

        self.assertEqual(update_details["nMatched"], 1)
        self.assertEqual(update_details["nModified"], 1)
        self.assertEqual(update_details["upserted"][0]["_id"], "upserted-1")
        self.assertEqual(delete_details["nRemoved"], 1)
        self.assertEqual(delete_details["writeErrors"][0]["index"], 0)

    def test_database_admin_helpers_and_command_wrappers_cover_edge_paths(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            await database.create_collection("events")
            await database.events.insert_one({"_id": 1})

            documents = await service._list_database_documents()
            snapshot = await service._build_collection_validation_snapshot("events")
            self.assertEqual(documents[0]["name"], "db")
            self.assertEqual(snapshot.namespace, "db.events")
            self.assertEqual(snapshot.record_count, 1)

            with self.assertRaisesRegex(CollectionInvalid, "collection 'missing' does not exist"):
                await service._build_collection_validation_snapshot("missing")

            with patch.object(service, "_compile_command_count_operation", return_value=("events", object())), patch.object(
                service,
                "_execute_count_command",
                return_value={"ok": 1},
            ) as execute_count:
                self.assertEqual(await service._command_count({"count": "events"}), {"ok": 1})
                execute_count.assert_awaited_once()

            with patch.object(service, "_compile_command_distinct_operation", return_value=("events", "name", object())), patch.object(
                service,
                "_execute_distinct_command",
                return_value={"values": []},
            ) as execute_distinct:
                self.assertEqual(await service._command_distinct({"distinct": "events", "key": "name"}), {"values": []})
                execute_distinct.assert_awaited_once()

            with patch.object(service, "_compile_command_find_operation", return_value=("events", object())), patch.object(
                service,
                "_execute_find_command",
                return_value={"cursor": {}},
            ) as execute_find:
                self.assertEqual(await service._command_find({"find": "events"}), {"cursor": {}})
                execute_find.assert_awaited_once()

            with patch.object(service, "_compile_command_aggregate_operation", return_value=("events", object())), patch.object(
                service,
                "_execute_aggregate_command",
                return_value={"cursor": {}},
            ) as execute_aggregate:
                self.assertEqual(await service._command_aggregate({"aggregate": "events", "pipeline": []}), {"cursor": {}})
                execute_aggregate.assert_awaited_once()

        asyncio.run(_run())

    def test_database_admin_additional_wrapper_paths_cover_read_write_and_cursor_fallbacks(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        class _CursorWithoutFirst:
            async def to_list(self):
                return [{"_id": 1}]

        async def _run():
            fake_collection = SimpleNamespace(_build_cursor=lambda _operation, session=None: _CursorWithoutFirst())
            with patch.object(service._database, "get_collection", return_value=fake_collection):
                document = await service._first_with_operation("events", object())
                self.assertEqual(document, {"_id": 1})

            with (
                patch.object(service._read_commands, "command_list_collections", return_value={"cursor": {}}) as list_collections,
                patch.object(service._read_commands, "command_list_databases", return_value={"databases": []}) as list_databases,
                patch.object(service._write_commands, "command_create", return_value={"ok": 1.0}) as create_mock,
                patch.object(service._write_commands, "command_drop", return_value={"ok": 1.0}) as drop_mock,
                patch.object(service._write_commands, "command_rename_collection", return_value={"ok": 1.0}) as rename_mock,
                patch.object(service._read_commands, "execute_db_hash_command", return_value={"md5": "x"}) as db_hash_exec,
            ):
                self.assertEqual(await service._command_list_collections({"listCollections": 1}), {"cursor": {}})
                self.assertEqual(await service._command_list_databases({"listDatabases": 1}), {"databases": []})
                self.assertEqual(await service._command_create({"create": "events"}), {"ok": 1.0})
                self.assertEqual(await service._command_drop({"drop": "events"}), {"ok": 1.0})
                self.assertEqual(await service._command_rename_collection({"renameCollection": "db.a", "to": "db.b"}), {"ok": 1.0})
                self.assertEqual(await service._execute_db_hash_command(("events",), comment="trace"), {"md5": "x"})

            list_collections.assert_awaited_once_with({"listCollections": 1}, session=None)
            list_databases.assert_awaited_once_with({"listDatabases": 1}, session=None)
            create_mock.assert_awaited_once_with({"create": "events"}, session=None)
            drop_mock.assert_awaited_once_with({"drop": "events"}, session=None)
            rename_mock.assert_awaited_once_with({"renameCollection": "db.a", "to": "db.b"}, session=None)
            db_hash_exec.assert_awaited_once_with(("events",), comment="trace", session=None)

        asyncio.run(_run())

    def test_database_admin_read_command_service_and_compiler_cover_direct_paths(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            with (
                patch.object(service._command_compiler, "compile_count_operation", return_value=("events", "count-op")) as count_compile,
                patch.object(service._read_commands, "execute_count_command", return_value={"n": 1}) as count_exec,
                patch.object(service._command_compiler, "compile_distinct_operation", return_value=("events", "kind", "distinct-op")) as distinct_compile,
                patch.object(service._read_commands, "execute_distinct_command", return_value={"values": []}) as distinct_exec,
                patch.object(service._command_compiler, "compile_find_operation", return_value=("events", "find-op")) as find_compile,
                patch.object(service._read_commands, "execute_find_command", return_value={"cursor": {}}) as find_exec,
                patch.object(service._command_compiler, "compile_aggregate_operation", return_value=("events", "agg-op")) as aggregate_compile,
                patch.object(service._read_commands, "execute_aggregate_command", return_value={"cursor": {}}) as aggregate_exec,
            ):
                self.assertEqual(await service._read_commands.command_count({"count": "events"}), {"n": 1})
                self.assertEqual(await service._read_commands.command_distinct({"distinct": "events", "key": "kind"}), {"values": []})
                self.assertEqual(await service._read_commands.command_find({"find": "events"}), {"cursor": {}})
                self.assertEqual(await service._read_commands.command_aggregate({"aggregate": "events", "pipeline": []}), {"cursor": {}})

            count_compile.assert_called_once_with({"count": "events"})
            count_exec.assert_awaited_once_with("events", "count-op", session=None)
            distinct_compile.assert_called_once_with({"distinct": "events", "key": "kind"})
            distinct_exec.assert_awaited_once_with("events", "kind", "distinct-op", session=None)
            find_compile.assert_called_once_with({"find": "events"}, collection_field="find")
            find_exec.assert_awaited_once_with("events", "find-op", session=None)
            aggregate_compile.assert_called_once_with({"aggregate": "events", "pipeline": []})
            aggregate_exec.assert_awaited_once_with("events", "agg-op", session=None)

            with self.assertRaisesRegex(TypeError, "collections must be a list of non-empty strings"):
                await service._read_commands.command_db_hash({"dbHash": 1, "collections": ["", "users"]})
            with self.assertRaisesRegex(TypeError, "comment must be a string"):
                await service._read_commands.command_db_hash({"dbHash": 1, "comment": 1})
            with self.assertRaisesRegex(TypeError, "comment must be a string"):
                await service._read_commands.command_list_indexes({"listIndexes": "events", "comment": 1})

        asyncio.run(_run())

    def test_database_admin_command_paths_validate_argument_errors(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin
        fake_collection = _FakeCollection()

        async def _run():
            with patch.object(database, "get_collection", return_value=fake_collection):
                with self.assertRaisesRegex(TypeError, "bypassDocumentValidation must be a bool"):
                    await service._command_insert({"insert": "users", "documents": [{"_id": 1}], "bypassDocumentValidation": "yes"})
                with self.assertRaisesRegex(TypeError, "bypassDocumentValidation must be a bool"):
                    await service._command_update(
                        {
                            "update": "users",
                            "updates": [{"q": {}, "u": {"$set": {"name": "Ada"}}}],
                            "bypassDocumentValidation": "yes",
                        }
                    )
                with self.assertRaises(BulkWriteError):
                    await service._command_update(
                        {
                            "update": "users",
                            "updates": [{"q": {}, "u": {"$set": {"name": "Ada"}}, "arrayFilters": "bad"}],
                        }
                    )
                with self.assertRaises(BulkWriteError):
                    await service._command_update(
                        {
                            "update": "users",
                            "ordered": True,
                            "updates": [
                                {"q": {}, "u": {"$set": {"name": "Ada"}}, "multi": "bad"},
                                {"q": {}, "u": {"$set": {"name": "Grace"}}},
                            ],
                        }
                    )
                self.assertEqual(len(fake_collection.update_one_calls), 0)
                with self.assertRaises(BulkWriteError):
                    await service._command_delete(
                        {
                            "delete": "users",
                            "deletes": [{"q": {}, "limit": 1, "let": "bad"}],
                        }
                    )
                with self.assertRaises(BulkWriteError):
                    await service._command_delete(
                        {
                            "delete": "users",
                            "ordered": True,
                            "deletes": [{"q": {}, "limit": 2}, {"q": {}, "limit": 1}],
                        }
                    )
                self.assertEqual(len(fake_collection.delete_one_calls), 0)

        asyncio.run(_run())

    def test_database_admin_explain_find_and_modify_and_index_commands_cover_edge_paths(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin
        fake_collection = _FakeCollection([{"_id": 1}])

        async def _run():
            with patch.object(database, "get_collection", return_value=fake_collection):
                with self.assertRaisesRegex(TypeError, "explain must be a non-empty document"):
                    await service._command_explain({"explain": []})
                with self.assertRaisesRegex(TypeError, "verbosity must be a string"):
                    await service._command_explain({"explain": {"find": "users"}, "verbosity": 1})
                with self.assertRaisesRegex(OperationFailure, "exactly one update specification"):
                    await service._command_explain({"explain": {"update": "users", "updates": [{}, {}]}})
                with self.assertRaisesRegex(OperationFailure, "exactly one delete specification"):
                    await service._command_explain({"explain": {"delete": "users", "deletes": [{}, {}]}})
                with self.assertRaisesRegex(TypeError, "multi must be a bool"):
                    await service._command_explain(
                        {"explain": {"update": "users", "updates": [{"q": {}, "u": {"$set": {"a": 1}}, "multi": "yes"}]}}
                    )
                with self.assertRaisesRegex(TypeError, "limit must be 0 or 1"):
                    await service._command_explain(
                        {"explain": {"delete": "users", "deletes": [{"q": {}, "limit": 2}]}}
                    )
                with self.assertRaisesRegex(OperationFailure, "Unsupported explain command"):
                    await service._command_explain({"explain": {"ping": 1}})
                count_explain = await service._command_explain({"explain": {"count": "users", "query": {}}})
                distinct_explain = await service._command_explain(
                    {"explain": {"distinct": "users", "key": "kind", "query": {}}}
                )
                find_and_modify_explain = await service._command_explain(
                    {
                        "explain": {
                            "findAndModify": "users",
                            "query": {"kind": "view"},
                            "update": {"$set": {"done": True}},
                            "new": True,
                            "upsert": True,
                            "fields": {"_id": 1},
                        }
                    }
                )
                self.assertEqual(count_explain["command"], "count")
                self.assertEqual(count_explain["explained_command"], "count")
                self.assertEqual(count_explain["collection"], "users")
                self.assertEqual(count_explain["namespace"], "db.users")
                self.assertEqual(distinct_explain["command"], "distinct")
                self.assertEqual(distinct_explain["explained_command"], "distinct")
                self.assertEqual(distinct_explain["collection"], "users")
                self.assertEqual(distinct_explain["namespace"], "db.users")
                self.assertEqual(distinct_explain["key"], "kind")
                self.assertEqual(find_and_modify_explain["command"], "findAndModify")
                self.assertEqual(find_and_modify_explain["explained_command"], "findAndModify")
                self.assertEqual(find_and_modify_explain["collection"], "users")
                self.assertEqual(find_and_modify_explain["namespace"], "db.users")
                self.assertTrue(find_and_modify_explain["new"])
                self.assertTrue(find_and_modify_explain["upsert"])
                self.assertEqual(find_and_modify_explain["fields"], {"_id": 1})

                remove_result = await service._execute_find_and_modify_remove(
                    fake_collection,
                    SimpleNamespace(
                        update_spec=None,
                        upsert=False,
                        query={},
                        fields=None,
                        collation=None,
                        sort=None,
                        hint=None,
                        comment=None,
                        max_time_ms=None,
                        let=None,
                    ),
                )
                self.assertEqual(remove_result.value, {"_id": 1})

                with self.assertRaisesRegex(OperationFailure, "cannot be specified together"):
                    await service._execute_find_and_modify_remove(
                        fake_collection,
                        SimpleNamespace(
                            update_spec={"$set": {"a": 1}},
                            upsert=False,
                            query={},
                            fields=None,
                            collation=None,
                            sort=None,
                            hint=None,
                            comment=None,
                            max_time_ms=None,
                            let=None,
                        ),
                    )
                with self.assertRaisesRegex(OperationFailure, "does not support upsert"):
                    await service._execute_find_and_modify_remove(
                        fake_collection,
                        SimpleNamespace(
                            update_spec=None,
                            upsert=True,
                            query={},
                            fields=None,
                            collation=None,
                            sort=None,
                            hint=None,
                            comment=None,
                            max_time_ms=None,
                            let=None,
                        ),
                    )

                with patch.object(service._write_commands, "find_and_modify_before_full", return_value=None), patch.object(
                    service._write_commands,
                    "find_and_modify_fetch_upserted_value",
                    return_value={"_id": "upserted-1"},
                ):
                    operator_result = await service._execute_find_and_modify_operator_update(
                        fake_collection,
                        SimpleNamespace(
                            collection_name="users",
                            query={},
                            update_spec={"$set": {"a": 1}},
                            fields=None,
                            collation=None,
                            sort=[("name", 1)],
                            upsert=True,
                            return_new=True,
                            array_filters=None,
                            hint="name_1",
                            comment="trace",
                            max_time_ms=10,
                            let={"tenant": "a"},
                            bypass_document_validation=True,
                        ),
                    )
                    replacement_result = await service._execute_find_and_modify_replacement(
                        fake_collection,
                        SimpleNamespace(
                            collection_name="users",
                            query={},
                            update_spec={"name": "Ada"},
                            fields=None,
                            collation=None,
                            sort=[("name", 1)],
                            upsert=True,
                            return_new=True,
                            hint="name_1",
                            comment="trace",
                            max_time_ms=10,
                            let={"tenant": "a"},
                            bypass_document_validation=True,
                        ),
                    )
                self.assertEqual(operator_result.value, {"_id": "upserted-1"})
                self.assertEqual(replacement_result.value, {"_id": "upserted-1"})

                indexes_result = await service._execute_list_indexes_command("users", comment="trace")
                self.assertEqual(indexes_result.first_batch, [{"name": "_id_", "key": {"_id": 1}, "ns": "db.users"}])
                self.assertEqual(fake_collection.list_indexes_calls[-1]["comment"], "trace")

                drop_all = await service._command_drop_indexes({"dropIndexes": "users", "index": "*"})
                self.assertIn("non-_id indexes", drop_all.message)
                with self.assertRaisesRegex(TypeError, "index must be '\\*', a name, or a key specification"):
                    await service._command_drop_indexes({"dropIndexes": "users", "index": 1.5})

        asyncio.run(_run())

    def test_database_admin_db_hash_covers_local_hashing_and_missing_collection(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            await database.get_collection("users").insert_one({"_id": "1", "name": "Ada"})
            await database.get_collection("users").create_index([("name", 1)], name="name_idx")

            full_hash = (await service._command_db_hash({"dbHash": 1})).to_document()
            filtered_hash = (await service._command_db_hash({"dbHash": 1, "collections": ["users"]})).to_document()

            self.assertEqual(list(filtered_hash["collections"]), ["users"])
            self.assertEqual(filtered_hash["collections"]["users"], full_hash["collections"]["users"])
            self.assertEqual(len(filtered_hash["md5"]), 32)

            with self.assertRaisesRegex(OperationFailure, "dbHash unknown collections: missing"):
                await service._command_db_hash({"dbHash": 1, "collections": ["missing"]})

        asyncio.run(_run())

    def test_database_admin_read_commands_validate_and_dispatch_list_indexes(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            with patch.object(
                service._read_commands,
                "execute_list_indexes_command",
                return_value={"cursor": {"firstBatch": []}},
            ) as execute_list_indexes:
                with self.assertRaisesRegex(TypeError, "comment must be a string"):
                    await service._read_commands.command_list_indexes(
                        {"listIndexes": "users", "comment": 1}
                    )

                result = await service._read_commands.command_list_indexes(
                    {"listIndexes": "users", "comment": "trace"}
                )
                self.assertEqual(result, {"cursor": {"firstBatch": []}})
                execute_list_indexes.assert_awaited_once_with(
                    "users",
                    comment="trace",
                    session=None,
                )

        asyncio.run(_run())

    def test_validate_warnings_cover_emulated_runtime_flags(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            await database.get_collection("users").insert_one({"_id": "1", "name": "Ada"})

            validated = await service.validate_collection(
                "users",
                scandata=True,
                full=True,
                background=False,
            )

            self.assertEqual(
                validated["warnings"],
                [
                    "validate scandata is accepted for compatibility but does not change local validation behavior",
                    "validate full is accepted for compatibility but does not change local validation behavior",
                    "validate background is accepted for compatibility but validation runs synchronously in mongoeco",
                ],
            )

        asyncio.run(_run())

    def test_validate_warns_when_ttl_index_targets_non_date_values(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        async def _run():
            collection = database.get_collection("users")
            await collection.insert_many(
                [
                    {"_id": "1", "expires_at": "soon"},
                    {"_id": "2", "expires_at": "later"},
                    {"_id": "3", "expires_at": None},
                ]
            )
            await collection.create_index(
                [("expires_at", 1)],
                name="expires_at_ttl",
                expire_after_seconds=30,
            )

            validated = await service.validate_collection("users")

            self.assertEqual(
                validated["warnings"],
                [
                    "TTL index 'expires_at_ttl' on 'users.expires_at' has 3 document(s) with no date values; those documents will not expire under local TTL semantics",
                ],
            )

        asyncio.run(_run())

    def test_ttl_validation_warnings_support_fields_fallback_and_skip_invalid_field_shapes(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        namespace = database._admin._namespace_admin

        warnings = namespace._ttl_validation_warnings(
            "users",
            documents=[{"expires_at": "soon"}, {"expires_at": None}],
            indexes=[
                {"name": "ttl_fields", "fields": ["expires_at"], "expireAfterSeconds": 30},
                {"name": "ttl_bad", "fields": [1], "expireAfterSeconds": 30},
            ],
        )

        self.assertEqual(
            warnings,
            [
                "TTL index 'ttl_fields' on 'users.expires_at' has 2 document(s) with no date values; those documents will not expire under local TTL semantics",
            ],
        )


if __name__ == "__main__":
    unittest.main()
