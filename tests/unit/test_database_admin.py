import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mongoeco.api._async.client import AsyncDatabase
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import BulkWriteError, OperationFailure


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


class _FakeCollection:
    def __init__(self, documents=None):
        self.documents = list(documents or [])
        self.insert_calls = []
        self.update_one_calls = []
        self.delete_one_calls = []

    def _build_cursor(self, operation, *, session=None):
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


class AsyncDatabaseAdminServiceTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
