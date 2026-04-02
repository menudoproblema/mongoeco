import asyncio
import unittest

from mongoeco.api._async.client import AsyncDatabase
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import CollectionInvalid


class DatabaseNamespaceAdminServiceTests(unittest.TestCase):
    def test_namespace_admin_reports_stats_and_validation_snapshots(self):
        engine = MemoryEngine()
        database = AsyncDatabase(engine, "db")

        async def _run():
            await database.create_collection("users")
            await database.users.insert_one({"_id": "1", "name": "Ada"})
            await database.users.create_index([("name", 1)], name="name_1")

            collection_stats = await database._admin._namespace_admin.collection_stats("users")
            database_stats = await database._admin._namespace_admin.database_stats()
            validation = await database._admin._namespace_admin.build_collection_validation_snapshot("users")
            return collection_stats, database_stats, validation

        collection_stats, database_stats, validation = asyncio.run(_run())

        self.assertEqual(collection_stats.namespace, "db.users")
        self.assertEqual(collection_stats.count, 1)
        self.assertEqual(collection_stats.index_count, 2)
        self.assertGreater(collection_stats.data_size, 0)

        self.assertEqual(database_stats.db_name, "db")
        self.assertEqual(database_stats.collection_count, 1)
        self.assertEqual(database_stats.object_count, 1)
        self.assertEqual(database_stats.index_count, 2)

        self.assertEqual(validation.namespace, "db.users")
        self.assertEqual(validation.record_count, 1)
        self.assertEqual(validation.index_count, 2)
        self.assertEqual(validation.keys_per_index["_id_"], 1)
        self.assertEqual(validation.keys_per_index["name_1"], 1)

    def test_namespace_admin_lists_database_snapshots_and_missing_collections(self):
        engine = MemoryEngine()
        primary = AsyncDatabase(engine, "db1")
        secondary = AsyncDatabase(engine, "db2")

        async def _run():
            await primary.create_collection("users")
            await primary.users.insert_one({"_id": "1"})
            await secondary.create_collection("logs")
            snapshots = await primary._admin._namespace_admin.list_database_snapshots()
            documents = await primary._admin._namespace_admin.list_database_documents()
            return snapshots, documents

        snapshots, documents = asyncio.run(_run())

        self.assertEqual([snapshot.name for snapshot in snapshots], ["db1", "db2"])
        self.assertEqual(
            [document["name"] for document in documents],
            ["db1", "db2"],
        )
        self.assertFalse(documents[0]["empty"])

        with self.assertRaisesRegex(CollectionInvalid, "does not exist"):
            asyncio.run(primary._admin._namespace_admin.build_collection_validation_snapshot("missing"))
