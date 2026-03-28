import datetime
import unittest

from mongoeco.api.operations import compile_update_operation
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines.semantic_core import compile_find_semantics
from tests.support import ENGINE_FACTORIES, open_engine


class StorageEngineContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_put_and_get_round_trip_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    doc = {"_id": "1", "name": "Val", "dt": datetime.datetime(2026, 3, 23)}

                    await engine.put_document(db, coll, doc)
                    found = await engine.get_document(db, coll, "1")

                    self.assertEqual(found, doc)
                    self.assertIsInstance(found["dt"], datetime.datetime)
                    self.assertIsNot(found, doc)

    async def test_get_document_accepts_projection_pushdown(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    await engine.put_document("db", "coll", {"_id": "1", "profile": {"name": "Ada"}, "role": "admin"})

                    found = await engine.get_document("db", "coll", "1", projection={"profile.name": 1, "_id": 0})

                    self.assertEqual(found, {"profile": {"name": "Ada"}})

    async def test_put_document_respects_collision_policy(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    doc = {"_id": "1", "v": 1}

                    self.assertTrue(await engine.put_document(db, coll, doc, overwrite=False))
                    self.assertFalse(
                        await engine.put_document(db, coll, {"_id": "1", "v": 2}, overwrite=False)
                    )

                    found = await engine.get_document(db, coll, "1")
                    self.assertEqual(found["v"], 1)

    async def test_put_document_with_overwrite_replaces_existing_value(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "same", "v": 1}, overwrite=False)

                    self.assertTrue(
                        await engine.put_document(db, coll, {"_id": "same", "v": 2}, overwrite=True)
                    )

                    found = await engine.get_document(db, coll, "same")
                    self.assertEqual(found["v"], 2)

    async def test_delete_document_removes_existing_value(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "del"})

                    self.assertTrue(await engine.delete_document(db, coll, "del"))
                    self.assertIsNone(await engine.get_document(db, coll, "del"))

    async def test_scan_find_semantics_returns_all_documents_as_copies(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "1", "nested": {"v": 1}})
                    await engine.put_document(db, coll, {"_id": "2", "nested": {"v": 2}})

                    documents = [
                        doc
                        async for doc in engine.scan_find_semantics(
                            db,
                            coll,
                            compile_find_semantics({}),
                        )
                    ]

                    self.assertEqual({doc["_id"] for doc in documents}, {"1", "2"})

                    documents[0]["nested"]["v"] = 99
                    stored = await engine.get_document(db, coll, documents[0]["_id"])
                    self.assertNotEqual(stored["nested"]["v"], 99)

    async def test_scan_find_semantics_accepts_filter_pushdown(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "1", "kind": "view"})
                    await engine.put_document(db, coll, {"_id": "2", "kind": "click"})

                    documents = [
                        doc
                        async for doc in engine.scan_find_semantics(
                            db,
                            coll,
                            compile_find_semantics({"kind": "view"}),
                        )
                    ]

                    self.assertEqual(documents, [{"_id": "1", "kind": "view"}])

    async def test_scan_find_semantics_accepts_projection_pushdown(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "1", "kind": "view", "payload": {"n": 1}})

                    documents = [
                        doc
                        async for doc in engine.scan_find_semantics(
                            db,
                            coll,
                            compile_find_semantics(
                                {"kind": "view"},
                                projection={"payload.n": 1, "_id": 0},
                            ),
                        )
                    ]

                    self.assertEqual(documents, [{"payload": {"n": 1}}])

    async def test_scan_find_semantics_accepts_sort_skip_and_limit_pushdown(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "db", "events"
                    await engine.put_document(db, coll, {"_id": "1", "rank": 3})
                    await engine.put_document(db, coll, {"_id": "2", "rank": 1})
                    await engine.put_document(db, coll, {"_id": "3", "rank": 2})

                    documents = [
                        doc
                        async for doc in engine.scan_find_semantics(
                            db,
                            coll,
                            compile_find_semantics(
                                {},
                                sort=[("rank", 1)],
                                skip=1,
                                limit=1,
                            ),
                        )
                    ]

                    self.assertEqual(documents, [{"_id": "3", "rank": 2}])

    async def test_list_databases_and_collections_reflect_written_documents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    await engine.put_document("db1", "coll1", {"_id": "1"})
                    await engine.put_document("db2", "coll2", {"_id": "2"})

                    dbs = await engine.list_databases()
                    colls = await engine.list_collections("db1")

                    self.assertIn("db1", dbs)
                    self.assertIn("db2", dbs)
                    self.assertIn("coll1", colls)

    async def test_drop_collection_removes_collection_contents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "1"})

                    await engine.drop_collection(db, coll)

                    self.assertIsNone(await engine.get_document(db, coll, "1"))
                    self.assertNotIn(coll, await engine.list_collections(db))

    async def test_create_collection_registers_empty_namespace(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    await engine.create_collection("db", "empty")

                    self.assertIn("db", await engine.list_databases())
                    self.assertEqual(await engine.list_collections("db"), ["empty"])

    async def test_embedded_document_comparison_does_not_crash(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test", "test"
                    await engine.put_document(db, coll, {"_id": "1", "data": {"b": 1}})

                    found = await engine.get_document(db, coll, "1")

                    self.assertFalse(QueryEngine.match(found, {"data": {"b": 2}}))
                    self.assertTrue(QueryEngine.match(found, {"data": {"b": 1}}))

    async def test_engine_supports_embedded_document_ids(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    doc_id = {"tenant": 1, "user": 2}
                    doc = {"_id": doc_id, "name": "Ada"}

                    await engine.put_document(db, coll, doc)
                    found = await engine.get_document(db, coll, {"tenant": 1, "user": 2})

                    self.assertEqual(found, doc)

    async def test_engine_can_update_with_operation_atomically(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "1", "kind": "view"})

                    result = await engine.update_with_operation(
                        db,
                        coll,
                        compile_update_operation(
                            {"kind": "view"},
                            update_spec={"$set": {"processed": True}},
                        ),
                    )
                    found = await engine.get_document(db, coll, "1")

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(found, {"_id": "1", "kind": "view", "processed": True})

    async def test_engine_can_delete_with_operation_atomically(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "1", "kind": "view"})
                    await engine.put_document(db, coll, {"_id": "2", "kind": "click"})

                    result = await engine.delete_with_operation(
                        db,
                        coll,
                        compile_update_operation({"kind": "view"}),
                    )

                    self.assertEqual(result.deleted_count, 1)
                    self.assertIsNone(await engine.get_document(db, coll, "1"))
                    self.assertIsNotNone(await engine.get_document(db, coll, "2"))

    async def test_engine_can_count_find_semantics_without_scanning_in_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    db, coll = "test_db", "test_coll"
                    await engine.put_document(db, coll, {"_id": "1", "kind": "view"})
                    await engine.put_document(db, coll, {"_id": "2", "kind": "click"})
                    await engine.put_document(db, coll, {"_id": "3", "kind": "view"})

                    count = await engine.count_find_semantics(
                        db,
                        coll,
                        compile_find_semantics({"kind": "view"}),
                    )

                    self.assertEqual(count, 2)

    async def test_engine_can_create_and_list_indexes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    name = await engine.create_index(
                        "test_db",
                        "test_coll",
                        ["kind", "created_at"],
                        unique=False,
                    )
                    indexes = await engine.list_indexes("test_db", "test_coll")

                    self.assertEqual(name, "kind_1_created_at_1")
                    self.assertEqual(
                        indexes,
                        [
                            {
                                "name": "_id_",
                                "fields": ["_id"],
                                "key": {"_id": 1},
                                "unique": True,
                            },
                            {
                                "name": "kind_1_created_at_1",
                                "fields": ["kind", "created_at"],
                                "key": {"kind": 1, "created_at": 1},
                                "unique": False,
                            }
                        ],
                    )

    async def test_engine_can_drop_and_describe_indexes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    await engine.create_index(
                        "test_db",
                        "test_coll",
                        [("kind", 1), ("created_at", -1)],
                        unique=True,
                    )

                    info = await engine.index_information("test_db", "test_coll")
                    await engine.drop_index("test_db", "test_coll", [("kind", 1), ("created_at", -1)])
                    indexes_after_drop = await engine.list_indexes("test_db", "test_coll")

                    self.assertEqual(
                        info,
                        {
                            "_id_": {"key": [("_id", 1)], "unique": True},
                            "kind_1_created_at_-1": {
                                "key": [("kind", 1), ("created_at", -1)],
                                "unique": True,
                            },
                        },
                    )
                    self.assertEqual(
                        indexes_after_drop,
                        [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}],
                    )

    async def test_engine_preserves_virtual_index_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    await engine.create_index(
                        "test_db",
                        "test_coll",
                        ["email"],
                        sparse=True,
                        partial_filter_expression={"active": True},
                    )

                    indexes = await engine.list_indexes("test_db", "test_coll")
                    info = await engine.index_information("test_db", "test_coll")

                    self.assertEqual(
                        indexes[1],
                        {
                            "name": "email_1",
                            "fields": ["email"],
                            "key": {"email": 1},
                            "unique": False,
                            "sparse": True,
                            "partialFilterExpression": {"active": True},
                        },
                    )
                    self.assertEqual(
                        info["email_1"],
                        {
                            "key": [("email", 1)],
                            "sparse": True,
                            "partialFilterExpression": {"active": True},
                        },
                    )
