import datetime
import unittest
import uuid
from unittest.mock import patch

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.query_plan import MatchAll, compile_filter
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, ExecutionTimeout, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import UNDEFINED


class _UnhashableValue:
    __hash__ = None

    def __repr__(self) -> str:
        return "unhashable-value"


class MemoryEngineTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _scan(
        engine: MemoryEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object] | None = None,
        *,
        plan=None,
        projection=None,
        collation=None,
        sort=None,
        skip: int = 0,
        limit: int | None = None,
        hint=None,
        comment=None,
        max_time_ms: int | None = None,
        dialect=None,
        context=None,
    ):
        return engine.scan_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics(
                filter_spec,
                plan=plan,
                projection=projection,
                collation=collation,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                dialect=dialect,
            ),
            context=context,
        )

    @staticmethod
    async def _count(
        engine: MemoryEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object] | None = None,
        *,
        plan=None,
        collation=None,
        dialect=None,
        context=None,
    ) -> int:
        return await engine.count_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics(
                filter_spec,
                plan=plan,
                collation=collation,
                dialect=dialect,
            ),
            context=context,
        )

    @staticmethod
    async def _update(
        engine: MemoryEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object],
        update_spec: dict[str, object],
        *,
        upsert: bool = False,
        upsert_seed=None,
        selector_filter=None,
        array_filters=None,
        plan=None,
        collation=None,
        sort=None,
        hint=None,
        comment=None,
        max_time_ms=None,
        let=None,
        dialect=None,
        context=None,
        bypass_document_validation: bool = False,
    ):
        effective_dialect = dialect or MONGODB_DIALECT_70
        return await engine.update_with_operation(
            db_name,
            coll_name,
            compile_update_operation(
                filter_spec,
                collation=collation,
                sort=sort,
                array_filters=array_filters,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                let=let,
                dialect=effective_dialect,
                plan=plan,
                update_spec=update_spec,
            ),
            upsert=upsert,
            upsert_seed=upsert_seed,
            selector_filter=selector_filter,
            dialect=effective_dialect,
            context=context,
            bypass_document_validation=bypass_document_validation,
        )

    @staticmethod
    async def _delete(
        engine: MemoryEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object],
        *,
        plan=None,
        collation=None,
        sort=None,
        hint=None,
        comment=None,
        max_time_ms=None,
        let=None,
        dialect=None,
        context=None,
    ):
        effective_dialect = dialect or MONGODB_DIALECT_70
        return await engine.delete_with_operation(
            db_name,
            coll_name,
            compile_update_operation(
                filter_spec,
                collation=collation,
                sort=sort,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                let=let,
                dialect=effective_dialect,
                plan=plan,
            ),
            dialect=effective_dialect,
            context=context,
        )

    @staticmethod
    async def _explain(
        engine: MemoryEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object] | None = None,
        *,
        plan=None,
        collation=None,
        sort=None,
        skip: int = 0,
        limit: int | None = None,
        hint=None,
        comment=None,
        max_time_ms=None,
        dialect=None,
        context=None,
    ):
        return await engine.explain_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics(
                filter_spec,
                plan=plan,
                collation=collation,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                dialect=dialect,
            ),
            context=context,
        )

    async def test_scan_collection_records_comment_and_max_time_in_session_state(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            session = ClientSession()
            engine.create_session_state(session)
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})

            documents = [
                doc
                async for doc in self._scan(
                    engine,
                    "db",
                    "coll",
                    {"kind": "view"},
                    comment="trace-memory",
                    max_time_ms=25,
                    context=session,
                )
            ]
            state = session.get_engine_state(f"memory:{id(engine)}")
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "kind": "view"}])
        self.assertIsInstance(state, dict)
        self.assertEqual(state["last_operation"]["comment"], "trace-memory")
        self.assertEqual(state["last_operation"]["max_time_ms"], 25)

    async def test_scan_collection_enforces_max_time_ms_deadline(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            with patch("mongoeco.engines.memory.enforce_deadline", side_effect=ExecutionTimeout("operation exceeded time limit")):
                with self.assertRaises(ExecutionTimeout):
                    [
                        doc
                        async for doc in self._scan(
                            engine,
                            "db",
                            "coll",
                            {"kind": "view"},
                            max_time_ms=1,
                        )
                    ]
        finally:
            await engine.disconnect()

    async def test_create_index_enforces_max_time_ms_deadline(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            with patch(
                "mongoeco.engines.memory.enforce_deadline",
                side_effect=ExecutionTimeout("operation exceeded time limit"),
            ):
                with self.assertRaises(ExecutionTimeout):
                    await engine.create_index(
                        "db",
                        "coll",
                        ["email"],
                        unique=True,
                        max_time_ms=1,
                    )
        finally:
            await engine.disconnect()

    async def test_engine_compiles_filter_with_requested_dialect_when_plan_is_omitted(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "legacy", "v": UNDEFINED})
            count = await self._count(
                engine,
                "db",
                "coll",
                {"v": None},
                dialect=MONGODB_DIALECT_80,
            )
        finally:
            await engine.disconnect()

        self.assertEqual(count, 0)

    async def test_delete_document_returns_false_when_id_is_missing(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            deleted = await engine.delete_document("db", "coll", "missing")
        finally:
            await engine.disconnect()

        self.assertFalse(deleted)

    async def test_update_matching_document_returns_zero_when_no_match_and_no_upsert(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            result = await self._update(
                engine,
                "db",
                "coll",
                {"kind": "missing"},
                {"$set": {"done": True}},
            )
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertIsNone(result.upserted_id)

    async def test_update_matching_document_can_upsert_from_seed(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            result = await self._update(
                engine,
                "db",
                "coll",
                {"kind": "view"},
                {"$set": {"done": True}},
                upsert=True,
                upsert_seed={"kind": "view", "meta": {"source": "seed"}},
            )
            found = await engine.get_document("db", "coll", result.upserted_id)
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertEqual(
            found,
            {"_id": result.upserted_id, "kind": "view", "meta": {"source": "seed"}, "done": True},
        )

    async def test_update_matching_document_raises_duplicate_on_upsert_id_collision(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "dup", "kind": "other"})

            with self.assertRaises(DuplicateKeyError):
                await self._update(
                    engine,
                    "db",
                    "coll",
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    upsert=True,
                    upsert_seed={"_id": "dup", "kind": "view"},
                )
        finally:
            await engine.disconnect()

    async def test_update_matching_document_upsert_raises_duplicate_on_secondary_unique_index_collision(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=True)

            with self.assertRaises(DuplicateKeyError):
                await self._update(
                    engine,
                    "db",
                    "coll",
                    {"kind": "missing"},
                    {"$set": {"email": "a@example.com"}},
                    upsert=True,
                    upsert_seed={"kind": "missing"},
                )
        finally:
            await engine.disconnect()

    async def test_engine_supports_datetime_ids_via_storage_key_normalization(self):
        engine = MemoryEngine()
        doc_id = datetime.datetime(2026, 3, 23, 12, 0, 0)
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": doc_id, "kind": "event"})
            found = await engine.get_document("db", "coll", doc_id)
        finally:
            await engine.disconnect()

        self.assertEqual(found, {"_id": doc_id, "kind": "event"})

    async def test_engine_supports_list_ids_via_storage_key_normalization(self):
        engine = MemoryEngine()
        doc_id = ["tenant", 1]
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": doc_id, "kind": "event"})
            found = await engine.get_document("db", "coll", doc_id)
        finally:
            await engine.disconnect()

        self.assertEqual(found, {"_id": doc_id, "kind": "event"})

    async def test_engine_distinguishes_bool_and_int_ids(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": True, "kind": "bool"})
            await engine.put_document("db", "coll", {"_id": 1, "kind": "int"})
            bool_doc = await engine.get_document("db", "coll", True)
            int_doc = await engine.get_document("db", "coll", 1)
        finally:
            await engine.disconnect()

        self.assertEqual(bool_doc, {"_id": True, "kind": "bool"})
        self.assertEqual(int_doc, {"_id": 1, "kind": "int"})

    async def test_typed_engine_key_covers_common_scalar_types(self):
        engine = MemoryEngine()

        self.assertEqual(engine._typed_engine_key(None), ("none", None))
        self.assertEqual(engine._typed_engine_key(1.5), ("float", 1.5))
        self.assertEqual(engine._typed_engine_key(b"abc"), ("bytes", b"abc"))
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        self.assertEqual(engine._typed_engine_key(session_id), ("uuid", session_id))

    async def test_engine_falls_back_to_repr_for_unhashable_unknown_id_types(self):
        engine = MemoryEngine()
        doc_id = _UnhashableValue()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": doc_id, "kind": "event"})
            found = await engine.get_document("db", "coll", doc_id)
        finally:
            await engine.disconnect()

        self.assertEqual(found, {"_id": doc_id, "kind": "event"})

    async def test_get_document_uses_is_none_check_for_storage_payload(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            with engine._meta_lock:
                engine._storage.setdefault("db", {}).setdefault("coll", {})[engine._storage_key("empty")] = {}
            found = await engine.get_document("db", "coll", "empty")
        finally:
            await engine.disconnect()

        self.assertEqual(found, {})

    async def test_create_index_is_idempotent_and_list_indexes_returns_copy(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            name1 = await engine.create_index("db", "coll", ["kind"], unique=False)
            name2 = await engine.create_index("db", "coll", ["kind"], unique=False)
            indexes = await engine.list_indexes("db", "coll")
            indexes[0]["name"] = "mutated"
            indexes_again = await engine.list_indexes("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(name1, "kind_1")
        self.assertEqual(name2, "kind_1")
        self.assertEqual(
            indexes_again,
            [
                {"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True},
                {"name": "kind_1", "fields": ["kind"], "key": {"kind": 1}, "unique": False},
            ],
        )

    async def test_create_index_rejects_conflicting_definition_for_existing_name(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["kind"], unique=False, name="idx")

            with self.assertRaises(OperationFailure):
                await engine.create_index("db", "coll", ["kind"], unique=True, name="idx")

            with self.assertRaises(OperationFailure):
                await engine.create_index("db", "coll", ["other"], unique=False, name="idx")
        finally:
            await engine.disconnect()

    async def test_create_unique_index_rejects_existing_duplicates(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.put_document("db", "coll", {"_id": "2", "email": "a@example.com"})

            with self.assertRaises(DuplicateKeyError):
                await engine.create_index("db", "coll", ["email"], unique=True)
        finally:
            await engine.disconnect()

    async def test_unique_index_is_enforced_on_put_and_update(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=True)

            with self.assertRaises(DuplicateKeyError):
                await engine.put_document("db", "coll", {"_id": "2", "email": "a@example.com"})

            await engine.put_document("db", "coll", {"_id": "2", "email": "b@example.com"})
            with self.assertRaises(DuplicateKeyError):
                await self._update(
                    engine,
                    "db",
                    "coll",
                    {"_id": "2"},
                    {"$set": {"email": "a@example.com"}},
                )
        finally:
            await engine.disconnect()

    async def test_sparse_unique_index_ignores_missing_values_but_rejects_duplicates(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], unique=True, sparse=True)
            await engine.put_document("db", "coll", {"_id": "1"})
            await engine.put_document("db", "coll", {"_id": "2"})
            await engine.put_document("db", "coll", {"_id": "3", "email": "a@example.com"})

            with self.assertRaises(DuplicateKeyError):
                await engine.put_document("db", "coll", {"_id": "4", "email": "a@example.com"})
        finally:
            await engine.disconnect()

    async def test_partial_unique_index_allows_duplicates_outside_filter(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                unique=True,
                partial_filter_expression={"active": True},
            )
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com", "active": False})
            await engine.put_document("db", "coll", {"_id": "2", "email": "a@example.com", "active": False})
            await engine.put_document("db", "coll", {"_id": "3", "email": "a@example.com", "active": True})

            with self.assertRaises(DuplicateKeyError):
                await engine.put_document("db", "coll", {"_id": "4", "email": "a@example.com", "active": True})
        finally:
            await engine.disconnect()

    async def test_unique_index_distinguishes_bool_and_int_values(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["flag"], unique=True)
            await engine.put_document("db", "coll", {"_id": "1", "flag": True})
            await engine.put_document("db", "coll", {"_id": "2", "flag": 1})
            first = await engine.get_document("db", "coll", "1")
            second = await engine.get_document("db", "coll", "2")
        finally:
            await engine.disconnect()

        self.assertEqual(first, {"_id": "1", "flag": True})
        self.assertEqual(second, {"_id": "2", "flag": 1})

    async def test_disconnect_resets_engine_state_when_last_connection_closes(self):
        engine = MemoryEngine()
        await engine.connect()
        await engine.put_document("db", "coll", {"_id": "1"})
        await engine.create_index("db", "coll", ["kind"])
        await engine.disconnect()

        await engine.connect()
        try:
            self.assertIsNone(await engine.get_document("db", "coll", "1"))
            self.assertEqual(
                await engine.list_indexes("db", "coll"),
                [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}],
            )
        finally:
            await engine.disconnect()

    async def test_drop_collection_removes_index_metadata(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "event"})
            await engine.create_index("db", "coll", ["kind"])
            await engine.drop_collection("db", "coll")
            found = await engine.get_document("db", "coll", "1")
            indexes = await engine.list_indexes("db", "coll")
        finally:
            await engine.disconnect()

        self.assertIsNone(found)
        self.assertEqual(indexes, [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}])

    async def test_drop_collection_prunes_empty_database_metadata(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "event"})
            await engine.create_index("db", "coll", ["kind"])

            await engine.drop_collection("db", "coll")

            databases = await engine.list_databases()
            collections = await engine.list_collections("db")
        finally:
            await engine.disconnect()

        self.assertEqual(databases, [])
        self.assertEqual(collections, [])

    async def test_index_information_and_drop_index_support_key_specs(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", [("kind", 1), ("rank", -1)], unique=True)
            info = await engine.index_information("db", "coll")
            await engine.drop_index("db", "coll", [("kind", 1), ("rank", -1)])
            indexes = await engine.list_indexes("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            info,
            {
                "_id_": {"key": [("_id", 1)], "unique": True},
                "kind_1_rank_-1": {"key": [("kind", 1), ("rank", -1)], "unique": True},
            },
        )
        self.assertEqual(indexes, [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}])

    async def test_list_indexes_and_index_information_include_virtual_index_metadata(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                sparse=True,
                partial_filter_expression={"active": True},
            )
            indexes = await engine.list_indexes("db", "coll")
            info = await engine.index_information("db", "coll")
        finally:
            await engine.disconnect()

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

    async def test_hint_rejects_partial_index_when_query_does_not_imply_filter(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com", "active": False})
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                partial_filter_expression={"active": True},
                name="idx_email_active",
            )
            with self.assertRaises(OperationFailure):
                await self._explain(
                    engine,
                    "db",
                    "coll",
                    {"email": "a@example.com"},
                    hint="idx_email_active",
                )
        finally:
            await engine.disconnect()

    async def test_drop_index_prunes_index_data(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], name="email_1")
            self.assertIn("email_1", engine._index_data["db"]["coll"])

            await engine.drop_index("db", "coll", "email_1")
            self.assertNotIn("db", engine._index_data)
        finally:
            await engine.disconnect()

    async def test_drop_indexes_prunes_collection_index_data(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], name="email_1")
            await engine.create_index("db", "coll", ["_id", "email"], name="id_email_1")

            await engine.drop_indexes("db", "coll")
            self.assertNotIn("db", engine._index_data)
        finally:
            await engine.disconnect()

    async def test_rename_collection_moves_index_data(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], name="email_1")

            await engine.rename_collection("db", "coll", "renamed")
            self.assertNotIn("coll", engine._index_data.get("db", {}))
            self.assertIn("renamed", engine._index_data.get("db", {}))
            self.assertIn("email_1", engine._index_data["db"]["renamed"])
        finally:
            await engine.disconnect()

    async def test_drop_collection_prunes_index_data(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], name="email_1")

            await engine.drop_collection("db", "coll")
            self.assertNotIn("db", engine._index_data)
        finally:
            await engine.disconnect()

    async def test_delete_document_prunes_unique_index_cache(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=True)

            deleted = await engine.delete_document("db", "coll", "1")
            await engine.put_document("db", "coll", {"_id": "2", "email": "a@example.com"})
        finally:
            await engine.disconnect()

        self.assertTrue(deleted)

    async def test_delete_with_operation_prunes_unique_index_cache(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=True)

            result = await self._delete(engine, "db", "coll", {"_id": "1"})
            await engine.put_document("db", "coll", {"_id": "2", "email": "a@example.com"})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 1)

    async def test_update_with_operation_refreshes_index_cache_for_updated_key(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], name="email_1")

            result = await self._update(
                engine,
                "db",
                "coll",
                {"_id": "1"},
                {"$set": {"email": "c@example.com"}},
            )
            old_matches = [doc async for doc in self._scan(engine, "db", "coll", {"email": "a@example.com"})]
            new_matches = [doc async for doc in self._scan(engine, "db", "coll", {"email": "c@example.com"})]
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(old_matches, [])
        self.assertEqual(new_matches, [{"_id": "1", "email": "c@example.com"}])

    async def test_update_with_operation_upsert_populates_index_cache(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], name="email_1", unique=True)

            result = await self._update(
                engine,
                "db",
                "coll",
                {"kind": "missing"},
                {"$set": {"email": "a@example.com"}},
                upsert=True,
                upsert_seed={"kind": "missing"},
            )
            matches = [doc async for doc in self._scan(engine, "db", "coll", {"email": "a@example.com"})]
        finally:
            await engine.disconnect()

        self.assertIsNotNone(result.upserted_id)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["email"], "a@example.com")

    async def test_builtin_id_index_cannot_be_dropped(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            with self.assertRaises(OperationFailure):
                await engine.drop_index("db", "coll", "_id_")
            with self.assertRaises(OperationFailure):
                await engine.drop_index("db", "coll", [("_id", 1)])
        finally:
            await engine.disconnect()

    async def test_disconnect_is_noop_when_engine_is_not_connected(self):
        engine = MemoryEngine()

        await engine.disconnect()

        self.assertEqual(engine._connection_count, 0)

    async def test_engine_accepts_injected_codec(self):
        calls: list[str] = []

        class TrackingCodec(DocumentCodec):
            @staticmethod
            def encode(data):
                calls.append("encode")
                return DocumentCodec.encode(data)

            @staticmethod
            def decode(data):
                calls.append("decode")
                return DocumentCodec.decode(data)

        engine = MemoryEngine(codec=TrackingCodec)
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "name": "Ada"})
            await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertIn("encode", calls)
        self.assertIn("decode", calls)

    async def test_drop_collection_preserves_collection_lock_entry(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1"})
            original_lock = engine._locks["db.coll"]
            self.assertIn("db.coll", engine._locks)

            await engine.drop_collection("db", "coll")
            self.assertIs(engine._locks["db.coll"], original_lock)
        finally:
            await engine.disconnect()

    async def test_drop_collection_keeps_same_lock_for_future_callers(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1"})
            original_lock = engine._get_lock("db", "coll")

            await engine.drop_collection("db", "coll")

            recreated_lock = engine._get_lock("db", "coll")
            self.assertIs(original_lock, recreated_lock)
            self.assertIs(engine._locks["db.coll"], recreated_lock)
        finally:
            await engine.disconnect()

    async def test_index_value_returns_none_when_field_is_missing(self):
        engine = MemoryEngine()

        self.assertIsNone(engine._index_value({"name": "Ada"}, "missing"))

    async def test_index_value_normalizes_list_values(self):
        engine = MemoryEngine()

        self.assertEqual(
            engine._index_value({"tags": ["python", {"level": 1}]}, "tags"),
            engine._typed_engine_key(["python", {"level": 1}]),
        )

    async def test_delete_matching_document_returns_zero_when_no_match(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            result = await self._delete(engine, "db", "coll", {"kind": "click"})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 0)

    async def test_count_matching_documents_counts_matching_rows(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "3", "kind": "click"})
            count = await self._count(engine, "db", "coll", {"kind": "view"})
        finally:
            await engine.disconnect()

        self.assertEqual(count, 2)

    async def test_ensure_unique_indexes_ignores_non_unique_and_excluded_rows(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=False)
            engine._ensure_unique_indexes("db", "coll", {"_id": "1", "email": "a@example.com"})

            await engine.create_index("db", "coll", ["_id"], unique=True)
            engine._ensure_unique_indexes(
                "db",
                "coll",
                {"_id": "1", "email": "changed@example.com"},
                exclude_storage_key="1",
            )
        finally:
            await engine.disconnect()

    async def test_scan_collection_applies_sort_skip_and_limit(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": 3})
            await engine.put_document("db", "coll", {"_id": "2", "rank": 1})
            await engine.put_document("db", "coll", {"_id": "3", "rank": 2})

            documents = [
                doc async for doc in self._scan(
                    engine,
                    "db",
                    "coll",
                    sort=[("rank", 1)],
                    skip=1,
                    limit=1,
                )
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "3", "rank": 2}])

    async def test_scan_collection_streams_skip_limit_and_projection_without_sort(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": 3, "kind": "a"})
            await engine.put_document("db", "coll", {"_id": "2", "rank": 1, "kind": "b"})
            await engine.put_document("db", "coll", {"_id": "3", "rank": 2, "kind": "c"})

            documents = [
                doc async for doc in self._scan(
                    engine,
                    "db",
                    "coll",
                    projection={"_id": 0, "kind": 1},
                    skip=1,
                    limit=1,
                )
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"kind": "b"}])

    async def test_scan_collection_preserves_storage_order_when_using_index_cache(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view", "tag": "python"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "view", "tag": "mongodb"})
            await engine.put_document("db", "coll", {"_id": "3", "kind": "click", "tag": "python"})
            await engine.create_index("db", "coll", ["kind"], name="kind_idx")

            documents = [
                doc async for doc in self._scan(
                    engine,
                    "db",
                    "coll",
                    {"kind": "view"},
                    hint="kind_idx",
                )
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(
            documents,
            [
                {"_id": "1", "kind": "view", "tag": "python"},
                {"_id": "2", "kind": "view", "tag": "mongodb"},
            ],
        )

    async def test_scan_collection_sorts_array_fields_by_min_and_max_element(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": [3, 8]})
            await engine.put_document("db", "coll", {"_id": "2", "rank": [1, 9]})
            await engine.put_document("db", "coll", {"_id": "3", "rank": [2, 4]})

            ascending = [
                doc["_id"]
                async for doc in self._scan(engine, "db", "coll", sort=[("rank", 1)])
            ]
            descending = [
                doc["_id"]
                async for doc in self._scan(engine, "db", "coll", sort=[("rank", -1)])
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(ascending, ["2", "3", "1"])
        self.assertEqual(descending, ["2", "1", "3"])

    async def test_scan_collection_keeps_array_sort_semantics_even_when_sort_field_is_indexed(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": [3, 8]})
            await engine.put_document("db", "coll", {"_id": "2", "rank": [1, 9]})
            await engine.put_document("db", "coll", {"_id": "3", "rank": [2, 4]})
            await engine.create_index("db", "coll", ["rank"], name="rank_idx")

            ascending = [
                doc["_id"]
                async for doc in self._scan(engine, "db", "coll", sort=[("rank", 1)])
            ]
            descending = [
                doc["_id"]
                async for doc in self._scan(engine, "db", "coll", sort=[("rank", -1)])
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(ascending, ["2", "3", "1"])
        self.assertEqual(descending, ["2", "1", "3"])

    async def test_scan_collection_sorts_scalar_field_when_sort_field_is_indexed(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": 3})
            await engine.put_document("db", "coll", {"_id": "2", "rank": 1})
            await engine.put_document("db", "coll", {"_id": "3", "rank": 2})
            await engine.create_index("db", "coll", ["rank"], name="rank_idx")

            ascending = [
                doc["_id"]
                async for doc in self._scan(engine, "db", "coll", sort=[("rank", 1)])
            ]
            descending = [
                doc["_id"]
                async for doc in self._scan(engine, "db", "coll", sort=[("rank", -1)])
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(ascending, ["2", "3", "1"])
        self.assertEqual(descending, ["1", "3", "2"])

    async def test_scan_collection_rejects_negative_skip_and_limit(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            with self.assertRaises(ValueError):
                async for _ in self._scan(engine, "db", "coll", skip=-1):
                    pass

            with self.assertRaises(ValueError):
                async for _ in self._scan(engine, "db", "coll", limit=-1):
                    pass
        finally:
            await engine.disconnect()

    async def test_list_databases_and_collections_reflect_structural_changes(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db1", "coll1", {"_id": "1"})
            await engine.put_document("db1", "coll2", {"_id": "2"})
            await engine.put_document("db2", "coll3", {"_id": "3"})

            self.assertEqual(set(await engine.list_databases()), {"db1", "db2"})
            self.assertEqual(set(await engine.list_collections("db1")), {"coll1", "coll2"})

            await engine.drop_collection("db1", "coll2")

            self.assertEqual(set(await engine.list_collections("db1")), {"coll1"})
        finally:
            await engine.disconnect()

    async def test_list_databases_and_collections_include_index_only_namespaces(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "empty", ["email"])
            self.assertEqual(await engine.list_databases(), ["db"])
            self.assertEqual(await engine.list_collections("db"), ["empty"])
        finally:
            await engine.disconnect()

    async def test_create_collection_registers_empty_namespace_and_rejects_duplicates(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_collection("db", "empty", options={"capped": True})
            self.assertEqual(await engine.list_databases(), ["db"])
            self.assertEqual(await engine.list_collections("db"), ["empty"])
            self.assertEqual(
                await engine.collection_options("db", "empty"),
                {"capped": True},
            )
            with self.assertRaises(CollectionInvalid):
                await engine.create_collection("db", "empty")
        finally:
            await engine.disconnect()

    async def test_delete_last_document_does_not_remove_collection_metadata(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1"})
            await engine.delete_document("db", "coll", "1")
            self.assertEqual(await engine.list_collections("db"), ["coll"])
        finally:
            await engine.disconnect()

    async def test_update_without_match_and_without_upsert_does_not_create_collection(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            result = await self._update(engine, "db", "ghosts", {"_id": "1"}, {"$set": {"v": 1}})
            self.assertEqual(result.matched_count, 0)
            self.assertEqual(await engine.list_databases(), [])
            self.assertEqual(await engine.list_collections("db"), [])
        finally:
            await engine.disconnect()

    async def test_rename_collection_moves_namespace_metadata(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_collection("db", "events", options={"capped": True})
            await engine.put_document("db", "events", {"_id": "1"})
            await engine.create_index("db", "events", ["kind"], name="kind_idx")

            await engine.rename_collection("db", "events", "archived")

            self.assertEqual(await engine.list_collections("db"), ["archived"])
            self.assertEqual(await engine.get_document("db", "archived", "1"), {"_id": "1"})
            self.assertIn("kind_idx", await engine.index_information("db", "archived"))
            self.assertEqual(await engine.collection_options("db", "archived"), {"capped": True})
        finally:
            await engine.disconnect()

    async def test_scan_and_count_prefer_explicit_plan_over_conflicting_filter(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "click"})
            plan = compile_filter({"kind": "view"})

            documents = [
                doc
                async for doc in self._scan(engine, "db", "coll", {"kind": "click"}, plan=plan)
            ]
            count = await self._count(engine, "db", "coll", {"kind": "click"}, plan=plan)
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "kind": "view"}])
        self.assertEqual(count, 1)

    async def test_scan_collection_short_circuits_match_all(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            with patch("mongoeco.engines.memory.QueryEngine.match_plan", side_effect=AssertionError("match_plan")):
                documents = [doc async for doc in self._scan(engine, "db", "coll", plan=MatchAll())]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "kind": "view"}])
