import datetime
import time
import unittest
import uuid
from unittest.mock import patch

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.search import SearchVectorQuery, compile_search_stage
from mongoeco.core.query_plan import MatchAll, compile_filter
from mongoeco.engines import memory as memory_module
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, ExecutionTimeout, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import EngineIndexRecord, SearchIndexDefinition, UNDEFINED


class _UnhashableValue:
    __hash__ = None

    def __repr__(self) -> str:
        return "unhashable-value"


class _CodecWithoutSignature:
    @staticmethod
    def decode(payload):
        return payload


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

    async def test_scan_returns_isolated_documents_when_storage_uses_borrowed_decode_cache(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "tags": ["a"]})
            documents = [doc async for doc in self._scan(engine, "db", "coll", {"_id": "1"})]
            documents[0]["tags"].append("b")
            stored = await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "tags": ["a", "b"]}])
        self.assertEqual(stored, {"_id": "1", "tags": ["a"]})

    def test_decode_storage_document_propagates_codec_type_errors(self):
        engine = MemoryEngine()

        with patch.object(engine._codec, "decode", side_effect=TypeError("broken")):
            with self.assertRaisesRegex(TypeError, "broken"):
                engine._decode_storage_document({"wrapped": {"$mongoeco": {"type": "int32", "value": 1}}})

    async def test_sorted_scan_returns_isolated_documents_when_storage_uses_borrowed_decode_cache(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": 2, "tags": ["a"]})
            await engine.put_document("db", "coll", {"_id": "2", "rank": 1, "tags": ["x"]})
            documents = [
                doc
                async for doc in self._scan(
                    engine,
                    "db",
                    "coll",
                    sort=[("rank", 1)],
                    limit=1,
                )
            ]
            documents[0]["tags"].append("y")
            stored = await engine.get_document("db", "coll", "2")
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "2", "rank": 1, "tags": ["x", "y"]}])
        self.assertEqual(stored, {"_id": "2", "rank": 1, "tags": ["x"]})

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
                {"name": "_id_", "key": {"_id": 1}, "unique": True},
                {"name": "kind_1", "key": {"kind": 1}, "unique": False},
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
                [{"name": "_id_", "key": {"_id": 1}, "unique": True}],
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
        self.assertEqual(indexes, [{"name": "_id_", "key": {"_id": 1}, "unique": True}])

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
        self.assertEqual(indexes, [{"name": "_id_", "key": {"_id": 1}, "unique": True}])

    async def test_drop_index_by_key_pattern_rejects_ambiguous_aliases(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], name="email_primary")
            await engine.create_index("db", "coll", ["email"], name="email_alias")

            with self.assertRaisesRegex(OperationFailure, "multiple indexes found with key pattern"):
                await engine.drop_index("db", "coll", [("email", 1)])

            await engine.drop_index("db", "coll", "email_primary")
            indexes = await engine.list_indexes("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes,
            [
                {"name": "_id_", "key": {"_id": 1}, "unique": True},
                {"name": "email_alias", "key": {"email": 1}, "unique": False},
            ],
        )

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
                "key": {"email": 1},
                "unique": False,
                "sparse": True,
                "partialFilterExpression": {"active": True},
            },
        )

    async def test_list_indexes_and_index_information_include_hidden_metadata(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], hidden=True, name="email_hidden")
            indexes = await engine.list_indexes("db", "coll")
            info = await engine.index_information("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes[1],
            {
                "name": "email_hidden",
                "key": {"email": 1},
                "unique": False,
                "hidden": True,
            },
        )
        self.assertTrue(info["email_hidden"]["hidden"])

    async def test_ttl_index_metadata_and_opportunistic_expiration_round_trip(self):
        engine = MemoryEngine()
        past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=120)
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "expired", "expires_at": past, "name": "old"})
            await engine.put_document("db", "coll", {"_id": "fresh", "expires_at": future, "name": "new"})
            await engine.create_index("db", "coll", ["expires_at"], expire_after_seconds=30)
            indexes = await engine.list_indexes("db", "coll")
            info = await engine.index_information("db", "coll")
            found = await engine.get_document("db", "coll", "expired")
            remaining = [document async for document in self._scan(engine, "db", "coll", {})]
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes[1],
            {
                "name": "expires_at_1",
                "key": {"expires_at": 1},
                "unique": False,
                "expireAfterSeconds": 30,
            },
        )
        self.assertEqual(
            info["expires_at_1"],
            {"key": [("expires_at", 1)], "expireAfterSeconds": 30},
        )
        self.assertIsNone(found)
        self.assertEqual(remaining, [{"_id": "fresh", "expires_at": future, "name": "new"}])

    def test_ttl_helpers_cover_invalid_values_naive_datetimes_and_empty_collections(self):
        engine = MemoryEngine()
        now = datetime.datetime(2026, 4, 1, tzinfo=datetime.timezone.utc)
        ttl_index = EngineIndexRecord(
            name="expires_at_1",
            fields=["expires_at"],
            key=[("expires_at", 1)],
            unique=False,
            expire_after_seconds=30,
        )
        invalid_index = EngineIndexRecord(
            name="compound",
            fields=["expires_at", "other"],
            key=[("expires_at", 1), ("other", 1)],
            unique=False,
            expire_after_seconds=30,
        )

        self.assertIsNone(engine._coerce_ttl_datetime("2026-04-01"))
        self.assertEqual(
            engine._coerce_ttl_datetime(datetime.datetime(2026, 4, 1, 12, 0)),
            datetime.datetime(2026, 4, 1, 12, 0, tzinfo=datetime.timezone.utc),
        )
        self.assertFalse(
            engine._document_expired_by_ttl(
                {"expires_at": "not-a-date"},
                ttl_index,
                now=now,
            )
        )
        self.assertFalse(
            engine._document_expired_by_ttl(
                {"expires_at": now},
                invalid_index,
                now=now,
            )
        )
        self.assertEqual(
            engine._purge_expired_documents_locked(
                "db",
                "missing",
                indexes_view={"db": {"missing": [ttl_index]}},
                storage_view={"db": {"missing": {}}},
                index_data_view={"db": {"missing": {}}},
            ),
            0,
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

    async def test_profile_settings_make_system_profile_namespace_visible(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            engine._profiler.set_level("db", 1)

            self.assertEqual(await engine.list_databases(), ["db"])
            self.assertEqual(await engine.list_collections("db"), ["system.profile"])
            self.assertEqual(await engine.collection_options("db", "system.profile"), {})
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

    async def test_explain_search_documents_reports_pending_status(self):
        engine = MemoryEngine(simulate_search_index_latency=60.0)
        await engine.connect()
        try:
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {"title": {"type": "string"}},
                        }
                    },
                    name="by_text",
                ),
            )

            explanation = await engine.explain_search_documents(
                "db",
                "coll",
                "$search",
                {"index": "by_text", "text": {"query": "ada", "path": "title"}},
            )
        finally:
            await engine.disconnect()

        self.assertEqual(explanation.hinted_index, "by_text")
        self.assertEqual(explanation.details["status"], "PENDING")
        self.assertTrue(explanation.details["backendAvailable"])
        self.assertFalse(explanation.details["backendMaterialized"])
        self.assertIsNone(explanation.details["physicalName"])
        self.assertIsNone(explanation.details["fts5Available"])
        self.assertIsNotNone(explanation.details["readyAtEpoch"])

    async def test_memory_runtime_diagnostics_surface_planner_search_and_cache_state(self):
        engine = MemoryEngine(simulate_search_index_latency=60.0)
        await engine.connect()
        try:
            await engine.create_collection("db", "docs", options={"capped": True})
            await engine.create_search_index(
                "db",
                "docs",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text"),
            )
            runtime = engine._runtime_diagnostics_info()
        finally:
            await engine.disconnect()

        self.assertEqual(runtime["planner"]["engine"], "python")
        self.assertEqual(runtime["planner"]["pushdownModes"], ["python"])
        self.assertEqual(runtime["search"]["backend"], "python")
        self.assertEqual(runtime["search"]["declaredIndexCount"], 1)
        self.assertEqual(runtime["search"]["pendingIndexCount"], 1)
        self.assertGreaterEqual(runtime["caches"]["trackedCollections"], 1)
        self.assertEqual(runtime["search"]["fts5Available"], None)

    async def test_search_index_readiness_and_drop_cleanup_remove_transient_state(self):
        engine = MemoryEngine(simulate_search_index_latency=0.01)
        await engine.connect()
        try:
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text"),
            )

            self.assertFalse(engine._search_index_is_ready("db", "coll", "by_text"))
            engine._search_index_ready_at[("db", "coll", "by_text")] = time.time() - 1
            self.assertTrue(engine._search_index_is_ready("db", "coll", "by_text"))
            self.assertNotIn(("db", "coll", "by_text"), engine._search_index_ready_at)

            await engine.drop_search_index("db", "coll", "by_text")
            self.assertNotIn("db", engine._search_indexes)
        finally:
            await engine.disconnect()

    def test_decode_codec_payload_tolerates_signature_lookup_failures(self):
        engine = MemoryEngine(codec=_CodecWithoutSignature)
        with patch("mongoeco.engines.memory.inspect.signature", side_effect=ValueError("no signature")):
            self.assertEqual(
                engine._decode_codec_payload({"_id": "1"}, preserve_bson_wrappers=False),
                {"_id": "1"},
            )

    def test_decode_storage_document_supports_public_decode_from_stored_payload(self):
        engine = MemoryEngine()
        payload = engine._encode_storage_document({"_id": "1", "kind": "view"})
        self.assertEqual(
            engine._decode_storage_document(payload, preserve_bson_wrappers=False),
            {"_id": "1", "kind": "view"},
        )

    async def test_resolve_hint_index_supports_builtin_id_by_key_pattern(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            hinted = engine._resolve_hint_index("db", "coll", [("_id", 1)])
        finally:
            await engine.disconnect()
        self.assertEqual(hinted["name"], "_id_")

    async def test_search_documents_and_explain_reject_wrong_search_index_types(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
                    name="vec",
                    index_type="vectorSearch",
                ),
            )
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="text", index_type="search"),
            )

            with self.assertRaisesRegex(OperationFailure, "does not support \\$search"):
                await engine.search_documents(
                    "db",
                    "coll",
                    "$search",
                    {"index": "vec", "text": {"query": "ada", "path": "title"}},
                )
            with self.assertRaisesRegex(OperationFailure, "search index not found"):
                await engine.search_documents(
                    "db",
                    "coll",
                    "$search",
                    {"index": "missing", "text": {"query": "ada", "path": "title"}},
                )
            with self.assertRaisesRegex(OperationFailure, "does not support \\$vectorSearch"):
                await engine.explain_search_documents(
                    "db",
                    "coll",
                    "$vectorSearch",
                    {
                        "index": "text",
                        "path": "embedding",
                        "queryVector": [0.1, 0.2],
                        "numCandidates": 10,
                        "limit": 5,
                    },
                )
        finally:
            await engine.disconnect()

    async def test_drop_collection_clears_profile_and_search_index_state(self):
        engine = MemoryEngine(simulate_search_index_latency=60.0)
        await engine.connect()
        try:
            await engine.set_profiling_level("db", 2)
            engine._profiler.record(
                "db",
                op="query",
                namespace="db.events",
                command={"find": "events"},
                duration_micros=10_000,
            )
            self.assertGreater(engine._profiler.count_entries("db"), 0)
            await engine.create_collection("db", "coll")
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="by_text"),
            )
            self.assertIn(("db", "coll", "by_text"), engine._search_index_ready_at)

            await engine.drop_collection("db", "system.profile")
            await engine.drop_collection("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(engine._profiler.count_entries("db"), 0)
        self.assertNotIn("db", engine._search_indexes)
        self.assertNotIn(("db", "coll", "by_text"), engine._search_index_ready_at)

    def test_memory_session_mvcc_helpers_cover_commit_abort_and_views(self):
        engine = MemoryEngine()
        session = ClientSession()
        other = ClientSession()
        engine.create_session_state(session)

        engine._sync_session_state(other, transaction_active=True, snapshot_version=7)
        state = session.get_engine_context(f"memory:{id(engine)}")
        self.assertIsNotNone(state)

        session.start_transaction()
        engine._start_session_transaction(session)
        self.assertIsNotNone(engine._active_mvcc_state(session))
        self.assertIsNone(engine._active_mvcc_state(other))

        snapshot = engine._mvcc_states[session.session_id]
        snapshot.storage.setdefault("db", {}).setdefault("coll", {})["1"] = {"_id": "1"}
        engine._commit_session_transaction(session)
        self.assertEqual(engine._storage["db"]["coll"]["1"], {"_id": "1"})
        self.assertIsNone(engine._active_mvcc_state(session))

        engine._start_session_transaction(session)
        engine._abort_session_transaction(session)
        self.assertIsNone(engine._active_mvcc_state(session))

    def test_memory_index_helpers_cover_default_views_and_delete_cleanup(self):
        engine = MemoryEngine()
        index = EngineIndexRecord(name="kind_1", fields=["kind"], key=[("kind", 1)], unique=False)
        engine._storage = {"db": {"coll": {"1": {"_id": "1", "kind": "view"}}}}
        engine._indexes = {"db": {"coll": [index]}}

        engine._rebuild_index_data_locked("db", "coll", index)
        self.assertEqual(
            engine._index_data["db"]["coll"]["kind_1"][(("str", "view"),)],
            {"1"},
        )

        engine._update_indexes_locked("db", "coll", "1", {"_id": "1", "kind": "view"}, action="delete")
        self.assertEqual(engine._index_data["db"]["coll"]["kind_1"], {})

    async def test_memory_hint_resolution_by_key_pattern_rejects_unusable_index(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.create_index(
                "db",
                "coll",
                ["kind"],
                name="kind_partial",
                partial_filter_expression={"kind": "click"},
            )
            with self.assertRaisesRegex(OperationFailure, "usable index"):
                engine._resolve_hint_index(
                    "db",
                    "coll",
                    [("kind", 1)],
                    plan=compile_filter({"kind": "view"}),
                )
        finally:
            await engine.disconnect()

    async def test_memory_unique_index_fallback_scan_detects_unhashable_duplicate(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["value"], name="value_unique", unique=True)
            document = {"_id": "1", "value": _UnhashableValue()}
            await engine.put_document("db", "coll", document)
            with self.assertRaises(DuplicateKeyError):
                engine._ensure_unique_indexes(
                    "db",
                    "coll",
                    {"_id": "2", "value": _UnhashableValue()},
                )
        finally:
            await engine.disconnect()

    async def test_memory_search_index_lifecycle_conflict_update_missing_and_phrase_vector_paths(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="search")
            await engine.create_search_index("db", "coll", definition)
            self.assertEqual(await engine.list_search_indexes("db", "coll"), [definition.to_document()])

            with self.assertRaisesRegex(OperationFailure, "Conflicting search index definition"):
                await engine.create_search_index(
                    "db",
                    "coll",
                    SearchIndexDefinition({"mappings": {"dynamic": False}}, name="search"),
                )

            with self.assertRaisesRegex(OperationFailure, "search index not found"):
                await engine.update_search_index(
                    "db",
                    "coll",
                    "missing",
                    SearchIndexDefinition({"mappings": {"dynamic": True}}, name="missing"),
                )

            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada Lovelace", "embedding": [0.0, 1.0]})
            phrase_hits = await engine.search_documents(
                "db",
                "coll",
                "$search",
                {"index": "search", "phrase": {"query": "Ada Lovelace", "path": "title"}},
            )
            self.assertEqual([doc["_id"] for doc in phrase_hits], ["1"])

            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
                    name="vec",
                    index_type="vectorSearch",
                ),
            )
            vector_hits = await engine.search_documents(
                "db",
                "coll",
                "$vectorSearch",
                {
                    "index": "vec",
                    "path": "embedding",
                    "queryVector": [0.0, 1.0],
                    "numCandidates": 5,
                    "limit": 5,
                },
            )
            self.assertEqual([doc["_id"] for doc in vector_hits], ["1"])
        finally:
            await engine.disconnect()

    async def test_memory_search_runtime_cache_is_invalidated_after_document_write(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            definition = SearchIndexDefinition(
                {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}},
                name="search",
            )
            await engine.create_search_index("db", "coll", definition)
            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada"})

            first_hits = await engine.search_documents(
                "db",
                "coll",
                "$search",
                {"index": "search", "text": {"query": "ada", "path": "title"}},
            )
            self.assertEqual([doc["_id"] for doc in first_hits], ["1"])
            self.assertIn(("db", "coll", "search"), engine._search_document_cache)

            await engine.put_document("db", "coll", {"_id": "1", "title": "Grace"})

            second_hits = await engine.search_documents(
                "db",
                "coll",
                "$search",
                {"index": "search", "text": {"query": "ada", "path": "title"}},
            )
            self.assertEqual(second_hits, [])
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

    async def test_memory_engine_helper_paths_cover_profile_hint_and_search_edges(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            self.assertEqual(engine._lock_key("db", "coll"), "db.coll")
            self.assertFalse(engine._namespace_exists_locked("db", "missing"))

            await engine.put_document("db", "users", {"_id": "1", "kind": "view", "embedding": [1.0, 0.0]})
            await engine.create_index("db", "users", ["kind"], name="kind_hidden", hidden=True)
            with self.assertRaisesRegex(OperationFailure, "usable index"):
                engine._resolve_hint_index("db", "users", [("kind", 1)])

            self.assertIsNone(await engine.get_document("db", "system.profile", "missing"))
            self.assertFalse(await engine.delete_document("db", "system.profile", "missing"))

            with self.assertRaisesRegex(TypeError, "hidden must be a bool"):
                await engine.create_index("db", "users", ["kind"], hidden="yes")  # type: ignore[arg-type]
            with self.assertRaisesRegex(TypeError, "expire_after_seconds must be a non-negative int"):
                await engine.create_index("db", "users", ["expires_at"], expire_after_seconds=-1)
            with self.assertRaisesRegex(OperationFailure, "Conflicting index definition for '_id_'"):
                await engine.create_index("db", "users", ["_id"], name="_id_")

            definition = SearchIndexDefinition(
                {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
                name="vec",
                index_type="vectorSearch",
            )
            await engine.create_search_index("db", "users", definition)
            with self.assertRaisesRegex(OperationFailure, "search index not found"):
                await engine.drop_search_index("db", "users", "missing")
            with self.assertRaisesRegex(OperationFailure, "search index not found"):
                await engine.explain_search_documents(
                    "db",
                    "users",
                    "$vectorSearch",
                    {"index": "missing", "path": "embedding", "queryVector": [1.0, 0.0], "limit": 1},
                )
            self.assertEqual(
                await engine.search_documents(
                    "db",
                    "users",
                    "$vectorSearch",
                    {
                        "index": "vec",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "numCandidates": 5,
                        "limit": 5,
                        "filter": {"kind": "missing"},
                    },
                ),
                [],
            )
            with self.assertRaises(CollectionInvalid):
                await engine.rename_collection("db", "missing", "renamed")
        finally:
            await engine.disconnect()

    async def test_memory_engine_covers_remaining_hint_unique_search_and_rename_branches(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "users", {"_id": "1", "kind": "view", "email": "ada@example.com", "embedding": ["bad", 1.0]})
            await engine.put_document("db", "users", {"_id": "2", "kind": "view", "email": "grace@example.com"})
            await engine.create_index("db", "users", ["email"], name="email_idx", unique=True)
            hinted = engine._resolve_hint_index("db", "users", [("email", 1)])
            self.assertEqual(hinted["name"], "email_idx")

            engine._index_data.clear()
            with self.assertRaises(DuplicateKeyError):
                engine._ensure_unique_indexes("db", "users", {"_id": "3", "email": "ada@example.com"})

            engine._profiler.set_level("db", 1)
            engine._profiler.record("db", op="query", namespace="db.users", command={"find": "users"}, duration_micros=200_000)
            profile_entry_id = engine._profiler.list_entries("db")[0]["_id"]
            profile_doc = await engine.get_document("db", "system.profile", profile_entry_id, projection={"op": 1, "_id": 0})
            self.assertEqual(profile_doc, {"op": "query"})

            await engine.create_index("db", "users", ["kind"], name="kind_idx")
            count = await self._count(engine, "db", "users", {"$and": [{"missing": "x"}, {"kind": "view"}]})
            self.assertEqual(count, 0)

            with self.assertRaisesRegex(OperationFailure, "Conflicting index definition for '_id_'"):
                await engine.create_index("db", "users", ["name"], name="_id_")
            await engine.create_index(
                "db",
                "users",
                ["inactive"],
                name="inactive_unique",
                unique=True,
                partial_filter_expression={"inactive": True},
            )
            with self.assertRaisesRegex(OperationFailure, "index not found with name \\[missing\\]"):
                await engine.drop_index("db", "users", "missing")
            with self.assertRaisesRegex(OperationFailure, "index not found with key pattern"):
                await engine.drop_index("db", "users", [("missing", 1)])

            definition = SearchIndexDefinition(
                {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
                name="vec",
                index_type="vectorSearch",
            )
            self.assertEqual(await engine.create_search_index("db", "users", definition), "vec")
            self.assertEqual(await engine.create_search_index("db", "users", definition), "vec")
            self.assertEqual(
                await engine.search_documents(
                    "db",
                    "users",
                    "$vectorSearch",
                    {"index": "vec", "path": "embedding", "queryVector": [1.0, 0.0], "numCandidates": 2, "limit": 2},
                ),
                [],
            )

            engine._search_index_ready_at[("db", "users", "vec")] = 123.0
            await engine.rename_collection("db", "users", "archived")
            self.assertIn("archived", engine._search_indexes["db"])
            self.assertIn(("db", "archived", "vec"), engine._search_index_ready_at)

            await engine.drop_database("db")
            self.assertFalse(any(key[0] == "db" for key in engine._search_index_ready_at))
        finally:
            await engine.disconnect()

    def test_memory_search_vector_helper_functions_cover_candidateable_paths(self):
        vector_definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 2,
                        "similarity": "dotProduct",
                    }
                ]
            },
            name="vec",
            index_type="vectorSearch",
        )
        documents = [
            {
                "_id": "1",
                "embedding": [1.0, 0.0],
                "kind": "keep",
                "score": 10,
                "meta": {"rank": 1, "published": datetime.date(2024, 1, 2)},
                "tags": ["a", "b"],
            },
            {
                "_id": "2",
                "embedding": [0.0, 1.0],
                "kind": "drop",
                "score": 5,
                "meta": {"rank": 2},
                "tags": ["b"],
            },
            {
                "_id": "3",
                "embedding": [1.0],
                "kind": "keep",
                "score": float("inf"),
            },
        ]
        vector_index = memory_module._build_materialized_vector_index(documents, vector_definition)

        self.assertEqual(memory_module._filter_value_key(None), ("null", None))
        self.assertEqual(memory_module._filter_value_key(True), ("bool", True))
        self.assertEqual(memory_module._filter_value_key(1), ("number", 1.0))
        self.assertEqual(
            memory_module._filter_value_key(datetime.datetime(2024, 1, 1, 12, 0, 0)),
            ("datetime", datetime.datetime(2024, 1, 1, 12, 0, 0)),
        )
        self.assertEqual(
            memory_module._filter_value_key(datetime.date(2024, 1, 1)),
            ("date", datetime.date(2024, 1, 1)),
        )
        self.assertIsNone(memory_module._filter_value_key(float("inf")))
        self.assertIsNone(memory_module._filter_value_key(object()))

        collected = memory_module._collect_filterable_values(
            {"": 1, "meta": {"rank": 2}, "tags": ["a", object(), 3]},
        )
        self.assertEqual(
            {path: values for path, values in collected},
            {
                "meta.rank": {("number", 2.0)},
                "tags": {("string", "a"), ("number", 3.0)},
            },
        )
        self.assertEqual(memory_module._collect_filterable_values("root"), [])
        self.assertEqual(
            memory_module._collect_filterable_values([], prefix="items"),
            [("items", set())],
        )

        self.assertFalse(
            memory_module._value_key_matches_range(("string", "x"), {"$gt": 1})
        )
        self.assertFalse(
            memory_module._value_key_matches_range(("number", 1.0), {"$regex": "x"})
        )
        self.assertFalse(
            memory_module._value_key_matches_range(("number", 1.0), {"$gt": "x"})
        )
        self.assertTrue(
            memory_module._value_key_matches_range(
                ("number", 2.0),
                {"$gte": 2, "$lte": 2},
            )
        )
        self.assertFalse(
            memory_module._value_key_matches_range(
                ("date", datetime.date(2024, 1, 1)),
                {"$lt": datetime.datetime(2024, 1, 1, 0, 0, 0)},
            )
        )

        self.assertEqual(vector_index.valid_vector_counts["embedding"], 2)
        self.assertEqual(vector_index.vector_paths, ("embedding",))
        self.assertIn("meta.rank", vector_index.exists_filter_index)
        self.assertIn("kind", vector_index.scalar_filter_index)

        self.assertEqual(
            memory_module._candidate_positions_for_vector_filter(vector_index, filter_spec=None),
            (None, None),
        )
        eq_positions, eq_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"kind": "keep"},
        )
        self.assertEqual(eq_positions, [0, 2])
        self.assertTrue(eq_description["candidateable"])
        self.assertTrue(eq_description["exact"])
        self.assertEqual(eq_description["supportedOperators"], ["eq"])

        in_positions, in_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"tags": {"$in": ["b"]}},
        )
        self.assertEqual(in_positions, [0, 1])
        self.assertEqual(in_description["supportedOperators"], ["$in"])

        range_positions, range_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"meta.rank": {"$gte": 1, "$lt": 2}},
        )
        self.assertEqual(range_positions, [0])
        self.assertEqual(range_description["supportedOperators"], ["range"])

        exists_positions, exists_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"meta.published": {"$exists": False}},
        )
        self.assertEqual(exists_positions, [1, 2])
        self.assertTrue(exists_description["exact"])

        or_positions, or_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"$or": [{"kind": "drop"}, {"meta.rank": {"$exists": True}}]},
        )
        self.assertEqual(or_positions, [0, 1])
        self.assertEqual(or_description["booleanShape"], "$or")
        self.assertTrue(or_description["exact"])

        partial_positions, partial_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"$and": [{"kind": "keep"}, {"kind": {"$regex": "k"}}]},
        )
        self.assertEqual(partial_positions, [0, 2])
        self.assertTrue(partial_description["candidateable"])
        self.assertFalse(partial_description["exact"])

        unsupported_positions, unsupported_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"$or": [{"kind": "keep"}, {"kind": {"$regex": "k"}}]},
        )
        self.assertIsNone(unsupported_positions)
        self.assertFalse(unsupported_description["candidateable"])
        self.assertEqual(unsupported_description["booleanShape"], "$or")

        scoped_positions, scoped_description = memory_module._candidate_positions_for_vector_filter(
            vector_index,
            filter_spec={"kind": "keep"},
            candidate_positions=(0, 1),
        )
        self.assertEqual(scoped_positions, [0])
        self.assertTrue(scoped_description["exact"])

        self.assertTrue(memory_module._matches_candidateable_filter(documents[0], {"kind": "keep"}))
        self.assertFalse(memory_module._matches_candidateable_filter(documents[0], {"kind": "drop"}))
        self.assertTrue(
            memory_module._matches_candidateable_filter(
                documents[0],
                {"meta.rank": {"$gte": 1, "$lte": 1}},
            )
        )
        self.assertFalse(
            memory_module._matches_candidateable_filter(
                documents[1],
                {"meta.rank": {"$gt": 2}},
            )
        )
        self.assertFalse(
            memory_module._matches_candidateable_filter(
                documents[1],
                {"meta.published": {"$exists": True}},
            )
        )
        self.assertIsNone(
            memory_module._matches_candidateable_filter(
                documents[0],
                {"kind": {"$regex": "keep"}},
            )
        )
        self.assertIsNone(
            memory_module._matches_candidateable_filter(
                documents[0],
                {"$or": "bad"},  # type: ignore[arg-type]
            )
        )

        dot_query = SearchVectorQuery(
            index_name="vec",
            path="embedding",
            query_vector=(1.0, 0.0),
            limit=2,
            num_candidates=4,
            similarity="dotProduct",
        )
        dot_scores = memory_module._vector_scores_for_positions(
            vector_index,
            query=dot_query,
            candidate_positions=[0, 1, 2],
            limit=1,
        )
        self.assertEqual(len(dot_scores), 1)
        self.assertEqual(dot_scores[0][1], 0)

        euclidean_definition = SearchIndexDefinition(
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 2,
                        "similarity": "euclidean",
                    }
                ]
            },
            name="vec_euclidean",
            index_type="vectorSearch",
        )
        euclidean_index = memory_module._build_materialized_vector_index(documents[:2], euclidean_definition)
        euclidean_scores = memory_module._vector_scores_for_positions(
            euclidean_index,
            query=SearchVectorQuery(
                index_name="vec_euclidean",
                path="embedding",
                query_vector=(1.0, 0.0),
                limit=2,
                num_candidates=4,
                similarity="cosine",
            ),
            candidate_positions=[0, 1],
            limit=None,
        )
        self.assertEqual([position for _score, position in euclidean_scores], [0, 1])
        self.assertEqual(
            memory_module._vector_scores_for_positions(
                vector_index,
                query=dot_query,
                candidate_positions=[99],
                limit=None,
            ),
            [],
        )
        scored_rows = memory_module._vector_scores_for_rows(
            vector_index,
            query=SearchVectorQuery(
                index_name="vec",
                path="embedding",
                query_vector=(1.0, 0.0),
                limit=2,
                num_candidates=4,
                similarity="cosine",
            ),
            candidate_rows=[0],
            limit=1,
        )
        self.assertEqual(scored_rows, [(1.0, 0)])
        cached_subset_scores = memory_module._vector_scores_for_rows(
            vector_index,
            query=SearchVectorQuery(
                index_name="vec",
                path="embedding",
                query_vector=(1.0, 0.0),
                limit=2,
                num_candidates=4,
                similarity="cosine",
            ),
            candidate_rows=[0],
            limit=1,
        )
        self.assertEqual(cached_subset_scores, [(1.0, 0)])
        self.assertEqual(len(vector_index.vector_ranked_row_cache), 1)

    async def test_memory_search_and_vector_runtime_cover_candidate_prefilter_paths(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document(
                "db",
                "coll",
                {
                    "_id": "1",
                    "title": "Ada algorithms",
                    "body": "vector ranking",
                    "kind": "note",
                    "score": 10,
                    "embedding": [1.0, 0.0],
                },
            )
            await engine.put_document(
                "db",
                "coll",
                {
                    "_id": "2",
                    "title": "Grace notes",
                    "body": "compiler vector",
                    "kind": "reference",
                    "score": 4,
                    "embedding": [0.0, 1.0],
                },
            )
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "title": {"type": "string"},
                                "body": {"type": "autocomplete"},
                                "kind": {"type": "token"},
                            },
                        }
                    },
                    name="by_text",
                ),
            )
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 2,
                                "similarity": "cosine",
                            }
                        ]
                    },
                    name="by_vector",
                    index_type="vectorSearch",
                ),
            )

            session = ClientSession()
            materialized_with_context = engine._materialized_search_documents(
                "db",
                "coll",
                SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {"title": {"type": "string"}},
                        }
                    },
                    name="by_text",
                ),
                context=session,
            )
            self.assertEqual(len(materialized_with_context), 2)
            vector_index_with_context = engine._materialized_vector_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 2,
                                "similarity": "cosine",
                            }
                        ]
                    },
                    name="by_vector",
                    index_type="vectorSearch",
                ),
                context=session,
            )
            self.assertEqual(vector_index_with_context.valid_vector_counts["embedding"], 2)

            compound_hits = await engine.search_documents(
                "db",
                "coll",
                "$search",
                {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "vector", "path": ["title", "body"]}}],
                        "should": [{"exists": {"path": "title"}}],
                        "minimumShouldMatch": 0,
                    },
                },
                result_limit_hint=1,
                downstream_filter_spec={"$and": [{"kind": "note"}, {"score": {"$gte": 5}}]},
            )
            self.assertEqual([document["_id"] for document in compound_hits], ["1"])

            text_hits = await engine.search_documents(
                "db",
                "coll",
                "$search",
                {"index": "by_text", "autocomplete": {"query": "vec", "path": ["title", "body"]}},
                result_limit_hint=1,
                downstream_filter_spec={"kind": {"$regex": "note"}},
            )
            self.assertEqual([document["_id"] for document in text_hits], ["1"])

            vector_hits = await engine.search_documents(
                "db",
                "coll",
                "$vectorSearch",
                {
                    "index": "by_vector",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0],
                    "numCandidates": 5,
                    "limit": 5,
                    "filter": {"$or": [{"kind": "note"}, {"score": {"$gte": 10}}]},
                },
                downstream_filter_spec={"kind": {"$regex": "note"}},
            )
            self.assertEqual([document["_id"] for document in vector_hits], ["1"])

            vector_explanation = await engine.explain_search_documents(
                "db",
                "coll",
                "$vectorSearch",
                {
                    "index": "by_vector",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0],
                    "numCandidates": 5,
                    "limit": 5,
                    "filter": {"$and": [{"kind": "note"}, {"kind": {"$regex": "n"}}]},
                },
            )
            self.assertEqual(
                vector_explanation.details["filterMode"],
                "candidate-prefilter+post-candidate",
            )
            self.assertTrue(vector_explanation.details["vectorFilterPrefilter"]["candidateable"])
            self.assertFalse(vector_explanation.details["vectorFilterPrefilter"]["exact"])
            self.assertTrue(vector_explanation.details["vectorFilterResidual"]["required"])
            self.assertEqual(vector_explanation.details["vectorFilterResidual"]["reason"], "unsupported-clauses")
            self.assertGreaterEqual(
                vector_explanation.details["documentsScanned"],
                vector_explanation.details["documentsScannedAfterPrefilter"],
            )
        finally:
            await engine.disconnect()
