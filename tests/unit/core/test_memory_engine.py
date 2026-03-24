import datetime
import unittest

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.identity import canonical_document_id
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import DuplicateKeyError


class _UnhashableValue:
    __hash__ = None

    def __repr__(self) -> str:
        return "unhashable-value"


class MemoryEngineTests(unittest.IsolatedAsyncioTestCase):
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
            result = await engine.update_matching_document(
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
            result = await engine.update_matching_document(
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
                await engine.update_matching_document(
                    "db",
                    "coll",
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    upsert=True,
                    upsert_seed={"_id": "dup", "kind": "view"},
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
        self.assertEqual(indexes_again, [{"name": "kind_1", "fields": ["kind"], "unique": False}])

    async def test_create_index_rejects_conflicting_definition_for_existing_name(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["kind"], unique=False, name="idx")

            with self.assertRaises(DuplicateKeyError):
                await engine.create_index("db", "coll", ["kind"], unique=True, name="idx")

            with self.assertRaises(DuplicateKeyError):
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
                await engine.update_matching_document(
                    "db",
                    "coll",
                    {"_id": "2"},
                    {"$set": {"email": "a@example.com"}},
                )
        finally:
            await engine.disconnect()

    async def test_disconnect_resets_engine_state_when_last_connection_closes(self):
        engine = MemoryEngine()
        await engine.connect()
        await engine.put_document("db", "coll", {"_id": "1"})
        await engine.create_index("db", "coll", ["kind"])
        await engine.disconnect()

        await engine.connect()
        try:
            self.assertIsNone(await engine.get_document("db", "coll", "1"))
            self.assertEqual(await engine.list_indexes("db", "coll"), [])
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
        self.assertEqual(indexes, [])

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

    async def test_drop_collection_releases_collection_lock_entry(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1"})
            self.assertIn("db.coll", engine._locks)

            await engine.drop_collection("db", "coll")
        finally:
            await engine.disconnect()

        self.assertNotIn("db.coll", engine._locks)

    async def test_index_value_returns_none_when_field_is_missing(self):
        engine = MemoryEngine()

        self.assertIsNone(engine._index_value({"name": "Ada"}, "missing"))

    async def test_index_value_normalizes_list_values(self):
        engine = MemoryEngine()

        self.assertEqual(
            engine._index_value({"tags": ["python", {"level": 1}]}, "tags"),
            canonical_document_id(["python", {"level": 1}]),
        )

    async def test_delete_matching_document_returns_zero_when_no_match(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            result = await engine.delete_matching_document("db", "coll", {"kind": "click"})
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
            count = await engine.count_matching_documents("db", "coll", {"kind": "view"})
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

    async def test_sort_value_handles_missing_and_empty_array(self):
        self.assertIsNone(MemoryEngine._sort_value({"name": "Ada"}, "rank", 1))
        self.assertEqual(MemoryEngine._sort_value({"rank": []}, "rank", 1), [])

    async def test_compare_documents_returns_zero_for_equal_sort_keys(self):
        result = MemoryEngine._compare_documents(
            {"_id": "1", "rank": 3},
            {"_id": "2", "rank": 3},
            [("rank", 1)],
        )

        self.assertEqual(result, 0)

    async def test_scan_collection_applies_sort_skip_and_limit(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": 3})
            await engine.put_document("db", "coll", {"_id": "2", "rank": 1})
            await engine.put_document("db", "coll", {"_id": "3", "rank": 2})

            documents = [
                doc async for doc in engine.scan_collection(
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

    async def test_scan_collection_sorts_array_fields_by_min_and_max_element(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": [3, 8]})
            await engine.put_document("db", "coll", {"_id": "2", "rank": [1, 9]})
            await engine.put_document("db", "coll", {"_id": "3", "rank": [2, 4]})

            ascending = [
                doc["_id"]
                async for doc in engine.scan_collection("db", "coll", sort=[("rank", 1)])
            ]
            descending = [
                doc["_id"]
                async for doc in engine.scan_collection("db", "coll", sort=[("rank", -1)])
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(ascending, ["2", "3", "1"])
        self.assertEqual(descending, ["2", "1", "3"])

    async def test_scan_collection_rejects_negative_skip_and_limit(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            with self.assertRaises(ValueError):
                async for _ in engine.scan_collection("db", "coll", skip=-1):
                    pass

            with self.assertRaises(ValueError):
                async for _ in engine.scan_collection("db", "coll", limit=-1):
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
