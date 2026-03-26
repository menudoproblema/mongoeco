import unittest
import asyncio
from unittest.mock import patch

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.compat import MongoDialect70
from mongoeco.core.query_plan import MatchAll
from mongoeco.core.upserts import _seed_filter_value, seed_upsert_document
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, DuplicateKeyError, OperationFailure
from mongoeco.types import (
    DeleteMany, DeleteOne, DeleteResult, IndexModel, InsertOne, ReplaceOne,
    ReturnDocument, UpdateMany, UpdateOne, UpdateResult,
)


class AsyncCollectionHelperTests(unittest.TestCase):
    def setUp(self):
        self.collection = AsyncCollection(MemoryEngine(), "db", "coll")

    def test_seed_upsert_document_skips_top_level_operators_and_non_eq_filters(self):
        document = {}

        seed_upsert_document(
            document,
            {
                "$or": [{"name": "Ada"}],
                "name": {"$eq": "Ada"},
                "age": {"$gt": 18},
                "profile.city": "Madrid",
            },
        )

        self.assertEqual(document, {"name": "Ada", "profile": {"city": "Madrid"}})

    def test_seed_upsert_document_keeps_literal_dict_values(self):
        document = {}

        seed_upsert_document(
            document,
            {
                "profile": {"nested": "x"},
                "empty": {},
            },
        )

        self.assertEqual(document, {"profile": {"nested": "x"}, "empty": {}})

    def test_direct_id_lookup_only_applies_to_literal_id_selector(self):
        self.assertTrue(self.collection._can_use_direct_id_lookup({"_id": "user-1"}))
        self.assertTrue(self.collection._can_use_direct_id_lookup({"_id": [1, 2, 3]}))
        self.assertFalse(self.collection._can_use_direct_id_lookup({"_id": {"$eq": "user-1"}}))
        self.assertFalse(self.collection._can_use_direct_id_lookup({"name": "Ada"}))

    def test_find_validates_sort_spec(self):
        with self.assertRaises(TypeError):
            self.collection.find(sort="name")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            self.collection.find(sort=[("name", 2)])  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            self.collection.aggregate([], let="bad")  # type: ignore[arg-type]

    def test_write_operations_validate_hint_max_time_ms_and_let(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.update_one({}, {"$set": {"done": True}}, hint=1))  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.replace_one({}, {"done": True}, let="bad"))  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.delete_one({}, hint=1.5))  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write([DeleteOne({})], let="bad"))  # type: ignore[arg-type]

    def test_write_operations_reject_unsupported_max_time_ms_surface(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.update_one({}, {"$set": {"done": True}}, max_time_ms=5))  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.update_many({}, {"$set": {"done": True}}, max_time_ms=5))  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.replace_one({}, {"done": True}, max_time_ms=5))  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.delete_one({}, max_time_ms=5))  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.delete_many({}, max_time_ms=5))  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write([DeleteOne({})], max_time_ms=5))  # type: ignore[call-arg]

    def test_seed_upsert_document_rejects_conflicting_paths(self):
        with self.assertRaises(OperationFailure):
            seed_upsert_document({}, {"a": 1, "a.b": 2})

    def test_seed_filter_value_rejects_conflicting_existing_scalar(self):
        with self.assertRaises(OperationFailure):
            _seed_filter_value({"a": 1}, "a", 2)

    def test_seed_filter_value_allows_same_existing_scalar(self):
        document = {"a": 1}

        _seed_filter_value(document, "a", 1)

        self.assertEqual(document, {"a": 1})

    def test_seed_filter_value_rejects_bool_number_conflict(self):
        with self.assertRaises(OperationFailure):
            _seed_filter_value({"a": 1}, "a", True)

    def test_insert_many_requires_non_empty_document_list(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.insert_many({"name": "Ada"}))  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            asyncio.run(self.collection.insert_many([]))
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.insert_many([{"name": "Ada"}, []]))  # type: ignore[list-item]

    def test_insert_many_raises_duplicate_key_error_when_engine_rejects_document(self):
        class EngineStub:
            def __init__(self):
                self.calls = 0

            async def put_document(self, *args, **kwargs):
                self.calls += 1
                return self.calls == 1

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with self.assertRaisesRegex(DuplicateKeyError, "Duplicate key"):
            asyncio.run(collection.insert_many([{"_id": "1"}, {"_id": "1"}]))

    def test_insert_one_mutates_original_document_id_and_detaches_nested_values(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                document = {"profile": {"name": "Ada"}}
                result = await collection.insert_one(document)
                document["profile"]["name"] = "Grace"
                stored = await collection.find_one({"_id": result.inserted_id})
                return document, stored, result.inserted_id
            finally:
                await engine.disconnect()

        document, stored, inserted_id = asyncio.run(_exercise())

        self.assertEqual(document["_id"], inserted_id)
        self.assertEqual(stored, {"_id": inserted_id, "profile": {"name": "Ada"}})

    def test_insert_many_mutates_original_documents_ids_and_detaches_nested_values(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                documents = [{"profile": {"name": "Ada"}}, {"profile": {"name": "Grace"}}]
                result = await collection.insert_many(documents)
                documents[0]["profile"]["name"] = "Changed"
                stored = await collection.find({}, sort=[("_id", 1)]).to_list()
                return documents, stored, result.inserted_ids
            finally:
                await engine.disconnect()

        documents, stored, inserted_ids = asyncio.run(_exercise())

        self.assertEqual([document["_id"] for document in documents], inserted_ids)
        self.assertEqual(
            stored,
            [
                {"_id": inserted_ids[0], "profile": {"name": "Ada"}},
                {"_id": inserted_ids[1], "profile": {"name": "Grace"}},
            ],
        )

    def test_find_one_does_not_apply_projection_twice(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "profile": {"city": "Madrid"}})
                with patch("mongoeco.api._async.collection.apply_projection", side_effect=AssertionError("projection should not be re-applied")):
                    return await collection.find_one({"_id": "1"}, {"name": 1, "_id": 0})
            finally:
                await engine.disconnect()

        found = asyncio.run(_exercise())

        self.assertEqual(found, {"name": "Ada"})

    def test_bulk_write_requires_non_empty_write_model_list(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write(InsertOne({"name": "Ada"})))  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            asyncio.run(self.collection.bulk_write([]))
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write([{"insert": {"name": "Ada"}}]))  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write([InsertOne({"name": "Ada"})], ordered=1))  # type: ignore[arg-type]

    def test_bulk_write_accepts_non_list_sequences(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                return await collection.bulk_write((InsertOne({"_id": "1"}),))
            finally:
                await engine.disconnect()

        result = asyncio.run(_exercise())

        self.assertEqual(result.inserted_count, 1)

    def test_update_one_with_sort_and_upsert_does_not_call_engine_update_path_twice(self):
        class EngineStub(MemoryEngine):
            def __init__(self):
                super().__init__()
                self.update_calls = 0

            async def update_matching_document(self, *args, **kwargs):
                self.update_calls += 1
                return await super().update_matching_document(*args, **kwargs)

        async def _exercise():
            engine = EngineStub()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                result = await collection.update_one(
                    {"kind": "missing"},
                    {"$set": {"done": True}},
                    upsert=True,
                    sort=[("rank", 1)],
                )
                stored = await collection.find_one({"_id": result.upserted_id})
                return engine.update_calls, stored
            finally:
                await engine.disconnect()

        update_calls, stored = asyncio.run(_exercise())

        self.assertEqual(update_calls, 0)
        self.assertEqual(stored["kind"], "missing")
        self.assertTrue(stored["done"])

    def test_find_one_and_update_propagates_sort_to_update_one_on_upsert(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                recorded: dict[str, object] = {}
                original = collection.update_one

                async def _wrapped(*args, **kwargs):
                    recorded["sort"] = kwargs.get("sort")
                    return await original(*args, **kwargs)

                collection.update_one = _wrapped  # type: ignore[method-assign]
                await collection.find_one_and_update(
                    {"kind": "missing"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                    upsert=True,
                )
                return recorded["sort"]
            finally:
                await engine.disconnect()

        self.assertEqual(asyncio.run(_exercise()), [("rank", 1)])

    def test_bulk_write_accumulates_results_and_upserts(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                await collection.insert_one({"_id": "base", "kind": "view", "rank": 2, "done": False})
                await collection.insert_one({"_id": "other", "kind": "view", "rank": 1, "done": False})
                result = await collection.bulk_write(
                    [
                        InsertOne({"_id": "new", "kind": "click"}),
                        UpdateOne({"kind": "view"}, {"$set": {"done": True}}, sort=[("rank", 1)]),
                        UpdateMany({"kind": "view"}, {"$set": {"tag": "seen"}}),
                        ReplaceOne({"_id": "new"}, {"kind": "click", "done": True}),
                        DeleteOne({"_id": "base"}),
                        DeleteMany({"kind": "view"}),
                        UpdateOne({"kind": "missing", "tenant": "a"}, {"$set": {"done": True}}, upsert=True),
                    ]
                )
                remaining = await collection.find({}, sort=[("_id", 1)]).to_list()
                return result, remaining
            finally:
                await engine.disconnect()

        result, remaining = asyncio.run(_exercise())

        self.assertEqual(result.inserted_count, 1)
        self.assertEqual(result.matched_count, 4)
        self.assertEqual(result.modified_count, 4)
        self.assertEqual(result.deleted_count, 2)
        self.assertEqual(result.upserted_count, 1)
        self.assertEqual(set(result.upserted_ids), {6})
        self.assertEqual(
            remaining,
            [
                {"_id": "new", "kind": "click", "done": True},
                {"_id": result.upserted_ids[6], "kind": "missing", "tenant": "a", "done": True},
            ],
        )

    def test_bulk_write_ordered_stops_on_first_write_error(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "dup", "done": False})
                await collection.bulk_write(
                    [
                        InsertOne({"_id": "ok"}),
                        InsertOne({"_id": "dup"}),
                        UpdateOne({"_id": "dup"}, {"$set": {"done": True}}),
                    ],
                    ordered=True,
                )
            finally:
                await engine.disconnect()

        with self.assertRaises(BulkWriteError) as ctx:
            asyncio.run(_exercise())

        self.assertEqual(ctx.exception.details["writeErrors"][0]["index"], 1)
        self.assertEqual(ctx.exception.details["nInserted"], 1)
        self.assertEqual(ctx.exception.details["nModified"], 0)

    def test_bulk_write_rejects_sort_write_models_for_older_profile(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.9")
                await collection.bulk_write(
                    [
                        UpdateOne({"_id": "1"}, {"$set": {"done": True}}, sort=[("rank", 1)]),
                    ]
                )
            finally:
                await engine.disconnect()

        with self.assertRaises(TypeError):
            asyncio.run(_exercise())

    def test_bulk_write_unordered_collects_later_successes(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "dup", "done": False})
                try:
                    await collection.bulk_write(
                        [
                            InsertOne({"_id": "dup"}),
                            UpdateOne({"_id": "dup"}, {"$set": {"done": True}}),
                            DeleteOne({"_id": "dup"}),
                        ],
                        ordered=False,
                    )
                except BulkWriteError as exc:
                    remaining = await collection.find({}).to_list()
                    return exc, remaining
                raise AssertionError("bulk_write should have failed")
            finally:
                await engine.disconnect()

        error, remaining = asyncio.run(_exercise())

        self.assertEqual(error.details["writeErrors"][0]["index"], 0)
        self.assertEqual(error.details["nModified"], 1)
        self.assertEqual(error.details["nRemoved"], 1)
        self.assertEqual(remaining, [])

    def test_bulk_write_normalizes_operation_failures_and_continues_when_unordered(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada"})
                try:
                    await collection.bulk_write(
                        [
                            ReplaceOne({"_id": "1"}, {"$bad": 1}),  # type: ignore[arg-type]
                            UpdateOne({"_id": "1"}, {"$set": {"done": True}}),
                        ],
                        ordered=False,
                    )
                except BulkWriteError as exc:
                    document = await collection.find_one({"_id": "1"})
                    return exc, document
                raise AssertionError("bulk_write should have failed")
            finally:
                await engine.disconnect()

        error, document = asyncio.run(_exercise())

        self.assertEqual(error.details["writeErrors"][0]["index"], 0)
        self.assertEqual(error.details["nModified"], 1)
        self.assertEqual(document, {"_id": "1", "name": "Ada", "done": True})

    def test_bulk_write_propagates_request_and_bulk_level_write_options(self):
        class EngineStub:
            pass

        async def _exercise():
            collection = AsyncCollection(EngineStub(), "db", "coll")
            recorded: list[tuple[str, dict[str, object | None]]] = []

            async def _update_one(*args, **kwargs):
                recorded.append(("update_one", kwargs))
                return UpdateResult(matched_count=1, modified_count=1)

            async def _update_many(*args, **kwargs):
                recorded.append(("update_many", kwargs))
                return UpdateResult(matched_count=2, modified_count=2)

            async def _replace_one(*args, **kwargs):
                recorded.append(("replace_one", kwargs))
                return UpdateResult(matched_count=1, modified_count=1)

            async def _delete_one(*args, **kwargs):
                recorded.append(("delete_one", kwargs))
                return DeleteResult(deleted_count=1)

            async def _delete_many(*args, **kwargs):
                recorded.append(("delete_many", kwargs))
                return DeleteResult(deleted_count=2)

            collection.update_one = _update_one  # type: ignore[method-assign]
            collection.update_many = _update_many  # type: ignore[method-assign]
            collection.replace_one = _replace_one  # type: ignore[method-assign]
            collection.delete_one = _delete_one  # type: ignore[method-assign]
            collection.delete_many = _delete_many  # type: ignore[method-assign]

            result = await collection.bulk_write(
                [
                    UpdateOne(
                        {"kind": "one"},
                        {"$set": {"done": True}},
                        hint="req_hint",
                        comment="req_comment",
                        let={"scope": "request"},
                    ),
                    UpdateMany({"kind": "many"}, {"$set": {"done": True}}),
                    ReplaceOne({"kind": "replace"}, {"done": True}),
                    DeleteOne({"kind": "delete-one"}),
                    DeleteMany({"kind": "delete-many"}),
                ],
                comment="bulk_comment",
                let={"scope": "bulk"},
            )
            return result, recorded

        result, recorded = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 4)
        self.assertEqual(result.modified_count, 4)
        self.assertEqual(result.deleted_count, 3)
        self.assertEqual(
            recorded,
            [
                (
                    "update_one",
                    {
                        "sort": None,
                        "hint": "req_hint",
                        "comment": "req_comment",
                        "let": {"scope": "request"},
                        "session": None,
                    },
                ),
                (
                    "update_many",
                    {
                        "hint": None,
                        "comment": "bulk_comment",
                        "let": {"scope": "bulk"},
                        "session": None,
                    },
                ),
                (
                    "replace_one",
                    {
                        "sort": None,
                        "hint": None,
                        "comment": "bulk_comment",
                        "let": {"scope": "bulk"},
                        "session": None,
                    },
                ),
                (
                    "delete_one",
                    {
                        "hint": None,
                        "comment": "bulk_comment",
                        "let": {"scope": "bulk"},
                        "session": None,
                    },
                ),
                (
                    "delete_many",
                    {
                        "hint": None,
                        "comment": "bulk_comment",
                        "let": {"scope": "bulk"},
                        "session": None,
                    },
                ),
            ],
        )

    def test_distinct_includes_null_once_for_missing_fields(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_many(
                    [
                        {"_id": "1", "kind": "view"},
                        {"_id": "2", "kind": "view", "profile": {"city": "Madrid"}},
                        {"_id": "3", "kind": "view", "profile": {"city": []}},
                    ]
                )
                return await collection.distinct("profile.city")
            finally:
                await engine.disconnect()

        self.assertEqual(asyncio.run(_exercise()), [None, "Madrid"])

    def test_distinct_uses_scalar_fallback_when_exact_path_exists_but_extract_values_is_empty(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    yield {"profile": {"city": "Madrid"}}

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with patch("mongoeco.api._async.collection.QueryEngine.extract_values", return_value=[]):
            with patch("mongoeco.api._async.collection.QueryEngine._get_field_value", return_value=(True, "Madrid")):
                result = asyncio.run(collection.distinct("profile.city"))

        self.assertEqual(result, ["Madrid"])

    def test_distinct_skips_list_fallback_when_exact_path_is_an_empty_array(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    yield {"profile": {"city": []}}

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with patch("mongoeco.api._async.collection.QueryEngine.extract_values", return_value=[]):
            with patch("mongoeco.api._async.collection.QueryEngine._get_field_value", return_value=(True, [])):
                result = asyncio.run(collection.distinct("profile.city"))

        self.assertEqual(result, [])

    def test_bulk_write_records_upserted_ids_for_update_many_and_replace_one(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                result = await collection.bulk_write(
                    [
                        UpdateMany({"kind": "missing", "tenant": "a"}, {"$set": {"done": True}}, upsert=True),
                        ReplaceOne({"kind": "replace", "tenant": "b"}, {"done": True}, upsert=True),
                    ]
                )
                documents = await collection.find({}, sort=[("tenant", 1)]).to_list()
                return result, documents
            finally:
                await engine.disconnect()

        result, documents = asyncio.run(_exercise())

        self.assertEqual(result.upserted_count, 2)
        self.assertEqual(set(result.upserted_ids), {0, 1})
        self.assertEqual(
            documents,
            [
                {"_id": result.upserted_ids[0], "kind": "missing", "tenant": "a", "done": True},
                {"_id": result.upserted_ids[1], "kind": "replace", "tenant": "b", "done": True},
            ],
        )

    def test_update_many_returns_zero_counts_when_nothing_matches_and_upsert_is_false(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.update_many({"kind": "missing"}, {"$set": {"done": True}}))

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_estimated_document_count_and_drop_delegate_to_engine(self):
        class EngineStub:
            def __init__(self):
                self.count_calls = []
                self.drop_calls = []

            async def count_matching_documents(self, *args, **kwargs):
                self.count_calls.append((args, kwargs))
                return 7

            async def drop_collection(self, db_name, coll_name, *, context=None):
                self.drop_calls.append((db_name, coll_name, context))

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")
        session = object()

        count = asyncio.run(collection.estimated_document_count())
        asyncio.run(collection.drop(session=session))

        self.assertEqual(count, 7)
        self.assertEqual(engine.drop_calls, [("db", "coll", session)])
        self.assertEqual(engine.count_calls[0][0][:3], ("db", "coll", {}))
        self.assertEqual(type(engine.count_calls[0][1]["plan"]).__name__, "MatchAll")

    def test_replace_one_rejects_update_operator_document(self):
        with self.assertRaises(ValueError):
            asyncio.run(self.collection.replace_one({"name": "Ada"}, {"$set": {"name": "Grace"}}))

    def test_replace_one_rejects_non_document_replacement(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.replace_one({"name": "Ada"}, []))  # type: ignore[arg-type]

    def test_replace_one_sort_is_rejected_by_older_pymongo_profile(self):
        collection = AsyncCollection(
            MemoryEngine(),
            "db",
            "coll",
            pymongo_profile="4.9",
        )

        with self.assertRaises(TypeError):
            asyncio.run(
                collection.replace_one(
                    {"name": "Ada"},
                    {"name": "Grace"},
                    sort=[("rank", 1)],
                )
            )

    def test_replace_one_returns_zero_when_nothing_matches_and_upsert_is_false(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.replace_one({"name": "Ada"}, {"name": "Grace"}))

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_replace_one_upsert_builds_seeded_document(self):
        class EngineStub:
            def __init__(self):
                self.document = None

            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

            async def put_document(self, _db, _coll, document, **kwargs):
                self.document = document
                return True

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        result = asyncio.run(
            collection.replace_one(
                {"kind": "missing", "tenant": "a"},
                {"done": True},
                upsert=True,
            )
        )

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertTrue(result.upserted_id)
        self.assertEqual(
            engine.document,
            {"_id": result.upserted_id, "kind": "missing", "tenant": "a", "done": True},
        )

    def test_replace_one_upsert_rejects_conflicting_seeded_id(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.replace_one(
                    {"_id": "filter-id"},
                    {"_id": "replacement-id", "done": True},
                    upsert=True,
                )
            finally:
                await engine.disconnect()

        with self.assertRaises(OperationFailure):
            asyncio.run(_exercise())

    def test_replace_one_identical_document_without_id_keeps_modified_count_zero(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "a": 1, "b": 2})
                result = await collection.replace_one({"_id": "1"}, {"a": 1, "b": 2})
                document = await collection.find_one({"_id": "1"})
                return result, document
            finally:
                await engine.disconnect()

        result, document = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 0)
        self.assertEqual(document, {"_id": "1", "a": 1, "b": 2})

    def test_replace_one_with_explicit_same_id_preserves_document_and_zero_modifications(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "a": 1, "b": 2})
                result = await collection.replace_one({"_id": "1"}, {"_id": "1", "a": 1, "b": 2})
                document = await collection.find_one({"_id": "1"})
                return result, document
            finally:
                await engine.disconnect()

        result, document = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 0)
        self.assertEqual(document, {"_id": "1", "a": 1, "b": 2})

    def test_replace_one_identical_document_without_id_preserves_non_initial_id_position(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"a": 1, "_id": "1", "b": 2})
                result = await collection.replace_one({"_id": "1"}, {"a": 1, "b": 2})
                document = await collection.find_one({"_id": "1"})
                return result, document
            finally:
                await engine.disconnect()

        result, document = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 0)
        self.assertEqual(list(document.keys()), ["a", "_id", "b"])

    def test_replace_one_identical_document_without_id_preserves_trailing_id_position(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"a": 1, "b": 2, "_id": "1"})
                result = await collection.replace_one({"_id": "1"}, {"a": 1, "b": 2})
                document = await collection.find_one({"_id": "1"})
                return result, document
            finally:
                await engine.disconnect()

        result, document = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 0)
        self.assertEqual(list(document.keys()), ["a", "b", "_id"])

    def test_materialize_replacement_document_inserts_trailing_id_when_selected_has_it_last(self):
        selected = {"a": 1, "b": 2, "_id": "1"}
        replacement = {"a": 1}

        materialized = AsyncCollection._materialize_replacement_document(selected, replacement)

        self.assertEqual(materialized, {"a": 1, "_id": "1"})
        self.assertEqual(list(materialized.keys()), ["a", "_id"])

    def test_replace_one_upsert_duplicate_key_error_when_engine_rejects_document(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

            async def put_document(self, *args, **kwargs):
                return False

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with self.assertRaises(DuplicateKeyError):
            asyncio.run(collection.replace_one({"kind": "missing"}, {"done": True}, upsert=True))

    def test_replace_one_rejects_changing_id(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada"})
                await collection.replace_one({"_id": "1"}, {"_id": "2", "name": "Grace"})
            finally:
                await engine.disconnect()

        with self.assertRaises(OperationFailure):
            asyncio.run(_exercise())

    def test_find_one_and_update_requires_return_document_enum(self):
        with self.assertRaises(TypeError):
            asyncio.run(
                self.collection.find_one_and_update(
                    {"name": "Ada"},
                    {"$set": {"name": "Grace"}},
                    return_document="after",  # type: ignore[arg-type]
                )
            )
        with self.assertRaises(TypeError):
            asyncio.run(
                self.collection.find_one_and_update(
                    {"name": "Ada"},
                    {"$set": {"name": "Grace"}},
                    let="bad",  # type: ignore[arg-type]
                )
            )

    def test_find_one_and_update_returns_none_when_nothing_matches_without_upsert(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(
            collection.find_one_and_update(
                {"name": "Ada"},
                {"$set": {"name": "Grace"}},
            )
        )

        self.assertIsNone(result)

    def test_find_one_and_update_returns_after_document_for_existing_match(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "done": False})
                return await collection.find_one_and_update(
                    {"_id": "1"},
                    {"$set": {"done": True}},
                    return_document=ReturnDocument.AFTER,
                    projection={"done": 1, "_id": 0},
                )
            finally:
                await engine.disconnect()

        result = asyncio.run(_exercise())

        self.assertEqual(result, {"done": True})

    def test_find_one_and_replace_covers_before_after_and_none_branches(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                none_result = await collection.find_one_and_replace(
                    {"name": "missing"},
                    {"name": "Grace"},
                )
                before_upsert = await collection.find_one_and_replace(
                    {"name": "upserted"},
                    {"done": True},
                    upsert=True,
                )
                after_upsert = await collection.find_one_and_replace(
                    {"name": "after-upsert"},
                    {"done": True},
                    upsert=True,
                    return_document=ReturnDocument.AFTER,
                    projection={"done": 1, "_id": 0},
                )
                await collection.insert_one({"_id": "1", "name": "Ada", "done": False})
                before_existing = await collection.find_one_and_replace(
                    {"_id": "1"},
                    {"name": "Ada", "done": True},
                    return_document=ReturnDocument.BEFORE,
                    projection={"done": 1, "_id": 0},
                )
                return none_result, before_upsert, after_upsert, before_existing
            finally:
                await engine.disconnect()

        none_result, before_upsert, after_upsert, before_existing = asyncio.run(_exercise())

        self.assertIsNone(none_result)
        self.assertIsNone(before_upsert)
        self.assertEqual(after_upsert, {"done": True})
        self.assertEqual(before_existing, {"done": False})

    def test_find_one_and_delete_returns_none_when_nothing_matches(self):
        class EngineStub:
            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    if False:
                        yield None

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.find_one_and_delete({"name": "missing"}))

        self.assertIsNone(result)

    def test_find_one_and_update_replace_and_delete_accept_option_surface(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                await collection.insert_one({"_id": "1", "name": "Ada", "done": False})
                updated = await collection.find_one_and_update(
                    {"_id": "1"},
                    {"$set": {"done": True}},
                    return_document=ReturnDocument.AFTER,
                    hint="_id_1",
                    comment="trace",
                    max_time_ms=5,
                    let={"tenant": "a"},
                )
                replaced = await collection.find_one_and_replace(
                    {"_id": "1"},
                    {"name": "Ada", "done": False},
                    return_document=ReturnDocument.AFTER,
                    hint="_id_1",
                    comment="trace",
                    max_time_ms=5,
                    let={"tenant": "a"},
                )
                deleted = await collection.find_one_and_delete(
                    {"_id": "1"},
                    hint="_id_1",
                    comment="trace",
                    max_time_ms=5,
                    let={"tenant": "a"},
                )
                return updated, replaced, deleted
            finally:
                await engine.disconnect()

        updated, replaced, deleted = asyncio.run(_exercise())

        self.assertEqual(updated["done"], True)
        self.assertEqual(replaced["done"], False)
        self.assertEqual(deleted["_id"], "1")

    def test_update_replace_and_delete_accept_and_propagate_option_surface(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                await collection.insert_many(
                    [
                        {"_id": "1", "kind": "view", "rank": 2, "done": False},
                        {"_id": "2", "kind": "view", "rank": 1, "done": False},
                        {"_id": "3", "kind": "replace", "done": False},
                        {"_id": "4", "kind": "delete"},
                    ]
                )

                recorded_find: list[dict[str, object | None]] = []
                recorded_select: list[dict[str, object | None]] = []
                original_find = collection.find
                original_select = collection._select_first_document

                def _wrapped_find(*args, **kwargs):
                    recorded_find.append(
                        {
                            "hint": kwargs.get("hint"),
                            "comment": kwargs.get("comment"),
                        }
                    )
                    return original_find(*args, **kwargs)

                async def _wrapped_select(*args, **kwargs):
                    recorded_select.append(
                        {
                            "hint": kwargs.get("hint"),
                            "comment": kwargs.get("comment"),
                        }
                    )
                    return await original_select(*args, **kwargs)

                collection.find = _wrapped_find  # type: ignore[method-assign]
                collection._select_first_document = _wrapped_select  # type: ignore[method-assign]

                update_one_result = await collection.update_one(
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                    hint="kind_rank_idx",
                    comment="trace-update-one",
                    let={"tenant": "a"},
                )
                update_many_result = await collection.update_many(
                    {"kind": "view"},
                    {"$set": {"tag": "seen"}},
                    hint="kind_idx",
                    comment="trace-update-many",
                    let={"tenant": "a"},
                )
                replace_one_result = await collection.replace_one(
                    {"kind": "replace"},
                    {"kind": "replace", "done": True},
                    hint="replace_idx",
                    comment="trace-replace",
                    let={"tenant": "a"},
                )
                delete_one_result = await collection.delete_one(
                    {"_id": "4"},
                    hint="_id_",
                    comment="trace-delete-one",
                    let={"tenant": "a"},
                )
                delete_many_result = await collection.delete_many(
                    {"kind": "view"},
                    hint="kind_idx",
                    comment="trace-delete-many",
                    let={"tenant": "a"},
                )
                return (
                    update_one_result,
                    update_many_result,
                    replace_one_result,
                    delete_one_result,
                    delete_many_result,
                    recorded_find,
                    recorded_select,
                )
            finally:
                await engine.disconnect()

        (
            update_one_result,
            update_many_result,
            replace_one_result,
            delete_one_result,
            delete_many_result,
            recorded_find,
            recorded_select,
        ) = asyncio.run(_exercise())

        self.assertEqual(update_one_result.modified_count, 1)
        self.assertEqual(update_many_result.modified_count, 2)
        self.assertEqual(replace_one_result.modified_count, 1)
        self.assertEqual(delete_one_result.deleted_count, 1)
        self.assertEqual(delete_many_result.deleted_count, 2)
        self.assertEqual(
            recorded_find,
            [
                {"hint": "kind_rank_idx", "comment": "trace-update-one"},
                {"hint": "kind_idx", "comment": "trace-update-many"},
                {"hint": "replace_idx", "comment": "trace-replace"},
                {"hint": "kind_idx", "comment": "trace-delete-many"},
            ],
        )
        self.assertEqual(
            recorded_select,
            [
                {"hint": "replace_idx", "comment": "trace-replace"},
            ],
        )

    def test_distinct_rejects_non_string_key(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.distinct(1))  # type: ignore[arg-type]

    def test_distinct_includes_null_for_documents_without_matching_values(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "kind": "view"})
                await collection.insert_one({"_id": "2", "other": 1})
                return await collection.distinct("kind")
            finally:
                await engine.disconnect()

        result = asyncio.run(_exercise())

        self.assertEqual(result, ["view", None])

    def test_distinct_honors_custom_dialect_equality(self):
        class CaseInsensitiveDialect(MongoDialect70):
            def values_equal(self, left, right) -> bool:
                if isinstance(left, str) and isinstance(right, str):
                    return left.lower() == right.lower()
                return super().values_equal(left, right)

        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(
                    engine,
                    "db",
                    "coll",
                    mongodb_dialect=CaseInsensitiveDialect(),
                )
                await collection.insert_one({"_id": "1", "tag": "Ada"})
                await collection.insert_one({"_id": "2", "tag": "ada"})
                return await collection.distinct("tag")
            finally:
                await engine.disconnect()

        result = asyncio.run(_exercise())

        self.assertEqual(result, ["Ada"])

    def test_find_one_returns_projection_already_applied_by_engine(self):
        class EngineStub:
            async def get_document(self, *args, **kwargs):
                return {"name": "Ada"}

            def scan_collection(self, *args, **kwargs):
                async def _scan():
                    yield {"_id": "1", "name": "Ada"}

                return _scan()

        collection = AsyncCollection(EngineStub(), "db", "coll")

        direct = asyncio.run(collection.find_one({"_id": "1"}, {"name": 1, "_id": 0}))
        scanned = asyncio.run(collection.find_one({"name": "Ada"}, {"role": 0, "_id": 1}))

        self.assertEqual(direct, {"name": "Ada"})
        self.assertEqual(scanned, {"_id": "1", "name": "Ada"})

    def test_collection_compiles_plan_once_and_passes_it_to_engine(self):
        class EngineStub:
            def __init__(self):
                self.scan_plan = None
                self.update_plan = None
                self.delete_plan = None
                self.count_plan = None
                self.scan_dialect = None
                self.update_dialect = None
                self.delete_dialect = None
                self.count_dialect = None

            async def put_document(self, *args, **kwargs):
                return True

            async def get_document(self, *args, **kwargs):
                return None

            def scan_collection(self, *args, **kwargs):
                self.scan_plan = kwargs["plan"]
                self.scan_dialect = kwargs["dialect"]

                async def _scan():
                    if False:
                        yield None

                return _scan()

            async def update_matching_document(self, *args, **kwargs):
                self.update_plan = kwargs["plan"]
                self.update_dialect = kwargs["dialect"]
                from mongoeco.types import UpdateResult

                return UpdateResult(matched_count=0, modified_count=0)

            async def delete_matching_document(self, *args, **kwargs):
                self.delete_plan = kwargs["plan"]
                self.delete_dialect = kwargs["dialect"]
                from mongoeco.types import DeleteResult

                return DeleteResult(deleted_count=0)

            async def count_matching_documents(self, *args, **kwargs):
                self.count_plan = kwargs["plan"]
                self.count_dialect = kwargs["dialect"]
                return 0

            async def create_index(self, *args, **kwargs):
                self.create_index_args = args
                self.create_index_kwargs = kwargs
                return "idx"

            async def create_indexes(self, *args, **kwargs):
                raise AssertionError("unexpected direct create_indexes call")

            async def list_indexes(self, *args, **kwargs):
                self.list_indexes_args = args
                self.list_indexes_kwargs = kwargs
                return [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}]

            async def index_information(self, *args, **kwargs):
                self.index_information_args = args
                self.index_information_kwargs = kwargs
                return {"_id_": {"key": [("_id", 1)], "unique": True}}

            async def drop_index(self, *args, **kwargs):
                self.drop_index_args = args
                self.drop_index_kwargs = kwargs

            async def drop_indexes(self, *args, **kwargs):
                self.drop_indexes_args = args
                self.drop_indexes_kwargs = kwargs

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        asyncio.run(collection.find({"name": "Ada"}).to_list())
        asyncio.run(collection.update_one({"name": "Ada"}, {"$set": {"active": True}}))
        asyncio.run(collection.delete_one({"name": "Ada"}))
        asyncio.run(collection.count_documents({"name": "Ada"}))

        self.assertEqual(type(engine.scan_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.update_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.delete_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.count_plan).__name__, "EqualsCondition")
        self.assertNotIsInstance(engine.scan_plan, MatchAll)
        self.assertIs(engine.scan_dialect, collection.mongodb_dialect)
        self.assertIs(engine.update_dialect, collection.mongodb_dialect)
        self.assertIs(engine.delete_dialect, collection.mongodb_dialect)
        self.assertIs(engine.count_dialect, collection.mongodb_dialect)

    def test_index_helpers_normalize_and_forward_arguments(self):
        class EngineStub:
            async def create_index(self, *args, **kwargs):
                self.create_index_args = args
                self.create_index_kwargs = kwargs
                return "email_1_created_at_-1"

            async def list_indexes(self, *args, **kwargs):
                self.list_indexes_args = args
                self.list_indexes_kwargs = kwargs
                return [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}]

            async def index_information(self, *args, **kwargs):
                self.index_information_args = args
                self.index_information_kwargs = kwargs
                return {"_id_": {"key": [("_id", 1)], "unique": True}}

            async def drop_index(self, *args, **kwargs):
                self.drop_index_args = args
                self.drop_index_kwargs = kwargs

            async def drop_indexes(self, *args, **kwargs):
                self.drop_indexes_args = args
                self.drop_indexes_kwargs = kwargs

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        name = asyncio.run(
            collection.create_index([("email", 1), ("created_at", -1)], unique=True, name="idx_email")
        )
        async def _exercise():
            cursor = await collection.list_indexes()
            indexes = await cursor.to_list()
            info = await collection.index_information()
            await collection.drop_index([("email", 1), ("created_at", -1)])
            await collection.drop_indexes()
            return indexes, info

        indexes, info = asyncio.run(_exercise())

        self.assertEqual(name, "email_1_created_at_-1")
        self.assertEqual(
            engine.create_index_args,
            ("db", "coll", [("email", 1), ("created_at", -1)]),
        )
        self.assertEqual(
            engine.create_index_kwargs,
            {"unique": True, "name": "idx_email", "context": None},
        )
        self.assertEqual(indexes, [{"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True}])
        self.assertEqual(info, {"_id_": {"key": [("_id", 1)], "unique": True}})
        self.assertEqual(engine.drop_index_args, ("db", "coll", [("email", 1), ("created_at", -1)]))
        self.assertEqual(engine.drop_index_kwargs, {"context": None})
        self.assertEqual(engine.drop_indexes_args, ("db", "coll"))
        self.assertEqual(engine.drop_indexes_kwargs, {"context": None})

    def test_list_indexes_returns_cursor(self):
        class EngineStub:
            async def list_indexes(self, *args, **kwargs):
                return [
                    {"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True},
                    {"name": "email_1", "fields": ["email"], "key": {"email": 1}, "unique": False},
                ]

        collection = AsyncCollection(EngineStub(), "db", "coll")

        async def _exercise():
            cursor = await collection.list_indexes()
            return await cursor.first(), await cursor.to_list()

        first, indexes = asyncio.run(_exercise())

        self.assertEqual(first, {"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True})
        self.assertEqual(
            indexes,
            [
                {"name": "_id_", "fields": ["_id"], "key": {"_id": 1}, "unique": True},
                {"name": "email_1", "fields": ["email"], "key": {"email": 1}, "unique": False},
            ],
        )

    def test_create_indexes_requires_index_models(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll")

        with self.assertRaises(TypeError):
            asyncio.run(collection.create_indexes([{"name": "idx"}]))

        with self.assertRaises(ValueError):
            asyncio.run(collection.create_indexes([]))

    def test_create_index_accepts_ordered_mapping_key_spec(self):
        class EngineStub:
            async def create_index(self, *args, **kwargs):
                self.create_index_args = args
                self.create_index_kwargs = kwargs
                return "email_1_created_at_-1"

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        name = asyncio.run(
            collection.create_index({"email": 1, "created_at": -1}, unique=True)
        )

        self.assertEqual(name, "email_1_created_at_-1")
        self.assertEqual(
            engine.create_index_args,
            ("db", "coll", [("email", 1), ("created_at", -1)]),
        )

    def test_create_indexes_uses_models_sequentially(self):
        class EngineStub:
            def __init__(self):
                self.calls = []

            async def create_index(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                return kwargs["name"] or "email_1"

            async def index_information(self, *args, **kwargs):
                return {"_id_": {"key": [("_id", 1)], "unique": True}}

            async def drop_index(self, *args, **kwargs):
                self.dropped = getattr(self, "dropped", [])
                self.dropped.append((args, kwargs))

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        names = asyncio.run(
            collection.create_indexes(
                [
                    IndexModel([("email", 1)], unique=True),
                    IndexModel([("tenant", 1), ("created_at", -1)], name="tenant_created"),
                ]
            )
        )

        self.assertEqual(names, ["email_1", "tenant_created"])
        self.assertEqual(
            engine.calls,
            [
                (
                    ("db", "coll", [("email", 1)]),
                    {"unique": True, "name": None, "context": None},
                ),
                (
                    ("db", "coll", [("tenant", 1), ("created_at", -1)]),
                    {"unique": False, "name": "tenant_created", "context": None},
                ),
            ],
        )

    def test_update_one_sort_is_rejected_by_older_pymongo_profile(self):
        collection = AsyncCollection(
            MemoryEngine(),
            "db",
            "coll",
            pymongo_profile="4.9",
        )

        with self.assertRaises(TypeError):
            asyncio.run(
                collection.update_one(
                    {"name": "Ada"},
                    {"$set": {"active": True}},
                    sort=[("rank", 1)],
                )
            )

    def test_update_one_sort_updates_first_sorted_match_when_profile_supports_it(self):
        async def _exercise() -> tuple[object, dict | None, dict | None]:
            collection = AsyncCollection(
                MemoryEngine(),
                "db",
                "coll",
                pymongo_profile="4.11",
            )
            await collection.insert_one({"_id": "first", "kind": "view", "rank": 2, "done": False})
            await collection.insert_one({"_id": "second", "kind": "view", "rank": 1, "done": False})

            result = await collection.update_one(
                {"kind": "view"},
                {"$set": {"done": True}},
                sort=[("rank", 1)],
            )
            first = await collection.find_one({"_id": "first"})
            second = await collection.find_one({"_id": "second"})
            return result, first, second

        result, first, second = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertFalse(first["done"])
        self.assertTrue(second["done"])

    def test_update_one_sort_returns_zero_counts_when_no_document_matches(self):
        async def _exercise():
            collection = AsyncCollection(
                MemoryEngine(),
                "db",
                "coll",
                pymongo_profile="4.11",
            )
            return await collection.update_one(
                {"kind": "view"},
                {"$set": {"done": True}},
                sort=[("rank", 1)],
            )

        result = asyncio.run(_exercise())

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_update_one_sort_uses_custom_dialect_for_document_selection(self):
        class ReverseNumericDialect(MongoDialect70):
            def compare_values(self, left, right):
                if (
                    isinstance(left, (int, float))
                    and not isinstance(left, bool)
                    and isinstance(right, (int, float))
                    and not isinstance(right, bool)
                    and left != right
                ):
                    return -1 if left > right else 1
                return super().compare_values(left, right)

        async def _exercise(engine):
            await engine.connect()
            collection = AsyncCollection(
                engine,
                "db",
                "coll",
                mongodb_dialect=ReverseNumericDialect(),
                pymongo_profile="4.11",
            )
            try:
                await collection.insert_one({"_id": "low", "kind": "view", "rank": 1, "done": False})
                await collection.insert_one({"_id": "high", "kind": "view", "rank": 2, "done": False})

                result = await collection.update_one(
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                )
                low = await collection.find_one({"_id": "low"})
                high = await collection.find_one({"_id": "high"})
                return result, low, high
            finally:
                await engine.disconnect()

        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                result, low, high = asyncio.run(_exercise(engine))
                self.assertEqual(result.matched_count, 1)
                self.assertEqual(result.modified_count, 1)
                self.assertFalse(low["done"])
                self.assertTrue(high["done"])

    def test_update_one_applies_custom_dialect_validation_in_engines(self):
        class NoSetDialect(MongoDialect70):
            def supports_update_operator(self, name: str) -> bool:
                if name == "$set":
                    return False
                return super().supports_update_operator(name)

        async def _exercise(engine):
            await engine.connect()
            collection = AsyncCollection(
                engine,
                "db",
                "coll",
                mongodb_dialect=NoSetDialect(),
            )
            try:
                await collection.insert_one({"_id": "1", "done": False})
                await collection.update_one({"_id": "1"}, {"$set": {"done": True}})
            finally:
                await engine.disconnect()

        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                with self.assertRaises(OperationFailure):
                    asyncio.run(_exercise(engine))

    def test_custom_projection_dialect_is_honored_by_find_and_find_one(self):
        class ProjectionFlagTwoDialect(MongoDialect70):
            def projection_flag(self, value: object) -> int | None:
                if value == 2:
                    return 1
                return super().projection_flag(value)

        async def _exercise(engine):
            await engine.connect()
            collection = AsyncCollection(
                engine,
                "db",
                "coll",
                mongodb_dialect=ProjectionFlagTwoDialect(),
            )
            try:
                await collection.insert_one({"_id": "1", "name": "Ada", "role": "admin"})
                direct = await collection.find_one({"_id": "1"}, {"name": 2, "_id": 0})
                scanned = await collection.find({"name": "Ada"}, {"name": 2, "_id": 0}).to_list()
                return direct, scanned
            finally:
                await engine.disconnect()

        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                direct, scanned = asyncio.run(_exercise(engine))
                self.assertEqual(direct, {"name": "Ada"})
                self.assertEqual(scanned, [{"name": "Ada"}])
