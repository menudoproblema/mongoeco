from tests.unit.api._collection_test_support import *  # noqa: F403

class AsyncCollectionFindModifyTests(AsyncCollectionHelperBase):
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
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = []

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.replace_one({"name": "Ada"}, {"name": "Grace"}))

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_replace_one_upsert_builds_seeded_document(self):
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = []

            def __init__(self):
                self.document = None

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
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = []

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

    def test_find_one_and_update_accepts_foreign_return_document_enum(self):
        class _ForeignReturnDocument(Enum):
            BEFORE = 0
            AFTER = 1

        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "done": False})
                return await collection.find_one_and_update(
                    {"_id": "1"},
                    {"$set": {"done": True}},
                    return_document=_ForeignReturnDocument.AFTER,
                    projection={"done": 1, "_id": 0},
                )
            finally:
                await engine.disconnect()

        result = asyncio.run(_exercise())

        self.assertEqual(result, {"done": True})

    def test_find_one_and_update_accepts_pymongo_style_bool_return_document(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "done": False})
                after = await collection.find_one_and_update(
                    {"_id": "1"},
                    {"$set": {"done": True}},
                    return_document=True,
                    projection={"done": 1, "_id": 0},
                )
                before = await collection.find_one_and_update(
                    {"_id": "1"},
                    {"$set": {"done": False}},
                    return_document=False,
                    projection={"done": 1, "_id": 0},
                )
                return after, before
            finally:
                await engine.disconnect()

        after, before = asyncio.run(_exercise())

        self.assertEqual(after, {"done": True})
        self.assertEqual(before, {"done": True})

    def test_find_one_and_update_returns_none_when_nothing_matches_without_upsert(self):
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = []

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

    def test_find_one_and_update_and_delete_support_positional_projection(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", pymongo_profile="4.11")
                await collection.insert_one(
                    {
                        "_id": "1",
                        "students": [
                            {"school": 100, "age": 7},
                            {"school": 102, "age": 10},
                            {"school": 102, "age": 11},
                        ],
                    }
                )
                before = await collection.find_one_and_update(
                    {"_id": "1", "students.school": 102, "students.age": {"$gt": 10}},
                    {"$set": {"flag": True}},
                    return_document=ReturnDocument.BEFORE,
                    projection={"students.$": 1, "_id": 0},
                )
                deleted = await collection.find_one_and_delete(
                    {"_id": "1", "students.school": 102, "students.age": {"$gt": 10}},
                    projection={"students.$": 1, "_id": 0},
                )
                return before, deleted
            finally:
                await engine.disconnect()

        before, deleted = asyncio.run(_exercise())

        self.assertEqual(before, {"students": [{"school": 102, "age": 11}]})
        self.assertEqual(deleted, {"students": [{"school": 102, "age": 11}]})

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
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = []

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
                await collection.create_index([("name", 1)])
                updated = await collection.find_one_and_update(
                    {"_id": "1"},
                    {"$set": {"done": True}},
                    return_document=ReturnDocument.AFTER,
                    hint="_id_",
                    comment="trace",
                    max_time_ms=5,
                    let={"tenant": "a"},
                )
                replaced = await collection.find_one_and_replace(
                    {"_id": "1"},
                    {"name": "Ada", "done": False},
                    return_document=ReturnDocument.AFTER,
                    hint="_id_",
                    comment="trace",
                    max_time_ms=5,
                    let={"tenant": "a"},
                )
                deleted = await collection.find_one_and_delete(
                    {"_id": "1"},
                    hint="_id_",
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
                await collection.create_index([("kind", 1), ("rank", 1)], name="kind_rank_idx")
                await collection.create_index([("kind", 1)], name="kind_idx")

                recorded_find: list[dict[str, object | None]] = []
                recorded_select: list[dict[str, object | None]] = []
                original_build_cursor = collection._build_cursor
                original_select = collection._select_first_document

                def _wrapped_build_cursor(*args, **kwargs):
                    operation = args[0] if args else None
                    recorded_find.append(
                        {
                            "hint": getattr(operation, "hint", kwargs.get("hint")),
                            "comment": getattr(operation, "comment", kwargs.get("comment")),
                        }
                    )
                    return original_build_cursor(*args, **kwargs)

                async def _wrapped_select(*args, **kwargs):
                    recorded_select.append(
                        {
                            "hint": kwargs.get("hint"),
                            "comment": kwargs.get("comment"),
                        }
                    )
                    return await original_select(*args, **kwargs)

                collection._build_cursor = _wrapped_build_cursor  # type: ignore[method-assign]
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
                    hint="kind_idx",
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
                {"hint": "kind_idx", "comment": "trace-replace"},
                {"hint": "_id_", "comment": "trace-delete-one"},
                {"hint": "kind_idx", "comment": "trace-delete-many"},
            ],
        )
        self.assertEqual(
            recorded_select,
            [
                {"hint": "kind_idx", "comment": "trace-replace"},
            ],
        )

    def test_find_rejects_missing_hint_index(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "kind": "view"})
                with self.assertRaises(OperationFailure):
                    await collection.find({"kind": "view"}, hint="missing_idx").to_list()
            finally:
                await engine.disconnect()

        asyncio.run(_exercise())

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
