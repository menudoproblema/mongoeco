import datetime

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

        with self.assertRaises(OperationFailure) as ctx:
            asyncio.run(_exercise())

        self.assertEqual(ctx.exception.code, 66)

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

    def test_materialize_replacement_document_does_not_invent_missing_id(self):
        selected = {"kind": "legacy"}
        replacement = {"done": True}

        materialized = AsyncCollection._materialize_replacement_document(selected, replacement)

        self.assertEqual(materialized, {"done": True})

    def test_legacy_documents_without_id_do_not_corrupt_storage(self):
        async def _exercise(engine_type):
            engine = engine_type()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")

                async def seed_legacy_document():
                    await engine.drop_collection("db", "coll")
                    await engine.put_document(
                        "db",
                        "coll",
                        {"kind": "legacy"},
                        overwrite=False,
                    )

                await seed_legacy_document()
                field_update = await collection.update_one(
                    {"kind": "legacy"},
                    {"$set": {"done": True}},
                )
                after_field_update = await collection.find({}).to_list()

                await seed_legacy_document()
                try:
                    await collection.update_one(
                        {"kind": "legacy"},
                        {"$set": {"_id": "new", "done": True}},
                    )
                except OperationFailure as exc:
                    id_update_error = exc
                else:
                    raise AssertionError("update_one should reject creating _id on legacy storage")
                after_id_update = await collection.find({}).to_list()

                await seed_legacy_document()
                replacement = await collection.replace_one(
                    {"kind": "legacy"},
                    {"kind": "legacy", "replaced": True},
                )
                after_replacement = await collection.find({}).to_list()

                await seed_legacy_document()
                try:
                    await collection.replace_one(
                        {"kind": "legacy"},
                        {"_id": "new", "kind": "legacy"},
                    )
                except OperationFailure as exc:
                    replacement_error = exc
                else:
                    raise AssertionError("replace_one should reject adding _id to legacy storage")
                after_replacement_error = await collection.find({}).to_list()

                await seed_legacy_document()
                find_one_update = await collection.find_one_and_update(
                    {"kind": "legacy"},
                    {"$set": {"done": True}},
                    return_document=ReturnDocument.AFTER,
                )
                after_find_one_update = await collection.find({}).to_list()

                await seed_legacy_document()
                update_many = await collection.update_many(
                    {"kind": "legacy"},
                    {"$set": {"done": True}},
                )
                after_update_many = await collection.find({}).to_list()

                await seed_legacy_document()
                delete_result = await collection.delete_one({"kind": "legacy"})
                after_delete = await collection.find({}).to_list()

                return (
                    field_update,
                    after_field_update,
                    id_update_error,
                    after_id_update,
                    replacement,
                    after_replacement,
                    replacement_error,
                    after_replacement_error,
                    find_one_update,
                    after_find_one_update,
                    update_many,
                    after_update_many,
                    delete_result,
                    after_delete,
                )
            finally:
                await engine.disconnect()

        for engine_type in (MemoryEngine, SQLiteEngine):
            with self.subTest(engine=engine_type.__name__):
                (
                    field_update,
                    after_field_update,
                    id_update_error,
                    after_id_update,
                    replacement,
                    after_replacement,
                    replacement_error,
                    after_replacement_error,
                    find_one_update,
                    after_find_one_update,
                    update_many,
                    after_update_many,
                    delete_result,
                    after_delete,
                ) = asyncio.run(_exercise(engine_type))

                self.assertEqual(field_update.modified_count, 1)
                self.assertEqual(after_field_update, [{"kind": "legacy", "done": True}])
                self.assertEqual(id_update_error.code, 66)
                self.assertEqual(after_id_update, [{"kind": "legacy"}])
                self.assertEqual(replacement.modified_count, 1)
                self.assertEqual(after_replacement, [{"kind": "legacy", "replaced": True}])
                self.assertEqual(replacement_error.code, 66)
                self.assertEqual(after_replacement_error, [{"kind": "legacy"}])
                self.assertEqual(find_one_update, {"kind": "legacy", "done": True})
                self.assertEqual(after_find_one_update, [{"kind": "legacy", "done": True}])
                self.assertEqual(update_many.modified_count, 1)
                self.assertEqual(after_update_many, [{"kind": "legacy", "done": True}])
                self.assertEqual(delete_result.deleted_count, 1)
                self.assertEqual(after_delete, [])

    def test_corrupt_documents_with_mismatched_storage_key_are_not_retargeted(self):
        async def _exercise(engine_type):
            engine = engine_type()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")

                async def seed_corrupt_document():
                    await engine.drop_collection("db", "coll")
                    await engine.put_document(
                        "db",
                        "coll",
                        {"_id": "old", "kind": "corrupt"},
                        overwrite=False,
                    )
                    if isinstance(engine, MemoryEngine):
                        engine._storage["db"]["coll"][
                            engine._storage_key("old")
                        ] = engine._encode_storage_document(
                            {"_id": "new", "kind": "corrupt"}
                        )
                    else:
                        with engine._lock:
                            conn = engine._require_connection(None)
                            conn.execute(
                                (
                                    "UPDATE documents SET document = ? "
                                    "WHERE db_name = ? AND coll_name = ? AND storage_key = ?"
                                ),
                                (
                                    engine._serialize_document({"_id": "new", "kind": "corrupt"}),
                                    "db",
                                    "coll",
                                    engine._storage_key("old"),
                                ),
                            )
                            conn.commit()

                async def capture_error(operation):
                    await seed_corrupt_document()
                    try:
                        await operation()
                    except OperationFailure as exc:
                        error = exc
                    else:
                        raise AssertionError("operation should reject mismatched _id storage")
                    return error, await collection.find({}).to_list()

                update_error = await capture_error(
                    lambda: collection.update_one({"kind": "corrupt"}, {"$set": {"done": True}})
                )
                replace_error = await capture_error(
                    lambda: collection.replace_one({"kind": "corrupt"}, {"kind": "replaced"})
                )
                noop_update_error = await capture_error(
                    lambda: collection.update_one({"_id": "new"}, {"$set": {"kind": "corrupt"}})
                )
                update_many_error = await capture_error(
                    lambda: collection.update_many({"kind": "corrupt"}, {"$set": {"done": True}})
                )
                delete_one_error = await capture_error(
                    lambda: collection.delete_one({"kind": "corrupt"})
                )
                delete_many_error = await capture_error(
                    lambda: collection.delete_many({"kind": "corrupt"})
                )
                find_one_delete_error = await capture_error(
                    lambda: collection.find_one_and_delete({"kind": "corrupt"})
                )
                await seed_corrupt_document()
                await collection.create_index("kind")
                try:
                    await collection.delete_one({"kind": "corrupt"}, hint="kind_1")
                except OperationFailure as exc:
                    delete_hint_error = exc
                else:
                    raise AssertionError("delete_one with hint should reject mismatched _id storage")
                after_delete_hint = await collection.find({}).to_list()

                return (
                    update_error,
                    replace_error,
                    noop_update_error,
                    update_many_error,
                    delete_one_error,
                    delete_many_error,
                    find_one_delete_error,
                    (delete_hint_error, after_delete_hint),
                )
            finally:
                await engine.disconnect()

        for engine_type in (MemoryEngine, SQLiteEngine):
            with self.subTest(engine=engine_type.__name__):
                results = asyncio.run(_exercise(engine_type))
                for error, after_operation in results:
                    self.assertEqual(error.code, 66)
                    self.assertEqual(after_operation, [{"_id": "new", "kind": "corrupt"}])

    def test_legacy_root_array_id_documents_are_not_retargeted(self):
        async def _exercise(engine_type):
            engine = engine_type()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")

                async def seed_array_id_document():
                    await engine.drop_collection("db", "coll")
                    document = {"_id": [1], "kind": "array-id"}
                    if isinstance(engine, MemoryEngine):
                        engine._storage.setdefault("db", {}).setdefault("coll", {})[
                            engine._storage_key([1])
                        ] = engine._encode_storage_document(document)
                    else:
                        with engine._lock:
                            conn = engine._require_connection(None)
                            engine._ensure_collection_row(conn, "db", "coll")
                            conn.execute(
                                (
                                    "INSERT OR REPLACE INTO documents "
                                    "(db_name, coll_name, storage_key, document) "
                                    "VALUES (?, ?, ?, ?)"
                                ),
                                (
                                    "db",
                                    "coll",
                                    engine._storage_key([1]),
                                    engine._serialize_document(document),
                                ),
                            )
                            conn.commit()

                async def capture_error(operation):
                    await seed_array_id_document()
                    try:
                        await operation()
                    except OperationFailure as exc:
                        error = exc
                    else:
                        raise AssertionError("operation should reject root array _id")
                    return error, await collection.find({}).to_list()

                classic_update_error = await capture_error(
                    lambda: collection.update_one({"kind": "array-id"}, {"$set": {"done": True}})
                )
                pipeline_update_error = await capture_error(
                    lambda: collection.update_one(
                        {"kind": "array-id"},
                        [{"$project": {"_id": 0, "kind": 1, "done": {"$literal": True}}}],
                    )
                )
                update_many_error = await capture_error(
                    lambda: collection.update_many({"kind": "array-id"}, {"$set": {"done": True}})
                )
                delete_one_error = await capture_error(
                    lambda: collection.delete_one({"kind": "array-id"})
                )
                find_one_delete_error = await capture_error(
                    lambda: collection.find_one_and_delete({"kind": "array-id"})
                )
                direct_delete_error = await capture_error(
                    lambda: engine.delete_document("db", "coll", [1])
                )

                return (
                    classic_update_error,
                    pipeline_update_error,
                    update_many_error,
                    delete_one_error,
                    find_one_delete_error,
                    direct_delete_error,
                )
            finally:
                await engine.disconnect()

        for engine_type in (MemoryEngine, SQLiteEngine):
            with self.subTest(engine=engine_type.__name__):
                results = asyncio.run(_exercise(engine_type))
                for error, after_operation in results:
                    self.assertEqual(error.code, 53)
                    self.assertEqual(after_operation, [{"_id": [1], "kind": "array-id"}])

    def test_ttl_purge_validates_legacy_storage_identity_before_delete(self):
        expired_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)

        async def _exercise(engine_type, storage_id, document):
            engine = engine_type()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.create_index("expires_at", expire_after_seconds=0)
                storage_key = engine._storage_key(storage_id)
                if isinstance(engine, MemoryEngine):
                    engine._storage.setdefault("db", {}).setdefault("coll", {})[
                        storage_key
                    ] = engine._encode_storage_document(document)
                    engine._collections.setdefault("db", set()).add("coll")
                else:
                    with engine._lock:
                        conn = engine._require_connection(None)
                        engine._ensure_collection_row(conn, "db", "coll")
                        conn.execute(
                            (
                                "INSERT OR REPLACE INTO documents "
                                "(db_name, coll_name, storage_key, document) "
                                "VALUES (?, ?, ?, ?)"
                            ),
                            (
                                "db",
                                "coll",
                                storage_key,
                                engine._serialize_document(document),
                            ),
                        )
                        conn.commit()

                try:
                    await collection.find({}).to_list()
                except OperationFailure as exc:
                    error = exc
                else:
                    raise AssertionError("TTL purge should reject corrupt storage identity")

                if isinstance(engine, MemoryEngine):
                    still_present = storage_key in engine._storage.get("db", {}).get("coll", {})
                else:
                    with engine._lock:
                        conn = engine._require_connection(None)
                        row = conn.execute(
                            (
                                "SELECT COUNT(*) FROM documents "
                                "WHERE db_name = ? AND coll_name = ? AND storage_key = ?"
                            ),
                            ("db", "coll", storage_key),
                        ).fetchone()
                    still_present = row[0] == 1
                return error, still_present
            finally:
                await engine.disconnect()

        cases = (
            ("array-id", [1], {"_id": [1], "kind": "array-id", "expires_at": expired_at}, 53),
            ("mismatched-key", "old", {"_id": "new", "kind": "corrupt", "expires_at": expired_at}, 66),
        )
        for engine_type in (MemoryEngine, SQLiteEngine):
            for _name, storage_id, document, expected_code in cases:
                with self.subTest(engine=engine_type.__name__, case=_name):
                    error, still_present = asyncio.run(_exercise(engine_type, storage_id, document))
                    self.assertEqual(error.code, expected_code)
                    self.assertTrue(still_present)

    def test_create_ttl_index_validates_legacy_storage_identity_atomically(self):
        expired_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)

        async def _exercise(engine_type, storage_id, document):
            engine = engine_type()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                storage_key = engine._storage_key(storage_id)
                if isinstance(engine, MemoryEngine):
                    engine._storage.setdefault("db", {}).setdefault("coll", {})[
                        storage_key
                    ] = engine._encode_storage_document(document)
                    engine._collections.setdefault("db", set()).add("coll")
                else:
                    with engine._lock:
                        conn = engine._require_connection(None)
                        engine._ensure_collection_row(conn, "db", "coll")
                        conn.execute(
                            (
                                "INSERT OR REPLACE INTO documents "
                                "(db_name, coll_name, storage_key, document) "
                                "VALUES (?, ?, ?, ?)"
                            ),
                            (
                                "db",
                                "coll",
                                storage_key,
                                engine._serialize_document(document),
                            ),
                        )
                        conn.commit()

                try:
                    await collection.create_index(
                        "expires_at",
                        expire_after_seconds=0,
                        name="ttl_idx",
                    )
                except OperationFailure as exc:
                    error = exc
                else:
                    raise AssertionError("create_index should reject corrupt TTL candidates")

                index_information = await collection.index_information()
                if isinstance(engine, MemoryEngine):
                    still_present = storage_key in engine._storage.get("db", {}).get("coll", {})
                else:
                    with engine._lock:
                        conn = engine._require_connection(None)
                        row = conn.execute(
                            (
                                "SELECT COUNT(*) FROM documents "
                                "WHERE db_name = ? AND coll_name = ? AND storage_key = ?"
                            ),
                            ("db", "coll", storage_key),
                        ).fetchone()
                    still_present = row[0] == 1
                return error, index_information, still_present
            finally:
                await engine.disconnect()

        cases = (
            ("array-id", [1], {"_id": [1], "kind": "array-id", "expires_at": expired_at}, 53),
            ("mismatched-key", "old", {"_id": "new", "kind": "corrupt", "expires_at": expired_at}, 66),
        )
        for engine_type in (MemoryEngine, SQLiteEngine):
            for _name, storage_id, document, expected_code in cases:
                with self.subTest(engine=engine_type.__name__, case=_name):
                    error, index_information, still_present = asyncio.run(
                        _exercise(engine_type, storage_id, document)
                    )
                    self.assertEqual(error.code, expected_code)
                    self.assertNotIn("ttl_idx", index_information)
                    self.assertTrue(still_present)

    def test_corrupt_documents_do_not_match_old_storage_key_id_lookup(self):
        async def _exercise(engine_type):
            engine = engine_type()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")

                async def seed_corrupt_document():
                    await engine.drop_collection("db", "coll")
                    await engine.put_document(
                        "db",
                        "coll",
                        {"_id": "old", "kind": "corrupt"},
                        overwrite=False,
                    )
                    if isinstance(engine, MemoryEngine):
                        engine._storage["db"]["coll"][
                            engine._storage_key("old")
                        ] = engine._encode_storage_document(
                            {"_id": "new", "kind": "corrupt"}
                        )
                    else:
                        with engine._lock:
                            conn = engine._require_connection(None)
                            conn.execute(
                                (
                                    "UPDATE documents SET document = ? "
                                    "WHERE db_name = ? AND coll_name = ? AND storage_key = ?"
                                ),
                                (
                                    engine._serialize_document({"_id": "new", "kind": "corrupt"}),
                                    "db",
                                    "coll",
                                    engine._storage_key("old"),
                                ),
                            )
                            conn.commit()

                await seed_corrupt_document()
                find_one_old = await collection.find_one({"_id": "old"})
                find_old = await collection.find({"_id": "old"}).to_list()
                find_one_new = await collection.find_one({"_id": "new"})
                find_new = await collection.find({"_id": "new"}).to_list()
                count_new = await collection.count_documents({"_id": "new"})
                try:
                    await collection.insert_one({"_id": "new", "kind": "fresh"})
                except DuplicateKeyError as exc:
                    duplicate_insert_error = exc
                else:
                    raise AssertionError("insert_one should reject duplicate payload _id")
                after_duplicate_insert = await collection.find({}).to_list()
                update_old = await collection.update_one(
                    {"_id": "old"},
                    {"$set": {"kind": "corrupt"}},
                )
                after_update_old = await collection.find({}).to_list()
                direct_delete_old = await engine.delete_document("db", "coll", "old")
                after_direct_delete_old = await collection.find({}).to_list()

                await seed_corrupt_document()
                delete_old = await collection.delete_one({"_id": "old"})
                after_delete_old = await collection.find({}).to_list()

                await engine.drop_collection("db", "coll")
                await engine.put_document("db", "coll", {"kind": "legacy"}, overwrite=False)
                find_missing_id = await collection.find_one({"_id": None})
                scan_missing_id = await collection.find({"_id": None}).to_list()
                direct_delete_missing_id = await engine.delete_document("db", "coll", None)
                after_direct_delete_missing_id = await collection.find({}).to_list()

                return (
                    find_one_old,
                    find_old,
                    find_one_new,
                    find_new,
                    count_new,
                    duplicate_insert_error,
                    after_duplicate_insert,
                    update_old,
                    after_update_old,
                    direct_delete_old,
                    after_direct_delete_old,
                    delete_old,
                    after_delete_old,
                    find_missing_id,
                    scan_missing_id,
                    direct_delete_missing_id,
                    after_direct_delete_missing_id,
                )
            finally:
                await engine.disconnect()

        for engine_type in (MemoryEngine, SQLiteEngine):
            with self.subTest(engine=engine_type.__name__):
                (
                    find_one_old,
                    find_old,
                    find_one_new,
                    find_new,
                    count_new,
                    duplicate_insert_error,
                    after_duplicate_insert,
                    update_old,
                    after_update_old,
                    direct_delete_old,
                    after_direct_delete_old,
                    delete_old,
                    after_delete_old,
                    find_missing_id,
                    scan_missing_id,
                    direct_delete_missing_id,
                    after_direct_delete_missing_id,
                ) = asyncio.run(_exercise(engine_type))

                self.assertIsNone(find_one_old)
                self.assertEqual(find_old, [])
                self.assertEqual(find_one_new, {"_id": "new", "kind": "corrupt"})
                self.assertEqual(find_new, [{"_id": "new", "kind": "corrupt"}])
                self.assertEqual(count_new, 1)
                self.assertIsInstance(duplicate_insert_error, DuplicateKeyError)
                self.assertEqual(after_duplicate_insert, [{"_id": "new", "kind": "corrupt"}])
                self.assertEqual(update_old.matched_count, 0)
                self.assertEqual(after_update_old, [{"_id": "new", "kind": "corrupt"}])
                self.assertFalse(direct_delete_old)
                self.assertEqual(after_direct_delete_old, [{"_id": "new", "kind": "corrupt"}])
                self.assertEqual(delete_old.deleted_count, 0)
                self.assertEqual(after_delete_old, [{"_id": "new", "kind": "corrupt"}])
                self.assertEqual(find_missing_id, {"kind": "legacy"})
                self.assertEqual(scan_missing_id, [{"kind": "legacy"}])
                self.assertTrue(direct_delete_missing_id)
                self.assertEqual(after_direct_delete_missing_id, [])

    def test_selected_writes_can_target_legacy_documents_without_id(self):
        async def _exercise(engine_type):
            engine = engine_type()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")

                async def seed_legacy_document():
                    await engine.drop_collection("db", "coll")
                    await engine.put_document("db", "coll", {"kind": "legacy"}, overwrite=False)

                await seed_legacy_document()
                update_many = await collection.update_many(
                    {"kind": "legacy"},
                    {"$set": {"value": 1}},
                )
                after_update_many = await collection.find({}).to_list()

                await seed_legacy_document()
                find_one_and_update_after = await collection.find_one_and_update(
                    {"kind": "legacy"},
                    {"$set": {"value": 1}},
                    return_document=ReturnDocument.AFTER,
                )
                after_find_one_and_update = await collection.find({}).to_list()

                await seed_legacy_document()
                find_one_and_replace_after = await collection.find_one_and_replace(
                    {"kind": "legacy"},
                    {"kind": "replaced"},
                    return_document=ReturnDocument.AFTER,
                )
                after_find_one_and_replace = await collection.find({}).to_list()

                await seed_legacy_document()
                delete_many = await collection.delete_many({"kind": "legacy"})
                after_delete_many = await collection.find({}).to_list()

                await seed_legacy_document()
                find_one_and_delete = await collection.find_one_and_delete({"kind": "legacy"})
                after_find_one_and_delete = await collection.find({}).to_list()

                return (
                    update_many,
                    after_update_many,
                    find_one_and_update_after,
                    after_find_one_and_update,
                    find_one_and_replace_after,
                    after_find_one_and_replace,
                    delete_many,
                    after_delete_many,
                    find_one_and_delete,
                    after_find_one_and_delete,
                )
            finally:
                await engine.disconnect()

        for engine_type in (MemoryEngine, SQLiteEngine):
            with self.subTest(engine=engine_type.__name__):
                (
                    update_many,
                    after_update_many,
                    find_one_and_update_after,
                    after_find_one_and_update,
                    find_one_and_replace_after,
                    after_find_one_and_replace,
                    delete_many,
                    after_delete_many,
                    find_one_and_delete,
                    after_find_one_and_delete,
                ) = asyncio.run(_exercise(engine_type))

                self.assertEqual(update_many.matched_count, 1)
                self.assertEqual(update_many.modified_count, 1)
                self.assertEqual(after_update_many, [{"kind": "legacy", "value": 1}])
                self.assertEqual(find_one_and_update_after, {"kind": "legacy", "value": 1})
                self.assertEqual(after_find_one_and_update, [{"kind": "legacy", "value": 1}])
                self.assertEqual(find_one_and_replace_after, {"kind": "replaced"})
                self.assertEqual(after_find_one_and_replace, [{"kind": "replaced"}])
                self.assertEqual(delete_many.deleted_count, 1)
                self.assertEqual(after_delete_many, [])
                self.assertEqual(find_one_and_delete, {"kind": "legacy"})
                self.assertEqual(after_find_one_and_delete, [])

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

        with self.assertRaises(OperationFailure) as ctx:
            asyncio.run(_exercise())

        self.assertEqual(ctx.exception.code, 66)

    def test_find_one_and_replace_rejects_changing_id_with_write_error_code(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada"})
                await collection.find_one_and_replace({"_id": "1"}, {"_id": "2", "name": "Grace"})
            finally:
                await engine.disconnect()

        with self.assertRaises(OperationFailure) as ctx:
            asyncio.run(_exercise())

        self.assertEqual(ctx.exception.code, 66)

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
