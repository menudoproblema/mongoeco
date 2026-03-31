from tests.unit.api._collection_test_support import *  # noqa: F403

class AsyncCollectionWriteTests(AsyncCollectionHelperBase):
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

    def test_insert_many_prefers_bulk_engine_path_when_available(self):
        class EngineStub:
            def __init__(self):
                self.bulk_calls = 0

            async def put_documents_bulk(self, *args, **kwargs):
                self.bulk_calls += 1
                return [True, True]

        async def _exercise():
            engine = EngineStub()
            collection = AsyncCollection(engine, "db", "coll")
            result = await collection.insert_many([{"name": "Ada"}, {"name": "Grace"}])
            return engine.bulk_calls, result.inserted_ids

        bulk_calls, inserted_ids = asyncio.run(_exercise())

        self.assertEqual(bulk_calls, 1)
        self.assertEqual(len(inserted_ids), 2)

    def test_insert_many_profiles_bulk_engine_errors(self):
        class EngineStub:
            async def put_documents_bulk(self, *args, **kwargs):
                raise RuntimeError("bulk boom")

        async def _exercise():
            collection = AsyncCollection(EngineStub(), "db", "coll")
            profiled = []

            async def _profile_operation(**payload):
                profiled.append(payload)

            collection._profile_operation = _profile_operation  # type: ignore[method-assign]
            await collection.insert_many([{"_id": "1"}])

        with self.assertRaisesRegex(RuntimeError, "bulk boom"):
            asyncio.run(_exercise())

    def test_insert_many_rejects_bulk_result_length_mismatch(self):
        class EngineStub:
            async def put_documents_bulk(self, *args, **kwargs):
                return [True]

        async def _exercise():
            collection = AsyncCollection(EngineStub(), "db", "coll")
            await collection.insert_many([{"name": "Ada"}, {"name": "Grace"}])

        with self.assertRaisesRegex(RuntimeError, "result count different"):
            asyncio.run(_exercise())

    def test_find_one_profiles_direct_id_lookup_errors(self):
        class EngineStub:
            async def get_document(self, *args, **kwargs):
                raise RuntimeError("lookup boom")

        async def _exercise():
            collection = AsyncCollection(EngineStub(), "db", "coll")
            profiled = []

            async def _profile_operation(**payload):
                profiled.append(payload)

            collection._profile_operation = _profile_operation  # type: ignore[method-assign]
            await collection.find_one({"_id": "1"})

        with self.assertRaisesRegex(RuntimeError, "lookup boom"):
            asyncio.run(_exercise())

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

    def test_filter_keyword_alias_is_supported_in_async_collection(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "kind": "view"})
                found = await collection.find_one(filter={"_id": "1"})
                count = await collection.count_documents(filter={"kind": "view"})
                distinct = await collection.distinct("kind", filter={"_id": "1"})
                return found, count, distinct
            finally:
                await engine.disconnect()

        found, count, distinct = asyncio.run(_exercise())

        self.assertEqual(found, {"_id": "1", "name": "Ada", "kind": "view"})
        self.assertEqual(count, 1)
        self.assertEqual(distinct, ["view"])

    def test_filter_keyword_alias_conflicts_with_filter_spec(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll")

        with self.assertRaises(TypeError):
            asyncio.run(collection.find_one({"_id": "1"}, filter={"_id": "1"}))

    def test_find_one_accepts_profile_normalized_kwargs(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "rank": 2})
                await collection.insert_one({"_id": "2", "name": "Grace", "rank": 1})
                return await collection.find_one(
                    filter={"name": {"$in": ["Ada", "Grace"]}},
                    sort=[("rank", 1)],
                    skip=1,
                )
            finally:
                await engine.disconnect()

        found = asyncio.run(_exercise())

        self.assertEqual(found, {"_id": "1", "name": "Ada", "rank": 2})

    def test_find_and_find_one_accept_dict_sort(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "rank": 2})
                await collection.insert_one({"_id": "2", "name": "Grace", "rank": 1})
                found = await collection.find_one(
                    {"name": {"$in": ["Ada", "Grace"]}},
                    sort={"rank": 1},
                )
                listed = await collection.find({}, sort={"rank": 1}).to_list()
                return found, listed
            finally:
                await engine.disconnect()

        found, listed = asyncio.run(_exercise())

        self.assertEqual(found, {"_id": "2", "name": "Grace", "rank": 1})
        self.assertEqual([document["_id"] for document in listed], ["2", "1"])

    def test_find_one_rejects_unknown_public_kwargs(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll")

        with self.assertRaises(TypeError):
            asyncio.run(collection.find_one(filter={"_id": "1"}, unsupported=True))

    def test_update_keyword_alias_is_supported_in_async_collection(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_one({"_id": "1", "name": "Ada", "count": 1})
                await collection.update_one(filter={"_id": "1"}, update={"$inc": {"count": 1}})
                found = await collection.find_one({"_id": "1"})
                return found
            finally:
                await engine.disconnect()

        found = asyncio.run(_exercise())
        self.assertEqual(found, {"_id": "1", "name": "Ada", "count": 2})

    def test_update_keyword_alias_conflicts_with_update_spec(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll")

        with self.assertRaises(TypeError):
            asyncio.run(
                collection.update_one(
                    {"_id": "1"},
                    {"$set": {"name": "Ada"}},
                    update={"$set": {"name": "Grace"}},
                )
            )

    def test_bulk_write_requires_non_empty_write_model_list(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write(InsertOne({"name": "Ada"})))  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            asyncio.run(self.collection.bulk_write([]))
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write([{"insert": {"name": "Ada"}}]))  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.bulk_write([InsertOne({"name": "Ada"})], ordered=1))  # type: ignore[arg-type]

    def test_bulk_write_requires_list_requests(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                return await collection.bulk_write((InsertOne({"_id": "1"}),))  # type: ignore[arg-type]
            finally:
                await engine.disconnect()

        with self.assertRaises(TypeError):
            asyncio.run(_exercise())

    def test_update_one_with_sort_and_upsert_does_not_call_engine_update_path_twice(self):
        class EngineStub(MemoryEngine):
            def __init__(self):
                super().__init__()
                self.update_calls = 0

            async def update_with_operation(self, *args, **kwargs):
                self.update_calls += 1
                return await super().update_with_operation(*args, **kwargs)

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
                        "array_filters": None,
                        "hint": "req_hint",
                        "comment": "req_comment",
                        "let": {"scope": "request"},
                        "session": None,
                    },
                ),
                (
                    "update_many",
                    {
                        "array_filters": None,
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

    def test_bulk_write_propagates_bypass_document_validation_to_wrapped_calls(self):
        class EngineStub:
            pass

        async def _exercise():
            collection = AsyncCollection(EngineStub(), "db", "coll")
            recorded = {}

            async def _update_one(*args, **kwargs):
                recorded["update_one"] = kwargs
                return UpdateResult(matched_count=1, modified_count=1)

            async def _update_many(*args, **kwargs):
                recorded["update_many"] = kwargs
                return UpdateResult(matched_count=1, modified_count=1)

            async def _replace_one(*args, **kwargs):
                recorded["replace_one"] = kwargs
                return UpdateResult(matched_count=1, modified_count=1)

            collection.update_one = _update_one  # type: ignore[method-assign]
            collection.update_many = _update_many  # type: ignore[method-assign]
            collection.replace_one = _replace_one  # type: ignore[method-assign]

            await collection.bulk_write(
                [
                    UpdateOne({"_id": "1"}, {"$set": {"done": True}}),
                    UpdateMany({"done": False}, {"$set": {"done": True}}),
                    ReplaceOne({"_id": "1"}, {"done": True}),
                ],
                bypass_document_validation=True,
            )
            return recorded

        recorded = asyncio.run(_exercise())

        self.assertTrue(recorded["update_one"]["bypass_document_validation"])
        self.assertTrue(recorded["update_many"]["bypass_document_validation"])
        self.assertTrue(recorded["replace_one"]["bypass_document_validation"])

    def test_update_methods_reject_invalid_array_filters_shapes(self):
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.update_one({}, {"$set": {"done": True}}, array_filters="bad"))  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.update_many({}, {"$set": {"done": True}}, array_filters=[1]))  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.find_one_and_update({}, {"$set": {"done": True}}, array_filters="bad"))  # type: ignore[arg-type]

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
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = [{"profile": {"city": "Madrid"}}]

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with patch("mongoeco.api._async.collection.QueryEngine.extract_values", return_value=[]):
            with patch("mongoeco.api._async.collection.QueryEngine._get_field_value", return_value=(True, "Madrid")):
                result = asyncio.run(collection.distinct("profile.city"))

        self.assertEqual(result, ["Madrid"])

    def test_distinct_skips_list_fallback_when_exact_path_is_an_empty_array(self):
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = [{"profile": {"city": []}}]

        collection = AsyncCollection(EngineStub(), "db", "coll")

        with patch("mongoeco.api._async.collection.QueryEngine.extract_values", return_value=[]):
            with patch("mongoeco.api._async.collection.QueryEngine._get_field_value", return_value=(True, [])):
                result = asyncio.run(collection.distinct("profile.city"))

        self.assertEqual(result, [])

    def test_distinct_keeps_nested_array_values_when_extract_values_returns_only_one_list(self):
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = [{"matrix": [[1, 2, 3]]}]

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.distinct("matrix"))

        self.assertEqual(result, [[1, 2, 3]])

    def test_distinct_candidates_covers_array_unwrap_edge_cases(self):
        from mongoeco.api._async.collection import _distinct_candidates

        self.assertEqual(_distinct_candidates([]), [])
        self.assertEqual(_distinct_candidates([[]]), [])
        self.assertEqual(_distinct_candidates([[1, 2], 1, 2]), [1, 2])
        self.assertEqual(_distinct_candidates([1, 2]), [1, 2])

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
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = []

        collection = AsyncCollection(EngineStub(), "db", "coll")

        result = asyncio.run(collection.update_many({"kind": "missing"}, {"$set": {"done": True}}))

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    def test_estimated_document_count_and_drop_delegate_to_engine(self):
        class EngineStub:
            def __init__(self):
                self.drop_calls = []

            async def drop_collection(self, db_name, coll_name, *, context=None):
                self.drop_calls.append((db_name, coll_name, context))

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")
        session = object()

        class CursorStub:
            async def to_list(self):
                return [{"_id": str(i)} for i in range(7)]

        with patch.object(collection, "find", return_value=CursorStub()) as mock_find:
            count = asyncio.run(collection.estimated_document_count())
        asyncio.run(collection.drop(session=session))

        self.assertEqual(count, 7)
        self.assertEqual(engine.drop_calls, [("db", "coll", session)])
        mock_find.assert_called_once_with(
            {},
            {"_id": 1},
            comment=None,
            max_time_ms=None,
            session=None,
        )

    def test_metadata_and_change_notifications_delegate_to_engine_and_hub(self):
        class EngineStub:
            def __init__(self):
                self.metadata_calls = []

            def _record_operation_metadata(self, session, **kwargs):
                self.metadata_calls.append((session, kwargs))

        class HubStub:
            def __init__(self):
                self.events = []

            def publish(self, **payload):
                self.events.append(payload)

        engine = EngineStub()
        hub = HubStub()
        collection = AsyncCollection(engine, "db", "coll", change_hub=hub)
        session = object()

        collection._record_operation_metadata(operation="find", comment="trace", max_time_ms=5, session=session)
        collection._publish_change_event(
            operation_type="update",
            document_key={"_id": "1"},
            full_document={"_id": "1", "done": True},
        )

        self.assertEqual(
            engine.metadata_calls,
            [(session, {"operation": "find", "comment": "trace", "max_time_ms": 5, "hint": None})],
        )
        self.assertEqual(
            hub.events,
            [
                {
                    "operation_type": "update",
                    "db_name": "db",
                    "coll_name": "coll",
                    "document_key": {"_id": "1"},
                    "full_document": {"_id": "1", "done": True},
                    "update_description": None,
                }
            ],
        )
