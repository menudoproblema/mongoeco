from tests.unit.api._collection_test_support import *  # noqa: F403

class AsyncCollectionValidationTests(AsyncCollectionHelperBase):
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

    def test_seed_upsert_document_extracts_singleton_in_and_top_level_and_equalities(self):
        document = {}

        seed_upsert_document(
            document,
            {
                "$and": [
                    {"kind": {"$in": ["user"]}},
                    {"tenant": "eu"},
                    {"ignored": {"$gt": 1}},
                ]
            },
        )

        self.assertEqual(document, {"kind": "user", "tenant": "eu"})

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

    def test_collection_helper_normalizers_and_subcollection_access(self):
        class _ForeignReturnDocument(Enum):
            BEFORE = "before"
            AFTER = "after"

        child = self.collection["events"]

        self.assertEqual(child.full_name, "db.coll.events")
        self.assertEqual(self.collection.events.full_name, "db.coll.events")
        self.assertEqual(child.planning_mode, self.collection.planning_mode)
        self.assertEqual(AsyncCollection._normalize_filter(None), {})
        self.assertIsNone(AsyncCollection._normalize_projection(None))
        self.assertIsNone(AsyncCollection._normalize_batch_size(None))
        self.assertEqual(AsyncCollection._normalize_batch_size(0), 0)
        normalized_collation = AsyncCollection._normalize_collation({"locale": "en", "strength": 1})
        self.assertEqual(normalized_collation["locale"], "en")
        self.assertEqual(normalized_collation["strength"], 1)
        self.assertEqual(AsyncCollection._normalize_let({"tenant": "a"}), {"tenant": "a"})
        self.assertEqual(AsyncCollection._normalize_search_index_name("idx"), "idx")
        self.assertEqual(AsyncCollection._normalize_return_document(False), ReturnDocument.BEFORE)
        self.assertEqual(AsyncCollection._normalize_return_document(True), ReturnDocument.AFTER)
        self.assertEqual(
            AsyncCollection._normalize_return_document(_ForeignReturnDocument.BEFORE),
            ReturnDocument.BEFORE,
        )
        self.assertIsInstance(
            AsyncCollection._normalize_search_index_model({"mappings": {"dynamic": True}}),
            SearchIndexModel,
        )
        self.assertEqual(AsyncCollection._normalize_expire_after_seconds(0), 0)
        self.assertEqual(
            AsyncCollection._normalize_index_model(IndexModel([("email", 1)], unique=True)).keys,
            [("email", 1)],
        )

        with self.assertRaises(AttributeError):
            getattr(self.collection, "_private")
        with self.assertRaises(TypeError):
            _ = self.collection[""]  # type: ignore[index]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_filter([])  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_projection([])  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_let("bad")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_array_filters("bad")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_search_index_model("bad")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_search_index_name("")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_expire_after_seconds(True)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_index_model(type("Model", (), {"document": "bad"})())
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_index_model(type("Model", (), {"document": {"name": "idx"}})())
        with self.assertRaisesRegex(TypeError, "unsupported IndexModel options"):
            AsyncCollection._normalize_index_model(
                type(
                    "Model",
                    (),
                    {"document": {"key": {"email": 1}, "name": "idx", "unknown": True}},
                )()
            )
        normalized = AsyncCollection._normalize_index_model(
            type(
                "Model",
                (),
                {"document": {"key": {"email": 1}, "name": "idx", "hidden": True, "collation": {"locale": "en"}}},
            )()
        )
        self.assertTrue(normalized.hidden)
        self.assertEqual(normalized.collation, {"locale": "en"})

    def test_collection_require_helpers_reject_invalid_inputs(self):
        self.assertEqual(AsyncCollection._require_documents(({"_id": "1"},)), [{"_id": "1"}])

        with self.assertRaises(TypeError):
            AsyncCollection._require_documents({"_id": "1"})  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            AsyncCollection._require_documents([])
        with self.assertRaises(ValueError):
            AsyncCollection._require_update([])
        with self.assertRaises(TypeError):
            AsyncCollection._require_update([1])  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            AsyncCollection._require_update([{"$set": {}, "$unset": {}}])
        with self.assertRaises(ValueError):
            AsyncCollection._require_update([{"field": 1}])
        with self.assertRaises(TypeError):
            AsyncCollection._require_update(1)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            AsyncCollection._require_update({})  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            AsyncCollection._require_update({"field": 1})  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._require_update({"$set": 1})  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            AsyncCollection._require_replacement({"$set": {"x": 1}})
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_batch_size("bad")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_array_filters([1])  # type: ignore[list-item]
        self.assertIsNone(AsyncCollection._normalize_collation(None))
        self.assertIsNone(AsyncCollection._normalize_array_filters(None))
        with self.assertRaises(TypeError):
            AsyncCollection._normalize_search_index_models("bad")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            AsyncCollection._normalize_search_index_models([])
