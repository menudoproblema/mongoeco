from tests.unit.api._collection_test_support import *  # noqa: F403

class AsyncCollectionManagementTests(AsyncCollectionHelperBase):
    def test_find_one_returns_projection_already_applied_by_engine(self):
        class EngineStub(_SemanticsScanMixin):
            _stub_documents = [{"_id": "1", "name": "Ada"}]

            async def get_document(self, *args, **kwargs):
                return {"name": "Ada"}

        collection = AsyncCollection(EngineStub(), "db", "coll")

        direct = asyncio.run(collection.find_one({"_id": "1"}, {"name": 1, "_id": 0}))
        scanned = asyncio.run(collection.find_one({"name": "Ada"}, {"role": 0, "_id": 1}))

        self.assertEqual(direct, {"name": "Ada"})
        self.assertEqual(scanned, {"_id": "1", "name": "Ada"})

    def test_collection_compiles_plan_once_and_passes_it_to_engine(self):
        class EngineStub:
            def __init__(self):
                self.scan_semantics = None
                self.scan_semantics_history = []
                self.update_plan = None
                self.delete_plan = None
                self.count_plan = None
                self.update_dialect = None
                self.delete_dialect = None
                self.count_dialect = None

            async def put_document(self, *args, **kwargs):
                return True

            async def get_document(self, *args, **kwargs):
                return None

            def scan_find_semantics(self, db_name, coll_name, semantics, *, context=None):
                self.scan_semantics = semantics
                self.scan_semantics_history.append(semantics)
                return _scan_stub_documents(
                    [],
                    skip=semantics.skip,
                    limit=semantics.limit,
                )

            async def update_with_operation(self, *args, **kwargs):
                operation = args[2]
                self.update_plan = operation.plan
                self.update_dialect = kwargs["dialect"]
                from mongoeco.types import UpdateResult

                return UpdateResult(matched_count=0, modified_count=0)

            async def delete_with_operation(self, *args, **kwargs):
                operation = args[2]
                self.delete_plan = operation.plan
                self.delete_dialect = kwargs["dialect"]
                from mongoeco.types import DeleteResult

                return DeleteResult(deleted_count=0)

            async def count_find_semantics(self, *args, **kwargs):
                semantics = args[2]
                self.count_plan = semantics.query_plan
                self.count_dialect = semantics.dialect
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
                return [{"name": "_id_", "key": {"_id": 1}, "unique": True}]

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

        self.assertEqual(type(engine.scan_semantics_history[0].query_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.update_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.delete_plan).__name__, "EqualsCondition")
        self.assertEqual(type(engine.count_plan).__name__, "EqualsCondition")
        self.assertNotIsInstance(engine.scan_semantics_history[0].query_plan, MatchAll)
        self.assertNotIsInstance(engine.count_plan, MatchAll)
        self.assertIs(engine.scan_semantics_history[0].dialect, collection.mongodb_dialect)
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
                return [{"name": "_id_", "key": {"_id": 1}, "unique": True}]

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
            cursor = collection.list_indexes()
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
            {
                "unique": True,
                "name": "idx_email",
                "sparse": False,
                "hidden": False,
                "partial_filter_expression": None,
                "expire_after_seconds": None,
                "max_time_ms": None,
                "context": None,
            },
        )
        self.assertEqual(indexes, [{"name": "_id_", "key": {"_id": 1}, "unique": True}])
        self.assertEqual(info, {"_id_": {"key": [("_id", 1)], "unique": True}})
        self.assertEqual(engine.drop_index_args, ("db", "coll", [("email", 1), ("created_at", -1)]))
        self.assertEqual(engine.drop_index_kwargs, {"context": None})
        self.assertEqual(engine.drop_indexes_args, ("db", "coll"))
        self.assertEqual(engine.drop_indexes_kwargs, {"context": None})

    def test_list_indexes_returns_cursor(self):
        class EngineStub:
            async def list_indexes(self, *args, **kwargs):
                return [
                    {"name": "_id_", "key": {"_id": 1}, "unique": True},
                    {"name": "email_1", "key": {"email": 1}, "unique": False},
                ]

        collection = AsyncCollection(EngineStub(), "db", "coll")

        async def _exercise():
            cursor = collection.list_indexes()
            return await cursor.first(), await cursor.to_list()

        first, indexes = asyncio.run(_exercise())

        self.assertEqual(first, {"name": "_id_", "key": {"_id": 1}, "unique": True})
        self.assertEqual(
            indexes,
            [
                {"name": "_id_", "key": {"_id": 1}, "unique": True},
                {"name": "email_1", "key": {"email": 1}, "unique": False},
            ],
        )

    def test_create_indexes_requires_index_models(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll")

        with self.assertRaises(TypeError):
            asyncio.run(collection.create_indexes([{"name": "idx"}]))

        with self.assertRaises(TypeError):
            asyncio.run(collection.create_indexes((IndexModel([("email", 1)]),)))  # type: ignore[arg-type]

        with self.assertRaises(ValueError):
            asyncio.run(collection.create_indexes([]))

    def test_create_indexes_accepts_pymongo_style_index_models(self):
        class FakePyMongoIndexModel:
            def __init__(self, document):
                self.document = document

        class EngineStub:
            def __init__(self):
                self.calls = []

            async def create_index(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                return kwargs["name"] or "email_1"

            async def index_information(self, *args, **kwargs):
                return {"_id_": {"key": [("_id", 1)], "unique": True}}

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        names = asyncio.run(
            collection.create_indexes(
                [
                    FakePyMongoIndexModel(
                        {
                            "key": [("email", 1)],
                            "name": "email_idx",
                            "unique": True,
                            "partialFilterExpression": {"active": True},
                        }
                    )
                ]
            )
        )

        self.assertEqual(names, ["email_idx"])
        self.assertEqual(
            engine.calls,
            [
                (
                    ("db", "coll", [("email", 1)]),
                    {
                        "unique": True,
                        "name": "email_idx",
                        "sparse": False,
                        "hidden": False,
                        "partial_filter_expression": {"active": True},
                        "expire_after_seconds": None,
                        "max_time_ms": None,
                        "context": None,
                    },
                )
            ],
        )

    def test_insert_many_accepts_iterable_but_rejects_mappings_and_empty_iterables(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll")

        async def _insert_generator():
            return await collection.insert_many(({"name": name} for name in ("Ada", "Grace")))

        result = asyncio.run(_insert_generator())

        self.assertEqual(len(result.inserted_ids), 2)

        with self.assertRaises(TypeError):
            asyncio.run(collection.insert_many({"name": "Ada"}))  # type: ignore[arg-type]

        async def _empty_insert():
            return await collection.insert_many(iter(()))

        with self.assertRaises(ValueError):
            asyncio.run(_empty_insert())

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
                    {
                        "unique": True,
                        "name": None,
                        "sparse": False,
                        "hidden": False,
                        "partial_filter_expression": None,
                        "expire_after_seconds": None,
                        "max_time_ms": None,
                        "context": None,
                    },
                ),
                (
                    ("db", "coll", [("tenant", 1), ("created_at", -1)]),
                    {
                        "unique": False,
                        "name": "tenant_created",
                        "sparse": False,
                        "hidden": False,
                        "partial_filter_expression": None,
                        "expire_after_seconds": None,
                        "max_time_ms": None,
                        "context": None,
                    },
                ),
            ],
        )

    def test_create_index_forwards_sparse_and_partial_filter_expression(self):
        class EngineStub:
            async def create_index(self, *args, **kwargs):
                self.kwargs = kwargs
                return "idx"

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        asyncio.run(
            collection.create_index(
                [("email", 1)],
                sparse=True,
                partial_filter_expression={"active": True},
            )
        )

        self.assertEqual(
            engine.kwargs,
            {
                "unique": False,
                "name": None,
                "sparse": True,
                "hidden": False,
                "partial_filter_expression": {"active": True},
                "expire_after_seconds": None,
                "max_time_ms": None,
                "context": None,
            },
        )

    def test_create_index_forwards_expire_after_seconds(self):
        class EngineStub:
            async def create_index(self, *args, **kwargs):
                self.kwargs = kwargs
                return "expires_at_1"

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        asyncio.run(
            collection.create_index(
                [("expires_at", 1)],
                expire_after_seconds=30,
            )
        )

        self.assertEqual(
            engine.kwargs,
            {
                "unique": False,
                "name": None,
                "sparse": False,
                "hidden": False,
                "partial_filter_expression": None,
                "expire_after_seconds": 30,
                "max_time_ms": None,
                "context": None,
            },
        )

    def test_create_index_forwards_hidden(self):
        class EngineStub:
            async def create_index(self, *args, **kwargs):
                self.kwargs = kwargs
                return "email_1"

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        asyncio.run(collection.create_index([("email", 1)], hidden=True))

        self.assertTrue(engine.kwargs["hidden"])

    def test_create_index_rejects_invalid_expire_after_seconds_type(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll")

        with self.assertRaises(TypeError):
            asyncio.run(collection.create_index([("expires_at", 1)], expire_after_seconds=-1))

        with self.assertRaises(TypeError):
            asyncio.run(collection.create_index([("expires_at", 1)], expire_after_seconds=True))

    def test_create_index_forwards_max_time_ms(self):
        class EngineStub:
            async def create_index(self, *args, **kwargs):
                self.kwargs = kwargs
                return "idx"

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        asyncio.run(collection.create_index([("email", 1)], max_time_ms=25))

        self.assertEqual(engine.kwargs["max_time_ms"], 25)

    def test_create_indexes_shares_batch_deadline_across_calls(self):
        class EngineStub:
            def __init__(self):
                self.calls = []

            async def create_index(self, *args, **kwargs):
                self.calls.append(kwargs["max_time_ms"])
                return kwargs["name"] or "idx"

            async def index_information(self, *args, **kwargs):
                return {"_id_": {"key": [("_id", 1)], "unique": True}}

            async def drop_index(self, *args, **kwargs):
                return None

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")

        asyncio.run(
            collection.create_indexes(
                [
                    IndexModel([("email", 1)]),
                    IndexModel([("tenant", 1)], name="tenant_idx"),
                ],
                max_time_ms=50,
            )
        )

        self.assertEqual(len(engine.calls), 2)
        self.assertTrue(all(isinstance(value, int) and value > 0 for value in engine.calls))

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

    def test_engine_update_and_delete_with_operation_profile_errors(self):
        class EngineStub:
            async def update_with_operation(self, *args, **kwargs):
                raise RuntimeError("update boom")

            async def delete_with_operation(self, *args, **kwargs):
                raise RuntimeError("delete boom")

        async def _exercise():
            collection = AsyncCollection(EngineStub(), "db", "coll")
            update_operation = compile_update_operation(
                {"_id": "1"},
                update_spec={"$set": {"done": True}},
                dialect=collection.mongodb_dialect,
            )
            delete_operation = compile_update_operation(
                {"_id": "1"},
                dialect=collection.mongodb_dialect,
            )
            profiled = []

            async def _profile_operation(**payload):
                profiled.append(payload)

            collection._profile_operation = _profile_operation  # type: ignore[method-assign]
            with self.assertRaisesRegex(RuntimeError, "update boom"):
                await collection._engine_update_with_operation(update_operation)
            with self.assertRaisesRegex(RuntimeError, "delete boom"):
                await collection._engine_delete_with_operation(delete_operation)
            return profiled

        profiled = asyncio.run(_exercise())

        self.assertEqual([entry["op"] for entry in profiled], ["update", "remove"])
        self.assertEqual(profiled[0]["errmsg"], "update boom")
        self.assertEqual(profiled[1]["errmsg"], "delete boom")

    def test_raw_batch_helpers_wrap_find_and_aggregate_results(self):
        async def _exercise():
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll")
                await collection.insert_many(
                    [
                        {"_id": "1", "kind": "view"},
                        {"_id": "2", "kind": "view"},
                    ]
                )
                find_batches = await collection.find_raw_batches({}, batch_size=1).to_list()
                aggregate_batches = await collection.aggregate_raw_batches(
                    [{"$match": {"kind": "view"}}],
                    batch_size=1,
                ).to_list()
                return len(find_batches), len(aggregate_batches)
            finally:
                await engine.disconnect()

        find_count, aggregate_count = asyncio.run(_exercise())

        self.assertEqual(find_count, 2)
        self.assertEqual(aggregate_count, 2)

    def test_search_index_helpers_rename_watch_and_options_delegate_correctly(self):
        class EngineStub:
            def __init__(self):
                self.rename_calls = []
                self.search_calls = []

            async def create_search_index(self, *args, **kwargs):
                self.search_calls.append(("create", args, kwargs))
                return "search_idx"

            async def list_search_indexes(self, *args, **kwargs):
                self.search_calls.append(("list", args, kwargs))
                return [{"name": "search_idx"}]

            async def update_search_index(self, *args, **kwargs):
                self.search_calls.append(("update", args, kwargs))

            async def drop_search_index(self, *args, **kwargs):
                self.search_calls.append(("drop", args, kwargs))

            async def rename_collection(self, *args, **kwargs):
                self.rename_calls.append((args, kwargs))

            async def collection_options(self, *args, **kwargs):
                return {"validator": {"$jsonSchema": {"bsonType": "object"}}}

        async def _exercise():
            engine = EngineStub()
            collection = AsyncCollection(engine, "db", "coll")
            created = await collection.create_search_index({"mappings": {"dynamic": True}}, max_time_ms=25)
            created_many = await collection.create_search_indexes(
                [{"mappings": {"dynamic": True}}],
                max_time_ms=25,
            )
            listed = await collection.list_search_indexes("search_idx").to_list()
            await collection.update_search_index("search_idx", {"mappings": {"dynamic": False}}, max_time_ms=10)
            await collection.drop_search_index("search_idx", max_time_ms=10)
            renamed = await collection.rename("renamed")
            options = await collection.options()
            stream = collection.watch(max_await_time_ms=5)
            return engine, created, created_many, listed, renamed, options, stream

        engine, created, created_many, listed, renamed, options, stream = asyncio.run(_exercise())

        self.assertEqual(created, "search_idx")
        self.assertEqual(created_many, ["search_idx"])
        self.assertEqual(listed, [{"name": "search_idx"}])
        self.assertEqual(renamed.full_name, "db.renamed")
        self.assertEqual(options, {"validator": {"$jsonSchema": {"bsonType": "object"}}})
        self.assertEqual(type(stream).__name__, "AsyncChangeStreamCursor")
        self.assertEqual(engine.rename_calls[0][0], ("db", "coll", "renamed"))
        self.assertEqual(engine.search_calls[0][0], "create")
        self.assertEqual(engine.search_calls[1][0], "create")
        self.assertEqual(engine.search_calls[2][0], "list")
        self.assertEqual(engine.search_calls[3][0], "update")
        self.assertEqual(engine.search_calls[4][0], "drop")

        with self.assertRaises(TypeError):
            self.collection.watch(max_await_time_ms=-1)

    def test_collection_properties_and_with_options_expose_normalized_configuration(self):
        collection = self.collection.with_options(planning_mode=Enum("Mode", {"RELAXED": "relaxed"}).RELAXED)

        self.assertEqual(self.collection.name, "coll")
        self.assertEqual(self.collection.full_name, "db.coll")
        self.assertIsNotNone(self.collection.mongodb_dialect)
        self.assertIsNotNone(self.collection.mongodb_dialect_resolution)
        self.assertIsNotNone(self.collection.pymongo_profile)
        self.assertIsNotNone(self.collection.pymongo_profile_resolution)
        self.assertIsNotNone(self.collection.write_concern)
        self.assertIsNotNone(self.collection.read_concern)
        self.assertIsNotNone(self.collection.read_preference)
        self.assertIsNotNone(self.collection.codec_options)
        self.assertEqual(collection.full_name, "db.coll")

    def test_rename_preserves_collection_configuration(self):
        class EngineStub(MemoryEngine):
            async def rename_collection(self, *args, **kwargs):
                return None

        async def _exercise():
            engine = EngineStub()
            collection = AsyncCollection(
                engine,
                "db",
                "coll",
                write_concern=WriteConcern("majority"),
                read_concern=ReadConcern("majority"),
                read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
                codec_options=CodecOptions(dict, tz_aware=True),
                planning_mode=PlanningMode.RELAXED,
                change_stream_history_size=7,
                change_stream_journal_path="/tmp/mongoeco-journal.json",
                change_stream_journal_fsync=True,
                change_stream_journal_max_bytes=2048,
            )
            renamed = await collection.rename("renamed")
            return collection, renamed

        collection, renamed = asyncio.run(_exercise())

        self.assertEqual(renamed.full_name, "db.renamed")
        self.assertEqual(renamed.write_concern, collection.write_concern)
        self.assertEqual(renamed.read_concern, collection.read_concern)
        self.assertEqual(renamed.read_preference, collection.read_preference)
        self.assertEqual(renamed.codec_options, collection.codec_options)
        self.assertEqual(renamed.planning_mode, collection.planning_mode)
        self.assertEqual(renamed.change_stream_history_size, collection.change_stream_history_size)
        self.assertEqual(renamed.change_stream_journal_path, collection.change_stream_journal_path)
        self.assertEqual(renamed.change_stream_journal_fsync, collection.change_stream_journal_fsync)
        self.assertEqual(renamed.change_stream_journal_max_bytes, collection.change_stream_journal_max_bytes)

    def test_database_preserves_change_stream_journal_configuration(self):
        collection = AsyncCollection(
            MemoryEngine(),
            "db",
            "coll",
            change_stream_history_size=7,
            change_stream_journal_path="/tmp/mongoeco-journal.json",
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=2048,
        )

        database = collection.database

        self.assertEqual(database.change_stream_history_size, 7)
        self.assertEqual(database.change_stream_journal_path, "/tmp/mongoeco-journal.json")
        self.assertTrue(database.change_stream_journal_fsync)
        self.assertEqual(database.change_stream_journal_max_bytes, 2048)

    def test_bulk_write_context_prepare_request_covers_variants_and_errors(self):
        collection = AsyncCollection(MemoryEngine(), "db", "coll", pymongo_profile="4.11")
        context = async_collection_module._BulkWriteContext(collection, [])

        prepared_insert = context._prepare_request(0, InsertOne({"name": "Ada"}))
        prepared_replace = context._prepare_request(
            1,
            ReplaceOne({"_id": "1"}, {"name": "Ada"}, sort=[("rank", 1)], let={"tenant": "a"}),
        )
        prepared_update_error = context._prepare_request(
            2,
            UpdateOne({"_id": "1"}, {"$set": {"done": True}}, array_filters="bad"),  # type: ignore[arg-type]
        )
        prepared_delete = context._prepare_request(
            3,
            DeleteOne({"_id": "1"}, hint="idx", let={"tenant": "a"}),
        )
        prepared_unknown = context._prepare_request(4, object())  # type: ignore[arg-type]

        self.assertIn("_id", prepared_insert.insert_document)
        self.assertEqual(prepared_replace.replacement_document, {"name": "Ada"})
        self.assertIsInstance(prepared_update_error.preparation_error, TypeError)
        self.assertIsNone(prepared_delete.preparation_error)
        self.assertIsInstance(prepared_unknown.preparation_error, TypeError)

    def test_profile_operation_handles_system_profile_and_planner_failures(self):
        class EngineStub:
            def __init__(self):
                self.events = []

            def _record_profile_event(self, *args, **kwargs):
                self.events.append((args, kwargs))

            async def plan_find_execution(self, *args, **kwargs):
                raise RuntimeError("planner boom")

        async def _exercise():
            engine = EngineStub()
            regular = AsyncCollection(engine, "db", "coll")
            system_profile = AsyncCollection(engine, "db", "system.profile")
            operation = regular.find({"kind": "view"})._as_operation()
            await regular._profile_operation(
                op="query",
                command={"find": "coll"},
                duration_ns=1000,
                operation=operation,
            )
            await system_profile._profile_operation(
                op="query",
                command={"find": "system.profile"},
                duration_ns=1000,
            )
            return engine.events

        events = asyncio.run(_exercise())

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0][1]["fallback_reason"], None)
        self.assertEqual(events[0][1]["execution_lineage"], ())

    def test_engine_update_requires_update_with_operation_and_rejects_deferred_issues(self):
        async def _exercise_missing_method():
            collection = AsyncCollection(object(), "db", "coll")
            operation = compile_update_operation(
                {"_id": "1"},
                update_spec={"$set": {"done": True}},
                dialect=collection.mongodb_dialect,
            )
            await collection._engine_update_with_operation(operation)

        async def _exercise_planning_issues():
            collection = AsyncCollection(MemoryEngine(), "db", "coll")
            operation = collection.find({"_id": "1"})._as_operation().with_overrides(
                planning_issues=(type("Issue", (), {"message": "blocked"})(),),
            )
            collection._ensure_operation_executable(operation)

        with self.assertRaisesRegex(TypeError, "update_with_operation"):
            asyncio.run(_exercise_missing_method())
        with self.assertRaisesRegex(OperationFailure, "blocked"):
            asyncio.run(_exercise_planning_issues())

    def test_create_indexes_rolls_back_created_indexes_when_later_creation_fails(self):
        class EngineStub:
            def __init__(self):
                self.created = []
                self.dropped = []

            async def index_information(self, *args, **kwargs):
                return {}

            async def create_index(self, _db, _coll, keys, **kwargs):
                if not self.created:
                    self.created.append("idx_a")
                    return "idx_a"
                raise RuntimeError("boom")

            async def drop_index(self, _db, _coll, name, **kwargs):
                self.dropped.append(name)

        holder = {}

        async def _exercise():
            engine = EngineStub()
            holder["engine"] = engine
            collection = AsyncCollection(engine, "db", "coll")
            await collection.create_indexes(
                [IndexModel([("a", 1)], name="idx_a"), IndexModel([("b", 1)], name="idx_b")]
            )

        with self.assertRaisesRegex(RuntimeError, "boom"):
            asyncio.run(_exercise())
        self.assertEqual(holder["engine"].dropped, ["idx_a"])

    def test_update_one_hint_and_change_hub_paths_publish_expected_events(self):
        class HubStub:
            def __init__(self):
                self.events = []

            def publish(self, **payload):
                self.events.append(payload)

        class CursorStub:
            def __init__(self, document):
                self._document = document

            async def first(self):
                return self._document

        async def _exercise():
            hub = HubStub()
            collection = AsyncCollection(MemoryEngine(), "db", "coll", change_hub=hub)

            selected = {"_id": "1"}
            updated = {"_id": "1", "done": True}

            collection._build_cursor = lambda *args, **kwargs: CursorStub(selected)  # type: ignore[method-assign]
            collection._engine_update_with_operation = lambda *args, **kwargs: asyncio.sleep(0, result=UpdateResult(matched_count=1, modified_count=1))  # type: ignore[method-assign]
            collection._document_by_id = lambda *args, **kwargs: asyncio.sleep(0, result=updated)  # type: ignore[method-assign]

            hinted = await collection.update_one({"_id": "1"}, {"$set": {"done": True}}, hint="idx")
            plain = await collection.update_one({"_id": "1"}, {"$set": {"done": True}})
            return hinted, plain, hub.events

        hinted, plain, events = asyncio.run(_exercise())

        self.assertEqual(hinted.modified_count, 1)
        self.assertEqual(plain.modified_count, 1)
        self.assertEqual([event["operation_type"] for event in events], ["update", "update"])

    def test_update_one_hint_upserts_when_no_document_matches(self):
        class CursorStub:
            async def first(self):
                return None

        async def _exercise():
            collection = AsyncCollection(MemoryEngine(), "db", "coll")
            collection._build_cursor = lambda *args, **kwargs: CursorStub()  # type: ignore[method-assign]
            collection._perform_upsert_update = lambda *args, **kwargs: asyncio.sleep(0, result=UpdateResult(matched_count=0, modified_count=0, upserted_id="1"))  # type: ignore[method-assign]
            return await collection.update_one(
                {"_id": "1"},
                {"$set": {"done": True}},
                hint="idx",
                upsert=True,
            )

        result = asyncio.run(_exercise())

        self.assertEqual(result.upserted_id, "1")

    def test_find_one_and_update_handles_upsert_after_lookup_and_missing_after_document(self):
        class CursorStub:
            def __init__(self, document):
                self._document = document

            async def first(self):
                return self._document

        async def _exercise_upsert():
            collection = AsyncCollection(MemoryEngine(), "db", "coll")
            collection._select_first_document = lambda *args, **kwargs: asyncio.sleep(0, result=None)  # type: ignore[method-assign]
            collection.update_one = lambda *args, **kwargs: asyncio.sleep(0, result=UpdateResult(matched_count=0, modified_count=0, upserted_id="1"))  # type: ignore[method-assign]
            collection.find = lambda *args, **kwargs: CursorStub({"_id": "1", "done": True})  # type: ignore[method-assign]
            return await collection.find_one_and_update(
                {"_id": "1"},
                {"$set": {"done": True}},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )

        async def _exercise_missing_after():
            collection = AsyncCollection(MemoryEngine(), "db", "coll")
            collection._select_first_document = lambda *args, **kwargs: asyncio.sleep(0, result={"_id": "1", "done": False})  # type: ignore[method-assign]
            collection._engine_update_with_operation = lambda *args, **kwargs: asyncio.sleep(0, result=UpdateResult(matched_count=1, modified_count=1))  # type: ignore[method-assign]
            collection._document_by_id = lambda *args, **kwargs: asyncio.sleep(0, result=None)  # type: ignore[method-assign]
            return await collection.find_one_and_update(
                {"_id": "1"},
                {"$set": {"done": True}},
                return_document=ReturnDocument.AFTER,
            )

        self.assertEqual(asyncio.run(_exercise_upsert()), {"_id": "1", "done": True})
        self.assertIsNone(asyncio.run(_exercise_missing_after()))

    def test_find_one_and_replace_returns_none_when_after_document_disappears(self):
        async def _exercise():
            collection = AsyncCollection(MemoryEngine(), "db", "coll")
            collection._select_first_document = lambda *args, **kwargs: asyncio.sleep(0, result={"_id": "1", "done": False})  # type: ignore[method-assign]
            collection.replace_one = lambda *args, **kwargs: asyncio.sleep(0, result=UpdateResult(matched_count=1, modified_count=1))  # type: ignore[method-assign]
            collection._document_by_id = lambda *args, **kwargs: asyncio.sleep(0, result=None)  # type: ignore[method-assign]
            return await collection.find_one_and_replace(
                {"_id": "1"},
                {"done": True},
                return_document=ReturnDocument.AFTER,
            )

        self.assertIsNone(asyncio.run(_exercise()))

    def test_delete_one_change_hub_and_hint_paths_cover_selection_branches(self):
        class HubStub:
            def __init__(self):
                self.events = []

            def publish(self, **payload):
                self.events.append(payload)

        class CursorStub:
            def __init__(self, document):
                self._document = document

            async def first(self):
                return self._document

        async def _exercise():
            hub = HubStub()
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(engine, "db", "coll", change_hub=hub)
                collection._build_cursor = lambda *args, **kwargs: CursorStub({"_id": "1"})  # type: ignore[method-assign]
                collection._engine_delete_with_operation = lambda *args, **kwargs: asyncio.sleep(0, result=DeleteResult(deleted_count=1))  # type: ignore[method-assign]

                plain = await collection.delete_one({"_id": "1"})
                hinted_none = await collection.delete_one({"_id": "missing"}, hint="idx")
                return plain, hinted_none, hub.events
            finally:
                await engine.disconnect()

        plain, hinted_none, events = asyncio.run(_exercise())

        self.assertEqual(plain.deleted_count, 1)
        self.assertIn(hinted_none.deleted_count, (0, 1))
        self.assertEqual(events[0]["operation_type"], "delete")

    def test_delete_one_hint_returns_zero_when_selection_is_empty(self):
        class CursorStub:
            async def first(self):
                return None

        async def _exercise():
            collection = AsyncCollection(MemoryEngine(), "db", "coll")
            collection._build_cursor = lambda *args, **kwargs: CursorStub()  # type: ignore[method-assign]
            return await collection.delete_one({"_id": "missing"}, hint="idx")

        result = asyncio.run(_exercise())

        self.assertEqual(result.deleted_count, 0)

    def test_drop_index_accepts_string_names_and_rename_rejects_empty_name(self):
        class EngineStub:
            def __init__(self):
                self.drop_calls = []

            async def drop_index(self, *args, **kwargs):
                self.drop_calls.append((args, kwargs))

        async def _exercise():
            engine = EngineStub()
            collection = AsyncCollection(engine, "db", "coll")
            await collection.drop_index("name_1")
            return engine.drop_calls

        calls = asyncio.run(_exercise())

        self.assertEqual(calls[0][0], ("db", "coll", "name_1"))
        with self.assertRaises(TypeError):
            asyncio.run(self.collection.rename(""))
