import asyncio
import inspect
import unittest
from unittest.mock import patch

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
from mongoeco.api._async.database_commands import (
    AsyncDatabaseCommandService,
    BuildInfoResult,
)
from mongoeco.api._async.client import AsyncDatabase
from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.api.admin_parsing import (
    normalize_list_collections_options,
    normalize_validate_command_options,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    UpdateOperation,
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.api._sync.collection import Collection
from mongoeco.api._sync.database_admin import DatabaseAdminService
from mongoeco.api._sync.database_commands import DatabaseCommandService
from mongoeco.api._sync._materialized_cursor import MaterializedCursor
from mongoeco.api._sync.index_cursor import IndexCursor
from mongoeco.api._sync.listing_cursor import ListingCursor
from mongoeco.compat import (
    OPERATION_OPTION_SUPPORT,
    OptionSupportStatus,
    export_full_compat_catalog,
    get_operation_option_support,
    is_operation_option_effective,
)
from mongoeco.compat.operation_support import (
    MANAGED_OPERATION_OPTION_NAMES,
    OPERATION_OPTION_SIGNATURE_EXCLUSIONS,
)
from mongoeco.core.aggregation import AGGREGATION_STAGE_HANDLERS
from mongoeco.engines.base import (
    AsyncAdminEngine,
    AsyncDatabaseAdminEngine,
    AsyncCrudEngine,
    AsyncExplainEngine,
    AsyncIndexAdminEngine,
    AsyncLifecycleEngine,
    AsyncNamespaceAdminEngine,
    AsyncReadPlanningEngine,
    AsyncSessionEngine,
    AsyncStorageEngine,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.semantic_core import (
    EngineFindSemantics,
    EngineReadExecutionPlan,
    build_query_plan_explanation,
    compile_find_semantics,
)
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import EngineIndexRecord, IndexDefinition, IndexInformation, default_id_index_definition
from mongoeco.types import (
    BulkWriteErrorDetails,
    CodecOptions,
    ExecutionLineageStep,
    QueryPlanExplanation,
    ReadConcern,
    ReadPreference,
    ReadPreferenceMode,
    TransactionOptions,
    WriteConcern,
    BuildInfoDocument,
    CollectionStatsSnapshot,
    CollectionValidationSnapshot,
    CollectionListingSnapshot,
    CollectionValidationDocument,
    CommandCursorResult,
    CountCommandResult,
    CreateIndexesCommandResult,
    DatabaseListingSnapshot,
    DatabaseStatsSnapshot,
    DistinctCommandResult,
    DropDatabaseCommandResult,
    DropIndexesCommandResult,
    FindAndModifyCommandResult,
    FindAndModifyLastErrorObject,
    ListDatabasesCommandResult,
    NamespaceOkResult,
    OkResult,
    UpsertedWriteEntry,
    WriteErrorEntry,
    WriteCommandResult,
)
from mongoeco.core.operators import UpdateEngine


class ArchitectureUnitTests(unittest.TestCase):
    def test_engines_satisfy_split_storage_protocols(self):
        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                self.assertIsInstance(engine, AsyncSessionEngine)
                self.assertIsInstance(engine, AsyncLifecycleEngine)
                self.assertIsInstance(engine, AsyncCrudEngine)
                self.assertIsInstance(engine, AsyncIndexAdminEngine)
                self.assertIsInstance(engine, AsyncReadPlanningEngine)
                self.assertIsInstance(engine, AsyncExplainEngine)
                self.assertIsInstance(engine, AsyncDatabaseAdminEngine)
                self.assertIsInstance(engine, AsyncNamespaceAdminEngine)
                self.assertIsInstance(engine, AsyncAdminEngine)
                self.assertIsInstance(engine, AsyncStorageEngine)

    def test_engines_produce_typed_query_plan_explanations(self):
        async def _run() -> None:
            memory = MemoryEngine()
            sqlite = SQLiteEngine()
            await memory.connect()
            await sqlite.connect()
            try:
                memory_explain = await memory.explain_query_plan("db", "users", {})
                sqlite_explain = await sqlite.explain_query_plan("db", "users", {})
                self.assertIsInstance(memory_explain, QueryPlanExplanation)
                self.assertIsInstance(sqlite_explain, QueryPlanExplanation)
                self.assertEqual(memory_explain.to_document()["engine"], "memory")
                self.assertEqual(sqlite_explain.to_document()["engine"], "sqlite")
            finally:
                await memory.disconnect()
                await sqlite.disconnect()

        asyncio.run(_run())

    def test_compile_find_semantics_returns_typed_read_semantics(self):
        semantics = compile_find_semantics(
            {"name": "Ada"},
            sort=[("rank", 1)],
            skip=1,
            limit=2,
            hint=[("name", 1)],
            comment="read",
            max_time_ms=25,
        )

        self.assertIsInstance(semantics, EngineFindSemantics)
        self.assertEqual(semantics.filter_spec, {"name": "Ada"})
        self.assertEqual(semantics.sort, [("rank", 1)])
        self.assertEqual(semantics.skip, 1)
        self.assertEqual(semantics.limit, 2)
        self.assertEqual(semantics.hint, [("name", 1)])
        self.assertEqual(semantics.comment, "read")
        self.assertEqual(semantics.max_time_ms, 25)

    def test_query_plan_explanation_builder_reuses_compiled_semantics(self):
        semantics = compile_find_semantics(
            {"name": "Ada"},
            sort=[("rank", 1)],
            skip=1,
            limit=2,
            hint=[("name", 1)],
            comment="read",
            max_time_ms=25,
        )

        explanation = build_query_plan_explanation(
            engine="memory",
            strategy="python",
            semantics=semantics,
            hinted_index="name_1",
        )

        self.assertEqual(explanation.engine, "memory")
        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.sort, semantics.sort)
        self.assertEqual(explanation.skip, semantics.skip)
        self.assertEqual(explanation.limit, semantics.limit)
        self.assertEqual(explanation.hint, semantics.hint)
        self.assertEqual(explanation.hinted_index, "name_1")
        self.assertEqual(explanation.comment, semantics.comment)
        self.assertEqual(explanation.max_time_ms, semantics.max_time_ms)

    def test_query_plan_explanation_serializes_execution_lineage(self):
        semantics = compile_find_semantics({"name": "Ada"})
        explanation = build_query_plan_explanation(
            engine="sqlite",
            strategy="sql",
            semantics=semantics,
            execution_lineage=(
                ExecutionLineageStep(runtime="sql", phase="scan", detail="engine pushdown"),
                ExecutionLineageStep(runtime="python", phase="project", detail="semantic core projection"),
            ),
            fallback_reason=None,
        )

        self.assertEqual(
            explanation.to_document()["execution_lineage"],
            [
                {"runtime": "sql", "phase": "scan", "detail": "engine pushdown"},
                {"runtime": "python", "phase": "project", "detail": "semantic core projection"},
            ],
        )

    def test_dialect_policy_is_explicit_behavior_boundary(self):
        from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80

        self.assertTrue(MONGODB_DIALECT_70.policy.null_query_matches_undefined())
        self.assertFalse(MONGODB_DIALECT_80.policy.null_query_matches_undefined())
        self.assertEqual(MONGODB_DIALECT_70.compare_values(1, 2), MONGODB_DIALECT_70.policy.compare_values(1, 2))

    def test_memory_engine_read_paths_compile_shared_semantics(self):
        async def _run() -> None:
            engine = MemoryEngine()
            await engine.connect()
            try:
                with patch("mongoeco.engines.memory.compile_find_semantics", wraps=compile_find_semantics) as compiler:
                    await engine.explain_query_plan("db", "users", {})
                    self.assertGreaterEqual(compiler.call_count, 1)
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_engines_plan_read_execution_before_explain(self):
        async def _run() -> None:
            operation = compile_find_operation({"name": "Ada"})
            memory = MemoryEngine()
            sqlite = SQLiteEngine()
            await memory.connect()
            await sqlite.connect()
            try:
                memory_plan = await memory.plan_find_execution("db", "users", operation)
                sqlite_plan = await sqlite.plan_find_execution("db", "users", operation)
                self.assertIsInstance(memory_plan, EngineReadExecutionPlan)
                self.assertIsInstance(sqlite_plan, EngineReadExecutionPlan)
                self.assertEqual(memory_plan.strategy, "python")
                self.assertEqual(sqlite_plan.strategy, "sql")
                self.assertTrue(sqlite_plan.execution_lineage)
            finally:
                await memory.disconnect()
                await sqlite.disconnect()

        asyncio.run(_run())

    def test_sqlite_read_planner_exposes_fallback_reason_in_negotiation(self):
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                operation = compile_find_operation({"payload": {"$gt": b"x"}})
                plan = await engine.plan_find_execution("db", "users", operation)
                self.assertEqual(plan.strategy, "python")
                self.assertIsNotNone(plan.fallback_reason)
                self.assertTrue(plan.execution_lineage)
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_sqlite_engine_read_paths_compile_shared_semantics(self):
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                with patch("mongoeco.engines.sqlite.compile_find_semantics", wraps=compile_find_semantics) as compiler:
                    await engine.explain_query_plan("db", "users", {})
                    self.assertGreaterEqual(compiler.call_count, 1)
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_namespace_admin_protocol_exposes_collection_options(self):
        self.assertIn("collection_options", AsyncNamespaceAdminEngine.__dict__)

    def test_async_database_delegates_admin_surface_to_dedicated_service(self):
        database = AsyncDatabase(MemoryEngine(), "db")

        self.assertIsInstance(database._admin, AsyncDatabaseAdminService)
        self.assertIsInstance(database._admin._commands, AsyncDatabaseCommandService)

    def test_find_operation_compiles_normalized_read_plan(self):
        operation = compile_find_operation(
            {"name": "Ada"},
            projection={"name": 1},
            sort=[("rank", 1)],
            skip=2,
            limit=5,
            hint=[("name", 1)],
            comment="read",
            max_time_ms=25,
            batch_size=10,
        )

        self.assertIsInstance(operation, FindOperation)
        self.assertEqual(operation.filter_spec, {"name": "Ada"})
        self.assertEqual(operation.projection, {"name": 1})
        self.assertEqual(operation.sort, [("rank", 1)])
        self.assertEqual(operation.skip, 2)
        self.assertEqual(operation.limit, 5)
        self.assertEqual(operation.hint, [("name", 1)])
        self.assertEqual(operation.comment, "read")
        self.assertEqual(operation.max_time_ms, 25)
        self.assertEqual(operation.batch_size, 10)

    def test_find_operation_rejects_invalid_skip(self):
        with self.assertRaises(TypeError):
            compile_find_operation({"name": "Ada"}, skip=-1)

    def test_update_operation_compiles_normalized_write_plan(self):
        operation = compile_update_operation(
            {"name": "Ada"},
            sort=[("rank", 1)],
            array_filters=[{"item.score": {"$gte": 10}}],
            hint=[("name", 1)],
            comment="write",
            max_time_ms=25,
            let={"target": "Ada"},
        )

        self.assertIsInstance(operation, UpdateOperation)
        self.assertEqual(operation.filter_spec, {"name": "Ada"})
        self.assertEqual(operation.sort, [("rank", 1)])
        self.assertEqual(operation.array_filters, [{"item.score": {"$gte": 10}}])
        self.assertEqual(operation.hint, [("name", 1)])
        self.assertEqual(operation.comment, "write")
        self.assertEqual(operation.max_time_ms, 25)
        self.assertEqual(operation.let, {"target": "Ada"})

    def test_find_selection_operation_derives_from_update_operation(self):
        update_operation = compile_update_operation(
            {"name": "Ada"},
            sort=[("rank", 1)],
            hint=[("name", 1)],
            comment="write",
            max_time_ms=25,
            let={"target": "Ada"},
        )

        selection = compile_find_selection_from_update_operation(
            update_operation,
            projection={"_id": 1},
            limit=1,
        )

        self.assertIsInstance(selection, FindOperation)
        self.assertEqual(selection.filter_spec, update_operation.filter_spec)
        self.assertEqual(selection.plan, update_operation.plan)
        self.assertEqual(selection.sort, update_operation.sort)
        self.assertEqual(selection.hint, update_operation.hint)
        self.assertEqual(selection.comment, update_operation.comment)
        self.assertEqual(selection.projection, {"_id": 1})
        self.assertEqual(selection.limit, 1)

    def test_update_operation_rejects_invalid_array_filters(self):
        with self.assertRaises(TypeError):
            compile_update_operation({"name": "Ada"}, array_filters=[1])

    def test_compiled_update_plan_is_executable_object(self):
        plan = UpdateEngine.compile_update_plan({"$set": {"name": "Ada"}})
        document = {"_id": "1"}

        modified = plan.apply(document)

        self.assertTrue(modified)
        self.assertEqual(document["name"], "Ada")

    def test_aggregate_operation_compiles_normalized_pipeline_plan(self):
        operation = compile_aggregate_operation(
            [{"$match": {"name": "Ada"}}],
            hint=[("name", 1)],
            comment="agg",
            max_time_ms=25,
            batch_size=10,
            let={"target": "Ada"},
        )

        self.assertIsInstance(operation, AggregateOperation)
        self.assertEqual(operation.pipeline, [{"$match": {"name": "Ada"}}])
        self.assertEqual(operation.hint, [("name", 1)])
        self.assertEqual(operation.comment, "agg")
        self.assertEqual(operation.max_time_ms, 25)
        self.assertEqual(operation.batch_size, 10)
        self.assertEqual(operation.let, {"target": "Ada"})

    def test_aggregate_operation_rejects_invalid_pipeline(self):
        with self.assertRaises(TypeError):
            compile_aggregate_operation({"$match": {"name": "Ada"}})

    def test_admin_cursors_share_materialized_base_classes(self):
        self.assertTrue(issubclass(AsyncIndexCursor, AsyncMaterializedCursor))
        self.assertTrue(issubclass(AsyncListingCursor, AsyncMaterializedCursor))
        self.assertTrue(issubclass(IndexCursor, MaterializedCursor))
        self.assertTrue(issubclass(ListingCursor, MaterializedCursor))

    def test_aggregation_stage_registry_is_the_dispatch_source(self):
        self.assertIn("$match", AGGREGATION_STAGE_HANDLERS)
        self.assertIn("$group", AGGREGATION_STAGE_HANDLERS)
        self.assertIn("$lookup", AGGREGATION_STAGE_HANDLERS)
        self.assertIs(AGGREGATION_STAGE_HANDLERS["$set"], AGGREGATION_STAGE_HANDLERS["$addFields"])

    def test_index_definition_is_shared_source_for_public_metadata(self):
        index = IndexDefinition([("email", 1), ("created_at", -1)], name="email_created", unique=True)

        self.assertEqual(
            index.to_list_document(),
            {
                "name": "email_created",
                "key": {"email": 1, "created_at": -1},
                "fields": ["email", "created_at"],
                "unique": True,
            },
        )
        self.assertEqual(
            index.to_information_entry_map(),
            {
                "email_created": {
                    "key": [("email", 1), ("created_at", -1)],
                    "unique": True,
                }
            },
        )
        self.assertEqual(default_id_index_definition().name, "_id_")

    def test_engine_index_record_wraps_typed_internal_index_metadata(self):
        record = EngineIndexRecord(
            name="email_idx",
            physical_name="idx_email",
            fields=["email"],
            key=[("email", 1)],
            unique=True,
        )

        self.assertEqual(record["name"], "email_idx")
        self.assertEqual(record.get("physical_name"), "idx_email")
        self.assertEqual(
            record.to_definition().to_list_document()["key"],
            {"email": 1},
        )

    def test_operation_option_support_exposes_effective_and_noop_statuses(self):
        self.assertEqual(
            get_operation_option_support("find", "hint"),
            OPERATION_OPTION_SUPPORT["find"]["hint"],
        )
        self.assertEqual(
            get_operation_option_support("find", "hint").status,
            OptionSupportStatus.EFFECTIVE,
        )
        self.assertTrue(is_operation_option_effective("find", "comment"))
        self.assertTrue(is_operation_option_effective("find", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("find", "batch_size"))
        self.assertTrue(is_operation_option_effective("count_documents", "hint"))
        self.assertTrue(is_operation_option_effective("count_documents", "comment"))
        self.assertTrue(is_operation_option_effective("count_documents", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("distinct", "hint"))
        self.assertTrue(is_operation_option_effective("distinct", "comment"))
        self.assertTrue(is_operation_option_effective("distinct", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("estimated_document_count", "comment"))
        self.assertTrue(is_operation_option_effective("estimated_document_count", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("aggregate", "comment"))
        self.assertTrue(is_operation_option_effective("aggregate", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("aggregate", "batch_size"))
        self.assertTrue(is_operation_option_effective("aggregate", "let"))
        self.assertTrue(is_operation_option_effective("update_one", "comment"))
        self.assertTrue(is_operation_option_effective("update_one", "let"))
        self.assertTrue(is_operation_option_effective("bulk_write", "let"))
        self.assertTrue(is_operation_option_effective("bulk_write", "comment"))
        self.assertTrue(is_operation_option_effective("create_index", "comment"))
        self.assertTrue(is_operation_option_effective("create_index", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("create_indexes", "max_time_ms"))
        self.assertEqual(
            get_operation_option_support("missing", "hint").status,
            OptionSupportStatus.UNSUPPORTED,
        )

    def test_operation_option_support_matches_tracked_async_collection_signatures(self):
        for operation, options in OPERATION_OPTION_SUPPORT.items():
            with self.subTest(operation=operation):
                params = set(inspect.signature(getattr(AsyncCollection, operation)).parameters)
                managed = (
                    params & MANAGED_OPERATION_OPTION_NAMES
                ) - OPERATION_OPTION_SIGNATURE_EXCLUSIONS.get(operation, frozenset())
                self.assertEqual(set(options), managed)

    def test_exported_full_catalog_tracks_public_operation_support_matrix(self):
        exported = export_full_compat_catalog()

        self.assertEqual(
            exported["operation_options"]["find"]["hint"]["status"],
            OptionSupportStatus.EFFECTIVE.value,
        )
        self.assertEqual(
            set(exported["operation_options"]),
            set(OPERATION_OPTION_SUPPORT),
        )

    def test_index_model_reuses_index_definition_contract(self):
        from mongoeco.types import IndexModel

        model = IndexModel([("email", 1)], name="email_idx", unique=True)

        self.assertEqual(
            model.definition.to_list_document(),
            {
                "name": "email_idx",
                "key": {"email": 1},
                "fields": ["email"],
                "unique": True,
            },
        )
        self.assertEqual(
            model.document,
            {
                "name": "email_idx",
                "key": {"email": 1},
                "unique": True,
            },
        )

    def test_index_information_annotations_share_type_alias(self):
        self.assertIs(
            inspect.signature(AsyncCollection.index_information).return_annotation,
            IndexInformation,
        )
        self.assertIs(
            inspect.signature(Collection.index_information).return_annotation,
            IndexInformation,
        )
        self.assertIs(
            inspect.signature(MemoryEngine.index_information).return_annotation,
            IndexInformation,
        )
        self.assertIs(
            inspect.signature(SQLiteEngine.index_information).return_annotation,
            IndexInformation,
        )

    def test_sync_database_delegates_admin_surface_to_dedicated_service(self):
        from mongoeco.api._sync.client import MongoClient

        database = MongoClient().get_database("db")

        self.assertIsInstance(database._admin, DatabaseAdminService)
        self.assertIsInstance(database._admin._commands, DatabaseCommandService)

    def test_pymongo_configuration_types_validate_and_are_immutable(self):
        write_concern = WriteConcern("majority", j=True, wtimeout=1000)
        read_concern = ReadConcern("majority")
        read_preference = ReadPreference(
            ReadPreferenceMode.SECONDARY_PREFERRED,
            tag_sets=[{"region": "eu-west"}],
            max_staleness_seconds=120,
        )
        codec_options = CodecOptions(dict, tz_aware=True)
        transaction_options = TransactionOptions(
            read_concern=read_concern,
            write_concern=write_concern,
            read_preference=read_preference,
            max_commit_time_ms=500,
        )

        self.assertEqual(write_concern.document, {"w": "majority", "j": True, "wtimeout": 1000})
        self.assertEqual(read_concern.document, {"level": "majority"})
        self.assertEqual(read_preference.name, "secondaryPreferred")
        self.assertEqual(codec_options.document_class, dict)
        self.assertEqual(transaction_options.max_commit_time_ms, 500)

    def test_pymongo_configuration_types_reject_invalid_values(self):
        with self.assertRaises(TypeError):
            WriteConcern(True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            ReadConcern("")
        with self.assertRaises(ValueError):
            ReadPreference("invalid")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            CodecOptions(list)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            TransactionOptions(max_commit_time_ms=0)

    def test_admin_surface_uses_structured_metadata_annotations(self):
        from mongoeco.api._sync.client import Database, MongoClient

        self.assertIs(AsyncDatabase.validate_collection.__annotations__["return"], CollectionValidationDocument)
        self.assertIs(Database.validate_collection.__annotations__["return"], CollectionValidationDocument)
        self.assertIs(AsyncMongoClient.server_info.__annotations__["return"], BuildInfoDocument)
        self.assertIs(MongoClient.server_info.__annotations__["return"], BuildInfoDocument)

    def test_admin_internal_stats_use_typed_snapshots(self):
        listing_snapshot = CollectionListingSnapshot(name="users")
        database_listing_snapshot = DatabaseListingSnapshot(
            name="db",
            size_on_disk=128,
            empty=False,
        )
        collection_snapshot = CollectionStatsSnapshot(
            namespace="db.users",
            count=4,
            data_size=200,
            index_count=2,
            scale=10,
        )
        database_snapshot = DatabaseStatsSnapshot(
            db_name="db",
            collection_count=3,
            object_count=4,
            data_size=200,
            index_count=5,
            scale=10,
        )

        self.assertEqual(listing_snapshot.to_document()["info"]["readOnly"], False)
        self.assertEqual(database_listing_snapshot.to_document()["sizeOnDisk"], 128)
        self.assertEqual(collection_snapshot.to_document()["size"], 20)
        self.assertEqual(database_snapshot.to_document()["dataSize"], 20)

    def test_admin_internal_results_use_typed_snapshots(self):
        cursor_result = CommandCursorResult(
            namespace="db.users",
            first_batch=[{"_id": 1}],
        )
        list_databases_result = ListDatabasesCommandResult(
            databases=[{"name": "db", "sizeOnDisk": 0, "empty": True}],
            total_size=0,
        )
        validation_result = CollectionValidationSnapshot(
            namespace="db.users",
            record_count=3,
            index_count=1,
            keys_per_index={"_id_": 1},
        )
        self.assertEqual(cursor_result.to_document()["cursor"]["ns"], "db.users")
        self.assertEqual(list_databases_result.to_document()["totalSize"], 0)
        self.assertTrue(validation_result.to_document()["valid"])
        self.assertEqual(CountCommandResult(3).to_document()["n"], 3)
        self.assertEqual(DistinctCommandResult(["Ada"]).to_document()["values"], ["Ada"])
        self.assertEqual(OkResult().to_document()["ok"], 1.0)
        self.assertEqual(
            WriteErrorEntry(index=1, errmsg="boom", code=42, operation="UpdateOne").to_document(),
            {"index": 1, "errmsg": "boom", "code": 42, "op": "UpdateOne"},
        )
        self.assertEqual(
            UpsertedWriteEntry(index=2, document_id="id-2").to_document(),
            {"index": 2, "_id": "id-2"},
        )
        self.assertEqual(
            BulkWriteErrorDetails(
                write_errors=[WriteErrorEntry(index=0, errmsg="dup")],
                inserted_count=1,
                upserted=[UpsertedWriteEntry(index=1, document_id="new-id")],
            ).to_document()["upserted"][0]["_id"],
            "new-id",
        )
        self.assertEqual(
            NamespaceOkResult("db.users").to_document()["ns"],
            "db.users",
        )
        self.assertEqual(
            WriteCommandResult(
                2,
                modified_count=1,
                upserted=[UpsertedWriteEntry(index=0, document_id="seed")],
            ).to_document()["upserted"][0]["_id"],
            "seed",
        )
        self.assertFalse(
            FindAndModifyCommandResult(
                last_error_object=FindAndModifyLastErrorObject(
                    count=1,
                    updated_existing=False,
                    upserted_id="new-id",
                ),
                value={"_id": "new-id"},
            ).to_document()["lastErrorObject"]["updatedExisting"]
        )
        self.assertEqual(
            CreateIndexesCommandResult(1, 2, True).to_document()["numIndexesAfter"],
            2,
        )
        self.assertEqual(
            DropIndexesCommandResult(2, message="done").to_document()["msg"],
            "done",
        )
        self.assertEqual(
            DropDatabaseCommandResult("db").to_document()["dropped"],
            "db",
        )

    def test_database_command_service_routes_use_typed_records(self):
        route = AsyncDatabaseCommandService._DELEGATED_COMMAND_HANDLERS["dropDatabase"]

        self.assertIsInstance(route, AsyncDatabaseCommandService.Route)
        self.assertFalse(route.passes_spec)

    def test_database_command_service_parses_typed_admin_commands(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin._commands

        coll_stats = service.parse_raw_command({"collStats": "users"})
        validate = service.parse_raw_command({"validate": "users"})
        find_and_modify = service.parse_raw_command(
            {"findAndModify": "users", "query": {"name": "Ada"}, "update": {"$set": {"rank": 1}}}
        )
        find = service.parse_raw_command({"find": "users", "filter": {}})
        aggregate = service.parse_raw_command({"aggregate": "users", "pipeline": []})
        count = service.parse_raw_command({"count": "users"})
        distinct = service.parse_raw_command({"distinct": "users", "key": "name"})
        list_indexes = service.parse_raw_command({"listIndexes": "users"})
        delegated = service.parse_raw_command({"create": "users"})

        self.assertIsInstance(
            coll_stats,
            AsyncDatabaseCommandService.CollectionStatsCommand,
        )
        self.assertIsInstance(
            validate,
            AsyncDatabaseCommandService.ValidateCollectionCommand,
        )
        self.assertIsInstance(
            find_and_modify,
            AsyncDatabaseCommandService.FindAndModifyCommand,
        )
        self.assertIsInstance(
            find,
            AsyncDatabaseCommandService.FindCommand,
        )
        self.assertIsInstance(
            aggregate,
            AsyncDatabaseCommandService.AggregateCommand,
        )
        self.assertIsInstance(
            count,
            AsyncDatabaseCommandService.CountCommand,
        )
        self.assertIsInstance(
            distinct,
            AsyncDatabaseCommandService.DistinctCommand,
        )
        self.assertIsInstance(
            list_indexes,
            AsyncDatabaseCommandService.ListIndexesCommand,
        )
        self.assertIsInstance(
            delegated,
            AsyncDatabaseCommandService.DelegatedAdminCommand,
        )
        self.assertIsInstance(find.operation, FindOperation)
        self.assertIsInstance(count.operation, FindOperation)
        self.assertIsInstance(distinct.operation, FindOperation)
        self.assertIsInstance(aggregate.operation, AggregateOperation)

    def test_database_admin_service_compiles_count_and_distinct_operations(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin

        count_collection, count_operation = service._compile_command_count_operation(
            {"count": "users", "query": {"name": "Ada"}, "skip": 1, "limit": 2}
        )
        distinct_collection, distinct_key, distinct_operation = (
            service._compile_command_distinct_operation(
                {"distinct": "users", "key": "name", "query": {"active": True}}
            )
        )

        self.assertEqual(count_collection, "users")
        self.assertEqual(count_operation.filter_spec, {"name": "Ada"})
        self.assertEqual(count_operation.skip, 1)
        self.assertEqual(count_operation.limit, 2)
        self.assertEqual(distinct_collection, "users")
        self.assertEqual(distinct_key, "name")
        self.assertEqual(distinct_operation.filter_spec, {"active": True})

    def test_admin_parsing_centralizes_listing_and_validation_options(self):
        list_options = normalize_list_collections_options(
            {
                "nameOnly": True,
                "authorizedCollections": False,
                "filter": {"name": "users"},
            }
        )
        validate_options = normalize_validate_command_options(
            scandata=True,
            full=False,
            background=None,
            comment="trace",
        )

        self.assertTrue(list_options.name_only)
        self.assertEqual(list_options.filter_spec, {"name": "users"})
        self.assertTrue(validate_options.scandata)
        self.assertEqual(validate_options.comment, "trace")

    def test_admin_parsing_rejects_invalid_validation_flags(self):
        with self.assertRaises(TypeError):
            normalize_validate_command_options(scandata="yes")
        with self.assertRaises(TypeError):
            normalize_list_collections_options({"nameOnly": "yes"})

    def test_database_command_service_executes_typed_static_results(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin._commands
        command = service.parse_raw_command("buildInfo")

        result = asyncio.run(service.execute(command))

        self.assertIsInstance(result, BuildInfoResult)
        self.assertEqual(result.to_document()["gitVersion"], "mongoeco")

    def test_database_command_service_executes_typed_delegated_results_before_serialization(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin._commands
        command = service.parse_raw_command({"listDatabases": 1})

        result = asyncio.run(service.execute(command))

        self.assertIsInstance(result, ListDatabasesCommandResult)
        self.assertEqual(result.to_document()["databases"], [])

    def test_database_command_service_executes_typed_stats_and_validation_snapshots(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin._commands

        async def _run():
            await database.get_collection("users").insert_one({"_id": "1", "name": "Ada"})
            coll_stats = await service.execute(service.parse_raw_command({"collStats": "users"}))
            db_stats = await service.execute(service.parse_raw_command("dbStats"))
            validate = await service.execute(service.parse_raw_command({"validate": "users"}))
            return coll_stats, db_stats, validate

        coll_stats, db_stats, validate = asyncio.run(_run())

        self.assertIsInstance(coll_stats, CollectionStatsSnapshot)
        self.assertIsInstance(db_stats, DatabaseStatsSnapshot)
        self.assertIsInstance(validate, CollectionValidationSnapshot)
        self.assertEqual(coll_stats.namespace, "db.users")
        self.assertEqual(db_stats.collection_count, 1)
        self.assertEqual(validate.record_count, 1)

    def test_database_command_service_can_execute_and_serialize_in_one_step(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin._commands

        result = asyncio.run(service.execute_document("buildInfo"))

        self.assertEqual(result["gitVersion"], "mongoeco")
        self.assertEqual(result["ok"], 1.0)
