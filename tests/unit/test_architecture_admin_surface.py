import asyncio
import inspect
import unittest

from mongoeco.api._async.collection import AsyncCollection
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
from mongoeco.core.aggregation import (
    AGGREGATION_STAGE_HANDLERS,
    AGGREGATION_STAGE_SPECS,
    get_aggregation_stage_spec,
    is_streamable_aggregation_stage,
    register_aggregation_stage,
    unregister_aggregation_stage,
)
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
    EngineUpdateSemantics,
    build_query_plan_explanation,
    compile_find_semantics,
    compile_update_semantics,
)
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import EngineIndexRecord, IndexDefinition, IndexInformation, default_id_index_definition
from mongoeco.types import (
    BulkWriteErrorDetails,
    CodecOptions,
    ExecutionLineageStep,
    PlanningMode,
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




class ArchitectureAdminSurfaceTests(unittest.TestCase):
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
