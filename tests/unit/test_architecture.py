import asyncio
import inspect
import unittest

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
from mongoeco.api._async.database_commands import (
    AsyncDatabaseCommandService,
    BuildInfoResult,
)
from mongoeco.api._async.client import AsyncDatabase
from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.api.operations import (
    FindOperation,
    UpdateOperation,
    compile_find_operation,
    compile_update_operation,
)
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.api._sync.collection import Collection
from mongoeco.api._sync.database_admin import DatabaseAdminService
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
from mongoeco.engines.base import (
    AsyncAdminEngine,
    AsyncDatabaseAdminEngine,
    AsyncCrudEngine,
    AsyncExplainEngine,
    AsyncIndexAdminEngine,
    AsyncLifecycleEngine,
    AsyncNamespaceAdminEngine,
    AsyncSessionEngine,
    AsyncStorageEngine,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import IndexDefinition, IndexInformation, default_id_index_definition
from mongoeco.types import (
    CodecOptions,
    ReadConcern,
    ReadPreference,
    ReadPreferenceMode,
    TransactionOptions,
    WriteConcern,
    BuildInfoDocument,
    CollectionStatsSnapshot,
    CollectionValidationDocument,
    DatabaseStatsSnapshot,
)


class ArchitectureUnitTests(unittest.TestCase):
    def test_engines_satisfy_split_storage_protocols(self):
        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                self.assertIsInstance(engine, AsyncSessionEngine)
                self.assertIsInstance(engine, AsyncLifecycleEngine)
                self.assertIsInstance(engine, AsyncCrudEngine)
                self.assertIsInstance(engine, AsyncIndexAdminEngine)
                self.assertIsInstance(engine, AsyncExplainEngine)
                self.assertIsInstance(engine, AsyncDatabaseAdminEngine)
                self.assertIsInstance(engine, AsyncNamespaceAdminEngine)
                self.assertIsInstance(engine, AsyncAdminEngine)
                self.assertIsInstance(engine, AsyncStorageEngine)

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

    def test_update_operation_rejects_invalid_array_filters(self):
        with self.assertRaises(TypeError):
            compile_update_operation({"name": "Ada"}, array_filters=[1])

    def test_admin_cursors_share_materialized_base_classes(self):
        self.assertTrue(issubclass(AsyncIndexCursor, AsyncMaterializedCursor))
        self.assertTrue(issubclass(AsyncListingCursor, AsyncMaterializedCursor))
        self.assertTrue(issubclass(IndexCursor, MaterializedCursor))
        self.assertTrue(issubclass(ListingCursor, MaterializedCursor))

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

    def test_sync_client_exposes_resource_return_helper(self):
        from mongoeco.api._sync.client import MongoClient

        self.assertTrue(callable(getattr(MongoClient, "_run_resource", None)))

    def test_sync_database_delegates_admin_surface_to_dedicated_service(self):
        from mongoeco.api._sync.client import MongoClient

        database = MongoClient().get_database("db")

        self.assertIsInstance(database._admin, DatabaseAdminService)

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

        self.assertEqual(collection_snapshot.to_document()["size"], 20)
        self.assertEqual(database_snapshot.to_document()["dataSize"], 20)

    def test_database_command_service_routes_use_typed_records(self):
        route = AsyncDatabaseCommandService._DELEGATED_COMMAND_HANDLERS["dropDatabase"]

        self.assertIsInstance(route, AsyncDatabaseCommandService.Route)
        self.assertFalse(route.passes_spec)

    def test_database_command_service_parses_typed_admin_commands(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin._commands

        coll_stats = service.parse_raw_command({"collStats": "users"})
        validate = service.parse_raw_command({"validate": "users"})
        delegated = service.parse_raw_command({"find": "users", "filter": {}})

        self.assertIsInstance(
            coll_stats,
            AsyncDatabaseCommandService.CollectionStatsCommand,
        )
        self.assertIsInstance(
            validate,
            AsyncDatabaseCommandService.ValidateCollectionCommand,
        )
        self.assertIsInstance(
            delegated,
            AsyncDatabaseCommandService.DelegatedAdminCommand,
        )

    def test_database_command_service_executes_typed_static_results(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        service = database._admin._commands
        command = service.parse_raw_command("buildInfo")

        result = asyncio.run(service.execute(command))

        self.assertIsInstance(result, BuildInfoResult)
        self.assertEqual(result.to_document()["gitVersion"], "mongoeco")
