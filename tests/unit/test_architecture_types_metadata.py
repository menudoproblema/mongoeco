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




class ArchitectureTypeMetadataTests(unittest.TestCase):
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
        self.assertTrue(is_operation_option_effective("aggregate", "allow_disk_use"))
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

