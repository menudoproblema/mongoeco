import asyncio
import decimal
import inspect
from typing import get_type_hints
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
from mongoeco.types import EngineIndexRecord, IndexDefinition, IndexInformation, IndexModel, default_id_index_definition
from mongoeco.types import (
    AggregateExplanation,
    BulkWriteErrorDetails,
    CodecOptions,
    ExecutionLineageStep,
    PhysicalPlanStep,
    PlanningIssue,
    SearchIndexDefinition,
    SearchIndexModel,
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
    default_id_index_document,
    default_id_index_information,
    is_ordered_index_spec,
    normalize_codec_options,
    normalize_index_keys,
    normalize_read_concern,
    normalize_read_preference,
    normalize_transaction_options,
    normalize_write_concern,
    special_index_directions,
)
from mongoeco.core.operators import UpdateEngine




class ArchitectureTypeMetadataTests(unittest.TestCase):
    def test_public_bson_value_types_normalize_equality_and_ordering(self):
        from mongoeco.types import Binary, Decimal128, Regex, Timestamp

        self.assertNotEqual(Binary(b"x", subtype=0), Binary(b"x", subtype=5))
        self.assertEqual(Regex("^a", "mi"), Regex("^a", "im"))
        self.assertEqual(Regex("^a", "mi").flags, "im")
        self.assertGreater(Timestamp(10, 0), Timestamp(2, 0))
        self.assertEqual(Decimal128(decimal.Decimal("NaN")), Decimal128(decimal.Decimal("NaN")))

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

    def test_index_definition_and_model_round_trip_expire_after_seconds(self):
        from mongoeco.types import IndexModel

        definition = IndexDefinition(
            [("expires_at", 1)],
            name="expires_at_1",
            expire_after_seconds=30,
        )
        model = IndexModel([("expires_at", 1)], expireAfterSeconds=30)

        self.assertEqual(
            definition.to_list_document(),
            {
                "name": "expires_at_1",
                "key": {"expires_at": 1},
                "unique": False,
                "expireAfterSeconds": 30,
            },
        )
        self.assertEqual(
            definition.to_information_entry(),
            {"key": [("expires_at", 1)], "expireAfterSeconds": 30},
        )
        self.assertEqual(model.expire_after_seconds, 30)
        self.assertEqual(model.document["expireAfterSeconds"], 30)
        self.assertEqual(model.definition.expire_after_seconds, 30)

    def test_index_definition_and_model_reject_invalid_expire_after_seconds(self):
        from mongoeco.types import IndexModel

        with self.assertRaises(TypeError):
            IndexDefinition([("expires_at", 1)], name="expires_at_1", expire_after_seconds=-1)
        with self.assertRaises(TypeError):
            IndexModel([("expires_at", 1)], expireAfterSeconds=-1)
        with self.assertRaises(TypeError):
            IndexModel([("expires_at", 1)], expireAfterSeconds=True)

    def test_index_information_annotations_share_type_alias(self):
        async_index_hints = get_type_hints(AsyncCollection.index_information)
        sync_index_hints = get_type_hints(Collection.index_information)
        memory_index_hints = get_type_hints(MemoryEngine.index_information)
        sqlite_index_hints = get_type_hints(SQLiteEngine.index_information)

        self.assertIs(
            async_index_hints["return"],
            IndexInformation,
        )
        self.assertIs(
            sync_index_hints["return"],
            IndexInformation,
        )
        self.assertIs(
            memory_index_hints["return"],
            IndexInformation,
        )
        self.assertIs(
            sqlite_index_hints["return"],
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

    def test_read_preference_normalizes_document_and_rejects_invalid_shapes(self):
        preference = ReadPreference(
            "nearest",
            tag_sets=[{"region": "eu", "rack": "a"}],
            max_staleness_seconds=90,
        )

        self.assertEqual(preference.name, "nearest")
        self.assertEqual(
            preference.document,
            {
                "mode": "nearest",
                "tag_sets": [{"region": "eu", "rack": "a"}],
                "maxStalenessSeconds": 90,
            },
        )

        with self.assertRaises(TypeError):
            ReadPreference(object())  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            ReadPreference(tag_sets="bad")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            ReadPreference(tag_sets=[("region", "eu")])  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            ReadPreference(tag_sets=[{"": "eu"}])
        with self.assertRaises(TypeError):
            ReadPreference(tag_sets=[{"region": 1}])  # type: ignore[dict-item]
        with self.assertRaises(ValueError):
            ReadPreference(max_staleness_seconds=0)
        with self.assertRaises(ValueError):
            ReadPreference(max_staleness_seconds=30)

    def test_normalize_configuration_helpers_require_matching_types(self):
        self.assertEqual(normalize_write_concern(None).document, {})
        self.assertEqual(normalize_read_concern(None).document, {})
        self.assertEqual(normalize_read_preference(None).document, {"mode": "primary"})
        self.assertEqual(normalize_codec_options(None).document_class, dict)
        self.assertIsNone(normalize_transaction_options(None).max_commit_time_ms)

        with self.assertRaises(TypeError):
            normalize_write_concern("majority")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            normalize_read_concern("majority")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            normalize_read_preference("primary")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            normalize_codec_options(dict)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            normalize_transaction_options({})  # type: ignore[arg-type]

    def test_normalize_index_keys_supports_multiple_forms_and_rejects_invalid_values(self):
        self.assertEqual(normalize_index_keys("email"), [("email", 1)])
        self.assertEqual(normalize_index_keys({"email": 1, "age": -1}), [("email", 1), ("age", -1)])
        self.assertEqual(normalize_index_keys(["email", ("age", -1)]), [("email", 1), ("age", -1)])
        self.assertEqual(
            normalize_index_keys({"content": "text", "location": "2dsphere"}),
            [("content", "text"), ("location", "2dsphere")],
        )
        self.assertFalse(is_ordered_index_spec(normalize_index_keys({"content": "text"})))
        self.assertEqual(
            special_index_directions(normalize_index_keys([("content", "text")])),
            ("text",),
        )

        with self.assertRaises(ValueError):
            normalize_index_keys("")
        with self.assertRaises(ValueError):
            normalize_index_keys({})
        with self.assertRaises(TypeError):
            normalize_index_keys(1)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            normalize_index_keys([])
        with self.assertRaises(ValueError):
            normalize_index_keys({"email": 0})
        with self.assertRaises(ValueError):
            normalize_index_keys({"email": "ascending"})
        with self.assertRaises(TypeError):
            normalize_index_keys({1: 1})  # type: ignore[dict-item]
        with self.assertRaises(ValueError):
            normalize_index_keys([""])
        with self.assertRaises(TypeError):
            normalize_index_keys([("email", 1, "extra")])  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            normalize_index_keys([(1, 1)])  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            normalize_index_keys([("email", True)])  # type: ignore[list-item]

    def test_admin_surface_uses_structured_metadata_annotations(self):
        from mongoeco.api._sync.client import Database, MongoClient

        async_database_hints = get_type_hints(AsyncDatabase.validate_collection)
        sync_database_hints = get_type_hints(Database.validate_collection)
        async_client_hints = get_type_hints(AsyncMongoClient.server_info)
        sync_client_hints = get_type_hints(MongoClient.server_info)

        self.assertIs(async_database_hints["return"], CollectionValidationDocument)
        self.assertIs(sync_database_hints["return"], CollectionValidationDocument)
        self.assertIs(async_client_hints["return"], BuildInfoDocument)
        self.assertIs(sync_client_hints["return"], BuildInfoDocument)

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

    def test_explanation_types_serialize_optional_metadata(self):
        explanation = QueryPlanExplanation(
            engine="memory",
            strategy="scan",
            plan="collection_scan",
            sort=[("score", -1)],
            skip=2,
            limit=5,
            hint="score_-1",
            hinted_index="score_-1",
            comment="trace",
            max_time_ms=10,
            details={"stage": "scan"},
            indexes=[{"name": "score_-1"}],
            planning_mode=PlanningMode.RELAXED,
            planning_issues=(PlanningIssue(scope="planner", message="fallback"),),
            execution_lineage=(ExecutionLineageStep(runtime="python", phase="match", detail="full scan"),),
            physical_plan=(PhysicalPlanStep(runtime="python", operation="scan", detail="seq"),),
            fallback_reason="unsupported filter",
        )
        aggregate = AggregateExplanation(
            engine_plan=explanation,
            remaining_pipeline=[{"$project": {"_id": 0}}],
            hint="score_-1",
            comment="trace",
            max_time_ms=10,
            batch_size=50,
            allow_disk_use=True,
            let={"tenant": "a"},
            streaming_batch_execution=True,
            planning_mode=PlanningMode.RELAXED,
            planning_issues=(PlanningIssue(scope="aggregate", message="streamed"),),
        )

        self.assertEqual(explanation.to_document()["planning_mode"], "relaxed")
        self.assertEqual(explanation.to_document()["planning_issues"][0]["scope"], "planner")
        self.assertEqual(explanation.to_document()["execution_lineage"][0]["phase"], "match")
        self.assertEqual(explanation.to_document()["physical_plan"][0]["operation"], "scan")
        self.assertEqual(explanation.to_document()["fallback_reason"], "unsupported filter")
        self.assertEqual(aggregate.to_document()["engine_plan"]["engine"], "memory")
        self.assertEqual(aggregate.to_document()["planning_issues"][0]["scope"], "aggregate")

    def test_stats_snapshots_reject_invalid_values(self):
        with self.assertRaises(ValueError):
            CollectionStatsSnapshot(namespace="", count=1, data_size=1, index_count=1)
        with self.assertRaises(ValueError):
            CollectionStatsSnapshot(namespace="db.users", count=-1, data_size=1, index_count=1)
        with self.assertRaises(ValueError):
            CollectionStatsSnapshot(namespace="db.users", count=1, data_size=1, index_count=1, scale=0)
        with self.assertRaises(ValueError):
            DatabaseStatsSnapshot(db_name="", collection_count=1, object_count=1, data_size=1, index_count=1)
        with self.assertRaises(ValueError):
            DatabaseStatsSnapshot(db_name="db", collection_count=-1, object_count=1, data_size=1, index_count=1)
        with self.assertRaises(ValueError):
            DatabaseStatsSnapshot(db_name="db", collection_count=1, object_count=1, data_size=1, index_count=1, scale=0)

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

    def test_index_definition_and_model_include_sparse_partial_and_default_id_helpers(self):
        partial_filter = {"expires_at": {"$exists": True}}
        definition = IndexDefinition(
            [("expires_at", 1)],
            name="ttl_sparse",
            unique=True,
            sparse=True,
            partial_filter_expression=partial_filter,
            expire_after_seconds=60,
        )
        partial_filter["expires_at"]["$exists"] = False
        model = definition.to_model_document()
        info = definition.to_information_entry()

        self.assertEqual(
            definition.to_list_document(),
            {
                "name": "ttl_sparse",
                "key": {"expires_at": 1},
                "unique": True,
                "sparse": True,
                "partialFilterExpression": {"expires_at": {"$exists": True}},
                "expireAfterSeconds": 60,
            },
        )
        self.assertEqual(
            model,
            {
                "name": "ttl_sparse",
                "key": {"expires_at": 1},
                "unique": True,
                "sparse": True,
                "partialFilterExpression": {"expires_at": {"$exists": True}},
                "expireAfterSeconds": 60,
            },
        )
        self.assertEqual(
            info,
            {
                "key": [("expires_at", 1)],
                "unique": True,
                "sparse": True,
                "partialFilterExpression": {"expires_at": {"$exists": True}},
                "expireAfterSeconds": 60,
            },
        )
        self.assertEqual(default_id_index_document()["name"], "_id_")
        self.assertEqual(default_id_index_information()["_id_"]["key"], [("_id", 1)])

        sparse_model = IndexModel(
            [("tenant", 1)],
            sparse=True,
            partialFilterExpression={"tenant": {"$exists": True}},
        )
        self.assertEqual(sparse_model.resolved_name, "tenant_1")
        self.assertTrue(sparse_model.document["sparse"])
        self.assertEqual(
            sparse_model.definition.partial_filter_expression,
            {"tenant": {"$exists": True}},
        )

        with self.assertRaises(TypeError):
            IndexDefinition([("tenant", 1)], name="bad", unique=1)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            IndexDefinition([("tenant", 1)], name="bad", sparse=1)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            IndexDefinition([("tenant", 1)], name="bad", partial_filter_expression="x")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            IndexModel([("tenant", 1)], unique=1)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            IndexModel([("tenant", 1)], sparse=1)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            IndexModel([("tenant", 1)], partialFilterExpression="x")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            IndexModel([("tenant", 1)], unexpected=True)  # type: ignore[arg-type]

    def test_search_index_types_expose_search_and_vector_snapshots(self):
        search_definition = SearchIndexDefinition(
            {"mappings": {"dynamic": True}},
            name="search_idx",
        )
        vector_definition = SearchIndexDefinition(
            {"fields": [{"type": "vector", "path": "embedding"}]},
            name="vector_idx",
            index_type="vectorSearch",
        )
        unsupported_vector = SearchIndexDefinition(
            {"fields": [{"type": "text", "path": "title"}]},
            name="broken_vector",
            index_type="vectorSearch",
        )
        model = SearchIndexModel(
            {"fields": [{"type": "vector", "path": "embedding"}]},
            type="vectorSearch",
        )

        self.assertEqual(search_definition.to_document()["status"], "READY")
        self.assertEqual(search_definition.to_document()["capabilities"], ["text", "phrase"])
        self.assertTrue(vector_definition.to_document()["queryable"])
        self.assertEqual(vector_definition.to_document()["queryMode"], "vector")
        self.assertEqual(vector_definition.to_document()["capabilities"], ["vectorSearch"])
        self.assertFalse(unsupported_vector.to_document()["queryable"])
        self.assertEqual(unsupported_vector.to_document()["status"], "UNSUPPORTED")
        self.assertEqual(model.resolved_name, "default")
        self.assertEqual(model.definition_snapshot.index_type, "vectorSearch")

        with self.assertRaises(TypeError):
            SearchIndexDefinition([], name="bad")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            SearchIndexDefinition({}, name="")
        with self.assertRaises(ValueError):
            SearchIndexDefinition({}, name="bad", index_type="")
        with self.assertRaises(TypeError):
            SearchIndexModel([], name="bad")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            SearchIndexModel({}, name="")
        with self.assertRaises(ValueError):
            SearchIndexModel({}, index_type="")
        with self.assertRaises(TypeError):
            SearchIndexModel({}, unexpected=True)  # type: ignore[arg-type]

    def test_write_concern_rejects_non_int_timeouts_and_invalid_w_values(self):
        with self.assertRaises(ValueError):
            WriteConcern(-1)
        with self.assertRaises(ValueError):
            WriteConcern("")
        with self.assertRaises(TypeError):
            WriteConcern(1.5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            WriteConcern(wtimeout=True)
        with self.assertRaises(ValueError):
            WriteConcern(wtimeout=-1)
        with self.assertRaises(TypeError):
            WriteConcern(j="yes")  # type: ignore[arg-type]
