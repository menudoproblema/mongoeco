import inspect
import unittest

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.api._sync.collection import Collection
from mongoeco.api._sync._materialized_cursor import MaterializedCursor
from mongoeco.api._sync.index_cursor import IndexCursor
from mongoeco.api._sync.listing_cursor import ListingCursor
from mongoeco.compat import (
    OPERATION_OPTION_SUPPORT,
    OptionSupportStatus,
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
        self.assertTrue(is_operation_option_effective("aggregate", "comment"))
        self.assertTrue(is_operation_option_effective("aggregate", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("aggregate", "let"))
        self.assertTrue(is_operation_option_effective("update_one", "comment"))
        self.assertTrue(is_operation_option_effective("bulk_write", "comment"))
        self.assertTrue(is_operation_option_effective("create_index", "comment"))
        self.assertTrue(is_operation_option_effective("create_index", "max_time_ms"))
        self.assertTrue(is_operation_option_effective("create_indexes", "max_time_ms"))
        self.assertFalse(is_operation_option_effective("update_one", "let"))
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
