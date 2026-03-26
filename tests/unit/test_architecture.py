import inspect
import unittest

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._sync.collection import Collection
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
    AsyncCrudEngine,
    AsyncExplainEngine,
    AsyncIndexAdminEngine,
    AsyncLifecycleEngine,
    AsyncSessionEngine,
    AsyncStorageEngine,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import IndexDefinition, IndexInformation, default_id_index_definition


class ArchitectureUnitTests(unittest.TestCase):
    def test_engines_satisfy_split_storage_protocols(self):
        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                self.assertIsInstance(engine, AsyncSessionEngine)
                self.assertIsInstance(engine, AsyncLifecycleEngine)
                self.assertIsInstance(engine, AsyncCrudEngine)
                self.assertIsInstance(engine, AsyncIndexAdminEngine)
                self.assertIsInstance(engine, AsyncExplainEngine)
                self.assertIsInstance(engine, AsyncAdminEngine)
                self.assertIsInstance(engine, AsyncStorageEngine)

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
        self.assertTrue(is_operation_option_effective("aggregate", "let"))
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
