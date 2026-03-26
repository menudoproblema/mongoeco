import unittest

from mongoeco.compat import (
    OPERATION_OPTION_SUPPORT,
    OptionSupportStatus,
    get_operation_option_support,
    is_operation_option_effective,
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
from mongoeco.types import IndexDefinition, default_id_index_definition


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
            OptionSupportStatus.ACCEPTED_NOOP,
        )
        self.assertTrue(is_operation_option_effective("aggregate", "let"))
        self.assertFalse(is_operation_option_effective("update_one", "let"))
        self.assertIsNone(get_operation_option_support("missing", "hint"))
