import unittest

from mongoeco.api.public_api import (
    COLLECTION_COUNT_DOCUMENTS_SPEC,
    COLLECTION_DELETE_MANY_SPEC,
    COLLECTION_DELETE_ONE_SPEC,
    COLLECTION_DISTINCT_SPEC,
    COLLECTION_FIND_ONE_AND_DELETE_SPEC,
    COLLECTION_FIND_ONE_AND_REPLACE_SPEC,
    COLLECTION_FIND_ONE_AND_UPDATE_SPEC,
    COLLECTION_FIND_SPEC,
    COLLECTION_REPLACE_ONE_SPEC,
    COLLECTION_UPDATE_MANY_SPEC,
    COLLECTION_UPDATE_ONE_SPEC,
)
from mongoeco.compat.operation_support import OPERATION_OPTION_SUPPORT


class PublicApiSpecTests(unittest.TestCase):
    def test_collection_specs_cover_declared_supported_options(self):
        specs = {
            "find": COLLECTION_FIND_SPEC,
            "update_one": COLLECTION_UPDATE_ONE_SPEC,
            "update_many": COLLECTION_UPDATE_MANY_SPEC,
            "replace_one": COLLECTION_REPLACE_ONE_SPEC,
            "find_one_and_update": COLLECTION_FIND_ONE_AND_UPDATE_SPEC,
            "find_one_and_replace": COLLECTION_FIND_ONE_AND_REPLACE_SPEC,
            "find_one_and_delete": COLLECTION_FIND_ONE_AND_DELETE_SPEC,
            "delete_one": COLLECTION_DELETE_ONE_SPEC,
            "delete_many": COLLECTION_DELETE_MANY_SPEC,
            "count_documents": COLLECTION_COUNT_DOCUMENTS_SPEC,
            "distinct": COLLECTION_DISTINCT_SPEC,
        }

        for operation, spec in specs.items():
            with self.subTest(operation=operation):
                self.assertTrue(
                    frozenset(OPERATION_OPTION_SUPPORT.get(operation, {})) <= spec.allowed_options
                )

    def test_collection_specs_keep_public_only_arguments(self):
        self.assertNotIn("batch_size", COLLECTION_COUNT_DOCUMENTS_SPEC.allowed_options)
        self.assertNotIn("limit", COLLECTION_DISTINCT_SPEC.allowed_options)
        self.assertIn("projection", COLLECTION_FIND_ONE_AND_DELETE_SPEC.allowed_options)


if __name__ == "__main__":
    unittest.main()
