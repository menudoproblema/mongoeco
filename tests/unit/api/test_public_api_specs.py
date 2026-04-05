import unittest

from mongoeco.api.public_api import (
    ARG_UNSET,
    PublicOperationSpec,
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
    _assert_catalog_alignment,
    normalize_public_operation_arguments,
)
from mongoeco.compat import PYMONGO_PROFILE_49
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

    def test_normalize_public_operation_arguments_covers_required_and_profile_errors(self):
        spec = PublicOperationSpec(
            name="update_one",
            allowed_options=frozenset({"filter_spec", "sort"}),
            required_arguments=frozenset({"filter_spec"}),
            sort_depends_on_profile=True,
        )
        with self.assertRaisesRegex(TypeError, "missing required argument"):
            normalize_public_operation_arguments(
                spec,
                explicit={"filter_spec": ARG_UNSET},
                extra_kwargs={},
            )
        with self.assertRaisesRegex(TypeError, "sort is not supported"):
            normalize_public_operation_arguments(
                spec,
                explicit={"filter_spec": {"_id": 1}},
                extra_kwargs={"sort": {"_id": 1}},
                profile=PYMONGO_PROFILE_49,
            )

    def test_assert_catalog_alignment_raises_for_missing_option(self):
        with self.assertRaisesRegex(AssertionError, "missing catalog options"):
            _assert_catalog_alignment(
                "find",
                PublicOperationSpec(
                    name="find",
                    allowed_options=frozenset({"filter_spec"}),
                ),
            )

    def test_normalize_public_operation_arguments_covers_alias_conflict_and_unexpected_option(self):
        spec = PublicOperationSpec(
            name="find_one",
            allowed_options=frozenset({"filter_spec"}),
            aliases={"filter": "filter_spec"},
        )
        with self.assertRaisesRegex(TypeError, "cannot pass both filter and filter_spec"):
            normalize_public_operation_arguments(
                spec,
                explicit={"filter_spec": {"_id": 1}},
                extra_kwargs={"filter": {"_id": 2}},
            )
        with self.assertRaisesRegex(TypeError, "unexpected keyword argument 'sort'"):
            normalize_public_operation_arguments(
                spec,
                explicit={"filter_spec": ARG_UNSET},
                extra_kwargs={"sort": {"_id": 1}},
            )


if __name__ == "__main__":
    unittest.main()
