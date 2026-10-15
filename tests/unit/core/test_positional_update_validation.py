"""Regression checks for positional traversal and mutable update candidates."""

import unittest

from mongoeco.core.bson_scalars import BsonInt64
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.paths import set_document_value
from mongoeco.core.update_paths import expand_positional_update_paths
from mongoeco.core.upserts import seed_upsert_document
from mongoeco.engines.sqlite_query import translate_compiled_update_plan
from mongoeco.errors import OperationFailure


class PositionalUpdateValidationTests(unittest.TestCase):
    def test_conflicting_concrete_paths_reject_the_whole_document(self):
        document = {"items": [{"key": "a", "value": 0}], "untouched": 0}
        with self.assertRaises(OperationFailure) as error:
            UpdateEngine.apply_update(
                document,
                {
                    "$inc": {"untouched": 1},
                    "$set": {"items.$[].value": 1, "items.$[i].value": 2},
                },
                array_filters=[{"i.key": "a"}],
            )
        self.assertEqual(error.exception.code, 40)
        self.assertEqual(
            document, {"items": [{"key": "a", "value": 0}], "untouched": 0}
        )

    def test_compiled_plan_does_not_reuse_matches_between_documents(self):
        plan = UpdateEngine.compile_update_plan(
            {"$inc": {"items.$[i].value": 1}, "$set": {"items.$[i].selected": True}},
            array_filters=[{"i.value": 0}],
        )
        selected = {"items": [{"value": 0}]}
        excluded = {"items": [{"value": 2}]}
        self.assertTrue(plan.apply(selected))
        self.assertFalse(plan.apply(excluded))
        self.assertEqual(selected, {"items": [{"value": 1, "selected": True}]})
        self.assertEqual(excluded, {"items": [{"value": 2}]})

    def test_new_numeric_path_components_create_objects(self):
        document = {"items": []}
        self.assertTrue(set_document_value(document, "items.2.0.1.value", 2))
        self.assertEqual(document, {"items": [None, None, {"0": {"1": {"value": 2}}}]})

    def test_missing_array_reports_the_full_concrete_path(self):
        with self.assertRaises(OperationFailure) as error:
            expand_positional_update_paths(
                {"groups": [{}]},
                "groups.$[].items.$[].value",
                filtered_matcher=lambda _identifier, _item: True,
            )
        self.assertEqual(error.exception.code, 2)
        self.assertIn("'groups.0.items' must exist", str(error.exception))

    def test_numeric_object_keys_and_list_indexes_resolve_by_container(self):
        for items in ({"0": {"tags": [1]}}, [{"tags": [1]}]):
            with self.subTest(items=items):
                self.assertEqual(
                    expand_positional_update_paths(
                        {"items": items},
                        "items.0.tags.$[]",
                        filtered_matcher=lambda _identifier, _item: True,
                    ),
                    ["items.0.tags.0"],
                )

    def test_existing_null_is_rejected_but_new_array_slots_can_be_created(self):
        document = {"items": [None]}
        with self.assertRaises(OperationFailure) as error:
            set_document_value(document, "items.0.value", 2)
        self.assertEqual(error.exception.code, 28)
        self.assertEqual(document, {"items": [None]})
        self.assertTrue(set_document_value(document, "items.2.value", 2))
        self.assertEqual(document, {"items": [None, None, {"value": 2}]})

    def test_seed_materializes_mutable_containers_and_preserves_bson_scalars(self):
        source = DocumentCodec.to_internal(
            {"items": {"$eq": [{"value": BsonInt64(1)}]}, "meta.kind": {"name": "a"}}
        )
        candidate = {}
        seed_upsert_document(candidate, source)
        self.assertIsInstance(candidate["items"][0]["value"], BsonInt64)
        candidate["items"][0]["value"] = 2
        candidate["items"].append({"value": 3})
        candidate["meta"]["kind"]["name"] = "b"
        self.assertEqual(source["items"]["$eq"], [{"value": BsonInt64(1)}])
        self.assertEqual(source["meta.kind"], {"name": "a"})

    def test_sql_translation_declines_all_positional_kinds(self):
        for operator in ("$set", "$unset"):
            for segment in ("$", "$[]", "$[i]"):
                with self.subTest(operator=operator, segment=segment):
                    plan = UpdateEngine.compile_update_plan(
                        {operator: {f"items.{segment}": 2}},
                        array_filters=[{"i": 0}] if segment == "$[i]" else None,
                    )
                    with self.assertRaisesRegex(NotImplementedError, "Positional"):
                        translate_compiled_update_plan(plan)
