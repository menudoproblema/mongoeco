import unittest
import re
import math

from mongoeco.compat import MongoDialect
from mongoeco.core.operators import UpdateEngine
from mongoeco.errors import OperationFailure


class UpdateEngineTests(unittest.TestCase):
    def test_update_engine_can_use_custom_dialect_operator_catalog(self):
        class _NoSetDialect(MongoDialect):
            def supports_update_operator(self, name: str) -> bool:
                return False if name == "$set" else super().supports_update_operator(name)

        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update(
                {},
                {"$set": {"field": 1}},
                dialect=_NoSetDialect(key="test", server_version="test", label="No Set"),
            )

    def test_update_engine_can_use_custom_dialect_field_ordering(self):
        class _InsertionFieldOrderDialect(MongoDialect):
            def sort_update_path_items(
                self,
                params: dict[str, object],
            ) -> list[tuple[str, object]]:
                return list(params.items())

        document: dict[str, object] = {}
        UpdateEngine.apply_update(
            document,
            {"$set": {"b": 1, "a": 2}},
            dialect=_InsertionFieldOrderDialect(
                key="test",
                server_version="test",
                label="Insertion Update Order",
            ),
        )

        self.assertEqual(list(document.keys()), ["b", "a"])

    def test_set_none_creates_missing_field(self):
        document = {}

        modified = UpdateEngine.apply_update(document, {"$set": {"field": None}})

        self.assertTrue(modified)
        self.assertEqual(document, {"field": None})

    def test_unknown_operator_raises_operation_failure(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"arr": []}, {"$currentDate": {"updated_at": True}})

    def test_update_engine_rejects_custom_supported_but_unimplemented_operator(self):
        class _FutureUpdateDialect(MongoDialect):
            def supports_update_operator(self, name: str) -> bool:
                return True if name == "$futureUpdate" else super().supports_update_operator(name)

        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update(
                {"arr": []},
                {"$futureUpdate": {"arr": "items"}},
                dialect=_FutureUpdateDialect(
                    key="test",
                    server_version="test",
                    label="Future Update",
                ),
            )

    def test_update_rejects_other_unsupported_update_operators_explicitly(self):
        unsupported_specs = [
            {"$currentDate": {"updated_at": True}},
            {"$setOnInsert": {"created_at": 1}},
            {"$pullAll": {"tags": ["python"]}},
            {"$bit": {"score": {"and": 1}}},
        ]

        for spec in unsupported_specs:
            with self.subTest(spec=spec):
                with self.assertRaises(OperationFailure):
                    UpdateEngine.apply_update({}, spec)

    def test_update_rejects_empty_or_non_document_specs(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"a": 1}, {})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"a": 1}, [])  # type: ignore[arg-type]
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"a": 1}, None)  # type: ignore[arg-type]
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"a": 1}, {"$set": []})  # type: ignore[dict-item]

    def test_set_cannot_modify_immutable_id(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"_id": "old", "name": "Ada"}, {"$set": {"_id": "new"}})

    def test_unset_cannot_modify_immutable_id(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"_id": "old", "name": "Ada"}, {"$unset": {"_id": ""}})

    def test_update_rejects_unsupported_positional_and_array_filter_paths(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"items": [{"qty": 1}]}, {"$set": {"items.$.qty": 2}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"items": [{"qty": 1}]}, {"$set": {"items.$[i].qty": 2}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"items": [{"qty": 1}]}, {"$set": {"items.$[].qty": 2}})

    def test_set_same_value_is_noop(self):
        document = {"field": 1}

        modified = UpdateEngine.apply_update(document, {"$set": {"field": 1}})

        self.assertFalse(modified)
        self.assertEqual(document, {"field": 1})

    def test_set_detaches_inserted_values_from_update_spec(self):
        document: dict[str, object] = {}
        payload = {"nested": {"value": 1}}

        UpdateEngine.apply_update(document, {"$set": {"field": payload}})
        payload["nested"]["value"] = 2

        self.assertEqual(document, {"field": {"nested": {"value": 1}}})

    def test_set_nested_rejects_crossing_non_container_parent(self):
        document = {"profile": "invalid"}

        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update(document, {"$set": {"profile.name": "Ada"}})

    def test_unset_existing_and_missing_fields(self):
        document = {"profile": {"name": "Ada"}, "role": "admin"}

        modified_existing = UpdateEngine.apply_update(document, {"$unset": {"profile.name": ""}})
        modified_missing = UpdateEngine.apply_update(document, {"$unset": {"profile.age": ""}})

        self.assertTrue(modified_existing)
        self.assertFalse(modified_missing)
        self.assertEqual(document, {"profile": {}, "role": "admin"})

    def test_unset_nested_path_returns_false_when_parent_is_missing(self):
        self.assertFalse(UpdateEngine.apply_update({}, {"$unset": {"profile.name": ""}}))

    def test_set_supports_numeric_segments_for_arrays(self):
        document = {"tags": ["old", "keep"]}

        modified = UpdateEngine.apply_update(document, {"$set": {"tags.0": "new"}})

        self.assertTrue(modified)
        self.assertEqual(document, {"tags": ["new", "keep"]})

    def test_update_operators_process_string_and_numeric_field_names_in_stable_order(self):
        document: dict[str, object] = {}

        UpdateEngine.apply_update(document, {"$set": {"b": 1, "a": 2}})
        self.assertEqual(list(document.keys()), ["a", "b"])

        numeric_document: dict[str, object] = {}
        UpdateEngine.apply_update(numeric_document, {"$set": {"10": "x", "2": "y"}})
        self.assertEqual(list(numeric_document.keys()), ["2", "10"])

    def test_unset_supports_numeric_segments_for_arrays(self):
        document = {"tags": ["old", "keep"]}

        modified = UpdateEngine.apply_update(document, {"$unset": {"tags.0": ""}})

        self.assertTrue(modified)
        self.assertEqual(document, {"tags": [None, "keep"]})

    def test_inc_supports_existing_and_missing_numeric_fields(self):
        document = {"count": 1}

        modified_existing = UpdateEngine.apply_update(document, {"$inc": {"count": 2}})
        modified_missing = UpdateEngine.apply_update(document, {"$inc": {"nested.total": 3}})

        self.assertTrue(modified_existing)
        self.assertTrue(modified_missing)
        self.assertEqual(document, {"count": 3, "nested": {"total": 3}})

    def test_inc_rejects_non_numeric_input_or_target(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"count": 1}, {"$inc": {"count": "x"}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"count": "x"}, {"$inc": {"count": 1}})

    def test_min_updates_missing_or_greater_values_using_bson_order(self):
        document = {"score": 10, "typed": "text"}

        changed_existing = UpdateEngine.apply_update(document, {"$min": {"score": 4}})
        changed_missing = UpdateEngine.apply_update(document, {"$min": {"nested.level": 3}})
        changed_type = UpdateEngine.apply_update(document, {"$min": {"typed": 5}})
        unchanged = UpdateEngine.apply_update(document, {"$min": {"score": 7}})

        self.assertTrue(changed_existing)
        self.assertTrue(changed_missing)
        self.assertTrue(changed_type)
        self.assertFalse(unchanged)
        self.assertEqual(document, {"score": 4, "typed": 5, "nested": {"level": 3}})

    def test_max_updates_missing_or_lower_values_using_bson_order(self):
        document = {"score": 10, "typed": None}

        changed_existing = UpdateEngine.apply_update(document, {"$max": {"score": 12}})
        changed_missing = UpdateEngine.apply_update(document, {"$max": {"nested.level": 3}})
        changed_type = UpdateEngine.apply_update(document, {"$max": {"typed": "text"}})
        unchanged = UpdateEngine.apply_update(document, {"$max": {"score": 11}})

        self.assertTrue(changed_existing)
        self.assertTrue(changed_missing)
        self.assertTrue(changed_type)
        self.assertFalse(unchanged)
        self.assertEqual(document, {"score": 12, "typed": "text", "nested": {"level": 3}})

    def test_mul_supports_missing_existing_and_rejects_non_numeric_values(self):
        document = {"count": 3, "ratio": 1.5}

        changed_existing = UpdateEngine.apply_update(document, {"$mul": {"count": 4, "ratio": 2.0}})
        changed_missing = UpdateEngine.apply_update(document, {"$mul": {"missing_int": 3, "missing_float": 1.5}})

        self.assertTrue(changed_existing)
        self.assertTrue(changed_missing)
        self.assertEqual(
            document,
            {"count": 12, "ratio": 3.0, "missing_int": 0, "missing_float": 0.0},
        )

        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"count": 1}, {"$mul": {"count": "x"}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"count": "x"}, {"$mul": {"count": 2}})

    def test_rename_moves_fields_and_removes_existing_target(self):
        document = {"profile": {"name": "Ada"}, "name": "Grace"}

        modified = UpdateEngine.apply_update(
            document,
            {"$rename": {"profile.name": "profile.alias", "name": "display_name"}},
        )

        self.assertTrue(modified)
        self.assertEqual(document, {"profile": {"alias": "Ada"}, "display_name": "Grace"})

    def test_rename_is_noop_for_missing_source_field(self):
        document = {"profile": {"name": "Ada"}}

        modified = UpdateEngine.apply_update(document, {"$rename": {"missing": "alias"}})

        self.assertFalse(modified)
        self.assertEqual(document, {"profile": {"name": "Ada"}})

    def test_rename_rejects_array_paths_and_conflicting_targets(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"items": [{"name": "Ada"}]}, {"$rename": {"items.0.name": "items.0.alias"}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"name": "Ada"}, {"$rename": {"name": "name"}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"profile": {"name": "Ada"}}, {"$rename": {"profile": "profile.name"}})

    def test_push_add_to_set_and_pull_support_array_mutation(self):
        document = {"tags": ["python"]}

        pushed = UpdateEngine.apply_update(document, {"$push": {"tags": "mongodb"}})
        add_duplicate = UpdateEngine.apply_update(document, {"$addToSet": {"tags": "python"}})
        add_new = UpdateEngine.apply_update(document, {"$addToSet": {"tags": "sqlite"}})
        pulled = UpdateEngine.apply_update(document, {"$pull": {"tags": "mongodb"}})

        self.assertTrue(pushed)
        self.assertFalse(add_duplicate)
        self.assertTrue(add_new)
        self.assertTrue(pulled)
        self.assertEqual(document, {"tags": ["python", "sqlite"]})

    def test_push_and_add_to_set_detach_inserted_values_from_update_spec(self):
        push_value = {"kind": "a"}
        add_value = {"kind": "b"}
        document = {"items": []}

        UpdateEngine.apply_update(document, {"$push": {"items": push_value}})
        UpdateEngine.apply_update(document, {"$addToSet": {"items": add_value}})
        push_value["kind"] = "changed"
        add_value["kind"] = "changed"

        self.assertEqual(document, {"items": [{"kind": "a"}, {"kind": "b"}]})

    def test_add_to_set_and_pull_support_embedded_documents(self):
        document = {"items": [{"kind": "a"}]}

        add_duplicate = UpdateEngine.apply_update(document, {"$addToSet": {"items": {"kind": "a"}}})
        add_new = UpdateEngine.apply_update(document, {"$addToSet": {"items": {"kind": "b"}}})
        pull_doc = UpdateEngine.apply_update(document, {"$pull": {"items": {"kind": "a"}}})

        self.assertFalse(add_duplicate)
        self.assertTrue(add_new)
        self.assertTrue(pull_doc)
        self.assertEqual(document, {"items": [{"kind": "b"}]})

    def test_add_to_set_and_pull_honor_custom_dialect_equality(self):
        class _CaseInsensitiveDialect(MongoDialect):
            def values_equal(self, left, right):
                if isinstance(left, str) and isinstance(right, str):
                    return left.lower() == right.lower()
                return super().values_equal(left, right)

        dialect = _CaseInsensitiveDialect(
            key="test",
            server_version="test",
            label="Case Insensitive",
        )
        document = {"tags": ["Ada"]}

        add_duplicate = UpdateEngine.apply_update(
            document,
            {"$addToSet": {"tags": "ada"}},
            dialect=dialect,
        )
        pulled = UpdateEngine.apply_update(
            document,
            {"$pull": {"tags": "ada"}},
            dialect=dialect,
        )

        self.assertFalse(add_duplicate)
        self.assertTrue(pulled)
        self.assertEqual(document, {"tags": []})

    def test_pull_predicate_honors_custom_dialect(self):
        class _CaseInsensitiveDialect(MongoDialect):
            def values_equal(self, left, right):
                if isinstance(left, str) and isinstance(right, str):
                    return left.lower() == right.lower()
                return super().values_equal(left, right)

        dialect = _CaseInsensitiveDialect(
            key="test",
            server_version="test",
            label="Case Insensitive",
        )
        document = {"items": [{"kind": "Ada"}, {"kind": "Grace"}]}

        modified = UpdateEngine.apply_update(
            document,
            {"$pull": {"items": {"kind": "ada"}}},
            dialect=dialect,
        )

        self.assertTrue(modified)
        self.assertEqual(document, {"items": [{"kind": "Grace"}]})

    def test_pull_supports_predicate_documents(self):
        document = {
            "items": [
                {"kind": "a", "qty": 1},
                {"kind": "b", "qty": 3},
                {"kind": "c", "qty": 5},
            ]
        }

        modified = UpdateEngine.apply_update(document, {"$pull": {"items": {"qty": {"$gte": 3}}}})

        self.assertTrue(modified)
        self.assertEqual(document, {"items": [{"kind": "a", "qty": 1}]})

    def test_pull_supports_python_regex_values(self):
        document = {"tags": ["python", "mongodb", "pytest"]}

        modified = UpdateEngine.apply_update(document, {"$pull": {"tags": re.compile(r"^py")}})

        self.assertTrue(modified)
        self.assertEqual(document, {"tags": ["mongodb"]})

    def test_pull_with_plain_document_requires_exact_document_match(self):
        document = {
            "items": [
                {"kind": "a"},
                {"kind": "a", "qty": 1},
                {"kind": "b"},
            ]
        }

        modified = UpdateEngine.apply_update(document, {"$pull": {"items": {"kind": "a"}}})

        self.assertTrue(modified)
        self.assertEqual(document, {"items": [{"kind": "a", "qty": 1}, {"kind": "b"}]})

    def test_pull_does_not_report_modification_when_nan_array_is_unchanged(self):
        document = {"values": [math.nan]}

        modified = UpdateEngine.apply_update(document, {"$pull": {"values": 1}})

        self.assertFalse(modified)
        self.assertEqual(len(document["values"]), 1)
        self.assertTrue(math.isnan(document["values"][0]))

    def test_add_to_set_treats_embedded_documents_with_different_key_order_as_distinct(self):
        document = {"items": [{"kind": "a", "qty": 1}]}

        modified = UpdateEngine.apply_update(
            document,
            {"$addToSet": {"items": {"qty": 1, "kind": "a"}}},
        )

        self.assertTrue(modified)
        self.assertEqual(document, {"items": [{"kind": "a", "qty": 1}, {"qty": 1, "kind": "a"}]})

    def test_add_to_set_and_pull_distinguish_bool_and_int_inside_compound_values(self):
        document = {"items": [{"flag": True}]}

        added = UpdateEngine.apply_update(document, {"$addToSet": {"items": {"flag": 1}}})
        removed = UpdateEngine.apply_update(document, {"$pull": {"items": {"flag": 1}}})

        self.assertTrue(added)
        self.assertTrue(removed)
        self.assertEqual(document, {"items": [{"flag": True}]})

    def test_pull_with_plain_document_respects_embedded_document_key_order(self):
        document = {"items": [{"kind": "a", "qty": 1}, {"qty": 1, "kind": "a"}]}

        modified = UpdateEngine.apply_update(
            document,
            {"$pull": {"items": {"kind": "a", "qty": 1}}},
        )

        self.assertTrue(modified)
        self.assertEqual(document, {"items": [{"qty": 1, "kind": "a"}]})

    def test_push_and_add_to_set_create_missing_arrays(self):
        document = {}

        pushed = UpdateEngine.apply_update(document, {"$push": {"tags": "python"}})
        added = UpdateEngine.apply_update(document, {"$addToSet": {"roles": "admin"}})

        self.assertTrue(pushed)
        self.assertTrue(added)
        self.assertEqual(document, {"tags": ["python"], "roles": ["admin"]})

    def test_push_and_add_to_set_support_each_modifier(self):
        document = {"tags": ["python"], "roles": ["admin"]}

        pushed = UpdateEngine.apply_update(document, {"$push": {"tags": {"$each": ["mongodb", "sqlite"]}}})
        added = UpdateEngine.apply_update(document, {"$addToSet": {"roles": {"$each": ["admin", "staff", "staff"]}}})

        self.assertTrue(pushed)
        self.assertTrue(added)
        self.assertEqual(document, {"tags": ["python", "mongodb", "sqlite"], "roles": ["admin", "staff"]})

    def test_add_to_set_with_each_deduplicates_when_creating_missing_array(self):
        document = {}

        modified = UpdateEngine.apply_update(document, {"$addToSet": {"roles": {"$each": ["admin", "admin", "staff"]}}})

        self.assertTrue(modified)
        self.assertEqual(document, {"roles": ["admin", "staff"]})

    def test_add_to_set_empty_operand_is_noop(self):
        document = {"roles": ["admin"]}

        modified = UpdateEngine.apply_update(document, {"$addToSet": {}})

        self.assertFalse(modified)
        self.assertEqual(document, {"roles": ["admin"]})

    def test_add_to_set_adds_array_as_single_element_and_preserves_existing_duplicates(self):
        document = {"items": [["existing"], ["dup"], ["dup"]]}

        modified = UpdateEngine.apply_update(document, {"$addToSet": {"items": ["new", "values"]}})

        self.assertTrue(modified)
        self.assertEqual(document, {"items": [["existing"], ["dup"], ["dup"], ["new", "values"]]})

    def test_push_and_add_to_set_reject_invalid_each_modifier_payloads(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": []}, {"$push": {"tags": {"$each": "python"}}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": []}, {"$push": {"tags": {"$each": ["python"], "$slice": 1}}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": []}, {"$push": {"tags": {"$each": ["python"], "$position": 0}}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": []}, {"$push": {"tags": {"$each": ["python"], "$sort": 1}}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": []}, {"$addToSet": {"tags": {"$each": "python"}}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": []}, {"$addToSet": {"tags": {"$each": ["python"], "$slice": 1}}})


    def test_pull_missing_array_is_noop_and_non_array_targets_raise(self):
        self.assertFalse(UpdateEngine.apply_update({}, {"$pull": {"tags": "python"}}))
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": "python"}, {"$pull": {"tags": "python"}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": "python"}, {"$push": {"tags": "mongodb"}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": "python"}, {"$addToSet": {"tags": "mongodb"}})

    def test_pull_removes_all_duplicate_matches(self):
        document = {"tags": ["python", "mongodb", "python", "sqlite", "python"]}

        modified = UpdateEngine.apply_update(document, {"$pull": {"tags": "python"}})

        self.assertTrue(modified)
        self.assertEqual(document, {"tags": ["mongodb", "sqlite"]})

    def test_pop_supports_first_and_last_and_handles_missing_or_empty_arrays(self):
        document = {"tags": ["python", "mongodb", "sqlite"], "empty": []}

        pop_first = UpdateEngine.apply_update(document, {"$pop": {"tags": -1}})
        pop_last = UpdateEngine.apply_update(document, {"$pop": {"tags": 1}})
        pop_empty = UpdateEngine.apply_update(document, {"$pop": {"empty": 1}})
        pop_missing = UpdateEngine.apply_update(document, {"$pop": {"missing": 1}})

        self.assertTrue(pop_first)
        self.assertTrue(pop_last)
        self.assertFalse(pop_empty)
        self.assertFalse(pop_missing)
        self.assertEqual(document, {"tags": ["mongodb"], "empty": []})

    def test_pop_supports_single_element_and_nested_array_paths(self):
        document = {
            "tags": ["python"],
            "profile": {"tags": ["mongodb", "sqlite"]},
        }

        pop_single = UpdateEngine.apply_update(document, {"$pop": {"tags": 1}})
        pop_nested = UpdateEngine.apply_update(document, {"$pop": {"profile.tags": -1}})

        self.assertTrue(pop_single)
        self.assertTrue(pop_nested)
        self.assertEqual(document, {"tags": [], "profile": {"tags": ["sqlite"]}})

    def test_inc_supports_nested_paths(self):
        document = {}

        modified = UpdateEngine.apply_update(document, {"$inc": {"stats.daily.count": 2}})

        self.assertTrue(modified)
        self.assertEqual(document, {"stats": {"daily": {"count": 2}}})

    def test_pop_rejects_invalid_direction_and_non_array_targets(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": ["python"]}, {"$pop": {"tags": 0}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": ["python"]}, {"$pop": {"tags": True}})
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"tags": "python"}, {"$pop": {"tags": 1}})

    def test_update_rejects_non_string_field_names(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({}, {"$set": {1: "x"}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            UpdateEngine._iter_ordered_update_items({1: "x"})  # type: ignore[dict-item]

    def test_update_rejects_conflicting_paths(self):
        conflict_specs = [
            {"$set": {"a": {}, "a.b": 1}},
            {"$set": {"a.b": 1, "a": {}}},
            {"$unset": {"a": 1, "a.b": 1}},
            {"$set": {"a": 1}, "$unset": {"a.b": 1}},
            {"$set": {"a": 1}, "$inc": {"a.b": 1}},
            {"$inc": {"a.b": 1}, "$set": {"a": 1}},
        ]

        for spec in conflict_specs:
            with self.subTest(spec=spec):
                with self.assertRaises(OperationFailure):
                    UpdateEngine.apply_update({}, spec)
