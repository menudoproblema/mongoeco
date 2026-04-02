import unittest

from mongoeco.api.admin_parsing import (
    normalize_command_batch_size,
    normalize_command_bool,
    normalize_command_document,
    normalize_command_hint,
    normalize_command_max_time_ms,
    normalize_command_ordered,
    normalize_command_projection,
    normalize_command_scale,
    normalize_command_sort_document,
    normalize_delete_specs,
    normalize_filter_document,
    normalize_find_and_modify_options,
    normalize_index_models_from_command,
    normalize_insert_documents,
    normalize_list_databases_options,
    normalize_namespace,
    normalize_update_specs,
    require_collection_name,
    resolve_collection_reference,
)
from mongoeco.errors import OperationFailure


class _CollectionRef:
    name = "users"


class _BadCollectionRef:
    name = ""


class AdminParsingTests(unittest.TestCase):
    def test_command_document_and_collection_reference_normalizers_validate_input(self):
        self.assertEqual(
            normalize_command_document("buildInfo", {"comment": "trace"}),
            {"buildInfo": 1, "comment": "trace"},
        )

        command = {"ping": 1}
        self.assertIs(normalize_command_document(command, {}), command)
        self.assertEqual(require_collection_name("users", "collection"), "users")
        self.assertEqual(
            resolve_collection_reference(_CollectionRef(), "collection"),
            "users",
        )

        with self.assertRaises(TypeError):
            normalize_command_document("", {})
        with self.assertRaises(TypeError):
            normalize_command_document({}, {})
        with self.assertRaises(TypeError):
            normalize_command_document({"ping": 1}, {"comment": "trace"})
        with self.assertRaises(TypeError):
            require_collection_name("", "collection")
        with self.assertRaises(TypeError):
            resolve_collection_reference(_BadCollectionRef(), "collection")

    def test_index_models_from_command_preserve_supported_options_and_reject_invalid_specs(self):
        models = normalize_index_models_from_command(
            [
                {
                    "key": {"email": 1},
                    "name": "email_1",
                    "unique": True,
                    "sparse": True,
                    "partialFilterExpression": {"active": True},
                }
            ]
        )

        self.assertEqual(len(models), 1)
        model = models[0]
        self.assertEqual(model.name, "email_1")
        self.assertTrue(model.unique)
        self.assertTrue(model.sparse)
        self.assertEqual(model.partial_filter_expression, {"active": True})
        ttl_models = normalize_index_models_from_command(
            [{"key": {"expires_at": 1}, "expireAfterSeconds": 60}]
        )
        self.assertEqual(ttl_models[0].expire_after_seconds, 60)

        with self.assertRaises(TypeError):
            normalize_index_models_from_command("email_1")
        with self.assertRaises(TypeError):
            normalize_index_models_from_command([1])
        with self.assertRaises(OperationFailure):
            normalize_index_models_from_command([{"name": "email_1"}])
        with self.assertRaises(TypeError):
            normalize_index_models_from_command(
                [{"key": {"email": 1}, "unsupported": True}]
            )

    def test_namespace_and_write_spec_normalizers_accept_valid_lists_and_reject_invalid_shapes(self):
        documents = [{"_id": "1"}]
        updates = [{"q": {"_id": "1"}, "u": {"$set": {"name": "Ada"}}}]
        deletes = [{"q": {"_id": "1"}, "limit": 1}]

        self.assertEqual(normalize_namespace("db.users", "ns"), ("db", "users"))
        self.assertIs(normalize_insert_documents(documents), documents)
        self.assertIs(normalize_update_specs(updates), updates)
        self.assertIs(normalize_delete_specs(deletes), deletes)

        with self.assertRaises(TypeError):
            normalize_namespace("users", "ns")
        with self.assertRaises(TypeError):
            normalize_namespace("db.", "ns")
        with self.assertRaises(TypeError):
            normalize_insert_documents([])
        with self.assertRaises(TypeError):
            normalize_update_specs([1])
        with self.assertRaises(TypeError):
            normalize_delete_specs([1])

    def test_list_databases_and_filter_normalizers_default_and_validate(self):
        options = normalize_list_databases_options(
            {
                "nameOnly": True,
                "filter": {"name": "analytics"},
            }
        )

        self.assertTrue(options.name_only)
        self.assertEqual(options.filter_spec, {"name": "analytics"})
        self.assertEqual(normalize_filter_document(None), {})
        self.assertEqual(normalize_filter_document({"active": True}), {"active": True})

        with self.assertRaises(TypeError):
            normalize_filter_document(["bad"])

    def test_command_helper_normalizers_cover_sort_hint_projection_batch_and_find_and_modify(self):
        self.assertEqual(normalize_command_sort_document({"name": 1}), [("name", 1)])
        self.assertEqual(normalize_command_hint({"name": 1}), [("name", 1)])
        self.assertEqual(normalize_command_projection({"name": 1}), {"name": 1})
        self.assertEqual(normalize_command_batch_size(5), 5)
        self.assertTrue(normalize_command_ordered(None))
        self.assertFalse(normalize_command_bool(None, "flag", default=False))
        self.assertEqual(normalize_command_scale(None), 1)
        self.assertEqual(normalize_command_max_time_ms(25), 25)

        options = normalize_find_and_modify_options(
            {
                "findAndModify": "users",
                "query": {"name": "Ada"},
                "sort": {"name": 1},
                "fields": {"name": 1},
                "remove": False,
                "new": True,
                "upsert": True,
                "bypassDocumentValidation": False,
                "arrayFilters": [{"x": 1}],
                "hint": {"name": 1},
                "maxTimeMS": 50,
                "let": {"tenant": "a"},
                "comment": "trace",
                "update": {"$set": {"rank": 1}},
                "collation": {"locale": "en"},
            }
        )
        self.assertEqual(options.collection_name, "users")
        self.assertEqual(options.sort, [("name", 1)])
        self.assertEqual(options.hint, [("name", 1)])
        self.assertTrue(options.return_new)
        self.assertTrue(options.upsert)

        with self.assertRaises(TypeError):
            normalize_command_sort_document([])
        with self.assertRaises(TypeError):
            normalize_command_hint(True)
        with self.assertRaises(TypeError):
            normalize_command_projection([])
        self.assertEqual(normalize_command_batch_size(0), 0)
        with self.assertRaises(TypeError):
            normalize_command_ordered("yes")
        with self.assertRaises(TypeError):
            normalize_command_bool("yes", "flag")
        with self.assertRaises(TypeError):
            normalize_command_scale(0)
        with self.assertRaises(TypeError):
            normalize_command_max_time_ms("slow")
        with self.assertRaises(TypeError):
            normalize_find_and_modify_options({"findAndModify": "users", "arrayFilters": ["bad"]})
        with self.assertRaises(TypeError):
            normalize_find_and_modify_options({"findAndModify": "users", "let": []})
        with self.assertRaises(TypeError):
            normalize_find_and_modify_options({"findAndModify": "users", "collation": []})
        with self.assertRaises(TypeError):
            normalize_find_and_modify_options({"findAndModify": "users", "bypassDocumentValidation": 1})
        pipeline_options = normalize_find_and_modify_options(
            {"findAndModify": "users", "update": [{"$set": {"rank": 1}}]}
        )
        self.assertEqual(pipeline_options.update_spec, [{"$set": {"rank": 1}}])


if __name__ == "__main__":
    unittest.main()
