import datetime
import unittest
import uuid

from mongoeco.core._filtering_support import (
    compile_regex,
    extract_all_candidates,
    extract_values,
    get_field_value,
    hashable_in_lookup_key,
    path_mapping,
    split_path,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import DBRef, ObjectId, Timestamp


class FilteringSupportTests(unittest.TestCase):
    def test_split_path_and_compile_regex_validate_inputs(self):
        self.assertEqual(split_path("a.b.0"), ("a", "b", "0"))
        self.assertEqual(split_path(""), ())

        pattern = compile_regex("^a", "im")
        self.assertTrue(pattern.match("Ada"))
        self.assertIs(pattern, compile_regex("^a", "im"))

        with self.assertRaisesRegex(OperationFailure, "Unsupported regex option"):
            compile_regex("^a", "q")
        with self.assertRaisesRegex(OperationFailure, "Duplicate regex option"):
            compile_regex("^a", "ii")

    def test_hashable_lookup_key_supports_bson_scalars(self):
        object_id = ObjectId("000000000000000000000123")
        stamp = Timestamp(5, 7)
        token = uuid.uuid4()
        moment = datetime.datetime(2026, 4, 1, 12, 0, tzinfo=datetime.timezone.utc)

        self.assertEqual(hashable_in_lookup_key(None), ("none", None))
        self.assertEqual(hashable_in_lookup_key(True), ("bool", True))
        self.assertEqual(hashable_in_lookup_key(b"abc"), ("bytes", b"abc"))
        self.assertEqual(hashable_in_lookup_key(moment), ("date", moment))
        self.assertEqual(hashable_in_lookup_key(token), ("uuid", token))
        self.assertEqual(hashable_in_lookup_key(object_id), ("objectid", object_id))
        self.assertEqual(hashable_in_lookup_key(stamp), ("timestamp", stamp))
        self.assertIsNone(hashable_in_lookup_key({"unsupported": True}))

    def test_path_helpers_support_dbref_and_nested_arrays(self):
        document = {
            "author": DBRef(
                "users",
                "ada",
                database="observe",
                extras={"meta": {"region": "eu"}},
            ),
            "authors": [
                DBRef("users", "ada"),
                DBRef("users", "grace", extras={"tenant": "t2"}),
            ],
            "items": [{"tags": ["a", "b"]}, {"tags": ["c"]}],
        }

        self.assertEqual(path_mapping(document["author"])["$id"], "ada")
        self.assertEqual(extract_values(document, "author.$db"), ["observe"])
        self.assertEqual(extract_values(document, "author.meta.region"), ["eu"])
        self.assertEqual(extract_values(document, "authors.$id"), ["ada", "grace"])
        self.assertEqual(extract_values(document, "items.tags"), [["a", "b"], "a", "b", ["c"], "c"])

        self.assertEqual(get_field_value(document, "author.$ref"), (True, "users"))
        self.assertEqual(get_field_value(document, "items.1.tags.0"), (True, "c"))
        self.assertEqual(get_field_value(document, "items.tags"), (False, None))

        self.assertEqual(extract_all_candidates(document, "authors.tenant"), ["t2", "t2"])
        self.assertEqual(extract_all_candidates(document, "items.tags"), ["a", "b", "c", "a", "b", "c"])
        self.assertEqual(extract_all_candidates(document, ""), [])
