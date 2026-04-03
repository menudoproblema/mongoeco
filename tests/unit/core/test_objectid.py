import unittest
import time
import builtins
import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

from mongoeco.types import DeleteResult, InsertManyResult, InsertOneResult, ObjectId, UNDEFINED, UndefinedType, UpdateResult
try:
    from bson.objectid import ObjectId as BsonObjectId
except Exception:  # pragma: no cover - optional dependency
    BsonObjectId = None

class TestObjectId(unittest.TestCase):
    def test_generation_is_unique(self):
        oid1 = ObjectId()
        oid2 = ObjectId()
        self.assertNotEqual(oid1, oid2)
        self.assertIsInstance(oid1, ObjectId)

    def test_string_representation(self):
        oid = ObjectId()
        s = str(oid)
        self.assertEqual(len(s), 24)
        self.assertTrue(all(c in "0123456789abcdef" for c in s))
        
        # Round-trip
        oid2 = ObjectId(s)
        self.assertEqual(oid, oid2)

    def test_is_valid(self):
        self.assertTrue(ObjectId.is_valid("507f1f77bcf86cd799439011"))
        self.assertTrue(ObjectId.is_valid(ObjectId()))
        self.assertTrue(ObjectId.is_valid(b"123456789012"))
        self.assertFalse(ObjectId.is_valid(b"short"))
        self.assertFalse(ObjectId.is_valid("invalid-oid"))
        self.assertFalse(ObjectId.is_valid(123))

    def test_generation_time(self):
        before = int(time.time())
        oid = ObjectId()
        after = int(time.time())
        
        self.assertTrue(before <= oid.generation_time <= after)

    def test_ordering(self):
        oid1 = ObjectId()
        time.sleep(0.01) # Asegurar que el timestamp o contador avance
        oid2 = ObjectId()
        self.assertTrue(oid1 < oid2)

    def test_invalid_init(self):
        with self.assertRaises(ValueError):
            ObjectId("short")
        with self.assertRaises(ValueError):
            ObjectId(b"short")
        with self.assertRaises(ValueError):
            ObjectId("z" * 24)
        with self.assertRaises(TypeError):
            ObjectId(123)

    def test_supports_copy_bytes_repr_and_binary(self):
        original = ObjectId()
        copied = ObjectId(original)
        from_bytes = ObjectId(original.binary)

        self.assertEqual(copied, original)
        self.assertEqual(from_bytes, original)
        self.assertEqual(from_bytes.binary, original.binary)
        self.assertEqual(repr(original), f"ObjectId('{original}')")

    def test_is_compatible_with_pymongo_objectid_constructor_when_available(self):
        if BsonObjectId is None:
            self.skipTest("bson is not installed")

        original = ObjectId()
        converted = BsonObjectId(original)

        self.assertEqual(str(converted), str(original))
        self.assertIsInstance(original, BsonObjectId)

    def test_equality_and_ordering_with_other_types(self):
        oid = ObjectId()

        self.assertNotEqual(oid, "not-an-objectid")
        self.assertIs(oid.__lt__("not-an-objectid"), NotImplemented)

    def test_result_types_default_to_acknowledged(self):
        self.assertTrue(InsertOneResult(inserted_id="x").acknowledged)
        self.assertTrue(InsertManyResult(inserted_ids=["x", "y"]).acknowledged)
        self.assertTrue(UpdateResult(matched_count=1, modified_count=1).acknowledged)
        self.assertTrue(DeleteResult(deleted_count=1).acknowledged)

    def test_result_types_use_slots(self):
        self.assertFalse(hasattr(InsertOneResult(inserted_id="x"), "__dict__"))
        self.assertFalse(hasattr(InsertManyResult(inserted_ids=["x"]), "__dict__"))
        self.assertFalse(hasattr(UpdateResult(matched_count=1, modified_count=0), "__dict__"))
        self.assertFalse(hasattr(DeleteResult(deleted_count=1), "__dict__"))

    def test_undefined_singleton_has_stable_repr_hash_and_equality(self):
        self.assertEqual(repr(UNDEFINED), "UNDEFINED")
        self.assertEqual(hash(UNDEFINED), hash(UndefinedType))
        self.assertEqual(UNDEFINED, UndefinedType())

    def test_fallback_objectid_implementation_behaves_without_bson_dependency(self):
        module_path = Path(__file__).resolve().parents[3] / "src" / "mongoeco" / "_types" / "_bson_objectid.py"
        module_name = "mongoeco._types._bson_objectid_fallback_test"
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        self.assertIsNotNone(spec)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)

        original_import = builtins.__import__

        def _import_without_bson(name, globals=None, locals=None, fromlist=(), level=0):
            if name.startswith("bson"):
                raise ImportError("blocked for fallback test")
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=_import_without_bson):
            sys.modules.pop(module_name, None)
            spec.loader.exec_module(module)

        FallbackObjectId = module.ObjectId
        generated = FallbackObjectId()
        copied = FallbackObjectId(generated)
        from_text = FallbackObjectId(str(generated))
        from_bytes = FallbackObjectId(generated.binary)

        self.assertEqual(copied, generated)
        self.assertEqual(from_text, generated)
        self.assertEqual(from_bytes, generated)
        self.assertEqual(from_bytes.binary, generated.binary)
        self.assertEqual(repr(generated), f"ObjectId('{generated}')")
        self.assertTrue(FallbackObjectId.is_valid(generated))
        self.assertTrue(FallbackObjectId.is_valid(generated.binary))
        self.assertTrue(FallbackObjectId.is_valid(str(generated)))
        self.assertFalse(FallbackObjectId.is_valid("short"))
        self.assertFalse(FallbackObjectId.is_valid(123))
        self.assertFalse(FallbackObjectId("0" * 24) == "other")
        self.assertIs(FallbackObjectId("0" * 24).__lt__("other"), NotImplemented)
        self.assertEqual(FallbackObjectId("0" * 24).generation_time, 0)
        self.assertEqual(hash(FallbackObjectId("0" * 24)), hash(FallbackObjectId("0" * 24)))
        self.assertTrue(FallbackObjectId("0" * 24) < FallbackObjectId("1" * 24))

        normalized_same = module.normalize_object_id(generated)
        normalized_other = module.normalize_object_id(str(generated))
        self.assertIs(normalized_same, generated)
        self.assertEqual(normalized_other, str(generated))
        self.assertEqual(module.normalize_object_id("plain"), "plain")
        self.assertTrue(module.is_object_id_like(generated))

        with self.assertRaises(ValueError):
            FallbackObjectId(b"short")
        with self.assertRaises(ValueError):
            FallbackObjectId("short")
        with self.assertRaises(ValueError):
            FallbackObjectId("z" * 24)
        with self.assertRaises(TypeError):
            FallbackObjectId(123)
