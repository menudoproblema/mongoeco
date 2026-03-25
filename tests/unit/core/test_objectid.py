import unittest
import time

from mongoeco.types import DeleteResult, InsertManyResult, InsertOneResult, ObjectId, UNDEFINED, UndefinedType, UpdateResult

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
