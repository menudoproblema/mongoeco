import datetime
import decimal
import unittest
import uuid

try:
    from bson.objectid import ObjectId as BsonObjectId
except Exception:  # pragma: no cover - optional dependency
    BsonObjectId = None

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.core.codec import DocumentCodec
from mongoeco.types import Binary, DBRef, Decimal128, ObjectId, Regex, SON, Timestamp, UNDEFINED


class DocumentCodecTests(unittest.TestCase):
    def test_document_codec_round_trip_restores_nested_special_types(self):
        original = {
            "created_at": datetime.datetime(2026, 3, 23, 12, 34, 56),
            "owner_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "nested": [
                {"updated_at": datetime.datetime(2026, 3, 23, 18, 0, 0)},
                {"member_id": uuid.UUID("87654321-4321-8765-4321-876543218765")},
            ],
        }

        encoded = DocumentCodec.encode(original)
        decoded = DocumentCodec.decode(encoded)

        self.assertEqual(decoded, original)
        self.assertIsNot(decoded, original)
        self.assertIsNot(decoded["nested"][0], original["nested"][0])

    def test_document_codec_decode_copies_flat_scalar_lists(self):
        payload = {"name": "Ada", "tags": ["python", "mongo"]}

        decoded = DocumentCodec.decode(payload)

        self.assertEqual(decoded, payload)
        self.assertIsNot(decoded, payload)
        self.assertIsNot(decoded["tags"], payload["tags"])

    def test_document_codec_does_not_mutate_original_input(self):
        original = {"items": [{"when": datetime.datetime(2026, 3, 23, 12, 0, 0)}]}

        encoded = DocumentCodec.encode(original)

        self.assertIsInstance(original["items"][0]["when"], datetime.datetime)
        self.assertNotEqual(encoded, original)

    def test_document_codec_does_not_confuse_user_dicts_with_internal_tags(self):
        original = {
            "$mongoeco": {"type": "datetime", "value": "user-data"},
            "legacy_date": {"$date": "2026-03-23T12:00:00"},
            "nested": [{"$uuid": "not-a-real-uuid"}],
        }

        encoded = DocumentCodec.encode(original)
        decoded = DocumentCodec.decode(encoded)

        self.assertEqual(decoded, original)

    def test_document_codec_can_round_trip_tagged_user_dict_inside_document(self):
        original = {
            "meta": {
                "$mongoeco": {
                    "type": "objectid",
                    "value": "507f1f77bcf86cd799439011",
                }
            }
        }

        self.assertEqual(DocumentCodec.decode(DocumentCodec.encode(original)), original)

    def test_document_codec_rejects_unknown_internal_tag_type(self):
        with self.assertRaises(ValueError):
            DocumentCodec.decode({"$mongoeco": {"type": "mystery", "value": "x"}})

    def test_document_codec_round_trip_restores_bytes(self):
        original = {"_id": b"123456789012", "payload": {"blob": b"\x00\x01\xff"}}

        decoded = DocumentCodec.decode(DocumentCodec.encode(original))

        self.assertEqual(decoded, original)

    def test_document_codec_round_trip_restores_undefined(self):
        original = {"value": UNDEFINED, "items": [1, UNDEFINED]}

        decoded = DocumentCodec.decode(DocumentCodec.encode(original))

        self.assertIs(decoded["value"], UNDEFINED)
        self.assertIs(decoded["items"][1], UNDEFINED)

    def test_document_codec_round_trip_restores_decimal(self):
        original = {"amount": decimal.Decimal("10.25")}

        decoded = DocumentCodec.decode(DocumentCodec.encode(original))

        self.assertEqual(decoded, original)

    def test_document_codec_rejects_ints_outside_bson_int64_range(self):
        with self.assertRaises(OverflowError):
            DocumentCodec.encode({"value": 1 << 80})

    def test_document_codec_round_trip_restores_explicit_bson_numeric_wrappers(self):
        original = {
            "i32": BsonInt32(1),
            "i64": BsonInt64(1 << 40),
            "double": BsonDouble(1.5),
            "decimal128": BsonDecimal128(decimal.Decimal("99.125")),
        }

        decoded = DocumentCodec.decode(DocumentCodec.encode(original))

        self.assertEqual(
            decoded,
            {
                "i32": 1,
                "i64": 1 << 40,
                "double": 1.5,
                "decimal128": decimal.Decimal("99.125"),
            },
        )

    def test_document_codec_can_preserve_bson_numeric_wrappers_for_internal_use(self):
        encoded = DocumentCodec.encode(
            {
                "i32": BsonInt32(1),
                "i64": BsonInt64(1 << 40),
                "double": BsonDouble(1.5),
                "decimal128": BsonDecimal128(decimal.Decimal("99.125")),
            }
        )

        decoded = DocumentCodec.decode(encoded, preserve_bson_wrappers=True)

        self.assertEqual(
            decoded,
            {
                "i32": BsonInt32(1),
                "i64": BsonInt64(1 << 40),
                "double": BsonDouble(1.5),
                "decimal128": BsonDecimal128(decimal.Decimal("99.125")),
            },
        )

    def test_document_codec_to_public_unwraps_nested_bson_numeric_wrappers(self):
        value = {
            "i32": BsonInt32(1),
            "items": [BsonInt64(2), {"double": BsonDouble(1.5)}],
        }

        self.assertEqual(
            DocumentCodec.to_public(value),
            {"i32": 1, "items": [2, {"double": 1.5}]},
        )

    def test_document_codec_decode_restores_dict_wrapper_payload(self):
        payload = {
            "$mongoeco": {
                "type": "dict",
                "value": {"nested": {"$mongoeco": {"type": "double", "value": "1.5"}}},
            }
        }

        self.assertEqual(DocumentCodec.decode(payload), {"nested": 1.5})

    def test_document_codec_round_trip_restores_public_bson_types(self):
        original = {
            "bin": Binary(b"\x00\x01\x02", subtype=4),
            "regex": Regex("^ad", "im"),
            "ts": Timestamp(1234567890, 7),
            "decimal128": Decimal128("10.25"),
            "son": SON([("b", 2), ("a", 1)]),
            "ref": DBRef("users", "ada", database="observe", extras={"tenant": "t1"}),
        }

        decoded = DocumentCodec.decode(DocumentCodec.encode(original))

        self.assertEqual(decoded["bin"], original["bin"])
        self.assertEqual(decoded["bin"].subtype, 4)
        self.assertEqual(decoded["regex"], original["regex"])
        self.assertEqual(decoded["ts"], original["ts"])
        self.assertEqual(decoded["decimal128"], original["decimal128"])
        self.assertEqual(list(decoded["son"].items()), [("b", 2), ("a", 1)])
        self.assertEqual(decoded["ref"], original["ref"])

    def test_document_codec_normalizes_pymongo_objectids_when_available(self):
        if BsonObjectId is None:
            self.skipTest("bson is not installed")

        original = {
            "_id": BsonObjectId("507f1f77bcf86cd799439011"),
            "tasks": [{"enrollment_task_id": BsonObjectId("65f0a1000000000000000000")}],
        }

        encoded = DocumentCodec.encode(original)
        decoded = DocumentCodec.decode(encoded)

        self.assertEqual(
            encoded,
            {
                "_id": {"$mongoeco": {"type": "objectid", "value": "507f1f77bcf86cd799439011"}},
                "tasks": [
                    {
                        "enrollment_task_id": {
                            "$mongoeco": {
                                "type": "objectid",
                                "value": "65f0a1000000000000000000",
                            }
                        }
                    }
                ],
            },
        )
        self.assertEqual(
            decoded,
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "tasks": [{"enrollment_task_id": ObjectId("65f0a1000000000000000000")}],
            },
        )

    def test_document_codec_decimal128_public_decode_applies_decimal128_context(self):
        decoded = DocumentCodec.decode(
            {
                "$mongoeco": {
                    "type": "decimal128_public",
                    "value": "1.2345678901234567890123456789012345",
                }
            }
        )

        self.assertEqual(str(decoded), "1.234567890123456789012345678901234")
