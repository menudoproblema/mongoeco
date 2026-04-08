import datetime
import decimal
import unittest
import uuid
from unittest.mock import patch

try:
    from bson.code import Code as BsonCode
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
    from bson.objectid import ObjectId as BsonObjectId
except Exception:  # pragma: no cover - optional dependency
    BsonCode = None
    BsonMaxKey = None
    BsonMinKey = None
    BsonObjectId = None

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.core.codec import DocumentCodec
from mongoeco.types import (
    Binary,
    CodecOptions,
    DBRef,
    Decimal128,
    ObjectId,
    Regex,
    SON,
    Timestamp,
    UNDEFINED,
    UuidRepresentation,
)


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

    def test_document_codec_coerces_tuple_and_bytearray_to_bson_compatible_types(self):
        original = {"items": (1, 2, 3), "payload": bytearray(b"\x00\x01\xff")}

        decoded = DocumentCodec.decode(DocumentCodec.encode(original))

        self.assertEqual(decoded, {"items": [1, 2, 3], "payload": b"\x00\x01\xff"})

    def test_document_codec_rejects_non_string_keys_and_set_values(self):
        with self.assertRaisesRegex(TypeError, "keys must be strings"):
            DocumentCodec.encode({1: "value"})  # type: ignore[dict-item]

        with self.assertRaisesRegex(TypeError, "set values are not BSON-serializable"):
            DocumentCodec.encode({"items": {1, 2}})
        with self.assertRaisesRegex(TypeError, "set values are not BSON-serializable"):
            DocumentCodec.encode(frozenset({1, 2}))

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

    def test_document_codec_round_trip_restores_pymongo_code_and_minmax(self):
        if BsonCode is None or BsonMinKey is None or BsonMaxKey is None:
            self.skipTest("bson is not installed")

        original = {
            "code": BsonCode("function() { return tenant; }", {"tenant": Binary(b"t1")}),
            "min": BsonMinKey(),
            "max": BsonMaxKey(),
        }

        decoded = DocumentCodec.decode(DocumentCodec.encode(original))

        self.assertEqual(decoded["code"], original["code"])
        self.assertEqual(decoded["code"].scope, original["code"].scope)
        self.assertEqual(decoded["min"], original["min"])
        self.assertEqual(decoded["max"], original["max"])

    def test_document_codec_decode_reports_missing_optional_bson_support(self):
        with patch("mongoeco.core.codec.BsonMinKey", None):
            with self.assertRaisesRegex(ValueError, "MinKey requires bson support"):
                DocumentCodec.decode({"$mongoeco": {"type": "minkey", "value": None}})
        with patch("mongoeco.core.codec.BsonMaxKey", None):
            with self.assertRaisesRegex(ValueError, "MaxKey requires bson support"):
                DocumentCodec.decode({"$mongoeco": {"type": "maxkey", "value": None}})
        with patch("mongoeco.core.codec.BsonCode", None):
            with self.assertRaisesRegex(ValueError, "Code requires bson support"):
                DocumentCodec.decode({"$mongoeco": {"type": "code", "value": {"code": "return 1", "scope": None}}})

    def test_document_codec_to_public_copy_on_write_covers_flat_list_changes(self):
        payload = {"items": [BsonInt32(1), 2], "meta": "x"}
        public = DocumentCodec.to_public(payload)
        self.assertEqual(public, {"items": [1, 2], "meta": "x"})
        self.assertIsNot(public, payload)

    def test_apply_codec_options_supports_tz_aware_document_class_and_type_registry(self):
        class AuditDocument(dict):
            pass

        aware_datetime = datetime.datetime(
            2026,
            4,
            8,
            10,
            30,
            tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
        )
        options = CodecOptions(
            document_class=AuditDocument,
            tz_aware=True,
            tzinfo=datetime.timezone.utc,
            type_registry={str: lambda value: value.upper()},
        )

        materialized = DocumentCodec.apply_codec_options(
            {
                "_id": "a1",
                "created_at": aware_datetime,
                "nested": {"owner": "ada"},
            },
            codec_options=options,
        )

        self.assertIsInstance(materialized, AuditDocument)
        self.assertIsInstance(materialized["nested"], AuditDocument)
        self.assertEqual(materialized["_id"], "A1")
        self.assertEqual(materialized["nested"]["owner"], "ADA")
        self.assertEqual(
            materialized["created_at"],
            datetime.datetime(2026, 4, 8, 8, 30, tzinfo=datetime.timezone.utc),
        )

    def test_apply_codec_options_supports_uuid_representation_variants(self):
        value = uuid.UUID("12345678-1234-5678-1234-567812345678")

        legacy = DocumentCodec.apply_codec_options(
            {"session": value},
            codec_options=CodecOptions(uuid_representation=UuidRepresentation.PYTHON_LEGACY),
        )
        unspecified = DocumentCodec.apply_codec_options(
            {"session": value},
            codec_options=CodecOptions(uuid_representation=UuidRepresentation.UNSPECIFIED),
        )
        standard = DocumentCodec.apply_codec_options(
            {"session": value},
            codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD),
        )

        self.assertEqual(legacy["session"], Binary(value.bytes, subtype=3))
        self.assertEqual(unspecified["session"], Binary(value.bytes, subtype=4))
        self.assertEqual(standard["session"], value)

    def test_apply_codec_options_returns_input_when_options_are_none(self):
        payload = {"_id": "a1", "nested": {"value": 1}}

        self.assertIs(DocumentCodec.apply_codec_options(payload, codec_options=None), payload)

    def test_apply_codec_options_supports_tuple_values_and_naive_datetime_tz_projection(self):
        options = CodecOptions(
            tz_aware=True,
            tzinfo=datetime.timezone(datetime.timedelta(hours=1)),
        )
        payload = {
            "items": (
                datetime.datetime(2026, 4, 8, 8, 0, 0),
                "x",
            )
        }

        materialized = DocumentCodec.apply_codec_options(payload, codec_options=options)

        self.assertIsInstance(materialized["items"], tuple)
        self.assertEqual(
            materialized["items"][0],
            datetime.datetime(2026, 4, 8, 9, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=1))),
        )
