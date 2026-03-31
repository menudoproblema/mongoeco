import decimal
import unittest
import uuid
from unittest.mock import patch

from bson import Decimal128
from bson.binary import Binary, STANDARD
from bson.dbref import DBRef as BsonDBRef
from bson.int64 import Int64
from bson.objectid import ObjectId as BsonObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp as BsonTimestamp

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.types import Binary as MongoecoBinary, DBRef, Decimal128 as MongoecoDecimal128, ObjectId, Regex as MongoecoRegex, SON, Timestamp, UNDEFINED
from mongoeco.wire.bson_bridge import decode_wire_value, encode_wire_value


class WireBsonBridgeTests(unittest.TestCase):
    def test_decode_wire_value_handles_special_bson_types(self):
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        decoded = decode_wire_value(
            {
                "_id": BsonObjectId("507f1f77bcf86cd799439011"),
                "long": Int64(9),
                "decimal": Decimal128("12.5"),
                "uuid": Binary.from_uuid(session_id, uuid_representation=STANDARD),
                "regex": Regex("^ab", "im"),
                "timestamp": BsonTimestamp(12, 3),
                "dbref": BsonDBRef("users", "ada", "observe", tenant="t1"),
            }
        )

        self.assertEqual(decoded["_id"], ObjectId("507f1f77bcf86cd799439011"))
        self.assertEqual(decoded["long"], 9)
        self.assertEqual(decoded["decimal"], MongoecoDecimal128("12.5"))
        self.assertEqual(decoded["uuid"], session_id)
        self.assertEqual(decoded["regex"], MongoecoRegex("^ab", "im"))
        self.assertEqual(decoded["timestamp"], Timestamp(12, 3))
        self.assertEqual(decoded["dbref"], DBRef("users", "ada", database="observe", extras={"tenant": "t1"}))

    def test_encode_wire_value_handles_internal_special_values(self):
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        encoded = encode_wire_value(
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "int32": BsonInt32(4),
                "int64": BsonInt64(5),
                "double": BsonDouble(1.25),
                "decimal": BsonDecimal128(decimal.Decimal("7.5")),
                "uuid": session_id,
                "payload": b"abc",
                "typed_payload": MongoecoBinary(b"xyz", subtype=4),
                "regex": MongoecoRegex("^ab", "im"),
                "timestamp": Timestamp(12, 3),
                "public_decimal": MongoecoDecimal128("8.5"),
                "ref": DBRef("users", "ada", database="observe", extras={"tenant": "t1"}),
                "son": SON([("b", 2), ("a", 1)]),
                "undefined": UNDEFINED,
            }
        )

        self.assertEqual(str(encoded["_id"]), "507f1f77bcf86cd799439011")
        self.assertEqual(encoded["int32"], 4)
        self.assertEqual(encoded["int64"], Int64(5))
        self.assertEqual(encoded["double"], 1.25)
        self.assertEqual(encoded["decimal"], Decimal128("7.5"))
        self.assertEqual(encoded["uuid"].as_uuid(uuid_representation=STANDARD), session_id)
        self.assertEqual(bytes(encoded["payload"]), b"abc")
        self.assertEqual(bytes(encoded["typed_payload"]), b"xyz")
        self.assertEqual(encoded["typed_payload"].subtype, 4)
        self.assertEqual(encoded["regex"], Regex("^ab", "im"))
        self.assertEqual(encoded["timestamp"], BsonTimestamp(12, 3))
        self.assertEqual(encoded["public_decimal"], Decimal128("8.5"))
        self.assertEqual(encoded["ref"], BsonDBRef("users", "ada", "observe", tenant="t1"))
        self.assertEqual(list(encoded["son"].items()), [("b", 2), ("a", 1)])
        self.assertIsNone(encoded["undefined"])

    def test_decode_wire_value_propagates_unexpected_uuid_decoding_errors(self):
        payload = Binary.from_uuid(
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
            uuid_representation=STANDARD,
        )

        with patch.object(Binary, "as_uuid", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                decode_wire_value(payload)
