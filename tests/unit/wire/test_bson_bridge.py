import decimal
import re
import unittest
import uuid

from bson import Decimal128
from bson.binary import Binary, STANDARD
from bson.int64 import Int64
from bson.objectid import ObjectId as BsonObjectId
from bson.regex import Regex

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.types import ObjectId, UNDEFINED
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
            }
        )

        self.assertEqual(decoded["_id"], ObjectId("507f1f77bcf86cd799439011"))
        self.assertEqual(decoded["long"], 9)
        self.assertEqual(decoded["decimal"], decimal.Decimal("12.5"))
        self.assertEqual(decoded["uuid"], session_id)
        self.assertIsInstance(decoded["regex"], re.Pattern)
        self.assertEqual(decoded["regex"].pattern, "^ab")

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
        self.assertIsNone(encoded["undefined"])

