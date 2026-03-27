import struct
import unittest

from bson import BSON

from mongoeco.types import ObjectId
from mongoeco.wire.protocol import (
    OP_MSG,
    OP_QUERY,
    decode_op_msg,
    decode_op_query,
    encode_op_msg_response,
    encode_op_reply,
    parse_message_header,
)


class WireProtocolTests(unittest.TestCase):
    def test_decode_op_msg_supports_body_and_document_sequence(self):
        body = BSON.encode({"insert": "events", "$db": "alpha", "ordered": True})
        documents = BSON.encode({"_id": "1", "kind": "view"}) + BSON.encode({"_id": "2", "kind": "click"})
        identifier = b"documents\x00"
        section_one = struct.pack("<i", 4 + len(identifier) + len(documents)) + identifier + documents
        payload = struct.pack("<i", 0) + b"\x00" + body + b"\x01" + section_one
        header = parse_message_header(struct.pack("<iiii", 16 + len(payload), 7, 0, OP_MSG))

        request = decode_op_msg(header, payload)

        self.assertEqual(request.body["insert"], "events")
        self.assertEqual(request.body["$db"], "alpha")
        self.assertEqual(
            request.body["documents"],
            [{"_id": "1", "kind": "view"}, {"_id": "2", "kind": "click"}],
        )

    def test_encode_op_msg_response_round_trips_special_values(self):
        response = encode_op_msg_response(
            {"ok": 1.0, "value": {"_id": ObjectId("507f1f77bcf86cd799439011")}},
            request_id=2,
            response_to=7,
        )

        header = parse_message_header(response[:16])
        self.assertEqual(header.op_code, OP_MSG)
        self.assertEqual(header.response_to, 7)

        flag_bits = struct.unpack("<i", response[16:20])[0]
        self.assertEqual(flag_bits, 0)
        self.assertEqual(response[20], 0)
        decoded = BSON(response[21:]).decode()
        self.assertEqual(decoded["ok"], 1.0)
        self.assertEqual(str(decoded["value"]["_id"]), "507f1f77bcf86cd799439011")

    def test_decode_op_query_supports_legacy_command_handshake(self):
        query = BSON.encode({"ismaster": 1, "helloOk": True})
        namespace = b"admin.$cmd\x00"
        payload = struct.pack("<i", 0) + namespace + struct.pack("<ii", 0, -1) + query
        header = parse_message_header(struct.pack("<iiii", 16 + len(payload), 9, 0, OP_QUERY))

        request = decode_op_query(header, payload)

        self.assertEqual(request.full_collection_name, "admin.$cmd")
        self.assertEqual(request.query, {"ismaster": 1, "helloOk": True})

    def test_encode_op_reply_wraps_documents_for_legacy_queries(self):
        response = encode_op_reply([{"ok": 1.0}], request_id=3, response_to=9)

        header = parse_message_header(response[:16])
        self.assertEqual(header.op_code, 1)
        flags, cursor_id, starting_from, number_returned = struct.unpack("<iqii", response[16:36])
        self.assertEqual(flags, 0)
        self.assertEqual(cursor_id, 0)
        self.assertEqual(starting_from, 0)
        self.assertEqual(number_returned, 1)
