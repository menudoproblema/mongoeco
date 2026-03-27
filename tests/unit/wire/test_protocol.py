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
    def test_parse_message_header_requires_exact_size(self):
        with self.assertRaisesRegex(ValueError, "16 bytes"):
            parse_message_header(b"short")

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

    def test_decode_op_msg_rejects_unsupported_flags(self):
        payload = struct.pack("<i", 1) + b"\x00" + BSON.encode({"ping": 1})
        header = parse_message_header(struct.pack("<iiii", 16 + len(payload), 7, 0, OP_MSG))

        with self.assertRaisesRegex(ValueError, "unsupported OP_MSG flags"):
            decode_op_msg(header, payload)

    def test_decode_op_query_rejects_unsupported_flags(self):
        query = BSON.encode({"ismaster": 1})
        namespace = b"admin.$cmd\x00"
        payload = struct.pack("<i", 1) + namespace + struct.pack("<ii", 0, -1) + query
        header = parse_message_header(struct.pack("<iiii", 16 + len(payload), 9, 0, OP_QUERY))

        with self.assertRaisesRegex(ValueError, "unsupported OP_QUERY flags"):
            decode_op_query(header, payload)

    def test_decode_op_msg_rejects_wrong_opcode_and_short_payload(self):
        wrong_header = parse_message_header(struct.pack("<iiii", 21, 7, 0, OP_QUERY))
        with self.assertRaisesRegex(ValueError, "unsupported wire opCode"):
            decode_op_msg(wrong_header, b"\x00" * 5)

        header = parse_message_header(struct.pack("<iiii", 20, 7, 0, OP_MSG))
        with self.assertRaisesRegex(ValueError, "payload is too short"):
            decode_op_msg(header, b"\x00\x00\x00\x00")

    def test_decode_op_msg_rejects_truncated_and_invalid_sections(self):
        header = parse_message_header(struct.pack("<iiii", 0, 7, 0, OP_MSG))

        with self.assertRaisesRegex(ValueError, "body section is truncated"):
            decode_op_msg(header, struct.pack("<i", 0) + b"\x00")

        with self.assertRaisesRegex(ValueError, "body document has invalid size"):
            decode_op_msg(header, struct.pack("<i", 0) + b"\x00" + struct.pack("<i", 999))

        with self.assertRaisesRegex(ValueError, "document sequence is truncated"):
            decode_op_msg(header, struct.pack("<i", 0) + b"\x01")

        with self.assertRaisesRegex(ValueError, "document sequence has invalid size"):
            decode_op_msg(header, struct.pack("<i", 0) + b"\x01" + struct.pack("<i", 3))

        body = BSON.encode({"ping": 1})
        identifier = b"x\x00"
        bad_seq = (
            struct.pack("<i", 0)
            + b"\x00"
            + body
            + b"\x01"
            + struct.pack("<i", 4 + len(identifier) + 2)
            + identifier
            + b"\x01\x02"
        )
        with self.assertRaisesRegex(ValueError, "document sequence entry is truncated"):
            decode_op_msg(header, bad_seq)

        documents = struct.pack("<i", 999)
        identifier = b"documents\x00"
        section = struct.pack("<i", 4 + len(identifier) + len(documents)) + identifier + documents
        with self.assertRaisesRegex(ValueError, "entry has invalid size"):
            decode_op_msg(header, struct.pack("<i", 0) + b"\x01" + section)

        with self.assertRaisesRegex(ValueError, "section kind"):
            decode_op_msg(header, struct.pack("<i", 0) + b"\x02")

    def test_decode_op_query_rejects_wrong_opcode_short_and_invalid_documents(self):
        wrong_header = parse_message_header(struct.pack("<iiii", 0, 9, 0, OP_MSG))
        with self.assertRaisesRegex(ValueError, "unsupported wire opCode"):
            decode_op_query(wrong_header, b"\x00" * 12)

        header = parse_message_header(struct.pack("<iiii", 0, 9, 0, OP_QUERY))
        with self.assertRaisesRegex(ValueError, "payload is too short"):
            decode_op_query(header, b"\x00" * 11)

        namespace = b"admin.$cmd\x00"
        truncated_doc = struct.pack("<i", 0) + namespace + struct.pack("<ii", 0, -1)
        with self.assertRaisesRegex(ValueError, "document is truncated"):
            decode_op_query(header, truncated_doc)

        invalid_doc = truncated_doc + struct.pack("<i", 999)
        with self.assertRaisesRegex(ValueError, "invalid size"):
            decode_op_query(header, invalid_doc)
