from __future__ import annotations

from dataclasses import dataclass
import struct
from typing import Any

from bson import BSON

from mongoeco.wire.bson_bridge import decode_wire_value, encode_wire_value


OP_REPLY = 1
OP_QUERY = 2004
OP_MSG = 2013
OP_MSG_ALLOWED_FLAGS_MASK = 0
OP_QUERY_ALLOWED_FLAGS_MASK = 0


@dataclass(frozen=True, slots=True)
class MessageHeader:
    message_length: int
    request_id: int
    response_to: int
    op_code: int


@dataclass(frozen=True, slots=True)
class OpMsgRequest:
    header: MessageHeader
    flag_bits: int
    body: dict[str, Any]


@dataclass(frozen=True, slots=True)
class OpQueryRequest:
    header: MessageHeader
    flags: int
    full_collection_name: str
    number_to_skip: int
    number_to_return: int
    query: dict[str, Any]


@dataclass(frozen=True, slots=True)
class OpReplyResponse:
    header: MessageHeader
    response_flags: int
    cursor_id: int
    starting_from: int
    documents: list[dict[str, Any]]


def parse_message_header(data: bytes) -> MessageHeader:
    if len(data) != 16:
        raise ValueError("wire message header must be 16 bytes")
    return MessageHeader(*struct.unpack("<iiii", data))


def decode_op_msg(header: MessageHeader, payload: bytes) -> OpMsgRequest:
    if header.op_code != OP_MSG:
        raise ValueError(f"unsupported wire opCode: {header.op_code}")
    if len(payload) < 5:
        raise ValueError("OP_MSG payload is too short")
    flag_bits = struct.unpack("<i", payload[:4])[0]
    if flag_bits & ~OP_MSG_ALLOWED_FLAGS_MASK:
        raise ValueError(f"unsupported OP_MSG flags: {flag_bits}")
    position = 4
    body: dict[str, Any] | None = None
    pending_sequences: dict[str, list[dict[str, Any]]] = {}
    while position < len(payload):
        kind = payload[position]
        position += 1
        if kind == 0:
            if body is not None:
                raise ValueError("OP_MSG body document section must not appear more than once")
            if position + 4 > len(payload):
                raise ValueError("OP_MSG body section is truncated")
            document_size = struct.unpack("<i", payload[position : position + 4])[0]
            if document_size <= 0 or position + document_size > len(payload):
                raise ValueError("OP_MSG body document has invalid size")
            raw_document = payload[position : position + document_size]
            body = decode_wire_value(BSON(raw_document).decode())
            for identifier, sequence in pending_sequences.items():
                if identifier in body:
                    raise ValueError("OP_MSG document sequence identifier must not duplicate a body field")
                body[identifier] = sequence
            pending_sequences.clear()
            position += document_size
            continue
        if kind == 1:
            if position + 4 > len(payload):
                raise ValueError("OP_MSG document sequence is truncated")
            section_size = struct.unpack("<i", payload[position : position + 4])[0]
            section_start = position
            section_end = section_start + section_size
            if section_size <= 4 or section_end > len(payload):
                raise ValueError("OP_MSG document sequence has invalid size")
            position += 4
            identifier_end = payload.index(0, position, section_end)
            identifier = payload[position:identifier_end].decode("utf-8")
            position = identifier_end + 1
            sequence: list[dict[str, Any]] = []
            while position < section_end:
                if position + 4 > section_end:
                    raise ValueError("OP_MSG document sequence entry is truncated")
                document_size = struct.unpack("<i", payload[position : position + 4])[0]
                if document_size <= 0 or position + document_size > section_end:
                    raise ValueError("OP_MSG document sequence entry has invalid size")
                raw_document = payload[position : position + document_size]
                sequence.append(decode_wire_value(BSON(raw_document).decode()))
                position += document_size
            if body is None:
                if identifier in pending_sequences:
                    raise ValueError("OP_MSG document sequence identifier must be unique")
                pending_sequences[identifier] = sequence
            else:
                if identifier in body:
                    raise ValueError("OP_MSG document sequence identifier must not duplicate a body field")
                body[identifier] = sequence
            continue
        raise ValueError(f"unsupported OP_MSG section kind: {kind}")
    if body is None:
        raise ValueError("OP_MSG body document is required")
    return OpMsgRequest(header=header, flag_bits=flag_bits, body=body)


def decode_op_query(header: MessageHeader, payload: bytes) -> OpQueryRequest:
    if header.op_code != OP_QUERY:
        raise ValueError(f"unsupported wire opCode: {header.op_code}")
    if len(payload) < 12:
        raise ValueError("OP_QUERY payload is too short")
    flags = struct.unpack("<i", payload[:4])[0]
    if flags & ~OP_QUERY_ALLOWED_FLAGS_MASK:
        raise ValueError(f"unsupported OP_QUERY flags: {flags}")
    position = 4
    namespace_end = payload.index(0, position)
    full_collection_name = payload[position:namespace_end].decode("utf-8")
    position = namespace_end + 1
    number_to_skip, number_to_return = struct.unpack("<ii", payload[position : position + 8])
    position += 8
    if position + 4 > len(payload):
        raise ValueError("OP_QUERY document is truncated")
    query_size = struct.unpack("<i", payload[position : position + 4])[0]
    if query_size <= 0 or position + query_size > len(payload):
        raise ValueError("OP_QUERY document has invalid size")
    query = decode_wire_value(BSON(payload[position : position + query_size]).decode())
    return OpQueryRequest(
        header=header,
        flags=flags,
        full_collection_name=full_collection_name,
        number_to_skip=number_to_skip,
        number_to_return=number_to_return,
        query=query,
    )


def encode_op_msg_response(
    document: dict[str, Any],
    *,
    request_id: int,
    response_to: int,
) -> bytes:
    encoded_body = BSON.encode(encode_wire_value(document))
    payload = struct.pack("<i", 0) + b"\x00" + encoded_body
    header = struct.pack("<iiii", 16 + len(payload), request_id, response_to, OP_MSG)
    return header + payload


def encode_op_msg_request(
    document: dict[str, Any],
    *,
    request_id: int,
) -> bytes:
    encoded_body = BSON.encode(encode_wire_value(document))
    payload = struct.pack("<i", 0) + b"\x00" + encoded_body
    header = struct.pack("<iiii", 16 + len(payload), request_id, 0, OP_MSG)
    return header + payload


def encode_op_reply(
    documents: list[dict[str, Any]],
    *,
    request_id: int,
    response_to: int,
) -> bytes:
    encoded_documents = b"".join(BSON.encode(encode_wire_value(document)) for document in documents)
    payload = struct.pack("<iqii", 0, 0, 0, len(documents)) + encoded_documents
    header = struct.pack("<iiii", 16 + len(payload), request_id, response_to, OP_REPLY)
    return header + payload


def decode_op_reply(header: MessageHeader, payload: bytes) -> OpReplyResponse:
    if header.op_code != OP_REPLY:
        raise ValueError(f"unsupported wire opCode: {header.op_code}")
    if len(payload) < 20:
        raise ValueError("OP_REPLY payload is too short")
    response_flags, cursor_id, starting_from, number_returned = struct.unpack("<iqii", payload[:20])
    position = 20
    documents: list[dict[str, Any]] = []
    for _ in range(number_returned):
        if position + 4 > len(payload):
            raise ValueError("OP_REPLY document is truncated")
        document_size = struct.unpack("<i", payload[position : position + 4])[0]
        if document_size <= 0 or position + document_size > len(payload):
            raise ValueError("OP_REPLY document has invalid size")
        documents.append(decode_wire_value(BSON(payload[position : position + document_size]).decode()))
        position += document_size
    return OpReplyResponse(
        header=header,
        response_flags=response_flags,
        cursor_id=cursor_id,
        starting_from=starting_from,
        documents=documents,
    )
