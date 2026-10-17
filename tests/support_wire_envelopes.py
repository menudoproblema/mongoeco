"""Independent socket observations; never import the candidate codec or client."""

import asyncio
import socket
import struct

from contextlib import suppress
from urllib.parse import urlsplit

from bson import BSON


REQUEST_ID = 42
PING_ID = 43
OP_MSG = 2013

CASES = (
    "legacy_namespace",
    "ordinary",
    "optional_17",
    "optional_31",
    "checksum",
    "checksum_more",
    "checksum_invalid",
    "checksum_missing",
    "checksum_truncated",
    "required_unknown",
    "section_unknown",
    "section_unknown_more",
    "length_short",
    "length_large",
)


def crc32c(data):
    value = 0xFFFFFFFF
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    return value ^ 0xFFFFFFFF


def packet(body, request_id, *, flags=0, checksum=False, section=None):
    payload = struct.pack("<I", flags) + (
        section if section is not None else b"\x00" + BSON.encode(body)
    )
    header = struct.pack(
        "<iiii", 16 + len(payload) + (4 if checksum else 0), request_id, 0, 2013
    )
    raw = header + payload
    return raw + struct.pack("<I", crc32c(raw)) if checksum else raw


def specimen(case, database):
    if case == "legacy_namespace":
        query = (
            struct.pack("<i", 0)
            + f"{database}.items".encode()
            + b"\x00"
            + struct.pack("<ii", 0, -1)
            + BSON.encode({})
        )
        return struct.pack("<iiii", 16 + len(query), REQUEST_ID, 0, 2004) + query
    body = {
        "update": "items",
        "updates": [{"q": {"_id": "a"}, "u": {"$inc": {"value": 1}}}],
        "$db": database,
    }
    flags = {
        "optional_17": 1 << 17,
        "optional_31": 1 << 31,
        "required_unknown": 4,
        "section_unknown_more": 2,
    }.get(case, 0)
    if case.startswith("length_"):
        length = 15 if case == "length_short" else 64_000_000
        return struct.pack("<iiii", length, REQUEST_ID, 0, 2013)
    if case.startswith("section_"):
        return packet(body, REQUEST_ID, flags=flags, section=b"\x63")
    if case.startswith("checksum"):
        flags = 3 if case == "checksum_more" else 1
        raw = packet(
            body,
            REQUEST_ID,
            flags=flags,
            checksum=case not in ("checksum_missing", "checksum_truncated"),
            section=b"\x00" if case == "checksum_truncated" else None,
        )
        return raw[:-1] + bytes([raw[-1] ^ 1]) if case == "checksum_invalid" else raw
    return packet(body, REQUEST_ID, flags=flags)


def decode(raw_header, payload):
    size, _, response_to, opcode = struct.unpack("<iiii", raw_header)
    assert size == 16 + len(payload)
    assert opcode in (OP_MSG, 1)
    start = 5 if opcode == OP_MSG else 20
    length = struct.unpack("<i", payload[start : start + 4])[0]
    document = BSON(payload[start : start + length]).decode()
    if opcode == 1:
        document["_observer_reply_flags"] = struct.unpack("<i", payload[:4])[0]
    return response_to, document


def response(document):
    return {
        **{
            key: document[key]
            for key in ("ok", "code", "codeName", "n", "nModified")
            if key in document
        },
        "errmsg_present": "errmsg" in document,
        "$err_present": "$err" in document,
        **(
            {"legacy_reply_flags": document["_observer_reply_flags"]}
            if "_observer_reply_flags" in document
            else {}
        ),
    }


def setup(database):
    return packet(
        {"insert": "items", "documents": [{"_id": "a", "value": 0}], "$db": database}, 1
    )


def query(database):
    return packet({"find": "items", "filter": {}, "$db": database}, 70)


def observe_sync(uri, database, case):
    address = urlsplit(uri)
    endpoint = (address.hostname, address.port or 27017)

    def receive(sock):
        def exact(size):
            data = b""
            while len(data) < size:
                chunk = sock.recv(size - len(data))
                if not chunk:
                    raise EOFError
                data += chunk
            return data

        header = exact(16)
        return decode(header, exact(struct.unpack("<i", header[:4])[0] - 16))

    with socket.create_connection(endpoint, timeout=3) as sock:
        sock.sendall(setup(database))
        assert receive(sock)[1]["n"] == 1
        sock.sendall(
            specimen(case, database) + packet({"ping": 1, "$db": "admin"}, PING_ID)
        )
        replies = []
        try:
            rid, document = receive(sock)
            replies.append((rid, response(document)))
            if rid == REQUEST_ID:
                rid, document = receive(sock)
                replies.append((rid, response(document)))
        except (EOFError, ConnectionError):
            pass
    with socket.create_connection(endpoint, timeout=3) as sock:
        sock.sendall(query(database))
        documents = receive(sock)[1]["cursor"]["firstBatch"]
    return {
        "replies": replies,
        "connection_reused": bool(replies and replies[-1][0] == PING_ID),
        "documents": documents,
    }


async def observe_async(uri, database, case):
    address = urlsplit(uri)

    async def connect():
        return await asyncio.open_connection(address.hostname, address.port or 27017)

    async def receive(reader):
        header = await asyncio.wait_for(reader.readexactly(16), timeout=3)
        payload = await asyncio.wait_for(
            reader.readexactly(struct.unpack("<i", header[:4])[0] - 16), timeout=3
        )
        return decode(header, payload)

    async def close(writer):
        writer.close()
        with suppress(ConnectionError):
            await writer.wait_closed()

    reader, writer = await connect()
    try:
        writer.write(setup(database))
        await writer.drain()
        assert (await receive(reader))[1]["n"] == 1
        writer.write(
            specimen(case, database) + packet({"ping": 1, "$db": "admin"}, PING_ID)
        )
        await writer.drain()
        replies = []
        try:
            rid, document = await receive(reader)
            replies.append((rid, response(document)))
            if rid == REQUEST_ID:
                rid, document = await receive(reader)
                replies.append((rid, response(document)))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
    finally:
        await close(writer)
    reader, writer = await connect()
    try:
        writer.write(query(database))
        await writer.drain()
        documents = (await receive(reader))[1]["cursor"]["firstBatch"]
    finally:
        await close(writer)
    return {
        "replies": replies,
        "connection_reused": bool(replies and replies[-1][0] == PING_ID),
        "documents": documents,
    }
