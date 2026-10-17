"""Independent socket observations of read requests with and without replies."""

import asyncio

from itertools import product
from urllib.parse import urlsplit

from tests.support_unacknowledged import command_reply, packet, reply


FILTERS = {"document": {}, "array": [], "null": None, "scalar": 5}
CASES = tuple(product((False, True), (False, True), FILTERS))


def case_id(mirrored, more, filter_kind):
    return f"mirrored_{mirrored}_more_{more}_filter_{filter_kind}"


async def observe(uri, database, *, mirrored, more, filter_kind):
    address = urlsplit(uri)
    reader, writer = await asyncio.open_connection(
        address.hostname, address.port or 27017
    )
    try:
        writer.write(
            packet({"insert": "items", "documents": [{"_id": "a"}], "$db": database}, 1)
        )
        await writer.drain()
        assert (await reply(reader, 1))["n"] == 1
        command = {
            "find": "items",
            "filter": FILTERS[filter_kind],
            "mirrored": mirrored,
            "$db": database,
        }
        # Pipelined ping proves that no suppressed reply contaminates the socket.
        writer.write(
            packet(command, 2, more=more) + packet({"ping": 1, "$db": "admin"}, 3)
        )
        await writer.drain()
        response = None if more else command_reply(await reply(reader, 2))
        assert (await reply(reader, 3))["ok"] == 1
        writer.write(
            packet(
                {
                    "insert": "items",
                    "documents": [{"_id": "following"}],
                    "$db": database,
                },
                4,
            )
        )
        await writer.drain()
        assert (await reply(reader, 4))["n"] == 1
        writer.write(
            packet(
                {"find": "items", "filter": {}, "sort": {"_id": 1}, "$db": database}, 5
            )
        )
        await writer.drain()
        documents = (await reply(reader, 5))["cursor"]["firstBatch"]
        return {"response": response, "documents": documents, "connection_reused": True}
    finally:
        writer.close()
        await writer.wait_closed()
