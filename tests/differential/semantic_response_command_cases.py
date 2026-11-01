"""Cursor command controls, independently of driver batch-size promotion."""

from bson import BSON
from bson.codec_options import CodecOptions, TypeDecoder, TypeRegistry
from bson.int64 import Int64


SEED = [{"_id": 0}]
DOCUMENT_BYTES = 4 * 1024 * 1024 - 16
MAX_RESPONSE_BATCHES = 16  # Nine one-row batches plus EOF; expose stalled cursors.
CURSOR_ARRAYS = ("cursorsKilled", "cursorsNotFound", "cursorsAlive", "cursorsUnknown")


def cursor_command_batches(collection):
    collection.delete_many({})
    payload = "x" * (DOCUMENT_BYTES - len(BSON.encode({"_id": 1, "p": ""})))
    collection.insert_many([{"_id": n, "p": payload} for n in range(1, 10)])
    db = collection.database
    result = {}
    for maximum in (None, 0, 1):
        reply = db.command(
            {"find": collection.name, "sort": {"_id": 1}, "batchSize": 0}
        )
        cursor_id = reply["cursor"]["id"]
        batches = [[len(reply["cursor"]["firstBatch"]), 0]]
        try:
            while cursor_id and len(batches) < MAX_RESPONSE_BATCHES:
                command = {"getMore": cursor_id, "collection": collection.name}
                if maximum is not None:
                    command["batchSize"] = maximum
                reply = db.command(command)
                batch = reply["cursor"]["nextBatch"]
                batches.append(
                    [len(batch), sum(len(BSON.encode(row)) for row in batch)]
                )
                cursor_id = reply["cursor"]["id"]
        finally:
            if cursor_id:
                db.command({"killCursors": collection.name, "cursors": [cursor_id]})
        result[str(maximum)] = {"batches": batches, "unconsumed": bool(cursor_id)}
    return result


def cursor_metadata_types(collection):
    calls = []

    class ObserveInt(TypeDecoder):
        bson_type = int

        def transform_bson(self, value):
            calls.append(value)
            return value

    db = collection.database.with_options(
        codec_options=CodecOptions(type_registry=TypeRegistry([ObserveInt()]))
    )
    opened = db.command({"find": collection.name, "batchSize": 0})
    cursor_id = opened["cursor"]["id"]
    killed = db.command(
        {"killCursors": collection.name, "cursors": [cursor_id, Int64(0)]}
    )
    return {
        "cursor_id_class": type(cursor_id).__name__,
        "decoder_call_count": len(calls),
        "arrays": {
            field: [type(value).__name__ for value in killed[field]]
            for field in CURSOR_ARRAYS
        },
    }


SEMANTIC_RESPONSE_COMMAND_CASES = (
    ("semantic_response_command_batches", cursor_command_batches),
    ("semantic_response_cursor_metadata", cursor_metadata_types),
)
