"""Observe raw BSON directly, before any SDK response-shape conversion."""

import hashlib
import struct

from functools import partial

from bson import Binary, Code, DBRef
from bson.codec_options import TypeDecoder, TypeRegistry


SEED = [{"_id": 0}]
BSON_MIN_DOCUMENT_SIZE = 5


def _reset(collection, rows):
    collection.delete_many({})
    collection.insert_many(rows)


def _cursor(collection, aggregate, size, pipeline=None):
    if aggregate:
        options = {} if size is None else {"batchSize": size}
        return collection.aggregate_raw_batches(
            [{"$sort": {"_id": 1}}] if pipeline is None else pipeline, **options
        )
    options = {} if size is None else {"batch_size": size}
    return collection.find_raw_batches({}, sort=[("_id", 1)], **options)


def _observe(open_cursor):
    phase = "open"
    pages = []
    digest = hashlib.sha256()
    cursor = None
    try:
        cursor = open_cursor()
        phase = "iterate"
        for raw in cursor:
            offset = 0
            count = 0
            while offset < len(raw):
                size = struct.unpack_from("<i", raw, offset)[0]
                if size < BSON_MIN_DOCUMENT_SIZE or offset + size > len(raw):
                    message = "invalid raw BSON document framing"
                    raise AssertionError(message)
                offset += size
                count += 1
            digest.update(raw)
            pages.append({"documents": count, "bytes": len(raw)})
        result = {"ok": True}
    except Exception as error:
        result = {
            "ok": False,
            "phase": phase,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
            "codeName": (getattr(error, "details", None) or {}).get("codeName"),
        }
    finally:
        if cursor is not None:
            cursor.close()
    return {**result, "pages": pages, "bson_sha256": digest.hexdigest()}


def raw_values(collection, *, aggregate):
    values = [
        {"$ref": "c", "$id": 1},
        {"$id": 1, "$ref": "c"},
        {"$ref": "c", "$id": 1, "$db": None},
        {"$id": 2, "$ref": "c", "extra": {"$id": 3, "$ref": "d"}},
        DBRef("c", 1),
        {"$ref": Code("c"), "$id": 1, "$db": Code("d")},
    ]
    _reset(
        collection,
        [
            {
                "_id": n,
                "v": [value],
                "code": Code("$v", {"nested": value}),
                "b0": Binary(b"a", 0),
                "b5": Binary(b"b" * 16, 5),
            }
            for n, value in enumerate(values, 1)
        ],
    )
    calls = []

    class OffsetInt(TypeDecoder):
        bson_type = int

        def transform_bson(self, value):
            calls.append(value)
            return value + 1000

    options = collection.codec_options
    decoder = OffsetInt()
    # The local API and PyMongo expose distinct registry construction APIs.
    # Adapt only that public option; observation of the raw bytes stays shared.
    registry = (
        {int: decoder.transform_bson}
        if isinstance(options.type_registry, tuple)
        else TypeRegistry([decoder])
    )
    decoded = collection.with_options(
        codec_options=type(options)(type_registry=registry)
    )
    result = {}
    for size in (None, 0, 1, 2):
        before = len(calls)
        result[str(size)] = {
            **_observe(partial(_cursor, decoded, aggregate=aggregate, size=size)),
            "decoder_calls": len(calls) - before,
        }
    before = len(calls)
    control = decoded.find_one({"_id": 1})
    result["control"] = {
        "decoded_id": control["_id"],
        "decoder_active": len(calls) > before,
    }
    return result


def raw_counts(collection, *, aggregate):
    _reset(collection, [{"_id": n, "v": n} for n in range(1, 604)])
    return {
        str(size): _observe(
            partial(_cursor, collection, aggregate=aggregate, size=size)
        )
        for size in (None, 0, 1, 2)
    }


def raw_aggregate_errors(collection):
    _reset(collection, [{"_id": n} for n in range(1, 114)])
    result = {}
    for size in (None, 0, 1, 2):
        for bad in (1, 3, 103):
            pipeline = [
                {"$sort": {"_id": 1}},
                {
                    "$project": {
                        "v": {
                            "$divide": [
                                1,
                                {
                                    "$cond": [
                                        {"$eq": ["$_id", bad]},
                                        0,
                                        1,
                                    ]
                                },
                            ]
                        }
                    }
                },
            ]
            result[f"{size}-{bad}"] = _observe(
                partial(
                    _cursor, collection, aggregate=True, size=size, pipeline=pipeline
                )
            )
    return result


def raw_write_opening(collection):
    _reset(collection, [{"_id": 1}, {"_id": 2}])
    target = collection.database[collection.name + "_raw_target"]
    result = {}
    for size in (None, 0, 1, 2):
        target.delete_many({})
        pipeline = [{"$merge": {"into": target.name}}]
        cursor = _cursor(collection, aggregate=True, size=size, pipeline=pipeline)
        before = target.count_documents({})
        observation = _observe(lambda cursor=cursor: cursor)
        result[str(size)] = {
            **observation,
            "before": before,
            "after": target.count_documents({}),
        }
    return result


SEMANTIC_RAW_RESPONSE_CASES = (
    ("semantic_raw_find_values", partial(raw_values, aggregate=False)),
    ("semantic_raw_aggregate_values", partial(raw_values, aggregate=True)),
    ("semantic_raw_find_counts", partial(raw_counts, aggregate=False)),
    ("semantic_raw_aggregate_counts", partial(raw_counts, aggregate=True)),
    ("semantic_raw_aggregate_errors", raw_aggregate_errors),
    ("semantic_raw_write_opening", raw_write_opening),
)
