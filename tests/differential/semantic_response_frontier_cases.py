"""Native response byte cuts and decoder effects, observed through public APIs."""

import hashlib
import struct

from functools import partial

from bson import BSON, Binary
from bson.codec_options import TypeDecoder, TypeRegistry


SEED = [{"_id": 0}]
QUARTER_RESPONSE_BYTES = 4 * 1024 * 1024
# Last fitting sizes differ between first find in 7 and other native routes.
BOUNDARY_OFFSETS = (-18, -17, -16, -15)
COUNT_CONTROL_OFFSET = -16
BSON_MIN_DOCUMENT_SIZE = 5


def _reset(collection, rows):
    collection.delete_many({})
    collection.insert_many(rows)


def _payload_rows(size, count):
    overhead = len(BSON.encode({"_id": 1, "p": ""}))
    payload = "x" * (size - overhead)
    return [{"_id": n, "p": payload} for n in range(1, count + 1)]


def _with_decoder(collection, calls, fail_at=None):
    class ObserveInt(TypeDecoder):
        bson_type = int

        def transform_bson(self, value):
            calls.append(value)
            if value == fail_at:
                message = "client decoder stopped"
                raise ValueError(message)
            return value

    options = collection.codec_options
    decoder = ObserveInt()
    registry = (
        {int: decoder.transform_bson}
        if isinstance(options.type_registry, tuple)
        else TypeRegistry([decoder])
    )
    return collection.with_options(codec_options=type(options)(type_registry=registry))


def _observe(open_cursor, *, raw, calls, namespace, observe_size=False):
    phase = "open"
    seen = []
    digest = hashlib.sha256()
    cursor = None
    sizes = []
    result = {"ok": True}
    try:
        cursor = open_cursor()
        phase = "iterate"
        for row in cursor:
            if raw:
                offset = 0
                count = 0
                while offset < len(row):
                    size = struct.unpack_from("<i", row, offset)[0]
                    if size < BSON_MIN_DOCUMENT_SIZE or offset + size > len(row):
                        message = "invalid BSON framing in response"
                        raise AssertionError(message)
                    offset += size
                    count += 1
                seen.append([count, len(row)])
                digest.update(row)
            else:
                # All callbacks for a successful response precede its first row.
                seen.append([row["_id"], len(calls)])
                if observe_size:
                    sizes.append(len(BSON.encode(row)))
    except Exception as error:
        details = getattr(error, "details", None) or {}
        result = {
            "ok": False,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
            "phase": phase,
            "codeName": details.get("codeName"),
            # Capture databases have fresh UUIDs. Normalize only that namespace.
            "errmsg": details.get("errmsg", str(error)).replace(
                namespace, "<namespace>"
            ),
        }
    finally:
        if cursor is not None:
            cursor.close()
    return {
        **result,
        "seen": seen,
        "decoder_calls": list(calls),
        **({"bson_sha256": digest.hexdigest()} if raw else {}),
        **({"document_bytes": sizes} if observe_size and not raw else {}),
    }


def _open(  # noqa: PLR0913 - independent public cursor API axes
    collection, *, aggregate, raw, batch, pipeline=None, filter_spec=None, hint=None
):
    if aggregate:
        options = {} if batch is None else {"batchSize": batch}
        method = collection.aggregate_raw_batches if raw else collection.aggregate
        return method(
            [{"$sort": {"_id": 1}}] if pipeline is None else pipeline, **options
        )
    options = {} if batch is None else {"batch_size": batch}
    if hint is not None:
        options["hint"] = hint
    method = collection.find_raw_batches if raw else collection.find
    return method(
        {} if filter_spec is None else filter_spec, sort=[("_id", 1)], **options
    )


def byte_boundaries(collection, *, aggregate):
    result = {}
    for offset in BOUNDARY_OFFSETS:
        size = QUARTER_RESPONSE_BYTES + offset
        _reset(collection, _payload_rows(size, 9))
        batches = (None, 2) if offset == COUNT_CONTROL_OFFSET else (None,)
        for raw in (False, True):
            for batch in batches:
                calls = []
                decoded = _with_decoder(collection, calls)
                result[f"{size}-{raw}-{batch}"] = _observe(
                    partial(_open, decoded, aggregate=aggregate, raw=raw, batch=batch),
                    raw=raw,
                    calls=calls,
                    namespace=collection.full_name,
                )
    return result


def byte_errors(collection):
    _reset(collection, _payload_rows(QUARTER_RESPONSE_BYTES + 1, 5))
    result = {}
    for bad in (3, 4, 5):
        pipeline = [
            {"$sort": {"_id": 1}},
            {
                "$project": {
                    "p": 1,
                    "v": {"$divide": [1, {"$cond": [{"$eq": ["$_id", bad]}, 0, 1]}]},
                }
            },
        ]
        for raw in (False, True):
            for batch in (None, 0, 2):
                calls = []
                decoded = _with_decoder(collection, calls)
                result[f"{bad}-{raw}-{batch}"] = _observe(
                    partial(
                        _open,
                        decoded,
                        aggregate=True,
                        raw=raw,
                        batch=batch,
                        pipeline=pipeline,
                    ),
                    raw=raw,
                    calls=calls,
                    namespace=collection.full_name,
                )
    return result


def filter_errors(collection):
    _reset(collection, [{"_id": 2, "v": 0}, {"_id": 1, "v": 1}, {"_id": 3, "v": 1}])
    collection.create_index("v", name="v_1")
    filter_spec = {"$expr": {"$gt": [{"$divide": [1, "$v"]}, 0]}}
    result = {}
    for aggregate in (False, True):
        pipeline = [{"$sort": {"_id": 1}}, {"$match": filter_spec}]
        for raw in (False, True):
            for batch in (None, 1, 2):
                calls = []
                decoded = _with_decoder(collection, calls)
                result[f"{aggregate}-{raw}-{batch}"] = _observe(
                    partial(
                        _open,
                        decoded,
                        aggregate=aggregate,
                        raw=raw,
                        batch=batch,
                        pipeline=pipeline,
                        filter_spec=filter_spec,
                    ),
                    raw=raw,
                    calls=calls,
                    namespace=collection.full_name,
                )
    for hint in ("_id_", "v_1"):
        for raw in (False, True):
            calls = []
            decoded = _with_decoder(collection, calls)
            result[f"hint-{hint}-{raw}"] = _observe(
                partial(
                    _open,
                    decoded,
                    aggregate=False,
                    raw=raw,
                    batch=1,
                    filter_spec=filter_spec,
                    hint=hint,
                ),
                raw=raw,
                calls=calls,
                namespace=collection.full_name,
            )
    return result


def expansion_callbacks(collection):
    result = {}
    for separate in (False, True):
        _reset(
            collection,
            [{"_id": 1, "v": [1]}, {"_id": 2, "v": [0]}]
            if separate
            else [{"_id": 1, "v": [1, 0]}],
        )
        pipeline = [{"$unwind": "$v"}, {"$project": {"x": {"$divide": [1, "$v"]}}}]
        for raw in (False, True):
            for batch in (None, 1, 2):
                for fail_at in (None, 1):
                    calls = []
                    decoded = _with_decoder(collection, calls, fail_at)
                    result[f"{separate}-{raw}-{batch}-{fail_at}"] = _observe(
                        partial(
                            _open,
                            decoded,
                            aggregate=True,
                            raw=raw,
                            batch=batch,
                            pipeline=pipeline,
                        ),
                        raw=raw,
                        calls=calls,
                        namespace=collection.full_name,
                    )
    return result


def client_decoder_errors(collection):
    _reset(collection, [{"_id": n} for n in range(1, 4)])
    result = {}
    for aggregate in (False, True):
        for raw in (False, True):
            for batch in (None, 1, 2):
                for fail_at in (1, 2):
                    calls = []
                    decoded = _with_decoder(collection, calls, fail_at)
                    result[f"{aggregate}-{raw}-{batch}-{fail_at}"] = _observe(
                        partial(
                            _open, decoded, aggregate=aggregate, raw=raw, batch=batch
                        ),
                        raw=raw,
                        calls=calls,
                        namespace=collection.full_name,
                    )
    return result


def document_limits(collection):
    limit = 16 * 1024 * 1024
    payload = "x" * (limit // 2 - 1024)
    binary = Binary(b"\xff" * 128, 0)
    _reset(collection, [{"_id": 1, "p": payload, "b": binary}])
    overhead = len(BSON.encode({"_id": 1, "a": "", "b": ""}))
    result = {}
    # Two legal Strings distinguish returned-document size from String capacity.
    for offset in (1, 16383, 16384, 16385):
        padding = limit + offset - overhead - 2 * len(payload)
        pipeline = [{"$project": {"a": "$p", "b": {"$concat": ["$p", "x" * padding]}}}]
        for raw in (False, True):
            for batch in (None, 0):
                calls = []
                decoded = _with_decoder(collection, calls)
                result[f"{offset}-{raw}-{batch}"] = _observe(
                    partial(
                        _open,
                        decoded,
                        aggregate=True,
                        raw=raw,
                        batch=batch,
                        pipeline=pipeline,
                    ),
                    raw=raw,
                    calls=calls,
                    namespace=collection.full_name,
                    observe_size=True,
                )
    # The first element is part of the 7/8 size error. Exercise the existing
    # renderer with large String/object values and an abbreviated Binary.
    for label, prefix, empty_prefix in (
        ("string", {"a": "$p"}, {"a": ""}),
        ("binary", {"z": "$b", "a": "$p"}, {"z": binary, "a": ""}),
        ("object", {"o": {"a": "$p"}}, {"o": {"a": ""}}),
    ):
        target = limit + 16385
        padding = (
            target - len(BSON.encode({**empty_prefix, "b": ""})) - 2 * len(payload)
        )
        pipeline = [
            {"$project": {**prefix, "b": {"$concat": ["$p", "x" * padding]}, "_id": 0}}
        ]
        for raw in (False, True):
            for batch in (None, 0):
                calls = []
                decoded = _with_decoder(collection, calls)
                result[f"first-{label}-{raw}-{batch}"] = _observe(
                    partial(
                        _open,
                        decoded,
                        aggregate=True,
                        raw=raw,
                        batch=batch,
                        pipeline=pipeline,
                    ),
                    raw=raw,
                    calls=calls,
                    namespace=collection.full_name,
                    observe_size=True,
                )
    return result


def string_limits(collection):
    half = 8 * 1024 * 1024
    result = {}
    for operator in ("concat", "replaceAll", "utf8_concat", "concat_null_tail"):
        utf8 = operator == "utf8_concat"
        payload = "é" * (half // 2 - 1) if utf8 else "x" * (half - 1)
        _reset(collection, [{"_id": 1, "p": payload}])
        offsets = (0,) if operator == "concat_null_tail" else (-1, 0, 1)
        for offset in offsets:
            padding = "y" * ((4 if utf8 else 2) + offset)
            if operator == "replaceAll":
                expression = {
                    "$replaceAll": {
                        "input": {"$concat": ["$p", padding]},
                        "find": "x",
                        "replacement": "xx",
                    }
                }
            else:
                args = ["$p", "$p", padding]
                if operator == "concat_null_tail":
                    args.append(None)
                expression = {"$concat": args}
            for raw in (False, True):
                for batch in (None, 0):
                    calls = []
                    decoded = _with_decoder(collection, calls)
                    result[f"{operator}-{offset}-{raw}-{batch}"] = _observe(
                        partial(
                            _open,
                            decoded,
                            aggregate=True,
                            raw=raw,
                            batch=batch,
                            pipeline=[{"$project": {"p": expression}}],
                        ),
                        raw=raw,
                        calls=calls,
                        namespace=collection.full_name,
                        observe_size=True,
                    )
    return result


def expanded_filter_errors(collection):
    bad_id = 5
    _reset(
        collection,
        [
            {"_id": n, "v": 0 if n == bad_id else 1, "p": "x" * 1024}
            for n in range(1, 10)
        ],
    )
    pipeline = [
        {"$match": {"$expr": {"$gt": [{"$divide": [1, "$v"]}, 0]}}},
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "p": {
                    "$replaceAll": {
                        "input": "$p",
                        "find": "x",
                        "replacement": "x" * 4096,
                    }
                }
            }
        },
    ]
    result = {}
    for raw in (False, True):
        for batch in (None, 0, 1, 2):
            calls = []
            decoded = _with_decoder(collection, calls)
            result[f"{raw}-{batch}"] = _observe(
                partial(
                    _open,
                    decoded,
                    aggregate=True,
                    raw=raw,
                    batch=batch,
                    pipeline=pipeline,
                ),
                raw=raw,
                calls=calls,
                namespace=collection.full_name,
            )
    return result


SEMANTIC_RESPONSE_FRONTIER_CASES = (
    ("semantic_response_byte_find", partial(byte_boundaries, aggregate=False)),
    ("semantic_response_byte_aggregate", partial(byte_boundaries, aggregate=True)),
    ("semantic_response_byte_errors", byte_errors),
    ("semantic_response_filter_errors", filter_errors),
    ("semantic_response_expansion_callbacks", expansion_callbacks),
    ("semantic_response_client_decoder_errors", client_decoder_errors),
    ("semantic_response_document_limits", document_limits),
    ("semantic_response_string_limits", string_limits),
    ("semantic_response_expanded_filter_errors", expanded_filter_errors),
)


def cursor_limit_contract(collection):  # noqa: PLR0912, PLR0915 - finite public cursor state matrix
    _reset(collection, [{"_id": n} for n in range(5)])
    result = {}
    for kind in ("keyword", "setter", "replace"):
        for limit in (0, 1, -1):
            phase = "open"
            cursor = None
            try:
                cursor = collection.find(
                    {},
                    sort=[("_id", 1)],
                    **({"limit": limit} if kind == "keyword" else {}),
                )
                if kind == "setter":
                    cursor = cursor.limit(limit)
                elif kind == "replace":
                    cursor = cursor.limit(1).limit(limit)
                phase = "iterate"
                result[f"{kind}/{limit}"] = {
                    "ok": True,
                    "ids": [row["_id"] for row in cursor],
                }
            except Exception as error:
                result[f"{kind}/{limit}"] = {
                    "ok": False,
                    "family": type(error).__name__,
                    "phase": phase,
                }
            finally:
                if cursor is not None:
                    cursor.close()
    cursor = collection.find({}, sort=[("_id", 1)])
    try:
        try:
            cursor.to_list(0)
            result["zero_length"] = {"ok": True}
        except Exception as error:
            result["zero_length"] = {"ok": False, "family": type(error).__name__}
        result["after_zero_length"] = [row["_id"] for row in cursor.to_list(1)]
        result["remainder"] = [row["_id"] for row in cursor]
    finally:
        cursor.close()
    result["skip_zero"] = [
        row["_id"] for row in collection.find().sort("_id", 1).skip(2).limit(0)
    ]
    for limit, batch in ((-5, 2), (-2, 2)):
        cursor = collection.find({}, sort=[("_id", 1)], limit=limit, batch_size=batch)
        try:
            clone = cursor.clone()
            try:
                result[f"clone/{limit}/{batch}"] = [row["_id"] for row in clone]
            finally:
                clone.close()
            result[f"batch/{limit}/{batch}"] = [
                row["_id"] for row in cursor.to_list(1)
            ] + [row["_id"] for row in cursor]
        finally:
            cursor.close()
    for length in (0, -1, "x"):
        cursor = collection.find()
        cursor.close()
        try:
            cursor.to_list(length)
            result[f"closed_length/{length}"] = {"ok": True}
        except Exception as error:
            result[f"closed_length/{length}"] = {
                "ok": False,
                "family": type(error).__name__,
            }
    _reset(collection, [{"_id": n} for n in range(200)])
    result["negative_default_batch"] = [
        row["_id"] for row in collection.find({}, sort=[("_id", 1)], limit=-200)
    ]
    return result


SEMANTIC_RESPONSE_FRONTIER_CASES += (
    ("semantic_response_cursor_limits", cursor_limit_contract),
)
