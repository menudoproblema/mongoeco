"""Public decoder dispatch and equivalent read options, with native oracles."""

from datetime import UTC, datetime
from uuid import UUID

from bson import BSON, SON, Binary, Code, DBRef, Decimal128, Int64, ObjectId, Timestamp
from bson.binary import UuidRepresentation
from bson.codec_options import TypeDecoder, TypeRegistry

from tests.support_write_parity import BSONObservation


SEED = [{"_id": 0}]
OID = ObjectId("0123456789abcdef01234567")
UUID_BYTES = bytes(range(16))
DECODER_FAILURE_VALUE = 2
REPRESENTATIONS = {
    "unspecified": UuidRepresentation.UNSPECIFIED,
    "standard": UuidRepresentation.STANDARD,
    "pythonLegacy": UuidRepresentation.PYTHON_LEGACY,
    "javaLegacy": UuidRepresentation.JAVA_LEGACY,
    "csharpLegacy": UuidRepresentation.CSHARP_LEGACY,
}


def _describe(value, *, namespace=None):
    result = {"type": type(value).__name__}
    if isinstance(value, Code):
        result.update(
            text=str(value),
            scope_keys=None if value.scope is None else list(value.scope),
        )
    elif isinstance(value, DBRef):
        result.update(collection=value.collection, id_type=type(value.id).__name__)
    elif isinstance(value, dict):
        result["keys"] = list(value)
    elif isinstance(value, list):
        result["length"] = len(value)
    elif isinstance(value, bytes):
        result.update(subtype=getattr(value, "subtype", 0), hex=bytes(value).hex())
    elif isinstance(value, datetime):
        result["iso"] = value.isoformat()
    elif isinstance(value, UUID):
        result["hex"] = value.hex
    elif isinstance(value, str | int | float | bool):
        result["value"] = "<namespace>" if value == namespace else value
    return result


def _options(
    collection,
    decoders=(),
    *,
    representation="unspecified",
    document_class=dict,
    aware=False,
):
    options = collection.codec_options
    local = isinstance(options.type_registry, tuple)

    def adapter(value_type, transform):
        class Decode(TypeDecoder):
            bson_type = value_type

            def transform_bson(self, value):
                return transform(value)

        return Decode()

    registry = (
        dict(decoders) if local else TypeRegistry([adapter(t, f) for t, f in decoders])
    )
    return collection.with_options(
        codec_options=type(options)(
            type_registry=registry,
            uuid_representation=representation
            if local
            else REPRESENTATIONS[representation],
            document_class=document_class,
            tz_aware=aware,
        )
    )


def _reset(collection, rows):
    collection.delete_many({})
    collection.insert_many(rows)


def _read(collection, *, aggregate=False):
    phase = "open"
    cursor = None
    try:
        cursor = collection.aggregate([]) if aggregate else collection.find({})
        phase = "iterate"
        rows = list(cursor)
        return {
            "ok": True,
            "rows": rows,
            "root_types": [type(row).__name__ for row in rows],
        }
    except Exception as error:
        details = getattr(error, "details", None) or {}
        return {
            "ok": False,
            "phase": phase,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
            "errmsg": details.get("errmsg", str(error)).replace(
                collection.full_name, "<namespace>"
            ),
        }
    finally:
        if cursor is not None:
            cursor.close()


def _marker(name, calls, *, namespace):
    def transform(value):
        description = _describe(value, namespace=namespace)
        calls.append({"decoder": name, "input": description})
        return {"decoded_by": name, "input": description}

    return transform


def scalar_dispatch(collection):
    _reset(
        collection,
        [
            {
                "_id": OID,
                "int": 1,
                "long": Int64(2),
                "bool": True,
                "double": 1.25,
                "str": "plain",
                "code": Code("plain"),
                "scope": Code("plain", {"n": Int64(3)}),
                "bytes": b"ab",
                "binary": Binary(b"ab", 5),
                "decimal": Decimal128("1.5"),
                "date": datetime(2001, 1, 1, tzinfo=UTC),
                "timestamp": Timestamp(1, 2),
                "null": None,
            }
        ],
    )
    result = {}
    for kind in (
        int,
        Int64,
        bool,
        float,
        str,
        Code,
        bytes,
        Binary,
        Decimal128,
        datetime,
        Timestamp,
        ObjectId,
        type(None),
    ):
        for aware in (False, True):
            for aggregate in (False, True):
                calls = []
                decoded = _options(
                    collection,
                    [
                        (
                            kind,
                            _marker(
                                kind.__name__, calls, namespace=collection.full_name
                            ),
                        )
                    ],
                    aware=aware,
                )
                result[f"{kind.__name__}-{aware}-{aggregate}"] = {
                    **_read(decoded, aggregate=aggregate),
                    "calls": calls,
                }
    return BSONObservation([OID]).value(result)


def decoder_single_pass(collection):
    _reset(
        collection,
        [{"_id": OID, "n": 1, "s": "original", "long": Int64(2), "bool": True}],
    )
    result = {}
    for reverse in (False, True):
        for aggregate in (False, True):
            calls = []

            def number(value, observed=calls):
                observed.append(_describe(value))
                return "from_int"

            decoders = [
                (int, number),
                (str, _marker("str", calls, namespace=collection.full_name)),
            ]
            if reverse:
                decoders.reverse()
            decoded = _options(collection, decoders)
            result[f"{reverse}-{aggregate}"] = {
                **_read(decoded, aggregate=aggregate),
                "calls": calls,
            }
    return BSONObservation([OID]).value(result)


def container_dispatch(collection):
    _reset(
        collection,
        [
            {
                "_id": OID,
                "object": {"n": 1},
                "array": [{"n": 2}, 3],
                "code": Code("x", {"n": 4, "array": [5]}),
                "ref": DBRef("c", {"n": 6}, extra={"n": 7}),
            }
        ],
    )
    result = {}
    for kinds in ((dict,), (list,), (dict, list), (SON,)):
        kinds_label = ",".join(kind.__name__ for kind in kinds)
        for document_class in (dict, SON):
            for aggregate in (False, True):
                calls = []
                decoded = _options(
                    collection,
                    [
                        (
                            kind,
                            _marker(
                                kind.__name__, calls, namespace=collection.full_name
                            ),
                        )
                        for kind in kinds
                    ],
                    document_class=document_class,
                )
                result[f"{kinds_label}-{document_class.__name__}-{aggregate}"] = {
                    **_read(decoded, aggregate=aggregate),
                    "calls": calls,
                }
    return BSONObservation([OID]).value(result)


def uuid_dispatch(collection):
    _reset(
        collection,
        [{"_id": OID, "old": Binary(UUID_BYTES, 3), "standard": Binary(UUID_BYTES, 4)}],
    )
    result = {}
    for representation in REPRESENTATIONS:
        for custom in (False, True):
            for aggregate in (False, True):
                calls = []
                decoders = (
                    [
                        (UUID, _marker("uuid", calls, namespace=collection.full_name)),
                        (
                            Binary,
                            _marker("binary", calls, namespace=collection.full_name),
                        ),
                    ]
                    if custom
                    else []
                )
                decoded = _options(collection, decoders, representation=representation)
                observed = _read(decoded, aggregate=aggregate)
                # EJSON under UNSPECIFIED cannot encode UUID. Observe the
                # public class and exact bits, without production converters.
                if observed["ok"]:
                    observed["rows"] = [
                        {
                            key: {"decoded_uuid": _describe(value)}
                            if isinstance(value, UUID)
                            else value
                            for key, value in row.items()
                        }
                        for row in observed["rows"]
                    ]
                result[f"{representation}-{custom}-{aggregate}"] = {
                    **observed,
                    "calls": calls,
                }
    return BSONObservation([OID]).value(result)


def dbref_callback_order(collection):
    result = {}
    for label, value in (
        ("ordered", {"$ref": "c", "$id": 1}),
        ("reversed", {"$id": 1, "$ref": "c"}),
        ("sdk", DBRef("c", 1)),
        ("scope_ref", Code("x", {"$ref": "c", "$id": 1})),
    ):
        _reset(collection, [{"_id": OID, "v": value}])
        for aggregate in (False, True):
            calls = []

            def identity(value, observed=calls):
                observed.append(_describe(value, namespace=collection.full_name))
                return value

            decoded = _options(collection, [(int, identity), (str, identity)])
            result[f"{label}-{aggregate}"] = {
                **_read(decoded, aggregate=aggregate),
                "calls": calls,
            }
    return BSONObservation([OID]).value(result)


def hint_orders(collection):
    _reset(collection, [{"_id": 2, "v": 0}, {"_id": 1, "v": 1}, {"_id": 3, "v": 1}])
    collection.create_index("v", name="v_1")
    query = {"$expr": {"$gt": [{"$divide": [1, "$v"]}, 0]}}
    result = {}
    for name, hint in (
        ("none", None),
        ("id_name", "_id_"),
        ("id_list", [("_id", 1)]),
        ("id_map", {"_id": 1}),
        ("v_name", "v_1"),
        ("v_list", [("v", 1)]),
    ):
        for locale in (None, "simple", "en"):
            for direction in (1, -1):
                seen = []
                options = {"sort": [("_id", direction)], "batch_size": 1}
                if hint is not None:
                    options["hint"] = hint
                if locale is not None:
                    options["collation"] = {"locale": locale}
                cursor = None
                outcome = {"ok": True}
                try:
                    cursor = collection.find(query, **options)
                    seen.extend(row["_id"] for row in cursor)
                except Exception as error:
                    details = getattr(error, "details", None) or {}
                    outcome = {
                        "ok": False,
                        "exception": type(error).__name__,
                        "code": getattr(error, "code", None),
                        "errmsg": details.get("errmsg", str(error)).replace(
                            collection.full_name, "<namespace>"
                        ),
                    }
                finally:
                    if cursor is not None:
                        cursor.close()
                result[f"{name}-{locale}-{direction}"] = {**outcome, "seen": seen}
    return BSONObservation([OID]).value(result)


def decoder_response_phases(collection):
    _reset(collection, [{"_id": ObjectId(f"{n:024x}"), "n": n} for n in range(1, 4)])
    result = {}
    for aggregate in (False, True):
        for batch in (0, 1, 2):
            for fail in (False, True):
                calls = []

                def number(value, observed=calls, should_fail=fail):
                    observed.append({"int": value})
                    if should_fail and value == DECODER_FAILURE_VALUE:
                        message = "decoder rejected two"
                        raise ValueError(message)
                    return value

                def metadata(value, observed=calls):
                    observed.append({"cursor": "active" if value else "exhausted"})
                    return value

                def text(value, observed=calls):
                    observed.append(
                        {
                            "str": "<namespace>"
                            if value == collection.full_name
                            else value
                        }
                    )
                    return value

                decoded = _options(
                    collection, [(int, number), (Int64, metadata), (str, text)]
                )
                phase = "open"
                cursor = None
                rows = []
                outcome = {"ok": True}
                try:
                    cursor = (
                        decoded.aggregate([], batchSize=batch)
                        if aggregate
                        else decoded.find({}, batch_size=batch)
                    )
                    phase = "iterate"
                    rows.extend(row["n"] for row in cursor)
                except Exception as error:
                    outcome = {
                        "ok": False,
                        "phase": phase,
                        "exception": type(error).__name__,
                        "message": str(error),
                    }
                finally:
                    if cursor is not None:
                        cursor.close()
                result[f"{aggregate}-{batch}-{fail}"] = {
                    **outcome,
                    "rows": rows,
                    "calls": calls,
                }
    return BSONObservation([OID]).value(result)


def decoder_container_results(collection, *, use_to_list=False):
    _reset(collection, [{"_id": ObjectId(f"{n:024x}"), "n": n} for n in range(1, 4)])
    result = {}
    for kind in ("find", "find_limit", "aggregate"):
        for batch in (1, 2):
            for mode in ("empty", "none", "marker"):
                calls = []

                def metadata(value, observed=calls):
                    observed.append({"cursor": "active" if value else "exhausted"})
                    return value

                def transform(value, observed=calls, selected=mode):
                    observed.append(
                        {"container": type(value).__name__, "size": len(value)}
                    )
                    if selected == "empty":
                        return []
                    if selected == "none":
                        return None
                    return ["generated"] if value else []

                decoded = _options(
                    collection,
                    [(dict if mode == "none" else list, transform), (Int64, metadata)],
                )
                cursor = None
                rows = []
                outcome = {"ok": True}
                try:
                    if kind == "aggregate":
                        cursor = decoded.aggregate([], batchSize=batch)
                    else:
                        cursor = decoded.find(
                            {},
                            batch_size=batch,
                            **({"limit": 1} if kind == "find_limit" else {}),
                        )
                    if use_to_list:
                        rows.extend(cursor.to_list())
                        outcome["next"] = {"rows": cursor.to_list(1)}
                    else:
                        iterator = iter(cursor)
                        rows.extend(iterator)
                        try:
                            extra = next(iterator)
                            outcome["next"] = {"returned": extra}
                        except StopIteration:
                            outcome["next"] = {"stopped": True}
                except Exception as error:
                    outcome = {
                        "ok": False,
                        "exception": type(error).__name__,
                        "message": str(error),
                    }
                finally:
                    if cursor is not None:
                        cursor.close()
                result[f"{kind}-{batch}-{mode}"] = {
                    **outcome,
                    "rows": rows,
                    "calls": calls,
                }
    return BSONObservation([OID]).value(result)


def decoder_container_lists(collection):
    return decoder_container_results(collection, use_to_list=True)


def single_document_dispatch(collection):
    result = {}
    for present in (False, True):
        _reset(
            collection,
            [
                {
                    "_id": OID,
                    "n": 1,
                    "s": "seed",
                    "nested": {"n": 2},
                    "code": Code("x", {"$id": 3, "$ref": "other"}),
                    "uuid": Binary(UUID_BYTES, 4),
                }
            ]
            if present
            else [{"_id": ObjectId("000000000000000000000000"), "n": 4}],
        )
        for representation in ("unspecified", "standard"):
            for kind in (int, str, dict, list, Code, UUID, Binary, Int64):
                calls = []
                decoded = _options(
                    collection,
                    [
                        (
                            kind,
                            _marker(
                                kind.__name__, calls, namespace=collection.full_name
                            ),
                        )
                    ],
                    representation=representation,
                )
                try:
                    row = decoded.find_one({"_id": OID})
                    observed = {"ok": True, "row": row, "root_type": type(row).__name__}
                except Exception as error:
                    observed = {
                        "ok": False,
                        "exception": type(error).__name__,
                        "message": str(error),
                    }
                result[f"{present}-{representation}-{kind.__name__}"] = {
                    **observed,
                    "calls": calls,
                }
    return BSONObservation([OID]).value(result)


def single_document_errors(collection):
    half = 8 * 1024 * 1024
    payload = "x" * (half - 100)
    _reset(collection, [{"_id": OID, "p": payload, "n": 0}])
    result = {}
    queries = {}
    for offset in (-1, 0, 1):
        concat = {"$concat": ["$p", "$p", "x" * (200 + offset)]}
        queries[f"string-{offset}"] = {"$expr": {"$gte": [{"$strLenBytes": concat}, 0]}}
    queries["null-tail"] = {
        "$expr": {"$eq": [{"$concat": ["$p", "$p", "x" * 200, None]}, None]}
    }
    queries["divide"] = {"$expr": {"$gt": [{"$divide": [1, "$n"]}, 0]}}
    for name, query in queries.items():
        calls = []

        def text(value, observed=calls):
            observed.append(
                "<namespace>" if value == collection.full_name else len(value)
            )
            return value

        decoded = _options(collection, [(str, text)])
        try:
            row = decoded.find_one(query)
            observed = {
                "ok": True,
                "bytes": None if row is None else len(BSON.encode(row)),
            }
        except Exception as error:
            details = getattr(error, "details", None) or {}
            observed = {
                "ok": False,
                "exception": type(error).__name__,
                "code": getattr(error, "code", None),
                "codeName": details.get("codeName"),
                "errmsg": details.get("errmsg", str(error)).replace(
                    collection.full_name, "<namespace>"
                ),
            }
        result[name] = {**observed, "calls": calls}
    return BSONObservation([OID]).value(result)


def decoder_empty_batch_resumption(collection):
    _reset(collection, [{"_id": ObjectId(f"{n:024x}"), "n": n} for n in range(1, 4)])
    calls = []

    def empty(rows):
        calls.append([row["n"] for row in rows])
        return []

    decoded = _options(collection, [(list, empty)])
    cursor = decoded.find({}, batch_size=1, sort=[("n", 1)])
    try:
        rounds = [list(cursor), cursor.to_list(1), cursor.to_list(1), cursor.to_list(1)]
        return BSONObservation([OID]).value({"rounds": rounds, "calls": calls})
    finally:
        cursor.close()


def mixed_decoder_consumption(collection):
    _reset(collection, [{"_id": ObjectId(f"{n:024x}"), "n": n} for n in range(1, 5)])
    result = {}
    for aggregate in (False, True):
        for batch in (1, 2):
            for none_at in (2, 3):
                for count in (1, 2):

                    def transform(row, target=none_at):
                        return None if row["n"] == target else {"n": row["n"]}

                    decoded = _options(collection, [(dict, transform)])
                    cursor = (
                        decoded.aggregate([], batchSize=batch)
                        if aggregate
                        else decoded.find({}, batch_size=batch)
                    )
                    try:
                        iterator = iter(cursor)
                        prefix = [next(iterator) for _ in range(count)]
                        result[f"{aggregate}-{batch}-{none_at}-{count}"] = {
                            "prefix": prefix,
                            "rest": cursor.to_list(),
                        }
                    finally:
                        cursor.close()
    return BSONObservation([OID]).value(result)


def expanded_batch_client_limit(collection):
    _reset(collection, [{"_id": ObjectId(f"{n:024x}"), "n": n} for n in range(1, 6)])
    result = {}
    for aggregate in (False, True):
        for batch in (1, 2):
            calls = []

            def expand(rows, observed=calls):
                observed.append([row["n"] for row in rows])
                return [row["n"] for row in rows for _ in range(2)]

            decoded = _options(collection, [(list, expand)])
            cursor = (
                decoded.aggregate([{"$limit": 3}], batchSize=batch)
                if aggregate
                else decoded.find({}, batch_size=batch, limit=3)
            )
            try:
                result[f"{aggregate}-{batch}"] = {"rows": list(cursor), "calls": calls}
            finally:
                cursor.close()
    return BSONObservation([OID]).value(result)


def unsized_batch_decoder(collection):
    _reset(collection, [{"_id": ObjectId(f"{n:024x}"), "n": n} for n in range(1, 4)])
    result = {}
    for kind in ("find", "find_one", "aggregate"):
        calls = []

        def unsized(rows, observed=calls):
            observed.append([row["n"] for row in rows])
            return iter(rows)

        decoded = _options(collection, [(list, unsized)])
        cursor = None
        phase = "open"
        try:
            if kind == "find_one":
                rows = [decoded.find_one({})]
            else:
                cursor = (
                    decoded.aggregate([], batchSize=1)
                    if kind == "aggregate"
                    else decoded.find({}, batch_size=1)
                )
                phase = "iterate"
                rows = list(cursor)
            observed = {"ok": True, "rows": rows}
        except Exception as error:
            observed = {
                "ok": False,
                "phase": phase,
                "exception": type(error).__name__,
                "message": str(error),
            }
        finally:
            if cursor is not None:
                cursor.close()
        result[kind] = {**observed, "calls": calls}
    return BSONObservation([OID]).value(result)


SEMANTIC_BSON_DISPATCH_CASES = (
    ("semantic_codec_scalar_dispatch", scalar_dispatch),
    ("semantic_codec_single_pass", decoder_single_pass),
    ("semantic_codec_container_dispatch", container_dispatch),
    ("semantic_codec_uuid_dispatch", uuid_dispatch),
    ("semantic_codec_dbref_order", dbref_callback_order),
    ("semantic_hint_order_forms", hint_orders),
    ("semantic_codec_response_phases", decoder_response_phases),
    ("semantic_codec_container_results", decoder_container_results),
    ("semantic_codec_container_lists", decoder_container_lists),
    ("semantic_codec_single_document", single_document_dispatch),
    ("semantic_codec_single_errors", single_document_errors),
    ("semantic_codec_empty_batch_resume", decoder_empty_batch_resumption),
    ("semantic_codec_mixed_consumption", mixed_decoder_consumption),
    ("semantic_codec_expanded_limit", expanded_batch_client_limit),
    ("semantic_codec_unsized_batch", unsized_batch_decoder),
)
