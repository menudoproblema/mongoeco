"""Client insert packets, error precedence and BSON admission versus mongod."""


# ruff: noqa: B023 - operation closures are invoked within each iteration

from datetime import datetime
from decimal import Decimal

from bson import Binary, DBRef, Decimal128, Int64
from bson.binary import UuidRepresentation

from tests.differential.semantic_bson_admission_cases import (
    SEED,
    UUID_VALUE,
    VALUES,
    _observe,
    _reset,
    _view,
)
from tests.support_write_parity import BSONObservation, public_error


__all__ = ["SEED", "SEMANTIC_BSON_INSERT_CASES"]

# Insert results retain the caller's naive, sub-millisecond ID, unlike storage.
NAIVE_ID = datetime(1970, 1, 1, 0, 0, 1, 123456)  # noqa: DTZ001 - native ID input


def _selected(collection, mode):
    options = collection.codec_options
    local = isinstance(options.type_registry, tuple)
    names = {
        "UNSPECIFIED": "unspecified",
        "STANDARD": "standard",
        "PYTHON_LEGACY": "pythonLegacy",
        "JAVA_LEGACY": "javaLegacy",
        "CSHARP_LEGACY": "csharpLegacy",
    }
    return collection.with_options(
        codec_options=type(options)(
            uuid_representation=names[mode]
            if local
            else getattr(UuidRepresentation, mode)
        )
    )


def insert_packet_values(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for many in (False, True):
            _reset(collection)
            document = {"_id": 2, "v": factory()}
            output[f"{label}/{'many' if many else 'one'}"] = _observe(
                collection,
                lambda: (
                    collection.insert_many([{"_id": 1}, document])
                    if many
                    else collection.insert_one(document)
                ),
            )
    return output


def insert_packet_boundaries(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for ordered in (True, False):
        for label, documents in (
            ("encoding-after-valid", [{"_id": 1}, {"_id": 2, "v": Decimal(1)}]),
            ("encoding-before-server-id", [{"_id": []}, {"_id": 2, "v": Decimal(1)}]),
            ("server-id-before-valid", [{"_id": []}, {"_id": 2}]),
            ("server-duplicate-before-valid", [{"_id": 0}, {"_id": 2}]),
            ("encoding-after-duplicate", [{"_id": 0}, {"_id": 2, "v": Decimal(1)}]),
        ):
            _reset(collection)
            output[f"{label}/{ordered}"] = _observe(
                collection,
                lambda: collection.insert_many(documents, ordered=ordered),
            )
    for many in (False, True):
        for label, options in (
            ("bad-id-and-value", {"document": {"_id": [], "v": Decimal(1)}}),
            ("bad-comment", {"document": {"_id": 2}, "comment": Decimal(1)}),
            ("bad-comment-and-id", {"document": {"_id": []}, "comment": {1}}),
            (
                "comment-before-overflow",
                {
                    "document": {"_id": 2, "v": 1 << 70},
                    "comment": "\ud800",
                },
            ),
            (
                "comment-before-surrogate",
                {
                    "document": {"_id": 2, "v": "\ud800"},
                    "comment": Decimal(1),
                },
            ),
            ("generated-id", {"document": {"v": Decimal(1)}}),
        ):
            _reset(collection)
            document = dict(options["document"])
            kwargs = {key: value for key, value in options.items() if key != "document"}
            observed = _observe(
                collection,
                lambda: (
                    collection.insert_many([document], **kwargs)
                    if many
                    else collection.insert_one(document, **kwargs)
                ),
            )
            observed["input_has_id"] = "_id" in document
            output[f"{label}/{'many' if many else 'one'}"] = observed
    for kind in ("list", "generator", "raising-generator"):
        _reset(collection)
        first = {"v": 1}

        def documents():
            yield first
            if kind == "raising-generator":
                message = "fixture generator failed"
                raise RuntimeError(message)
            yield []

        supplied = [first, []] if kind == "list" else documents()
        observed = _observe(collection, lambda: collection.insert_many(supplied))
        observed["first_input_has_id"] = "_id" in first
        output[f"parameter-preparation/{kind}"] = observed
    return output


def insert_packet_uuid(collection):
    output = {}
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        selected = _selected(collection, mode)
        for many in (False, True):
            _reset(selected)
            document = {"_id": 2, "v": UUID_VALUE}
            output[f"{mode}/{'many' if many else 'one'}"] = _observe(
                selected,
                lambda: (
                    selected.insert_many([document])
                    if many
                    else selected.insert_one(document)
                ),
            )
    return output


def insert_packet_ids(collection):
    output = {}
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        selected = _selected(collection, mode)
        for label, value in (
            ("uuid", UUID_VALUE),
            ("uuid-document", {"u": UUID_VALUE}),
            ("datetime", NAIVE_ID),
            (
                "datetime-document",
                {"t": NAIVE_ID},
            ),
            ("long", Int64(1)),
            ("decimal128", Decimal128("1.0")),
            ("binary0", Binary(b"ab", 0)),
            ("binary5", Binary(b"ab", 5)),
        ):
            for many in (False, True):
                _reset(selected)
                document = {"_id": value}
                output[f"{mode}/{label}/{'many' if many else 'one'}"] = _observe(
                    selected,
                    lambda: (
                        selected.insert_many([document])
                        if many
                        else selected.insert_one(document)
                    ),
                )
    return output


def insert_packet_errors(collection):
    output = {}
    for label, value in (
        ("reference", DBRef("items", 1)),
        ("binary0", Binary(b"x", 0)),
        ("binary5", Binary(b"x", 5)),
        ("datetime", NAIVE_ID),
        ("uuid", UUID_VALUE),
    ):
        for many in (False, True):
            name = collection.name + "_validation"
            collection.database.drop_collection(name)
            subject = collection.database.create_collection(
                name,
                validator={
                    "$jsonSchema": {"properties": {"a": {"bsonType": "string"}}},
                },
            )
            selected = _selected(subject, "STANDARD")
            details = []
            families = []

            def invoke():
                try:
                    document = {"_id": 2, "a": value}
                    return (
                        selected.insert_many([document])
                        if many
                        else selected.insert_one(document)
                    )
                except Exception as error:
                    details.append(_view(error.details))
                    families.append(public_error(error, BSONObservation([]))["family"])
                    raise

            observed = _observe(selected, invoke)
            observed["exception"] = families[0]
            observed["details"] = details
            output[f"{label}/{'many' if many else 'one'}"] = observed
    return output


SEMANTIC_BSON_INSERT_CASES = (
    ("semantic_insert_packet_values", insert_packet_values),
    ("semantic_insert_packet_boundaries", insert_packet_boundaries),
    ("semantic_insert_packet_uuid", insert_packet_uuid),
    ("semantic_insert_packet_ids", insert_packet_ids),
    ("semantic_insert_packet_errors", insert_packet_errors),
)
