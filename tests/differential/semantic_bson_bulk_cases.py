"""Classic bulk BSON frames: preparation, family admission and partial writes."""

# ruff: noqa: B023 - each closure is invoked within its loop iteration

import asyncio
import inspect

from decimal import Decimal

from bson import Binary, DBRef
from pymongo import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne

from tests.differential.semantic_bson_admission_cases import (
    UUID_VALUE,
    VALUES,
    _observe,
)
from tests.differential.semantic_bson_insert_cases import NAIVE_ID, _selected
from tests.support_write_parity import BSONObservation, public_error


SEED = [{"_id": 0, "v": 1}, {"_id": 1, "v": 2}]
ROUTES = ("insert", "update", "update_many", "replace", "delete", "delete_many")


def _reset(collection):
    collection.drop()
    collection.insert_many(SEED)


def _models(route, value):
    if route == "insert":
        return [InsertOne({"_id": 2}), InsertOne({"_id": 3, "v": value})]
    if route == "replace":
        return [ReplaceOne({"_id": 0}, {"v": 3}), ReplaceOne({"_id": 1}, {"v": value})]
    if route in {"update", "update_many"}:
        model = UpdateOne if route == "update" else UpdateMany
        return [
            model({"_id": 0}, {"$set": {"v": 3}}),
            model({"_id": 1}, {"$set": {"v": value}}),
        ]
    model = DeleteOne if route == "delete" else DeleteMany
    return [model({"_id": 0}), model({"v": value})]


def bulk_packet_values(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in ROUTES:
            for ordered in (True, False):
                _reset(collection)
                output[f"{label}/{route}/{ordered}"] = _observe(
                    collection,
                    lambda: collection.bulk_write(
                        _models(route, factory()), ordered=ordered
                    ),
                )
    return output


def bulk_packet_headers(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in ("insert-comment", "update-comment", "insert-let", "mixed-let"):
            _reset(collection)
            value = factory()

            def invoke():
                if route == "insert-comment":
                    return collection.bulk_write([InsertOne({"_id": 2})], comment=value)
                if route == "update-comment":
                    return collection.bulk_write(
                        [UpdateOne({}, {"$set": {"v": 3}})], comment=value
                    )
                models = [InsertOne({"_id": 2})]
                if route == "mixed-let":
                    models.append(UpdateOne({}, [{"$set": {"v": "$$x"}}]))
                return collection.bulk_write(models, let={"x": {"$literal": value}})

            output[f"{label}/{route}"] = _observe(collection, invoke)
    return output


def bulk_packet_boundaries(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for ordered in (True, False):
        for label, factory in (
            (
                "prior-family",
                lambda: [InsertOne({"_id": 2}), *_models("update", Decimal(1))],
            ),
            (
                "later-insert-family",
                lambda: [*_models("update", Decimal(1)), InsertOne({"_id": 2})],
            ),
            (
                "encoding-after-invalid-id",
                lambda: [
                    InsertOne({"_id": []}),
                    InsertOne({"_id": 2, "v": Decimal(1)}),
                ],
            ),
            (
                "encoding-after-duplicate",
                lambda: [InsertOne({"_id": 0}), InsertOne({"_id": 2, "v": Decimal(1)})],
            ),
            (
                "semantic-duplicate",
                lambda: [InsertOne({"_id": 0}), InsertOne({"_id": 2})],
            ),
            ("semantic-id", lambda: [InsertOne({"_id": []}), InsertOne({"_id": 2})]),
            (
                "encoding-before-query",
                lambda: [
                    UpdateOne({"$unknown": 1}, {"$set": {"v": 3}}),
                    UpdateOne({}, {"$set": {"v": Decimal(1)}}),
                ],
            ),
            (
                "encoding-before-array-filter",
                lambda: [
                    UpdateOne({}, {"$set": {"v": 3}}, array_filters=[5]),
                    UpdateOne({}, {"$set": {"v": Decimal(1)}}),
                ],
            ),
            ("header-before-values", lambda: _models("insert", 1 << 70)),
            ("header-before-id", lambda: [InsertOne({"_id": []})]),
        ):
            _reset(collection)
            kwargs = {"ordered": ordered}
            if label.startswith("header-"):
                kwargs["comment"] = Decimal(1)
            output[f"{label}/{ordered}"] = _observe(
                collection, lambda: collection.bulk_write(factory(), **kwargs)
            )
    return output


def bulk_preparation_shapes(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for route in ("update", "replace"):
        for label, change in (
            ("empty", {}),
            ("empty-pipeline", []),
            ("scalar", 5),
            ("mixed", {"$set": {"v": Decimal(1)}, "v": 2}),
            ("later-dollar", {"v": Decimal(1), "$x": 2}),
            ("integer-key", {1: 2}),
        ):
            _reset(collection)
            document = {"v": 3}

            def invoke():
                model = UpdateOne if route == "update" else ReplaceOne
                return collection.bulk_write([InsertOne(document), model({}, change)])

            result = _observe(collection, invoke)
            result["assigned_id"] = "_id" in document
            output[f"{label}/{route}"] = result
    return output


def bulk_uuid_modes(collection):
    output = {}
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        selected = _selected(collection, mode)
        for route in ROUTES:
            _reset(selected)
            output[f"{mode}/{route}"] = _observe(
                selected, lambda: selected.bulk_write(_models(route, UUID_VALUE))
            )
    return output


SEMANTIC_BSON_BULK_CASES = (
    ("semantic_bulk_packet_values", bulk_packet_values),
    ("semantic_bulk_packet_headers", bulk_packet_headers),
    ("semantic_bulk_packet_boundaries", bulk_packet_boundaries),
    ("semantic_bulk_preparation_shapes", bulk_preparation_shapes),
    ("semantic_bulk_uuid_modes", bulk_uuid_modes),
)


def bulk_byte_frames(collection):
    """Observe large frames without serializing payloads into the golden model."""
    output = {}
    for ordered in (True, False):
        for label, invalid_after_overflow in (
            ("lookahead-invalid", False),
            ("after-overflow", True),
        ):
            _reset(collection)
            payload = "x" * 16_000_000
            models = [
                InsertOne({"_id": 2, "payload": payload}),
                InsertOne({"_id": 3, "payload": payload}),
            ]
            if invalid_after_overflow:
                models.append(InsertOne({"_id": 4, "payload": payload}))
            models.append(InsertOne({"_id": 5, "v": Decimal(1)}))
            try:
                collection.bulk_write(models, ordered=ordered)
                observed = {"ok": True}
            except Exception as error:
                observed = {
                    "ok": False,
                    "exception": type(error).__name__,
                    "code": getattr(error, "code", None),
                }
            observed["ids"] = [
                d["_id"] for d in collection.find({}, {"_id": 1}).sort("_id")
            ]
            observed["payload_lengths"] = [
                len(d["payload"])
                for d in collection.find({"payload": {"$exists": True}}, {"payload": 1})
            ]
            output[f"{label}/{ordered}"] = observed
    return output


def bulk_client_phases(collection):

    output = {}
    collection = _selected(collection, "UNSPECIFIED")
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    for label in ("valid", "bad-value", "bad-change", "bad-let"):
        _reset(collection)
        document = {"v": 3}
        session = client.start_session()
        end = getattr(session, "end_session", None) or session.close
        ended = end()
        if inspect.isawaitable(ended):
            asyncio.run_coroutine_threadsafe(ended, collection.loop).result(timeout=10)

        def invoke():
            change = (
                {}
                if label == "bad-change"
                else {"$set": {"v": Decimal(1) if label == "bad-value" else 3}}
            )
            return collection.bulk_write(
                [InsertOne(document), UpdateOne({}, change)],
                session=session,
                let=5 if label == "bad-let" else None,
            )

        observed = _observe(collection, invoke)
        observed["assigned_id"] = "_id" in document
        output[f"ended/{label}"] = observed
    for label in ("empty", "late-invalid-model"):
        _reset(collection)
        document = {"v": 3}
        observed = _observe(
            collection,
            lambda: collection.bulk_write(
                [] if label == "empty" else [InsertOne(document), object()]
            ),
        )
        observed["assigned_id"] = "_id" in document
        output[label] = observed
    _reset(collection)
    output["null-ordered-duplicate"] = _observe(
        collection,
        lambda: collection.bulk_write(
            [InsertOne({"_id": 0}), InsertOne({"_id": 2})], ordered=None
        ),
    )
    for option in ("ordered", "bypass_document_validation"):
        for label, value in (
            ("number", 1),
            ("null", None),
            ("string", "yes"),
            ("decimal", Decimal(1)),
        ):
            _reset(collection)
            output[f"{option}/{label}"] = _observe(
                collection,
                lambda: collection.bulk_write(
                    [InsertOne({"_id": 2})], **{option: value}
                ),
            )
    return output


SEMANTIC_BSON_BULK_CASES += (
    ("semantic_bulk_byte_frames", bulk_byte_frames),
    ("semantic_bulk_client_phases", bulk_client_phases),
)


def bulk_upsert_replies(collection):
    output = {}
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        selected = _selected(collection, mode)
        for label, identifier in (
            ("uuid", UUID_VALUE),
            ("binary0", Binary(b"x", 0)),
            ("binary5", Binary(b"x", 5)),
            ("date", NAIVE_ID),
            ("reference", DBRef("items", 1)),
        ):
            for model in (UpdateOne, UpdateMany, ReplaceOne):
                _reset(selected)
                change = {"v": 3} if model is ReplaceOne else {"$set": {"v": 3}}
                output[f"{mode}/{label}/{model.__name__}"] = _observe(
                    selected,
                    lambda: selected.bulk_write(
                        [model({"_id": identifier}, change, upsert=True)]
                    ),
                )
    return output


def bulk_error_replies(collection):
    output = {}
    selected = _selected(collection, "STANDARD")
    for label, value in (
        ("uuid", UUID_VALUE),
        ("binary0", Binary(b"x", 0)),
        ("binary5", Binary(b"x", 5)),
        ("date", NAIVE_ID),
        ("reference", DBRef("items", 1)),
    ):
        for ordered in (True, False):
            _reset(selected)
            selected.create_index("v", unique=True)
            errors = []

            def invoke():
                try:
                    return selected.bulk_write(
                        [
                            InsertOne({"_id": 2, "v": value}),
                            InsertOne({"_id": 3, "v": value}),
                        ],
                        ordered=ordered,
                    )
                except Exception as error:
                    errors.append(
                        public_error(
                            error,
                            BSONObservation(
                                SEED,
                                namespace=f"{selected.database.name}.{selected.name}",
                                error_contract=True,
                            ),
                        )
                    )
                    raise

            observed = _observe(selected, invoke)
            observed["error_contract"] = errors
            output[f"{label}/{ordered}"] = observed
    return output


SEMANTIC_BSON_BULK_CASES += (
    ("semantic_bulk_upsert_replies", bulk_upsert_replies),
    ("semantic_bulk_error_replies", bulk_error_replies),
)
