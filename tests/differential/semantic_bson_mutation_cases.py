"""Client mutation packets and shape/encoding precedence against live MongoDB."""

# ruff: noqa: B023 - every closure is invoked within its iteration

import asyncio
import inspect

from decimal import Decimal

from bson import Binary, DBRef

from tests.differential.semantic_bson_admission_cases import (
    UUID_VALUE,
    VALUES,
    _observe,
    _view,
)
from tests.differential.semantic_bson_insert_cases import NAIVE_ID, _selected
from tests.support_write_parity import BSONObservation, public_error


SEED = [{"_id": 0, "v": 1, "items": [{"x": 1}]}]
ROUTES = (
    "update",
    "update_many",
    "replace",
    "delete",
    "delete_many",
    "find_update",
    "find_replace",
    "find_delete",
)


def _reset(collection):
    collection.drop()
    collection.insert_many(SEED)


def _call(collection, route, query, value=2, **kwargs):  # noqa: PLR0911 - explicit public routes
    if route.startswith("find_") and "bypass_document_validation" in kwargs:
        kwargs["bypassDocumentValidation"] = kwargs.pop("bypass_document_validation")
    if route == "update":
        return collection.update_one(query, {"$set": {"v": value}}, **kwargs)
    if route == "update_many":
        return collection.update_many(query, {"$set": {"v": value}}, **kwargs)
    if route == "pipeline":
        return collection.update_one(
            query, [{"$set": {"v": {"$literal": value}}}], **kwargs
        )
    if route == "replace":
        return collection.replace_one(query, {"_id": 0, "v": value}, **kwargs)
    if route == "delete":
        return collection.delete_one(query, **kwargs)
    if route == "delete_many":
        return collection.delete_many(query, **kwargs)
    if route == "find_update":
        kwargs.setdefault("return_document", True)
        return collection.find_one_and_update(query, {"$set": {"v": value}}, **kwargs)
    if route == "find_replace":
        kwargs.setdefault("return_document", True)
        return collection.find_one_and_replace(query, {"_id": 0, "v": value}, **kwargs)
    return collection.find_one_and_delete(query, **kwargs)


def mutation_payloads(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in (
            "update",
            "update_many",
            "pipeline",
            "replace",
            "find_update",
            "find_replace",
        ):
            _reset(collection)
            output[f"{label}/{route}"] = _observe(
                collection, lambda: _call(collection, route, {"_id": 0}, factory())
            )
    return output


def mutation_filters(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in ROUTES:
            _reset(collection)
            output[f"{label}/{route}"] = _observe(
                collection, lambda: _call(collection, route, {"v": factory()})
            )
    return output


def mutation_options(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in (
            "comment",
            "find_comment",
            "let",
            "find_let",
            "array",
            "find_array",
        ):
            _reset(collection)
            value = factory()

            def invoke():
                if route == "comment":
                    return _call(collection, "update", {}, comment=value)
                if route == "find_comment":
                    return _call(collection, "find_update", {}, comment=value)
                if route in {"let", "find_let"}:
                    method = (
                        collection.update_one
                        if route == "let"
                        else collection.find_one_and_update
                    )
                    return method(
                        {}, [{"$set": {"v": "$$arg"}}], let={"arg": {"$literal": value}}
                    )
                method = (
                    collection.update_one
                    if route == "array"
                    else collection.find_one_and_update
                )
                return method(
                    {},
                    {"$set": {"items.$[i].seen": True}},
                    array_filters=[{"i.x": value}],
                )

            output[f"{label}/{route}"] = _observe(collection, invoke)
        # Positive computed projections have their own D06/B08 acceptance.
        if label in {
            "decimal128",
            "binary0",
            "binary5",
            "bytes",
            "datetime",
            "tuple",
            "document",
        }:
            continue
        for route in ("find_update", "find_replace", "find_delete"):
            _reset(collection)
            output[f"{label}/projection/{route}"] = _observe(
                collection,
                lambda: _call(collection, route, {}, projection={"out": factory()}),
            )
    return output


def mutation_precedence(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for route in ROUTES:
        for label, query, value, kwargs in (
            ("null-filter", None, 2, {}),
            ("null-filter-and-value", None, Decimal(1), {}),
            ("comment-before-filter", {"v": 1 << 70}, 2, {"comment": "\ud800"}),
            ("comment-before-change", {}, 1 << 70, {"comment": Decimal(1)}),
            (
                "let-before-comment",
                {},
                2,
                {"let": {"arg": Decimal(1)}, "comment": "\ud800"},
            ),
            ("unknown-query-and-value", {"$unknown": 1}, Decimal(1), {}),
        ):
            _reset(collection)
            output[f"{label}/{route}"] = _observe(
                collection, lambda: _call(collection, route, query, value, **kwargs)
            )
    for route in ("update", "find_update", "replace", "find_replace"):
        for label, change in (
            ("empty", {}),
            ("empty-pipeline", []),
            ("scalar", 5),
            ("first-literal", {"v": 2}),
            ("mixed-keys", {"$set": {"v": 2}, "v": Decimal(1)}),
            ("later-dollar", {"v": Decimal(1), "$x": 2}),
            ("integer-key", {1: 2}),
        ):
            _reset(collection)
            method = {
                "update": collection.update_one,
                "find_update": collection.find_one_and_update,
                "replace": collection.replace_one,
                "find_replace": collection.find_one_and_replace,
            }[route]
            output[f"shape/{label}/{route}"] = _observe(
                collection, lambda: method({"v": Decimal(1)}, change)
            )
    for option, values in (
        ("upsert", (1, None, "yes", Decimal(1))),
        ("bypass_document_validation", (1, None, "yes", Decimal(1))),
        ("array_filters", (5, [5], ({},))),
    ):
        for index, value in enumerate(values):
            for route in ("update", "find_update"):
                _reset(collection)
                output[f"shape/{option}/{index}/{route}"] = _observe(
                    collection, lambda: _call(collection, route, {}, **{option: value})
                )
    for value in (1, "after", {}, []):
        _reset(collection)
        output[f"return/{type(value).__name__}"] = _observe(
            collection,
            lambda: _call(collection, "find_update", {}, return_document=value),
        )
    return output


def mutation_uuid_modes(collection):
    output = {}
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        selected = _selected(collection, mode)
        for route in (
            "update",
            "update_many",
            "pipeline",
            "replace",
            "find_update",
            "find_replace",
        ):
            _reset(selected)
            output[f"{mode}/{route}"] = _observe(
                selected, lambda: _call(selected, route, {"_id": 0}, UUID_VALUE)
            )
        for route in ("update", "replace", "find_update"):
            _reset(selected)
            selected.delete_many({})

            assigned = []

            def invoke():
                query = {"_id": UUID_VALUE}
                if route == "replace":
                    result = selected.replace_one(
                        query, {"_id": UUID_VALUE}, upsert=True
                    )
                    assigned.append(_view(result.upserted_id))
                    return result
                if route == "find_update":
                    return selected.find_one_and_update(
                        query, {"$set": {"v": 2}}, upsert=True, return_document=True
                    )
                result = selected.update_one(query, {"$set": {"v": 2}}, upsert=True)
                assigned.append(_view(result.upserted_id))
                return result

            observed = _observe(selected, invoke)
            observed["upserted_id"] = assigned
            output[f"{mode}/upsert/{route}"] = observed
    return output


def mutation_validation_errors(collection):
    output = {}
    for label, value in (
        ("reference", DBRef("items", 1)),
        ("binary0", Binary(b"x", 0)),
        ("binary5", Binary(b"x", 5)),
        ("date", NAIVE_ID),
        ("uuid", UUID_VALUE),
    ):
        for route in (
            "update",
            "update_many",
            "replace",
            "find_update",
            "find_replace",
        ):
            name = collection.name + "_validation"
            collection.database.drop_collection(name)
            base = collection.database.create_collection(
                name,
                validator={
                    "$jsonSchema": {"properties": {"v": {"bsonType": "string"}}},
                },
            )
            selected = _selected(base, "STANDARD")
            selected.insert_many(SEED, bypass_document_validation=True)
            errors = []

            def invoke():
                try:
                    return _call(selected, route, {"_id": 0}, value)
                except Exception as error:
                    errors.append(
                        public_error(
                            error,
                            BSONObservation(
                                SEED,
                                namespace=f"{selected.database.name}.{name}",
                                error_contract=True,
                            ),
                        )
                    )
                    raise

            observed = _observe(selected, invoke)
            observed["exception"] = errors[0]["family"]
            observed["error_contract"] = errors
            output[f"{label}/{route}"] = observed
    return output


def mutation_closed_sessions(collection):
    output = {}
    collection = _selected(collection, "UNSPECIFIED")
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    for route in ROUTES:
        for label, query, value, kwargs in (
            ("valid", {}, 2, {}),
            ("bad-value", {}, Decimal(1), {}),
            ("bad-filter", {"v": Decimal(1)}, 2, {}),
            ("null-filter", None, 2, {}),
            ("bad-let", {}, 2, {"let": 5}),
        ):
            _reset(collection)
            session = client.start_session()
            end = getattr(session, "end_session", None) or session.close
            ended = end()
            if inspect.isawaitable(ended):
                asyncio.run_coroutine_threadsafe(ended, collection.loop).result(
                    timeout=10
                )
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _call(
                    collection, route, query, value, session=session, **kwargs
                ),
            )
    for route in ("update", "update_many", "find_update"):
        for label, kwargs in (
            ("bad-upsert", {"upsert": 1}),
            ("bad-sort", {"sort": 5}),
        ):
            # update_one.sort is outside the default 4.9 profile. Its 4.19
            # session precedence has a separate explicit-profile control.
            if label == "bad-sort" and route != "find_update":
                continue
            _reset(collection)
            session = client.start_session()
            end = getattr(session, "end_session", None) or session.close
            ended = end()
            if inspect.isawaitable(ended):
                asyncio.run_coroutine_threadsafe(ended, collection.loop).result(
                    timeout=10
                )
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _call(collection, route, {}, session=session, **kwargs),
            )
    return output


SEMANTIC_BSON_MUTATION_CASES = (
    ("semantic_mutation_payloads", mutation_payloads),
    ("semantic_mutation_filters", mutation_filters),
    ("semantic_mutation_options", mutation_options),
    ("semantic_mutation_precedence", mutation_precedence),
    ("semantic_mutation_uuid_modes", mutation_uuid_modes),
    ("semantic_mutation_validation_errors", mutation_validation_errors),
    ("semantic_mutation_closed_sessions", mutation_closed_sessions),
)
