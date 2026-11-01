"""Search client BSON rejection, independently of Atlas server capabilities."""

# ruff: noqa: B023, PLR0913 - immediate observations with independent request fields

import asyncio
import inspect

from decimal import Decimal

from pymongo.operations import SearchIndexModel

from tests.differential.semantic_bson_admission_cases import (
    UUID_VALUE,
    VALUES,
    _observe,
)
from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_bson_namespace_cases import _client


SEED = [{"_id": 0, "v": 1}]
ROUTES = ("create", "create-many", "list", "update", "drop")
DEFINITION = {"mappings": {"dynamic": True}}
# These are all rejected at BSON encoding with the driver's own UNSPECIFIED options.
REJECTED = VALUES[:15]


def _invoke(collection, route, *, comment=None, value=None, field=None, session=None):
    kwargs = {"comment": comment, "session": session}
    if field == "maxTimeMS":
        local = isinstance(collection.codec_options.type_registry, tuple)
        kwargs["max_time_ms" if local else "maxTimeMS"] = value
        field = None
    if route in {"create", "create-many"}:
        fields = {"name": "subject", "definition": DEFINITION}
        if field is not None:
            fields[field] = value
        model = SearchIndexModel(**fields)
        if route == "create":
            return collection.create_search_index(model, **kwargs)
        return collection.create_search_indexes([model], **kwargs)
    name = value if field == "name" else "subject"
    if route == "list":
        return collection.list_search_indexes(name, **kwargs)
    if route == "update":
        definition = value if field == "definition" else DEFINITION
        return collection.update_search_index(name, definition, **kwargs)
    return collection.drop_search_index(name, **kwargs)


def search_rejected_headers(collection):
    output = {}
    for label, factory in REJECTED:
        for route in ROUTES:
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, comment=factory()),
                cursor=route == "list",
            )
    return output


def search_rejected_operands(collection):
    output = {}
    for label, factory in REJECTED:
        for route, field in (
            ("create", "definition"),
            ("create", "name"),
            ("create", "type"),
            ("create-many", "definition"),
            ("update", "definition"),
            ("update", "name"),
            ("drop", "name"),
            ("list", "name"),
        ):
            output[f"{label}/{route}/{field}"] = _observe(
                collection,
                lambda: _invoke(collection, route, value=factory(), field=field),
                cursor=route == "list",
            )
    return output


def search_rejected_deadlines(collection):
    output = {}
    for label, factory in REJECTED:
        for route in ("create", "create-many", "update", "drop"):
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, value=factory(), field="maxTimeMS"),
            )
    return output


def search_uuid_owners(collection):
    output = {}
    for mode in (
        "unspecified",
        "standard",
        "pythonLegacy",
        "javaLegacy",
        "csharpLegacy",
    ):
        calls = []

        class Factory(dict):
            def __init__(self, *args, **kwargs):
                calls.append("factory")
                super().__init__(*args, **kwargs)

        def decoder(value):
            calls.append("decoder")
            return value

        target = _options(
            collection, [(int, decoder)], document_class=Factory, representation=mode
        )
        for route in ROUTES:
            calls.clear()
            output[f"{mode}/{route}"] = _observe(
                collection,
                lambda: _invoke(target, route, comment=UUID_VALUE),
                cursor=route == "list",
            )
            output[f"{mode}/{route}"]["callbacks"] = list(calls)
    return output


def search_precedence(collection):
    output = {}
    session = _client(collection).start_session()
    end = getattr(session, "end_session", None) or session.close
    ended = end()
    if inspect.isawaitable(ended):
        asyncio.run_coroutine_threadsafe(ended, collection.loop).result(timeout=10)
    for route in ROUTES:
        output[f"ended/{route}"] = _observe(
            collection,
            lambda: _invoke(collection, route, comment=UUID_VALUE, session=session),
            cursor=route == "list",
        )
    for label, model in (("object", object()), ("int", 5), ("none", None)):
        output[f"model/{label}"] = _observe(
            collection,
            lambda: collection.create_search_index(
                model, comment=UUID_VALUE, session=session
            ),
        )
        output[f"models/{label}"] = _observe(
            collection,
            lambda: collection.create_search_indexes(
                [model], comment=UUID_VALUE, session=session
            ),
        )
    for route in ("create", "create-many", "update", "drop"):
        for label, deadline, comment in (
            ("deadline-first", "\ud800", Decimal(1)),
            ("comment-last", Decimal(1), "\ud800"),
        ):
            output[f"competing/{route}/{label}"] = _observe(
                collection,
                lambda: _invoke(
                    collection,
                    route,
                    field="maxTimeMS",
                    value=deadline,
                    comment=comment,
                ),
            )
    for label, model in (
        ("name-first", {"definition": {"x": UUID_VALUE}, "name": Decimal(1)}),
        ("definition-before-type", {"type": "\ud800", "definition": {"x": Decimal(1)}}),
        ("none-fields", {"definition": {"x": UUID_VALUE}, "type": None, "name": None}),
        ("keyword-key", {"definition": DEFINITION, 1: UUID_VALUE}),
    ):
        output[f"mapping/{label}"] = _observe(
            collection, lambda: collection.create_search_index(model)
        )
    return output


SEMANTIC_BSON_SEARCH_INPUT_CASES = (
    ("semantic_bson_search_rejected_headers", search_rejected_headers),
    ("semantic_bson_search_rejected_operands", search_rejected_operands),
    ("semantic_bson_search_uuid_owners", search_uuid_owners),
    ("semantic_bson_search_rejected_deadlines", search_rejected_deadlines),
    ("semantic_bson_search_precedence", search_precedence),
)
