"""Database listing/validation admission and command-owned codec options."""

# ruff: noqa: B023 - each closure runs before the next loop iteration

import asyncio
import inspect

from collections import UserDict
from decimal import Decimal

from bson import Binary, Decimal128

from tests.differential.semantic_bson_admission_cases import VALUES, _observe, _view
from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_bson_insert_cases import _selected


SEED = [{"_id": 0, "v": 1}]
ROUTES = ("names", "collections", "validate")


def _listing_cursor(database, collection_name, **kwargs):
    cursor = database.list_collections(**kwargs)

    def names():
        try:
            for document in cursor:
                name = document["name"]
                yield "<collection>" if name == collection_name else name
        finally:
            cursor.close()

    return names()


def _invoke(collection, route, **kwargs):
    database = collection.database
    if route == "names":
        return [
            "<collection>" if name == collection.name else name
            for name in sorted(database.list_collection_names(**kwargs))
        ]
    if route == "collections":
        return _listing_cursor(database, collection.name, **kwargs)
    return {"valid": database.validate_collection(collection.name, **kwargs)["valid"]}


def admin_read_headers(collection):
    output = {}
    for label, factory in VALUES:
        for route in ROUTES:
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, comment=factory()),
                cursor=route == "collections",
            )
    return output


def admin_read_filters(collection):
    output = {}
    for label, factory in VALUES:
        for route in ("names", "collections"):
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, filter={"name": factory()}),
                cursor=route == "collections",
            )
    return output


def admin_read_validate_fields(collection):
    output = {}
    for label, factory in (
        *VALUES,
        ("none", lambda: None),
        ("bool", lambda: True),
        ("int", lambda: 1),
        ("text", lambda: "yes"),
        ("false", lambda: False),
        ("zero", lambda: 0),
        ("negative-zero", lambda: -0.0),
        ("empty-text", lambda: ""),
        ("empty-array", list),
        ("empty-document", dict),
        ("decimal-zero", lambda: Decimal128("0")),
        ("decimal-nan", lambda: Decimal128("NaN")),
    ):
        for field in ("scandata", "full", "background"):
            output[f"{label}/{field}"] = _observe(
                collection,
                lambda: _invoke(
                    collection,
                    "validate",
                    **(
                        {"full": True, field: factory()}
                        if field == "background"
                        else {field: factory()}
                    ),
                ),
            )
    output["full-background"] = _observe(
        collection,
        lambda: _invoke(collection, "validate", full=True, background=True),
    )
    return output


def admin_read_precedence(collection):
    output = {}
    for filter_value in (5, [], "bad", None, {}):
        for route in ("names", "collections"):
            for invalid in (False, True):
                output[f"{filter_value!r}/{route}/invalid-{invalid}"] = _observe(
                    collection,
                    lambda: _invoke(
                        collection,
                        route,
                        filter=filter_value,
                        comment=Decimal(1) if invalid else 1,
                    ),
                    cursor=route == "collections",
                )
    for route in ("names", "collections"):
        output[f"mapping/{route}"] = _observe(
            collection,
            lambda: _invoke(
                collection, route, filter=UserDict({"name": collection.name})
            ),
            cursor=route == "collections",
        )
    for filter_value in (None, 5, []):

        def raw_listing():
            result = collection.database.command(
                {"listCollections": 1, "filter": filter_value}
            )
            return {"names": ["<collection>" for _ in result["cursor"]["firstBatch"]]}

        output[f"command-filter-{filter_value!r}"] = _observe(collection, raw_listing)
    for name in (5, "", "missing"):
        for invalid in (False, True):

            def invoke():
                value = collection.database.validate_collection(
                    name,
                    comment=Decimal(1) if invalid else 1,
                )
                return {"valid": value["valid"]}

            output[f"name-{name!r}/invalid-{invalid}"] = _observe(collection, invoke)
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    for route in ROUTES:
        session = client.start_session()
        end = getattr(session, "end_session", None) or session.close
        ended = end()
        if inspect.isawaitable(ended):
            asyncio.run_coroutine_threadsafe(ended, collection.loop).result(timeout=10)
        output[f"closed/{route}"] = _observe(
            collection,
            lambda: _invoke(collection, route, comment=Decimal(1), session=session),
            cursor=route == "collections",
        )
    return output


def admin_read_codec_owners(collection):
    output = {}
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        selected = _selected(collection, mode)
        database = collection.database.with_options(
            codec_options=selected.codec_options
        )
        for route in ROUTES:
            value = next(factory() for label, factory in VALUES if label == "uuid")
            output[f"uuid/{mode}/{route}"] = _observe(
                collection,
                lambda: _invoke(database[collection.name], route, comment=value),
                cursor=route == "collections",
            )
    for route in ROUTES:
        calls = []

        class Factory(dict):
            def __init__(self, *args, **kwargs):
                calls.append("factory")
                message = "inherited factory must not run"
                raise RuntimeError(message)

        def decoder(value):
            calls.append("decoder")
            return value

        target = _options(
            collection,
            [(int, decoder)],
            document_class=Factory,
            representation="standard",
        )
        database = collection.database.with_options(codec_options=target.codec_options)
        output[f"factory/{route}"] = _observe(
            collection,
            lambda: _invoke(database[collection.name], route),
            cursor=route == "collections",
        )
        output[f"factory/{route}"]["callbacks"] = list(calls)
    for command in ("collStats", "dbStats", "dbHash"):
        calls = []

        class Factory(dict):
            def __init__(self, *args, **kwargs):
                calls.append("factory")
                message = "administrative source reader used a public codec"
                raise RuntimeError(message)

        target = _options(collection, document_class=Factory)
        database = collection.database.with_options(codec_options=target.codec_options)

        def inspect_command():
            spec = {command: collection.name if command == "collStats" else 1}
            result = database.command(spec)
            return {"root": type(result).__name__, "ok": result["ok"]}

        output[f"source-reader/{command}"] = _observe(collection, inspect_command)
        output[f"source-reader/{command}"]["callbacks"] = list(calls)
    name = collection.name + "_metadata"
    auxiliary = collection.database[name]
    auxiliary.drop()
    collection.database.create_collection(
        name,
        validator={"v": {"$eq": Binary(bytes(range(16)), 4)}},
    )
    target = _options(collection, representation="standard")
    database = collection.database.with_options(codec_options=target.codec_options)

    def metadata():
        cursor = database.list_collections(filter={"name": name})
        try:
            row = next(iter(cursor))
        finally:
            cursor.close()
        return {
            "root": type(row).__name__,
            "value": _view(row["options"]["validator"]["v"]["$eq"]),
        }

    output["metadata/binary4"] = _observe(collection, metadata)
    auxiliary.drop()
    return output


SEMANTIC_BSON_ADMIN_READ_CASES = (
    ("semantic_admin_read_headers", admin_read_headers),
    ("semantic_admin_read_filters", admin_read_filters),
    ("semantic_admin_read_validate_fields", admin_read_validate_fields),
    ("semantic_admin_read_precedence", admin_read_precedence),
    ("semantic_admin_read_codec_owners", admin_read_codec_owners),
)
