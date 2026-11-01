"""Index administration request ownership, admission phase and catalog effects."""

# ruff: noqa: B023 - witnesses execute before the next loop iteration

import asyncio
import inspect

from decimal import Decimal

from bson import Binary

from tests.differential.semantic_bson_admission_cases import VALUES, _observe, _view
from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_bson_insert_cases import _selected


SEED = [{"_id": 0, "v": 1}]
ROUTES = ("list", "information", "drop", "drop-all")


def _reset(collection):
    collection.drop()
    collection.insert_many(SEED)
    collection.create_index("v", name="subject")


def _invoke(collection, route, *, comment=None, session=None):
    if route == "list":
        cursor = collection.list_indexes(comment=comment, session=session)

        def names():
            try:
                for index in cursor:
                    yield index["name"]
            finally:
                cursor.close()

        return names()
    if route == "information":
        return sorted(collection.index_information(comment=comment, session=session))
    if route == "drop":
        return collection.drop_index("subject", comment=comment, session=session)
    return collection.drop_indexes(comment=comment, session=session)


def _observe_index(collection, route, call):
    observed = _observe(collection, call, cursor=route == "list")
    observed["index_names"] = sorted(
        index["name"] for index in collection.list_indexes()
    )
    return observed


def index_admin_headers(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in ROUTES:
            _reset(collection)
            value = factory()
            output[f"{label}/{route}"] = _observe_index(
                collection,
                route,
                lambda: _invoke(collection, route, comment=value),
            )
    return output


def index_admin_uuid_owners(collection):
    output = {}
    value = next(factory() for label, factory in VALUES if label == "uuid")
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        target = _selected(collection, mode)
        for route in ROUTES:
            _reset(collection)
            output[f"{mode}/{route}"] = _observe_index(
                collection,
                route,
                lambda: _invoke(target, route, comment=value),
            )
    return output


def index_admin_precedence(collection):
    output = {}
    for target in (5, {}, (), [], [("v", 1)], "subject", "missing"):
        _reset(collection)
        output[f"{target!r}/invalid-comment"] = _observe_index(
            collection,
            "drop",
            lambda: collection.drop_index(target, comment=Decimal(1)),
        )
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    for route in ROUTES:
        _reset(collection)
        session = client.start_session()
        end = getattr(session, "end_session", None) or session.close
        ended = end()
        if inspect.isawaitable(ended):
            asyncio.run_coroutine_threadsafe(ended, collection.loop).result(timeout=10)
        output[f"closed/{route}"] = _observe_index(
            collection,
            route,
            lambda: _invoke(collection, route, comment=Decimal(1), session=session),
        )
    return output


def index_admin_codec_owners(collection):
    output = {}
    for route in ROUTES:
        for failing in (False, True):
            _reset(collection)
            collection.create_index(
                "extra",
                name="metadata",
                partialFilterExpression={"v": Binary(bytes(range(16)), 4)},
            )
            calls = []

            class Factory(dict):
                def __init__(self, *args, **kwargs):
                    calls.append("factory")
                    if failing:
                        message = "factory witness"
                        raise RuntimeError(message)
                    super().__init__(*args, **kwargs)

                def __setitem__(self, key, value):
                    calls.append({"field": key, "value": _view(value)})
                    super().__setitem__(key, value)

            def decoder(value):
                calls.append("decoder")
                return value

            target = _options(
                collection,
                [(int, decoder)],
                representation="standard",
                document_class=Factory,
            )

            def invoke():
                if route == "list":
                    cursor = target.list_indexes()
                    try:
                        documents = list(cursor)
                    finally:
                        cursor.close()
                    entry = next(d for d in documents if d["name"] == "metadata")
                    return {
                        "root": type(entry).__name__,
                        "key": type(entry["key"]).__name__,
                        "payload": _view(entry["partialFilterExpression"]["v"]),
                    }
                if route == "information":
                    entry = target.index_information()["metadata"]
                    return {
                        "root": type(entry).__name__,
                        "key": type(entry["key"]).__name__,
                        "payload": _view(entry["partialFilterExpression"]["v"]),
                    }
                return _invoke(target, route)

            output[f"{route}/factory-fails-{failing}"] = _observe_index(
                collection,
                route if route != "list" else "information",
                invoke,
            )
            output[f"{route}/factory-fails-{failing}"]["callbacks"] = list(calls)
    return output


SEMANTIC_BSON_INDEX_ADMIN_CASES = (
    ("semantic_index_admin_headers", index_admin_headers),
    ("semantic_index_admin_uuid_owners", index_admin_uuid_owners),
    ("semantic_index_admin_precedence", index_admin_precedence),
    ("semantic_index_admin_codec_owners", index_admin_codec_owners),
)
