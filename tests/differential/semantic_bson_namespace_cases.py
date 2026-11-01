"""Namespace client validation, BSON frames and prepared DDL effects."""

# ruff: noqa: B023 - each operation is observed within the iteration

import asyncio
import inspect

from decimal import Decimal

from bson import Code

from tests.differential.semantic_bson_admission_cases import UUID_VALUE, _observe, _view
from tests.differential.semantic_bson_dispatch_cases import _options


SEED = [{"_id": 0, "v": 1}]
SOURCE = "namespace_source"
TARGET = "namespace_target"


def _client(collection):
    client = getattr(collection, "_client", None)
    return collection.database.client if client is None else client


def _reset(collection):
    _client(collection).drop_database(collection.database.name)
    collection.insert_many(SEED)
    source = collection.database[SOURCE]
    source.insert_many(SEED)
    return source


def _state(collection, observed):
    try:
        database = collection.database
        observed["namespaces"] = {
            name: _view(list(database[name].find().sort("_id")))
            for name in sorted(database.list_collection_names())
            if name != collection.name
        }
    except Exception as error:
        observed["namespaces"] = {"exception": type(error).__name__}
    return observed


def namespace_rename_targets(collection):
    output = {}
    for label, value in (
        ("int", 5),
        ("none", None),
        ("empty", ""),
        ("leading-dot", ".target"),
        ("trailing-dot", "target."),
        ("double-dot", "a..b"),
        ("dollar", "a$b"),
        ("ordinary", TARGET),
        ("dotted", "namespace.dotted"),
        ("unicode", "namespace_ñ"),
        ("surrogate", "namespace_\ud800"),
        ("nul", "namespace_\x00target"),
        ("same", SOURCE),
        ("code", Code(TARGET)),
        ("code-scope", Code(TARGET, {"x": Decimal(1)})),
        ("existing", TARGET),
    ):
        source = _reset(collection)
        if label == "existing":
            collection.database[TARGET].insert_one({"_id": 9})

        def invoke():
            # The local API deliberately returns Collection; observe request/effects.
            source.rename(value)

        output[label] = _state(collection, _observe(collection, invoke))
    return output


def namespace_drop_sources(collection):
    output = {}
    for label, name in (
        ("ordinary", SOURCE),
        ("missing", "namespace_missing"),
        ("surrogate", "namespace_\ud800"),
        ("code", Code(SOURCE)),
        ("code-scope", Code(SOURCE, {"x": Decimal(1)})),
    ):
        _reset(collection)
        output[label] = _state(
            collection,
            _observe(
                collection, lambda: collection.database.get_collection(name).drop()
            ),
        )
    return output


def namespace_codec_owners(collection):
    output = {}
    for route in ("drop", "rename"):
        for mode in (
            "unspecified",
            "standard",
            "pythonLegacy",
            "javaLegacy",
            "csharpLegacy",
        ):
            source = _reset(collection)
            calls = []

            class Factory(dict):
                def __init__(self, *args, **kwargs):
                    calls.append("factory")
                    message = "namespace helpers ignore collection response codecs"
                    raise RuntimeError(message)

            def decoder(value):
                calls.append("decoder")
                return value

            source = _options(
                source, [(int, decoder)], document_class=Factory, representation=mode
            )

            def invoke():
                if route == "drop":
                    source.drop()
                else:
                    source.rename(TARGET)

            output[f"{route}/{mode}"] = _state(collection, _observe(collection, invoke))
            output[f"{route}/{mode}"]["callbacks"] = list(calls)
            _reset(collection)
            coded = collection.database.get_collection(Code(SOURCE, {"x": UUID_VALUE}))
            coded = _options(coded, representation=mode)

            def invoke_coded():
                if route == "drop":
                    coded.drop()
                else:
                    coded.rename(Code(TARGET, {"x": UUID_VALUE}))

            output[f"scope-uuid/{route}/{mode}"] = _state(
                collection,
                _observe(collection, invoke_coded),
            )
    return output


def namespace_precedence(collection):
    output = {}
    for route, target in (
        ("drop", None),
        ("rename", TARGET),
        ("rename", ""),
        ("rename", 5),
        ("rename", "namespace_\ud800"),
    ):
        source = _reset(collection)
        session = _client(collection).start_session()
        end = getattr(session, "end_session", None) or session.close
        ended = end()
        if inspect.isawaitable(ended):
            asyncio.run_coroutine_threadsafe(ended, collection.loop).result(timeout=10)

        def invoke():
            if route == "drop":
                source.drop(session=session)
            else:
                source.rename(target, session=session)

        output[f"closed/{route}/{target!r}"] = _state(
            collection, _observe(collection, invoke)
        )
    for label, name in (
        ("empty", ""),
        ("nul", "namespace_\x00target"),
        ("code", Code(SOURCE)),
        ("int", 5),
        ("none", None),
        ("array", []),
        ("document", {}),
        ("binary", b"ab"),
        ("bool", True),
    ):
        _reset(collection)

        def invoke():
            collection.database.command({"drop": name})

        output[f"raw-drop/{label}"] = _state(collection, _observe(collection, invoke))
    return output


SEMANTIC_BSON_NAMESPACE_CASES = (
    ("semantic_namespace_rename_targets", namespace_rename_targets),
    ("semantic_namespace_drop_sources", namespace_drop_sources),
    ("semantic_namespace_codec_owners", namespace_codec_owners),
    ("semantic_namespace_precedence", namespace_precedence),
)
