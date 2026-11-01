"""Database command BSON admission, caller options and response ownership."""

# ruff: noqa: B023 - each closure executes within its iteration

import asyncio
import inspect

from decimal import Decimal

from tests.differential.semantic_bson_admission_cases import (
    UUID_VALUE,
    VALUES,
    _observe,
)
from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_bson_insert_cases import _selected


SEED = [{"_id": 0, "v": 1}, {"_id": 1, "v": 2}]


def _reset(collection):
    collection.drop()
    collection.insert_many(SEED)


def command_payloads(collection):
    output = {}
    for label, factory in VALUES:
        for route in ("find", "insert", "update", "aggregate", "create", "indexes"):
            _reset(collection)
            auxiliary = collection.name + "_command_options"
            collection.database.drop_collection(auxiliary)
            value = factory()

            def invoke():
                commands = {
                    "find": {"find": collection.name, "filter": {"v": value}},
                    "insert": {
                        "insert": collection.name,
                        "documents": [{"_id": 2}, {"_id": 3, "v": value}],
                    },
                    "update": {
                        "update": collection.name,
                        "updates": [{"q": {"_id": 0}, "u": {"$set": {"v": value}}}],
                    },
                    "aggregate": {
                        "aggregate": collection.name,
                        "pipeline": [{"$set": {"v": {"$literal": value}}}],
                        "cursor": {},
                    },
                    "create": {"create": auxiliary, "validator": {"v": value}},
                    "indexes": {
                        "createIndexes": auxiliary,
                        "indexes": [
                            {
                                "key": {"v": 1},
                                "name": "v_1",
                                "partialFilterExpression": {"v": value},
                            }
                        ],
                    },
                }
                return collection.database.command(commands[route])

            observed = _observe(collection, invoke)
            observed["aux_exists"] = bool(
                list(collection.database.list_collections(filter={"name": auxiliary}))
            )
            output[f"{label}/{route}"] = observed
    return output


def command_keyword_frames(collection):
    output = {}
    for label, factory in VALUES:
        for route in ("ping-comment", "find-let", "mapping-keywords"):
            _reset(collection)
            command = {"find": collection.name, "filter": {"_id": 0}}
            value = factory()

            def invoke():
                if route == "ping-comment":
                    return collection.database.command("ping", comment=value)
                if route == "find-let":
                    return collection.database.command(
                        "find", find=collection.name, let={"arg": {"$literal": value}}
                    )
                return collection.database.command(command, comment=value)

            observed = _observe(collection, invoke)
            if route == "mapping-keywords":
                observed["caller_keys"] = list(command)
            output[f"{label}/{route}"] = observed
    return output


def command_precedence(collection):
    output = {}
    for label, spec, kwargs in (
        ("unknown", {"unknown_command": 1}, {}),
        ("unknown-and-invalid-bson", {"unknown_command": 1, "v": Decimal(1)}, {}),
        (
            "id-and-invalid-bson",
            {
                "insert": collection.name,
                "documents": [{"_id": []}, {"_id": 2, "v": Decimal(1)}],
            },
            {},
        ),
        (
            "query-and-invalid-bson",
            {"find": collection.name, "filter": {"$unknown": Decimal(1)}},
            {},
        ),
        (
            "comment-before-overflow",
            {"ping": 1, "comment": Decimal(1), "v": 1 << 70},
            {},
        ),
        (
            "overflow-before-comment",
            {"ping": 1, "v": 1 << 70, "comment": Decimal(1)},
            {},
        ),
        ("blank-name", "", {}),
        ("numeric-name", {1: 1}, {}),
        ("scalar-name", 5, {}),
        (
            "keyword-overrides",
            {"find": collection.name, "filter": {"_id": 0}},
            {"filter": {"v": Decimal(1)}},
        ),
    ):
        _reset(collection)
        output[label] = _observe(
            collection, lambda: collection.database.command(spec, **kwargs)
        )
    return output


def command_uuid_options(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        options = _selected(collection, mode).codec_options
        selected_db = collection.database.with_options(codec_options=options)
        for source in ("database", "explicit"):
            _reset(collection)
            kwargs = {} if source == "database" else {"codec_options": options}
            output[f"{mode}/{source}"] = _observe(
                collection,
                lambda: selected_db.command(
                    {
                        "insert": collection.name,
                        "documents": [{"_id": 2, "v": UUID_VALUE}],
                    },
                    **kwargs,
                ),
            )
    return output


def command_error_controls(collection):
    output = {}
    for label, kwargs in (
        ("check-true", {}),
        ("check-false", {"check": False}),
        ("allowable-code", {"allowable_errors": [59]}),
        (
            "allowable-message",
            {"allowable_errors": ["no such command: 'unknown_command'"]},
        ),
    ):
        _reset(collection)

        def invoke():
            return collection.database.command({"unknown_command": 1}, **kwargs)

        output[label] = _observe(collection, invoke)
    return output


def command_codec_callbacks(collection):
    output = {}
    for source in ("database", "explicit"):
        for route in ("ping", "find", "aggregate", "distinct"):
            _reset(collection)
            events = []

            class ObservedDocument(dict):
                def __init__(self):
                    events.append(["new"])
                    super().__init__()

                def __setitem__(self, key, value):
                    events.append(["set", key])
                    super().__setitem__(key, value)

            def decode(value):
                events.append(["decode", value])
                return value

            options = _options(
                collection, [(int, decode)], document_class=ObservedDocument
            ).codec_options
            db = collection.database.with_options(codec_options=options)
            kwargs = {} if source == "database" else {"codec_options": options}
            spec = (
                {"ping": 1}
                if route == "ping"
                else (
                    {"find": collection.name, "filter": {}}
                    if route == "find"
                    else {"distinct": collection.name, "key": "v"}
                    if route == "distinct"
                    else {"aggregate": collection.name, "pipeline": [], "cursor": {}}
                )
            )
            observed = _observe(collection, lambda: db.command(spec, **kwargs))
            observed["events"] = events
            output[f"{source}/{route}"] = observed
    return output


def command_closed_sessions(collection):
    output = {}
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    for label, spec, kwargs in (
        ("valid", {"ping": 1}, {}),
        ("invalid-bson", {"ping": 1, "bad": Decimal(1)}, {}),
        ("unknown", {"unknown_command": 1}, {}),
        ("scalar", 5, {}),
        ("numeric-key", {1: 1}, {}),
        ("keyword-before-session", {"ping": 1}, {"comment": Decimal(1)}),
    ):
        _reset(collection)
        session = client.start_session()
        end = getattr(session, "end_session", None) or session.close
        ended = end()
        if inspect.isawaitable(ended):
            asyncio.run_coroutine_threadsafe(ended, collection.loop).result(timeout=10)
        observed = _observe(
            collection,
            lambda: collection.database.command(spec, session=session, **kwargs),
        )
        if isinstance(spec, dict):
            observed["caller_keys"] = list(spec)
        output[label] = observed
    return output


SEMANTIC_BSON_COMMAND_CASES = (
    ("semantic_command_payloads", command_payloads),
    ("semantic_command_keyword_frames", command_keyword_frames),
    ("semantic_command_precedence", command_precedence),
    ("semantic_command_uuid_options", command_uuid_options),
    ("semantic_command_error_controls", command_error_controls),
    ("semantic_command_codec_callbacks", command_codec_callbacks),
    ("semantic_command_closed_sessions", command_closed_sessions),
)
