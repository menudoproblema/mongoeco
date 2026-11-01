"""Public collection/index catalog admission, complete requests and precedence."""

# ruff: noqa: B023 - closures execute within each iteration

import asyncio
import inspect

from decimal import Decimal

from pymongo import IndexModel

from tests.differential.semantic_bson_admission_cases import (
    UUID_VALUE,
    VALUES,
    _observe,
    _view,
)
from tests.differential.semantic_bson_dispatch_cases import _options
from tests.differential.semantic_bson_insert_cases import _selected


SEED = [{"_id": 0, "v": 1}]


def _target(collection):
    collection.drop()
    collection.insert_many(SEED)
    target = collection.database.with_options(codec_options=collection.codec_options)[
        collection.name + "_catalog"
    ]
    target.drop()
    return target


def _state(target, observed):
    exists = bool(list(target.database.list_collections(filter={"name": target.name})))
    observed["aux_exists"] = exists
    observed["indexes"] = (
        sorted(
            [
                {"name": index["name"], "key": list(index["key"].items())}
                for index in target.list_indexes()
            ],
            key=lambda row: row["name"],
        )
        if exists
        else []
    )
    return observed


def catalog_payloads(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in ("create", "index", "indexes"):
            target = _target(collection)
            value = factory()

            def invoke():
                if route == "create":
                    return target.database.create_collection(
                        target.name, validator={"v": value}
                    )
                if route == "index":
                    return target.create_index(
                        "v", partialFilterExpression={"v": value}
                    )
                return target.create_indexes(
                    [
                        IndexModel("prefix"),
                        IndexModel("v", partialFilterExpression={"v": value}),
                    ]
                )

            output[f"{label}/{route}"] = _state(target, _observe(collection, invoke))
    return output


def catalog_headers(collection):
    collection = _selected(collection, "UNSPECIFIED")
    output = {}
    for label, factory in VALUES:
        for route in ("create", "index", "indexes"):
            target = _target(collection)
            value = factory()

            def invoke():
                if route == "create":
                    return target.database.create_collection(target.name, comment=value)
                if route == "index":
                    return target.create_index("v", comment=value)
                return target.create_indexes([IndexModel("v")], comment=value)

            output[f"{label}/{route}"] = _state(target, _observe(collection, invoke))
    return output


def catalog_precedence(collection):
    output = {}
    cases = (
        "existing-invalid-bson",
        "existing-skip-check",
        "size-before-bson",
        "bson-before-capped",
        "index-keys-before-bson",
        "index-collation-before-bson",
        "index-ttl-bson",
        "model-shape-before-bson",
        "bad-bson-after-valid-model",
        "empty-models",
        "tuple-models",
    )
    for label in cases:
        target = _target(collection)
        if label.startswith("existing"):
            target.insert_one({"_id": 0})

        def invoke():  # noqa: PLR0911 - explicit independent native precedence witnesses
            if label == "existing-invalid-bson":
                return target.database.create_collection(
                    target.name, validator={"v": Decimal(1)}
                )
            if label == "existing-skip-check":
                return target.database.create_collection(
                    target.name, check_exists=False, validator={"v": Decimal(1)}
                )
            if label == "size-before-bson":
                return target.database.create_collection(
                    target.name, size=object(), validator={"v": Decimal(1)}
                )
            if label == "bson-before-capped":
                return target.database.create_collection(
                    target.name, capped=5, validator={"v": Decimal(1)}
                )
            if label == "index-keys-before-bson":
                return target.create_index([], comment=Decimal(1))
            if label == "index-collation-before-bson":
                return target.create_index("v", collation=5, comment=Decimal(1))
            if label == "index-ttl-bson":
                return target.create_index("v", expireAfterSeconds=Decimal(1))
            if label == "model-shape-before-bson":
                return target.create_indexes(
                    [
                        IndexModel("v", partialFilterExpression={"v": Decimal(1)}),
                        object(),
                    ]
                )
            if label == "bad-bson-after-valid-model":
                return target.create_indexes(
                    [
                        IndexModel("prefix"),
                        IndexModel("v", partialFilterExpression={"v": Decimal(1)}),
                    ]
                )
            if label == "empty-models":
                return target.create_indexes([])
            return target.create_indexes((IndexModel("v"),))

        output[label] = _state(target, _observe(collection, invoke))
    return output


def catalog_uuid_owners(collection):
    output = {}
    collection = _selected(collection, "UNSPECIFIED")
    for mode in (
        "UNSPECIFIED",
        "STANDARD",
        "PYTHON_LEGACY",
        "JAVA_LEGACY",
        "CSHARP_LEGACY",
    ):
        options = _selected(collection, mode).codec_options
        for route in ("create-database", "create-explicit", "index", "indexes"):
            target = _target(collection)
            selected = target.with_options(codec_options=options)

            def invoke():
                if route == "create-database":
                    return target.database.with_options(
                        codec_options=options
                    ).create_collection(target.name, validator={"v": UUID_VALUE})
                if route == "create-explicit":
                    return target.database.create_collection(
                        target.name, codec_options=options, validator={"v": UUID_VALUE}
                    )
                if route == "index":
                    return selected.create_index(
                        "v", partialFilterExpression={"v": UUID_VALUE}
                    )
                return selected.create_indexes(
                    [IndexModel("v", partialFilterExpression={"v": UUID_VALUE})]
                )

            observed = _observe(collection, invoke)
            if observed["ok"] and route.startswith("create"):
                info = next(
                    iter(target.database.list_collections(filter={"name": target.name}))
                )
                observed["validator"] = _view(info["options"]["validator"])
            output[f"{mode}/{route}"] = _state(target, observed)
    return output


def catalog_closed_sessions(collection):
    output = {}
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    for route in ("create", "index", "indexes"):
        for invalid in (False, True):
            target = _target(collection)
            session = client.start_session()
            end = getattr(session, "end_session", None) or session.close
            ended = end()
            if inspect.isawaitable(ended):
                asyncio.run_coroutine_threadsafe(ended, collection.loop).result(
                    timeout=10
                )

            def invoke():
                value = Decimal(1) if invalid else 1
                if route == "create":
                    return target.database.create_collection(
                        target.name, session=session, validator={"v": value}
                    )
                if route == "index":
                    return target.create_index(
                        "v", session=session, partialFilterExpression={"v": value}
                    )
                return target.create_indexes(
                    [IndexModel("v", partialFilterExpression={"v": value})],
                    session=session,
                )

            output[f"{route}/{invalid}"] = _state(target, _observe(collection, invoke))
    return output


def catalog_create_options(collection):
    output = {}
    for label, options in (
        ("size-bool", {"size": True}),
        ("size-float", {"size": 5.5}),
        ("size-string", {"size": "512"}),
        ("capped-size-string", {"capped": True, "size": "1024"}),
        ("capped-size-zero", {"capped": True, "size": 0}),
        ("capped-max-zero", {"capped": True, "size": 1024, "max": 0}),
        ("size-decimal", {"size": Decimal(512)}),
        ("size-none", {"size": None}),
        ("size-negative", {"size": -1}),
        ("size-zero", {"size": 0}),
        ("capped-missing-size", {"capped": True}),
        ("capped-number", {"capped": 5}),
        ("capped-string", {"capped": "yes"}),
        ("max-bool", {"max": True}),
        ("max-float", {"max": 5.5}),
        ("max-string", {"max": "5"}),
        ("existing-skip-check-valid", {"check_exists": False}),
    ):
        target = _target(collection)
        if label == "existing-skip-check-valid":
            target.insert_one({"_id": 0})
        output[label] = _state(
            target,
            _observe(
                collection,
                lambda: target.database.create_collection(target.name, **options),
            ),
        )
    return output


def catalog_create_idempotence(collection):
    output = {}
    for label, original, requested in (
        ("empty", {}, {}),
        ("default-capped", {}, {"capped": False}),
        ("default-images", {}, {"changeStreamPreAndPostImages": {"enabled": False}}),
        ("same-validator", {"validator": {"v": 1}}, {"validator": {"v": 1}}),
        ("different-validator", {"validator": {"v": 1}}, {"validator": {"v": 2}}),
        ("empty-validator", {}, {"validator": {}}),
        ("explicit-default", {}, {"validationLevel": "strict"}),
        ("omit-explicit-default", {"validationLevel": "strict"}, {}),
        (
            "validator-order",
            {"validator": {"v": 1, "a": 1}},
            {"validator": {"a": 1, "v": 1}},
        ),
        (
            "option-order",
            {"validator": {"v": 1}, "validationLevel": "moderate"},
            {"validationLevel": "moderate", "validator": {"v": 1}},
        ),
        ("comment-control", {}, {"comment": "repeat"}),
    ):
        target = _target(collection)
        target.database.create_collection(target.name, **original)
        output[label] = _state(
            target,
            _observe(
                collection,
                lambda: target.database.create_collection(
                    target.name, check_exists=False, **requested
                ),
            ),
        )
    return output


def catalog_codec_owners(collection):
    output = {}
    for route in ("create", "index", "indexes", "command-indexes"):
        for fail in (False, True):
            target = _target(collection)
            events = []

            class Document(dict):
                def __init__(self):
                    events.append(["new"])
                    if fail:
                        message = "document factory rejected reply"
                        raise RuntimeError(message)
                    super().__init__()

                def __setitem__(self, key, value):
                    events.append(["set", key])
                    super().__setitem__(key, value)

            def decode(value):
                events.append(["decode", value])
                return value

            options = _options(
                target, [(int, decode)], document_class=Document
            ).codec_options
            selected = target.with_options(codec_options=options)
            db = target.database.with_options(codec_options=options)
            returned = []

            def invoke():
                if route == "create":
                    result = db.create_collection(target.name)
                    returned.append(result.codec_options.document_class is Document)
                    return result
                if route == "index":
                    return selected.create_index("v")
                if route == "indexes":
                    return selected.create_indexes([IndexModel("v")])
                return db.command(
                    {
                        "createIndexes": target.name,
                        "indexes": [IndexModel("v").document],
                    }
                )

            observed = _state(target, _observe(collection, invoke))
            observed["events"] = events
            observed["returned_factory"] = returned
            output[f"{route}/{fail}"] = observed
    return output


def catalog_validation_shapes(collection):
    output = {}
    for field in (
        "validator",
        "validationLevel",
        "validationAction",
        "collation",
        "size",
    ):
        values = (None,) if field in {"collation", "size"} else (None, 5, [], "bad")
        for value in values:
            for route in ("public", "command"):
                target = _target(collection)

                def invoke():
                    if route == "public":
                        return target.database.create_collection(
                            target.name, **{field: value}
                        )
                    return target.database.command(
                        {"create": target.name, field: value}
                    )

                output[f"{field}/{type(value).__name__}/{route}"] = _state(
                    target, _observe(collection, invoke)
                )
    return output


SEMANTIC_BSON_CATALOG_CASES = (
    ("semantic_catalog_payloads", catalog_payloads),
    ("semantic_catalog_headers", catalog_headers),
    ("semantic_catalog_precedence", catalog_precedence),
    ("semantic_catalog_uuid_owners", catalog_uuid_owners),
    ("semantic_catalog_closed_sessions", catalog_closed_sessions),
    ("semantic_catalog_create_options", catalog_create_options),
    ("semantic_catalog_create_idempotence", catalog_create_idempotence),
    ("semantic_catalog_codec_owners", catalog_codec_owners),
    ("semantic_catalog_validation_shapes", catalog_validation_shapes),
)
