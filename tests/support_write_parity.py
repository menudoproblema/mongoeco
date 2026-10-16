"""Independent public observations shared by native, direct and wire write tests."""

from __future__ import annotations

import inspect
import json
import re

from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from bson import ObjectId, json_util
from bson.binary import UuidRepresentation
from bson.errors import InvalidDocument
from pymongo import errors


if TYPE_CHECKING:
    from tests.differential.write_cases import WriteCase


SEMANTIC_ERRORS = (errors.OperationFailure, TypeError, ValueError, InvalidDocument)
RESULT_FIELDS = (
    "acknowledged",
    "inserted_id",
    "inserted_ids",
    "matched_count",
    "modified_count",
    "deleted_count",
    "upserted_id",
    "did_upsert",
    "inserted_count",
    "upserted_count",
    "upserted_ids",
)
BULK_COUNTS = ("nInserted", "nMatched", "nModified", "nRemoved", "nUpserted")
DUPLICATE_KEY_CODE = 11000


class BSONObservation:
    """Preserve BSON scalar types and embedded order; bind generated IDs bijectively."""

    def __init__(self, explicit: Any, *, namespace=None, error_contract=False):
        self.namespace = namespace
        self.error_contract = error_contract
        self.explicit = set()
        self.generated: dict[ObjectId, str] = {}
        self._collect(explicit)

    def _collect(self, value):
        if isinstance(value, ObjectId):
            self.explicit.add(value)
        elif isinstance(value, Mapping):
            for item in value.values():
                self._collect(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                self._collect(item)
        elif hasattr(value, "_doc"):
            self._collect(value._doc)

    def value(self, value, *, root=False):
        if isinstance(value, ObjectId) and value not in self.explicit:
            token = self.generated.setdefault(value, f"generated:{len(self.generated)}")
            return {"$oid_token": token}
        if isinstance(value, Mapping):
            keys = sorted(value) if root else value
            return {"$document": [[str(key), self.value(value[key])] for key in keys]}
        if isinstance(value, (list, tuple)):
            return [self.value(item) for item in value]
        return json.loads(
            json_util.dumps(
                value,
                json_options=json_util.CANONICAL_JSON_OPTIONS.with_options(
                    uuid_representation=UuidRepresentation.STANDARD
                ),
            )
        )

    def documents(self, documents):
        return [self.value(document, root=True) for document in documents]


def public_result(result, bson):
    if isinstance(result, list):
        return {"documents": bson.documents(result)}
    if isinstance(result, (bool, int, float)):
        return {"value": bson.value(result)}
    if result is None or isinstance(result, Mapping):
        return {"document": None if result is None else bson.value(result, root=True)}
    output = {}
    for field in RESULT_FIELDS:
        if hasattr(result, field):
            value = getattr(result, field)
            if field == "upserted_ids":
                output[field] = [
                    [int(k), bson.value(v)] for k, v in sorted(value.items())
                ]
            elif field.endswith("_id") or field == "inserted_ids":
                output[field] = bson.value(value)
            else:
                output[field] = value
    return output


def public_error(error, bson, *, command_namespace=None):
    if getattr(error, "timeout", False):
        raise error
    family = next(
        (
            kind.__name__
            for kind in (
                errors.BulkWriteError,
                errors.DuplicateKeyError,
                errors.WriteError,
                errors.OperationFailure,
                InvalidDocument,
                TypeError,
                ValueError,
            )
            if isinstance(error, kind)
        ),
        None,
    )
    if family is None:
        raise error
    details = getattr(error, "details", None) or {}
    observation = {
        "family": family,
        "code": getattr(error, "code", None),
        "codeName": details.get("codeName"),
        "labels": sorted(details.get("errorLabels", [])),
        "write_errors": sorted(
            [
                [entry["index"], entry.get("code")]
                for entry in details.get("writeErrors", [])
            ]
        ),
    }
    if command_namespace is not None:
        observation["command_details"] = command_error_details(
            error, bson, command_namespace
        )
    if isinstance(error, errors.BulkWriteError):
        observation["write_concern_errors"] = {
            "present": "writeConcernErrors" in details,
            "value": bson.value(details.get("writeConcernErrors")),
        }
        observation["write_error_messages"] = [
            error_message_shape(entry) for entry in details.get("writeErrors", [])
        ]
        observation["write_error_operations"] = error_operations(details, bson)
        observation["partial"] = {key: details.get(key, 0) for key in BULK_COUNTS}
        observation["partial"]["upserted"] = [
            [item["index"], bson.value(item["_id"])]
            for item in details.get("upserted", [])
        ]
    if bson.error_contract:
        observation["details"] = error_document_contract(
            getattr(error, "details", None), bson
        )
    return observation


def error_message_shape(document):
    message = document.get("errmsg")
    return {
        "present": "errmsg" in document,
        "type": type(message).__name__,
        "nonempty": isinstance(message, str) and bool(message),
        "formatted_exception": isinstance(message, str) and ", full error:" in message,
    }


def error_document_contract(document, bson):
    """Retain every field and BSON value, projecting diagnostic text separately."""
    if not isinstance(document, Mapping):
        return {"state": "none" if document is None else type(document).__name__}
    fields = {}
    for key, value in sorted(document.items()):
        if key == "errmsg":
            fields[key] = error_message_shape(document)
        elif key in {"writeErrors", "writeConcernErrors"} and isinstance(value, list):
            fields[key] = [error_document_contract(entry, bson) for entry in value]
        else:
            fields[key] = bson.value(value)
    result = {"state": "document" if document else "empty", "fields": fields}
    if document.get("code") == DUPLICATE_KEY_CODE:
        result["duplicate_diagnostic"] = duplicate_diagnostic(document, bson.namespace)
    return result


def duplicate_diagnostic(document, namespace):
    """Retain duplicate diagnostics while normalizing only the database namespace."""
    message = document.get("errmsg")
    match = re.fullmatch(
        r"(.*?)E11000 duplicate key error collection: (.*?) index: (.*?) dup key: (.*)",
        message if isinstance(message, str) else "",
        re.DOTALL,
    )
    if match is None:
        return {"valid": False}
    return {
        "valid": True,
        "prefix": match[1],
        "namespace_matches": match[2] == namespace,
        "index": match[3],
        "key": match[4],
    }


def command_error_details(error, bson, namespace):
    """Observe the command envelope, including absent, empty and malformed details."""
    details = getattr(error, "details", None)
    if not isinstance(details, Mapping):
        return {"state": "none" if details is None else type(details).__name__}
    message = details.get("errmsg")
    result = {
        "state": "document" if details else "empty",
        "fields": {
            key: {"present": key in details, "value": bson.value(details[key])}
            if key in details
            else {"present": False}
            for key in (
                "ok",
                "code",
                "codeName",
                "keyPattern",
                "keyValue",
                "hexEncoded",
                "collation",
            )
        },
        "errmsg": {
            "present": "errmsg" in details,
            "type": type(message).__name__,
            "nonempty": isinstance(message, str) and bool(message),
        },
    }
    if getattr(error, "code", None) == DUPLICATE_KEY_CODE:
        match = re.fullmatch(
            r"Plan executor error during findAndModify :: caused by :: "
            r"E11000 duplicate key error collection: (.*?) index: (.*?) dup key: (.*)",
            message if isinstance(message, str) else "",
            re.DOTALL,
        )
        result["duplicate_message"] = (
            {
                "valid": True,
                "namespace_matches": match[1] == namespace,
                "index": match[2],
                "key": match[3],
            }
            if match
            else {"valid": False}
        )
    return result


def error_operations(details, bson):
    """Observe presence separately: a missing op is distinct from op=None."""
    return [
        {
            "index": entry["index"],
            "present": "op" in entry,
            "value": bson.value(entry["op"], root=True) if "op" in entry else None,
        }
        for entry in sorted(
            details.get("writeErrors", []), key=lambda item: item["index"]
        )
    ]


def _index(index, collection):
    options = deepcopy(index)
    keys = [tuple(pair) for pair in options.pop("keys")]
    if (
        "partial_filter_expression" in options
        and "partial_filter_expression"
        not in inspect.signature(collection.create_index).parameters
    ):
        options["partialFilterExpression"] = options.pop("partial_filter_expression")
    return keys, options


def _caller(request, bson):
    if request.method in ("insert_one", "insert_many"):
        value = request.args[0]
        docs = value if request.method == "insert_many" else [value]
        return [bson.value(doc, root=True) for doc in docs]
    return None


def _probe(case, request):
    if case.probe:
        request.args[0]["nested"]["value"] = 2
        request.args[0]["items"] = [9]


def _observation(request, result, error, bson, documents):
    output = {
        "status": "error" if error else "ok",
        "documents": bson.documents(documents),
    }
    output["error" if error else "result"] = (
        public_error(
            error,
            bson,
            command_namespace=bson.namespace
            if request.method.startswith("find_one_and_")
            else None,
        )
        if error
        else public_result(result, bson)
    )
    caller = _caller(request, bson)
    if caller is not None:
        output["caller"] = caller
    return output


def observe_write_case(
    database, name, case: WriteCase, *, existing=False, error_contract=False
):
    """Run a specimen via a synchronous public collection; transport failures escape."""
    case = deepcopy(case)
    bson = BSONObservation(
        [case.documents, [r.args for r in case.requests]],
        namespace=f"{database.name}.{name}",
        error_contract=error_contract,
    )
    collection = (
        database[name] if existing else database.create_collection(name, **case.options)
    )
    if case.documents:
        collection.insert_many(case.documents, bypass_document_validation=True)
    for index in case.indexes:
        keys, options = _index(index, collection)
        collection.create_index(keys, **options)
    observations = []
    for request in case.requests:
        result = error = None
        try:
            if request.method == "command":
                command = deepcopy(request.args[0])
                command[next(iter(command))] = name
                result = raw_result(database.command(command), bson)
            else:
                result = getattr(collection, request.method)(
                    *request.args, **request.kwargs
                )
                if request.method == "aggregate":
                    result = list(result)
        except SEMANTIC_ERRORS as exc:
            error = exc
        _probe(case, request)
        documents = list(collection.find({}).sort("_id", 1))
        observations.append(_observation(request, result, error, bson, documents))
    return observations


async def observe_async_write_case(
    database, name, case: WriteCase, *, existing=False, error_contract=False
):
    """Run the identical specimen via an asynchronous public collection."""
    case = deepcopy(case)
    bson = BSONObservation(
        [case.documents, [r.args for r in case.requests]],
        namespace=f"{database.name}.{name}",
        error_contract=error_contract,
    )
    collection = (
        database[name]
        if existing
        else await database.create_collection(name, **case.options)
    )
    if case.documents:
        await collection.insert_many(case.documents, bypass_document_validation=True)
    for index in case.indexes:
        keys, options = _index(index, collection)
        await collection.create_index(keys, **options)
    observations = []
    for request in case.requests:
        result = error = None
        try:
            if request.method == "command":
                command = deepcopy(request.args[0])
                command[next(iter(command))] = name
                result = raw_result(await database.command(command), bson)
            else:
                result = await getattr(collection, request.method)(
                    *request.args, **request.kwargs
                )
                if request.method == "aggregate":
                    result = await result.to_list()
        except SEMANTIC_ERRORS as exc:
            error = exc
        _probe(case, request)
        documents = await collection.find({}).sort("_id", 1).to_list()
        observations.append(_observation(request, result, error, bson, documents))
    return observations


def assert_write_observations(actual, expected, case):
    """Compare native outputs, allowing only the declared unordered scan envelope."""
    actual = deepcopy(actual)
    expected = deepcopy(expected)
    if case.admissible_documents is not None:
        admissible = [
            BSONObservation([]).documents(docs) for docs in case.admissible_documents
        ]
        assert actual[-1]["documents"] in admissible
        assert expected[-1]["documents"] in admissible
        actual[-1]["documents"] = expected[-1]["documents"]
    # codeName is comparable where the native route supplies it. Collection helpers
    # discard it for per-item write errors, whereas the local bridge may retain it.
    for left, right in zip(actual, expected, strict=True):
        if "error" in left and "error" in right and right["error"]["codeName"] is None:
            left["error"]["codeName"] = None
    assert actual == expected, (case.id, actual, expected)


RAW_CASE_IDS = ("I01", "I03", "I06", "V02", "N02", "R05", "B02", "B03")


def raw_write_command(case, name):
    """Classic command view of the selected specimen; retain the request order."""
    request = deepcopy(case.requests[0])
    if request.method == "command":
        command = request.args[0]
        command["update" if "update" in command else "delete"] = name
        return command
    if request.method in ("insert_one", "insert_many"):
        return {
            "insert": name,
            "documents": request.args
            if request.method == "insert_one"
            else request.args[0],
            "ordered": True,
            **request.kwargs,
        }
    if request.method == "bulk_write":
        return {
            "insert": name,
            "documents": [m._doc for m in request.args[0]],
            **request.kwargs,
        }
    if request.method in ("delete_one", "delete_many"):
        return {
            "delete": name,
            "deletes": [
                {"q": request.args[0], "limit": int(request.method == "delete_one")}
            ],
            "ordered": True,
        }
    if request.method in ("update_one", "update_many", "replace_one"):
        return {
            "update": name,
            "updates": [
                {
                    "q": request.args[0],
                    "u": request.args[1],
                    "multi": request.method == "update_many",
                    "upsert": request.kwargs.get("upsert", False),
                }
            ],
            "ordered": True,
        }
    assert request.method in ("find_one_and_update", "find_one_and_replace")
    command = {
        "findAndModify": name,
        "query": request.args[0],
        "update": request.args[1],
        "new": bool(request.kwargs.get("return_document", True)),
    }
    if "upsert" in request.kwargs:
        command["upsert"] = request.kwargs["upsert"]
    if "projection" in request.kwargs:
        command["fields"] = request.kwargs["projection"]
    return command


def raw_result(result, bson):
    """Project stable command data, excluding explanatory text and server clocks."""
    output = {key: result[key] for key in ("ok", "n", "nModified") if key in result}
    if "writeErrors" in result:
        if bson.error_contract:
            output["write_error_documents"] = [
                error_document_contract(entry, bson) for entry in result["writeErrors"]
            ]
        output["write_error_operations"] = error_operations(result, bson)
        output["write_errors"] = [
            [e.get("index"), e.get("code")] for e in result["writeErrors"]
        ]
    if "value" in result:
        output["value"] = (
            None if result["value"] is None else bson.value(result["value"], root=True)
        )
    if "lastErrorObject" in result:
        output["lastErrorObject"] = result["lastErrorObject"]
    if "upserted" in result:
        output["upserted"] = bson.value(result["upserted"])
    return output


def observe_raw_write(database, name, case):
    case = deepcopy(case)
    collection = database.create_collection(name, **case.options)
    if case.documents:
        collection.insert_many(case.documents, bypass_document_validation=True)
    for index in case.indexes:
        keys, options = _index(index, collection)
        collection.create_index(keys, **options)
    bson = BSONObservation(
        [case.documents, [r.args for r in case.requests]],
        namespace=f"{database.name}.{name}",
    )
    try:
        response = {
            "status": "ok",
            "result": raw_result(database.command(raw_write_command(case, name)), bson),
        }
    except SEMANTIC_ERRORS as error:
        response = {
            "status": "error",
            "error": public_error(error, bson, command_namespace=bson.namespace),
        }
    response["documents"] = bson.documents(list(collection.find({}).sort("_id", 1)))
    return response


async def observe_async_raw_write(database, name, case):
    case = deepcopy(case)
    collection = await database.create_collection(name, **case.options)
    if case.documents:
        await collection.insert_many(case.documents, bypass_document_validation=True)
    for index in case.indexes:
        keys, options = _index(index, collection)
        await collection.create_index(keys, **options)
    bson = BSONObservation(
        [case.documents, [r.args for r in case.requests]],
        namespace=f"{database.name}.{name}",
    )
    try:
        response = {
            "status": "ok",
            "result": raw_result(
                await database.command(raw_write_command(case, name)), bson
            ),
        }
    except SEMANTIC_ERRORS as error:
        response = {
            "status": "error",
            "error": public_error(error, bson, command_namespace=bson.namespace),
        }
    response["documents"] = bson.documents(
        await collection.find({}).sort("_id", 1).to_list()
    )
    return response
