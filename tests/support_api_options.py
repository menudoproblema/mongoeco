"""Observe API conveniences using only public PyMongo-shaped calls and BSON."""

import inspect

from copy import deepcopy

from bson import decode_all

from tests.differential.api_option_cases import PROJECTIONS
from tests.support_write_parity import (
    SEMANTIC_ERRORS,
    BSONObservation,
    public_error,
    public_result,
)


def inputs(case):
    seed = [
        {
            "_id": "a",
            "name": "Ada",
            "profile": {"city": "Madrid", "zip": 1},
            "items": [{"sku": "x"}, {"sku": "y"}],
            "counter": 0,
        }
    ]
    options = {}
    documents = []
    if case.projection is not None:
        projection = (
            (name for name in ["name"])
            if case.projection == "generator"
            else deepcopy(PROJECTIONS[case.projection])
        )
        query = {"_id": "a"}
        if case.method == "find_one_and_update":
            args = [query, {"$inc": {"counter": 1}}]
        elif case.method == "find_one_and_replace":
            args = [query, {"name": "Bea", "profile": {"city": "Paris"}}]
        else:
            args = [query]
        kwargs = {"projection": projection}
    else:
        seed = [{"_id": "duplicate", "name": "Existing"}]
        documents = [{"name": "First"}]
        if case.method == "insert_many":
            documents += [{"_id": "second", "name": "Second"}, {"name": "Last"}]
        if case.failure == "duplicate":
            documents[0 if case.method == "insert_one" else 1]["_id"] = "duplicate"
        if case.failure == "schema":
            documents[0 if case.method == "insert_one" else 1].pop("name")
            options = {"validator": {"$jsonSchema": {"required": ["name"]}}}
        payload = (
            documents[0]
            if case.method == "insert_one"
            else (
                tuple(documents)
                if case.shape == "tuple"
                else (
                    (document for document in documents)
                    if case.shape == "generator"
                    else documents
                )
            )
        )
        args = [payload]
        kwargs = {"comment": deepcopy(case.comment)}
        if case.method == "insert_many":
            kwargs["ordered"] = case.ordered
    return seed, options, documents, args, kwargs


def observation(case, bson, documents, caller, result, error, profiles):  # noqa: PLR0913, PLR0917 - independent state, result, error and profile observations
    if error is not None:
        outcome = {"error": public_error(error, bson)}
    elif case.method in ("find", "find_raw_batches"):
        outcome = {"result": {"documents": bson.documents(result)}}
    else:
        outcome = {"result": public_result(result, bson)}
    return {
        **outcome,
        "documents": bson.documents(documents),
        "caller": bson.documents(caller),
        "profile_comments": [bson.value(p["command"]["comment"]) for p in profiles],
    }


def profile_filter(name):
    return {"command.insert": name, "command.comment": {"$exists": True}}


def assert_api_observation(actual, expected):
    actual = deepcopy(actual)
    # Same policy as the write corpus: compare native codeName when present.
    # Per-item PyMongo errors drop it; local validation may retain metadata.
    if "error" in expected and expected["error"]["codeName"] is None:
        actual["error"]["codeName"] = None
    assert actual == expected


def observe_api_case(database, name, case):
    seed, options, caller, args, kwargs = inputs(case)
    bson = BSONObservation([seed, caller, case.comment])
    collection = database.create_collection(name, **options)
    collection.insert_many(seed)
    result = error = None
    try:
        result = getattr(collection, case.method)(*args, **kwargs)
        if case.method == "find":
            result = list(result)
        if case.method == "find_raw_batches":
            result = [doc for batch in result for doc in decode_all(batch)]
    except SEMANTIC_ERRORS as exc:
        error = exc
    documents = list(collection.find({}).sort("_id", 1))
    profiles = list(database["system.profile"].find(profile_filter(name)))
    return observation(case, bson, documents, caller, result, error, profiles)


async def observe_async_api_case(database, name, case):
    seed, options, caller, args, kwargs = inputs(case)
    bson = BSONObservation([seed, caller, case.comment])
    collection = await database.create_collection(name, **options)
    await collection.insert_many(seed)
    result = error = None
    try:
        result = getattr(collection, case.method)(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        if case.method == "find":
            result = [document async for document in result]
        if case.method == "find_raw_batches":
            result = [doc async for batch in result for doc in decode_all(batch)]
    except SEMANTIC_ERRORS as exc:
        error = exc
    documents = await collection.find({}).sort("_id", 1).to_list()
    profiles = await database["system.profile"].find(profile_filter(name)).to_list()
    return observation(case, bson, documents, caller, result, error, profiles)
