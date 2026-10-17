"""Observe public reads independently of Mongoeco implementation details."""

import inspect

from copy import deepcopy

from tests.differential.read_lifecycle_cases import DOCUMENTS, READ_CASES
from tests.support_error_phases import encoded, err


def _cursor(collection, specification):
    cursor = collection.find(
        deepcopy(specification["filter"]),
        specification.get("projection"),
        **deepcopy(specification.get("options", {})),
    )
    cursor = cursor.sort("_id", 1).skip(specification.get("skip", 0))
    if "limit" in specification:
        cursor = cursor.limit(specification["limit"])
    return cursor.batch_size(1)


def _count_options(specification):
    return {
        **deepcopy(specification.get("options", {})),
        **{k: specification[k] for k in ("skip", "limit") if k in specification},
    }


def _remaining_requests(specification):
    query = deepcopy(specification["filter"])
    requests = [("count", "count_documents", [query], _count_options(specification))]
    if specification.get("invalid_regex"):
        requests.extend(
            [
                (
                    "update_many",
                    "update_many",
                    [deepcopy(query), {"$set": {"changed": True}}],
                    {},
                ),
                ("delete_many", "delete_many", [deepcopy(query)], {}),
            ]
        )
    return requests


def _close_cursor(cursor):
    if cursor is not None:
        cursor.close()


async def _close_async_cursor(cursor):
    if cursor is not None:
        await _resolved(cursor.close())


def observe_reads(client, database):
    result = {}
    for name, specification in READ_CASES.items():
        collection = client[database].create_collection(name)
        documents = deepcopy(specification.get("documents", DOCUMENTS))
        if documents:
            collection.insert_many(documents)
        stages = []
        cursor = None
        try:
            for action in ("find", "rewind", "clone"):
                try:
                    # A constructor may reject syntax before returning a cursor.
                    if action == "find" or cursor is None:
                        cursor = _cursor(collection, specification)
                    elif action == "rewind":
                        cursor.rewind()
                    if action == "clone":
                        clone = cursor.clone()
                        try:
                            documents = list(clone)
                        finally:
                            clone.close()
                    else:
                        documents = list(cursor)
                    stages.append({"method": action, "result": encoded(documents)})
                except Exception as error:
                    stages.append(
                        {"method": action, "error": err(error, collection.full_name)}
                    )
        finally:
            _close_cursor(cursor)
        for label, method, args, options in _remaining_requests(specification):
            try:
                value = getattr(collection, method)(*args, **options)
                stages.append(
                    {
                        "method": label,
                        "result": value if label == "count" else "unexpected success",
                    }
                )
            except Exception as error:
                stages.append(
                    {"method": label, "error": err(error, collection.full_name)}
                )
        collection.insert_one({"_id": "following", "group": "recovery"})
        stages.append({"method": "recovery", "result": collection.count_documents({})})
        result[name] = {
            "stages": stages,
            "documents": encoded(list(collection.find().sort("_id", 1))),
        }
    return result


async def _resolved(value):
    return await value if inspect.isawaitable(value) else value


async def observe_async_reads(client, database):
    result = {}
    for name, specification in READ_CASES.items():
        collection = await client[database].create_collection(name)
        documents = deepcopy(specification.get("documents", DOCUMENTS))
        if documents:
            await collection.insert_many(documents)
        stages = []
        cursor = None
        try:
            for action in ("find", "rewind", "clone"):
                try:
                    # A constructor may reject syntax before returning a cursor.
                    if action == "find" or cursor is None:
                        cursor = _cursor(collection, specification)
                    elif action == "rewind":
                        await _resolved(cursor.rewind())
                    if action == "clone":
                        clone = await _resolved(cursor.clone())
                        try:
                            documents = await clone.to_list()
                        finally:
                            await _resolved(clone.close())
                    else:
                        documents = await cursor.to_list()
                    stages.append({"method": action, "result": encoded(documents)})
                except Exception as error:
                    stages.append(
                        {"method": action, "error": err(error, collection.full_name)}
                    )
        finally:
            await _close_async_cursor(cursor)
        for label, method, args, options in _remaining_requests(specification):
            try:
                value = await getattr(collection, method)(*args, **options)
                stages.append(
                    {
                        "method": label,
                        "result": value if label == "count" else "unexpected success",
                    }
                )
            except Exception as error:
                stages.append(
                    {"method": label, "error": err(error, collection.full_name)}
                )
        await collection.insert_one({"_id": "following", "group": "recovery"})
        stages.append(
            {"method": "recovery", "result": await collection.count_documents({})}
        )
        result[name] = {
            "stages": stages,
            "documents": encoded(await collection.find().sort("_id", 1).to_list()),
        }
    return result
