"""Finite concurrent histories coordinated at public invocation boundaries."""

from __future__ import annotations

import asyncio
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from pymongo import ReturnDocument

from tests.differential.write_cases import WriteRequest
from tests.support_write_parity import (
    SEMANTIC_ERRORS,
    BSONObservation,
    public_error,
    public_result,
)


ACTOR_COUNT = 2
DUPLICATE_KEY_CODE = 11000

HISTORY_CASES = {
    "Q01": (
        [{"_id": "a", "revision": 1, "wins": 0}],
        [
            WriteRequest(
                "find_one_and_update",
                [{"_id": "a", "revision": 1}, {"$inc": {"revision": 1, "wins": 1}}],
                {"return_document": ReturnDocument.AFTER},
            )
        ]
        * 2,
    ),
    "Q02": (
        [{"_id": "a", "revision": 1, "wins": 0}],
        [
            WriteRequest(
                "update_one",
                [{"_id": "a", "revision": 1}, {"$inc": {"revision": 1, "wins": 1}}],
                {"hint": "_id_"},
            )
        ]
        * 2,
    ),
    "Q03": (
        [{"_id": "a", "revision": 1}],
        [
            WriteRequest(
                "find_one_and_replace",
                [{"_id": "a", "revision": 1}, {"_id": "a", "revision": 2}],
                {"return_document": ReturnDocument.AFTER},
            )
        ]
        * 2,
    ),
    "Q04": (
        [{"_id": "a", "revision": 1}],
        [
            WriteRequest(
                "replace_one",
                [{"_id": "a", "revision": 1}, {"_id": "a", "revision": 2}],
            )
        ]
        * 2,
    ),
    "Q05": (
        [],
        [
            WriteRequest("insert_one", [{"_id": actor, "email": "same"}])
            for actor in ("a", "b")
        ],
    ),
    "Q06": ([{"_id": "a"}], [WriteRequest("find_one_and_delete", [{"_id": "a"}])] * 2),
}


def _response(actor, result, error):
    bson = BSONObservation([])
    return {
        "actor": actor,
        "event": "response",
        "time_ns": time.monotonic_ns(),
        "error" if error else "result": public_error(error, bson)
        if error
        else public_result(result, bson),
    }


def _invocation(actor, request):
    return {
        "actor": actor,
        "event": "invocation",
        "time_ns": time.monotonic_ns(),
        "method": request.method,
        "args": deepcopy(request.args),
        "kwargs": deepcopy(request.kwargs),
    }


def validate_history(identifier, history):
    events = history["events"]
    responses = sorted(
        (e for e in events if e["event"] == "response"), key=lambda e: e["actor"]
    )
    invocations = [e for e in events if e["event"] == "invocation"]
    assert len(responses) == len(invocations) == ACTOR_COUNT
    assert max(e["time_ns"] for e in invocations) <= min(
        e["time_ns"] for e in responses
    )
    docs = history["documents"]
    if identifier == "Q05":
        successes = [e for e in responses if "result" in e]
        errors = [e["error"] for e in responses if "error" in e]
        assert len(successes) == len(errors) == 1
        assert errors[0]["family"] == "DuplicateKeyError"
        assert errors[0]["code"] == DUPLICATE_KEY_CODE
        assert docs == [{"_id": successes[0]["result"]["inserted_id"], "email": "same"}]
        return
    assert all("result" in e for e in responses), responses
    values = [e["result"] for e in responses]
    if identifier in ("Q02", "Q04"):
        assert sorted(r["matched_count"] for r in values) == [0, 1]
        assert sorted(r["modified_count"] for r in values) == [0, 1]
        assert all(r["acknowledged"] and r["upserted_id"] is None for r in values)
    else:
        assert sum(r["document"] is not None for r in values) == 1
        returned = next(r["document"] for r in values if r["document"] is not None)
        expected = {"_id": "a"} if identifier == "Q06" else {"_id": "a", "revision": 2}
        if identifier == "Q01":
            expected["wins"] = 1
        assert returned == BSONObservation([]).value(expected, root=True)
    expected_docs = [] if identifier == "Q06" else [{"_id": "a", "revision": 2}]
    if identifier in ("Q01", "Q02"):
        expected_docs[0]["wins"] = 1
    assert docs == expected_docs, (identifier, docs)


def observe_history(collections, identifier):
    documents, requests = deepcopy(HISTORY_CASES[identifier])
    if documents:
        collections[0].insert_many(documents)
    if identifier == "Q05":
        collections[0].create_index([("email", 1)], unique=True)
    events = []
    barrier = threading.Barrier(3, timeout=10)

    def actor(index):
        request = requests[index]
        events.append(_invocation(index, request))
        barrier.wait()
        result = error = None
        try:
            result = getattr(collections[index], request.method)(
                *request.args, **request.kwargs
            )
        except SEMANTIC_ERRORS as exc:
            error = exc
        events.append(_response(index, result, error))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(actor, index) for index in range(2)]
        barrier.wait()
        for future in futures:
            future.result(timeout=10)
    history = {
        "id": identifier,
        "events": sorted(events, key=lambda e: e["time_ns"]),
        "documents": list(collections[0].find({}).sort("_id", 1)),
    }
    validate_history(identifier, history)
    return history


async def observe_async_history(collections, identifier):
    documents, requests = deepcopy(HISTORY_CASES[identifier])
    if documents:
        await collections[0].insert_many(documents)
    if identifier == "Q05":
        await collections[0].create_index([("email", 1)], unique=True)
    events = []
    barrier = asyncio.Barrier(3)

    async def actor(index):
        request = requests[index]
        events.append(_invocation(index, request))
        await barrier.wait()
        result = error = None
        try:
            result = await getattr(collections[index], request.method)(
                *request.args, **request.kwargs
            )
        except SEMANTIC_ERRORS as exc:
            error = exc
        events.append(_response(index, result, error))

    async with asyncio.timeout(10):
        tasks = [asyncio.create_task(actor(index)) for index in range(2)]
        await barrier.wait()
        await asyncio.gather(*tasks)
    history = {
        "id": identifier,
        "events": sorted(events, key=lambda e: e["time_ns"]),
        "documents": await collections[0].find({}).sort("_id", 1).to_list(),
    }
    validate_history(identifier, history)
    return history
