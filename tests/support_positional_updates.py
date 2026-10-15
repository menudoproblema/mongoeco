"""Shared observations for positional-update regressions and real-server parity."""

from __future__ import annotations

import json

from copy import deepcopy
from pathlib import Path
from typing import Any

from pymongo import UpdateMany, UpdateOne
from pymongo.errors import OperationFailure


FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "positional_update_mongodb8.json").read_text()
)
POSITIONAL_UPDATE_CASES = FIXTURE["cases"]


def _arguments(case: dict[str, Any]) -> dict[str, Any]:
    arguments = {"upsert": case["upsert"]}
    if case["array_filters"] is not None:
        arguments["array_filters"] = deepcopy(case["array_filters"])
    return arguments


def _bulk_models(case: dict[str, Any]) -> list[Any]:
    model = UpdateOne if case["api"] == "bulk_one" else UpdateMany
    models = [
        model(deepcopy(case["query"]), deepcopy(case["update"]), **_arguments(case))
    ]
    if case["batch_followup"]:
        models.append(UpdateOne({"_id": "b"}, {"$set": {"untouched": 1}}))
    return models


def _error_observation(error: OperationFailure | None) -> dict[str, Any]:
    details = error.details or {} if error is not None else {}
    return {
        "status": "error" if error is not None else "ok",
        "code": error.code if error is not None else None,
        "write_errors": [
            [item["index"], item["code"]] for item in details.get("writeErrors", [])
        ],
    }


def observe_positional_update(collection: Any, case: dict[str, Any]) -> dict[str, Any]:
    """Observe native error codes, bulk item errors and persisted documents."""
    if case["seed_documents"]:
        collection.insert_many(deepcopy(case["seed_documents"]))
    failure = None
    try:
        if case["api"].startswith("bulk"):
            collection.bulk_write(_bulk_models(case), ordered=case["ordered"])
        else:
            getattr(collection, case["api"])(
                deepcopy(case["query"]), deepcopy(case["update"]), **_arguments(case)
            )
    except OperationFailure as error:
        failure = error
    observation = _error_observation(failure)
    observation["documents"] = list(collection.find({}).sort("_id", 1))
    return observation


async def observe_async_positional_update(
    collection: Any, case: dict[str, Any]
) -> dict[str, Any]:
    """Observe the same contract through the native asynchronous API."""
    if case["seed_documents"]:
        await collection.insert_many(deepcopy(case["seed_documents"]))
    failure = None
    try:
        if case["api"].startswith("bulk"):
            await collection.bulk_write(_bulk_models(case), ordered=case["ordered"])
        else:
            await getattr(collection, case["api"])(
                deepcopy(case["query"]), deepcopy(case["update"]), **_arguments(case)
            )
    except OperationFailure as error:
        failure = error
    observation = _error_observation(failure)
    observation["documents"] = await collection.find({}).sort("_id", 1).to_list()
    return observation
