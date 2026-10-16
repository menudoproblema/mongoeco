"""Historical positional inputs and native request models, without a local engine."""

from __future__ import annotations

import json

from copy import deepcopy
from pathlib import Path
from typing import Any

from pymongo import UpdateMany, UpdateOne


FIXTURE = json.loads(
    (
        Path(__file__).parents[1] / "fixtures" / "positional_update_mongodb8.json"
    ).read_text()
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
