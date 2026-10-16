"""Materialized Python specimens retain every approved input and matrix cell."""

import hashlib
import json

from pathlib import Path

import pymongo

from bson import json_util

from tests.differential.positional_cases import POSITIONAL_UPDATE_CASES
from tests.differential.write_cases import WRITE_CASES
from tests.support_write_parity import BSONObservation


ROOT = Path(__file__).parents[2]


def freeze(value):
    if isinstance(value, (list, tuple)):
        return [freeze(item) for item in value]
    if isinstance(value, dict):
        return {key: freeze(item) for key, item in value.items()}
    if type(value).__module__ == "pymongo.operations":
        return {
            "model": type(value).__name__,
            "attributes": {
                key: freeze(getattr(value, key))
                for key in (
                    "_filter",
                    "_doc",
                    "_upsert",
                    "_array_filters",
                    "_hint",
                    "_collation",
                    "_sort",
                )
                if hasattr(value, key)
            },
        }
    return value


def test_python_specimens_match_the_approved_inventory_without_executing_json():
    plan = json.loads((ROOT / "docs/write-parity-corpus.json").read_text())
    legacy = ROOT / "tests/fixtures/positional_update_mongodb8.json"
    assert (
        hashlib.sha256(legacy.read_bytes()).hexdigest() == plan["legacy_fixture_sha256"]
    )
    assert [case.id for case in WRITE_CASES] == [
        row["id"] for row in plan["sequential"]
    ]
    observer = BSONObservation([])
    for row, case in zip(plan["sequential"], WRITE_CASES, strict=True):
        if row["family"] == "P":
            index = int(row["id"][1:]) - 1
            assert row["source_case_name"] == POSITIONAL_UPDATE_CASES[index]["name"]
            assert case.documents == POSITIONAL_UPDATE_CASES[index]["seed_documents"]
            continue
        decoded = json_util.loads(json.dumps(row))
        assert observer.value(case.documents) == observer.value(
            decoded["given"]["documents"]
        )
        assert case.options == decoded["given"].get("collection_options", {})
        assert case.indexes == decoded["given"].get("indexes", [])
        assert len(case.requests) == len(decoded["requests"])
        for actual, expected in zip(case.requests, decoded["requests"], strict=True):
            assert actual.method == expected["api"]
            kwargs = expected["kwargs"].copy()
            args = expected["args"]
            if "requests" in kwargs:
                models = kwargs.pop("requests")
                args = [
                    [
                        getattr(pymongo, model["model"])(
                            *model["args"], **model["kwargs"]
                        )
                        for model in models
                    ]
                ]
            if "return_document" in kwargs:
                kwargs["return_document"] = getattr(
                    pymongo.ReturnDocument, kwargs["return_document"]
                )
            assert observer.value(freeze(actual.args)) == observer.value(
                freeze(args)
            ), case.id
            assert observer.value(freeze(actual.kwargs), root=True) == observer.value(
                freeze(kwargs), root=True
            ), case.id
