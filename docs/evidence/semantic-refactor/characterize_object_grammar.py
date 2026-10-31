"""Native-only H14 census, before changing structured expression validation.

Use MONGOECO_REAL_MONGODB_URI for an isolated server and pass an output path.
This is characterization, not an installed acceptance corpus or parity claim.
"""

import os
import sys
import uuid
from copy import deepcopy
from pathlib import Path

import pymongo
from bson.json_util import CANONICAL_JSON_OPTIONS, dumps


SEED = [{"_id": 1, "a": 5, "name": "a", "object": {"a": 3}}]
FORMS = {
    "get_empty": {"$getField": {}},
    "get_no_input": {"$getField": {"field": "a"}},
    "get_no_field": {"$getField": {"input": {}}},
    "get_unknown": {"$getField": {"field": "a", "input": {}, "other": 1}},
    "get_field_null": {"$getField": {"field": None, "input": {}}},
    "get_field_number": {"$getField": {"field": 1, "input": {}}},
    "get_input_scalar": {"$getField": {"field": "a", "input": 1}},
    "get_input_array": {"$getField": {"field": "a", "input": []}},
    "get_input_missing": {"$getField": {"field": "a", "input": "$absent"}},
    "get_dynamic": {"$getField": {"field": "$name", "input": "$$CURRENT"}},
    "get_shorthand": {"$getField": "a"},
    "get_current": {"$let": {"vars": {"CURRENT": {"a": 9}}, "in": {"$getField": "a"}}},
    "set_empty": {"$setField": {}},
    "set_no_field": {"$setField": {"input": {}, "value": 1}},
    "set_no_input": {"$setField": {"field": "a", "value": 1}},
    "set_no_value": {"$setField": {"field": "a", "input": {}}},
    "set_unknown": {"$setField": {"field": "a", "input": {}, "value": 1, "other": 1}},
    "set_field_number": {"$setField": {"field": 1, "input": {}, "value": 1}},
    "set_dynamic": {"$setField": {"field": "$name", "input": {}, "value": 1}},
    "set_input_scalar": {"$setField": {"field": "a", "input": 1, "value": 1}},
    "set_constant": {
        "$setField": {"field": {"$concat": ["a", "b"]}, "input": {}, "value": 1}
    },
    "unset_empty": {"$unsetField": {}},
    "unset_no_field": {"$unsetField": {"input": {}}},
    "unset_no_input": {"$unsetField": {"field": "a"}},
    "unset_value": {"$unsetField": {"field": "a", "input": {}, "value": 1}},
    "unset_unknown": {"$unsetField": {"field": "a", "input": {}, "other": 1}},
    "unset_field_number": {"$unsetField": {"field": 1, "input": {}}},
    "unset_input_scalar": {"$unsetField": {"field": "a", "input": 1}},
}
for operator in ("$getField", "$setField", "$unsetField"):
    for label, operand in (("null", None), ("number", 5), ("array", [])):
        FORMS[f"{operator[1:]}_{label}"] = {operator: operand}


def reset(collection, documents):
    collection.delete_many({})
    if documents:
        collection.insert_many(deepcopy(documents))


def observe(collection, invoke):
    try:
        result = invoke()
        if hasattr(result, "matched_count"):
            result = {
                "matched": result.matched_count,
                "modified": result.modified_count,
            }
        observation = {"ok": True, "result": result}
    except pymongo.errors.OperationFailure as error:
        observation = {
            "ok": False,
            "family": type(error).__name__,
            "code": error.code,
            "details": error.details,
        }
    observation["state"] = list(collection.find({}, sort=[("_id", 1)]))
    observation["recovery_count"] = collection.count_documents({})
    return observation


def main():
    with pymongo.MongoClient(
        os.environ["MONGOECO_REAL_MONGODB_URI"], serverSelectionTimeoutMS=3000
    ) as client:
        database = client["mongoeco_h14_" + uuid.uuid4().hex]
        collection = database.records
        output = {
            "scope": "H14 native characterization only; no candidate acceptance",
            "runtime": {
                "build_info": client.admin.command("buildInfo"),
                "fcv": client.admin.command(
                    {"getParameter": 1, "featureCompatibilityVersion": 1}
                )["featureCompatibilityVersion"],
                "pymongo": pymongo.version,
            },
            "seed": SEED,
            "cases": {},
        }
        try:
            for name, expression in FORMS.items():
                for state, seed in (("empty", []), ("rows", SEED)):
                    for route in ("project", "update"):
                        reset(collection, seed)
                        if route == "project":
                            invoke = lambda: list(
                                collection.aggregate([{"$project": {"v": expression}}])
                            )
                        else:
                            invoke = lambda: collection.update_many(
                                {}, [{"$set": {"v": expression}}]
                            )
                        output["cases"][f"{name}_{state}_{route}"] = {
                            "expression": expression,
                            **observe(collection, invoke),
                        }
            for operator in (
                "$sum",
                "$avg",
                "$first",
                "$last",
                "$min",
                "$max",
                "$push",
                "$addToSet",
            ):
                for label, operand in (
                    ("empty", []),
                    ("one", [1]),
                    ("two", [1, 2]),
                    ("literal", {"$literal": [1, 2]}),
                ):
                    for state, seed in (("empty", []), ("rows", SEED)):
                        reset(collection, seed)
                        pipeline = [{"$group": {"_id": None, "v": {operator: operand}}}]
                        output["cases"][f"group_{operator[1:]}_{label}_{state}"] = {
                            "pipeline": pipeline,
                            **observe(
                                collection, lambda: list(collection.aggregate(pipeline))
                            ),
                        }
            Path(sys.argv[1]).write_text(
                dumps(output, json_options=CANONICAL_JSON_OPTIONS, indent=2) + "\n"
            )
        finally:
            client.drop_database(database.name)


if __name__ == "__main__":
    main()
