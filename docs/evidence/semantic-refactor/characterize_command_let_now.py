"""Supplemental NOW phase witness; run only against an isolated MongoDB 9.

MONGOECO_REAL_MONGODB_URI=<isolated-9-uri> PYTHONPATH=src:. python <this-file>
The maintained 7/8/9 corpus owns acceptance; this probe characterizes six extra
branches in 9 and compares them using only the two public client APIs.
"""

import json
import os
from pathlib import Path

import pymongo
from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine

values = {
    "direct": "$$NOW",
    "inactive_cond": {"$cond": [True, 5, "$$NOW"]},
    "inactive_switch": {
        "$switch": {"branches": [{"case": True, "then": 5}], "default": "$$NOW"}
    },
    "literal": {"$literal": "$$NOW"},
    "unused_lexical": {"$let": {"vars": {"unused": "$$NOW"}, "in": 5}},
    "inactive_lexical": {
        "$cond": [True, 5, {"$let": {"vars": {"unused": "$$NOW"}, "in": 5}}]
    },
}


def observe(c, v):
    try:
        return {
            "documents": list(
                c.aggregate([{"$project": {"_id": 0, "v": "$$x"}}], let={"x": v})
            )
        }
    except pymongo.errors.OperationFailure as e:
        return {
            "family": type(e).__name__,
            "code": getattr(e, "code", None),
            "errmsg": (getattr(e, "details", None) or {}).get("errmsg", str(e)),
        }


with (
    pymongo.MongoClient(os.environ["MONGOECO_REAL_MONGODB_URI"]) as client,
    MongoClient(MemoryEngine(), mongodb_dialect="9.0") as eco,
):
    c = client.mongoeco_now_let_probe.records
    local = eco.mongoeco_now_let_probe.records
    try:
        c.insert_one({"_id": 1})
        local.insert_one({"_id": 1})
        out = {
            n: {"native": observe(c, v), "local": observe(local, v)}
            for n, v in values.items()
        }
        Path(
            "docs/evidence/semantic-refactor/command-let-now-branches-9.json"
        ).write_text(json.dumps(out, indent=2) + "\n")
        print(json.dumps(out, indent=2))
    finally:
        client.drop_database("mongoeco_now_let_probe")
