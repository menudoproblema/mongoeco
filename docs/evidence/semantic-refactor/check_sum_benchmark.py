import os
import sys
import uuid

from pathlib import Path

import pymongo

from benchmarks.data.generator import generate_users
from bson.json_util import CANONICAL_JSON_OPTIONS, dumps

import mongoeco

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine

from tests.differential._real_parity_base import _normalize


pipeline = [
    {"$match": {"active": True, "age": {"$gte": 20}}},
    {
        "$group": {
            "_id": "$city",
            "avg_score": {"$avg": "$score"},
            "total_users": {"$sum": 1},
            "roles": {"$addToSet": "$role"},
        }
    },
    {"$sort": {"avg_score": -1}},
    {"$limit": 10},
]
output = {"local_module": mongoeco.__file__, "cases": {}}
with (
    pymongo.MongoClient(os.environ["MONGOECO_REAL_MONGODB_URI"]) as native,
    MongoClient(MemoryEngine()) as local,
):
    db = "mongoeco_sum_bench_" + uuid.uuid4().hex
    try:
        real = native[db].t
        eco = local[db].t
        data = generate_users(1000)
        real.insert_many(data)
        eco.insert_many(data)
        for name, p in [
            ("group", pipeline),
            ("facet", [{"$facet": {"groups": pipeline}}]),
        ]:
            expected = list(real.aggregate(p))
            actual = list(eco.aggregate(p))
            erows = expected[0]["groups"] if name == "facet" else expected
            arows = actual[0]["groups"] if name == "facet" else actual
            for row in [*erows, *arows]:
                row["roles"] = sorted(row["roles"])
            output["cases"][name] = {
                "native": expected,
                "local": actual,
                "matches_native": _normalize(actual) == _normalize(expected),
            }
        Path(sys.argv[1]).write_text(
            dumps(output, json_options=CANONICAL_JSON_OPTIONS, indent=2) + "\n"
        )
    finally:
        native.drop_database(db)
