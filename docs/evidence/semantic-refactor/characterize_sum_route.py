"""Record native sum/avg route differences without importing the local evaluator."""

import argparse
import json
import os
import uuid

from pathlib import Path

import pymongo

from bson.json_util import CANONICAL_JSON_OPTIONS, dumps

from tests.differential.semantic_sum_cases import (
    GROUP_PIPELINE,
    SEED,
    STREAM_PIPELINE,
    STREAM_SEED,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output")
    args = parser.parse_args()
    with pymongo.MongoClient(os.environ["MONGOECO_REAL_MONGODB_URI"]) as client:
        database = client[f"mongoeco_sum_route_{uuid.uuid4().hex}"]
        try:
            output = {
                "source": "real-mongodb",
                "server": client.server_info()["version"],
                "pymongo": pymongo.version,
                "cases": {},
            }
            for name, seed, pipeline in (
                ("ordered_group", STREAM_SEED, STREAM_PIPELINE),
                ("unwind_group", SEED, GROUP_PIPELINE),
                ("facet_group", STREAM_SEED, [{"$facet": {"groups": STREAM_PIPELINE}}]),
            ):
                collection = database[name]
                collection.insert_many(seed)
                explain = database.command(
                    "explain",
                    {
                        "aggregate": name,
                        "pipeline": pipeline,
                        "cursor": {},
                    },
                )
                result = list(collection.aggregate(pipeline))
                records = result[0]["groups"] if name == "facet_group" else result
                output["cases"][name] = {
                    "witness": [
                        d for d in records if d["_id"] == "long_overflow_cancel"
                    ],
                    "has_slot_based_plan": "slotBasedPlan"
                    in json.dumps(explain, default=str),
                    "explain": explain,
                }
            Path(args.output).write_text(
                dumps(output, json_options=CANONICAL_JSON_OPTIONS, indent=2) + "\n"
            )
        finally:
            client.drop_database(database.name)


if __name__ == "__main__":
    main()
