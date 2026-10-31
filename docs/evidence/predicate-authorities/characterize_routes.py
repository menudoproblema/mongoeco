import argparse
import asyncio
import sys

from pathlib import Path
from unittest.mock import patch


sys.path[:0] = [str(Path.cwd() / "src"), str(Path.cwd())]
from bson import json_util

from mongoeco import AsyncMongoClient, SearchIndexModel
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_90
from mongoeco.core.compiled_query import CompiledQuery
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.schema_validation import compile_collection_validator
from mongoeco.core.search_filter_prefilter import (
    filter_value_key,
    matches_candidateable_filter,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import UNDEFINED


CASES = [
    ("string_number", {"a": "z"}, {"a": {"$gt": 5}}),
    ("nested_empty", {"o": {"a": []}}, {"o.a": {"$gt": 5}}),
    ("large_integer", {"a": 2**53 + 1}, {"a": {"$gt": 2**53}}),
    ("array_range", {"a": [1, 20]}, {"a": {"$gt": 5, "$lt": 10}}),
    ("array_and", {"a": [1, 20]}, {"$and": [{"a": {"$gt": 5}}, {"a": {"$lt": 10}}]}),
    ("missing_null", {}, {"a": None}),
    ("broadcast", {"items": [{"x": 7}]}, {"items.x": {"$gt": 5}}),
    ("array_position", {"a": [7]}, {"a.0": 7}),
]


def observed(call):
    try:
        return {"ok": True, "value": call()}
    except Exception as e:
        return {
            "ok": False,
            "type": type(e).__name__,
            "message": str(e),
            "code": getattr(e, "code", None),
        }


async def main(output):
    report = {
        "base": "79529a2",
        "scope": "synthetic data in private Memory and SQLite :memory: engines only; no existing databases",
        "routes": [],
    }
    definition = {
        "fields": [
            {
                "type": "vector",
                "path": "embedding",
                "numDimensions": 2,
                "similarity": "cosine",
            }
        ]
    }
    for factory in [MemoryEngine, SQLiteEngine]:
        async with AsyncMongoClient(factory()) as client:
            for name, doc, query in CASES:
                coll = client.analysis[name]
                await coll.insert_one({"_id": 0, "embedding": [1.0, 0.0], **doc})
                item = {
                    "case": name,
                    "engine": factory.__name__,
                    "find": [x["_id"] for x in await coll.find(query).to_list()],
                    "helper": observed(
                        lambda: matches_candidateable_filter(doc, query)
                    ),
                }
                await coll.create_search_index(
                    SearchIndexModel(definition, name="vectors", type="vectorSearch")
                )
                spec = {
                    "index": "vectors",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0],
                    "numCandidates": 10,
                    "limit": 10,
                    "filter": query,
                }
                try:
                    item["vector"] = {
                        "ok": True,
                        "ids": [
                            x["_id"]
                            for x in await coll.aggregate(
                                [{"$vectorSearch": spec}]
                            ).to_list()
                        ],
                    }
                except Exception as e:
                    item["vector"] = {
                        "ok": False,
                        "message": str(e),
                        "type": type(e).__name__,
                    }
                report["routes"].append(item)
                print(item)
            for name, doc, query in CASES[:2]:
                coll = client.analysis[name]
                await coll.create_index([(next(iter(query)), 1)])
                report["routes"].append(
                    {
                        "case": name,
                        "engine": factory.__name__,
                        "indexed_find": [
                            x["_id"] for x in await coll.find(query).to_list()
                        ],
                    }
                )
            coll = await client.analysis.create_collection(
                "schema_enum",
                validator={"$jsonSchema": {"properties": {"a": {"enum": [1]}}}},
            )
            try:
                await coll.insert_one({"_id": 0, "a": True})
                report["routes"].append(
                    {
                        "case": "enum_bool_write",
                        "engine": factory.__name__,
                        "accepted": True,
                    }
                )
            except Exception as e:
                report["routes"].append(
                    {
                        "case": "enum_bool_write",
                        "engine": factory.__name__,
                        "accepted": False,
                        "code": getattr(e, "code", None),
                        "type": type(e).__name__,
                    }
                )
    options = {"validator": {"a": {"$ne": None}}, "validationLevel": "moderate"}
    validator = compile_collection_validator(options, dialect=MONGODB_DIALECT_90)
    old = {"a": UNDEFINED}
    new = {"a": None}
    report["moderate"] = {
        "original_valid_9": validator.validate_document(
            old, dialect=MONGODB_DIALECT_90
        ).valid,
        "original_valid_7": validator.validate_document(
            old, dialect=MONGODB_DIALECT_70
        ).valid,
        "new_valid_9": validator.validate_document(
            new, dialect=MONGODB_DIALECT_90
        ).valid,
        "moderate_transition_accepted": validator.validate_document(
            new, original_document=old, dialect=MONGODB_DIALECT_90
        ).valid,
    }
    CompiledQuery._compile_expression.cache_clear()
    with patch.object(
        QueryEngine,
        "extract_values",
        side_effect=TypeError("injected internal extraction failure"),
    ):
        matcher = CompiledQuery(compile_filter({"o.a": {"$gt": 5}}))
        report["exception_injection"] = observed(lambda: matcher.match({"o": {"a": 6}}))
    CompiledQuery._compile_expression.cache_clear()
    one = CompiledQuery(compile_filter({"a": 1}))
    two = CompiledQuery(compile_filter({"a": 2}))
    report["cache_control"] = {
        "shared_template": one._match_func is two._match_func,
        "distinct_values": one._values is not two._values,
        "first_matches_1": one.match({"a": 1}),
        "first_matches_2": one.match({"a": 2}),
        "second_matches_1": two.match({"a": 1}),
        "second_matches_2": two.match({"a": 2}),
    }
    report["key_collision"] = {
        "key_2_pow_53": filter_value_key(2**53),
        "key_next_integer": filter_value_key(2**53 + 1),
    }
    Path(output).write_text(
        json_util.dumps(report, json_options=json_util.CANONICAL_JSON_OPTIONS, indent=2)
        + "\n"
    )
    print("moderate", report["moderate"])
    print("injection", report["exception_injection"])
    print("cache", report["cache_control"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    asyncio.run(main(parser.parse_args().output))
