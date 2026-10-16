"""Replay real consumer entry points after the launcher removes semantic repairs."""

from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import importlib
import uuid

from copy import deepcopy
from pathlib import Path

import pymongo

from bson import json_util
from bson.binary import UuidRepresentation

import mongoeco

from mongoeco import MongoClient
from mongoeco.api._sync.collection import Collection
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine

from tests.support_write_parity import (
    SEMANTIC_ERRORS,
    BSONObservation,
    public_error,
    public_result,
)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--consumer-root", type=Path, required=True)
parser.add_argument("--consumer-module", required=True)
parser.add_argument("--consumer-tests", type=Path, required=True)
parser.add_argument("--overlay-root", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--uris", nargs=3, required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
mock = importlib.import_module(args.consumer_module)


assert pymongo.version == "4.19.0", pymongo.version
assert str(args.overlay_root / "env") in mongoeco.__file__, mongoeco.__file__
assert str(args.overlay_root / "src") in mock.__file__, mock.__file__
source = args.consumer_root / args.consumer_tests
tree = ast.parse(source.read_text())
nodes = {
    node.name: node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
}
anchors = {
    "C01": "test_mongodb_mock_filter_matches_field_before_top_level_operator",
    "C02": "test_mongodb_mock_bulk_write_upsert_is_idempotent",
    "C03": "test_mongodb_mock_bulk_write_applies_delete_one",
    "C04": "test_mongodb_mock_lowers_array_filter_update_atomically",
    "C05": "test_mongodb_mock_lowers_array_filter_update_atomically",
    "C06": "test_mongodb_mock_keeps_array_when_no_filtered_item_matches",
    "C07": "test_mongodb_mock_repairs_dotted_array_lookup_after_pipeline_write",
    "C08": "test_mongodb_mock_probe_precedes_a_real_embedded_array_mutation",
}
imports = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
namespace = {}
exec(  # noqa: S102 - execute only selected tests from the declared consumer source
    compile(ast.Module(body=imports, type_ignores=[]), str(source), "exec"), namespace
)


def forbidden(*args, **kwargs):
    message = "semantic interpreter invoked"
    raise AssertionError(message)


for name in [
    "_lower_array_filter_update",
    "_lower_unsupported_aggregation_expressions",
    "_order_filter_fields_before_operators",
    "_rewrite_dotted_array_filter",
]:
    setattr(mock, name, forbidden)
mock.AsyncMongoMockCollection._execute_supported_bulk_write = forbidden
originals = {
    name: getattr(Collection, name)
    for name in (
        "insert_one",
        "insert_many",
        "update_one",
        "update_many",
        "find_one",
        "find_one_and_update",
        "find_one_and_delete",
        "bulk_write",
        "delete_one",
        "delete_many",
    )
}
allruns = []
for engine in (MemoryEngine, SQLiteEngine):
    for identifier, anchor in anchors.items():
        node = deepcopy(nodes[anchor])
        node.decorator_list = []
        if identifier in ("C04", "C05"):
            split = next(
                i
                for i, n in enumerate(node.body)
                if isinstance(n, ast.Assign)
                and any(
                    isinstance(t, ast.Name) and t.id == "finalized" for t in n.targets
                )
            )
            if identifier == "C04":
                node.body = node.body[:split]
            else:
                expected = next(
                    n.test.comparators[0]
                    for n in node.body
                    if isinstance(n, ast.Assert)
                    and isinstance(n.test, ast.Compare)
                    and isinstance(n.test.left, ast.Name)
                    and n.test.left.id == "accepted"
                )
                seed = deepcopy(node.body[1])
                seed.value.value.args[0] = deepcopy(expected)
                node.body = [node.body[0], seed, *node.body[split:]]
        if identifier == "C08":
            index = next(
                i
                for i, n in enumerate(node.body)
                if isinstance(n, ast.Assign)
                and any(
                    isinstance(t, ast.Attribute) and t.attr == "_collection"
                    for t in n.targets
                )
            )
            node.body.insert(
                index + 1,
                ast.parse(
                    "attach_mongodb_mock_collection(SimpleNamespace(storage=storage))"
                ).body[0],
            )
        exec(  # noqa: S102 - execute only selected tests from the declared consumer source
            compile(
                ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                str(source),
                "exec",
            ),
            namespace,
        )
        clients = []
        calls = []
        bson = BSONObservation([])

        def factory(*args, _engine=engine, _clients=clients, **kwargs):
            client = MongoClient(
                _engine(), mongodb_dialect="7.0", pymongo_profile="4.19", **kwargs
            )
            _clients.append(client)
            return client

        namespace["MongoClient"] = factory
        for name, original in originals.items():

            def traced(
                self,
                *args,
                __name=name,
                __original=original,
                _bson=bson,
                _calls=calls,
                **kwargs,
            ):
                before = deepcopy((args, kwargs))
                result = error = None
                try:
                    result = __original(self, *args, **kwargs)
                except SEMANTIC_ERRORS as exc:
                    error = exc
                docs = list(self.find({}).sort("_id", 1))
                observation = {
                    "error" if error else "result": public_error(error, _bson)
                    if error
                    else public_result(result, _bson),
                    "documents": _bson.documents(docs),
                }
                _calls.append(
                    {
                        "method": __name,
                        "args": before[0],
                        "kwargs": before[1],
                        "observation": observation,
                    }
                )
                if error:
                    raise error
                return result

            setattr(Collection, name, traced)
        try:
            asyncio.run(namespace[anchor]())
        finally:
            for name, original in originals.items():
                setattr(Collection, name, original)
            for client in clients:
                client.close()
        assert calls
        assert identifier not in ("C04", "C05", "C06", "C08") or any(
            "array_filters" in call["kwargs"] for call in calls
        )
        native = []
        for i, target in enumerate(("7.0", "8.0", "9.0")):
            with pymongo.MongoClient(
                args.uris[i],
                serverSelectionTimeoutMS=1500,
                uuidRepresentation="standard",
            ) as client:
                db = client[f"mongoeco_consumer_{identifier}_{uuid.uuid4().hex}"]
                coll = db.items
                normal = BSONObservation([])
                try:
                    for call in calls:
                        result = error = None
                        try:
                            result = getattr(coll, call["method"])(
                                *deepcopy(call["args"]), **deepcopy(call["kwargs"])
                            )
                        except SEMANTIC_ERRORS as exc:
                            error = exc
                        observation = {
                            "error" if error else "result": public_error(error, normal)
                            if error
                            else public_result(result, normal),
                            "documents": normal.documents(
                                list(coll.find({}).sort("_id", 1))
                            ),
                        }
                        assert observation == call["observation"], (
                            identifier,
                            target,
                            call["method"],
                            observation,
                            call["observation"],
                        )
                    native.append(
                        {
                            "target": target,
                            "version": client.server_info()["version"],
                            "steps": len(calls),
                            "parity": True,
                        }
                    )
                finally:
                    client.drop_database(db.name)
                    assert db.name not in client.list_database_names()
        record = {
            "id": identifier,
            "engine": engine.__name__,
            "profile": "4.19",
            "dialect": "7.0",
            "source_anchor": anchor,
            "steps": calls,
            "native": native,
            "semantic_correctors_invoked": 0,
            "storage_role": "consumer-database-adapter"
            if identifier == "C08"
            else None,
        }

        # Model objects are descriptive receipts; the in-process replay above uses
        # original PyMongo instances and never interprets the serialized description.
        def default(value):
            if hasattr(value, "_filter"):
                return {
                    "model": type(value).__name__,
                    "filter": value._filter,
                    "document": value._doc if hasattr(value, "_doc") else None,
                }
            return str(value)

        allruns.append(record)
        args.output.write_text(
            json_util.dumps(
                {
                    "runs": allruns,
                    "sdk": pymongo.version,
                    "mongoeco_import": mongoeco.__file__,
                    "testkit_import": "isolated-overlay/consumer-adapter.py",
                    "provenance_redacted": True,
                    "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                },
                indent=2,
                json_options=json_util.CANONICAL_JSON_OPTIONS.with_options(
                    uuid_representation=UuidRepresentation.STANDARD
                ),
                default=default,
            )
            + "\n"
        )
        print(engine.__name__, identifier, len(calls), "native7/8/9 passed", flush=True)  # noqa: T201 - canary receipt
EXPECTED_RUNS = 16
assert len(allruns) == EXPECTED_RUNS
