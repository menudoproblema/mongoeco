"""Bindings and nested logical scopes survive the native command boundary."""

import asyncio

import pymongo
import pytest

from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_wire_preserves_let_lookup_facet_scopes_and_early_errors(backend):

    async def exercise():
        engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
        async with AsyncMongoEcoProxyServer(
            engine=engine, mongodb_dialect="9.0", pymongo_profile="4.18"
        ) as proxy:

            def run_client():
                failed_to_parse = 9
                expected_max_wire_version = 20
                with pymongo.MongoClient(
                    proxy.address.uri,
                    directConnection=True,
                    serverSelectionTimeoutMS=3000,
                ) as client:
                    client.test.records.insert_one({"_id": 1, "n": 2})
                    client.test.foreign.insert_one({"_id": "foreign", "n": 2})
                    pipeline = [
                        {"$match": {"$expr": {"$eq": ["$n", "$$wanted"]}}},
                        {
                            "$facet": {
                                "values": [
                                    {
                                        "$lookup": {
                                            "from": "foreign",
                                            "let": {"local": "$n"},
                                            "pipeline": [
                                                {
                                                    "$match": {
                                                        "$expr": {
                                                            "$and": [
                                                                {
                                                                    "$eq": [
                                                                        "$n",
                                                                        "$$local",
                                                                    ]
                                                                },
                                                                {
                                                                    "$eq": [
                                                                        "$n",
                                                                        "$$wanted",
                                                                    ]
                                                                },
                                                            ]
                                                        }
                                                    }
                                                }
                                            ],
                                            "as": "joined",
                                        }
                                    },
                                    {"$project": {"_id": 0, "n": 1, "joined": 1}},
                                ]
                            }
                        },
                    ]
                    expected = [
                        {"values": [{"n": 2, "joined": [{"_id": "foreign", "n": 2}]}]}
                    ]
                    assert (
                        list(client.test.records.aggregate(pipeline, let={"wanted": 2}))
                        == expected
                    )
                    assert (
                        client.test.command(
                            {
                                "aggregate": "records",
                                "pipeline": pipeline,
                                "let": {"wanted": 2},
                                "cursor": {},
                            }
                        )["cursor"]["firstBatch"]
                        == expected
                    )
                    for expression, code in (("$$", 9), ("$$local", 17276)):
                        with pytest.raises(pymongo.errors.OperationFailure) as caught:
                            list(
                                client.test.empty.aggregate(
                                    [{"$project": {"v": expression}}]
                                )
                            )
                        assert caught.value.code == code
                        assert caught.value.details["codeName"] == (
                            "FailedToParse"
                            if code == failed_to_parse
                            else "Location17276"
                        )
                    assert (
                        client.admin.command("hello")["maxWireVersion"]
                        == expected_max_wire_version
                    )

            await asyncio.to_thread(run_client)

    asyncio.run(exercise())
