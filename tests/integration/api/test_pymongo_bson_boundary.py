import unittest
from datetime import UTC, datetime, timedelta, timezone

from bson.binary import Binary as BsonBinary
from bson.code import Code as BsonCode
from bson.dbref import DBRef as BsonDBRef
from bson.decimal128 import Decimal128 as BsonDecimal128
from bson.max_key import MaxKey as BsonMaxKey
from bson.min_key import MinKey as BsonMinKey
from bson.objectid import ObjectId as BsonObjectId
from bson.regex import Regex as BsonRegex
from bson.timestamp import Timestamp as BsonTimestamp
from pymongo import IndexModel, InsertOne, ReplaceOne, UpdateOne

from mongoeco import (
    Binary,
    DBRef,
    Decimal128,
    ObjectId,
    Regex,
    SON,
    Timestamp,
)
from tests.support import ENGINE_FACTORIES, open_client


class PyMongoBsonBoundaryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_official_bson_inputs_round_trip_through_public_reads(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.official_bson_boundary
                    object_id = BsonObjectId()
                    reference_id = BsonObjectId()
                    await collection.insert_one(
                        {
                            "_id": object_id,
                            "payload": {
                                "binary": BsonBinary(b"data", subtype=128),
                                "decimal": BsonDecimal128("12.50"),
                                "regex": BsonRegex("^value", "im"),
                                "timestamp": BsonTimestamp(10, 2),
                                "reference": BsonDBRef(
                                    "other",
                                    reference_id,
                                    "test",
                                    tenant="acme",
                                ),
                                "code": BsonCode("return value", {"value": 1}),
                                "minimum": BsonMinKey(),
                                "maximum": BsonMaxKey(),
                            },
                        }
                    )

                    found = await collection.find_one({"_id": object_id})
                    aggregated = (
                        await collection.aggregate(
                            [{"$match": {"_id": object_id}}]
                        ).to_list()
                    )[0]

                    for document in (found, aggregated):
                        assert document is not None
                        payload = document["payload"]
                        self.assertIs(type(document["_id"]), BsonObjectId)
                        self.assertIs(type(payload["binary"]), BsonBinary)
                        self.assertIs(type(payload["decimal"]), BsonDecimal128)
                        self.assertIs(type(payload["regex"]), BsonRegex)
                        self.assertIs(type(payload["timestamp"]), BsonTimestamp)
                        self.assertIs(type(payload["reference"]), BsonDBRef)
                        self.assertIs(type(payload["code"]), BsonCode)
                        self.assertIs(type(payload["minimum"]), BsonMinKey)
                        self.assertIs(type(payload["maximum"]), BsonMaxKey)

    async def test_public_boundary_uses_pymongo_bson_types(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.bson_boundary
                    result = await collection.insert_one(
                        {
                            "payload": {
                                "object_id": ObjectId(),
                                "binary": Binary(b"data", subtype=128),
                                "decimal": Decimal128("12.50"),
                                "regex": Regex("^value", "im"),
                                "timestamp": Timestamp(10, 2),
                                "reference": DBRef(
                                    "other",
                                    ObjectId(),
                                    database="test",
                                    extras={"tenant": "acme"},
                                ),
                                "ordered": SON([("first", 1), ("second", 2)]),
                            }
                        }
                    )

                    self.assertIs(type(result.inserted_id), BsonObjectId)
                    document = await collection.find_one(
                        {"_id": result.inserted_id}
                    )
                    assert document is not None
                    payload = document["payload"]
                    self.assertIs(type(document["_id"]), BsonObjectId)
                    self.assertIs(type(payload["object_id"]), BsonObjectId)
                    self.assertIs(type(payload["binary"]), BsonBinary)
                    self.assertIs(type(payload["decimal"]), BsonDecimal128)
                    self.assertIs(type(payload["regex"]), BsonRegex)
                    self.assertIs(type(payload["timestamp"]), BsonTimestamp)
                    self.assertIs(type(payload["reference"]), BsonDBRef)
                    self.assertIs(type(payload["ordered"]), dict)

                    upsert = await collection.update_one(
                        {"kind": "generated"},
                        {"$set": {"value": 1}},
                        upsert=True,
                    )
                    self.assertIs(type(upsert.upserted_id), BsonObjectId)

    async def test_generated_ids_mutate_input_documents_with_public_bson_types(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.generated_input_ids
                    single = {"kind": "single"}
                    many = [{"kind": "first"}, {"kind": "second"}]

                    single_result = await collection.insert_one(single)
                    many_result = await collection.insert_many(many)

                    self.assertIs(type(single["_id"]), BsonObjectId)
                    self.assertEqual(single["_id"], single_result.inserted_id)
                    for document, inserted_id in zip(
                        many,
                        many_result.inserted_ids,
                        strict=True,
                    ):
                        self.assertIs(type(document["_id"]), BsonObjectId)
                        self.assertEqual(document["_id"], inserted_id)

    async def test_public_boundary_applies_bson_datetime_precision(self):
        source = datetime(
            2026,
            1,
            2,
            10,
            4,
            5,
            987_654,
            tzinfo=timezone(timedelta(hours=2)),
        )
        expected = datetime(2026, 1, 2, 8, 4, 5, 987_000)

        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.datetime_boundary
                    inserted = await collection.insert_one(
                        {
                            "nested": {
                                "values": [source],
                            }
                        }
                    )
                    document = await collection.find_one(
                        {"_id": inserted.inserted_id}
                    )
                    assert document is not None
                    self.assertEqual(document["nested"]["values"][0], expected)
                    self.assertEqual(
                        await collection.count_documents(
                            {"nested.values": source}
                        ),
                        1,
                    )
                    self.assertEqual(
                        await collection.count_documents(
                            {
                                "$expr": {
                                    "$eq": [
                                        {"$arrayElemAt": ["$nested.values", 0]},
                                        "$$expected",
                                    ]
                                }
                            },
                            let={"expected": source},
                        ),
                        1,
                    )

                    projected = (
                        await collection.aggregate(
                            [{"$project": {"literal": source}}]
                        ).to_list()
                    )[0]
                    self.assertEqual(projected["literal"], expected)

                    updated_source = source + timedelta(seconds=1)
                    await collection.update_one(
                        {"_id": inserted.inserted_id},
                        {"$set": {"updated_at": updated_source}},
                    )
                    updated = await collection.find_one(
                        {"_id": inserted.inserted_id}
                    )
                    assert updated is not None
                    self.assertEqual(
                        updated["updated_at"],
                        expected + timedelta(seconds=1),
                    )
                    self.assertIsNone(updated["updated_at"].tzinfo)

                    replacement_source = source + timedelta(seconds=2)
                    await collection.replace_one(
                        {"_id": inserted.inserted_id},
                        {
                            "_id": inserted.inserted_id,
                            "replaced_at": replacement_source,
                        },
                    )
                    replaced = await collection.find_one(
                        {"replaced_at": replacement_source}
                    )
                    assert replaced is not None
                    self.assertEqual(
                        replaced["replaced_at"],
                        expected + timedelta(seconds=2),
                    )

                    bulk_source = source + timedelta(seconds=3)
                    final_bulk_source = source + timedelta(seconds=5)
                    await collection.bulk_write(
                        [
                            InsertOne(
                                {"_id": "bulk", "observed_at": bulk_source}
                            ),
                            UpdateOne(
                                {"_id": "bulk"},
                                {
                                    "$set": {
                                        "observed_at": source
                                        + timedelta(seconds=4)
                                    }
                                },
                            ),
                            ReplaceOne(
                                {"_id": "bulk"},
                                {
                                    "_id": "bulk",
                                    "observed_at": final_bulk_source,
                                },
                            ),
                        ]
                    )
                    bulk_document = await collection.find_one(
                        {"observed_at": final_bulk_source}
                    )
                    assert bulk_document is not None
                    self.assertEqual(
                        bulk_document["observed_at"],
                        expected + timedelta(seconds=5),
                    )

    async def test_projection_and_partial_indexes_use_the_bson_boundary(self):
        source = datetime(
            2026,
            1,
            2,
            10,
            4,
            5,
            987_654,
            tzinfo=timezone(timedelta(hours=2)),
        )
        expected = datetime(2026, 1, 2, 8, 4, 5, 987_000)

        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.bson_boundary_adjacencies
                    object_id = BsonObjectId()
                    await collection.insert_one(
                        {
                            "_id": "selected",
                            "observed_at": source,
                            "items": [{"object_id": object_id, "at": source}],
                        }
                    )
                    await collection.create_indexes(
                        [
                            IndexModel(
                                [("observed_at", 1)],
                                name="observed_partial",
                                partialFilterExpression={
                                    "observed_at": {"$gte": source},
                                    "items.object_id": object_id,
                                },
                            )
                        ]
                    )

                    projected = await collection.find_one(
                        {"_id": "selected"},
                        {
                            "items": {
                                "$elemMatch": {
                                    "object_id": object_id,
                                    "at": source,
                                }
                            }
                        },
                    )

                    assert projected is not None
                    self.assertEqual(
                        projected["items"],
                        [{"object_id": object_id, "at": expected}],
                    )
                    self.assertEqual(
                        await collection.count_documents(
                            {
                                "observed_at": source,
                                "items.object_id": object_id,
                            },
                            hint="observed_partial",
                        ),
                        1,
                    )
                    listed = {
                        document["name"]: document
                        for document in await collection.list_indexes().to_list()
                    }
                    information = await collection.index_information()
                    for partial_filter in (
                        listed["observed_partial"]["partialFilterExpression"],
                        information["observed_partial"][
                            "partialFilterExpression"
                        ],
                    ):
                        self.assertEqual(
                            partial_filter["observed_at"]["$gte"],
                            expected,
                        )
                        self.assertIs(
                            type(partial_filter["items.object_id"]),
                            BsonObjectId,
                        )

    async def test_change_stream_events_use_the_public_bson_boundary(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.bson_change_stream
                    stream = collection.watch(max_await_time_ms=5)
                    nested_id = BsonObjectId()

                    result = await collection.insert_one(
                        {"nested": {"object_id": nested_id}}
                    )
                    event = await stream.try_next()

                    assert event is not None
                    self.assertIs(
                        type(event["documentKey"]["_id"]),
                        BsonObjectId,
                    )
                    self.assertEqual(
                        event["documentKey"]["_id"],
                        result.inserted_id,
                    )
                    self.assertIs(
                        type(event["fullDocument"]["nested"]["object_id"]),
                        BsonObjectId,
                    )


if __name__ == "__main__":
    unittest.main()
