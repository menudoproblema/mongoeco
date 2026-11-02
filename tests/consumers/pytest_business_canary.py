"""Standalone SDK consumer: account, order and ledger contracts over a database URI.

The existing canary launcher replaces only ``open_client``. This corpus is new;
it does not claim to reconstruct the unavailable historical private consumer.
"""

from datetime import UTC, datetime
from uuid import UUID

import pytest

from bson import Decimal128
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from pymongo import ASCENDING, DeleteOne, InsertOne, ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError, OperationFailure


# Business examples retain literal expected counts and amounts.
# ruff: noqa: PLR2004
pytestmark = pytest.mark.anyio


def open_client():
    message = "The isolated launcher must inject a native SDK client"
    raise RuntimeError(message)


def accounts():
    return open_client().business.accounts


async def test_account_creation():
    result = await accounts().insert_one({"_id": "a", "name": "Ada"})
    assert result.inserted_id == "a"
    assert await accounts().find_one({"_id": "a"}) == {"_id": "a", "name": "Ada"}


async def test_duplicate_account_is_atomic():
    await accounts().insert_one({"_id": "a", "name": "Ada"})
    with pytest.raises(DuplicateKeyError):
        await accounts().insert_one({"_id": "a", "name": "Other"})
    assert (await accounts().find_one({"_id": "a"}))["name"] == "Ada"


async def test_account_projection():
    await accounts().insert_one({"_id": "a", "name": "Ada", "secret": 7})
    assert await accounts().find_one({}, {"_id": 0, "name": 1}) == {"name": "Ada"}


async def test_missing_account():
    assert await accounts().find_one({"_id": "absent"}) is None


async def test_ordered_account_page():
    await accounts().insert_many([{"_id": n, "rank": n} for n in (3, 1, 2)])
    rows = await accounts().find().sort("rank", ASCENDING).skip(1).limit(1).to_list()
    assert rows == [{"_id": 2, "rank": 2}]


async def test_account_count():
    await accounts().insert_many([{"_id": n, "active": n % 2 == 0} for n in range(4)])
    assert await accounts().count_documents({"active": True}) == 2


async def test_distinct_account_regions():
    await accounts().insert_many([{"region": r} for r in ("east", "west", "east")])
    assert sorted(await accounts().distinct("region")) == ["east", "west"]


async def test_rename_account():
    await accounts().insert_one({"_id": "a", "name": "Ada"})
    result = await accounts().update_one({"_id": "a"}, {"$set": {"name": "Grace"}})
    assert (result.matched_count, result.modified_count) == (1, 1)
    assert (await accounts().find_one({"_id": "a"}))["name"] == "Grace"


async def test_repeated_balance_is_noop():
    await accounts().insert_one({"_id": "a", "balance": 1.0})
    result = await accounts().update_one({"_id": "a"}, {"$set": {"balance": 1.0}})
    assert (result.matched_count, result.modified_count) == (1, 0)


async def test_increment_balance():
    await accounts().insert_one({"_id": "a", "balance": 10})
    await accounts().update_one({"_id": "a"}, {"$inc": {"balance": -3}})
    assert (await accounts().find_one({"_id": "a"}))["balance"] == 7


async def test_idempotent_provisioning():
    for _ in range(2):
        await accounts().update_one(
            {"_id": "a"}, {"$setOnInsert": {"balance": 0}}, upsert=True
        )
    assert await accounts().count_documents({}) == 1
    assert (await accounts().find_one({"_id": "a"}))["balance"] == 0


async def test_replace_account():
    await accounts().insert_one({"_id": "a", "old": True})
    result = await accounts().replace_one({"_id": "a"}, {"name": "Ada"})
    assert result.modified_count == 1
    assert await accounts().find_one({"_id": "a"}) == {"_id": "a", "name": "Ada"}


async def test_return_updated_account():
    await accounts().insert_one({"_id": "a", "balance": 10})
    row = await accounts().find_one_and_update(
        {"_id": "a"}, {"$inc": {"balance": 2}}, return_document=ReturnDocument.AFTER
    )
    assert row == {"_id": "a", "balance": 12}


async def test_return_previous_account():
    await accounts().insert_one({"_id": "a", "balance": 10})
    row = await accounts().find_one_and_update({"_id": "a"}, {"$inc": {"balance": 2}})
    assert row == {"_id": "a", "balance": 10}


async def test_delete_account():
    await accounts().insert_one({"_id": "a"})
    assert (await accounts().delete_one({"_id": "a"})).deleted_count == 1
    assert await accounts().find_one({"_id": "a"}) is None


async def test_archive_accounts():
    await accounts().insert_many([{"_id": n, "archived": n < 2} for n in range(3)])
    assert (await accounts().delete_many({"archived": True})).deleted_count == 2
    assert await accounts().count_documents({}) == 1


async def test_bulk_account_import():
    result = await accounts().bulk_write(
        [
            InsertOne({"_id": "a", "balance": 1}),
            UpdateOne({"_id": "a"}, {"$inc": {"balance": 2}}),
            InsertOne({"_id": "b"}),
            DeleteOne({"_id": "b"}),
        ]
    )
    assert (result.inserted_count, result.modified_count, result.deleted_count) == (
        2,
        1,
        1,
    )
    assert await accounts().find_one({"_id": "a"}) == {"_id": "a", "balance": 3}


async def test_region_ledger_total():
    await accounts().insert_many(
        [{"region": r, "balance": n} for r, n in [("e", 2), ("e", 3), ("w", 7)]]
    )
    rows = await accounts().aggregate(
        [
            {"$group": {"_id": "$region", "total": {"$sum": "$balance"}}},
            {"$sort": {"_id": 1}},
        ]
    )
    assert await rows.to_list() == [{"_id": "e", "total": 5}, {"_id": "w", "total": 7}]


async def test_unpaid_orders():
    await accounts().insert_many(
        [{"_id": "a", "paid": False}, {"_id": "b", "paid": True}]
    )
    cursor = await accounts().aggregate(
        [{"$match": {"paid": False}}, {"$project": {"_id": 1}}]
    )
    assert await cursor.to_list() == [{"_id": "a"}]


async def test_order_customer_join():
    await accounts().insert_one({"_id": "a", "name": "Ada"})
    orders = open_client().business.orders
    await orders.insert_one({"_id": 1, "account": "a"})
    cursor = await orders.aggregate(
        [
            {
                "$lookup": {
                    "from": "accounts",
                    "localField": "account",
                    "foreignField": "_id",
                    "as": "customer",
                }
            }
        ]
    )
    assert (await cursor.to_list())[0]["customer"] == [{"_id": "a", "name": "Ada"}]


async def test_customer_tags():
    await accounts().insert_one({"_id": "a", "tags": ["new"]})
    await accounts().update_one({}, {"$addToSet": {"tags": "new"}})
    await accounts().update_one({}, {"$push": {"tags": "vip"}})
    assert (await accounts().find_one({}))["tags"] == ["new", "vip"]


async def test_remove_customer_tag():
    await accounts().insert_one({"_id": "a", "tags": ["new", "vip"]})
    await accounts().update_one({}, {"$pull": {"tags": "new"}})
    assert (await accounts().find_one({}))["tags"] == ["vip"]


async def test_numeric_search_excludes_text():
    await accounts().insert_many(
        [{"_id": "number", "balance": 8}, {"_id": "text", "balance": "8"}]
    )
    assert [r["_id"] async for r in accounts().find({"balance": {"$gt": 5}})] == [
        "number"
    ]


async def test_missing_and_null_query():
    await accounts().insert_many(
        [
            {"_id": "missing"},
            {"_id": "null", "refund": None},
            {"_id": "paid", "refund": 1},
        ]
    )
    assert sorted([r["_id"] async for r in accounts().find({"refund": None})]) == [
        "missing",
        "null",
    ]


async def test_explicit_null_expression():
    await accounts().insert_many([{"_id": "missing"}, {"_id": "null", "refund": None}])
    cursor = await accounts().aggregate(
        [{"$match": {"$expr": {"$eq": ["$refund", None]}}}]
    )
    assert [r["_id"] async for r in cursor] == ["null"]


async def test_pipeline_balance_type_change():
    await accounts().insert_one({"_id": "a", "balance": 1})
    result = await accounts().update_one({}, [{"$set": {"balance": 1.0}}])
    assert result.modified_count == 1
    assert type((await accounts().find_one({}))["balance"]) is float


async def test_decimal_balance_roundtrip():
    await accounts().insert_one({"_id": "a", "balance": Decimal128("10.25")})
    assert (await accounts().find_one({}))["balance"] == Decimal128("10.25")


async def test_invoice_timestamp_roundtrip():
    stamp = datetime(2020, 1, 1, 0, 0, 1, 1000, tzinfo=UTC)
    collection = accounts().with_options(codec_options=CodecOptions(tz_aware=True))
    await collection.insert_one({"_id": "a", "at": stamp})
    assert (await collection.find_one({}))["at"] == stamp


async def test_standard_uuid_identifier():
    value = UUID("00000000-0000-0000-0000-000000000001")

    collection = accounts().with_options(
        codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
    )
    await collection.insert_one({"_id": value})
    assert (await collection.find_one({"_id": value}))["_id"] == value


async def test_unique_customer_email():
    await accounts().create_index("email", unique=True)
    await accounts().insert_one({"email": "a@example.invalid"})
    with pytest.raises(DuplicateKeyError):
        await accounts().insert_one({"email": "a@example.invalid"})
    assert await accounts().count_documents({}) == 1


async def test_invalid_query_preserves_session():
    with pytest.raises(OperationFailure):
        await accounts().find_one({"$invalid": 1})
    await accounts().insert_one({"_id": "after"})
    assert await accounts().find_one({"_id": "after"}) == {"_id": "after"}


def test_database_selection():
    assert open_client()["business"].name == "business"


def test_collection_selection():
    assert accounts().name == "accounts"


def test_collection_namespace():
    assert accounts().full_name == "business.accounts"


def test_projection_cursor_construction():
    assert accounts().find({}, {"name": 1}) is not None


def test_sorted_cursor_construction():
    assert accounts().find().sort("name", ASCENDING) is not None


def test_limited_cursor_construction():
    assert accounts().find().skip(1).limit(2) is not None


def test_aware_collection_options():
    assert (
        accounts()
        .with_options(codec_options=CodecOptions(tz_aware=True))
        .codec_options.tz_aware
    )
