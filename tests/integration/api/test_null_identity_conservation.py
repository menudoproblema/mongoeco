"""Null-ID mutations retain the SPI effect and publish exactly one insert."""

import asyncio
import re

from dataclasses import replace

import pytest

from bson import Regex as BsonRegex

from mongoeco import AsyncMongoClient
from mongoeco.core.identity import assert_valid_root_document_id
from mongoeco.engines.results import MutationOutcome
from mongoeco.errors import WriteError
from mongoeco.types import Regex, UpdateResult

from tests.integration.api.test_insert_many_write_contract import BATCH_FACTORIES


def test_update_result_null_upsert_flag_preserves_legacy_constructors_and_invariants():
    legacy = UpdateResult(0, 0, "new", True)  # noqa: FBT003 - preserve positional constructor
    assert legacy.did_upsert
    null = UpdateResult(0, 0, None, acknowledged=True, did_upsert=True)
    assert null.did_upsert
    assert null.upserted_id is None
    assert not UpdateResult(0, 0).did_upsert
    outcome = MutationOutcome(null, after_document={"_id": None}, commit_sequence=1)
    assert outcome.after_document == {"_id": None}
    assert replace(outcome, commit_sequence=2).result.did_upsert
    with pytest.raises(ValueError, match="non-matching"):
        MutationOutcome(UpdateResult(0, 0), after_document={"_id": None})
    with pytest.raises(ValueError, match="cannot also match"):
        MutationOutcome(
            UpdateResult(1, 0, did_upsert=True), after_document={"_id": None}
        )


@pytest.mark.parametrize(
    "identifier", [Regex("x", "i"), BsonRegex("x", "i"), re.compile("x", re.IGNORECASE)]
)
def test_root_regex_validation_covers_public_internal_and_python_regex(identifier):
    with pytest.raises(WriteError) as failure:
        assert_valid_root_document_id(identifier)
    assert failure.value.code == 53  # noqa: PLR2004 - native InvalidIdField code
    assert_valid_root_document_id({"pattern": identifier})


@pytest.mark.parametrize("factory", BATCH_FACTORIES)
@pytest.mark.parametrize(
    "method",
    [
        "update_one",
        "update_many",
        "replace_one",
        "find_one_and_update",
        "find_one_and_replace",
    ],
)
def test_null_upsert_publishes_one_insert_then_a_noop_publishes_nothing(
    factory, method
):
    async def exercise():
        async with AsyncMongoClient(factory()) as client:
            collection = client.null_identity.items
            stream = collection.watch(max_await_time_ms=5)
            try:
                update = {"value": 1} if "replace" in method else {"$set": {"value": 1}}
                await getattr(collection, method)({"_id": None}, update, upsert=True)
                event = await stream.try_next()
                assert event["operationType"] == "insert"
                assert event["documentKey"] == {"_id": None}
                assert event["fullDocument"] == {"_id": None, "value": 1}
                assert await stream.try_next() is None
                # A matched update with the same value remains a no-op.
                result = await collection.update_one(
                    {"_id": None}, {"$set": {"value": 1}}, upsert=True
                )
                assert not result.did_upsert
                assert result.matched_count == 1
                assert await stream.try_next() is None
                await collection.update_one({"_id": None}, {"$set": {"value": 2}})
                event = await stream.try_next()
                assert event["operationType"] == "update"
                assert event["documentKey"] == {"_id": None}
                assert await stream.try_next() is None
            finally:
                stream.close()

    asyncio.run(exercise())
