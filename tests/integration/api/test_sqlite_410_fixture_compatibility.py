"""Read and preserve files created by the last published wheel, with provenance."""

import asyncio
import hashlib
import json
import shutil

from pathlib import Path

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import SQLiteEngine

from tests.integration.api import test_sqlite_45_fixture_compatibility as bridge


FIXTURE = Path("tests/fixtures/sqlite/mongoeco-4.10.1-preparation.sqlite")
NOTE_COUNT = 2


def test_fixture_was_created_by_verified_published_wheel():
    metadata = json.loads(FIXTURE.with_suffix(".json").read_text())
    assert metadata["generator"]["version"] == "4.10.1"
    assert metadata["generator"]["fromSitePackages"]
    assert (
        metadata["generator"]["artifactSha256"]
        == "0b4419ec41777a2914e1a331473c5bc2dc3e9770c31a61061b894d89135a7ddd"
    )
    assert metadata["fixtureSha256"] == hashlib.sha256(FIXTURE.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "method",
    [
        "test_current_engine_reads_indexes_search_and_bson",
        "test_current_engine_replays_only_pending_outbox_suffix",
        "test_opening_fixture_is_idempotent_and_coordinates_connections",
        "test_future_schema_rejection_does_not_mutate_fixture_copy",
    ],
)
@pytest.mark.asyncio
async def test_current_engine_preserves_previous_release_fixture(monkeypatch, method):
    monkeypatch.setattr(bridge, "FIXTURE", FIXTURE)
    case = bridge.SQLite45FixtureCompatibilityTests()
    await getattr(case, method)()


@pytest.mark.parametrize("surface", ["sync", "async"])
def test_old_file_supports_mutation_then_reopening(tmp_path, surface):
    path = tmp_path / "previous.sqlite"
    shutil.copyfile(FIXTURE, path)

    async def asynchronous():
        async with AsyncMongoClient(SQLiteEngine(str(path))) as client:
            assert (
                await client.bridge.items.update_many(
                    {"kind": "note"}, {"$set": {"prepared": True}}
                )
            ).modified_count == NOTE_COUNT
        async with AsyncMongoClient(SQLiteEngine(str(path))) as client:
            assert (
                await client.bridge.items.count_documents({"prepared": True})
                == NOTE_COUNT
            )

    if surface == "async":
        asyncio.run(asynchronous())
    else:
        with MongoClient(SQLiteEngine(str(path))) as client:
            assert (
                client.bridge.items.update_many(
                    {"kind": "note"}, {"$set": {"prepared": True}}
                ).modified_count
                == NOTE_COUNT
            )
        with MongoClient(SQLiteEngine(str(path))) as client:
            assert client.bridge.items.count_documents({"prepared": True}) == NOTE_COUNT
