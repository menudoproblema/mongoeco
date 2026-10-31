"""A persisted partial index cannot hide newly recognized query matches."""

import shutil

from pathlib import Path

import pytest

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_legacy_traversal_membership_is_conserved_across_reopen(tmp_path, dialect):
    fixture = (
        Path(__file__).parents[2] / "fixtures/sqlite/query-traversal-before.sqlite"
    )
    path = tmp_path / "legacy.sqlite"
    shutil.copyfile(fixture, path)
    with MongoClient(SQLiteEngine(str(path)), mongodb_dialect=dialect) as client:
        collection = client.db.records
        query = {"b": 1, "a.x": {"$gte": None}}
        assert [doc["_id"] for doc in collection.find(query)] == [1]
        assert [doc["_id"] for doc in collection.find(query, hint="partial_b")] == [1]
        with pytest.raises(DuplicateKeyError):
            collection.insert_one({"_id": 2, "a": {"x": None}, "b": 1})
        collection.update_one({"_id": 1}, {"$set": {"a": []}})
        collection.insert_one({"_id": 2, "a": {"x": None}, "b": 1})
    with MongoClient(SQLiteEngine(str(path)), mongodb_dialect=dialect) as client:
        collection = client.db.records
        assert [doc["_id"] for doc in collection.find(query)] == [2]
        assert collection.count_documents({}) == 2  # noqa: PLR2004 - two accepted writes
