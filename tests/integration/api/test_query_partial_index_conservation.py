"""Legacy partial-index membership must not invent a uniqueness violation."""

import shutil

from pathlib import Path

import pytest

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError


EXPECTED_IDS = [1, 2]


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_reopened_legacy_partial_index_uses_current_query_membership(tmp_path, dialect):
    fixture = (
        Path(__file__).parents[2]
        / "fixtures/sqlite/query-partial-before-bracketing.sqlite"
    )
    path = tmp_path / "legacy.sqlite"
    shutil.copyfile(fixture, path)
    with MongoClient(SQLiteEngine(str(path)), mongodb_dialect=dialect) as client:
        collection = client.db.records
        assert list(collection.find({"a": {"$gt": 5}})) == []
        # The legacy implementation indexed the string row under a numeric
        # partial filter. Native MongoDB allows this second b=1 document.
        collection.insert_one({"_id": 2, "a": 6, "b": 1})
        with pytest.raises(DuplicateKeyError):
            collection.insert_one({"_id": 3, "a": 7, "b": 1})
        assert collection.count_documents({}) == len(EXPECTED_IDS)
    with MongoClient(SQLiteEngine(str(path)), mongodb_dialect=dialect) as client:
        assert [
            row["_id"] for row in client.db.records.find({}, sort=[("_id", 1)])
        ] == EXPECTED_IDS
