"""Physical IDs survive persistence; unsupported index signatures delegate."""

import struct

from unittest.mock import patch

import pytest

from bson import BSON, Decimal128

from mongoeco import MongoClient
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines import SQLiteEngine
from mongoeco.errors import DuplicateKeyError


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_sqlite_nan_id_reopens_and_keeps_stored_key_for_unrelated_updates(
    tmp_path, nested, dialect
):
    number = struct.unpack("<d", bytes.fromhex("010000000000f87f"))[0]
    value = {"n": [{"v": number}]} if nested else number
    path = str(tmp_path / "nan.sqlite")
    engine = SQLiteEngine(path)
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.nan.records
        collection.insert_one({"_id": value, "v": 0})
        collection.create_index([("v", 1)])
        old_key = engine._connection.execute(
            "SELECT storage_key FROM documents"
        ).fetchone()[0]
    engine = SQLiteEngine(path)
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.nan.records
        assert collection.update_one({}, {"$set": {"v": 1}}).modified_count == 1
        stored = collection.find_one({"v": 1}, hint="v_1")
        assert BSON.encode({"_id": stored["_id"]}) == BSON.encode({"_id": value})
        assert (
            engine._connection.execute("SELECT storage_key FROM documents").fetchone()[
                0
            ]
            == old_key
        )
        assert collection.delete_one({}).deleted_count == 1


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("array", [False, True])
def test_nonfinite_indexes_delegate_and_match_disabled_translation(dialect, array):
    engine = SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.nonfinite.records
        values = [float("nan"), float("inf"), Decimal128("NaN"), 1]
        collection.insert_many(
            [{"_id": i, "v": [v] if array else v} for i, v in enumerate(values)]
        )
        collection.create_index([("v", 1)], name="nonfinite")
        for operand, expected in ((float("nan"), [0, 2]), (1, [3])):
            query = {"v": operand}
            with patch.object(
                QueryEngine, "match_plan", wraps=QueryEngine.match_plan
            ) as matcher:
                assert [
                    r["_id"]
                    for r in collection.find(query, hint="nonfinite", sort=[("_id", 1)])
                ] == expected
            assert matcher.call_count > 0
            with patch.object(
                engine,
                "_translate_query_plan_with_multikey",
                side_effect=NotImplementedError,
            ):
                assert [
                    r["_id"]
                    for r in collection.find(query, hint="nonfinite", sort=[("_id", 1)])
                ] == expected


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), Decimal128("NaN"), Decimal128("Infinity")]
)
def test_nonfinite_unique_index_fallback_keeps_conflicts_and_atomic_state(
    value,
):
    with MongoClient(SQLiteEngine()) as client:
        collection = client.nonfinite_unique.records
        collection.create_index([("v", 1)], unique=True)
        collection.insert_many([{"_id": 1, "v": value}, {"_id": 2, "v": 1}])
        original = [BSON.encode(row) for row in collection.find({}, sort=[("_id", 1)])]
        with pytest.raises(DuplicateKeyError) as error:
            collection.update_one({"_id": 2}, [{"$set": {"v": {"$literal": value}}}])
        assert error.value.code == 11000  # noqa: PLR2004 - native duplicate-key code
        assert [
            BSON.encode(row) for row in collection.find({}, sort=[("_id", 1)])
        ] == original
        assert collection.count_documents({"v": value}) == 1
