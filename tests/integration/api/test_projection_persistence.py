"""Projection context and execution metadata after SQLite reopen."""

from mongoeco import MongoClient
from mongoeco.engines import SQLiteEngine
from mongoeco.types import Decimal128


def test_projection_context_and_text_metadata_survive_sqlite_reopen(tmp_path):
    path = str(tmp_path / "projections.sqlite")
    document = {"_id": 1, "name": "Ada", "o": {"x": 7}}
    with MongoClient(SQLiteEngine(path)) as client:
        client.test.rows.insert_one(document)
        client.test.rows.create_index([("name", "text")])

    with MongoClient(SQLiteEngine(path)) as client:
        collection = client.test.rows
        assert collection.find_one(
            {"_id": 1},
            {"_id": Decimal128("0"), "value": "$$bound"},
            let={"bound": "kept"},
        ) == {"value": "kept"}
        assert list(
            collection.aggregate(
                [
                    {"$match": {"$text": {"$search": "Ada"}}},
                    {"$project": {"name": 1}},
                    {"$project": {"available": {"$gt": [{"$meta": "textScore"}, 0]}}},
                ]
            )
        ) == [{"_id": 1, "available": True}]
        assert collection.find_one({"_id": 1}) == document
