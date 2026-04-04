from __future__ import annotations

from pathlib import Path

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine


def main() -> None:
    database_path = Path("mongoeco-example.db")

    with MongoClient(SQLiteEngine(database_path)) as client:
        collection = client.embedded.todos

        collection.delete_many({})
        collection.insert_many(
            [
                {"_id": 1, "kind": "task", "title": "Ship release notes", "done": False},
                {"_id": 2, "kind": "task", "title": "Benchmark search runtime", "done": True},
                {"_id": 3, "kind": "note", "title": "Keep docs aligned", "done": False},
            ]
        )

        collection.create_index([("kind", 1)], name="kind_idx")

        open_items = collection.find({"done": False}).sort([("title", 1)]).to_list()
        print("open items:", open_items)

        stats = client.embedded.command({"collStats": "todos"})
        print("collStats count:", stats["count"])


if __name__ == "__main__":
    main()
