from __future__ import annotations

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from _demo_support import EMBEDDED_APP_DOCUMENTS, demo_database_path, load_demo_documents


def main() -> None:
    database_path = demo_database_path("mongoeco-example.db")

    with MongoClient(SQLiteEngine(database_path)) as client:
        collection = client.embedded.todos

        load_demo_documents(collection, EMBEDDED_APP_DOCUMENTS)

        collection.create_index([("kind", 1)], name="kind_idx")

        open_items = collection.find({"done": False}).sort([("title", 1)]).to_list()
        print("open items:", open_items)

        stats = client.embedded.command({"collStats": "todos"})
        print("collStats count:", stats["count"])


if __name__ == "__main__":
    main()
