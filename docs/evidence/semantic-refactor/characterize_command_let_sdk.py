"""Observe SDK clone separately from equivalent native find requests.

Run with MONGOECO_REAL_MONGODB_URI pointing to an isolated server. This probe
uses only PyMongo and does not turn an SDK omission into a server expectation.
"""

import json
import os
import uuid
from pathlib import Path
from sys import argv

import pymongo
from pymongo import monitoring


class FindCommands(monitoring.CommandListener):
    def __init__(self):
        self.requests = []

    def started(self, event):
        if event.command_name == "find":
            self.requests.append(
                {"let": event.command.get("let"), "let_present": "let" in event.command}
            )

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


def main():
    listener = FindCommands()
    with pymongo.MongoClient(
        os.environ["MONGOECO_REAL_MONGODB_URI"],
        event_listeners=[listener],
        serverSelectionTimeoutMS=3000,
    ) as client:
        name = "mongoeco_sdk_let_" + uuid.uuid4().hex
        collection = client[name].records
        try:
            collection.insert_one({"_id": 1, "a": 5})
            query = {"$expr": {"$eq": ["$a", "$$x"]}}
            cursor = collection.find(query, let={"x": 5})
            result = {
                "pymongo": pymongo.version,
                "server": client.server_info()["version"],
                "original": list(cursor),
            }
            try:
                result["clone"] = {"documents": list(cursor.clone())}
            except pymongo.errors.OperationFailure as error:
                result["clone"] = {
                    "family": type(error).__name__,
                    "code": error.code,
                    "errmsg": error.details.get("errmsg"),
                }
            result["fresh"] = list(collection.find(query, let={"x": 5}))
            result["find_commands"] = listener.requests
            Path(argv[1]).write_text(json.dumps(result, indent=2) + "\n")
        finally:
            client.drop_database(name)


if __name__ == "__main__":
    main()
