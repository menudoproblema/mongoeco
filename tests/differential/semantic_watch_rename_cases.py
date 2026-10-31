"""Administrative rename publishes one event, including destination overwrite."""

import time

from tests.differential.semantic_rename_command_cases import _client, command_spec
from tests.differential.semantic_watch_lifecycle_cases import _observe


SEED = [{"_id": "initial"}]


def rename_events(collection):
    db, client = collection.database, _client(collection)
    output = {}
    for action in (
        "default",
        "overwrite",
        "collision",
        "missing_source",
        "same",
        "nonadmin",
    ):
        for scope in ("source", "target", "database", "client"):
            client.drop_database(db.name)
            if action != "missing_source":
                db["source"].insert_one({"_id": "source"})
            if action != "default":
                db["target"].insert_one({"_id": "target"})
            owner = {
                "source": db["source"],
                "target": db["target"],
                "database": db,
                "client": client,
            }[scope]
            stream = owner.watch(max_await_time_ms=20)
            try:
                try:
                    command_owner = db if action == "nonadmin" else client["admin"]
                    command_owner.command(
                        command_spec(action, db.name, "source", "target")
                    )
                    result = {"ok": True}
                except Exception as error:
                    result = {
                        "error": type(error).__name__,
                        "code": getattr(error, "code", None),
                    }
                result["source"] = list(db["source"].find({}))
                result["target"] = list(db["target"].find({}))
                barrier = db[scope] if scope in {"source", "target"} else db["barrier"]
                barrier.insert_one({"_id": "barrier"})
                events, delivered = [], False
                deadline = time.monotonic() + 3
                while stream.alive and time.monotonic() < deadline:
                    event = stream.try_next()
                    if event is not None:
                        if event["operationType"] == "insert":
                            delivered = True
                            break
                        events.append(_observe(event, db.name))
                if stream.alive and not delivered:
                    message = "rename or following barrier was not delivered"
                    raise AssertionError(message)
                result.update(events=events, alive=stream.alive)
                output[f"{action}/{scope}"] = result
            finally:
                stream.close()
    return output


SEMANTIC_WATCH_RENAME_CASES = (("semantic_watch_rename", rename_events),)
