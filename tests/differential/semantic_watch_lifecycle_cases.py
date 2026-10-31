"""Namespace effects, scope and following writes against native replica sets."""

import time


SEED = [{"_id": "initial"}]


def _observe(event, database):
    observed = {
        key: value
        # Event envelope member order is not the namespace contract. Retain
        # every other field and all ordered BSON values without normalization.
        for key in sorted(event.keys() - {"_id", "clusterTime", "wallTime"})
        for value in (event[key],)
    }
    for field in ("ns", "to"):
        if field in observed:
            observed[field] = dict(observed[field])
            if observed[field].get("db") == database:
                observed[field]["db"] = "<database>"
    return observed


def _mutate_namespace(action, db, client, source, missing):
    if action == "drop_api":
        source.drop()
    elif action == "drop_command":
        db.command({"drop": "source"})
    elif action == "rename_api":
        source.rename("target")
    elif action == "drop_database_command":
        db.command({"dropDatabase": 1})
    elif action == "drop_missing":
        missing.drop()
    else:
        client.drop_database(db.name)


def namespace_lifecycle(collection):
    db = collection.database
    client = getattr(collection, "_client", None)
    if client is None:
        client = db.client
    output = {}
    actions = (
        "drop_api",
        "drop_command",
        "rename_api",
        "drop_database_api",
        "drop_database_command",
        "drop_missing",
        "drop_empty_database",
    )
    for action in actions:
        for scope in ("collection", "missing", "target", "database", "client"):
            client.drop_database(db.name)
            source, target, missing = db["source"], db["target"], db["missing"]
            if action != "drop_empty_database":
                source.insert_one({"_id": "initial"})
            owner = {
                "collection": source,
                "missing": missing,
                "target": target,
                "database": db,
                "client": client,
            }[scope]
            stream = owner.watch(max_await_time_ms=20)
            try:
                _mutate_namespace(action, db, client, source, missing)
                barrier = {
                    "collection": source,
                    "missing": missing,
                    "target": target,
                }.get(scope, db["barrier"])
                barrier.insert_one({"_id": "barrier"})
                events = []
                deadline = time.monotonic() + 3
                while stream.alive and time.monotonic() < deadline:
                    event = stream.try_next()
                    if event is not None:
                        events.append(_observe(event, db.name))
                        if event["operationType"] == "insert":
                            break
                if stream.alive and not any(
                    event["operationType"] == "insert" for event in events
                ):
                    message = "following namespace write was not delivered"
                    raise AssertionError(message)
                output[f"{action}/{scope}"] = {"events": events, "alive": stream.alive}
            finally:
                stream.close()
    return output


SEMANTIC_WATCH_LIFECYCLE_CASES = (
    ("semantic_watch_namespace_lifecycle", namespace_lifecycle),
)
