"""Native TTL delete effects, independent of scheduling latency and tokens."""

import asyncio
import inspect
import time

from datetime import UTC, datetime, timedelta


SEED = [{"_id": "initial"}]
_EXPECTED_TTL_DELETIONS = 2


def _resolve(value):
    return asyncio.run(value) if inspect.isawaitable(value) else value


def deletes(collection):
    collection.delete_many({})
    future = datetime(2100, 1, 1, tzinfo=UTC)
    collection.insert_many([{"_id": i, "expires": future} for i in (1, 2, 3)])
    collection.database.command(
        {
            "createIndexes": collection.name,
            "indexes": [
                {"name": "expires_1", "key": {"expires": 1}, "expireAfterSeconds": 0}
            ],
        }
    )
    stream = _resolve(
        collection.watch(
            [{"$match": {"operationType": "delete"}}], max_await_time_ms=50
        )
    )
    observed = []
    try:
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        for i, milliseconds in ((1, 1001), (2, -1001)):
            collection.update_one(
                {"_id": i},
                {"$set": {"expires": epoch + timedelta(milliseconds=milliseconds)}},
            )
        deadline = time.monotonic() + 5
        while len(observed) < _EXPECTED_TTL_DELETIONS and time.monotonic() < deadline:
            # Local TTL is demand-driven; MongoDB uses its background monitor.
            collection.count_documents({})
            event = _resolve(stream.try_next())
            if event is not None:
                observed.append(event["documentKey"]["_id"])
        extra = _resolve(stream.try_next())
        return {
            "deleted": sorted(observed),
            "extra": None if extra is None else extra["documentKey"]["_id"],
            "remaining": [d["_id"] for d in collection.find({}, sort=[("_id", 1)])],
        }
    finally:
        _resolve(stream.close())


SEMANTIC_TTL_PUBLICATION_CASES = (("semantic_ttl_publication_deletes", deletes),)
