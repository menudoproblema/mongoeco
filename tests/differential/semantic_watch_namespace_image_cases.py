"""Images, namespace effects and continuation through the public watch APIs."""

import time

from functools import partial

from bson import Binary, Int64

from tests.differential.semantic_watch_lifecycle_cases import _observe
from tests.differential.semantic_watch_protocol_cases import _owners


SEED = [{"_id": "initial"}]


def _observed(event, database):
    result = _observe(event, database)
    if result.get("fullDocument") is not None:
        result["fullDocument"] = {
            key: value for key, value in result["fullDocument"].items() if key != "pad"
        }
    return result


def _through_marker(stream, database, marker):
    events, tokens = [], []
    deadline = time.monotonic() + 5
    while stream.alive and time.monotonic() < deadline:
        event = stream.try_next()
        if event is None:
            continue
        assert event["_id"] not in tokens, "duplicate event token"
        tokens.append(event["_id"])
        events.append(_observed(event, database))
        if event.get("documentKey") == {"_id": marker}:
            return events
    assert not stream.alive, "following namespace barrier was not delivered"
    return events


def namespace_images(collection, action):
    database = collection.database
    client = dict(_owners(collection))["client"]
    output = {}
    for scope in ("collection", "database", "client"):
        for enabled in (False, True):
            for mode in ("default", "updateLookup", "whenAvailable", "required"):
                for recreate in (False, True):
                    client.drop_database(database.name)
                    subject = database.create_collection(
                        "source", changeStreamPreAndPostImages={"enabled": enabled}
                    )
                    subject.insert_one(
                        {
                            "_id": 1,
                            "v": 0,
                            "n": Int64(7),
                            "b": Binary(b"same", 5),
                            "pad": "x" * 1000,
                        }
                    )
                    owner = {
                        "collection": subject,
                        "database": database,
                        "client": client,
                    }[scope]
                    pipeline = [
                        {
                            "$match": {
                                "$or": [
                                    {"ns.db": database.name},
                                    {"operationType": "invalidate"},
                                ]
                            }
                        },
                        {"$set": {"numberType": {"$type": "$fullDocument.n"}}},
                    ]
                    stream = owner.watch(
                        pipeline, full_document=mode, max_await_time_ms=20
                    )
                    resumed = None
                    try:
                        subject.update_one({}, {"$set": {"v": 1}})
                        mutate = {
                            "drop": subject.drop,
                            "rename": partial(subject.rename, "target"),
                            "drop_database": partial(
                                client.drop_database, database.name
                            ),
                        }
                        mutate[action]()
                        if recreate:
                            replacement = database.create_collection(
                                "source",
                                changeStreamPreAndPostImages={"enabled": not enabled},
                            )
                            replacement.insert_one(
                                {
                                    "_id": 1,
                                    "v": 9,
                                    "n": Int64(9),
                                    "b": Binary(b"new", 0),
                                }
                            )
                        database["barrier"].insert_one({"_id": "after_ddl"})
                        try:
                            events = _through_marker(stream, database.name, "after_ddl")
                        except Exception as error:
                            if getattr(error, "code", None) is None:
                                raise
                            result = {
                                "error": type(error).__name__,
                                "code": error.code,
                                "code_name": (
                                    getattr(error, "details", None) or {}
                                ).get("codeName"),
                                "alive": stream.alive,
                            }
                        else:
                            options = {
                                "resume_after"
                                if stream.alive
                                else "start_after": stream.resume_token
                            }
                            result = {"events": events, "alive": stream.alive}
                            resumed = owner.watch(
                                pipeline,
                                full_document=mode,
                                max_await_time_ms=20,
                                **options,
                            )
                            subject.insert_one({"_id": "after_resume", "n": Int64(11)})
                            following = _through_marker(
                                resumed, database.name, "after_resume"
                            )
                            assert following[-1]["documentKey"] == {
                                "_id": "after_resume"
                            }
                            result["resumed"] = following
                            result["resumed_alive"] = resumed.alive
                        name = f"{scope}/{enabled}/{mode}/{recreate}"
                        output[name] = result
                    finally:
                        if resumed is not None:
                            resumed.close()
                        stream.close()
    return output


def dropped_images(collection):
    return namespace_images(collection, "drop")


def renamed_images(collection):
    return namespace_images(collection, "rename")


def dropped_database_images(collection):
    return namespace_images(collection, "drop_database")


SEMANTIC_WATCH_NAMESPACE_IMAGE_CASES = (
    ("semantic_watch_dropped_images", dropped_images),
    ("semantic_watch_renamed_images", renamed_images),
    ("semantic_watch_dropped_database_images", dropped_database_images),
)
